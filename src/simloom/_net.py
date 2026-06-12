"""The simulated network: in-memory transports behind the loop primitives.

Real asyncio libraries (aiohttp, httpx, asyncpg, the streams API) bottom out
in ``loop.create_connection`` / ``loop.create_server`` / ``loop.getaddrinfo``.
SimNetwork implements those three against in-memory transport pairs, so
unmodified applications talk to each other in-process, on virtual time, with
every latency and loss decision drawn from the choice tape.

Design rules carried from the Phase 0 spikes:

- **Stream order is a network invariant, not a scheduler choice.** Chunks
  enter a per-direction FIFO arrival queue with non-decreasing arrival
  times; the scheduler only decides when a connection's drain runs, never
  the byte order within a direction.
- Packet loss on a *stream* transport is modeled the way an application
  actually observes TCP loss: as retransmission delay, never as missing or
  corrupted bytes. Hard faults (resets, partitions) arrive in Phase C.
"""

from __future__ import annotations

import asyncio
import socket
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ._context import current_host
from ._errors import EscapedSimulationError

if TYPE_CHECKING:
    from ._loop import SimLoop

_EOF = object()

#: Tape labels for network decisions.
NET_DELAY = "net.delay"
NET_LOSS = "net.loss"

#: Quantization of the latency range: draws are integers in [0, 64).
_DELAY_STEPS = 64


class FakeSocket:
    """Just enough socket-shape for libraries that poke at the raw socket
    (aiohttp sets TCP_NODELAY; anyio reads addresses)."""

    family = socket.AF_INET
    type = socket.SOCK_STREAM
    proto = socket.IPPROTO_TCP

    def __init__(self, sockname: tuple[str, int], peername: tuple[str, int]) -> None:
        self._sockname = sockname
        self._peername = peername

    def setsockopt(self, *args: object) -> None:
        pass

    def getsockopt(self, *args: object) -> int:
        return 0

    def getsockname(self) -> tuple[str, int]:
        return self._sockname

    def getpeername(self) -> tuple[str, int]:
        return self._peername

    def fileno(self) -> int:
        return -1

    def gettimeout(self) -> float:
        return 0.0


class SimTransport(asyncio.Transport):
    """One direction-pair endpoint of an in-memory connection."""

    def __init__(
        self,
        network: SimNetwork,
        sockname: tuple[str, int],
        peername: tuple[str, int],
    ) -> None:
        super().__init__()
        self._network = network
        self._loop = network._loop
        self._sockname = sockname
        self._peername = peername
        self._protocol: asyncio.BaseProtocol | None = None
        self._peer: SimTransport | None = None
        self._inbox: deque[object] = deque()
        self._pending: deque[object] = deque()
        self._last_arrival = 0.0
        self._paused = False
        self._pump_scheduled = False
        self._closing = False
        self._closed = False
        #: The simulated host this endpoint belongs to (None = the root world).
        self.owner: Any = None
        self._extra: dict[str, Any] = {
            "sockname": sockname,
            "peername": peername,
            "socket": FakeSocket(sockname, peername),
            "sslcontext": None,
            "ssl_object": None,
        }

    def _start(self, protocol: asyncio.BaseProtocol, peer: SimTransport) -> None:
        self._protocol = protocol
        self._peer = peer

    # --- write side ---

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if self._closing or not data:
            return
        self._network._count_send(len(data))
        self._send(bytes(data))

    def writelines(self, list_of_data: Any) -> None:
        for data in list_of_data:
            self.write(data)

    def can_write_eof(self) -> bool:
        return True

    def write_eof(self) -> None:
        if not self._closing:
            self._send(_EOF)

    def _send(self, item: object) -> None:
        """Queue an item for the peer after a tape-drawn delay, preserving
        per-direction FIFO order (arrival times never decrease)."""
        peer = self._peer
        assert peer is not None
        arrival = self._loop.time() + self._network._draw_delay()
        arrival = max(arrival, peer._last_arrival)
        peer._last_arrival = arrival
        peer._pending.append(item)
        self._loop.call_at(arrival, peer._arrive)

    # --- read side ---

    def _arrive(self) -> None:
        if self._closed:
            return
        if self._pending:
            self._inbox.append(self._pending.popleft())
        self._schedule_pump()

    def _schedule_pump(self) -> None:
        if not self._pump_scheduled and not self._closed:
            self._pump_scheduled = True
            self._loop.call_soon(self._pump)

    def _pump(self) -> None:
        self._pump_scheduled = False
        protocol = self._protocol
        assert protocol is not None
        while self._inbox and not self._paused and not self._closed:
            item = self._inbox.popleft()
            if item is _EOF:
                eof_handler = getattr(protocol, "eof_received", None)
                keep_open = eof_handler() if eof_handler is not None else None
                if not keep_open:
                    self.close()
            else:
                assert isinstance(item, bytes)
                data_received = getattr(protocol, "data_received", None)
                if data_received is not None:
                    data_received(item)

    def pause_reading(self) -> None:
        self._paused = True

    def resume_reading(self) -> None:
        self._paused = False
        self._schedule_pump()

    def is_reading(self) -> bool:
        return not self._paused

    # --- lifecycle ---

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        if self._loop.is_closed():
            # Destructor-time close after the universe ended (e.g. a leaked
            # StreamWriter's __del__): just mark dead, schedule nothing.
            self._closed = True
            self._network._forget(self)
            return
        if self._peer is not None and not self._peer._closing:
            self._send(_EOF)
        self._loop.call_soon(self._finalize)

    def abort(self) -> None:
        self.close()

    def _finalize(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._network._forget(self)
        if self._protocol is not None:
            self._protocol.connection_lost(None)

    def _drop_on_crash(self) -> None:
        """The owning host died: this side vanishes without callbacks; the
        peer observes a reset."""
        self._closing = True
        self._closed = True
        self._network._forget(self)
        peer = self._peer
        if peer is not None and not peer._closed:
            self._loop.call_soon(peer._reset_by_peer)

    def _reset_by_peer(self) -> None:
        if self._closed:
            return
        self._closing = True
        self._closed = True
        self._network._forget(self)
        if self._protocol is not None:
            self._protocol.connection_lost(ConnectionResetError("simulated peer crashed"))

    def is_closing(self) -> bool:
        return self._closing

    # --- misc surface ---

    def get_extra_info(self, name: str, default: object = None) -> Any:
        return self._extra.get(name, default)

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._protocol = protocol

    def get_protocol(self) -> asyncio.BaseProtocol:
        assert self._protocol is not None
        return self._protocol

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        pass

    def get_write_buffer_size(self) -> int:
        return 0

    def get_write_buffer_limits(self) -> tuple[int, int]:
        return (0, 0)


class SimServer:
    """Stand-in for asyncio.Server, returned by ``loop.create_server``."""

    def __init__(
        self,
        network: SimNetwork,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        address: tuple[str, int],
    ) -> None:
        self._network = network
        self._protocol_factory = protocol_factory
        self._address = address
        self._closed = False
        #: The simulated host serving here (None = the root world).
        self.owner: Any = None
        self.sockets = [FakeSocket(address, ("0.0.0.0", 0))]

    @property
    def address(self) -> tuple[str, int]:
        return self._address

    def make_protocol(self) -> asyncio.BaseProtocol:
        return self._protocol_factory()

    def close(self) -> None:
        self._closed = True
        self._network._listeners.pop(self._address, None)

    async def wait_closed(self) -> None:
        return

    def is_serving(self) -> bool:
        return not self._closed

    def get_loop(self) -> SimLoop:
        return self._network._loop


class SimDNS:
    """Deterministic name resolution. Unknown names fail like NXDOMAIN."""

    def __init__(self) -> None:
        self._records: dict[str, str] = {}
        self._next_ip = 1

    def register(self, name: str, ip: str | None = None) -> str:
        """Bind ``name`` to ``ip`` (auto-assigned from 10.0.0.0/24 if None)."""
        if name not in self._records:
            if ip is None:
                ip = f"10.0.0.{self._next_ip}"
                self._next_ip += 1
            self._records[name] = ip
        return self._records[name]

    def resolve(self, name: str) -> str:
        if _looks_like_ip(name):
            return name
        if name in ("localhost",):
            return "127.0.0.1"
        if name not in self._records:
            raise socket.gaierror(socket.EAI_NONAME, f"simulated DNS has no record for {name!r}")
        return self._records[name]


def _looks_like_ip(name: str) -> bool:
    parts = name.split(".")
    return len(parts) == 4 and all(p.isdigit() for p in parts)


class SimNetwork:
    """The wires between simulated endpoints; owns listeners, DNS, and the
    latency/loss model. Every decision is a tape draw."""

    def __init__(self, loop: SimLoop) -> None:
        self._loop = loop
        self.dns = SimDNS()
        self._listeners: dict[tuple[str, int], SimServer] = {}
        self._live_transports: dict[int, SimTransport] = {}  # id -> transport, ordered
        self._transport_seq = 0
        self._next_client_port = 40000
        self._min_delay = 0.0005
        self._max_delay = 0.020
        self._loss_percent = 0
        self.bytes_sent = 0
        self.chunks_sent = 0
        self.chunks_delayed_by_loss = 0

    # --- configuration ---

    def set_latency(self, min_seconds: float, max_seconds: float) -> None:
        if not 0 <= min_seconds <= max_seconds:
            raise ValueError("latency range must satisfy 0 <= min <= max")
        self._min_delay = min_seconds
        self._max_delay = max_seconds

    def set_loss(self, percent: int) -> None:
        """Stream loss is observed as TCP retransmission delay, never as
        missing bytes (see docs/determinism.md)."""
        if not 0 <= percent <= 100:
            raise ValueError("loss percent must be in [0, 100]")
        self._loss_percent = percent

    # --- tape-driven decisions ---

    def _draw_delay(self) -> float:
        span = self._max_delay - self._min_delay
        step = self._loop.tape.draw(NET_DELAY, _DELAY_STEPS)
        delay = self._min_delay + span * (step / (_DELAY_STEPS - 1))
        if self._loss_percent and self._loop.tape.draw(NET_LOSS, 100) < self._loss_percent:
            # A lost segment costs roughly a retransmission round trip.
            self.chunks_delayed_by_loss += 1
            delay += 3 * self._max_delay
        return delay

    def _count_send(self, size: int) -> None:
        self.bytes_sent += size
        self.chunks_sent += 1

    # --- loop delegation (SimLoop forwards its network APIs here) ---

    async def getaddrinfo(
        self, host: str | bytes | None, port: str | int | None, **kwargs: Any
    ) -> list[tuple[int, int, int, str, tuple[str, int]]]:
        if host is None:
            raise socket.gaierror(socket.EAI_NONAME, "no host given")
        if isinstance(host, bytes):
            host = host.decode()  # anyio passes the host IDNA-encoded
        ip = self.dns.resolve(host)
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, int(port or 0)))]

    async def create_server(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        host: str | None = None,
        port: int | None = None,
        *,
        ssl: Any = None,
        **kwargs: Any,
    ) -> SimServer:
        if ssl is not None:
            raise EscapedSimulationError(
                api="loop.create_server(ssl=...)",
                hint="TLS is not simulated yet; serve plain in-sim",
            )
        if host is None or port is None:
            raise ValueError("simulated create_server requires explicit host and port")
        ip = self.dns.register(host) if not _looks_like_ip(host) else host
        address = (ip, int(port))
        if address in self._listeners:
            raise OSError(98, f"address already in use: {address}")
        server = SimServer(self, protocol_factory, address)
        server.owner = current_host.get()
        self._listeners[address] = server
        self._loop.log.emit("net_listen", t=self._loop.time(), host=host, port=int(port))
        return server

    async def create_connection(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        host: str | None = None,
        port: int | None = None,
        *,
        ssl: Any = None,
        **kwargs: Any,
    ) -> tuple[SimTransport, asyncio.BaseProtocol]:
        if ssl is not None:
            raise EscapedSimulationError(
                api="loop.create_connection(ssl=...)",
                hint="TLS is not simulated yet; connect plain in-sim",
            )
        if host is None or port is None:
            raise ValueError("simulated create_connection requires explicit host and port")
        ip = self.dns.resolve(host)
        server = self._listeners.get((ip, int(port)))
        if server is None or not server.is_serving():
            raise ConnectionRefusedError(111, f"connection refused: {(ip, int(port))}")

        client_addr = ("10.0.0.99", self._next_client_port)
        self._next_client_port += 1
        server_addr = (ip, int(port))

        client_transport = SimTransport(self, sockname=client_addr, peername=server_addr)
        server_transport = SimTransport(self, sockname=server_addr, peername=client_addr)
        client_transport.owner = current_host.get()
        server_transport.owner = server.owner
        server_protocol = server.make_protocol()
        client_protocol = protocol_factory()
        client_transport._start(client_protocol, peer=server_transport)
        server_transport._start(server_protocol, peer=client_transport)
        self._remember(client_transport)
        self._remember(server_transport)
        self._loop.log.emit("net_connect", t=self._loop.time(), host=host, port=int(port))
        server_protocol.connection_made(server_transport)
        client_protocol.connection_made(client_transport)
        return client_transport, client_protocol

    # --- transport registry (ordered, for deterministic host crashes) ---

    def _remember(self, transport: SimTransport) -> None:
        self._transport_seq += 1
        self._live_transports[self._transport_seq] = transport

    def _forget(self, transport: SimTransport) -> None:
        for key, value in list(self._live_transports.items()):
            if value is transport:
                del self._live_transports[key]
                break
