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
import asyncio.sslproto
import contextlib
import contextvars
import socket
import ssl as ssl_module
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ._context import current_host

if TYPE_CHECKING:
    from ._loop import SimLoop

_EOF = object()

#: Sentinel: default a pairing endpoint's owner to the current host.
_USE_CURRENT = object()

#: Tape labels for network decisions.
NET_DELAY = "net.delay"
NET_LOSS = "net.loss"

#: Tape labels for datagram (UDP) decisions — real loss/reorder/duplication,
#: unlike the stream model which only ever delays.
UDP_DELAY = "udp.delay"
UDP_LOSS = "udp.loss"
UDP_DUP = "udp.dup"
UDP_REORDER = "udp.reorder"

#: Quantization of the latency range: draws are integers in [0, 64).
_DELAY_STEPS = 64

#: The root world (code not running on any Host) in partition bookkeeping.
WORLD = "<world>"


def _owner_name(owner: Any) -> str:
    return WORLD if owner is None else str(owner.name)


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
        self._held: deque[object] = deque()  # chunks trapped behind a partition
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
        # Protocol callbacks run in the owner host's context, so tasks they
        # spawn (e.g. asyncio.start_server handler tasks) belong to the host
        # that serves the connection — and die with it on crash.
        ctx = contextvars.copy_context()
        ctx.run(current_host.set, self.owner)
        self._host_ctx = ctx

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
        if self._held or self._network._is_blocked(
            _owner_name(self.owner), _owner_name(peer.owner)
        ):
            # Behind a partition: hold, in order. Real TCP retransmits until
            # the link heals; dropping mid-stream bytes would corrupt.
            self._held.append(item)
            return
        arrival = self._loop.time() + self._network._draw_delay(
            _owner_name(self.owner), _owner_name(peer.owner)
        )
        arrival = max(arrival, peer._last_arrival)
        peer._last_arrival = arrival
        peer._pending.append(item)
        self._loop.call_at(arrival, peer._arrive, context=peer._host_ctx)

    def _flush_held(self) -> None:
        peer = self._peer
        if peer is None or self._closed:
            return
        while self._held and not self._network._is_blocked(
            _owner_name(self.owner), _owner_name(peer.owner)
        ):
            item = self._held.popleft()
            arrival = self._loop.time() + self._network._draw_delay(
                _owner_name(self.owner), _owner_name(peer.owner)
            )
            arrival = max(arrival, peer._last_arrival)
            peer._last_arrival = arrival
            peer._pending.append(item)
            self._loop.call_at(arrival, peer._arrive, context=peer._host_ctx)

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
            self._loop.call_soon(self._pump, context=self._host_ctx)

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
                self._deliver(protocol, item)

    def _deliver(self, protocol: asyncio.BaseProtocol, data: bytes) -> None:
        """Hand received bytes to the protocol via the buffered interface
        (``get_buffer``/``buffer_updated``, used by asyncio's SSLProtocol) or the
        classic ``data_received`` — whichever the protocol implements."""
        if isinstance(protocol, asyncio.BufferedProtocol):
            view = memoryview(data)
            while view and not self._closed:
                buffer = memoryview(protocol.get_buffer(len(view))).cast("B")
                count = min(len(buffer), len(view))
                if count <= 0:
                    raise RuntimeError("get_buffer() returned a zero-length buffer")
                buffer[:count] = view[:count]
                protocol.buffer_updated(count)
                view = view[count:]
        else:
            data_received = getattr(protocol, "data_received", None)
            if data_received is not None:
                data_received(data)

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
        self._loop.call_soon(self._finalize, context=self._host_ctx)

    def abort(self) -> None:
        self.close()

    def _force_close(self, exc: BaseException | None) -> None:
        """asyncio's SSLProtocol aborts the underlying transport this way."""
        if self._closed:
            return
        self._closing = True
        self._closed = True
        self._network._forget(self)
        protocol = self._protocol
        if protocol is not None and not self._loop.is_closed():
            self._loop.call_soon(
                lambda: protocol.connection_lost(exc),  # type: ignore[arg-type]
                context=self._host_ctx,
            )

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
            self._loop.call_soon(peer._reset_by_peer, context=peer._host_ctx)

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


class SimDatagramTransport(asyncio.DatagramTransport):
    """A connectionless (UDP) endpoint. Unlike :class:`SimTransport`, datagrams
    are genuinely unreliable: the network may drop, duplicate, or reorder them —
    the faults a UDP protocol must actually tolerate."""

    def __init__(
        self,
        network: SimNetwork,
        local_addr: tuple[str, int],
        protocol: asyncio.BaseProtocol,
        remote_addr: tuple[str, int] | None = None,
    ) -> None:
        super().__init__()
        self._network = network
        self._loop = network._loop
        self._local_addr = local_addr
        self._remote_addr = remote_addr
        self._protocol = protocol
        self._closing = False
        self._closed = False
        self.owner: Any = None
        self._host_ctx = contextvars.copy_context()
        self._extra: dict[str, Any] = {
            "socket": FakeSocket(local_addr, remote_addr or ("0.0.0.0", 0)),
            "sockname": local_addr,
            "peername": remote_addr,
        }

    def _start(self) -> None:
        ctx = contextvars.copy_context()
        ctx.run(current_host.set, self.owner)
        self._host_ctx = ctx

    def sendto(self, data: Any, addr: Any = None) -> None:
        if self._closing:
            return
        dst = addr if addr is not None else self._remote_addr
        if dst is None:
            raise ValueError("sendto needs a destination address (or a connected remote_addr)")
        ip = dst[0] if _looks_like_ip(dst[0]) else self._network.dns.resolve(dst[0])
        self._network._route_datagram(self, (ip, int(dst[1])), bytes(data))

    def _deliver(self, data: bytes, src_addr: tuple[str, int]) -> None:
        if self._closed:
            return
        received = getattr(self._protocol, "datagram_received", None)
        if received is not None:
            received(data, src_addr)

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        if self._loop.is_closed():
            self._closed = True
            self._network._forget_datagram(self)
            return
        self._loop.call_soon(self._finalize, context=self._host_ctx)

    def abort(self) -> None:
        self.close()

    def _finalize(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._network._forget_datagram(self)
        if self._protocol is not None:
            self._protocol.connection_lost(None)

    def _drop_on_crash(self) -> None:
        self._closing = True
        self._closed = True
        self._network._forget_datagram(self)

    def is_closing(self) -> bool:
        return self._closing

    def get_extra_info(self, name: str, default: object = None) -> Any:
        return self._extra.get(name, default)

    def get_protocol(self) -> asyncio.BaseProtocol:
        return self._protocol

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._protocol = protocol


class SimServer:
    """Stand-in for asyncio.Server, returned by ``loop.create_server``."""

    def __init__(
        self,
        network: SimNetwork,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        address: tuple[str, int],
        ssl: Any = None,
    ) -> None:
        self._network = network
        self._protocol_factory = protocol_factory
        self._address = address
        self._closed = False
        self._close_waiter: asyncio.Event | None = None
        #: The server's TLS context (None = plain). When set, connections to
        #: this listener are wrapped with asyncio's memory-BIO SSLProtocol.
        self.ssl: Any = ssl
        #: The simulated host serving here (None = the root world).
        self.owner: Any = None
        self.sockets = [FakeSocket(address, ("0.0.0.0", 0))]

    @property
    def address(self) -> tuple[str, int]:
        return self._address

    def make_protocol(self) -> asyncio.BaseProtocol:
        return self._protocol_factory()

    def _waiter(self) -> asyncio.Event:
        if self._close_waiter is None:
            self._close_waiter = asyncio.Event()
            if self._closed:
                self._close_waiter.set()
        return self._close_waiter

    def close(self) -> None:
        self._closed = True
        self._network._listeners.pop(self._address, None)
        if self._close_waiter is not None:
            self._close_waiter.set()

    async def wait_closed(self) -> None:
        await self._waiter().wait()

    def is_serving(self) -> bool:
        return not self._closed

    def get_loop(self) -> SimLoop:
        return self._network._loop

    async def serve_forever(self) -> None:
        # Matches asyncio.Server: block until the server is closed.
        await self._waiter().wait()

    async def __aenter__(self) -> SimServer:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.close()
        await self.wait_closed()


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
        #: Per-link (directional src -> dst) and per-node shaping overrides.
        #: Precedence: explicit link > egress (node out) > ingress (node in) > global.
        self._link_latency: dict[tuple[str, str], tuple[float, float]] = {}
        self._link_loss: dict[tuple[str, str], int] = {}
        self._node_out_latency: dict[str, tuple[float, float]] = {}
        self._node_out_loss: dict[str, int] = {}
        self._node_in_latency: dict[str, tuple[float, float]] = {}
        self._node_in_loss: dict[str, int] = {}
        #: Datagram endpoints by bound address, and their crash-ordered registry.
        self._datagram_endpoints: dict[tuple[str, int], SimDatagramTransport] = {}
        #: Raw sockets mid-connect (happy-eyeballs): id(sock) -> (server, addr, owner).
        self._pending_socks: dict[int, tuple[SimServer, tuple[str, int], Any]] = {}
        self._datagram_seq = 0
        #: Per-flow (src -> dst) last arrival time, for in-order delivery unless
        #: a packet is explicitly reordered.
        self._udp_arrival: dict[tuple[tuple[str, int], tuple[str, int]], float] = {}
        self._udp_loss = 0
        self._udp_dup = 0
        self._udp_reorder = 0
        self.bytes_sent = 0
        self.chunks_sent = 0
        self.chunks_delayed_by_loss = 0
        self.datagrams_dropped = 0
        self.datagrams_duplicated = 0
        self.datagrams_reordered = 0
        self._blocked: set[tuple[str, str]] = set()  # directional (src, dst)
        self._heal_waiters: asyncio.Event | None = None

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

    def set_datagram_loss(self, percent: int) -> None:
        """Fraction of datagrams the network drops outright (real UDP loss —
        no retransmission, the packet is gone)."""
        if not 0 <= percent <= 100:
            raise ValueError("loss percent must be in [0, 100]")
        self._udp_loss = percent

    def set_datagram_duplication(self, percent: int) -> None:
        """Fraction of datagrams delivered twice."""
        if not 0 <= percent <= 100:
            raise ValueError("duplication percent must be in [0, 100]")
        self._udp_dup = percent

    def set_datagram_reorder(self, percent: int) -> None:
        """Fraction of datagrams held back so a later one overtakes them."""
        if not 0 <= percent <= 100:
            raise ValueError("reorder percent must be in [0, 100]")
        self._udp_reorder = percent

    # --- per-link / per-node shaping (asymmetric, directional) ---

    def set_link_latency(self, src: Any, dst: Any, min_seconds: float, max_seconds: float) -> None:
        """Latency for the directional link ``src -> dst`` only (overrides the
        global latency for that direction). The reverse direction is untouched —
        real links are asymmetric."""
        if not 0 <= min_seconds <= max_seconds:
            raise ValueError("latency range must satisfy 0 <= min <= max")
        self._link_latency[(_owner_name(src), _owner_name(dst))] = (min_seconds, max_seconds)

    def set_link_loss(self, src: Any, dst: Any, percent: int) -> None:
        """Loss for the directional link ``src -> dst`` only."""
        if not 0 <= percent <= 100:
            raise ValueError("loss percent must be in [0, 100]")
        self._link_loss[(_owner_name(src), _owner_name(dst))] = percent

    def clog_node_out(
        self,
        node: Any,
        *,
        latency: tuple[float, float] | None = None,
        loss: int | None = None,
    ) -> None:
        """Shape everything leaving ``node`` (its egress) — slow uplink or a
        congested sender, applied to every directional link out of it."""
        name = _owner_name(node)
        if latency is not None:
            if not 0 <= latency[0] <= latency[1]:
                raise ValueError("latency range must satisfy 0 <= min <= max")
            self._node_out_latency[name] = latency
        if loss is not None:
            if not 0 <= loss <= 100:
                raise ValueError("loss percent must be in [0, 100]")
            self._node_out_loss[name] = loss

    def clog_node_in(
        self,
        node: Any,
        *,
        latency: tuple[float, float] | None = None,
        loss: int | None = None,
    ) -> None:
        """Shape everything arriving at ``node`` (its ingress) — a slow downlink
        or an overloaded receiver, applied to every directional link into it."""
        name = _owner_name(node)
        if latency is not None:
            if not 0 <= latency[0] <= latency[1]:
                raise ValueError("latency range must satisfy 0 <= min <= max")
            self._node_in_latency[name] = latency
        if loss is not None:
            if not 0 <= loss <= 100:
                raise ValueError("loss percent must be in [0, 100]")
            self._node_in_loss[name] = loss

    def _effective_latency(self, src: str, dst: str) -> tuple[float, float]:
        if (src, dst) in self._link_latency:
            return self._link_latency[(src, dst)]
        if src in self._node_out_latency:
            return self._node_out_latency[src]
        if dst in self._node_in_latency:
            return self._node_in_latency[dst]
        return (self._min_delay, self._max_delay)

    def _effective_loss(self, src: str, dst: str) -> int:
        if (src, dst) in self._link_loss:
            return self._link_loss[(src, dst)]
        if src in self._node_out_loss:
            return self._node_out_loss[src]
        if dst in self._node_in_loss:
            return self._node_in_loss[dst]
        return self._loss_percent

    # --- the fault matrix (all observable effects, never corruption) ---

    def _is_blocked(self, src: str, dst: str) -> bool:
        return (src, dst) in self._blocked

    def block(self, src: Any, dst: Any) -> None:
        """Block traffic one way: src -> dst (asymmetric partitions)."""
        edge = (_owner_name(src), _owner_name(dst))
        self._blocked.add(edge)
        self._loop.log.emit("net_block", t=self._loop.time(), src=edge[0], dst=edge[1])

    def unblock(self, src: Any, dst: Any) -> None:
        self._blocked.discard((_owner_name(src), _owner_name(dst)))
        self._release()

    def partition(self, group_a: list[Any], group_b: list[Any]) -> None:
        """Full bidirectional partition between two groups of hosts."""
        names_a = [_owner_name(h) for h in group_a]
        names_b = [_owner_name(h) for h in group_b]
        for a in names_a:
            for b in names_b:
                self._blocked.add((a, b))
                self._blocked.add((b, a))
        self._loop.log.emit("net_partition", t=self._loop.time(), a=names_a, b=names_b)

    def heal(self) -> None:
        """Remove every block; held traffic is delivered, in order."""
        self._blocked.clear()
        self._loop.log.emit("net_heal", t=self._loop.time())
        self._release()

    def _release(self) -> None:
        for transport in list(self._live_transports.values()):
            transport._flush_held()
        if self._heal_waiters is not None:
            self._heal_waiters.set()
            self._heal_waiters = None

    async def _wait_until_unblocked(self, src: str, dst: str) -> None:
        while self._is_blocked(src, dst) or self._is_blocked(dst, src):
            if self._heal_waiters is None:
                self._heal_waiters = asyncio.Event()
            await self._heal_waiters.wait()

    def reset_connections(self, a: Any, b: Any) -> None:
        """Inject ConnectionResetError on every live connection between two
        hosts (either direction)."""
        pair = {_owner_name(a), _owner_name(b)}
        self._loop.log.emit("net_reset", t=self._loop.time(), between=sorted(pair))
        for transport in list(self._live_transports.values()):
            peer = transport._peer
            ends = {_owner_name(transport.owner), _owner_name(peer.owner if peer else None)}
            if ends == pair:
                self._loop.call_soon(transport._reset_by_peer, context=transport._host_ctx)

    # --- tape-driven decisions ---

    def _draw_delay(self, src: str, dst: str) -> float:
        lo, hi = self._effective_latency(src, dst)
        span = hi - lo
        step = self._loop.tape.draw(NET_DELAY, _DELAY_STEPS)
        delay = lo + span * (step / (_DELAY_STEPS - 1))
        loss = self._effective_loss(src, dst)
        if loss and self._loop.tape.draw(NET_LOSS, 100) < loss:
            # A lost segment costs roughly a retransmission round trip.
            self.chunks_delayed_by_loss += 1
            delay += 3 * hi
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
        if host is None or port is None:
            raise ValueError("simulated create_server requires explicit host and port")
        ip = self.dns.register(host) if not _looks_like_ip(host) else host
        address = (ip, int(port))
        if address in self._listeners:
            raise OSError(98, f"address already in use: {address}")
        server = SimServer(self, protocol_factory, address, ssl=ssl)
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
        server_hostname: str | None = None,
        sock: Any = None,
        **kwargs: Any,
    ) -> tuple[asyncio.BaseTransport, asyncio.BaseProtocol]:
        # Happy-eyeballs clients (aiohttp) connect a raw socket via sock_connect,
        # then hand it here; we routed the connection at sock_connect time.
        if sock is not None:
            entry = self._pending_socks.pop(id(sock), None)
            with contextlib.suppress(OSError):
                sock.close()  # the SimTransport replaces it
            if entry is None:
                raise OSError("socket was not connected through the simulated network")
            server, server_addr, client_owner = entry
            return await self._finish_connect(
                protocol_factory,
                server,
                client_owner,
                server_addr,
                ssl,
                server_hostname or server_addr[0],
            )
        if host is None or port is None:
            raise ValueError("simulated create_connection requires explicit host and port")
        ip = self.dns.resolve(host)
        client_owner = current_host.get()
        server = await self._reach_listener(ip, int(port), client_owner)
        return await self._finish_connect(
            protocol_factory, server, client_owner, (ip, int(port)), ssl, server_hostname or host
        )

    async def _reach_listener(self, ip: str, port: int, client_owner: Any) -> SimServer:
        server = self._listeners.get((ip, port))
        if server is not None:
            # A SYN cannot cross a partition: hang until heal (a caller timeout,
            # or the deadlock oracle, turns this into a finding).
            await self._wait_until_unblocked(_owner_name(client_owner), _owner_name(server.owner))
            server = self._listeners.get((ip, port))
        if server is None or not server.is_serving():
            raise ConnectionRefusedError(111, f"connection refused: {(ip, port)}")
        return server

    async def _finish_connect(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        server: SimServer,
        client_owner: Any,
        server_addr: tuple[str, int],
        ssl: Any,
        server_hostname: str,
    ) -> tuple[asyncio.BaseTransport, asyncio.BaseProtocol]:
        client_addr = ("10.0.0.99", self._next_client_port)
        self._next_client_port += 1
        if ssl is not None or server.ssl is not None:
            return await self._tls_connect(
                protocol_factory,
                server,
                client_owner,
                client_addr,
                server_addr,
                client_ssl=ssl,
                server_hostname=server_hostname,
            )
        self._loop.log.emit(
            "net_connect", t=self._loop.time(), host=server_hostname, port=server_addr[1]
        )
        (client_transport, client_protocol), _server = self.connected_pair(
            protocol_factory,
            server.make_protocol,
            client_owner=client_owner,
            server_owner=server.owner,
            client_addr=client_addr,
            server_addr=server_addr,
        )
        return client_transport, client_protocol

    async def sock_connect(self, sock: Any, address: tuple[str, int]) -> None:
        """Route a raw socket's connect through the simulation (the happy-
        eyeballs path). The pairing is stashed and consumed by a following
        ``create_connection(sock=...)``."""
        host, port = address[0], int(address[1])
        ip = host if _looks_like_ip(host) else self.dns.resolve(host)
        client_owner = current_host.get()
        server = await self._reach_listener(ip, port, client_owner)
        self._pending_socks[id(sock)] = (server, (ip, port), client_owner)

    async def _tls_connect(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        server: SimServer,
        client_owner: Any,
        client_addr: tuple[str, int],
        server_addr: tuple[str, int],
        *,
        client_ssl: Any,
        server_hostname: str,
    ) -> tuple[asyncio.BaseTransport, asyncio.BaseProtocol]:
        """TLS over a simulated connection: wrap each app protocol in asyncio's
        own memory-BIO ``SSLProtocol`` and run the handshake over the in-memory
        transport pair. The handshake structure is deterministic, so the run
        replays byte-for-byte even though OpenSSL's RNG is not seeded."""
        if not isinstance(server.ssl, ssl_module.SSLContext):
            raise ssl_module.SSLError("server endpoint has no TLS certificate configured")
        client_ctx = (
            client_ssl
            if isinstance(client_ssl, ssl_module.SSLContext)
            else ssl_module.create_default_context()
        )
        loop = self._loop
        client_app = protocol_factory()
        server_app = server.make_protocol()
        client_waiter = loop.create_future()
        server_waiter = loop.create_future()
        client_proto = asyncio.sslproto.SSLProtocol(
            loop,
            client_app,
            client_ctx,
            client_waiter,
            server_side=False,
            server_hostname=server_hostname,
        )
        server_proto = asyncio.sslproto.SSLProtocol(
            loop,
            server_app,
            server.ssl,
            server_waiter,
            server_side=True,
        )
        loop.log.emit(
            "net_connect", t=loop.time(), host=server_hostname, port=server_addr[1], tls=True
        )
        self.connected_pair(
            lambda: client_proto,
            lambda: server_proto,
            client_owner=client_owner,
            server_owner=server.owner,
            client_addr=client_addr,
            server_addr=server_addr,
        )
        await asyncio.gather(client_waiter, server_waiter)
        return client_proto._app_transport, client_app

    def connected_pair(
        self,
        client_factory: Callable[[], asyncio.BaseProtocol],
        server_factory: Callable[[], asyncio.BaseProtocol],
        *,
        client_owner: Any = _USE_CURRENT,
        server_owner: Any = _USE_CURRENT,
        client_addr: tuple[str, int] | None = None,
        server_addr: tuple[str, int] | None = None,
    ) -> tuple[
        tuple[SimTransport, asyncio.BaseProtocol],
        tuple[SimTransport, asyncio.BaseProtocol],
    ]:
        """Pair two ``asyncio.Protocol``\\ s over a two-sided in-memory connection
        — no listener, no hand-written stub transport. Both ``connection_made``
        callbacks have run when this returns. The pair is an ordinary connection:
        latency, loss, partitions, and resets from ``world.net`` apply to it like
        any other. Returns ``((client_transport, client_protocol), (server_…))``.
        """
        owner_c = current_host.get() if client_owner is _USE_CURRENT else client_owner
        owner_s = current_host.get() if server_owner is _USE_CURRENT else server_owner
        if client_addr is None:
            client_addr = ("10.0.0.99", self._next_client_port)
            self._next_client_port += 1
        if server_addr is None:
            server_addr = ("10.0.0.1", self._next_client_port)
            self._next_client_port += 1

        client_transport = SimTransport(self, sockname=client_addr, peername=server_addr)
        server_transport = SimTransport(self, sockname=server_addr, peername=client_addr)
        client_transport.owner = owner_c
        server_transport.owner = owner_s
        client_protocol = client_factory()
        server_protocol = server_factory()
        client_transport._start(client_protocol, peer=server_transport)
        server_transport._start(server_protocol, peer=client_transport)
        self._remember(client_transport)
        self._remember(server_transport)
        server_transport._host_ctx.run(server_protocol.connection_made, server_transport)
        client_transport._host_ctx.run(client_protocol.connection_made, client_transport)
        return (client_transport, client_protocol), (server_transport, server_protocol)

    async def create_datagram_endpoint(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        *,
        local_addr: tuple[str, int] | None = None,
        remote_addr: tuple[str, int] | None = None,
        **kwargs: Any,
    ) -> tuple[SimDatagramTransport, asyncio.BaseProtocol]:
        owner = current_host.get()
        if local_addr is not None:
            host, port = local_addr
            ip = host if _looks_like_ip(host) else self.dns.register(host)
            local = (ip, int(port))
        else:
            local = ("10.0.0.99", self._next_client_port)
            self._next_client_port += 1
        if local in self._datagram_endpoints:
            raise OSError(98, f"address already in use: {local}")
        remote: tuple[str, int] | None = None
        if remote_addr is not None:
            rhost, rport = remote_addr
            rip = rhost if _looks_like_ip(rhost) else self.dns.resolve(rhost)
            remote = (rip, int(rport))

        protocol = protocol_factory()
        transport = SimDatagramTransport(self, local, protocol, remote_addr=remote)
        transport.owner = owner
        transport._start()
        self._datagram_endpoints[local] = transport
        self._datagram_seq += 1
        self._loop.log.emit("net_datagram_open", t=self._loop.time(), local=list(local))
        transport._host_ctx.run(protocol.connection_made, transport)
        return transport, protocol

    def _route_datagram(
        self, source: SimDatagramTransport, dst: tuple[str, int], data: bytes
    ) -> None:
        """Apply real UDP semantics — drop / duplicate / reorder / delay — and
        schedule delivery to the destination endpoint. Every decision is a tape
        draw, so the whole packet fate replays."""
        self._count_send(len(data))
        dest = self._datagram_endpoints.get(dst)
        if dest is None or dest._closed:
            return  # no listener bound: the packet simply vanishes (UDP)
        if self._is_blocked(_owner_name(source.owner), _owner_name(dest.owner)):
            return  # partitioned: dropped, never retransmitted
        if self._udp_loss and self._loop.tape.draw(UDP_LOSS, 100) < self._udp_loss:
            self.datagrams_dropped += 1
            return
        copies = 1
        if self._udp_dup and self._loop.tape.draw(UDP_DUP, 100) < self._udp_dup:
            copies = 2
            self.datagrams_duplicated += 1
        flow = (source._local_addr, dst)
        last = self._udp_arrival.get(flow, 0.0)
        for _ in range(copies):
            arrival = self._loop.time() + self._draw_udp_delay(
                _owner_name(source.owner), _owner_name(dest.owner)
            )
            if self._udp_reorder and self._loop.tape.draw(UDP_REORDER, 100) < self._udp_reorder:
                # Held back past the in-order front, so a later datagram in this
                # flow overtakes it — and it does not advance the flow's front.
                arrival = max(arrival, last) + 5 * self._max_delay
                self.datagrams_reordered += 1
            else:
                # In-order by default: each datagram lands at a strictly later
                # instant than the last (so same-instant scheduler picks cannot
                # reorder a flow). UDP reordering is an opt-in fault, above.
                arrival = max(arrival, last + self._min_delay)
                last = arrival
            self._loop.call_at(
                arrival, dest._deliver, data, source._local_addr, context=dest._host_ctx
            )
        self._udp_arrival[flow] = last

    def _draw_udp_delay(self, src: str, dst: str) -> float:
        lo, hi = self._effective_latency(src, dst)
        span = hi - lo
        step = self._loop.tape.draw(UDP_DELAY, _DELAY_STEPS)
        return lo + span * (step / (_DELAY_STEPS - 1))

    def _forget_datagram(self, transport: SimDatagramTransport) -> None:
        for addr, value in list(self._datagram_endpoints.items()):
            if value is transport:
                del self._datagram_endpoints[addr]
                break

    # --- transport registry (ordered, for deterministic host crashes) ---

    def _remember(self, transport: SimTransport) -> None:
        self._transport_seq += 1
        self._live_transports[self._transport_seq] = transport

    def _forget(self, transport: SimTransport) -> None:
        for key, value in list(self._live_transports.items()):
            if value is transport:
                del self._live_transports[key]
                break
