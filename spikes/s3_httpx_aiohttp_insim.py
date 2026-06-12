"""S3 — the load-bearing spike: unmodified httpx <-> aiohttp, in-sim, in-process.

Claim under test (the project thesis): because the async ecosystem bottoms out
in event-loop primitives (`create_connection`, `create_server`, `getaddrinfo`),
a deterministic loop with in-memory transports can run a *real* aiohttp server
and a *real* httpx client against each other — no sockets, no real time, no
modifications to either library — and the whole exchange is seeded + replayable.

Run:    uv run --all-extras python spikes/s3_httpx_aiohttp_insim.py
Pass:   prints "S3 PASS" and exits 0.

Builds on SpikeLoop from S1. Per-direction link latency is drawn from the same
seeded RNG that drives scheduling — a preview of the single choice tape.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import socket
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from s1_seeded_scheduler import SpikeLoop

_EOF = object()


class FakeSocket:
    """Just enough socket-shape for libraries that set options on it."""

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
    """In-memory stream transport. Writes land in the peer's inbox after the
    link latency; an inbox pump respects pause_reading/resume_reading."""

    def __init__(self, loop: NetLoop, sockname: tuple[str, int], peername: tuple[str, int]):
        super().__init__()
        self._loop = loop
        self._sockname = sockname
        self._peername = peername
        self._protocol: asyncio.Protocol | None = None
        self._peer: SimTransport | None = None
        self._latency = 0.0
        self._inbox: deque[object] = deque()
        self._pending: deque[tuple[float, object]] = deque()
        self._paused = False
        self._pump_scheduled = False
        self._closing = False
        self._closed = False
        self._extra: dict[str, Any] = {
            "sockname": sockname,
            "peername": peername,
            "socket": FakeSocket(sockname, peername),
            "sslcontext": None,
            "ssl_object": None,
        }

    # --- wiring (done by the loop at connection time) ---

    def _start(self, protocol: asyncio.Protocol, peer: SimTransport, latency: float) -> None:
        self._protocol = protocol
        self._peer = peer
        self._latency = latency

    # --- write side ---

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if self._closing or not data:
            return
        self._send(bytes(data))

    def writelines(self, list_of_data: list[bytes]) -> None:
        for data in list_of_data:
            self.write(data)

    def can_write_eof(self) -> bool:
        return True

    def write_eof(self) -> None:
        if not self._closing:
            self._send(_EOF)

    def _send(self, item: object) -> None:
        # Stream order is a *network* invariant, not a scheduler choice: if each
        # chunk were its own ready callback, the seeded scheduler could legally
        # reorder bytes within one TCP direction (it did — that's how this spike
        # found the design rule). Chunks go into a FIFO arrival queue; the
        # scheduler only decides when the drain runs.
        assert self._peer is not None
        peer = self._peer
        arrival = self._loop.time() + self._latency
        peer._pending.append((arrival, item))
        self._loop.call_at(arrival, peer._arrive)

    # --- read side ---

    def _arrive(self) -> None:
        if self._closed:
            return
        now = self._loop.time()
        while self._pending and self._pending[0][0] <= now:
            self._inbox.append(self._pending.popleft()[1])
        self._schedule_pump()

    def _schedule_pump(self) -> None:
        if not self._pump_scheduled:
            self._pump_scheduled = True
            self._loop.call_soon(self._pump)

    def _pump(self) -> None:
        self._pump_scheduled = False
        assert self._protocol is not None
        while self._inbox and not self._paused and not self._closed:
            item = self._inbox.popleft()
            if item is _EOF:
                keep_open = self._protocol.eof_received()
                if not keep_open:
                    self.close()
            else:
                assert isinstance(item, bytes)
                self._protocol.data_received(item)

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
        if self._peer is not None and not self._peer._closing:
            self._send(_EOF)
        self._loop.call_soon(self._finalize)

    def abort(self) -> None:
        self.close()

    def _finalize(self) -> None:
        if self._closed:
            return
        self._closed = True
        assert self._protocol is not None
        self._protocol.connection_lost(None)

    def is_closing(self) -> bool:
        return self._closing

    # --- misc protocol/transport surface ---

    def get_extra_info(self, name: str, default: object = None) -> Any:
        return self._extra.get(name, default)

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._protocol = protocol  # type: ignore[assignment]

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
    """Minimal stand-in for asyncio.Server, as returned by create_server."""

    def __init__(
        self, loop: NetLoop, factory: Callable[[], asyncio.Protocol], address: tuple[str, int]
    ):
        self._loop = loop
        self._factory = factory
        self._address = address
        self._closed = False
        self.sockets = [FakeSocket(address, ("0.0.0.0", 0))]

    def protocol_factory(self) -> asyncio.Protocol:
        return self._factory()

    def close(self) -> None:
        self._closed = True
        self._loop._servers.pop(self._address, None)

    async def wait_closed(self) -> None:
        return

    def is_serving(self) -> bool:
        return not self._closed

    def get_loop(self) -> NetLoop:
        return self._loop


class NetLoop(SpikeLoop):
    """SpikeLoop + simulated network: DNS, listeners, in-memory connections."""

    def __init__(self, seed: int) -> None:
        super().__init__(seed)
        self._dns: dict[str, str] = {}
        self._next_host_ip = 1
        self._next_client_port = 40000
        self._servers: dict[tuple[str, int], SimServer] = {}

    def _resolve(self, host: str) -> str:
        if host not in self._dns:
            self._dns[host] = f"10.0.0.{self._next_host_ip}"
            self._next_host_ip += 1
        return self._dns[host]

    async def getaddrinfo(
        self,
        host: str | bytes | None,
        port: str | int | None,
        *,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[tuple[int, int, int, str, tuple[str, int]]]:
        assert host is not None
        if isinstance(host, bytes):
            host = host.decode()  # anyio passes the host IDNA-encoded
        ip = host if host.startswith("10.") else self._resolve(host)
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, int(port or 0)))]

    async def create_server(  # type: ignore[override]
        self,
        protocol_factory: Callable[[], asyncio.Protocol],
        host: str | None = None,
        port: int | None = None,
        **kwargs: object,
    ) -> SimServer:
        assert host is not None
        assert port is not None
        ip = host if host.startswith("10.") else self._resolve(host)
        address = (ip, port)
        if address in self._servers:
            raise OSError(98, f"address already in use: {address}")
        server = SimServer(self, protocol_factory, address)
        self._servers[address] = server
        return server

    async def create_connection(  # type: ignore[override]
        self,
        protocol_factory: Callable[[], asyncio.Protocol],
        host: str | None = None,
        port: int | None = None,
        **kwargs: object,
    ) -> tuple[SimTransport, asyncio.Protocol]:
        assert host is not None
        assert port is not None
        ip = host if host.startswith("10.") else self._resolve(host)
        server = self._servers.get((ip, port))
        if server is None:
            raise ConnectionRefusedError(111, f"connection refused: {(ip, port)}")

        client_addr = ("10.0.0.99", self._next_client_port)
        self._next_client_port += 1
        server_addr = (ip, port)

        client_transport = SimTransport(self, sockname=client_addr, peername=server_addr)
        server_transport = SimTransport(self, sockname=server_addr, peername=client_addr)
        # One latency draw per direction, from the same seeded RNG that picks
        # schedules — every source of nondeterminism flows through one place.
        client_transport._start(
            protocol_factory(), peer=server_transport, latency=self._rng.uniform(0.001, 0.020)
        )
        server_transport._start(
            server.protocol_factory(),
            peer=client_transport,
            latency=self._rng.uniform(0.001, 0.020),
        )
        server_transport.get_protocol().connection_made(server_transport)
        client_protocol = client_transport.get_protocol()
        client_protocol.connection_made(client_transport)
        return client_transport, client_protocol


# --- the unmodified application: real aiohttp server, real httpx client ---


async def serve_and_fetch() -> dict[str, Any]:
    import httpx
    from aiohttp import web

    hits = {"count": 0}

    async def hello(request: web.Request) -> web.Response:
        hits["count"] += 1
        return web.json_response({"message": "hello from in-sim aiohttp", "hit": hits["count"]})

    async def echo(request: web.Request) -> web.Response:
        body = await request.read()
        return web.Response(body=body[::-1])

    app = web.Application()
    app.router.add_get("/hello", hello)
    app.router.add_post("/echo", echo)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="app.sim", port=8080)
    await site.start()

    loop = asyncio.get_running_loop()
    results: dict[str, Any] = {"t_start": loop.time()}
    async with httpx.AsyncClient() as client:
        r1 = await client.get("http://app.sim:8080/hello")
        r2 = await client.get("http://app.sim:8080/hello")
        r3 = await client.post("http://app.sim:8080/echo", content=b"simloom")
        results["hello1"] = (r1.status_code, r1.json())
        results["hello2"] = (r2.status_code, r2.json())
        results["echo"] = (r3.status_code, r3.content.decode())
    results["t_end"] = loop.time()

    await runner.cleanup()
    return results


def run(seed: int) -> tuple[dict[str, Any], str, int, float]:
    loop = NetLoop(seed)
    wall_start = time.perf_counter()
    results = loop.run_until_complete(serve_and_fetch())
    wall = time.perf_counter() - wall_start
    raw = "\n".join(json.dumps(e, sort_keys=True) for e in loop.events).encode()
    return results, hashlib.sha256(raw).hexdigest(), len(loop.events), wall


def main() -> None:
    results, digest, steps, wall = run(seed=7)

    assert results["hello1"] == (200, {"message": "hello from in-sim aiohttp", "hit": 1})
    assert results["hello2"] == (200, {"message": "hello from in-sim aiohttp", "hit": 2})
    assert results["echo"] == (200, "moolmis")

    # Replay: the identical universe, byte for byte.
    results2, digest2, steps2, _ = run(seed=7)
    assert (results2, digest2, steps2) == (results, digest, steps), "replay diverged"

    # A different seed still works, but takes a different path (latency draws
    # and scheduling differ => virtual timings differ).
    results3, digest3, _, _ = run(seed=8)
    assert results3["hello1"][0] == 200
    assert digest3 != digest, "different seeds produced identical event logs"

    vdur = results["t_end"] - results["t_start"]
    print("3 HTTP exchanges, real httpx <-> real aiohttp, zero sockets")
    print(f"  scheduler steps: {steps}, log sha256: {digest[:16]}")
    print(f"  virtual duration: {vdur * 1000:.1f}ms simulated, wall: {wall * 1000:.0f}ms")
    print(
        f"  seed 8 virtual duration: "
        f"{(results3['t_end'] - results3['t_start']) * 1000:.1f}ms (different universe)"
    )
    print("\nS3 PASS — unmodified httpx and aiohttp ran in-sim, seeded and replayable.")


if __name__ == "__main__":
    main()
