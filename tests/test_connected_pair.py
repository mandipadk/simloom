"""Phase I — connected_pair: drive any asyncio.Protocol pair over a two-sided
simulated connection, with no listener and no hand-written stub transport."""

from __future__ import annotations

import asyncio

import simloom


class Recorder(asyncio.Protocol):
    def __init__(self, name: str = "peer", echo: bool = False) -> None:
        self.name = name
        self.echo = echo
        self.transport: asyncio.Transport | None = None
        self.received: list[bytes] = []
        self.made = False
        self.lost: BaseException | None | bool = False

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, asyncio.Transport)
        self.transport = transport
        self.made = True

    def data_received(self, data: bytes) -> None:
        self.received.append(data)
        if self.echo and self.transport is not None:
            self.transport.write(b"echo:" + data)

    def connection_lost(self, exc: BaseException | None) -> None:
        self.lost = exc if exc is not None else True


class TestConnectedPair:
    def test_both_sides_connect_and_exchange(self) -> None:
        async def main(world: simloom.World) -> dict[str, list[bytes]]:
            (ct, cp), (_st, sp) = world.connected_pair(
                lambda: Recorder("client"), lambda: Recorder("server", echo=True)
            )
            assert cp.made  # both connection_made callbacks ran before return
            assert sp.made
            ct.write(b"ping")
            await asyncio.sleep(0.1)
            return {"server_got": sp.received, "client_got": cp.received}

        result = simloom.run(main, seed=0)
        assert result.value == {"server_got": [b"ping"], "client_got": [b"echo:ping"]}

    def test_is_deterministic_and_replayable(self) -> None:
        async def main(world: simloom.World) -> int:
            (ct, cp), _server = world.connected_pair(
                lambda: Recorder("c"), lambda: Recorder("s", echo=True)
            )
            for i in range(5):
                ct.write(f"m{i}".encode())
            await asyncio.sleep(0.2)
            return len(cp.received)

        a = simloom.run(main, seed=7)
        b = simloom.run(main, seed=7)
        replay = simloom.replay(main, tape=a)
        assert a.digest == b.digest == replay.digest

    def test_faults_apply_to_the_pair(self) -> None:
        async def main(world: simloom.World) -> bool:
            sp = Recorder("s", echo=True)
            (ct, _cp), _server = world.connected_pair(lambda: Recorder("c"), lambda: sp)
            world.net.set_latency(0.5, 0.5)  # half a second each way
            ct.write(b"slow")
            await asyncio.sleep(0.1)  # not enough time
            early = list(sp.received)
            await asyncio.sleep(1.0)  # now it arrives
            return early == [] and sp.received == [b"slow"]

        assert simloom.run(main, seed=0).value is True

    def test_reset_delivers_connection_lost(self) -> None:
        async def main(world: simloom.World) -> bool:
            cp = Recorder("c")
            sp = Recorder("s")
            (ct, _cp), _server = world.connected_pair(lambda: cp, lambda: sp)
            ct.close()
            await asyncio.sleep(0.1)
            return cp.lost is not False and sp.lost is not False

        assert simloom.run(main, seed=0).value is True

    def test_drives_asyncio_streams_without_a_server(self) -> None:
        # The headline use: pair real asyncio protocols (here a StreamReader-
        # backed server) and speak streams, with no create_server / listener.
        async def main(world: simloom.World) -> bytes:
            server_reader = asyncio.StreamReader()

            def server_factory() -> asyncio.Protocol:
                return asyncio.StreamReaderProtocol(server_reader)

            (ct, _cp), _server = world.connected_pair(lambda: Recorder("client"), server_factory)
            ct.write(b"hello stream\n")
            return await asyncio.wait_for(server_reader.readline(), timeout=1.0)

        assert simloom.run(main, seed=0).value == b"hello stream\n"
