"""The simulated world: network, DNS, hosts, crash/restart, disk."""

from __future__ import annotations

import asyncio
import socket

import pytest

import simloom
from simloom import EscapedSimulationError, SimDisk, World


async def _echo_server(world: World, name: str = "echo.sim", port: int = 9000) -> None:
    """Spawn a stream echo server on its own host and wait for it to listen."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while data := await reader.read(1024):
            writer.write(data[::-1])
            await writer.drain()
        writer.close()

    async def serve() -> None:
        await asyncio.start_server(handle, name, port)
        await asyncio.sleep(1_000_000)

    world.host("server").spawn(lambda: serve())
    await world.sleep(0.1)


class TestNetwork:
    def test_streams_echo(self) -> None:
        async def main(world: World) -> bytes:
            await _echo_server(world)
            reader, writer = await asyncio.open_connection("echo.sim", 9000)
            writer.write(b"simloom")
            await writer.drain()
            writer.write_eof()
            reply = await reader.read(1024)
            writer.close()
            return reply

        assert simloom.run(main, seed=1).value == b"moolmis"

    def test_chunk_order_preserved(self) -> None:
        """Many small writes arrive in order despite tape-driven delays."""

        async def main(world: World) -> bytes:
            received = bytearray()
            done = asyncio.Event()

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                while data := await reader.read(1024):
                    received.extend(data)
                writer.close()
                done.set()

            async def serve() -> None:
                await asyncio.start_server(handle, "sink.sim", 1)
                await asyncio.sleep(1_000_000)

            world.host("sink").spawn(lambda: serve())
            await world.sleep(0.1)

            _, writer = await asyncio.open_connection("sink.sim", 1)
            for i in range(50):
                writer.write(bytes([i]))
            writer.write_eof()
            await done.wait()
            writer.close()
            return bytes(received)

        assert simloom.run(main, seed=9).value == bytes(range(50))

    def test_unknown_host_is_nxdomain(self) -> None:
        async def main(world: World) -> None:
            await asyncio.open_connection("nowhere.sim", 80)

        with pytest.raises(socket.gaierror, match=r"nowhere\.sim"):
            simloom.run(main, seed=0)

    def test_connection_refused_when_nothing_listens(self) -> None:
        async def main(world: World) -> None:
            world.net.dns.register("ghost.sim")
            await asyncio.open_connection("ghost.sim", 80)

        with pytest.raises(ConnectionRefusedError):
            simloom.run(main, seed=0)

    def test_tls_escapes(self) -> None:
        import ssl as ssl_module

        async def main(world: World) -> None:
            ctx = ssl_module.create_default_context()
            await asyncio.get_running_loop().create_connection(
                asyncio.Protocol, "tls.sim", 443, ssl=ctx
            )

        with pytest.raises(EscapedSimulationError, match="ssl"):
            simloom.run(main, seed=0)

    def test_latency_shapes_virtual_time(self) -> None:
        async def main(world: World) -> float:
            world.net.set_latency(0.5, 0.5)  # constant half-second links
            await _echo_server(world)
            start = world.time
            reader, writer = await asyncio.open_connection("echo.sim", 9000)
            writer.write(b"x")
            await reader.read(1024)
            writer.close()
            return world.time - start

        elapsed = simloom.run(main, seed=2).value
        assert elapsed >= 1.0  # at least one round trip at 0.5s each way

    def test_loss_delays_but_never_corrupts(self) -> None:
        async def main(world: World) -> tuple[bytes, int]:
            world.net.set_loss(40)
            await _echo_server(world)
            reader, writer = await asyncio.open_connection("echo.sim", 9000)
            writer.write(b"simloom")
            await writer.drain()
            writer.write_eof()
            reply = await reader.read(1024)
            writer.close()
            return reply, world.net.chunks_delayed_by_loss

        result = simloom.run(main, seed=7, raise_on_error=False)
        assert result.outcome == "ok"
        reply, _delayed = result.value
        assert reply == b"moolmis"  # loss may never corrupt a stream

    def test_network_determinism(self) -> None:
        async def main(world: World) -> bytes:
            world.net.set_loss(30)
            await _echo_server(world)
            reader, writer = await asyncio.open_connection("echo.sim", 9000)
            writer.write(b"determinism")
            await writer.drain()
            writer.write_eof()
            reply = await reader.read(1024)
            writer.close()
            return reply

        first = simloom.run(main, seed=21)
        again = simloom.run(main, seed=21)
        replayed = simloom.replay(main, tape=first)
        assert first.digest == again.digest == replayed.digest


class TestWorldPlumbing:
    def test_world_passed_by_arity(self) -> None:
        async def with_world(world: World) -> bool:
            return isinstance(world, World)

        async def without_world() -> str:
            return "plain"

        assert simloom.run(with_world, seed=0).value is True
        assert simloom.run(without_world, seed=0).value == "plain"

    def test_too_many_parameters_rejected(self) -> None:
        async def bad(world: World, extra: int) -> None:
            pass

        with pytest.raises(TypeError, match="at most one"):
            simloom.run(bad, seed=0)

    def test_until_succeeds_and_times_out(self) -> None:
        async def main(world: World) -> float:
            box: list[int] = []

            async def later() -> None:
                await world.sleep(5)
                box.append(1)

            world.host("h").spawn(lambda: later())
            await world.until(lambda: bool(box), timeout=60)
            reached = world.time
            with pytest.raises(TimeoutError):
                await world.until(lambda: False, timeout=1)
            return reached

        assert simloom.run(main, seed=0).value == pytest.approx(5.0, abs=0.1)


class TestHostCrash:
    def test_crash_runs_no_cleanup_during_sim(self) -> None:
        observed: dict[str, int] = {}

        async def main(world: World) -> None:
            cleanups: list[str] = []

            async def node() -> None:
                try:
                    await asyncio.sleep(1_000_000)
                finally:
                    cleanups.append("ran")

            host = world.host("n1")
            host.spawn(lambda: node())
            await world.sleep(1)
            host.crash()
            await world.sleep(100)  # plenty of sim time for cleanup to leak
            observed["during_sim"] = len(cleanups)

        simloom.run(main, seed=0)
        # No finally during the simulation; teardown ran it after the
        # universe ended.
        assert observed["during_sim"] == 0

    def test_crash_resets_peer_connections(self) -> None:
        async def main(world: World) -> str:
            await _echo_server(world)
            reader, writer = await asyncio.open_connection("echo.sim", 9000)
            writer.write(b"hello")
            await reader.read(1024)

            world.host("server").crash()
            try:
                data = await reader.read(1024)
            except ConnectionResetError:
                return "reset"
            return "eof" if data == b"" else f"data: {data!r}"

        assert simloom.run(main, seed=3).value == "reset"

    def test_crash_closes_listeners(self) -> None:
        async def main(world: World) -> str:
            await _echo_server(world)
            world.host("server").crash()
            try:
                await asyncio.open_connection("echo.sim", 9000)
            except ConnectionRefusedError:
                return "refused"
            return "connected"

        assert simloom.run(main, seed=0).value == "refused"

    def test_restart_requires_factories(self) -> None:
        async def main(world: World) -> None:
            async def node() -> None:
                await asyncio.sleep(10)

            host = world.host("n1")
            host.spawn(node())  # raw coroutine: not restartable
            await world.sleep(1)
            host.crash()
            host.restart()

        with pytest.raises(RuntimeError, match="restartable"):
            simloom.run(main, seed=0)

    def test_crash_restart_with_disk(self) -> None:
        async def main(world: World) -> tuple[list[str], bytes]:
            host = world.host("db")
            generations: list[str] = []

            async def node() -> None:
                generation = host.generation
                generations.append(f"gen{generation}-up")
                if generation == 0:
                    host.disk.write("wal", b"synced-entry")
                    host.disk.fsync("wal")
                    host.disk.write("cache", b"never-synced")
                    await asyncio.sleep(1_000_000)
                else:
                    # After restart: synced data survived, unsynced is gone.
                    assert host.disk.read("wal") == b"synced-entry"
                    assert not host.disk.exists("cache")

            host.spawn(lambda: node())
            await world.sleep(1)
            host.crash()
            host.restart()
            await world.sleep(1)
            return generations, host.disk.read("wal")

        generations, wal = simloom.run(main, seed=0).value
        assert generations == ["gen0-up", "gen1-up"]
        assert wal == b"synced-entry"

    def test_crash_determinism(self) -> None:
        async def main(world: World) -> None:
            async def chatter(n: int) -> None:
                for _ in range(n):
                    await asyncio.sleep(0.01)

            host = world.host("h")
            for i in range(3):
                host.spawn(lambda i=i: chatter(10 + i))
            await world.sleep(0.05)
            host.crash()
            await world.sleep(0.05)

        first = simloom.run(main, seed=13)
        replayed = simloom.replay(main, tape=first)
        assert first.digest == replayed.digest


class TestSimDisk:
    def test_buffered_reads_and_fsync(self) -> None:
        disk = SimDisk()
        disk.write("a", b"1")
        assert disk.read("a") == b"1"  # page-cache view
        disk.drop_unsynced()
        assert not disk.exists("a")  # was never durable

        disk.write("a", b"2")
        disk.fsync("a")
        disk.drop_unsynced()
        assert disk.read("a") == b"2"

    def test_delete_needs_fsync_to_be_durable(self) -> None:
        disk = SimDisk()
        disk.write("a", b"1")
        disk.fsync()
        disk.delete("a")
        assert not disk.exists("a")
        disk.drop_unsynced()  # crash before the delete was synced
        assert disk.read("a") == b"1"

        disk.delete("a")
        disk.fsync()
        assert not disk.exists("a")
        with pytest.raises(FileNotFoundError):
            disk.read("a")

    def test_missing_file_errors(self) -> None:
        disk = SimDisk()
        with pytest.raises(FileNotFoundError):
            disk.read("ghost")
        with pytest.raises(FileNotFoundError):
            disk.delete("ghost")

    def test_files_listing(self) -> None:
        disk = SimDisk()
        disk.write("b", b"")
        disk.write("a", b"")
        disk.fsync()
        disk.write("c", b"")
        disk.delete("a")
        assert disk.files() == ["b", "c"]
