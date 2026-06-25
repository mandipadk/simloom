"""The simulated world: network, DNS, hosts, crash/restart, disk."""

from __future__ import annotations

import asyncio
import contextlib
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

    def test_tls_is_simulated_not_escaped(self) -> None:
        import ssl as ssl_module

        async def main(world: World) -> None:
            ctx = ssl_module.create_default_context()
            # No server here: refused by the simulated network, NOT an escape —
            # TLS is simulated now (see test_tls.py for the happy path).
            await asyncio.get_running_loop().create_connection(
                asyncio.Protocol, "10.0.0.7", 443, ssl=ctx
            )

        result = simloom.run(main, seed=0, raise_on_error=False)
        assert not isinstance(result.error, EscapedSimulationError)
        assert isinstance(result.error, ConnectionRefusedError)

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
                    # After restart: synced data always survives; the unsynced
                    # write is lost, torn, or flushed — never anything else.
                    assert host.disk.read("wal") == b"synced-entry"
                    if host.disk.exists("cache"):
                        assert b"never-synced".startswith(host.disk.read("cache"))

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


class TestFaults:
    @staticmethod
    async def _sink(world: World, host_name: str, dns: str) -> list[bytes]:
        inbox: list[bytes] = []

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            with contextlib.suppress(ConnectionResetError):
                while data := await reader.read(64):
                    inbox.append(data)
            writer.close()

        async def serve() -> None:
            await asyncio.start_server(handle, dns, 1)
            await asyncio.sleep(1e9)

        world.host(host_name).spawn(lambda: serve())
        await world.sleep(0.1)
        return inbox

    def test_partition_holds_then_heals_in_order(self) -> None:
        async def main(world: World) -> tuple[list[bytes], bytes]:
            inbox = await self._sink(world, "a", "a.sim")
            ha, hb = world.host("a"), world.host("b")

            async def client() -> None:
                _, writer = await asyncio.open_connection("a.sim", 1)
                writer.write(b"one")
                await world.sleep(0.5)
                writer.write(b"two")  # held behind the partition
                await world.sleep(0.5)
                writer.write(b"three")  # after heal
                await world.sleep(0.5)
                writer.close()

            hb.spawn(lambda: client())
            await world.sleep(0.3)
            world.net.partition([ha], [hb])
            await world.sleep(0.5)
            during = list(inbox)
            world.net.heal()
            await world.sleep(1.5)
            return during, b"".join(inbox)

        during, final = simloom.run(main, seed=4).value
        assert during == [b"one"]  # nothing crossed while partitioned
        assert final == b"onetwothree"  # nothing lost, order intact

    def test_asymmetric_block(self) -> None:
        """One-way block on an established connection: client->server bytes
        keep flowing, server->client replies are held."""

        async def main(world: World) -> tuple[bytes, bytes, bytes]:
            received: list[bytes] = []

            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                while data := await reader.read(64):
                    received.append(data)
                    writer.write(data.upper())
                    await writer.drain()
                writer.close()

            async def serve() -> None:
                await asyncio.start_server(handle, "a.sim", 1)
                await asyncio.sleep(1e9)

            ha, hb = world.host("a"), world.host("b")
            ha.spawn(lambda: serve())
            await world.sleep(0.1)

            echoes: list[bytes] = []

            async def client() -> None:
                reader, writer = await asyncio.open_connection("a.sim", 1)
                writer.write(b"one")
                echoes.append(await reader.read(64))
                world.net.block(ha, hb)  # server -> client now held
                writer.write(b"two")
                await world.sleep(2)  # reply cannot arrive
                writer.close()

            hb.spawn(lambda: client())
            await world.sleep(3)
            return b"".join(received), b"".join(echoes), b"|".join(echoes)

        received, echoed, _ = simloom.run(main, seed=0).value
        assert received == b"onetwo"  # b->a stayed open
        assert echoed == b"ONE"  # a->b held after the block

    def test_connect_across_partition_hangs_until_timeout(self) -> None:
        async def main(world: World) -> str:
            await self._sink(world, "a", "a.sim")
            ha, hb = world.host("a"), world.host("b")
            world.net.partition([ha], [hb])

            async def client() -> str:
                try:
                    async with asyncio.timeout(3):
                        await asyncio.open_connection("a.sim", 1)
                    return "connected"
                except TimeoutError:
                    return f"timed out at t={asyncio.get_running_loop().time()}"

            task = hb.spawn(lambda: client())
            await world.sleep(10)
            return str(task.result())

        assert "timed out" in simloom.run(main, seed=0).value

    def test_connect_succeeds_after_heal(self) -> None:
        async def main(world: World) -> bool:
            await self._sink(world, "a", "a.sim")
            ha, hb = world.host("a"), world.host("b")
            world.net.partition([ha], [hb])
            connected = asyncio.Event()

            async def client() -> None:
                _, writer = await asyncio.open_connection("a.sim", 1)  # hangs until heal
                connected.set()
                writer.close()

            hb.spawn(lambda: client())
            await world.sleep(5)
            assert not connected.is_set()
            world.net.heal()
            await world.until(connected.is_set, timeout=5)
            return True

        assert simloom.run(main, seed=1).value is True

    def test_reset_injection(self) -> None:
        async def main(world: World) -> str:
            await self._sink(world, "a", "a.sim")
            ha, hb = world.host("a"), world.host("b")
            result: list[str] = []

            async def client() -> None:
                reader, writer = await asyncio.open_connection("a.sim", 1)
                writer.write(b"hello")
                try:
                    await reader.read(64)
                    result.append("clean")
                except ConnectionResetError:
                    result.append("reset")

            hb.spawn(lambda: client())
            await world.sleep(0.5)
            world.net.reset_connections(ha, hb)
            await world.sleep(0.5)
            return result[0]

        assert simloom.run(main, seed=0).value == "reset"

    def test_torn_writes_on_crash(self) -> None:
        async def main(world: World) -> tuple[bool, bytes | None]:
            host = world.host("db")

            async def node() -> None:
                host.disk.write("durable", b"safe")
                host.disk.fsync()
                host.disk.write("risky", b"0123456789")
                await asyncio.sleep(1e9)

            host.spawn(lambda: node())
            await world.sleep(1)
            host.crash()
            risky = host.disk.read("risky") if host.disk.exists("risky") else None
            assert host.disk.read("durable") == b"safe"  # fsync always holds
            return host.disk.exists("risky"), risky

        fates = set()
        for seed in range(40):
            exists, risky = simloom.run(main, seed=seed).value
            if not exists:
                fates.add("lost")
            elif risky == b"0123456789":
                fates.add("flushed")
            else:
                assert risky is not None
                assert b"0123456789".startswith(risky)
                assert 0 < len(risky) < 10
                fates.add("torn")
        assert fates == {"lost", "torn", "flushed"}, fates

    def test_buggify_in_a_world(self) -> None:
        async def main(world: World) -> int:
            hits = 0
            for _ in range(50):
                if simloom.sometimes("flaky_path", percent=20):
                    hits += 1
                await asyncio.sleep(0.01)
            return hits

        result = simloom.run(main, seed=11)
        assert 0 < result.value < 50
        assert result.coverage["flaky_path"] == result.value
