"""Phase L — stand-in services: sim-redis. An unmodified ``redis.asyncio.Redis``
client speaks RESP to an in-sim server, including WATCH/MULTI/EXEC optimistic
locking, with every ``world.net`` fault applied to the wire."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import Any

import pytest

import simloom

pytest.importorskip("redis")
import redis.asyncio as aioredis
from redis.backoff import NoBackoff
from redis.exceptions import WatchError
from redis.retry import Retry


@contextlib.asynccontextmanager
async def _client(host: str = "redis", port: int = 6379) -> AsyncIterator[aioredis.Redis]:
    # single connection (no pool) + no retry: deterministic, and a wire fault
    # surfaces immediately instead of triggering a reconnect. Always closed.
    redis = aioredis.Redis(
        host=host,
        port=port,
        protocol=2,
        single_connection_client=True,
        retry=Retry(NoBackoff(), 0),
    )
    try:
        yield redis
    finally:
        with contextlib.suppress(Exception):
            await redis.aclose()


async def _serve(world: simloom.World, host: str = "redis", port: int = 6379) -> None:
    await world.run_service(simloom.SimRedis(), host=host, port=port)


class TestBasics:
    def test_set_get_del_incr(self) -> None:
        async def main(world: simloom.World) -> dict[str, Any]:
            await _serve(world)
            async with _client() as r:
                return {
                    "set": await r.set("k", "v1"),
                    "get": await r.get("k"),
                    "incr": await r.incr("n"),
                    "incr2": await r.incrby("n", 5),
                    "exists": await r.exists("k", "missing"),
                    "del": await r.delete("k"),
                    "get_after_del": await r.get("k"),
                }

        assert simloom.run(main, seed=0).value == {
            "set": True,
            "get": b"v1",
            "incr": 1,
            "incr2": 6,
            "exists": 1,
            "del": 1,
            "get_after_del": None,
        }

    def test_is_deterministic_and_replayable(self) -> None:
        async def main(world: simloom.World) -> bytes | None:
            await _serve(world)
            async with _client() as r:
                await r.set("k", "value")
                return await r.get("k")

        a = simloom.run(main, seed=3)
        b = simloom.run(main, seed=3)
        replay = simloom.replay(main, tape=a)
        assert a.digest == b.digest == replay.digest
        assert a.value == b"value"


class TestTransactions:
    def test_watch_multi_exec_commits(self) -> None:
        async def main(world: simloom.World) -> Any:
            await _serve(world)
            async with _client() as r:
                async with r.pipeline(transaction=True) as pipe:
                    await pipe.watch("counter")
                    current = await pipe.get("counter")
                    pipe.multi()
                    pipe.set("counter", str(int(current or 0) + 1))
                    result = await pipe.execute()
                return (result, await r.get("counter"))

        assert simloom.run(main, seed=0).value == ([True], b"1")

    def test_watch_aborts_when_a_watched_key_changes(self) -> None:
        async def main(world: simloom.World) -> Any:
            await _serve(world)
            async with _client() as watcher, _client() as other:
                await watcher.set("x", "1")
                aborted = False
                async with watcher.pipeline(transaction=True) as pipe:
                    await pipe.watch("x")
                    await other.set("x", "999")  # concurrent write to the watched key
                    pipe.multi()
                    pipe.set("x", "2")
                    try:
                        await pipe.execute()
                    except WatchError:
                        aborted = True
                return (aborted, await watcher.get("x"))

        # the transaction aborts, and the other client's write stands
        assert simloom.run(main, seed=0).value == (True, b"999")


class TestWireFault:
    def test_planted_reset_is_found_and_replays(self) -> None:
        async def main(world: simloom.World) -> str:
            db = world.host("db")
            app = world.host("app")

            async def serve() -> None:
                await _serve(world, host="db")
                await world.sleep(10_000)

            await db.spawn(serve())

            async def client() -> str:
                async with _client(host="db") as r:
                    await r.set("k", "v")
                    world.net.reset_connections(app, db)  # the planted wire fault
                    await r.get("k")  # raises on the reset connection
                    return "no error"

            return await app.spawn(client())

        result = simloom.run(main, seed=0, raise_on_error=False)
        assert result.outcome == "error"
        assert "ConnectionError" in type(result.error).__name__
        # the wire fault replays byte-for-byte
        replay = simloom.replay(main, tape=result, raise_on_error=False)
        assert replay.digest == result.digest
