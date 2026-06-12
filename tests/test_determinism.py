"""The determinism torture: the harness's central claim, enforced.

For every seed: running twice must produce byte-identical event logs, and
replaying the recorded tape must reproduce the run exactly — same digest,
same outcome, same value. The workload deliberately mixes everything Phase A
supports, including schedule-sensitive branching, so any nondeterminism in
the harness itself shows up as a digest mismatch.

Seed count defaults to 300; CI's dedicated job sets SIMLOOM_TORTURE_SEEDS=10000.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from typing import Any

import simloom

SEEDS = int(os.environ.get("SIMLOOM_TORTURE_SEEDS", "300"))


async def kitchen_sink() -> dict[str, Any]:
    """A schedule-sensitive workload touching every Phase A feature."""
    loop = asyncio.get_running_loop()
    record: dict[str, Any] = {"events": []}
    queue: asyncio.Queue[int] = asyncio.Queue(maxsize=3)
    lock = asyncio.Lock()

    async def producer(n: int) -> None:
        for i in range(4):
            await queue.put(n * 10 + i)
            if i % 2:
                await asyncio.sleep(0.001 * n)

    async def consumer() -> None:
        for _ in range(8):
            item = await queue.get()
            async with lock:
                record["events"].append(item)

    # A race whose winner depends on the schedule: both contenders sleep the
    # same virtual duration; the tape decides who wakes first.
    winner_box: list[str] = []

    async def contender(name: str) -> None:
        await asyncio.sleep(0.5)
        if not winner_box:
            winner_box.append(name)

    # Timeout racing: sometimes the work beats the timeout, sometimes not,
    # depending on scheduling around the same virtual deadline.
    async def racy_timeout() -> str:
        try:
            async with asyncio.timeout(0.5):
                await asyncio.sleep(0.5)
            return "work-won"
        except TimeoutError:
            return "timeout-won"

    # Cancellation with cleanup.
    cancelled_cleanup: list[bool] = []

    async def doomed() -> None:
        try:
            await asyncio.sleep(10_000)
        finally:
            cancelled_cleanup.append(True)

    # An async generator, an executor call, a shielded task.
    async def counter_gen() -> Any:
        for i in range(3):
            await asyncio.sleep(0.01)
            yield i

    doomed_task = loop.create_task(doomed())
    shielded = loop.create_task(asyncio.sleep(0.2, "shielded-result"))

    async with asyncio.TaskGroup() as group:
        group.create_task(producer(1))
        group.create_task(producer(2))
        group.create_task(consumer())
        group.create_task(contender("alpha"))
        group.create_task(contender("beta"))
        timeout_task = group.create_task(racy_timeout())

    record["generated"] = [value async for value in counter_gen()]
    record["executor"] = await loop.run_in_executor(None, len, "simloom")
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(asyncio.shield(shielded), timeout=0.1)
    record["shielded"] = await shielded

    doomed_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await doomed_task
    record["cleanup"] = cancelled_cleanup
    record["winner"] = winner_box[0]
    record["timeout"] = timeout_task.result()

    # Exercise exception handling on a path the schedule influences.
    try:
        if record["events"][0] >= 20:
            raise ValueError("producer 2 won the opening")
    except ValueError:
        record["opening"] = "p2"
    else:
        record["opening"] = "p1"
    return record


def test_torture_record_rerecord_replay() -> None:
    digests: set[str] = set()
    winners: set[str] = set()
    for seed in range(SEEDS):
        first = simloom.run(kitchen_sink, seed=seed, raise_on_error=False)
        again = simloom.run(kitchen_sink, seed=seed, raise_on_error=False)
        replayed = simloom.replay(kitchen_sink, tape=first, raise_on_error=False)

        assert first.outcome == "ok", f"seed {seed}: {first.error!r}"
        assert again.digest == first.digest, f"seed {seed}: re-run diverged"
        assert replayed.digest == first.digest, f"seed {seed}: replay diverged"
        assert again.value == first.value, f"seed {seed}: re-run value diverged"
        assert replayed.value == first.value, f"seed {seed}: replay value diverged"
        assert replayed.outcome == first.outcome

        digests.add(first.digest)
        winners.add(first.value["winner"])

    # Exploration sanity: seeds genuinely explore different universes.
    assert len(digests) > SEEDS * 0.5, "seeds are not exploring distinct schedules"
    assert winners == {"alpha", "beta"}, "the schedule never flipped the race"


def test_torture_value_is_schedule_dependent() -> None:
    """The workload must actually be schedule-sensitive, or the torture
    above proves less than it claims."""
    openings = {simloom.run(kitchen_sink, seed=seed).value["opening"] for seed in range(60)}
    assert openings == {"p1", "p2"}


async def world_kitchen_sink(world: Any) -> dict[str, Any]:
    """A world workload: lossy network traffic plus a crash and restart."""
    received: list[bytes] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while data := await reader.read(64):
            received.append(data)
            writer.write(data[::-1])
            await writer.drain()
        writer.close()

    async def serve() -> None:
        host = world.host("srv")
        host.disk.write("boot", f"gen-{host.generation}".encode())
        host.disk.fsync("boot")
        await asyncio.start_server(handle, "srv.sim", 7)
        await asyncio.sleep(1_000_000)

    world.net.set_loss(25)
    server = world.host("srv")
    server.spawn(lambda: serve())
    await world.sleep(0.1)

    reader, writer = await asyncio.open_connection("srv.sim", 7)
    writer.write(b"alpha")
    reply = await reader.read(64)

    server.crash()
    try:
        await reader.read(64)
        outcome = "clean"
    except ConnectionResetError:
        outcome = "reset"
    server.restart()
    await world.sleep(0.2)

    reader2, writer2 = await asyncio.open_connection("srv.sim", 7)
    writer2.write(b"beta")
    reply2 = await reader2.read(64)
    writer2.close()
    return {
        "reply": reply,
        "outcome": outcome,
        "reply2": reply2,
        "boot": server.disk.read("boot"),
        "generation": server.generation,
    }


def test_world_torture_record_rerecord_replay() -> None:
    """The Phase B claim at scale: network + crash + restart, hash-identical
    across re-runs and replays. Heavier per seed than the Phase A torture,
    so it runs a tenth of the seed budget."""
    for seed in range(max(SEEDS // 10, 30)):
        first = simloom.run(world_kitchen_sink, seed=seed, raise_on_error=False)
        again = simloom.run(world_kitchen_sink, seed=seed, raise_on_error=False)
        replayed = simloom.replay(world_kitchen_sink, tape=first, raise_on_error=False)
        assert first.outcome == "ok", f"seed {seed}: {first.error!r}"
        assert first.value["reply"] == b"ahpla"
        assert first.value["reply2"] == b"ateb"
        assert first.value["boot"] == b"gen-1"
        assert again.digest == first.digest, f"seed {seed}: re-run diverged"
        assert replayed.digest == first.digest, f"seed {seed}: replay diverged"
        assert replayed.value == first.value
