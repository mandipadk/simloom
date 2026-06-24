"""Small programs that exercise the Phase F property monitors, reused by the
monitor tests and the determinism gate. Module-level so the multiprocess
explorer can pickle them."""

from __future__ import annotations

import asyncio

import simloom


async def mutex_safety() -> None:
    """A check-then-set lock with the check and set straddling an await, plus a
    ``world.always`` mutual-exclusion monitor. Some schedules let two tasks into
    the critical section → the monitor fires."""
    state = {"in_cs": 0, "locked": False}
    simloom.always("mutex", lambda: state["in_cs"] <= 1)

    async def contender(stagger: int) -> None:
        for _ in range(stagger + 1):
            await asyncio.sleep(0)
        if not state["locked"]:
            await asyncio.sleep(0)
            state["locked"] = True
            state["in_cs"] += 1
            await asyncio.sleep(0)
            state["in_cs"] -= 1
            state["locked"] = False

    await asyncio.gather(*(contender(i) for i in range(4)))


async def mutex_correct() -> None:
    """The same shape but with a real asyncio.Lock — the monitor never fires."""
    state = {"in_cs": 0}
    lock = asyncio.Lock()
    simloom.always("mutex", lambda: state["in_cs"] <= 1)

    async def contender(stagger: int) -> None:
        for _ in range(stagger + 1):
            await asyncio.sleep(0)
        async with lock:
            state["in_cs"] += 1
            await asyncio.sleep(0)
            state["in_cs"] -= 1

    await asyncio.gather(*(contender(i) for i in range(4)))


async def livelock_spin() -> None:
    """Two tasks re-arm each other at the same virtual instant forever — busy,
    never quiescent, never advancing the clock. The deadlock oracle cannot see
    it; the livelock detector must."""
    loop = asyncio.get_running_loop()

    async def ping() -> None:
        while True:  # noqa: ASYNC110 — the spin is the bug under test
            await asyncio.sleep(0)

    async def pong() -> None:
        while True:  # noqa: ASYNC110 — the spin is the bug under test
            await asyncio.sleep(0)

    spinners = [loop.create_task(ping()), loop.create_task(pong())]
    assert len(spinners) == 2  # keep strong refs; the run never lets them finish
    await asyncio.sleep(1_000_000)
