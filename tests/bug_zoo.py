"""The benchmark bug zoo: small programs with known concurrency bugs.

Doubles as a regression suite (the harness must keep finding these) and as
the measurement bench for explorer strategies — find-rates per strategy are
the data behind the PCT-vs-random-walk claim. Module-level callables so the
multiprocess explorer can pickle them.
"""

from __future__ import annotations

import asyncio


async def shallow_race() -> None:
    """Lost update: read-modify-write straddles one await. Depth 1; a
    uniform random walk finds it easily."""
    state = {"value": 0}

    async def worker() -> None:
        for _ in range(3):
            current = state["value"]
            await asyncio.sleep(0)
            state["value"] = current + 1

    await asyncio.gather(*(worker() for _ in range(3)))
    assert state["value"] == 9, f"lost update: {state['value']} != 9"


async def check_then_set() -> None:
    """Mutual-exclusion violation via check/set straddling an await."""
    state = {"locked": False, "depth": 0, "worst": 0}

    async def contender(stagger: int) -> None:
        for _ in range(stagger + 1):
            await asyncio.sleep(0)
        if not state["locked"]:
            await asyncio.sleep(0)
            state["locked"] = True
            state["depth"] += 1
            state["worst"] = max(state["worst"], state["depth"])
            await asyncio.sleep(0)
            state["depth"] -= 1
            state["locked"] = False

    await asyncio.gather(*(contender(i) for i in range(4)))
    assert state["worst"] <= 1, "two tasks in the critical section"


async def deep_ordering() -> None:
    """A depth-3 ordering bug buried under decoys.

    The writer publishes in two steps (pointer, then ready-flag) with a gap;
    the failure needs the reader's two probes to *bracket* the gap — and ten
    decoy tasks keep the ready set wide, so a uniform random walk almost
    never lines up the three constraints. This is PCT's home turf.
    """
    shared = {"pointer": None, "ready": False}
    probes: list[tuple[bool, bool]] = []

    async def decoy() -> None:
        for _ in range(8):
            await asyncio.sleep(0)

    async def writer() -> None:
        for _ in range(2):
            await asyncio.sleep(0)
        shared["pointer"] = object()  # step 1 of publication
        for _ in range(4):
            await asyncio.sleep(0)  # the torn window
        shared["ready"] = True  # step 2

    async def reader() -> None:
        for _ in range(2):
            await asyncio.sleep(0)
        first = (shared["pointer"] is not None, shared["ready"])
        await asyncio.sleep(0)
        second = (shared["pointer"] is not None, shared["ready"])
        probes.append((first[0] and not first[1], second[0] and not second[1]))

    tasks = [writer(), reader()] + [decoy() for _ in range(10)]
    await asyncio.gather(*tasks)
    torn_twice = any(a and b for a, b in probes)
    assert not torn_twice, "reader observed the torn publication on both probes"


async def starvation() -> None:
    """A starvation-triggered bug: the failure needs one task to win the
    scheduler repeatedly while another makes no progress — a single
    1-in-2^14 streak under a uniform random walk, but routine under a
    priority schedule. PCT's guarantee class (and the reason it exists)."""
    progress = {"worker": 0}

    async def hog() -> None:
        for _ in range(14):
            await asyncio.sleep(0)
        # The hog finished its burst; if the worker never got a single
        # step in, the lease it was supposed to renew has expired.
        assert progress["worker"] > 0, "worker starved: lease expired during the burst"

    async def worker() -> None:
        for _ in range(20):
            await asyncio.sleep(0)
            progress["worker"] += 1

    await asyncio.gather(hog(), worker())


#: name -> program
ZOO = {
    "shallow_race": shallow_race,
    "check_then_set": check_then_set,
    "deep_ordering": deep_ordering,
    "starvation": starvation,
}
