"""Buggify: tape-drawn randomness for the code under test.

FoundationDB's trick, adapted: user code annotates rare-but-legal behaviors
(`if simloom.sometimes("drop_cache"): ...`) and the simulation explores them;
in production the same call is a constant False, so the annotations cost
nothing and never fire outside a test.

Coverage counters (``reached``, and every ``sometimes`` that fires) land in
``RunResult.coverage`` so a corpus runner can assert that fault-handling
branches were actually exercised somewhere — the "sometimes assertion" that
catches dead recovery code.
"""

from __future__ import annotations

import asyncio.events

from ._loop import SimLoop


def _sim_loop() -> SimLoop | None:
    loop = asyncio.events._get_running_loop()
    return loop if isinstance(loop, SimLoop) else None


def sometimes(label: str, percent: int = 25) -> bool:
    """True with roughly ``percent`` probability inside a simulation;
    always False outside one (safe to leave in production code)."""
    if not 0 <= percent <= 100:
        raise ValueError("percent must be in [0, 100]")
    loop = _sim_loop()
    if loop is None:
        return False
    fired = loop.tape.draw(f"buggify.{label}", 100) < percent
    if fired:
        loop.record_coverage(label)
    return fired


def draw(label: str, bound: int) -> int:
    """A labeled integer draw in ``[0, bound)`` from the run's choice tape.

    For randomness the *program under test* needs (election timeouts, victim
    picks in chaos scripts) — it replays and shrinks with everything else.
    Unlike :func:`sometimes` this has no meaningful production value, so it
    raises outside a simulation.
    """
    loop = _sim_loop()
    if loop is None:
        raise RuntimeError("simloom.draw() requires a running simulation")
    return loop.tape.draw(label, bound)


def reached(label: str) -> None:
    """Record that this point was reached (no-op outside a simulation).

    Counts appear in ``RunResult.coverage``; a corpus runner can assert a
    label was reached somewhere across many seeds.
    """
    loop = _sim_loop()
    if loop is not None:
        loop.record_coverage(label)
