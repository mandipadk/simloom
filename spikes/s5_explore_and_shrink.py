"""S5 — exploration + shrinking spike: the failure-artifact story, end to end.

Claim under test: random exploration over seeds finds a real (planted) mutual-
exclusion race in unmodified asyncio code; the failing run's choice tape can be
shrunk — deletion and zeroing, re-running after each candidate edit — to a
minimal schedule that still violates the invariant; and the shrunk tape replays
the failure deterministically with the PRNG forbidden. Find -> shrink -> seed.

Run:    uv run python spikes/s5_explore_and_shrink.py
Pass:   prints "S5 PASS" and exits 0.

Reuses Tape/TapeLoop from S4.
"""

from __future__ import annotations

import asyncio
import contextlib
import random

from s4_tape_replay import Tape, TapeLoop, _ForbiddenRandom


async def lock_protocol() -> None:
    """Four contenders around a naive check-then-set 'lock'.

    The bug: the check (`if not locked`) and the set (`locked = True`)
    straddle an await, so two contenders can both pass the check before
    either sets the flag. Most polite schedules never hit it.
    """
    state = {"locked": False, "in_critical": 0, "max_in_critical": 0}

    async def contender(idx: int) -> None:
        for _ in range(idx + 1):  # staggered arrivals widen the schedule space
            await asyncio.sleep(0)
        if not state["locked"]:  # check ...
            await asyncio.sleep(0)  # ... the racy gap ...
            state["locked"] = True  # ... set
            state["in_critical"] += 1
            state["max_in_critical"] = max(state["max_in_critical"], state["in_critical"])
            await asyncio.sleep(0)  # critical-section work
            state["in_critical"] -= 1
            state["locked"] = False

    loop = asyncio.get_running_loop()
    tasks = [loop.create_task(contender(i)) for i in range(4)]
    for t in tasks:
        await t
    assert state["max_in_critical"] <= 1, (
        f"mutual exclusion violated: {state['max_in_critical']} tasks in the critical section"
    )


def fails(draws: list[tuple[str, int]]) -> bool:
    """Replay a candidate tape; misaligned tails fall back to a fixed RNG, so
    every candidate is a well-defined, deterministic universe."""
    tape = Tape(random.Random(0), recorded=list(draws))
    loop = TapeLoop(tape)
    try:
        loop.run_until_complete(lock_protocol())
    except AssertionError:
        return True
    except RuntimeError:
        return False  # e.g. deadlock under a mangled tape: not our failure
    return False


def rerecord(draws: list[tuple[str, int]]) -> list[tuple[str, int]]:
    """Run a candidate and return the draws it actually consumed, so the final
    artifact is exact and self-contained (no fallback RNG needed to replay)."""
    tape = Tape(random.Random(0), recorded=list(draws))
    loop = TapeLoop(tape)
    with contextlib.suppress(AssertionError):
        loop.run_until_complete(lock_protocol())
    return list(tape.draws)


def shrink(draws: list[tuple[str, int]]) -> tuple[list[tuple[str, int]], int]:
    """Greedy shrink to a shortlex-style local minimum.

    Deletion rarely helps here — the tape's length is intrinsic to the program
    (it always needs ~N scheduler picks to finish) — but we try it anyway.
    The metric that matters for a *schedule* is distance from the canonical
    FIFO order: pick=0 means "run the oldest ready callback", so every nonzero
    draw is one deliberate scheduling perversion. Minimize each value.
    """
    attempts = 0
    current = list(draws)

    def try_accept(candidate: list[tuple[str, int]]) -> bool:
        """Accept iff the candidate still fails AND its exact re-recorded form
        (RNG-refilled tails become explicit draws) improves the shortlex key."""
        nonlocal current
        if not candidate or not fails(candidate):
            return False
        exact = rerecord(candidate)
        if shortlex(exact) < shortlex(current):
            current = exact
            return True
        return False

    improved = True
    while improved:
        improved = False
        chunk = max(1, len(current) // 2)
        while chunk >= 1:
            index = 0
            while index < len(current):
                attempts += 1
                if try_accept(current[:index] + current[index + chunk :]):
                    improved = True
                else:
                    index += chunk
            chunk //= 2
        for index in range(len(current)):
            label, value = current[index]
            for smaller in range(value):  # ascending: take the smallest that fails
                candidate = list(current)
                candidate[index] = (label, smaller)
                attempts += 1
                if try_accept(candidate):
                    improved = True
                    break
    return current, attempts


def shortlex(draws: list[tuple[str, int]]) -> tuple[int, tuple[int, ...]]:
    """Hypothesis's ordering: shorter first, then lexicographically smaller."""
    return (len(draws), tuple(value for _, value in draws))


def perversions(draws: list[tuple[str, int]]) -> int:
    """Nonzero scheduler picks = deviations from the canonical FIFO schedule."""
    return sum(1 for _, value in draws if value != 0)


def main() -> None:
    # 1. Explore: fresh seeded universes until the invariant breaks.
    failing_seed = None
    failing_draws: list[tuple[str, int]] | None = None
    passed = 0
    for seed in range(10_000):
        tape = Tape(random.Random(seed))
        loop = TapeLoop(tape)
        try:
            loop.run_until_complete(lock_protocol())
            passed += 1
        except AssertionError:
            failing_seed = seed
            failing_draws = list(tape.draws)
            break
    assert failing_draws is not None, "no failing seed in 10k runs — race too rare?"

    # 2. Shrink: minimize the tape while the failure keeps reproducing.
    artifact, attempts = shrink(failing_draws)
    assert fails(artifact), "shrunk artifact must still fail"
    assert shortlex(artifact) <= shortlex(failing_draws), "shrink made it worse"

    # 3. The artifact replays the failure with randomness forbidden, twice.
    for _ in range(2):
        tape = Tape(_ForbiddenRandom(), recorded=list(artifact))
        loop = TapeLoop(tape)
        try:
            loop.run_until_complete(lock_protocol())
            raise SystemExit("artifact replay did not fail")
        except AssertionError as exc:
            failure_message = str(exc)

    print(
        f"explored: seed {failing_seed} violates mutual exclusion "
        f"({passed} polite universes before it)"
    )
    print(
        f"shrunk:   {len(failing_draws)} draws -> {len(artifact)}, "
        f"FIFO deviations {perversions(failing_draws)} -> {perversions(artifact)} "
        f"({attempts} shrink attempts)"
    )
    print(f"replayed: deterministic failure, PRNG forbidden: {failure_message!r}")
    deviations = [(i, v) for i, (_, v) in enumerate(artifact) if v != 0]
    print(
        f"\nminimal schedule: FIFO everywhere except picks "
        f"{', '.join(f'#{i}={v}' for i, v in deviations)}"
    )
    print("\nS5 PASS — explore finds the race, shrink minimizes it, the seed replays it.")


if __name__ == "__main__":
    main()
