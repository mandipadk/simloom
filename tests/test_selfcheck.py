"""Phase G — the per-test determinism self-check: run a seed twice, and if the
two universes differ, locate the first diverging event."""

from __future__ import annotations

import asyncio
import itertools
import random
import time

import pytest

import simloom
from simloom import SimloomNondeterminismError

# A process-global counter: a deterministic, guaranteed difference between two
# runs (external mutable state the tape does not control — the same class as a
# real clock or RNG, but 100% reliable for a test).
_counter = itertools.count()


async def clean() -> None:
    box = {"v": 0}

    async def w() -> None:
        for _ in range(3):
            c = box["v"]
            await asyncio.sleep(0)
            box["v"] = c + 1

    await asyncio.gather(*(w() for _ in range(3)))


async def counter_leak() -> None:
    # Each run spawns a different number of tasks (external counter) -> the two
    # runs of one seed produce different event logs.
    loop = asyncio.get_running_loop()
    n = next(_counter)
    await asyncio.gather(*(loop.create_task(asyncio.sleep(0)) for _ in range(n + 1)))


async def random_branch() -> None:
    # Deterministic WHEN the RNG is tape-seeded; the values still drive a task count.
    loop = asyncio.get_running_loop()
    n = 1 + int(random.random() * 5)
    await asyncio.gather(*(loop.create_task(asyncio.sleep(0)) for _ in range(n)))


async def clock_branch() -> None:
    loop = asyncio.get_running_loop()
    n = 1 + int(time.monotonic() * 3)
    await asyncio.gather(*(loop.create_task(asyncio.sleep(0)) for _ in range(n)))


class TestSelfCheck:
    def test_clean_program_passes(self) -> None:
        assert simloom.run(clean, seed=0, check_determinism=True).outcome == "ok"

    def test_external_state_leak_is_detected_and_located(self) -> None:
        r = simloom.run(counter_leak, seed=0, check_determinism=True, raise_on_error=False)
        assert isinstance(r.error, SimloomNondeterminismError)
        assert "First divergence at event #" in str(r.error)

    def test_raises_when_raise_on_error(self) -> None:
        with pytest.raises(SimloomNondeterminismError):
            simloom.run(counter_leak, seed=0, check_determinism=True)

    def test_returns_error_result_when_not_raising(self) -> None:
        r = simloom.run(counter_leak, seed=0, check_determinism=True, raise_on_error=False)
        assert r.outcome == "error"
        # the returned result still carries a real universe (the first run)
        assert r.tape
        assert len(r.log) > 0

    def test_seeded_randomness_passes_the_self_check(self) -> None:
        # With seed_randomness the RNG is tape-driven -> the two runs are
        # identical -> the self-check passes (this is the G1+G2 payoff).
        assert (
            simloom.run(
                random_branch, seed=0, check_determinism=True, seed_randomness=True
            ).outcome
            == "ok"
        )

    def test_virtual_clock_passes_the_self_check(self) -> None:
        assert (
            simloom.run(clock_branch, seed=0, check_determinism=True, virtual_time=True).outcome
            == "ok"
        )


class TestPluginIntegration:
    def test_simloom_test_check_determinism_catches_a_leak(
        self, pytester: pytest.Pytester
    ) -> None:
        pytester.makepyfile(
            """
            import asyncio, itertools
            import simloom

            _counter = itertools.count()

            @simloom.test(runs=10, check_determinism=True)
            async def test_leaky():
                loop = asyncio.get_running_loop()
                n = next(_counter)  # external state -> nondeterministic
                await asyncio.gather(
                    *(loop.create_task(asyncio.sleep(0)) for _ in range(n + 1))
                )
            """
        )
        result = pytester.runpytest("-p", "no:cacheprovider")
        result.assert_outcomes(failed=1)
        assert "SimloomNondeterminismError" in result.stdout.str()

    def test_cli_flag_forces_the_check(self, pytester: pytest.Pytester) -> None:
        pytester.makepyfile(
            """
            import asyncio, itertools
            import simloom

            _counter = itertools.count()

            @simloom.test(runs=10)
            async def test_maybe_leaky():
                loop = asyncio.get_running_loop()
                n = next(_counter)
                await asyncio.gather(
                    *(loop.create_task(asyncio.sleep(0)) for _ in range(n + 1))
                )
            """
        )
        # without the flag, one run per seed -> nothing to compare -> passes
        pytester.runpytest("-p", "no:cacheprovider").assert_outcomes(passed=1)
        # with the flag, each seed runs twice and is compared -> fails
        flagged = pytester.runpytest("-p", "no:cacheprovider", "--simloom-check-determinism")
        flagged.assert_outcomes(failed=1)


pytest_plugins = ["pytester"]
