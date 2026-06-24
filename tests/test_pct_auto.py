"""Phase H — PCT auto-horizon. The default ``k=4096`` is far larger than a small
test's step count, so PCT's change points land past the run's end and never
fire — degrading it to a fixed priority schedule. ``pct:auto`` measures the
horizon from a probe run instead."""

from __future__ import annotations

import os

import simloom
from bug_zoo import ZOO, deep_ordering, shallow_race, starvation

N = int(os.environ.get("SIMLOOM_PCT_AUTO_RUNS", "200"))


class TestAutoHorizon:
    def test_resolves_to_a_concrete_horizon(self) -> None:
        r = simloom.run(starvation, seed=0, scheduler="pct:auto", raise_on_error=False)
        assert r.scheduler.startswith("pct:d=3,k=")
        # not the broken default
        assert r.scheduler != "pct:d=3,k=4096"

    def test_replays_under_the_measured_horizon(self) -> None:
        r = simloom.run(deep_ordering, seed=1, scheduler="pct:auto", raise_on_error=False)
        replayed = simloom.replay(deep_ordering, tape=r, raise_on_error=False)
        assert replayed.scheduler == r.scheduler
        assert replayed.digest == r.digest

    def test_depth_is_configurable(self) -> None:
        r = simloom.run(starvation, seed=0, scheduler="pct:auto,d=2", raise_on_error=False)
        assert r.scheduler.startswith("pct:d=2,k=")

    def test_standalone_resolve_rejects_auto(self) -> None:
        import pytest

        from simloom._sched import resolve_scheduler

        with pytest.raises(ValueError, match="pct:auto"):
            resolve_scheduler("pct:auto")


class TestGate:
    """pct:auto fixes the broken k=4096 default: it finds the bugs that need
    change points to fire, and every zoo bug is found by random-or-pct:auto."""

    def test_auto_finds_the_pct_class_bugs(self) -> None:
        # The ordering/starvation bugs PCT is built for — reliably found.
        for program in (shallow_race, deep_ordering, starvation):
            found = len(
                simloom.explore(
                    program, runs=N, stop_on_failure=False, scheduler="pct:auto"
                ).failures
            )
            assert found > 0

    def test_every_zoo_bug_is_found_by_random_or_pct_auto(self) -> None:
        for name, program in ZOO.items():
            rw = len(simloom.explore(program, runs=N, stop_on_failure=False).failures)
            auto = len(
                simloom.explore(
                    program, runs=N, stop_on_failure=False, scheduler="pct:auto"
                ).failures
            )
            assert rw + auto > 0, f"{name} found by neither in {N} seeds"

    def test_default_horizon_misses_deep_bugs(self) -> None:
        # The clearest demonstration of the broken default: deep_ordering needs
        # change points to fire, which k=4096 prevents.
        default = len(
            simloom.explore(deep_ordering, runs=N, stop_on_failure=False, scheduler="pct").failures
        )
        auto = len(
            simloom.explore(
                deep_ordering, runs=N, stop_on_failure=False, scheduler="pct:auto"
            ).failures
        )
        assert default == 0
        assert auto > 0

    def test_multiprocess_explore_with_auto(self) -> None:
        # The resolved descriptor is a concrete string -> picklable across the pool.
        pooled = simloom.explore(
            starvation, runs=40, stop_on_failure=False, scheduler="pct:auto", processes=2
        )
        serial = simloom.explore(starvation, runs=40, stop_on_failure=False, scheduler="pct:auto")
        assert {f.seed for f in pooled.failures} == {f.seed for f in serial.failures}
