"""Explorer and shrinker behavior (Phase D)."""

from __future__ import annotations

import pytest

import simloom
from bug_zoo import check_then_set, shallow_race


class TestExplore:
    def test_stops_on_first_failure(self) -> None:
        exploration = simloom.explore(shallow_race, runs=500)
        assert exploration.failed
        assert exploration.runs < 500  # stopped early
        assert exploration.first_failure is not None
        assert exploration.failures[0].seed == exploration.first_failure.seed
        assert "lost update" in exploration.failures[0].message
        assert "failed" in exploration.summary()

    def test_full_scan_counts_failures(self) -> None:
        exploration = simloom.explore(shallow_race, runs=60, stop_on_failure=False)
        assert exploration.runs == 60
        assert 0 < len(exploration.failures) <= 60

    def test_clean_program_reports_clean(self) -> None:
        async def fine() -> int:
            return 1

        exploration = simloom.explore(fine, runs=10)
        assert not exploration.failed
        assert exploration.first_failure is None
        assert "none failed" in exploration.summary()

    def test_first_failure_replays(self) -> None:
        exploration = simloom.explore(check_then_set, runs=300)
        first = exploration.first_failure
        assert first is not None
        replayed = simloom.replay(check_then_set, tape=first, raise_on_error=False)
        assert replayed.digest == first.digest

    def test_coverage_union(self) -> None:
        async def annotated() -> None:
            simloom.reached("checkpoint")
            if simloom.sometimes("rare", percent=30):
                simloom.reached("rare-branch")

        exploration = simloom.explore(annotated, runs=20, stop_on_failure=False)
        assert exploration.coverage["checkpoint"] == 20
        assert 0 < exploration.coverage["rare"] < 20

    def test_multiprocess_agrees_with_serial(self) -> None:
        serial = simloom.explore(shallow_race, runs=40, stop_on_failure=False)
        pooled = simloom.explore(shallow_race, runs=40, stop_on_failure=False, processes=2)
        assert {f.seed for f in pooled.failures} == {f.seed for f in serial.failures}
        assert pooled.first_failure is not None
        assert pooled.first_failure.seed == serial.first_failure.seed  # type: ignore[union-attr]


class TestShrink:
    def test_shrinks_to_few_deviations(self) -> None:
        exploration = simloom.explore(check_then_set, runs=300)
        assert exploration.first_failure is not None
        shrunk = simloom.shrink(check_then_set, exploration.first_failure)

        assert shrunk.result.outcome == "error"
        assert shrunk.deviations <= 3, shrunk.describe()
        assert shrunk.deviations < shrunk.initial_deviations
        # The artifact replays the failure exactly, strict policy.
        replayed = simloom.replay(check_then_set, tape=shrunk.result, raise_on_error=False)
        assert replayed.digest == shrunk.result.digest
        assert replayed.outcome == "error"

    def test_describe_is_human_readable(self) -> None:
        exploration = simloom.explore(check_then_set, runs=300)
        assert exploration.first_failure is not None
        shrunk = simloom.shrink(check_then_set, exploration.first_failure)
        text = shrunk.describe()
        assert "deviations" in text
        assert "FIFO" in text
        assert "reproduces: AssertionError" in text

    def test_budget_respected(self) -> None:
        exploration = simloom.explore(check_then_set, runs=300)
        assert exploration.first_failure is not None
        shrunk = simloom.shrink(check_then_set, exploration.first_failure, max_runs=25)
        assert shrunk.runs_used <= 25

    def test_rejects_passing_result(self) -> None:
        async def fine() -> int:
            return 1

        result = simloom.run(fine, seed=0)
        with pytest.raises(ValueError, match="failing"):
            simloom.shrink(fine, result)

    def test_custom_interesting_predicate(self) -> None:
        exploration = simloom.explore(shallow_race, runs=200)
        assert exploration.first_failure is not None
        # Only accept candidates reproducing the same final-count message.
        message = str(exploration.first_failure.error)
        shrunk = simloom.shrink(
            shallow_race,
            exploration.first_failure,
            interesting=lambda r: r.outcome == "error" and str(r.error) == message,
        )
        assert str(shrunk.result.error) == message


class TestSchedulerReplay:
    def test_pct_universe_replays_under_pct(self) -> None:
        result = simloom.run(shallow_race, seed=3, scheduler="pct:d=2,k=64", raise_on_error=False)
        replayed = simloom.replay(shallow_race, tape=result, raise_on_error=False)
        assert replayed.scheduler == result.scheduler == "pct:d=2,k=64"
        assert replayed.digest == result.digest
