"""Phase: systematic deep search — a stateless model checker over the choice
tape with delay bounding.

Two capabilities random walk can never offer: it finds a bug that needs a
specific non-default interleaving *deterministically* (every run), and it
*proves* a correct program has no failing interleaving within a delay bound."""

from __future__ import annotations

import asyncio

import pytest

import simloom
from bug_zoo import check_then_set


async def correct_mutex() -> None:
    """A genuinely correct critical section (real asyncio.Lock). No interleaving
    should ever see two tasks inside it."""
    lock = asyncio.Lock()
    state = {"inside": 0, "violated": False}

    async def worker() -> None:
        async with lock:
            state["inside"] += 1
            if state["inside"] > 1:
                state["violated"] = True
            await asyncio.sleep(0)
            state["inside"] -= 1

    await asyncio.gather(*(worker() for _ in range(3)))
    assert not state["violated"], "two tasks in the critical section"


class TestFindsBugs:
    def test_finds_a_bug_needing_a_non_default_interleaving(self) -> None:
        # check_then_set is SAFE under the default (front-of-queue) schedule;
        # the violation needs a delay. Systematic finds it every time.
        result = simloom.explore_systematic(check_then_set, max_delays=2)
        assert result.failed
        assert result.first_failure_delays is not None
        assert result.first_failure_delays >= 1  # not the default schedule

    def test_finding_is_deterministic(self) -> None:
        a = simloom.explore_systematic(check_then_set, max_delays=2)
        b = simloom.explore_systematic(check_then_set, max_delays=2)
        assert a.first_failure_at == b.first_failure_at
        assert a.schedules == b.schedules

    def test_found_failure_replays_and_shrinks(self) -> None:
        result = simloom.explore_systematic(check_then_set, max_delays=2)
        assert result.first is not None
        replayed = simloom.replay(check_then_set, tape=result.first, raise_on_error=False)
        assert replayed.outcome == "error"
        assert replayed.digest == result.first.digest
        shrunk = simloom.shrink(check_then_set, result.first)
        assert len(shrunk.tape) <= len(result.first.tape)


class TestProvesCorrectness:
    def test_proves_a_correct_program_has_no_bug(self) -> None:
        result = simloom.explore_systematic(correct_mutex, max_delays=3, stop_on_failure=False)
        assert result.exhaustive
        assert result.proven_correct
        assert not result.failures
        assert result.schedules > 1  # it really explored the space

    def test_a_buggy_program_is_not_proven_correct(self) -> None:
        result = simloom.explore_systematic(check_then_set, max_delays=2, stop_on_failure=False)
        assert result.failed
        assert not result.proven_correct

    def test_higher_delay_bound_explores_more(self) -> None:
        counts = [
            simloom.explore_systematic(
                correct_mutex, max_delays=d, stop_on_failure=False
            ).schedules
            for d in range(4)
        ]
        assert counts == sorted(counts)
        assert counts[0] < counts[-1]  # the space genuinely grows with the bound


class TestBasics:
    def test_validation(self) -> None:
        with pytest.raises(ValueError, match="max_delays"):
            simloom.explore_systematic(correct_mutex, max_delays=-1)
        with pytest.raises(ValueError, match="max_schedules"):
            simloom.explore_systematic(correct_mutex, max_schedules=0)

    def test_budget_truncates_without_claiming_exhaustive(self) -> None:
        # A tiny budget cannot exhaust, so it must not claim a proof.
        result = simloom.explore_systematic(
            correct_mutex, max_delays=3, max_schedules=5, stop_on_failure=False
        )
        assert result.schedules == 5
        assert not result.exhaustive
        assert not result.proven_correct  # never claim correctness on a truncated search
