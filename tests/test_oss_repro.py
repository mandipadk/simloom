"""Demo #4 as a regression test: the historical asyncio.wait_for race
(bpo-42130 / python/cpython#86296) reproduced under simloom; the modern
stdlib implementation survives the identical torture."""

from __future__ import annotations

import asyncio

from bpo42130 import buggy_wait_for, cancellation_invariant

import simloom


def test_historical_wait_for_swallows_cancellation() -> None:
    exploration = simloom.explore(cancellation_invariant(buggy_wait_for), runs=200)
    assert exploration.failed, "the bpo-42130 interleaving was not found"
    assert "swallowed the cancellation" in exploration.failures[0].message

    failure = exploration.first_failure
    assert failure is not None
    replayed = simloom.replay(
        cancellation_invariant(buggy_wait_for), tape=failure, raise_on_error=False
    )
    assert replayed.digest == failure.digest
    assert replayed.outcome == "error"


def test_modern_wait_for_survives_the_same_torture() -> None:
    exploration = simloom.explore(
        cancellation_invariant(asyncio.wait_for), runs=120, stop_on_failure=False
    )
    assert not exploration.failed, exploration.summary()


def test_cancel_or_complete_both_observed() -> None:
    """The torture genuinely exercises both sides of the boundary: across
    seeds, sometimes the cancel lands first, sometimes the work wins."""
    outcomes = {
        simloom.run(cancellation_invariant(asyncio.wait_for), seed=seed).value
        for seed in range(60)
    }
    assert outcomes == {"cancelled-cleanly", "completed-before-cancel"}
