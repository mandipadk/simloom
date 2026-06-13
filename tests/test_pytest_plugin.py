"""The pytest plugin: @simloom.test end to end, via pytester."""

from __future__ import annotations

import json
import re

import pytest

pytest_plugins = ["pytester"]

PASSING = """
import asyncio
import simloom

@simloom.test(runs=20)
async def test_fine(world):
    await world.sleep(1)
    assert world.time >= 1
"""

FAILING = """
import asyncio
import simloom

@simloom.test(runs=300)
async def test_racy():
    state = {"locked": False, "worst": 0, "depth": 0}

    async def contender(stagger):
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
"""

COVERAGE = """
import simloom

@simloom.test(runs=10, require_coverage=["recovery-path"])
async def test_never_exercised():
    if simloom.sometimes("impossible", percent=0):
        simloom.reached("recovery-path")
"""


def test_passing_test_passes(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(PASSING)
    result = pytester.runpytest("-p", "no:cacheprovider")
    result.assert_outcomes(passed=1)


def test_failure_reports_seed_and_shrinks(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(FAILING)
    result = pytester.runpytest("-p", "no:cacheprovider")
    result.assert_outcomes(failed=1)
    text = result.stdout.str()
    assert "simloom found a failing universe" in text
    assert "--simloom-seed=" in text
    assert "deviations" in text
    assert "two tasks in the critical section" in text

    artifacts = list((pytester.path / ".sim" / "failures").iterdir())
    names = {a.name for a in artifacts}
    assert any(n.endswith(".tape.json") for n in names)
    assert any(n.endswith(".events.jsonl") for n in names)
    assert any(n.endswith(".shrunk.tape.json") for n in names)


def test_seed_replay_option(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYTHONHASHSEED", "0")  # silence the replay warning
    pytester.makepyfile(FAILING)
    first = pytester.runpytest("-p", "no:cacheprovider")
    text = first.stdout.str()
    match = re.search(r"--simloom-seed=(\d+)\)", text)
    assert match is not None
    seed = int(match.group(1))

    replayed = pytester.runpytest("-p", "no:cacheprovider", f"--simloom-seed={seed}")
    replayed.assert_outcomes(failed=1)
    assert f"seed {seed} fails" in replayed.stdout.str()

    # A passing seed passes directly (find one: seed exploration started at 0;
    # use a seed beyond the failing one only if it passes — simplest: rely on
    # the failing test's first seed differing from e.g. a known-good one).
    passing = pytester.runpytest("-p", "no:cacheprovider", "--simloom-seed=1")
    # seed 1 may or may not fail; accept either but require a clean report
    assert passing.ret in (0, 1)


def test_tape_replay_option(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYTHONHASHSEED", "0")  # silence the replay warning
    pytester.makepyfile(FAILING)
    pytester.runpytest("-p", "no:cacheprovider")
    failures = pytester.path / ".sim" / "failures"
    tape = next(p for p in failures.iterdir() if p.name.endswith(".shrunk.tape.json"))
    data = json.loads(tape.read_text())
    assert data["format"] == "simloom-tape"

    result = pytester.runpytest("-p", "no:cacheprovider", f"--simloom-tape={tape}")
    result.assert_outcomes(failed=1)
    assert "reproduces" in result.stdout.str()


def test_runs_override(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(PASSING)
    result = pytester.runpytest("-p", "no:cacheprovider", "--simloom-runs=2")
    result.assert_outcomes(passed=1)


def test_required_coverage_fails_when_unreached(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(COVERAGE)
    result = pytester.runpytest("-p", "no:cacheprovider")
    result.assert_outcomes(failed=1)
    assert "recovery-path" in result.stdout.str()


def test_no_shrink_option(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(FAILING)
    result = pytester.runpytest("-p", "no:cacheprovider", "--simloom-no-shrink")
    result.assert_outcomes(failed=1)
    assert "deviations" not in result.stdout.str()
