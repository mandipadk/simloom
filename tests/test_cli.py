"""Phase J — the trace/diff CLI over a causal event log, and simloom.observe."""

from __future__ import annotations

import asyncio
from pathlib import Path

import simloom
from simloom._cli import (
    causal_stack,
    changed_steps,
    classify_divergence,
    first_divergence,
    load_log,
    main,
    steps_of,
)


async def counting(world: simloom.World) -> None:
    counter = {"n": 0}

    async def worker(bump: int) -> None:
        for _ in range(3):
            await asyncio.sleep(0.01)
            counter["n"] += bump
            simloom.observe("counter", counter["n"])

    await asyncio.gather(worker(1), worker(10))


def _write_causal_log(tmp_path: Path, seed: int, name: str = "trace.jsonl") -> Path:
    result = simloom.run(counting, seed=seed, causal=True)
    path = tmp_path / name
    result.log.write_to(path)
    return path


class TestObserve:
    def test_is_a_noop_without_causal(self) -> None:
        # observe must not perturb the digest in a normal run
        a = simloom.run(counting, seed=0)
        b = simloom.run(counting, seed=0)
        assert a.digest == b.digest
        assert not any(e["kind"] == "observe" for e in a.log.events)

    def test_emits_under_causal(self) -> None:
        r = simloom.run(counting, seed=0, causal=True)
        observes = [e for e in r.log.events if e["kind"] == "observe"]
        assert observes
        assert all(e["name"] == "counter" for e in observes)


class TestTrace:
    def test_load_log_roundtrips(self, tmp_path: Path) -> None:
        path = _write_causal_log(tmp_path, seed=0)
        header, events = load_log(path)
        assert header["format"] == "simloom-events"
        assert steps_of(events)

    def test_changed_query_returns_exactly_the_changes(self, tmp_path: Path) -> None:
        path = _write_causal_log(tmp_path, seed=0)
        _header, events = load_log(path)
        changes = changed_steps(events, "counter")
        values = [c["value"] for c in changes]
        # exactly the points where the value changed: no two consecutive equal
        assert values
        assert all(values[i] != values[i - 1] for i in range(1, len(values)))
        # and it is a strict subsequence of all observations (some were no-ops?)
        all_obs = [e for e in events if e.get("kind") == "observe" and e["name"] == "counter"]
        assert len(changes) <= len(all_obs)
        # every actual change is captured: reconstruct the change points by hand
        manual = []
        prev = object()
        for o in all_obs:
            if o["value"] != prev:
                manual.append(o)
                prev = o["value"]
        assert changes == manual

    def test_step_reconstruction_has_a_causal_stack(self, tmp_path: Path) -> None:
        path = _write_causal_log(tmp_path, seed=0)
        _header, events = load_log(path)
        steps = steps_of(events)
        n = len(steps) // 2
        stack = causal_stack(steps, n)
        assert stack[0] == n  # starts at the requested step
        # the chain is strictly decreasing and ends at a root
        assert all(stack[i] > stack[i + 1] for i in range(len(stack) - 1))
        assert steps[stack[-1]]["woke_by"] is None

    def test_cli_trace_step_runs(self, tmp_path: Path, capsys: object) -> None:
        path = _write_causal_log(tmp_path, seed=0)
        assert main(["trace", str(path), "--step", "1"]) == 0
        out = capsys.readouterr().out  # type: ignore[attr-defined]
        assert "state at step 1" in out
        assert "causal stack" in out

    def test_cli_trace_changed_runs(self, tmp_path: Path, capsys: object) -> None:
        path = _write_causal_log(tmp_path, seed=0)
        assert main(["trace", str(path), "--changed", "counter"]) == 0
        out = capsys.readouterr().out  # type: ignore[attr-defined]
        assert "counter=" in out

    def test_cli_trace_out_of_range(self, tmp_path: Path) -> None:
        path = _write_causal_log(tmp_path, seed=0)
        assert main(["trace", str(path), "--step", "999999"]) == 2


class TestDiff:
    def test_identical_universes_do_not_diverge(self, tmp_path: Path) -> None:
        a = _write_causal_log(tmp_path, seed=0, name="a.jsonl")
        b = _write_causal_log(tmp_path, seed=0, name="b.jsonl")
        _ha, ea = load_log(a)
        _hb, eb = load_log(b)
        assert first_divergence(ea, eb) is None

    def test_different_seeds_diverge_with_a_cause(self, tmp_path: Path, capsys: object) -> None:
        a = _write_causal_log(tmp_path, seed=0, name="a.jsonl")
        b = _write_causal_log(tmp_path, seed=1, name="b.jsonl")
        _ha, ea = load_log(a)
        _hb, eb = load_log(b)
        index = first_divergence(ea, eb)
        assert index is not None
        # the CLI prints the divergence and a classification, exits non-zero
        assert main(["diff", str(a), str(b)]) == 1
        out = capsys.readouterr().out  # type: ignore[attr-defined]
        assert f"first divergence at event #{index}" in out
        assert "cause:" in out

    def test_classify_step_divergence(self) -> None:
        a = {"kind": "step", "ran": "task-0.step", "choice": 0, "t": 1.0}
        b = {"kind": "step", "ran": "task-1.step", "choice": 1, "t": 1.0}
        assert "different callback" in classify_divergence(a, b)
