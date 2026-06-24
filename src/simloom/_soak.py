"""Soak testing: continuous, shardable, resumable exploration.

``explore`` runs a fixed batch of seeds and returns. A soak is the long-running
swarm you point at a service and leave: it walks an unbounded seed space, can be
split into provably disjoint shards across machines, and checkpoints its cursor
so a killed run resumes exactly where it left off — no seed skipped, none run
twice.

Sharding is by stride: shard ``k`` of ``S`` (starting at ``start``) covers seeds
``start + k, start + k + S, start + k + 2S, …``. Across ``k = 0 … S-1`` that is a
partition of ``[start, ∞)`` — every seed lands in exactly one shard (disjoint and
complete, by construction). Each shard advances its own cursor; the checkpoint
file records every shard's cursor, so resuming continues each shard from its
next unrun seed.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ._explore import Failure
from ._run import RunResult, run
from ._sched import SchedulerFactory


@dataclass(frozen=True, slots=True)
class SoakReport:
    """The outcome of one soak call (one shard's slice of work)."""

    shard: int
    shards: int
    start: int
    seeds: tuple[int, ...]
    failures: list[Failure]
    coverage: dict[str, int] = field(default_factory=dict)
    next_index: int = 0
    stopped_early: bool = False

    @property
    def seeds_run(self) -> int:
        return len(self.seeds)

    @property
    def failed(self) -> bool:
        return bool(self.failures)


def shard_seed(start: int, shards: int, shard: int, index: int) -> int:
    """The ``index``-th seed assigned to ``shard`` of ``shards`` from ``start``."""
    return start + shard + index * shards


def _read_cursor(path: Path, start: int, shards: int, shard: int) -> int:
    if not path.exists():
        return 0
    data = json.loads(path.read_text())
    if data.get("start") != start or data.get("shards") != shards:
        raise ValueError(
            f"checkpoint {path} is for start={data.get('start')} shards={data.get('shards')}, "
            f"not start={start} shards={shards}; use a fresh checkpoint or matching parameters"
        )
    return int(data.get("cursors", {}).get(str(shard), 0))


def _write_cursor(path: Path, start: int, shards: int, shard: int, index: int) -> None:
    data: dict[str, Any] = {"start": start, "shards": shards, "cursors": {}}
    if path.exists():
        existing = json.loads(path.read_text())
        if existing.get("start") == start and existing.get("shards") == shards:
            data = existing
            data.setdefault("cursors", {})
    data["cursors"][str(shard)] = index
    # Atomic replace so a kill mid-write cannot corrupt the checkpoint.
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".soak-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(data, handle)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def soak(
    main: Callable[..., Coroutine[Any, Any, Any]],
    *,
    count: int,
    start: int = 0,
    shards: int = 1,
    shard: int = 0,
    scheduler: str | SchedulerFactory | None = None,
    checkpoint: str | os.PathLike[str] | None = None,
    stop_on_failure: bool = False,
    on_failure: Callable[[Failure, RunResult], None] | None = None,
    **run_kwargs: Any,
) -> SoakReport:
    """Run ``count`` seeds of shard ``shard`` of ``shards`` (from ``start``).

    With a ``checkpoint`` path, the shard's cursor is persisted after every seed,
    so a killed soak resumes from its next unrun seed. ``on_failure`` is called
    with each failure as it is found (e.g. to log or upload the artifact).
    """
    if shards < 1:
        raise ValueError("shards must be >= 1")
    if not 0 <= shard < shards:
        raise ValueError(f"shard must be in [0, {shards}), got {shard}")
    if count < 0:
        raise ValueError("count must be >= 0")

    cp = Path(checkpoint) if checkpoint is not None else None
    index = _read_cursor(cp, start, shards, shard) if cp is not None else 0

    seeds: list[int] = []
    failures: list[Failure] = []
    coverage: dict[str, int] = {}
    stopped = False

    while index < count:
        seed = shard_seed(start, shards, shard, index)
        result = run(main, seed=seed, raise_on_error=False, scheduler=scheduler, **run_kwargs)
        seeds.append(seed)
        for label, hits in result.coverage.items():
            coverage[label] = coverage.get(label, 0) + hits
        if result.outcome == "error":
            assert result.error is not None
            failure = Failure(seed, type(result.error).__name__, str(result.error)[:200])
            failures.append(failure)
            if on_failure is not None:
                on_failure(failure, result)
        index += 1
        if cp is not None:
            _write_cursor(cp, start, shards, shard, index)
        if failures and stop_on_failure:
            stopped = True
            break

    return SoakReport(
        shard=shard,
        shards=shards,
        start=start,
        seeds=tuple(seeds),
        failures=failures,
        coverage=coverage,
        next_index=index,
        stopped_early=stopped,
    )
