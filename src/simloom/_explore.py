"""The explorer: run many fresh universes and collect what broke.

Random exploration is embarrassingly parallel: with ``processes > 1`` seeds
fan out over a process pool (``main`` must then be importable — a module-
level callable). Workers report which seeds failed; the parent re-runs the
first failing seed locally so the returned artifact is exactly reproducible
in the caller's process.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from ._run import RunResult, run
from ._sched import SchedulerFactory


@dataclass(frozen=True, slots=True)
class Failure:
    seed: int
    error: str  # exception type name
    message: str


@dataclass(slots=True)
class Exploration:
    """What ``explore`` found across a corpus of seeds."""

    runs: int
    failures: list[Failure]
    #: Full artifact for the lowest failing seed (replayable, shrinkable).
    first_failure: RunResult | None
    #: Union of buggify/reached counters across all runs — the data for
    #: corpus-level sometimes-assertions ("was this branch ever hit?").
    coverage: dict[str, int] = field(default_factory=dict)

    @property
    def failed(self) -> bool:
        return bool(self.failures)

    def summary(self) -> str:
        if not self.failures:
            return f"{self.runs} universes explored, none failed"
        first = self.failures[0]
        return (
            f"{self.runs} universes explored, {len(self.failures)} failed; "
            f"first: seed {first.seed} raised {first.error}: {first.message}"
        )


def explore(
    main: Callable[..., Coroutine[Any, Any, Any]],
    *,
    runs: int,
    start_seed: int = 0,
    stop_on_failure: bool = True,
    processes: int = 1,
    scheduler: str | SchedulerFactory | None = None,
    **run_kwargs: Any,
) -> Exploration:
    """Run ``main`` under ``runs`` fresh seeds and report the failures."""
    if runs < 1:
        raise ValueError("runs must be >= 1")
    seeds = range(start_seed, start_seed + runs)
    if processes > 1:
        return _explore_pool(main, seeds, stop_on_failure, processes, scheduler, run_kwargs)

    failures: list[Failure] = []
    first: RunResult | None = None
    coverage: dict[str, int] = {}
    executed = 0
    for seed in seeds:
        result = run(main, seed=seed, raise_on_error=False, scheduler=scheduler, **run_kwargs)
        executed += 1
        for label, count in result.coverage.items():
            coverage[label] = coverage.get(label, 0) + count
        if result.outcome == "error":
            assert result.error is not None
            failures.append(Failure(seed, type(result.error).__name__, str(result.error)[:200]))
            if first is None:
                first = result
            if stop_on_failure:
                break
    return Exploration(executed, failures, first, coverage)


# --- process-pool fan-out ---


def _probe(
    main: Callable[..., Coroutine[Any, Any, Any]],
    seed: int,
    scheduler: str | None,
    run_kwargs: dict[str, Any],
) -> tuple[int, str | None, str, dict[str, int]]:
    result = run(main, seed=seed, raise_on_error=False, scheduler=scheduler, **run_kwargs)
    error = type(result.error).__name__ if result.error is not None else None
    message = str(result.error)[:200] if result.error is not None else ""
    return seed, error, message, result.coverage


def _explore_pool(
    main: Callable[..., Coroutine[Any, Any, Any]],
    seeds: range,
    stop_on_failure: bool,
    processes: int,
    scheduler: str | SchedulerFactory | None,
    run_kwargs: dict[str, Any],
) -> Exploration:
    if scheduler is not None and not isinstance(scheduler, str):
        raise TypeError("processes > 1 requires a string scheduler spec (picklable)")
    failures: list[Failure] = []
    coverage: dict[str, int] = {}
    executed = 0
    with ProcessPoolExecutor(max_workers=processes) as pool:
        for seed, error, message, run_coverage in pool.map(
            _probe,
            (main for _ in seeds),
            seeds,
            (scheduler for _ in seeds),
            (run_kwargs for _ in seeds),
            chunksize=8,
        ):
            executed += 1
            for label, count in run_coverage.items():
                coverage[label] = coverage.get(label, 0) + count
            if error is not None:
                failures.append(Failure(seed, error, message))
                if stop_on_failure:
                    break
    failures.sort(key=lambda f: f.seed)
    first: RunResult | None = None
    if failures:
        # Re-run locally: the artifact must reproduce in the caller's process.
        first = run(
            main,
            seed=failures[0].seed,
            raise_on_error=False,
            scheduler=scheduler,
            **run_kwargs,
        )
    return Exploration(executed, failures, first, coverage)
