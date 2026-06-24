"""The ``@simloom.test`` decorator: DST as an ordinary pytest test.

The decorated coroutine function becomes a plain synchronous test that
explores ``runs`` fresh universes; the first failure is shrunk, written to
disk as a replayable artifact, and reported with the seed and the minimal
schedule. The decorator itself never imports pytest — the plugin half
(``simloom._pytest_plugin``) supplies the ``simloom_settings`` fixture.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ._explore import explore
from ._run import RunResult, replay, run
from ._shrink import ShrinkResult, shrink
from ._tape import Tape, serialize_draws


class SimloomTestFailure(AssertionError):
    """A failing universe, reported with its seed and minimal schedule."""


@dataclass(slots=True)
class Settings:
    """Per-session knobs, normally provided by the pytest plugin fixture."""

    seed_override: int | None = None
    runs_override: int | None = None
    tape_path: str | None = None
    artifact_dir: Path | None = None
    shrink_enabled: bool = True
    extra: dict[str, Any] = field(default_factory=dict)


def test(
    runs: int = 100,
    *,
    scheduler: str = "random",
    start_seed: int = 0,
    shrink_budget: int = 1500,
    require_coverage: Sequence[str] = (),
    virtual_time: bool = True,
    seed_randomness: bool = True,
    **run_kwargs: Any,
) -> Callable[[Callable[..., Coroutine[Any, Any, Any]]], Callable[..., None]]:
    """Turn an async test into a seed-exploring simulation test.

    ``require_coverage`` lists buggify/reached labels that must be hit
    somewhere in the explored corpus — the "sometimes assertion" that
    catches fault-handling code the torture never actually exercised.

    ``virtual_time`` and ``seed_randomness`` default to True here (unlike the
    lower-level ``run``/``replay``): a test wants full determinism, so the wall
    clock (``time.time``/``monotonic``) and the stdlib RNG (``random``,
    ``os.urandom``, ``uuid4``) are tape-driven by default. Pass False to opt out.
    """
    run_kwargs.setdefault("virtual_time", virtual_time)
    run_kwargs.setdefault("seed_randomness", seed_randomness)

    def decorate(fn: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., None]:
        def wrapper(simloom_settings: Settings) -> None:
            __tracebackhide__ = True
            _execute(
                fn,
                settings=simloom_settings,
                runs=runs,
                scheduler=scheduler,
                start_seed=start_seed,
                shrink_budget=shrink_budget,
                require_coverage=tuple(require_coverage),
                run_kwargs=run_kwargs,
            )

        # Copy identity by hand: functools.wraps would set __wrapped__ and
        # pytest would then introspect the *async* signature for fixtures.
        wrapper.__name__ = fn.__name__
        wrapper.__qualname__ = fn.__qualname__
        wrapper.__doc__ = fn.__doc__
        wrapper.__module__ = fn.__module__
        return wrapper

    return decorate


def _execute(
    fn: Callable[..., Coroutine[Any, Any, Any]],
    *,
    settings: Settings,
    runs: int,
    scheduler: str,
    start_seed: int,
    shrink_budget: int,
    require_coverage: tuple[str, ...],
    run_kwargs: dict[str, Any],
) -> None:
    __tracebackhide__ = True
    if settings.tape_path is not None:
        _replay_tape_file(fn, settings.tape_path, scheduler, run_kwargs)
        return
    if settings.seed_override is not None:
        result = run(
            fn,
            seed=settings.seed_override,
            raise_on_error=False,
            scheduler=scheduler,
            **run_kwargs,
        )
        if result.outcome == "error":
            assert result.error is not None
            raise SimloomTestFailure(
                f"seed {settings.seed_override} fails: "
                f"{type(result.error).__name__}: {result.error}"
            ) from result.error
        return

    total_runs = settings.runs_override or runs
    exploration = explore(
        fn,
        runs=total_runs,
        start_seed=start_seed,
        stop_on_failure=True,
        scheduler=scheduler,
        **run_kwargs,
    )
    if not exploration.failed:
        missing = [label for label in require_coverage if not exploration.coverage.get(label)]
        if missing:
            raise SimloomTestFailure(
                f"no failures in {exploration.runs} universes, but required "
                f"coverage was never reached: {', '.join(missing)} — the "
                f"corpus did not exercise the paths this test is about"
            )
        return

    failure = exploration.first_failure
    assert failure is not None
    original = failure.error
    assert original is not None

    shrunk: ShrinkResult | None = None
    if settings.shrink_enabled:
        # shrink() replays under failure.scheduler on its own.
        shrunk = shrink(fn, failure, max_runs=shrink_budget, **run_kwargs)
    artifact_note = _write_artifacts(fn.__name__, failure, shrunk, settings.artifact_dir)

    lines = [
        f"simloom found a failing universe for {fn.__name__}",
        f"  seed: {failure.seed}   "
        f"(re-run: pytest -k {fn.__name__} --simloom-seed={failure.seed})",
        f"  error: {type(original).__name__}: {str(original)[:300]}",
        f"  explored: {exploration.runs} universe(s); {len(exploration.failures)} failed",
    ]
    if shrunk is not None:
        lines.append("  " + shrunk.describe().replace("\n", "\n  "))
    if artifact_note:
        lines.append(f"  artifacts: {artifact_note}")
    raise SimloomTestFailure("\n".join(lines)) from original


def _replay_tape_file(
    fn: Callable[..., Coroutine[Any, Any, Any]],
    path: str,
    scheduler: str,
    run_kwargs: dict[str, Any],
) -> None:
    __tracebackhide__ = True
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    recorded_scheduler = data.get("scheduler") or scheduler
    draws = Tape.draws_from_dict(data)
    result = replay(
        fn, tape=draws, raise_on_error=False, scheduler=recorded_scheduler, **run_kwargs
    )
    if result.outcome == "error":
        assert result.error is not None
        raise SimloomTestFailure(
            f"tape {path} reproduces: {type(result.error).__name__}: {result.error}"
        ) from result.error


def _write_artifacts(
    name: str,
    failure: RunResult,
    shrunk: ShrinkResult | None,
    directory: Path | None,
) -> str:
    if directory is None:
        return ""
    directory.mkdir(parents=True, exist_ok=True)
    stem = f"{name}-seed{failure.seed}"
    written: list[str] = []

    tape_payload = serialize_draws(failure.tape, seed=failure.seed)
    tape_payload["scheduler"] = failure.scheduler
    tape_file = directory / f"{stem}.tape.json"
    tape_file.write_text(json.dumps(tape_payload, indent=1), encoding="utf-8")
    written.append(str(tape_file))

    failure.log.write_to(directory / f"{stem}.events.jsonl")

    if shrunk is not None:
        shrunk_payload = serialize_draws(shrunk.tape, seed=None)
        shrunk_payload["scheduler"] = shrunk.result.scheduler
        shrunk_file = directory / f"{stem}.shrunk.tape.json"
        shrunk_file.write_text(json.dumps(shrunk_payload, indent=1), encoding="utf-8")
        written.append(str(shrunk_file))

    return ", ".join(written)
