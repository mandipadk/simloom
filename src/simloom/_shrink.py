"""The shrinker: reduce a failing tape to a minimal, human-readable repro.

Greedy minimization in the Hypothesis shortlex order — fewer draws first,
then lexicographically smaller values — with two lessons the Phase 0 spikes
paid for:

- Every accepted candidate is **re-recorded**: replaying an edited tape can
  run past it (the fallback PRNG fills the tail), and the working tape must
  always be the exact draw sequence the failing run actually consumed.
- For *schedule* tapes, length is mostly intrinsic to the program, so the
  number that matters for readability is the count of nonzero scheduler
  picks: every nonzero pick is one deliberate deviation from the canonical
  run-the-oldest (FIFO) order. The shrinker minimizes shortlex; FIFO
  deviations fall out of the value minimization and are what we report.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from typing import Any

from ._run import RunResult, replay
from ._tape import Draw, MisalignmentPolicy


@dataclass(slots=True)
class ShrinkResult:
    """A minimized failing universe plus the story of getting there."""

    tape: tuple[Draw, ...]
    result: RunResult
    runs_used: int
    initial_length: int
    initial_deviations: int

    @property
    def length(self) -> int:
        return len(self.tape)

    @property
    def deviations(self) -> int:
        return _deviations(self.tape)

    def describe(self) -> str:
        lines = [
            f"shrunk: {self.initial_length} draws -> {self.length}, "
            f"schedule deviations {self.initial_deviations} -> {self.deviations} "
            f"({self.runs_used} candidate runs)",
        ]
        interesting = [(i, d) for i, d in enumerate(self.tape) if d.value != 0]
        if not interesting:
            lines.append("minimal schedule: pure FIFO (every draw forced to 0)")
        else:
            lines.append("minimal schedule: FIFO everywhere except:")
            for index, d in interesting:
                lines.append(f"  draw #{index}: {d.label} = {d.value} (of {d.bound})")
        error = self.result.error
        if error is not None:
            lines.append(f"reproduces: {type(error).__name__}: {str(error)[:160]}")
        return "\n".join(lines)


def _deviations(draws: Sequence[Draw]) -> int:
    return sum(1 for d in draws if d.value != 0)


def _key(draws: Sequence[Draw]) -> tuple[int, int, tuple[int, ...]]:
    """The minimization order: fewest deviations from the canonical FIFO
    schedule first (that is what a human reads), then shortlex. A real
    domain divergence from Hypothesis, where shorter is always simpler:
    a schedule's length is mostly intrinsic to the program."""
    return (_deviations(draws), len(draws), tuple(d.value for d in draws))


def shrink(
    main: Callable[..., Coroutine[Any, Any, Any]],
    failure: RunResult,
    *,
    max_runs: int = 1500,
    interesting: Callable[[RunResult], bool] | None = None,
    **run_kwargs: Any,
) -> ShrinkResult:
    """Minimize ``failure``'s tape while it keeps failing the same way.

    ``interesting`` decides whether a candidate run still reproduces the
    failure; the default accepts the same exception type as the original.
    The budget ``max_runs`` bounds candidate executions, so shrinking always
    terminates in predictable time.
    """
    if failure.outcome != "error" or failure.error is None:
        raise ValueError("shrink() needs a failing RunResult")
    error_type = type(failure.error)
    if interesting is None:

        def interesting(candidate: RunResult) -> bool:
            return candidate.outcome == "error" and isinstance(candidate.error, error_type)

    runs_used = 0

    def execute(draws: Sequence[Draw]) -> RunResult:
        nonlocal runs_used
        runs_used += 1
        return replay(
            main,
            tape=draws,
            policy=MisalignmentPolicy.FALLBACK,
            fallback="zero",
            raise_on_error=False,
            scheduler=failure.scheduler,
            **run_kwargs,
        )

    current: tuple[Draw, ...] = failure.tape
    current_result = failure
    initial_length = len(current)
    initial_deviations = _deviations(current)

    def try_accept(candidate: Sequence[Draw]) -> bool:
        nonlocal current, current_result
        if runs_used >= max_runs:
            return False
        outcome = execute(candidate)
        if not interesting(outcome):
            return False
        exact = outcome.tape  # re-recorded: the draws actually consumed
        if _key(exact) < _key(current):
            current = exact
            current_result = outcome
            return True
        return False

    improved = True
    while improved and runs_used < max_runs:
        improved = False
        # Pass 1: chunked deletion, coarse to fine.
        chunk = max(1, len(current) // 2)
        while chunk >= 1 and runs_used < max_runs:
            index = 0
            while index < len(current) and runs_used < max_runs:
                candidate = current[:index] + current[index + chunk :]
                if candidate and try_accept(candidate):
                    improved = True
                else:
                    index += chunk
            chunk //= 2
        # Pass 1.5: block zeroing — force whole chunks back to the canonical
        # FIFO choice. Labels and bounds stay aligned, so these candidates
        # replay the prefix exactly; for schedule tapes this is the
        # workhorse pass.
        chunk = max(1, len(current) // 2)
        while chunk >= 1 and runs_used < max_runs:
            index = 0
            while index < len(current) and runs_used < max_runs:
                block = current[index : index + chunk]
                if any(d.value != 0 for d in block):
                    zeroed = tuple(Draw(d.label, 0, d.bound) for d in block)
                    if try_accept(current[:index] + zeroed + current[index + chunk :]):
                        improved = True
                index += chunk
            chunk //= 2
        # Pass 2: value minimization — 0 first, then halving descent.
        # Accepted candidates are re-recorded and may change the tape's
        # length, so walk with a live bounds check, not a snapshot range.
        index = 0
        while index < len(current) and runs_used < max_runs:
            draw = current[index]
            if draw.value != 0:
                for smaller in _descend(draw.value):
                    replacement = (Draw(draw.label, smaller, draw.bound),)
                    candidate = current[:index] + replacement + current[index + 1 :]
                    if try_accept(candidate):
                        improved = True
                        break
            index += 1

    return ShrinkResult(
        tape=current,
        result=current_result,
        runs_used=runs_used,
        initial_length=initial_length,
        initial_deviations=initial_deviations,
    )


def _descend(value: int) -> list[int]:
    """Candidate replacements for a draw value, most aggressive first."""
    candidates = [0]
    half = value // 2
    if half not in (0, value):
        candidates.append(half)
    if value - 1 not in candidates and value - 1 != value:
        candidates.append(value - 1)
    return candidates
