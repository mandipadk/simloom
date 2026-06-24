"""Systematic exploration: a stateless model checker over the choice tape.

Random walk and PCT *sample* the schedule space; they find bugs with a
probability, and can never tell you a program is *correct*. Systematic
exploration instead *enumerates* the schedule space: it walks every distinct
interleaving (up to a bound) exactly once, so it finds a bug deterministically
if one exists in scope — and, just as valuably, **proves its absence** when it
exhausts the space without finding one.

How it works, reusing the existing machinery whole: a schedule is just a tape of
choice draws, and ``replay(tape=prefix, policy=FALLBACK, fallback="zero")`` forces
a prefix and runs the rest along the canonical "front of the ready queue" default.
So exploration is a mixed-radix **odometer over the choice tape**: run, read the
realised draws, find the rightmost draw with an untried alternative, bump it,
truncate, repeat. Each step yields a distinct interleaving; when the odometer
rolls over, the space is provably exhausted. A failure it finds is already a
tape — it replays and shrinks like any other.

The reduction is **delay bounding** (Emmi, Qadeer & Rakamarić, POPL 2011): a
"delay" is any choice other than the default (front of the queue). Bounding the
number of delays to ``max_delays`` collapses the space — every reordering of
independent steps that the default order already covers is skipped — while still
catching the bugs that matter (the empirical rule is that almost all concurrency
bugs surface within two or three delays). Unlike true partial-order reduction it
needs no happens-before / memory-access tracking, which simloom does not do.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from ._explore import Failure
from ._run import RunResult, replay
from ._sched import SchedulerFactory
from ._tape import Draw, MisalignmentPolicy


@dataclass(frozen=True, slots=True)
class SystematicResult:
    """The outcome of a systematic campaign."""

    schedules: int
    failures: list[Failure]
    max_delays: int
    #: True iff the whole delay-bounded space was covered (not budget-truncated).
    exhaustive: bool
    first_failure_at: int | None = None
    #: How many delays the first witness needed (its non-default choices).
    first_failure_delays: int | None = None
    first: RunResult | None = field(default=None, repr=False)

    @property
    def failed(self) -> bool:
        return bool(self.failures)

    @property
    def proven_correct(self) -> bool:
        """True iff the space was exhausted with no failure — a proof that no
        interleaving within ``max_delays`` delays fails."""
        return self.exhaustive and not self.failures


def _delays(draws: Sequence[Draw]) -> int:
    """The number of non-default choices (delays) in a schedule prefix."""
    return sum(1 for d in draws if d.value)


def explore_systematic(
    main: Callable[..., Coroutine[Any, Any, Any]],
    *,
    max_delays: int = 2,
    max_schedules: int = 100_000,
    stop_on_failure: bool = True,
    scheduler: str | SchedulerFactory | None = None,
    **run_kwargs: Any,
) -> SystematicResult:
    """Enumerate every interleaving within ``max_delays`` delays of the default.

    Returns a :class:`SystematicResult`. If it exhausts the space (``exhaustive``)
    with no failure, ``proven_correct`` is True — no interleaving within the bound
    fails. Raise ``max_delays`` to widen the proof; ``max_schedules`` caps work.
    """
    if max_delays < 0:
        raise ValueError("max_delays must be >= 0")
    if max_schedules < 1:
        raise ValueError("max_schedules must be >= 1")

    plan: list[Draw] = []
    failures: list[Failure] = []
    first: RunResult | None = None
    first_at: int | None = None
    first_delays: int | None = None
    explored = 0
    exhaustive = False

    while explored < max_schedules:
        result = replay(
            main,
            tape=plan,
            policy=MisalignmentPolicy.FALLBACK,
            fallback="zero",
            raise_on_error=False,
            scheduler=scheduler,
            **run_kwargs,
        )
        explored += 1
        if result.outcome == "error":
            assert result.error is not None
            failures.append(
                Failure(explored, type(result.error).__name__, str(result.error)[:200])
            )
            if first is None:
                first, first_at, first_delays = result, explored, _delays(result.tape)
            if stop_on_failure:
                break

        # Odometer: bump the rightmost draw that has an untried alternative and
        # whose prefix still has delay budget for one more non-default choice.
        realized = list(result.tape)
        index = len(realized) - 1
        while index >= 0:
            draw = realized[index]
            if draw.value < draw.bound - 1 and _delays(realized[:index]) < max_delays:
                break
            index -= 1
        if index < 0:
            exhaustive = True
            break
        bumped = replace(realized[index], value=realized[index].value + 1)
        plan = [*realized[:index], bumped]

    return SystematicResult(
        schedules=explored,
        failures=failures,
        max_delays=max_delays,
        exhaustive=exhaustive,
        first_failure_at=first_at,
        first_failure_delays=first_delays,
        first=first,
    )
