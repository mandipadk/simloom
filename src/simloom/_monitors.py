"""Property monitors: safety (``always``), liveness (``eventually``), and
``leads_to``, checked against the deterministic step sequence.

The point of Phase F: turn simloom from "finds crashes" into "finds *wrong*
and *stuck* behaviour". A monitor is a pure ``Callable[[], bool]`` predicate
over user state, evaluated *between* scheduler steps — never during a coroutine
step, so it adds no tape draw and (when it passes) emits no event. That keeps
the determinism contract intact: a run whose monitors all pass produces a
byte-identical event log to a run with no monitors at all. Only a *violation*
perturbs the universe (it emits an ``invariant`` event and stops the run),
which is exactly what we want.

The loop owns evaluation timing (it knows the deterministic step boundaries and
the virtual clock); this module owns the bookkeeping and the verdicts. Methods
*return* an :class:`InvariantViolation` rather than raising, so the loop can
emit the log event at the right virtual time before propagating it.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ._errors import InvariantViolation

Predicate = Callable[[], bool]


def _validate(label: str, predicate: object, role: str = "predicate") -> None:
    if not label:
        raise ValueError("monitor label must be a non-empty string")
    # inspect's variant, not asyncio's: the latter is deprecated in 3.14.
    if inspect.iscoroutinefunction(predicate):
        raise TypeError(
            f"monitor {label!r} {role} must be a plain (non-async) callable; "
            f"a monitor is checked synchronously between steps and cannot await"
        )
    if not callable(predicate):
        raise TypeError(f"monitor {label!r} {role} must be callable, got {predicate!r}")


@dataclass(slots=True)
class _Always:
    label: str
    predicate: Predicate


@dataclass(slots=True)
class _Eventually:
    label: str
    predicate: Predicate
    deadline: float
    satisfied: bool = False


@dataclass(slots=True)
class _LeadsTo:
    label: str
    trigger: Predicate
    response: Predicate
    within: float
    armed_deadline: float | None = None


class MonitorSet:
    """The registered properties for one run, plus their evaluation logic."""

    __slots__ = ("_always", "_eventually", "_leads_to")

    def __init__(self) -> None:
        self._always: list[_Always] = []
        self._eventually: list[_Eventually] = []
        self._leads_to: list[_LeadsTo] = []

    @property
    def empty(self) -> bool:
        return not (self._always or self._eventually or self._leads_to)

    # --- registration -------------------------------------------------

    def add_always(self, label: str, predicate: Predicate) -> None:
        _validate(label, predicate)
        self._always.append(_Always(label, predicate))

    def add_eventually(self, label: str, predicate: Predicate, deadline: float) -> None:
        _validate(label, predicate)
        self._eventually.append(_Eventually(label, predicate, deadline))

    def add_leads_to(
        self, label: str, trigger: Predicate, response: Predicate, within: float
    ) -> None:
        _validate(label, trigger, "trigger")
        _validate(label, response, "response")
        if within <= 0:
            raise ValueError("leads_to 'within' must be > 0")
        self._leads_to.append(_LeadsTo(label, trigger, response, within))

    # --- evaluation (called by the loop, between steps) ---------------

    def after_step(self, now: float) -> InvariantViolation | None:
        """Evaluate every monitor at a step boundary. Returns the first safety
        violation found, or None. Liveness goals are only *armed/satisfied*
        here (their deadlines are enforced by :meth:`fire_due`)."""
        for m in self._always:
            if not m.predicate():
                return InvariantViolation(m.label, "safety", now, "predicate became false")
        for e in self._eventually:
            if not e.satisfied and e.predicate():
                e.satisfied = True
        for lt in self._leads_to:
            if lt.armed_deadline is not None:
                if lt.response():
                    lt.armed_deadline = None  # obligation discharged
            elif lt.trigger() and not lt.response():
                lt.armed_deadline = now + lt.within  # fresh obligation opened
        return None

    def next_deadline(self) -> float | None:
        """The earliest armed liveness deadline, or None. The loop consults
        this to decide whether a deadline expires before the next scheduled
        event would run."""
        best: float | None = None
        for e in self._eventually:
            if not e.satisfied and (best is None or e.deadline < best):
                best = e.deadline
        for lt in self._leads_to:
            if lt.armed_deadline is not None and (best is None or lt.armed_deadline < best):
                best = lt.armed_deadline
        return best

    def fire_due(self, now: float) -> InvariantViolation | None:
        """Enforce liveness deadlines that have been reached. No coroutine step
        runs between ``after_step`` and here, so a goal still unsatisfied at its
        deadline genuinely failed — but we re-check the predicate defensively."""
        for e in self._eventually:
            if not e.satisfied and e.deadline <= now:
                if e.predicate():
                    e.satisfied = True
                else:
                    return InvariantViolation(
                        e.label, "liveness", e.deadline, "not satisfied within its deadline"
                    )
        for lt in self._leads_to:
            if lt.armed_deadline is not None and lt.armed_deadline <= now:
                if lt.response():
                    lt.armed_deadline = None
                else:
                    return InvariantViolation(
                        lt.label,
                        "liveness",
                        lt.armed_deadline,
                        "response did not follow trigger within its deadline",
                    )
        return None

    def finalize(self, now: float) -> InvariantViolation | None:
        """At the end of a run, a liveness goal that was never satisfied is a
        violation: ``eventually`` is an assertion, not a hint. (A goal already
        satisfied during the run, or a discharged obligation, is fine.)"""
        for e in self._eventually:
            if not e.satisfied:
                if e.predicate():
                    e.satisfied = True
                else:
                    return InvariantViolation(
                        e.label, "liveness", now, "never satisfied before the run ended"
                    )
        for lt in self._leads_to:
            if lt.armed_deadline is not None and not lt.response():
                return InvariantViolation(
                    lt.label,
                    "liveness",
                    now,
                    "response still pending when the run ended",
                )
        return None


# --------------------------------------------------------------------------
# Module-level API — works on the running simulation's monitor set, so a test
# that does not take a `world` parameter can still register properties.
# (World.always/eventually/leads_to delegate to the same MonitorSet.)
# --------------------------------------------------------------------------


def _running_monitor_set() -> tuple[Any, MonitorSet]:
    loop = asyncio.events._get_running_loop()
    monitors = getattr(loop, "_monitors", None)
    if not isinstance(monitors, MonitorSet):
        raise RuntimeError("simloom property monitors require a running simloom simulation")
    return loop, monitors


def always(label: str, predicate: Predicate) -> None:
    """Assert a safety property: ``predicate()`` must hold at every step
    boundary for the rest of the run. The first step after which it is false
    raises ``InvariantViolation`` (kind="safety")."""
    _running_monitor_set()[1].add_always(label, predicate)


def eventually(label: str, predicate: Predicate, *, within: float) -> None:
    """Assert a liveness property: ``predicate()`` must become true at some
    point within ``within`` virtual seconds (measured from now), and before the
    run ends. Otherwise raises ``InvariantViolation`` (kind="liveness") at the
    deadline (or at run end if the run finishes first)."""
    if within <= 0:
        raise ValueError("eventually 'within' must be > 0")
    loop, monitors = _running_monitor_set()
    monitors.add_eventually(label, predicate, loop.time() + within)


def leads_to(label: str, trigger: Predicate, response: Predicate, *, within: float) -> None:
    """Assert a response property: whenever ``trigger()`` holds, ``response()``
    must hold within ``within`` virtual seconds. Each time the trigger opens an
    obligation, the response must discharge it before the deadline."""
    _running_monitor_set()[1].add_leads_to(label, trigger, response, within)
