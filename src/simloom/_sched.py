"""Scheduling strategies: how the next ready callback is chosen.

Every strategy draws its randomness from the run's choice tape, so a PCT run
records, replays, and shrinks exactly like a random-walk run — the tape stays
the single source of nondeterminism.

- RandomWalk: uniform pick among ready callbacks (the default; one
  ``sched.pick`` draw per contested step).
- PCT: Probabilistic Concurrency Testing (Burckhardt et al., ASPLOS 2010).
  Each scheduling entity (task) gets a tape-drawn priority; the highest
  priority ready entity always runs; at d-1 tape-chosen step indices the
  running entity is demoted. For a bug of depth d among n entities and k
  steps, PCT guarantees finding probability ≥ 1/(n*k^(d-1)) — much better
  than a random walk for deep orderings.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from ._loop import SimLoop


class Scheduler(Protocol):
    """Picks which of the ready handles runs next."""

    descriptor: str

    def pick(self, ready: Sequence[asyncio.Handle]) -> int: ...


class SchedulerFactory(Protocol):
    def __call__(self, loop: SimLoop) -> Scheduler: ...


class _RandomWalk:
    descriptor = "random"

    def __init__(self, loop: SimLoop) -> None:
        self._loop = loop

    def pick(self, ready: Sequence[asyncio.Handle]) -> int:
        count = len(ready)
        if count == 1:
            return 0
        return self._loop.tape.draw("sched.pick", count)


@dataclass(frozen=True)
class RandomWalk:
    """The default strategy: uniform tape-drawn pick among ready callbacks."""

    descriptor: str = "random"

    def __call__(self, loop: SimLoop) -> Scheduler:
        return _RandomWalk(loop)


class _PCT:
    def __init__(self, loop: SimLoop, depth: int, horizon: int, descriptor: str) -> None:
        self._loop = loop
        self.descriptor = descriptor
        self._priorities: dict[int, tuple[int, int]] = {}
        self._floor = -1  # demoted entities stack below every drawn priority
        self._steps = 0
        tape = loop.tape
        self._change_points = {tape.draw("pct.change", horizon) for _ in range(max(0, depth - 1))}

    def _entity(self, handle: asyncio.Handle) -> int:
        callback: Any = getattr(handle, "_callback", None)
        # functools.partial chains
        while True:
            inner = getattr(callback, "func", None)
            if inner is None or not callable(inner):
                break
            callback = inner
        owner = getattr(callback, "__self__", None)
        if isinstance(owner, asyncio.Task):
            order = self._loop._task_order.get(owner)
            if order is not None:
                return order
        return -1  # ownerless callbacks share the "loop" entity

    def _priority(self, entity: int) -> tuple[int, int]:
        found = self._priorities.get(entity)
        if found is None:
            # Tape-drawn rank; entity id breaks ties deterministically.
            found = (self._loop.tape.draw("pct.prio", 1024), -entity)
            self._priorities[entity] = found
        return found

    def pick(self, ready: Sequence[asyncio.Handle]) -> int:
        if len(ready) == 1:
            index = 0
        else:
            index = max(
                range(len(ready)),
                key=lambda i: (self._priority(self._entity(ready[i])), -i),
            )
        if self._steps in self._change_points:
            entity = self._entity(ready[index])
            self._priorities[entity] = (self._floor, -entity)
            self._floor -= 1
        self._steps += 1
        return index


@dataclass(frozen=True)
class PCT:
    """Probabilistic Concurrency Testing. ``depth`` is the bug depth to
    target (d ordering constraints); ``horizon`` is the expected number of
    scheduling steps (change points are drawn from it)."""

    depth: int = 3
    horizon: int = 4096

    @property
    def descriptor(self) -> str:
        return f"pct:d={self.depth},k={self.horizon}"

    def __call__(self, loop: SimLoop) -> Scheduler:
        return _PCT(loop, self.depth, self.horizon, self.descriptor)


def auto_pct_depth(spec: str | SchedulerFactory | None) -> int | None:
    """If ``spec`` is an auto-horizon PCT request (``"pct:auto"`` or
    ``"pct:auto,d=2"``), return its depth; otherwise None. The horizon ``k`` is
    then measured by a probe run (in ``run``/``explore``) instead of guessed."""
    if not isinstance(spec, str) or not spec.startswith("pct:auto"):
        return None
    rest = spec[len("pct:auto") :].lstrip(",")
    params = dict(p.split("=", 1) for p in rest.split(",") if p) if rest else {}
    return int(params.get("d", 3))


def resolve_scheduler(spec: str | SchedulerFactory | None) -> SchedulerFactory:
    """Accepts "random", "pct", "pct:d=2,k=1000", a factory, or None.

    ``"pct:auto"`` is NOT resolvable here — its horizon must be measured by a
    probe run first; ``run``/``explore`` handle that before this is called.
    """
    if spec is None or spec == "random":
        return RandomWalk()
    if isinstance(spec, str):
        if spec.startswith("pct:auto"):
            raise ValueError(
                "'pct:auto' must be resolved by run()/explore() (it probes a run "
                "to measure the horizon); it cannot be resolved standalone"
            )
        if spec == "pct":
            return PCT()
        if spec.startswith("pct:"):
            params = dict(part.split("=", 1) for part in spec[4:].split(","))
            return PCT(depth=int(params.get("d", 3)), horizon=int(params.get("k", 4096)))
        raise ValueError(f"unknown scheduler {spec!r}")
    return spec
