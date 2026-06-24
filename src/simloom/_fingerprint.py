"""Interleaving fingerprints and the regression corpus — the feedback
substrate for coverage-guided exploration.

A whole-run digest (``RunResult.digest``) is too fine to steer with: every
distinct schedule has a unique digest, so it can tell you two runs differ but
not whether a run explored *novel structure*. The fingerprint is coarser: the
set of ``(previous-callback, current-callback)`` edges in the step sequence —
"which callback ran after which". Two runs that interleave the same tasks in a
structurally similar way share edges; a run that orders them a genuinely new
way contributes new edges. That is the signal a greybox explorer follows:
keep the tapes that hit new edges, mutate them to reach more.

The ``ran`` labels are loop-owned and deterministic across seeds (``task-0``,
``task-1``, … by creation order; callback qualnames otherwise), so edges are
comparable from one seed to the next.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ._tape import Draw

Edge = tuple[str, str]


def interleaving_edges(events: Iterable[Mapping[str, Any]]) -> set[Edge]:
    """The set of consecutive ``(prev_ran, ran)`` step edges of a run."""
    edges: set[Edge] = set()
    prev: str | None = None
    for event in events:
        if event.get("kind") != "step":
            continue
        ran = event["ran"]
        if prev is not None:
            edges.add((prev, ran))
        prev = ran
    return edges


def fingerprint(events: Iterable[Mapping[str, Any]]) -> frozenset[Edge]:
    """A hashable fingerprint of a run's interleaving (its edge set)."""
    return frozenset(interleaving_edges(events))


@dataclass(frozen=True, slots=True)
class CorpusEntry:
    """A tape that contributed novel interleaving structure — a seed worth
    keeping and mutating."""

    tape: tuple[Draw, ...]
    scheduler: str
    seed: int | None
    new_edges: int


@dataclass(slots=True)
class InterleavingCorpus:
    """Accumulates the global set of interleaving edges seen so far, and keeps
    the tapes that first reached new ones. This is what a coverage-guided
    explorer grows and samples from."""

    _seen: set[Edge] = field(default_factory=set)
    entries: list[CorpusEntry] = field(default_factory=list)

    def observe(
        self,
        events: Iterable[Mapping[str, Any]],
        tape: tuple[Draw, ...],
        *,
        scheduler: str = "random",
        seed: int | None = None,
    ) -> int:
        """Fold a run into the corpus. Returns the number of *new* edges it
        contributed (0 if it explored nothing structurally novel); a run with
        new edges is kept as a corpus entry."""
        edges = interleaving_edges(events)
        new = edges - self._seen
        if new:
            self._seen |= new
            self.entries.append(
                CorpusEntry(tape=tape, scheduler=scheduler, seed=seed, new_edges=len(new))
            )
        return len(new)

    @property
    def coverage(self) -> int:
        """The number of distinct interleaving edges seen so far."""
        return len(self._seen)

    def __len__(self) -> int:
        return len(self.entries)
