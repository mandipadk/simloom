"""Consistency checking — the headline "finds *wrong answers*", not just crashes.

An Elle-style (Kingsbury & Alvaro) list-append checker. The model: keys map to
lists; an ``append(k, v)`` adds a *globally unique* ``v`` to key ``k``'s list, and
a ``read(k)`` observes the whole list. Unique appends make the version order
*recoverable from the reads*, which is what lets the checker reconstruct the
dependency graph between transactions:

- **ww** (write→write): if version order shows ``…v_i, v_{i+1}…`` for a key, the
  transaction that appended ``v_i`` precedes the one that appended ``v_{i+1}``.
- **wr** (write→read): a transaction that observed ``v`` ran after the one that
  appended it.
- **rw** (read→write, the anti-dependency): a transaction that read a key up to
  some version must precede every transaction that appended a *later* version —
  it didn't see those writes.

A serial order exists iff this dependency graph is acyclic. A cycle is a
serializability violation (Adya's G0/G1c/G2): the checker reports it with the
dependency type on each edge. A lost update / write skew shows up as a two-step
``rw`` cycle.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Op:
    """One micro-operation: ``f`` is ``"append"`` (``value`` is the element) or
    ``"read"`` (``value`` is the observed list)."""

    f: str
    key: str
    value: Any


@dataclass(frozen=True, slots=True)
class Transaction:
    index: int
    process: int
    ops: tuple[Op, ...]


@dataclass(frozen=True, slots=True)
class SerializabilityResult:
    ok: bool
    #: Transaction indices forming the dependency cycle (violation), else ().
    cycle: tuple[int, ...] = ()
    #: Dependency type (ww/wr/rw) on each edge of the cycle.
    edge_types: tuple[str, ...] = ()
    message: str = "serializable"

    def __bool__(self) -> bool:
        return self.ok


class _TxnBuilder:
    def __init__(self, history: History, process: int) -> None:
        self._history = history
        self._process = process
        self._ops: list[Op] = []

    def append(self, key: str, value: Any) -> None:
        self._ops.append(Op("append", key, value))

    def read(self, key: str, observed: Sequence[Any]) -> None:
        self._ops.append(Op("read", key, list(observed)))

    def _commit(self) -> None:
        self._history.record(self._ops, process=self._process)


class History:
    """A recorded history of list-append transactions, in completion order."""

    def __init__(self) -> None:
        self._txns: list[Transaction] = []

    def record(self, ops: Sequence[tuple[str, str, Any] | Op], *, process: int = 0) -> None:
        normalized = tuple(o if isinstance(o, Op) else Op(*o) for o in ops)
        self._txns.append(Transaction(len(self._txns), process, normalized))

    @contextmanager
    def transaction(self, process: int = 0) -> Iterator[_TxnBuilder]:
        builder = _TxnBuilder(self, process)
        yield builder
        builder._commit()

    @property
    def transactions(self) -> tuple[Transaction, ...]:
        return tuple(self._txns)

    def __len__(self) -> int:
        return len(self._txns)


def _shortest_cycle(succ: dict[int, list[int]], nodes: Sequence[int]) -> list[int] | None:
    """The shortest directed cycle (BFS from each node), so a violation is
    reported as a *minimal* witness — a two-step rw cycle for a lost update,
    not whatever longer cycle a DFS happened to find first."""
    best: list[int] | None = None
    for source in nodes:
        parent: dict[int, int] = {}
        distance = {source: 0}
        queue: deque[int] = deque([source])
        found: list[int] | None = None
        while queue and found is None:
            node = queue.popleft()
            if best is not None and distance[node] + 1 >= len(best):
                continue  # cannot beat the current best from here
            for nxt in succ.get(node, ()):
                if nxt == source:  # closed a cycle source -> … -> node -> source
                    path = [node]
                    while path[-1] != source:
                        path.append(parent[path[-1]])
                    path.reverse()
                    found = path
                    break
                if nxt not in distance:
                    distance[nxt] = distance[node] + 1
                    parent[nxt] = node
                    queue.append(nxt)
        if found is not None and (best is None or len(found) < len(best)):
            best = found
            if len(best) == 2:  # nothing is shorter than a two-cycle
                break
    return best


@dataclass(slots=True)
class _Graph:
    succ: dict[int, list[int]] = field(default_factory=dict)
    types: dict[tuple[int, int], str] = field(default_factory=dict)

    def add(self, u: int, v: int, kind: str) -> None:
        if u == v:
            return
        if (u, v) not in self.types:
            self.succ.setdefault(u, []).append(v)
            self.types[(u, v)] = kind


def check_serializable(history: History) -> SerializabilityResult:
    """Check a list-append history for serializability (Elle-style). Returns a
    result whose ``cycle`` (with per-edge ``edge_types``) witnesses any
    violation."""
    txns = history.transactions
    if not txns:
        return SerializabilityResult(ok=True, message="empty history is serializable")

    # 1. Recover each key's version order from the longest read that observed it,
    #    and which transaction appended each (unique) value.
    version_order: dict[str, list[Any]] = {}
    appender: dict[Any, int] = {}
    for txn in txns:
        for op in txn.ops:
            if op.f == "append":
                appender[op.value] = txn.index
            elif op.f == "read":
                observed = list(op.value)
                if op.key not in version_order or len(observed) > len(version_order[op.key]):
                    version_order[op.key] = observed

    graph = _Graph()
    for key, order in version_order.items():
        index_of = {v: i for i, v in enumerate(order)}
        # 2. ww edges along the version order.
        for i in range(len(order) - 1):
            a, b = appender.get(order[i]), appender.get(order[i + 1])
            if a is not None and b is not None:
                graph.add(a, b, "ww")
        # 3. wr and rw edges from each read of this key.
        for txn in txns:
            for op in txn.ops:
                if op.f != "read" or op.key != key:
                    continue
                observed = list(op.value)
                for v in observed:  # wr: the writer of an observed value precedes us
                    w = appender.get(v)
                    if w is not None:
                        graph.add(w, txn.index, "wr")
                # rw: we precede the writer of every version we did NOT see
                last_seen = index_of.get(observed[-1], -1) if observed else -1
                for v in order[last_seen + 1 :]:
                    w = appender.get(v)
                    if w is not None:
                        graph.add(txn.index, w, "rw")

    cycle = _shortest_cycle(graph.succ, [t.index for t in txns])
    if cycle is None:
        return SerializabilityResult(ok=True, message=f"{len(txns)} transactions serializable")
    edge_types = tuple(
        graph.types[(cycle[i], cycle[(i + 1) % len(cycle)])] for i in range(len(cycle))
    )
    path = " -> ".join(str(c) for c in [*cycle, cycle[0]])
    return SerializabilityResult(
        ok=False,
        cycle=tuple(cycle),
        edge_types=edge_types,
        message=f"serializability violation: dependency cycle {path} ({'-'.join(edge_types)})",
    )
