"""Exception types raised by simloom."""

from __future__ import annotations


class SimloomError(Exception):
    """Base class for every error simloom raises on its own behalf."""


class EscapedSimulationError(SimloomError):
    """The program under test reached for the real world from inside the sim.

    Determinism only holds while every effect flows through the simulated
    loop. Real sockets, file-descriptor callbacks, signal handlers, real DNS,
    subprocesses — any of these would reintroduce nondeterminism silently, so
    simloom turns them into this error at the exact call site instead.
    """

    def __init__(self, api: str, hint: str) -> None:
        self.api = api
        self.hint = hint
        super().__init__(
            f"{api} escapes the simulation: {hint} (see docs/determinism.md for the full boundary)"
        )


class SimDeadlockError(SimloomError):
    """The simulated universe went quiescent with work still pending.

    No callback is runnable and no timer is scheduled, but the run has not
    finished: every remaining task is waiting on something that can no longer
    happen. This is the classic distributed-systems deadlock, caught at the
    moment it forms instead of as a test timeout.
    """


class InvariantViolation(SimloomError):
    """A registered property (``world.always``/``eventually``/``leads_to``)
    was violated.

    ``kind`` is ``"safety"`` for an ``always`` predicate that became false,
    or ``"liveness"`` for an ``eventually``/``leads_to`` goal that was not
    satisfied by its deadline (or by the end of the run). ``t`` is the virtual
    time at which the violation was detected. This is what turns simloom from
    "finds crashes" into "finds wrong and stuck behaviour": it flows through
    the ordinary error path, so it is found, shrunk, and replayed like any
    other failure.
    """

    def __init__(self, label: str, kind: str, t: float, detail: str = "") -> None:
        self.label = label
        self.kind = kind
        self.t = t
        suffix = f": {detail}" if detail else ""
        super().__init__(f"{kind} property {label!r} violated at t={t}{suffix}")


class SimLivelockError(SimloomError):
    """The simulated universe is busy but making no temporal progress.

    Unlike :class:`SimDeadlockError` (quiescent — nothing runnable), a livelock
    keeps scheduling callbacks forever at a single virtual instant: a hot
    ``while True`` that only ever ``await``\\ s a zero-delay sleep, or two tasks
    that re-arm each other with ``call_soon``. The virtual clock never advances,
    so no real work is ever done. Caught by a bound on consecutive steps that do
    not advance the clock.
    """


class ConsistencyViolation(SimloomError):
    """A recorded operation history is not serializable.

    The store returned a *wrong answer*, not a crash: a read saw a stale or
    impossible value. ``cycle`` is the dependency cycle that witnesses it, with
    ``edge_types`` (ww/wr/rw) on each edge.
    """

    def __init__(
        self, message: str, cycle: tuple[int, ...] = (), edge_types: tuple[str, ...] = ()
    ) -> None:
        self.cycle = cycle
        self.edge_types = edge_types
        super().__init__(message)


class TapeMisalignmentError(SimloomError):
    """A replayed tape could not satisfy the draw the program asked for.

    Replay re-executes the program and feeds it recorded decisions; if the
    program requests a draw whose label or bound differs from what was
    recorded — or runs past the end of the tape — the execution has diverged
    from the recording (changed code, unpinned hash randomization, or an
    escape simloom failed to catch).
    """


class SimloomNondeterminismError(SimloomError):
    """The same seed produced two different universes.

    simloom guarantees determinism *given the tape* — but a test can smuggle in
    nondeterminism the tape does not control: iterating a set/dict keyed by
    object identity (address-ordered), a stray ``time.time()``/``random`` not
    routed through simloom, or threads doing real work. The self-check
    (``check_determinism=True``) runs a seed twice and, if the event logs
    differ, raises this with the first diverging event located.
    """


class UnhandledExceptionError(SimloomError):
    """An exception reached the loop's exception handler and nothing else.

    asyncio's default behavior is to log fire-and-forget task failures and
    keep going; a testing harness must not let them pass silently. Configure
    with ``on_unhandled`` if a test legitimately expects orphaned failures.
    """
