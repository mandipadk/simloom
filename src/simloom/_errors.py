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


class TapeMisalignmentError(SimloomError):
    """A replayed tape could not satisfy the draw the program asked for.

    Replay re-executes the program and feeds it recorded decisions; if the
    program requests a draw whose label or bound differs from what was
    recorded — or runs past the end of the tape — the execution has diverged
    from the recording (changed code, unpinned hash randomization, or an
    escape simloom failed to catch).
    """


class UnhandledExceptionError(SimloomError):
    """An exception reached the loop's exception handler and nothing else.

    asyncio's default behavior is to log fire-and-forget task failures and
    keep going; a testing harness must not let them pass silently. Configure
    with ``on_unhandled`` if a test legitimately expects orphaned failures.
    """
