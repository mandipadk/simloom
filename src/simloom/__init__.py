"""simloom — deterministic simulation testing for asyncio.

Run unmodified asyncio programs inside a fully simulated universe: a seeded
choice tape owns every scheduling decision, the clock is virtual, and every
failure replays exactly from its recording.

Phase A surface: the deterministic loop, the choice tape, run/replay, escape
detection, and the versioned event log. The simulated world (hosts, network,
faults) and the explorer arrive in later phases — see docs/plan.md.
"""

from ._errors import (
    EscapedSimulationError,
    SimDeadlockError,
    SimloomError,
    TapeMisalignmentError,
    UnhandledExceptionError,
)
from ._eventlog import EVENT_LOG_FORMAT_VERSION, EventLog
from ._loop import SimLoop
from ._run import RunResult, replay, run
from ._tape import TAPE_FORMAT_VERSION, Draw, MisalignmentPolicy, Tape
from ._version import __version__

__all__ = [
    "EVENT_LOG_FORMAT_VERSION",
    "TAPE_FORMAT_VERSION",
    "Draw",
    "EscapedSimulationError",
    "EventLog",
    "MisalignmentPolicy",
    "RunResult",
    "SimDeadlockError",
    "SimLoop",
    "SimloomError",
    "Tape",
    "TapeMisalignmentError",
    "UnhandledExceptionError",
    "__version__",
    "replay",
    "run",
]
