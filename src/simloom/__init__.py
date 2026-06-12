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
from ._net import SimNetwork, SimServer, SimTransport
from ._run import RunResult, replay, run
from ._tape import TAPE_FORMAT_VERSION, Draw, MisalignmentPolicy, Tape
from ._version import __version__
from ._world import Host, SimDisk, World

__all__ = [
    "EVENT_LOG_FORMAT_VERSION",
    "TAPE_FORMAT_VERSION",
    "Draw",
    "EscapedSimulationError",
    "EventLog",
    "Host",
    "MisalignmentPolicy",
    "RunResult",
    "SimDeadlockError",
    "SimDisk",
    "SimLoop",
    "SimNetwork",
    "SimServer",
    "SimTransport",
    "SimloomError",
    "Tape",
    "TapeMisalignmentError",
    "UnhandledExceptionError",
    "World",
    "__version__",
    "replay",
    "run",
]
