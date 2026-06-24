"""simloom — deterministic simulation testing for asyncio.

Run unmodified asyncio programs inside a fully simulated universe: a seeded
choice tape owns every scheduling decision, the clock is virtual, and every
failure replays exactly from its recording.

Phase A surface: the deterministic loop, the choice tape, run/replay, escape
detection, and the versioned event log. The simulated world (hosts, network,
faults) and the explorer arrive in later phases — see docs/plan.md.
"""

from ._boundary import BoundaryEntry, BoundaryStatus, boundary
from ._buggify import draw, reached, sometimes
from ._errors import (
    EscapedSimulationError,
    InvariantViolation,
    SimDeadlockError,
    SimLivelockError,
    SimloomError,
    SimloomNondeterminismError,
    TapeMisalignmentError,
    UnhandledExceptionError,
)
from ._eventlog import EVENT_LOG_FORMAT_VERSION, EventLog
from ._explore import Exploration, Failure, explore
from ._fingerprint import CorpusEntry, InterleavingCorpus, fingerprint, interleaving_edges
from ._hashseed import is_pinned, pin_hashseed
from ._loop import SimLoop
from ._monitors import always, eventually, leads_to
from ._net import SimDatagramTransport, SimNetwork, SimServer, SimTransport
from ._run import RunResult, replay, run
from ._sched import PCT, RandomWalk
from ._shrink import ShrinkResult, shrink
from ._soak import SoakReport, soak
from ._systematic import SystematicResult, explore_systematic
from ._tape import TAPE_FORMAT_VERSION, Draw, MisalignmentPolicy, Tape
from ._testing import Settings, SimloomTestFailure, test
from ._version import __version__
from ._world import Host, SimDisk, World

__all__ = [
    "EVENT_LOG_FORMAT_VERSION",
    "PCT",
    "TAPE_FORMAT_VERSION",
    "BoundaryEntry",
    "BoundaryStatus",
    "CorpusEntry",
    "Draw",
    "EscapedSimulationError",
    "EventLog",
    "Exploration",
    "Failure",
    "Host",
    "InterleavingCorpus",
    "InvariantViolation",
    "MisalignmentPolicy",
    "RandomWalk",
    "RunResult",
    "Settings",
    "ShrinkResult",
    "SimDatagramTransport",
    "SimDeadlockError",
    "SimDisk",
    "SimLivelockError",
    "SimLoop",
    "SimNetwork",
    "SimServer",
    "SimTransport",
    "SimloomError",
    "SimloomNondeterminismError",
    "SimloomTestFailure",
    "SoakReport",
    "SystematicResult",
    "Tape",
    "TapeMisalignmentError",
    "UnhandledExceptionError",
    "World",
    "__version__",
    "always",
    "boundary",
    "draw",
    "eventually",
    "explore",
    "explore_systematic",
    "fingerprint",
    "interleaving_edges",
    "is_pinned",
    "leads_to",
    "pin_hashseed",
    "reached",
    "replay",
    "run",
    "shrink",
    "soak",
    "sometimes",
    "test",
]
