"""The boundary registry: a single machine-readable table of every place the
simulation meets reality, and what simloom does there.

simloom's credibility rests on an honest, complete account of its boundary
(``docs/determinism.md``). This registry makes that account *queryable and
enforceable* rather than prose: each real-world API has a status —

- ``DETECTED``  — calling it from inside the sim raises ``EscapedSimulationError``
  at the call site (no silent nondeterminism).
- ``SIMULATED`` — fully modelled in-sim (e.g. the network when a ``World`` is
  used, the virtual clock, the disk).
- ``PATCHED``   — redirected to the deterministic source when the opt-in
  patches are on (``virtual_time`` / ``seed_randomness``).
- ``DOCUMENTED``— a known escape that is not yet handled; using it leaks real
  nondetermism and is the user's responsibility (the determinism self-check is
  the backstop).

A test cross-checks that every ``DETECTED`` entry corresponds to a real escape
site in ``SimLoop`` and vice versa, so the contract cannot silently drift.
``simloom.boundary()`` returns the table for users and tooling.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass


class BoundaryStatus(enum.Enum):
    DETECTED = "detected"
    SIMULATED = "simulated"
    PATCHED = "patched"
    DOCUMENTED = "documented"


@dataclass(frozen=True, slots=True)
class BoundaryEntry:
    api: str
    status: BoundaryStatus
    note: str


_D = BoundaryStatus.DETECTED
_S = BoundaryStatus.SIMULATED
_P = BoundaryStatus.PATCHED
_DOC = BoundaryStatus.DOCUMENTED

_REGISTRY: tuple[BoundaryEntry, ...] = (
    # --- DETECTED: raise EscapedSimulationError at the call site ----------
    BoundaryEntry("loop.getaddrinfo", _D, "real DNS; simulated only with a World (SimDNS)"),
    BoundaryEntry("loop.getnameinfo", _D, "real reverse DNS"),
    BoundaryEntry("loop.create_connection", _D, "real socket; simulated with a World"),
    BoundaryEntry("loop.create_server", _D, "real listener; simulated with a World"),
    BoundaryEntry("loop.create_datagram_endpoint", _D, "real UDP; simulated with a World"),
    BoundaryEntry("loop.create_unix_connection", _D, "real unix socket"),
    BoundaryEntry("loop.create_unix_server", _D, "real unix listener"),
    BoundaryEntry("loop.start_tls", _D, "TLS not simulated yet"),
    BoundaryEntry("loop.create_connection(ssl=...)", _D, "TLS not simulated yet"),
    BoundaryEntry("loop.create_server(ssl=...)", _D, "TLS not simulated yet"),
    BoundaryEntry("loop.sendfile", _D, "real file/socket sendfile"),
    BoundaryEntry("loop.sock_recv", _D, "raw socket I/O"),
    BoundaryEntry("loop.sock_recv_into", _D, "raw socket I/O"),
    BoundaryEntry("loop.sock_recvfrom", _D, "raw socket I/O"),
    BoundaryEntry("loop.sock_recvfrom_into", _D, "raw socket I/O"),
    BoundaryEntry("loop.sock_sendall", _D, "raw socket I/O"),
    BoundaryEntry("loop.sock_sendto", _D, "raw socket I/O"),
    BoundaryEntry("loop.sock_connect", _D, "raw socket I/O"),
    BoundaryEntry("loop.sock_accept", _D, "raw socket I/O"),
    BoundaryEntry("loop.sock_sendfile", _D, "raw socket I/O"),
    BoundaryEntry("loop.add_reader", _D, "file-descriptor readiness needs a real selector"),
    BoundaryEntry("loop.remove_reader", _D, "file-descriptor readiness"),
    BoundaryEntry("loop.add_writer", _D, "file-descriptor readiness"),
    BoundaryEntry("loop.remove_writer", _D, "file-descriptor readiness"),
    BoundaryEntry("loop.connect_read_pipe", _D, "real pipes live outside the sim"),
    BoundaryEntry("loop.connect_write_pipe", _D, "real pipes live outside the sim"),
    BoundaryEntry("loop.subprocess_shell", _D, "real subprocesses run outside virtual time"),
    BoundaryEntry("loop.subprocess_exec", _D, "real subprocesses run outside virtual time"),
    BoundaryEntry("loop.add_signal_handler", _D, "OS signals arrive nondeterministically"),
    BoundaryEntry("loop.remove_signal_handler", _D, "OS signals"),
    BoundaryEntry("loop.call_soon_threadsafe", _D, "rejected only from a foreign thread"),
    # --- SIMULATED: modelled deterministically in-sim --------------------
    BoundaryEntry("loop.time", _S, "virtual clock; jumps to the next timer"),
    BoundaryEntry("asyncio primitives", _S, "Event/Lock/Condition/Queue/Semaphore/sleep/gather"),
    BoundaryEntry("World.net", _S, "in-memory transports, DNS, latency/loss/partition faults"),
    BoundaryEntry("World.net datagrams", _S, "UDP with real loss/reorder/duplication faults"),
    BoundaryEntry("Host.disk", _S, "honest fsync, torn/lost writes on crash"),
    # --- PATCHED: deterministic when the opt-in patches are on -----------
    BoundaryEntry("time.time", _P, "virtual_time=True -> wall_epoch + loop.time()"),
    BoundaryEntry("time.monotonic", _P, "virtual_time=True -> loop.time()"),
    BoundaryEntry("time.perf_counter", _P, "virtual_time=True -> loop.time()"),
    BoundaryEntry("time.time_ns", _P, "virtual_time=True"),
    BoundaryEntry("time.monotonic_ns", _P, "virtual_time=True"),
    BoundaryEntry("time.perf_counter_ns", _P, "virtual_time=True"),
    BoundaryEntry("random", _P, "seed_randomness=True -> tape-seeded global RNG"),
    BoundaryEntry("os.urandom", _P, "seed_randomness=True (covers uuid.uuid4)"),
    BoundaryEntry("uuid.uuid4", _P, "seed_randomness=True (via os.urandom)"),
    BoundaryEntry("secrets", _P, "seed_randomness=True (via random._urandom)"),
    # --- DOCUMENTED: known, unhandled escapes ----------------------------
    BoundaryEntry("datetime.now", _DOC, "C-accelerated; not redirected by virtual_time yet"),
    BoundaryEntry("from time import <fn>", _DOC, "a pre-bound alias is not redirected"),
    BoundaryEntry("time.sleep", _DOC, "blocking; cannot be preempted (watchdog catches hangs)"),
    BoundaryEntry("blocking file I/O", _DOC, "open()/read() bypass the sim; use Host.disk"),
    BoundaryEntry("C-extension I/O", _DOC, "psycopg2/requests/grpc C core run real I/O"),
    BoundaryEntry("threads doing real work", _DOC, "the self-check is the backstop"),
    BoundaryEntry("set/dict iteration by identity", _DOC, "address-ordered; pin PYTHONHASHSEED"),
)

_BY_API: dict[str, BoundaryEntry] = {e.api: e for e in _REGISTRY}


def boundary() -> tuple[BoundaryEntry, ...]:
    """The full boundary registry — every API and what simloom does there."""
    return _REGISTRY


def lookup(api: str) -> BoundaryEntry | None:
    """The entry for an API name, or None if it is not in the registry."""
    return _BY_API.get(api)


def detected_apis() -> frozenset[str]:
    """Every API that raises ``EscapedSimulationError`` — the enforced set."""
    return frozenset(e.api for e in _REGISTRY if e.status is BoundaryStatus.DETECTED)
