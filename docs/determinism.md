# What simloom does and does not guarantee

This document is the honest boundary of the simulation. It will always state
exactly what is deterministic, what escapes, and what we detect versus what
we merely document. Status: **Phase F** — the deterministic loop, tape,
simulated world, the fault matrix (partitions, asymmetric blocks, resets,
crashes with torn writes, latency/loss), buggify, and the Phase D explorer
(random walk + PCT, serial or multiprocess), tape shrinker, the pytest plugin
(`@simloom.test`), and the **property monitors** (`always`/`eventually`/
`leads_to`, the livelock oracle, convergence) exist.

Explorer/shrinker notes: PCT scheduling draws its priorities and change
points from the tape, so PCT universes replay and shrink like any other.
The multiprocess explorer reports failing seeds from workers and re-runs
the first one locally, so the returned artifact reproduces in your process
regardless of PYTHONHASHSEED; pin it anyway for cross-process digest
comparisons.

## The guarantee

Given the same tape (or the same seed on the same build):

- the scheduler makes identical decisions,
- virtual time advances identically,
- the event log is **byte-identical** (equal sha256 digests),
- the program observes the same world and computes the same results.

This holds because every source of scheduling nondeterminism flows through
one seeded choice tape, the clock is virtual, the garbage collector runs at
fixed step intervals (not allocation-triggered moments), task naming is
loop-owned, and anything that would touch the real world raises
`EscapedSimulationError` instead.

Enforced in CI: 10,000 seeds, each run twice and replayed once, all three
event logs hash-identical (`tests/test_determinism.py`).

## Scope of the guarantee

**Within one process, on one build: unconditional** (modulo the escapes
below).

**Across processes and machines, additionally required:**

- `PYTHONHASHSEED` must be pinned (e.g. `PYTHONHASHSEED=0`). Set and dict
  iteration order for hash-randomized types differs per process otherwise,
  and any program that iterates a set of strings makes hash-order-dependent
  decisions. The run header records `hash_randomization_pinned`; the pytest
  plugin warns when replaying a seed or tape without it pinned.
- Iterating sets/dicts keyed by *identity* (default `object.__hash__`) is
  ordered by memory address and is **not reproducible across processes even
  with PYTHONHASHSEED pinned**. simloom avoids this internally (ordered
  registries everywhere); user code that does it will replay within a
  process but may diverge across processes. This is the main residual
  caveat.
- Same CPython feature version recommended. The tape replays *decisions*,
  but which decision points exist depends on asyncio internals
  (e.g. how many callbacks a given primitive schedules), which can change
  between Python versions. Tapes record bounds, so cross-version divergence
  is *detected* (strict replay fails loudly), not silent.

## The simulated world

When the program under test accepts a ``World`` parameter, the network is
simulated: ``create_connection``, ``create_server``, and ``getaddrinfo``
route through in-memory transports instead of escaping. What that means,
honestly:

- **Streams never corrupt.** Packet "loss" on a stream connection is
  modeled the way an application actually observes TCP loss: as
  retransmission delay (one extra round trip per lost segment). Bytes are
  never dropped, duplicated, or reordered within a direction. The same
  honesty governs partitions: chunks sent across a partition are *held*
  (TCP retransmits until the link heals) and delivered in order on
  ``heal()`` — losing mid-stream bytes is something real TCP can't do.
  Duplicate/reorder faults belong to datagram transports, which don't exist
  yet.
- **The fault matrix** (`world.net`): `partition(a, b)` / `heal()`,
  asymmetric `block(src, dst)` / `unblock`, `reset_connections(a, b)`
  (ConnectionResetError on live connections), `set_latency`, `set_loss`.
  A connect across a partition hangs — real SYN behavior — until a caller
  timeout fires or the deadlock oracle reports it.
- **Crashes tear writes.** On `host.crash()`, each buffered (unsynced)
  write independently turns out lost, torn (a prefix reached the platter),
  or flushed — drawn from the tape. Only `fsync()` is a promise.
- **Buggify**: `simloom.sometimes(label, percent)` is tape-drawn inside a
  simulation and constant-False outside one; `simloom.draw(label, bound)`
  gives the program under test replayable randomness; `simloom.reached`
  feeds `RunResult.coverage` so a corpus can assert its fault paths were
  actually exercised.
- **Clock skew is deferred**: `loop.time()` is monotonic, and real skew
  bugs live in wall-clock comparisons (`time.time()`), which simloom does
  not intercept yet (see below). Skew lands with stdlib-time patching.
- **TLS is not simulated** — passing ``ssl=`` raises ``EscapedSimulationError``.
  Serve plain inside the sim.
- **DNS is simulated and strict**: names are registered when a server binds
  to them (or explicitly via ``world.net.dns.register``); unknown names
  raise ``socket.gaierror`` like NXDOMAIN.

**Crash semantics.** ``host.crash()`` is a power cut, not a shutdown:

- The host's tasks are never scheduled again. No ``CancelledError``, no
  ``finally`` blocks, no ``__aexit__`` runs *during the simulation* — that
  would be a graceful shutdown, which is exactly what a crash is not.
- The loop holds strong references to the abandoned tasks (so GC cannot run
  their cleanup mid-run) and parks their pending wakeups. After the
  simulated universe ends, teardown revives and cancels them at a
  deterministic point; their cleanup runs then, where it can no longer
  affect the simulation.
- Unsynced disk writes are lost (``host.disk`` has honest fsync semantics);
  peers of open connections observe ``ConnectionResetError``; the host's
  listeners stop accepting.
- Known limitation: plain ``call_soon`` callbacks scheduled by host code
  before the crash (not bound to one of its tasks) may still run — only
  task wakeups are crash-filtered today.
- ``host.disk`` is an explicit API. Real file I/O (``open()``) bypasses the
  simulation undetectably and is not crash-consistent.

## Systematic exploration (bounded verification)

`simloom.explore_systematic(main, max_delays=N)` is a stateless model checker
over the choice tape: it enumerates every distinct interleaving within `N`
*delays* (non-default scheduling choices) exactly once. Unlike the sampling
strategies it is **exhaustive within the bound** — so it finds an interleaving
bug deterministically if one exists in scope, and when it exhausts the space
with no failure (`result.proven_correct`) that is a *proof*: no schedule within
`N` delays of the default fails. It reuses the tape whole (the odometer bumps
draws and replays under FALLBACK), so a witness it finds replays and shrinks
like any other failure. The reduction is delay bounding, not happens-before
partial-order reduction (which would need memory-access tracking simloom does
not do); almost all concurrency bugs surface within two or three delays.

## The boundary registry

`simloom.boundary()` returns the full table this document describes — every real-world API and its status (`detected` / `simulated` / `patched` / `documented`). A test cross-checks it against the actual escape sites in `SimLoop`, so the boundary cannot silently drift between the code and the docs. `simloom.lookup(api)` queries a single entry.

## The determinism self-check

`run(check_determinism=True)` (and `@simloom.test(check_determinism=True)`, or `pytest --simloom-check-determinism`) runs a seed twice and raises `SimloomNondeterminismError`, locating the first diverging event, if the two event logs differ. This is the standing guard that a *user's* test has not smuggled in nondeterminism the tape cannot control — identity-ordered set/dict iteration, a real clock/RNG not routed through the patches above, or threads doing real work. The clock/random patches close the common cases; the self-check catches the rest.

## Property monitors (Phase F)

`world.always(label, pred)` / `world.eventually(label, pred, within=)` /
`world.leads_to(label, trigger, response, within=)` (also available as module-level
`simloom.always`/`eventually`/`leads_to`) assert safety and liveness properties
over the deterministic step sequence:

- Predicates are **pure** `Callable[[], bool]`: synchronous, no `await`, and no
  tape draw (`simloom.draw`/`sometimes` raise if called from a predicate). They
  are evaluated *between* steps, so they never perturb scheduling.
- A **passing** monitor is zero-perturbation: the event log (and digest) is
  byte-identical to a run with no monitor. Only a violation emits an `invariant`
  event and stops the run (as an `InvariantViolation`, which is found, shrunk,
  and replayed like any other failure).
- A liveness deadline fires at *exactly* its virtual time; a monitor never masks
  a genuine `SimDeadlockError` (quiescence is reported in preference to a pending
  deadline). An `eventually` goal unmet when the run ends is a violation.
- A separate **livelock** detector (`SimLivelockError`) catches a busy spin that
  never advances the virtual clock (configurable via `max_steps_per_instant`),
  which the quiescence-based deadlock oracle structurally cannot see.

## What escapes — detected

These raise `EscapedSimulationError` at the call site, with the API named:

- file-descriptor readiness: `add_reader`, `add_writer`, …
- raw sockets: `sock_connect`, `sock_recv`, `sock_sendall`, …
- network/DNS when no World is in play (and `create_datagram_endpoint`,
  `start_tls`, TLS connects always): the simulated network only exists
  when the program under test accepts a `World`, …
- pipes and subprocesses: `connect_read_pipe`, `subprocess_exec`, …
- OS signals: `add_signal_handler`, …
- cross-thread injection: `call_soon_threadsafe` from a foreign thread.

Because the high-level asyncio ecosystem (streams, `aiohttp`, `httpx`,
`asyncpg`, …) bottoms out in these loop primitives, escape detection turns
"mystery nondeterminism" into an immediate, located error.

## What escapes — documented, not (yet) detected

- **Direct time access.** `time.time()`/`monotonic()`/`perf_counter()` (and
  `_ns` variants) are redirected to the virtual clock when a run passes
  `virtual_time=True` (default on under `@simloom.test`). Residual escape:
  a local alias bound before the patch (`from time import monotonic as _m`)
  and the C-accelerated `datetime.now()` are not redirected — a documented
  gap until a module-walking patcher lands.
- **`random` in user code.** With `seed_randomness=True` (default on under
  `@simloom.test`) the global `random`, `os.urandom`, `random._urandom`
  (so `secrets`/`SystemRandom`), and `uuid.uuid4` are seeded from one tape
  draw (`entropy.seed`) at run start, so library jitter replays. A
  user-supplied `Random()` instance is still theirs to seed.
- **Blocking calls.** `time.sleep()`, blocking file I/O, `requests`, C
  extensions doing their own I/O (`psycopg2`, `grpc`'s C core): these block
  the loop thread for real and exit the simulation invisibly. Cooperative
  scheduling cannot preempt them. Mock at the client boundary or use the
  asyncio-native alternative (`asyncpg`, `httpx`, `grpclib`).
- **`run_in_executor`** runs the function *inline* at a tape-chosen point.
  CPU-bound work is fine; blocking real I/O inside an executor function is
  the previous bullet.
- **Hot loops without `await`.** A `while True: pass` can never be
  preempted; virtual time never advances. The opt-in wall-clock watchdog
  (`run(..., watchdog=seconds)`) dumps all stacks and exits the process.
- **Real servers and external processes** cannot run inside the simulation.
  The in-sim story (Phase B+) is Python talking to Python, with simulated
  stand-ins for external infrastructure.

## Internal discipline (how the harness keeps itself honest)

- One randomness source: the tape. A second PRNG anywhere in simloom is a
  bug by definition (D3 in DIRECTIVES.md).
- Event-log fields must be address-free, wall-clock-free, and
  counter-stable; `docs/event-log.md` states the rules.
- GC is disabled during runs and collected every 1009 steps and at teardown,
  so finalizers and weakref callbacks land deterministically.
- Teardown is deterministic: leftover tasks are cancelled in (label,
  creation-order) sequence, async generators close in first-iteration order,
  and a final collect + drain runs finalizer-scheduled callbacks.
- The CI torture job re-proves all of this on every push.
