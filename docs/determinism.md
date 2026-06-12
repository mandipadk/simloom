# What simloom does and does not guarantee

This document is the honest boundary of the simulation. It will always state
exactly what is deterministic, what escapes, and what we detect versus what
we merely document. Status: **Phase A** — the deterministic loop and tape
exist; the simulated world (network, hosts, disk) does not yet.

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
  plugin (Phase E) will enforce it.
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

## What escapes — detected

These raise `EscapedSimulationError` at the call site, with the API named:

- file-descriptor readiness: `add_reader`, `add_writer`, …
- raw sockets: `sock_connect`, `sock_recv`, `sock_sendall`, …
- network/DNS (until SimWorld lands in Phase B): `create_connection`,
  `create_server`, `create_datagram_endpoint`, `getaddrinfo`, `start_tls`, …
- pipes and subprocesses: `connect_read_pipe`, `subprocess_exec`, …
- OS signals: `add_signal_handler`, …
- cross-thread injection: `call_soon_threadsafe` from a foreign thread.

Because the high-level asyncio ecosystem (streams, `aiohttp`, `httpx`,
`asyncpg`, …) bottoms out in these loop primitives, escape detection turns
"mystery nondeterminism" into an immediate, located error.

## What escapes — documented, not (yet) detected

- **Direct time access.** `time.time()`, `time.monotonic()`,
  `datetime.now()` bypass the loop clock and return real wall time. Planned:
  opt-in stdlib-time patching. Until then: anything reading the wall clock
  sees the real world (e.g. HTTP `Date` headers built from `time.time()`).
- **`random` in user code.** The global `random` module is not seeded by the
  tape (planned as a default-on convenience in the pytest plugin). Seed your
  own RNGs, or results will vary while *scheduling* stays deterministic.
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
