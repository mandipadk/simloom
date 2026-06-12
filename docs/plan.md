# simloom — canonical plan

Deterministic simulation testing (DST) for Python's asyncio ecosystem: run an
*unmodified* asyncio application inside a fully simulated world (virtual clock, simulated
network/disk/DNS, seeded scheduler), explore thousands of interleavings with fault
injection, and reduce every failure to a 100%-reproducible, automatically shrunk seed.

Source of truth for decisions: `DIRECTIVES.md`. Background and full rationale live in the
project brief (internal, untracked).

## Problems (the hard catalog)

1. **Cross-process determinism needs `PYTHONHASHSEED`** — set/dict iteration is
   hash-dependent across processes. Harness must detect/pin it, loudly.
2. **Crash semantics vs. cancellation** — `task.cancel()` is graceful (`finally` runs).
   Real crash = stop scheduling forever, hold strong refs to abandoned coroutines until
   end-of-run teardown, close them deterministically after the simulated universe ends.
3. **GC nondeterminism** — `gc.disable()` in-sim; `gc.collect()` at tape-chosen points.
4. **Direct time access** — `time.time()` bypasses loop clock; opt-in stdlib-time
   patching + escape detection.
5. **`random` in user code** — tape-seeded patching of the global instance by default.
6. **Executors** — `run_in_executor` runs inline at a tape-chosen point; blocking real
   I/O in an executor is an escape.
7. **Libraries that don't bottom out in the loop** (psycopg2, requests, grpc C core) —
   documented boundary; users mock at the client edge or use asyncio-native alternatives.
8. **Real servers can't run in-sim** — Python stand-ins (`sim-redis`, `sim-s3`) later.
9. **Hot loops without `await`** — wall-clock watchdog dumps the stuck frame.
10. **uvloop** — tests must run on SimLoop; document.
11. **Schedule-space explosion** — explorer ladder: random walk → PCT → coverage-guided.

## Pillars (architecture, dependency-ordered)

1. **Choice tape** — single seeded source of all nondeterminism; labeled draws;
   replayable; shrinkable as a sequence (Hypothesis-conjecture-style). The keystone.
2. **SimLoop** — deterministic event loop: tape-picked ready callback, virtual clock,
   owned `call_soon`/`call_at`/executor/DNS, escape detection.
3. **SimWorld** — hosts (own clock, durable disk, crash/restart), in-memory transports
   behind `create_connection`/`create_server`, honest-fsync disk, SimDNS.
4. **Fault injector** — partitions, latency, drop/dup/reorder, resets, crash/restart,
   clock skew, disk faults; `sometimes()` buggify + sometimes-assertions.
5. **Oracles** — invariants, `world.until()` liveness, quiescence/deadlock detection.
6. **Explorer** — random walk → PCT → coverage-guided (`sys.monitoring` fingerprints).
7. **Shrinker + reporting** — shrink the tape; artifacts: seed, shrunk tape, versioned
   JSONL event log (public format — Pick 2 consumes it).

## Phases

### Phase 0 — Spikes (de-risk the premise)

Each spike: standalone script under `spikes/` + findings section in `spikes/FINDINGS.md`.

- [x] **S1** Seeded scheduler: two seeds → two interleavings; same seed → byte-identical
      event log.
- [x] **S2** Virtual clock: 1 simulated hour of sleep traffic in <1s wall.
- [x] **S3** **Load-bearing:** unmodified `httpx` ↔ `aiohttp` in-sim via loop-level
      transports.
- [x] **S4** Tape replay equality under faults (drop/delay decisions on the tape).
- [x] **S5** Seeded toy race found by random exploration; manual shrink proves the
      artifact story.

**Gate:** all five pass → `spikes/FINDINGS.md` complete, package scaffolded, D1–D8
locked. Then Phase A. ✅ **Gate met 2026-06-12.**

Progress notes:
- 2026-06-12: Project started. Name locked (simloom), D1–D9 ratified, repo scaffolded.
- 2026-06-12: **All five spikes pass; Phase 0 complete in one session.** Evidence and
  design lessons in `spikes/FINDINGS.md` — notably: stream FIFO must be a network-layer
  invariant (the scheduler reordered TCP bytes until it was); task labeling must be
  loop-owned (asyncio's global `Task-N` counter leaks); schedule-tape shrinking
  minimizes FIFO deviations, not length (12 → 1 on the S5 race). The S3 loop-surface
  list is Phase A's implementation checklist.

### Phase A — SimLoop + tape, production quality

Loop, virtual time, tape, replay, escape detection, event log v1.
**Gate:** 10k-seed determinism torture (replay hash equality) in CI.

Progress notes:
- 2026-06-12: **Phase A core shipped.** `src/simloom/`: choice tape (labeled bounded
  draws, strict/fallback replay, versioned serialization), event log v1 (canonical
  JSONL + digest; `docs/event-log.md`), SimLoop (tape-driven scheduling, virtual
  clock, loop-owned task naming, escape detection across the full real-world API
  surface, inline executor, controlled GC, ordered asyncgen shutdown, deadlock
  reporting with task dump), `run()`/`replay()` with asyncio.run-parity deterministic
  teardown and an unhandled-exception policy. 88 tests incl. hypothesis properties;
  warnings-as-errors; mypy --strict clean. `docs/determinism.md` v1 states the honest
  boundary. Torture: every seed runs twice + replays once, digests must match;
  CI job at 10k seeds (also verified locally).
- Design notes for later phases: replay STRICT now also fails if a run finishes
  without consuming the whole tape (silent-divergence case); after a misalignment
  error the tape force-falls-back so teardown stays deterministic; task collections
  sort by (label, creation order) because label-only ties would fall back to
  address-ordered set iteration.

### Phase B — SimWorld

Hosts, network transports, DNS, `asyncpg`/`httpx`/`aiohttp` compat tests, crash/restart
semantics (problem #2), disk with fsync honesty.
**Gate:** demo #2 (real client/server with injected packet loss) runs as a test.

Progress notes:
- 2026-06-12: **Phase B core shipped; gate met.** `_net.py`: in-memory transports
  (per-direction FIFO arrival, pause/resume, eof/close, peer-reset on crash),
  SimServer, strict SimDNS, per-chunk latency + loss-as-retransmit-delay drawn from
  the tape. `_world.py`: World (passed by arity to `main`), Host (contextvar task
  ownership, crash = power cut with parked wakeups + post-universe revive-and-cancel,
  restart from entry factories), SimDisk (buffered/synced, honest fsync, durable
  deletes). Gate test `tests/test_world_http.py`: unmodified aiohttp + httpx exchange
  requests over the simulated network with 20% loss and 1–30ms latency, replay
  hash-identical. World torture added to the CI determinism job. 113 tests green.
- Hard-won correctness notes: (1) crashed-task handles must be *parked*, not
  dropped — a task whose awaited future completed during the crash window can
  otherwise never be stepped again, not even to deliver teardown cancellation;
  (2) transports must tolerate destructor-time close after the loop closed
  (leaked StreamWriter `__del__`); (3) the host contextvar is set inside the
  task's own context and never reset (reset tokens are invalid when a crashed
  task's coroutine is closed from another context).
- Deferred within Phase B scope: `asyncpg` compat needs a Postgres stand-in
  (Phase C+ stand-in library); plain `call_soon` callbacks from host code are
  not crash-filtered yet (documented in determinism.md).

### Phase C — Faults + buggify

Full injector matrix, `sometimes()`, deadlock/quiescence oracle.
**Gate:** toy Raft torture suite finds seeded-in bugs (demo #3).

### Phase D — Explorer + shrinker

Random walk → parallel runner → shrinking → PCT.
**Gate:** shrunk repros human-readable; PCT beats random walk on the benchmark bug zoo
(measured — also launch content).

Progress notes:
- 2026-06-12: **Phase D core shipped; gate met.** Pluggable schedulers (`_sched.py`):
  RandomWalk (default) and PCT (priorities + d-1 change points, all tape-drawn, so PCT
  universes replay/shrink like any other; RunResult records the strategy and replay
  auto-matches it). Explorer (`simloom.explore`): serial + ProcessPool fan-out, failure
  list, first-failure artifact re-run locally, corpus coverage union. Shrinker
  (`simloom.shrink`): chunked deletion, block zeroing, value descent; every accepted
  candidate re-recorded; budgeted; `describe()` prints the FIFO-deviation story.
- Two findings that shaped the design: (1) the minimization order is
  **(deviations, length, values)** — deviations-first, a real divergence from
  Hypothesis shortlex, because a schedule's length is intrinsic to the program;
  (2) the shrinker's fallback refill must be **zeros** (canonical FIFO completion),
  not a PRNG — random refill made most improving candidates look worse and stalled
  shrinking at 18+ deviations; with zero-refill the check_then_set race shrinks
  25 -> 1 deviations in ~100 candidate runs.
- Gate measurements (`tests/test_zoo.py`, deterministic, re-verified in CI; N=400
  local benchmark): random walk vs pct:d=2,k=64 — shallow_race 395 vs 44,
  check_then_set 173 vs 0, deep_ordering 127 vs 8, **starvation 0 vs 151**. The
  strategies are complementary, exactly as the PCT paper claims: random dominates
  shallow races; PCT owns the priority/starvation class random can essentially
  never hit (a 2^-14 streak). Honest both ways — this is launch content.
- PCT horizon caveat: k must approximate the run's step count (default 4096 is far
  too large for small tests and degrades PCT to a fixed priority schedule); the
  explorer should auto-tune k from a probe run — noted for Phase E polish.

### Phase E — pytest plugin, docs, launch

`@sim.test`, failure UX, `docs/determinism.md`, examples, OSS-bug reproduction hunt
(demo #4), launch post.

## Validation strategy

Everything local. CI enforces the determinism claim itself: a dedicated job re-runs the
corpus under many seeds and asserts replay hash-equality. Hypothesis-test the
tape/shrinker. Maintain a benchmark bug zoo (known races, deadlocks, crash-recovery
bugs); track find-rate per explorer strategy.
