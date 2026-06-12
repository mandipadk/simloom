# simloom — canonical plan

Deterministic simulation testing (DST) for Python's asyncio ecosystem: run an
*unmodified* asyncio application inside a fully simulated world (virtual clock, simulated
network/disk/DNS, seeded scheduler), explore thousands of interleavings with fault
injection, and reduce every failure to a 100%-reproducible, automatically shrunk seed.

Source of truth for decisions: `DIRECTIVES.md`. Background and full rationale: `BRIEF.md`.

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

### Phase B — SimWorld

Hosts, network transports, DNS, `asyncpg`/`httpx`/`aiohttp` compat tests, crash/restart
semantics (problem #2), disk with fsync honesty.
**Gate:** demo #2 (real client/server with injected packet loss) runs as a test.

### Phase C — Faults + buggify

Full injector matrix, `sometimes()`, deadlock/quiescence oracle.
**Gate:** toy Raft torture suite finds seeded-in bugs (demo #3).

### Phase D — Explorer + shrinker

Random walk → parallel runner → shrinking → PCT.
**Gate:** shrunk repros human-readable; PCT beats random walk on the benchmark bug zoo
(measured — also launch content).

### Phase E — pytest plugin, docs, launch

`@sim.test`, failure UX, `docs/determinism.md`, examples, OSS-bug reproduction hunt
(demo #4), launch post.

## Validation strategy

Everything local. CI enforces the determinism claim itself: a dedicated job re-runs the
corpus under many seeds and asserts replay hash-equality. Hypothesis-test the
tape/shrinker. Maintain a benchmark bug zoo (known races, deadlocks, crash-recovery
bugs); track find-rate per explorer strategy.
