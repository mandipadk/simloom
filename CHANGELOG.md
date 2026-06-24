# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Phase I (1/5) — `connected_pair`.** `world.connected_pair(client_factory,
  server_factory)` drives any two `asyncio.Protocol`s against each other over a
  two-sided simulated connection — no listener, no hand-written stub transport.
  Both `connection_made` callbacks run before it returns, and the pair carries
  `world.net` faults (latency/loss/partition/reset) like a real connection.
  `create_connection` is refactored onto it (behaviour byte-identical).
- **Systematic deep search — a stateless model checker over the choice tape.**
  `simloom.explore_systematic(main, max_delays=2)` enumerates every distinct
  interleaving within a *delay bound* exactly once (a mixed-radix odometer over
  the tape, reusing FALLBACK replay). Two things random walk and PCT cannot do:
  it finds a bug needing a specific non-default interleaving *deterministically*
  (e.g. a check-then-set race the default schedule never hits), and it *proves
  correctness* — `result.proven_correct` is True when it exhausts the space with
  no failure. Delay bounding (Emmi/Qadeer/Rakamarić) keeps it tractable without
  the happens-before tracking full partial-order reduction needs; found failures
  replay and shrink like any tape. Available as `@simloom.test(systematic=True,
  max_delays=2)` — bounded verification as an ordinary pytest test: the test
  passes as a proof up to the bound, or fails deterministically with a shrunk,
  replayable witness interleaving.
- **Phase H (3/3) — soak testing.** `simloom.soak(main, count=, shards=, shard=,
  checkpoint=)` runs continuous, shardable, resumable exploration. Sharding is by
  stride, so shards are disjoint and complete by construction; the checkpoint
  records each shard's cursor (atomic write), so a killed soak resumes from its
  next unrun seed — none skipped, none repeated.
- **Phase H (2/4) — interleaving fingerprints + regression corpus.**
  `interleaving_edges`/`fingerprint` summarise a run by its `(prev_callback,
  callback)` step edges; `InterleavingCorpus` accumulates the global edge set
  and keeps the tapes that first reached new edges. This is the feedback
  substrate for coverage-guided search: sparse novelty (most runs add no edges)
  is the steering signal.
- **Phase H (1/4) — PCT auto-horizon.** `scheduler="pct:auto"` measures the
  scheduling horizon `k` from a probe run instead of the broken `k=4096`
  default (whose change points land past a small test's end and never fire,
  degrading PCT to a fixed priority schedule). pct:auto finds the
  ordering/starvation bugs the default misses; the resolved descriptor is
  concrete and replayable.
- **Phase G (1/2) — the determinism boundary.** Opt-in `virtual_time=True`
  redirects `time.time`/`monotonic`/`perf_counter` (+`_ns`) to the virtual
  clock; `seed_randomness=True` seeds the global `random`, `os.urandom`,
  `secrets`/`SystemRandom`, and `uuid.uuid4` from one tape draw. Both default
  ON under `@simloom.test` and are always restored (even on a crashed run).
  An unmodified asyncio library that reads the clock and rolls randomness now
  replays byte-for-byte.
- **Phase G (2/2) — per-test determinism self-check.** `run(check_determinism=
  True)` / `@simloom.test(check_determinism=True)` / `pytest --simloom-check-
  determinism` run a seed twice and raise `SimloomNondeterminismError` locating
  the first diverging event if the two universes differ — catching
  nondeterminism the tape cannot control (identity-ordered iteration, a stray
  real clock/RNG, threads doing real work).
- **Phase G (3/3) — boundary registry + PYTHONHASHSEED auto-pin.**
  `simloom.boundary()` is a machine-readable table of every real-world API and
  what simloom does there (detected / simulated / patched / documented); a test
  cross-checks it against the actual escape sites so the honesty contract cannot
  drift. `simloom.pin_hashseed()` (and `pytest --simloom-pin-hashseed`) re-execs
  with `PYTHONHASHSEED=0` if unpinned, so cross-process seed/tape replay is sound.
- **Phase F — property monitors (oracles).** `world.always` / `world.eventually`
  / `world.leads_to` (and module-level `simloom.always`/`eventually`/`leads_to`)
  assert safety and liveness properties over the deterministic step sequence;
  `world.assert_converged` checks replica equality. A new livelock oracle
  (`SimLivelockError`) catches busy-but-stuck spins the deadlock oracle cannot
  see. Violations surface as `InvariantViolation` and are found/shrunk/replayed
  like any failure. Passing monitors are zero-perturbation (byte-identical
  digest); liveness deadlines fire at exactly their virtual time and never mask
  a `SimDeadlockError`. This turns simloom from "finds crashes" into "finds
  wrong and stuck behaviour."

## [0.1.0] - 2026-06-13

First public release. The deterministic core, simulated world, fault matrix,
explorer, shrinker, and pytest plugin are all present and exercised by a
10,000-seed determinism torture in CI.

### Added
- Project scaffold: `src/` layout, strict mypy, ruff, pytest + hypothesis, CI.
- Phase 0 feasibility spikes under `spikes/`.
- **Phase A core**: the choice tape (`Tape`, labeled bounded draws, strict/fallback
  replay, versioned JSON serialization), the deterministic event loop (`SimLoop`)
  with virtual time, loop-owned task naming, escape detection
  (`EscapedSimulationError`), deadlock reporting (`SimDeadlockError`), controlled
  GC, and deterministic teardown; `run()`/`replay()` entry points returning
  `RunResult` with a sha256 universe digest; event log format v1
  (`docs/event-log.md`); honesty doc (`docs/determinism.md`); 10,000-seed
  determinism-torture CI job.
- **Phase B world**: simulated network behind the loop primitives (in-memory
  transports with FIFO stream guarantees, `SimServer`, strict `SimDNS`, tape-driven
  latency and loss-as-retransmission-delay), `World`/`Host` with power-cut crash
  semantics and entry-factory restart, `SimDisk` with honest fsync. Gate: unmodified
  aiohttp + httpx exchanging requests over the lossy simulated network, replayable
  byte-for-byte (`tests/test_world_http.py`).
- **Phase C faults + buggify**: partition/heal (hold-and-release, streams never
  corrupt), asymmetric block, connection-reset injection, partition-aware connects,
  torn/lost/flushed unsynced writes on crash; `simloom.sometimes`/`draw`/`reached`
  with `RunResult.coverage`. Gate: the toy Raft torture suite
  (`tests/test_raft_demo.py`) catches a planted double-vote bug via exploration and
  replays it exactly; the correct implementation survives.
- **Phase D explorer + shrinker**: `simloom.explore` (random walk or PCT scheduling,
  serial or multiprocess, failure corpus + coverage union), `simloom.shrink`
  (deviations-first greedy minimization with re-recorded candidates and zero-refill
  fallback; human-readable `describe()`), `PCT`/`RandomWalk` strategies with
  replay-matched recording. Gate: the benchmark bug zoo measures find rates — PCT
  finds the starvation-class bug a random walk never does (0 vs 151 of 400 seeds),
  and random dominates shallow races; both results asserted in CI.
- **Phase E pytest plugin**: `@simloom.test(runs=N)` turns an async test into a
  seed-exploring simulation test — failures are shrunk, written to `.sim/failures/`
  as replayable artifacts, and reported with seed + re-run command + minimal
  schedule. Options: `--simloom-seed`, `--simloom-runs`, `--simloom-tape`,
  `--simloom-no-shrink`; `require_coverage` for per-test sometimes-assertions.
- **Examples + demo #4**: `examples/toy_raft.py` (runnable) and
  `examples/bpo42130.py` — the historical `asyncio.wait_for` cancellation-swallowing
  race (bpo-42130) reproduced deterministically from a seed; the modern stdlib
  survives the identical torture. Launch-post draft in `docs/launch-post.md`.

[Unreleased]: https://github.com/mandipadk/simloom/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mandipadk/simloom/releases/tag/v0.1.0
