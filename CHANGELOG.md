# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
