# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
