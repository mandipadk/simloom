# DIRECTIVES

Locked owner decisions. Where any other document or plan is ambiguous, this file wins.
Changes here require explicit owner sign-off; record the date.

## Session 1 — 2026-06-12

| # | Decision | Locked value |
|---|----------|--------------|
| D0 | Name | **simloom** (PyPI free, no significant GitHub collisions as of 2026-06-12). Directory rename `dst-python/` → `simloom/` pending. |
| D1 | Min Python | **3.12+**. `sys.monitoring`, eager task factory control, modern asyncio internals. No 3.11 compat tax. |
| D2 | Scope | **asyncio only.** No Trio, no threads-first, no multiprocessing. Deterministic-executor shim for incidental threads only. |
| D3 | Randomness | **Single choice tape, Hypothesis-conjecture-style.** All nondeterminism flows through one seeded, labeled, replayable, shrinkable source. No second randomness source, ever. |
| D4 | Interposition | **Loop-level only for v1.** No CPython patching, no socket monkeypatching, no ptrace. C-extension I/O is a documented boundary with escape detection. |
| D5 | Packaging | **Core library + thin pytest plugin shipped together** from day 1. Core importable without pytest. |
| D6 | Honesty doc | `docs/determinism.md` states exactly what is and isn't deterministic and what escapes the sim. Disclosed limitations, not marketing. |
| D7 | License / infra | **Apache-2.0**, `uv`, mypy `--strict`, ruff, hypothesis in the test suite. Keep-a-Changelog + SemVer. GitHub releases with wheel + sdist attached. |
| D8 | Out of scope v1 | Free-threaded CPython (nogil), multiprocessing, linearizability checking, non-asyncio frameworks, real-subprocess simulation. Consciously deferred, not forgotten. |
| D9 | Visibility | **Private until Phase A gate passes**, then revisit. Local git from day 1. |

## Standing working agreements (carried from colabctl)

- Quality bar: no cut corners. Strict typing on `src/`, ruff clean, hermetic offline tests.
- Git: commit freely with clean messages; **never push until told.** Pre-push audit:
  no personal paths, no ephemeral identifiers, no internal/meta references, no secrets.
- Canonical plan lives in `docs/plan.md`; update per-phase progress notes in place.
- Spikes live in `spikes/` with runbooks + findings docs; validation gates between phases.
