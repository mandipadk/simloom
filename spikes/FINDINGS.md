# Phase 0 spike findings

Date: 2026-06-12. All five spikes pass; the Phase 0 gate is met. Each spike is a
standalone script in this directory; run with `uv run --all-extras python spikes/<name>.py`
(only S3 needs the extras). Every spike exits 0 on pass and asserts its own claims.

**Verdict: the thesis holds.** Loop-level interposition carries real, unmodified
libraries; the choice-tape design delivers replay, editing, and shrinking from one
mechanism. Proceed to Phase A.

## S1 — seeded scheduler (`s1_seeded_scheduler.py`)

A ~150-line event loop (`AbstractEventLoop` subclass) where a seeded RNG picks which
ready callback runs next.

- 20/20 seeds produced **distinct interleavings** of an unmodified racy program
  (3 workers × 3 non-atomic increments straddling an `await`).
- Final counter ranged 3–9; only 1 seed in 20 hit the race-free answer (9).
- Same seed twice → **byte-identical event logs** (sha256-verified), every seed.

Design lessons:
- asyncio's default `Task-N` naming is a **process-global counter** — it leaks
  run-to-run nondeterminism into any log that names tasks. The loop must own task
  naming (and label tasks born outside `loop.create_task` deterministically, e.g. by
  coroutine qualname).
- Event-log entries must be **address-free**: `repr(handle)` contains memory addresses.
- `AbstractEventLoop`'s unimplemented methods raising `NotImplementedError` is a free,
  crude preview of escape detection: nothing can touch a selector or a real socket
  without an immediate loud error.

## S2 — virtual clock (`s2_virtual_clock.py`)

When nothing is ready, jump `loop.time()` to the next timer.

- One simulated hour (50 long-period workers + a 1 Hz ticker; ~7,200 timer events)
  ran in **~19 ms wall — ≈191,000× real time** — and replays identically.
- The timer heap needs an insertion-order tiebreak so equal deadlines stay FIFO.

## S3 — unmodified httpx ↔ aiohttp in-sim (`s3_httpx_aiohttp_insim.py`) — load-bearing

Real `aiohttp` server (`AppRunner`/`TCPSite`) and real `httpx` client exchanged three
HTTP requests **in-process, zero sockets**, over in-memory transports: 81 scheduler
steps, replay byte-identical, different seed → different (still correct) universe with
different virtual timings.

Loop surface the ecosystem actually demanded (discovered empirically — this list is
Phase A's checklist):
- `getaddrinfo` — **anyio passes the host as IDNA-encoded `bytes`**, not `str`.
- `create_connection` / `create_server` (aiohttp passes `ssl`, `backlog`,
  `reuse_address`; the returned server needs `.sockets`, `close()`, `wait_closed()`).
- `get_task_factory`/`set_task_factory` — anyio task groups consult it.
- Transport surface: `write`, `writelines`, `write_eof`/`can_write_eof`,
  `pause_reading`/`resume_reading`/`is_reading`, `close`/`abort`/`is_closing`,
  `get_extra_info` (`peername`, `sockname`, `socket` — aiohttp sets `TCP_NODELAY` on
  it, a fake socket object suffices), `set_write_buffer_limits`,
  `get_write_buffer_size`, `set_protocol`/`get_protocol`.

Design rules discovered (both found by failures, which is what spikes are for):
1. **Stream FIFO is a network invariant, not a scheduler choice.** First
   implementation delivered each written chunk as its own ready callback; the seeded
   scheduler legally reordered bytes within one TCP direction and aiohttp's parser saw
   the POST body as a new request line. Delivery must go through per-direction FIFO
   arrival queues; the scheduler only decides *when* a connection's drain runs.
2. **Task labeling, again**: anyio spawns default-named tasks (`Task-N`), which made
   two identical runs differ only in labels. Same fix as S1.

## S4 — the choice tape (`s4_tape_replay.py`)

`Tape`: a sequence of labeled integer draws — generated from a seeded RNG when
exploring, recorded, replayed, and edited. Scheduler picks, packet drops, and delay
amounts all draw from one tape. Workload: a retry protocol over lossy links (25% drop
each direction).

- Replay with **the PRNG forbidden** (it raises if touched) reproduced the universe
  byte-for-byte: same event log hash, same fault decisions, same draw sequence.
- Editing one draw (`c2s.drop` flipped: a delivered packet becomes dropped) produced a
  **different valid universe** — one more retry, longer virtual duration — with no
  other code change. The tape *is* the universe.
- Misaligned or exhausted replay (possible after edits change control flow) falls back
  to a fixed-seed RNG, Hypothesis-style, so every edited tape executes and is itself a
  well-defined deterministic universe.

## S5 — explore + shrink (`s5_explore_and_shrink.py`)

Four contenders around a check-then-set "lock" with the check and set straddling an
`await`. Exploration over fresh seeds found a mutual-exclusion violation at seed 4;
greedy shrinking (chunked deletion + per-draw value minimization, shortlex-ordered,
re-running after every candidate edit) minimized it; the artifact replays the failure
with randomness forbidden.

- **12 FIFO deviations → 1.** The minimal repro: run everything in arrival order
  except one scheduling pick that takes the second-oldest callback. One perversion
  breaks the lock.
- Shrink-correctness details that will shape the real shrinker:
  - Accepted candidates must be **re-recorded** (RNG-refilled tails made explicit)
    or the working tape silently accumulates new randomness.
  - Acceptance needs a monotone criterion (shortlex) or greedy passes can regress.
  - **For schedule tapes, length is intrinsic** — the program always needs ~N picks to
    finish — so unlike Hypothesis (where shorter = simpler), the metric that matters
    is *distance from the canonical FIFO schedule* (count of nonzero picks). This is a
    real design divergence from conjecture shrinking; record it in Phase D design.

## Open items carried to Phase A

- The spike loop's `is_running()` lies (always `True`) and `run_until_complete` leans
  on `events._set_running_loop` — the production SimLoop needs honest lifecycle.
- Cancelled handles are logged as "ran"; production event log should distinguish.
- No escape detection beyond `NotImplementedError`; build the real thing (D4, §5.2).
- `PYTHONHASHSEED` is pinned in CI but the harness doesn't yet detect/re-exec.
- aiohttp's `Date` header touches real wall time (`time.time` via its helpers). It
  didn't break replay within a process, but cross-process replay will see different
  bytes — in-sim stdlib-time patching (plan problem #4) is the answer.
