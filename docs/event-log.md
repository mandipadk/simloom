# The simloom event log and tape formats (version 1)

Both formats are **public contracts**. Failure artifacts ship them, tooling
consumes them, and the planned time-travel debugger replays from them. Any
schema change bumps the version; readers must reject versions they don't know.

## Tape format (`simloom-tape`, version 1)

A tape is the complete record of one universe's decisions: a JSON object.

```json
{
  "format": "simloom-tape",
  "version": 1,
  "seed": 1234,
  "draws": [["sched.pick", 2, 3], ["sched.pick", 0, 2]]
}
```

- `seed` — the generating PRNG seed, or `null` if the tape was produced by
  replay or editing. The draws, not the seed, are authoritative: replaying
  reads the draw list and never consults a PRNG.
- `draws` — ordered `[label, value, bound]` triples. `label` identifies the
  draw *site* (e.g. `"sched.pick"` for scheduler choices; fault-injection
  sites get their own labels in later phases). `value` is an integer in
  `[0, bound)`. Bounds are recorded so that replay detects divergence (a
  changed bound means the program saw a different world) and so tools can
  edit values without running the program.
- Draws with bound 1 are forced and never recorded.

Replay semantics: a draw whose label or bound differs from the recording, or
a draw past the end of the tape, is a **misalignment**. Strict replay raises
`TapeMisalignmentError`; fallback replay (used for tape editing/shrinking)
switches permanently to a fixed-seed PRNG and reports `misaligned_at`. A
strict replay that *finishes* without consuming every recorded draw is
equally divergent and fails the run.

## Event log format (`simloom-events`, version 1)

One JSONL document per run: a header object, then one object per event.
Within objects, keys are sorted and separators are compact (`,`/`:`) — the
serialization is canonical so logs can be compared byte-for-byte.

### Header

The first line. Metadata only — **never part of the digest**:

```json
{"format": "simloom-events", "version": 1, "simloom": "0.0.1.dev0",
 "python": "3.12.12", "implementation": "CPython", "seed": 1234,
 "epoch": 0.0, "hash_randomization_pinned": true}
```

### Events

Every event has `seq` (0-based, dense), `kind`, and `t` (virtual time,
float seconds). Kind-specific fields:

| kind | fields | meaning |
|---|---|---|
| `run_start` | — | the universe begins at `t` = epoch |
| `step` | `choice`, `ready`, `ran` | the scheduler ran ready callback #`choice` of `ready`; `ran` is an address-free label (`<task>.step`, `<task>.wakeup`, or a callback qualname) |
| `clock_jump` | — | nothing was runnable; the clock jumped to `t` |
| `task_created` | `task`, `coro` | a task was created (loop-owned deterministic name + coroutine qualname) |
| `task_done` | `task`, `outcome` | `finished` or `cancelled` — never the exception (reading it would suppress asyncio's never-retrieved detection) |
| `gc_collect` | — | the controlled GC ran at a deterministic step interval (object counts are process-global and deliberately excluded) |
| `unhandled_exception` | `error` | an exception type reached the loop's handler |
| `deadlock` | `pending` | quiescence with the listed tasks still waiting |
| `escape` | `api` | the program touched a real-world API |
| `invariant` | `label`, `category` | a registered property was violated; `category` is `safety` (`always`), `liveness` (`eventually`/`leads_to`), or `convergence` |
| `net_listen` | `host`, `port` | a simulated server started listening |
| `net_connect` | `host`, `port` | a simulated connection was established |
| `host_crash` | `host` | a simulated host lost power |
| `host_restart` | `host`, `generation` | a crashed host came back up |
| `net_partition` | `a`, `b` | a bidirectional partition between host groups |
| `net_block` | `src`, `dst` | a one-way block (asymmetric partition) |
| `net_heal` | — | every block removed; held traffic delivered in order |
| `net_reset` | `between` | resets injected on live connections between two hosts |
| `run_end` | `outcome`, `error` | `ok`/`error` and the error type name (or `null`) |

Draw sites on the tape beyond `sched.pick`: `net.delay` (quantized link
latency, bound 64) and `net.loss` (percent roll, bound 100) per written
chunk; `disk.fate`/`disk.tear` per buffered file at a host crash;
`buggify.<label>` (bound 100) per `simloom.sometimes` call; any label the
program under test passes to `simloom.draw`; and under the PCT scheduler,
`pct.prio` (bound 1024, one per scheduling entity) and `pct.change` (bound =
horizon, d-1 of them). The header records which scheduler produced the run;
replaying under a different one misaligns loudly.

Property monitors (`always`/`eventually`/`leads_to`) add **no** tape draws and, while passing, emit **no** events — they are evaluated between steps. A run whose monitors all pass has a digest byte-identical to the same run with no monitors. Only a violation emits an `invariant` event.

### Digest

`sha256` over the canonical event lines joined by `\n` (header excluded).
Two runs are the same universe iff their digests match. The header is
excluded so the same universe hashes identically regardless of machine,
interpreter, or simloom build — but note that *producing* the same universe
from a tape on a different Python version is only as stable as the asyncio
internals involved (see docs/determinism.md).

### Determinism rules for emitters

Field values must be deterministic given the tape:

- nothing derived from `id()`, `repr()` with addresses, or hashes of objects;
- no wall-clock times;
- no process-global counters (asyncio's default `Task-N` names are rewritten
  to loop-owned names for exactly this reason);
- no absolute file paths (they vary across machines).
