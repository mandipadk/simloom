<h1 align="center">simloom</h1>

<p align="center">
  <strong>Deterministic simulation testing for Python's asyncio.</strong><br>
  Find the race before it ships. Replay it forever from a seed.
</p>

<p align="center">
  <a href="https://pypi.org/project/simloom/"><img src="https://img.shields.io/pypi/v/simloom.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/simloom/"><img src="https://img.shields.io/pypi/pyversions/simloom.svg" alt="Python versions"></a>
  <a href="https://github.com/mandipadk/simloom/actions/workflows/ci.yml"><img src="https://github.com/mandipadk/simloom/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License: Apache 2.0"></a>
  <img src="https://img.shields.io/badge/types-strict-blue.svg" alt="Typed: strict">
</p>

---

Your async code is tested one interleaving at a time — the polite one your laptop
happened to schedule. Races ship. Flakes get retried. "Works on my machine" is the
state of the art.

**simloom runs your unmodified asyncio program inside a fully simulated world** — a
seeded scheduler that owns every interleaving, a virtual clock, an in-memory network
with injectable latency, loss, partitions, and crashes — and explores thousands of
hostile schedules looking for the one that breaks your invariants. It doesn't just find
crashes: it asserts **safety and liveness** properties, **proves** correctness by
exhaustive systematic search, and catches **wrong answers** (a store that returns a stale
or impossible read) with serializability checking. When it finds a problem, it hands you a
**seed** that replays the failure byte-for-byte, forever, **shrinks** it to the minimal
schedule that still triggers the bug, and lets you **walk the causal trace** of what woke
what.

```text
FAILED test_lease_exclusivity — simloom found a failing universe
  seed: 17   (re-run: pytest -k lease --simloom-seed=17)
  error: AssertionError: two holders of an exclusive lease
  shrunk: 31 draws → 29, schedule deviations 25 → 1 (106 candidate runs)
  minimal schedule: FIFO everywhere except:
    draw #0: sched.pick = 1 (of 4)
  artifacts: .sim/failures/test_lease_exclusivity-seed17.tape.json, …
```

The entire bug, above, is *"one callback ran out of order, once."* No more staring at a
flake that reproduces every thousandth CI run.

## Install

```sh
pip install simloom        # or: uv add simloom
```

Python 3.12+. Zero runtime dependencies. The pytest plugin loads automatically.

## Quickstart

Write an ordinary async test, decorate it, and let simloom explore the schedule space:

```python
import asyncio
import simloom

@simloom.test(runs=2000)          # explores 2000 schedules; pytest collects this
async def test_counter_is_atomic():
    state = {"value": 0}

    async def worker():
        for _ in range(3):
            current = state["value"]
            await asyncio.sleep(0)          # a scheduling point — the race lives here
            state["value"] = current + 1

    await asyncio.gather(*(worker() for _ in range(3)))
    assert state["value"] == 9             # a plain assert: it fires under exploration
```

```sh
pytest                                  # finds the lost-update race, shrinks it, prints the seed
pytest --simloom-seed=42                # replay one exact universe
```

Need a distributed system? Ask for a `world` and you get hosts, a network, and faults:

```python
@simloom.test(runs=5000)
async def test_leader_election(world):
    nodes = [world.host(f"n{i}") for i in range(5)]
    for h in nodes:
        h.spawn(lambda h=h: run_node(h, peers=nodes))   # your real, unmodified asyncio code

    world.net.partition(nodes[:2], nodes[2:])           # faults are first-class
    await world.sleep(30)                               # virtual seconds — wall time ≈ 0
    world.net.heal()
    nodes[0].crash()                                    # a real power cut: no finally blocks
    nodes[0].restart()                                  # comes back against fsynced disk only

    await world.until(lambda: exactly_one_leader(nodes), timeout=120)
```

### Beyond crashes

Assert what *should* hold, prove it can't be violated, and catch wrong answers:

```python
@simloom.test(runs=5000)
async def test_lease_safety(world):
    cluster = start_cluster(world)
    # safety: never two leaders. liveness: one eventually emerges.
    world.always("≤1 leader", lambda: sum(n.is_leader for n in cluster) <= 1)
    world.eventually("a leader", lambda: any(n.is_leader for n in cluster), within=120)
    await world.sleep(300)

@simloom.test(systematic=True, max_delays=3)     # exhaustive, not sampled
async def test_critical_section():
    ...                                          # passes ⇒ a bounded PROOF of correctness

@simloom.test(runs=5000)
async def test_store_is_serializable(world):
    await run_transactions(world)                # records ops into world.history
    world.assert_serializable()                  # finds lost updates / write skew, with the cycle
```

And once it finds something, replay and walk it:

```sh
pytest --simloom-seed=17 -k lease          # replay the exact failing universe
simloom trace failure.jsonl --step 42      # reconstruct state + the happens-before stack
simloom diff run_a.jsonl run_b.jsonl       # first divergence between two universes
```

## Why this didn't exist before

Rust has [`loom`](https://github.com/tokio-rs/loom),
[`turmoil`](https://github.com/tokio-rs/turmoil),
[`madsim`](https://github.com/madsim-rs/madsim), and
[`shuttle`](https://github.com/awslabs/shuttle). .NET had
[Coyote](https://github.com/microsoft/coyote). FoundationDB built a company-defining
simulator; [Antithesis](https://antithesis.com) sells the methodology at the hypervisor
level. Python — where a huge share of backend glue and agent orchestration is written —
had **nothing**.

And asyncio is *structurally perfect* for it: every interleaving decision happens at an
`await`, under a **replaceable event loop**. simloom swaps in a deterministic one — no
forked interpreter, no hypervisor, no recompilation. Because the ecosystem (aiohttp,
httpx, redis, the streams API) bottoms out in loop primitives, **real, unmodified
libraries run inside the simulation**. Our CI runs an unmodified **aiohttp HTTPS** server
against an unmodified aiohttp client over a memory-BIO TLS handshake, and an unmodified
**`redis.asyncio.Redis`** against an in-sim Redis — all over the simulated network, with
faults injected, replayable byte-for-byte from a seed.

## What you get

**Run it.** Your real asyncio code, unchanged, inside the simulation.

- **🎲 Seeded scheduling** — a single *choice tape* (the Hypothesis trick, applied to
  schedules) drives every decision. One seed → one exact universe.
- **⏱ Virtual time** — an hour of simulated `asyncio.sleep` traffic runs in milliseconds;
  opt-in patching makes `time.time()` and the stdlib RNG tape-driven too, so an unmodified
  library that timestamps or rolls a random timeout replays byte-for-byte.
- **🌐 A simulated network** — in-memory streams (loss modeled as TCP retransmit delay —
  bytes never corrupt), **UDP datagrams** with real loss/reorder/duplication,
  partitions, asymmetric per-link latency/loss shaping, resets, and **TLS in-sim**
  (memory-BIO SSL, so aiohttp HTTPS just works).
- **💥 Honest crashes & disk** — `host.crash()` is a power cut: tasks stop with no
  `finally` blocks, unsynced disk writes are lost or *torn*, peers see resets.
- **🧩 Stand-in services** — `world.run_service(SimRedis(), …)` gives you an in-sim Redis
  (SET/GET, WATCH/MULTI/EXEC) to test your logic against, with wire faults applied.

**Break it.** Explore the schedules and faults your laptop never will.

- **🔬 Fault injection** — `simloom.sometimes("drop_cache")` is tape-driven inside the sim
  and a constant `False` in production. Annotate rare branches; explore them.
- **🧭 Pluggable search** — uniform random walk, **`pct:auto`** (auto-tuned
  [Probabilistic Concurrency Testing](https://www.microsoft.com/en-us/research/publication/a-randomized-scheduler-with-probabilistic-guarantees-of-finding-bugs/)
  that finds deep-ordering and starvation bugs random walk never hits), and `simloom.soak`
  (resumable, shardable continuous exploration).

**Check it.** Assertions far past "didn't crash."

- **🛡 Safety & liveness oracles** — `world.always("one leader", …)`,
  `world.eventually("elects a leader", …, within=120)`, `world.leads_to(…)`, plus a
  livelock detector and replica-convergence checks.
- **✅ Prove it** — `@simloom.test(systematic=True)` switches from sampling seeds to
  *exhaustive* delay-bounded model checking: it finds a deep interleaving bug
  deterministically, or **passes as a bounded proof of correctness**.
- **🧾 Find wrong answers** — an Elle-style serializability checker
  (`world.check_serializable`) catches a store that returns a stale or impossible read —
  not just one that crashes — and reports the cyclic read/write dependency.

**Reproduce & debug it.**

- **🪓 Automatic shrinking** — failures reduce to the minimal schedule deviation, with a
  replayable artifact on disk.
- **🔁 Determinism guarantees** — a per-test self-check (run twice, locate the first
  diverging event), `PYTHONHASHSEED` auto-pin, and a queryable boundary registry.
- **🕰 Causal trace** — record with `causal=True` and walk it: `simloom trace LOG --step N`
  reconstructs state and the *happens-before stack* that woke it; `--changed x` is an
  omniscient query; `simloom diff A B` finds the first divergence between two universes.
- **🚨 Escape detection** — touch a real socket, signal, subprocess, or (un-patched) wall
  clock from inside the sim and you get an `EscapedSimulationError` at the exact call site
  instead of silent nondeterminism.

## It finds real bugs

**A multi-year CPython race.** Pre-3.12 `asyncio.wait_for` could *swallow a delivered
cancellation* when the inner future completed in the same window as the cancel
([bpo-42130](https://github.com/python/cpython/issues/86296)). In production it took an
exact wall-clock collision; under simloom the timeout boundary is just another
scheduling choice, so exploration finds the interleaving **from a seed** and replays it
exactly. The modern implementation survives the identical torture. → `examples/bpo42130.py`

**The canonical demo.** A toy Raft over the simulated network — persisted term/votedFor,
JSON-RPC, the works — tortured with partitions, crashes, and restarts. Plant the classic
double-vote bug and exploration elects two leaders in one term in ~1 seed of 5; the fixed
version survives, with coverage counters proving the faults actually fired.
→ `examples/toy_raft.py`

## Honesty first

A determinism claim is only as good as its disclosed limits. simloom raises a loud error
when your code reaches outside the simulation, and [`docs/determinism.md`](docs/determinism.md)
states **exactly** what is and isn't deterministic — now machine-readable via
`simloom.boundary()`. Known boundaries: blocking C-extension I/O (`psycopg2`, `requests`,
grpc's C core) can't run in-sim; a `from time import …` alias or the C-accelerated
`datetime.now()` isn't redirected by the clock patch; real subprocesses and external
servers need stand-ins. None of these fail silently — and the per-test determinism
self-check locates anything that slips through.

## Status

**Alpha, and built in the open.** The deterministic core, the simulated world and fault
matrix (streams, UDP, TLS, partitions, crashes, torn disk writes), the explorer, shrinker,
property oracles, systematic verifier, serializability checker, causal-trace CLI, and the
pytest plugin all exist — exercised by a 10,000-seed determinism torture on every CI run
(the harness holds itself to the same hostility it applies to your code) across Python
3.12/3.13/3.14. The API may still shift before 1.0. If you try it,
[open an issue](https://github.com/mandipadk/simloom/issues) — early feedback shapes it.

## Learn more

- [`docs/determinism.md`](docs/determinism.md) — the honest boundary of the simulation
- [`docs/event-log.md`](docs/event-log.md) — the versioned event-log and tape formats
- [`CHANGELOG.md`](CHANGELOG.md) — every capability, phase by phase
- [`examples/`](examples/) — the toy Raft and the bpo-42130 reproduction, both runnable

## Development

```sh
uv run --all-extras pytest          # tests (incl. the determinism torture)
uv run --all-extras mypy src        # strict typing
uv run --all-extras ruff check .    # lint
```

## License

Apache-2.0. See [LICENSE](LICENSE).
