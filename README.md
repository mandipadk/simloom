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
hostile schedules looking for the one that breaks your invariants. When it finds one,
it hands you a **seed** that replays the failure byte-for-byte, forever, and **shrinks**
it to the minimal schedule that still triggers the bug.

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
httpx, the streams API) bottoms out in loop primitives, **real, unmodified libraries run
inside the simulation**. Our CI runs a genuine aiohttp server against a genuine httpx
client over the simulated network, with 20% packet loss injected, replayable from a seed.

## What you get

- **🎲 Seeded, exhaustive-ish scheduling** — a single *choice tape* (the Hypothesis
  trick, applied to schedules) drives every decision. One seed → one exact universe.
- **⏱ Virtual time** — an hour of simulated `asyncio.sleep` traffic runs in
  milliseconds. Timeouts and retries are tested at full speed.
- **🌐 A simulated network** — in-memory transports with tape-driven latency, loss
  (modeled as TCP retransmit delay — streams never corrupt), partitions, asymmetric
  blocks, and connection resets.
- **💥 Honest crashes** — `host.crash()` is a power cut: tasks stop with no `finally`
  blocks, unsynced disk writes are lost or *torn*, peers see resets. `restart()` brings
  the host back against its surviving fsynced state.
- **🔬 Fault injection in your code** — `simloom.sometimes("drop_cache")` is tape-driven
  inside the sim and a constant `False` in production. Annotate rare branches; explore
  them.
- **🪓 Automatic shrinking** — failures reduce to the minimal schedule deviation, with a
  human-readable explanation and a replayable artifact on disk.
- **🧭 Pluggable search** — a uniform random walk *and* PCT
  ([Probabilistic Concurrency Testing](https://www.microsoft.com/en-us/research/publication/a-randomized-scheduler-with-probabilistic-guarantees-of-finding-bugs/)),
  which finds deep ordering and starvation bugs a random walk essentially never hits.
- **🚨 Escape detection** — touch a real socket, signal, subprocess, or the wall clock
  from inside the sim and you get an `EscapedSimulationError` at the exact call site
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
states **exactly** what is and isn't deterministic. Known boundaries: blocking
C-extension I/O (`psycopg2`, `requests`, grpc's C core) can't run in-sim; direct
`time.time()` reads bypass the virtual clock; real subprocesses and external servers
need Python stand-ins. None of these fail silently.

## Status

**Pre-alpha, and built in the open.** The deterministic core, simulated world, fault
matrix, explorer, shrinker, and pytest plugin all exist and are exercised by a
10,000-seed determinism torture on every CI run (the harness holds itself to the same
hostility it applies to your code). The API may still shift before 1.0. If you try it,
[open an issue](https://github.com/mandipadk/simloom/issues) — early feedback shapes it.

## Learn more

- [`docs/determinism.md`](docs/determinism.md) — the honest boundary of the simulation
- [`docs/event-log.md`](docs/event-log.md) — the versioned event-log and tape formats
- [`examples/`](examples/) — the toy Raft and the bpo-42130 reproduction, both runnable
- [`docs/plan.md`](docs/plan.md) — the architecture and the road to 1.0

## Development

```sh
uv run --all-extras pytest          # tests (incl. the determinism torture)
uv run --all-extras mypy src        # strict typing
uv run --all-extras ruff check .    # lint
```

## License

Apache-2.0. See [LICENSE](LICENSE).
