# simloom

**Deterministic simulation testing for asyncio.** Run an unmodified asyncio application
inside a fully simulated world — virtual clock, simulated network, seeded scheduler —
explore thousands of execution interleavings with fault injection, and reduce every
failure to a seed that replays it exactly, forever.

> **Status: pre-alpha, Phase A (deterministic core).** The seeded loop, choice
> tape, replay, escape detection, and event log exist and hold a 10,000-seed
> determinism torture in CI. There is no simulated network, no fault injection,
> and no pytest plugin yet — those are Phases B–E. The plan is `docs/plan.md`;
> locked decisions are `DIRECTIVES.md`; the honest boundary of what is and isn't
> deterministic is `docs/determinism.md`.

What works today:

```python
import asyncio
import simloom

async def main() -> str:
    async def worker(n: int) -> int:
        await asyncio.sleep(n)        # virtual seconds; wall time ~0
        return n
    results = await asyncio.gather(*(worker(i) for i in range(5)))
    return f"sum={sum(results)}"

result = simloom.run(main, seed=1234)   # a fresh universe from a seed
print(result.value, result.digest)      # the digest fingerprints the universe

replayed = simloom.replay(main, tape=result)
assert replayed.digest == result.digest  # byte-identical, forever
```

## The idea

Python concurrency is tested one interleaving at a time — the polite one your laptop
happens to schedule. Rust has `loom`, `turmoil`, `madsim`, `shuttle`; .NET had Coyote;
FoundationDB built a company-defining simulator. Python's asyncio — where every
scheduling decision happens at an `await`, under a replaceable event loop — is
structurally perfect for the same methodology, and has nothing.

simloom aims to be that harness:

```python
import simloom as sim

@sim.test(runs=2000)
async def test_leader_election(world):
    nodes = [world.host(f"n{i}") for i in range(5)]
    for h in nodes:
        h.spawn(run_node(h, peers=nodes))      # your real, unmodified asyncio code

    world.net.partition(nodes[:2], nodes[2:])
    await world.sleep(30)                      # virtual seconds; wall time ~0
    world.net.heal()
    nodes[0].crash()
    nodes[0].restart()

    await world.until(lambda: exactly_one_leader(nodes), timeout=120)
```

```
FAILED test_leader_election — invariant violated: 2 leaders (n1, n3)
  seed: 0x9f3a11c2e4   (re-run: pytest -k leader --simloom-seed=0x9f3a11c2e4)
  shrunk: 41 scheduling decisions, 2 faults (from 38,114 / 9)
```

## Honesty

This project will always document exactly what is and isn't deterministic
(`docs/determinism.md`, once it exists) and what escapes the simulation. Known
boundaries already on record: C-extension I/O (`psycopg2`, `requests`, grpc's C core)
cannot run in-sim; blocking I/O in executors escapes; real external servers need Python
stand-ins. Escape detection turns these from silent nondeterminism into loud errors.

## Development

```sh
uv run --all-extras pytest          # tests
uv run --all-extras mypy src        # strict typing
uv run --all-extras ruff check .    # lint
```

License: Apache-2.0.
