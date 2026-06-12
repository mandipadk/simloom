"""The Phase D gate: measured strategy comparison on the bug zoo.

Everything here is deterministic (fixed seed ranges, deterministic runs), so
these are real measurements, re-verified on every CI run. The honest result,
consistent with the PCT literature:

- a uniform random walk dominates on shallow races;
- PCT finds the starvation-class bug that a random walk essentially cannot
  (the failure needs one task to win ~14 consecutive picks: ~2^-14 per seed
  for random, but routine once priorities exist).

The strategies are complementary; the explorer offers both.
"""

from __future__ import annotations

import os

import simloom
from bug_zoo import ZOO, shallow_race, starvation

RUNS = int(os.environ.get("SIMLOOM_ZOO_RUNS", "150"))
PCT = "pct:d=2,k=64"


def find_count(program: object, scheduler: str | None) -> int:
    exploration = simloom.explore(
        program,  # type: ignore[arg-type]
        runs=RUNS,
        stop_on_failure=False,
        scheduler=scheduler,
    )
    return len(exploration.failures)


def test_zoo_bugs_are_all_findable() -> None:
    """Regression: every zoo bug is found by at least one strategy."""
    for name, program in ZOO.items():
        found = find_count(program, None) + find_count(program, PCT)
        assert found > 0, f"{name}: no strategy found the bug in {RUNS} seeds each"


def test_pct_beats_random_walk_on_its_guarantee_class() -> None:
    """The gate measurement: the starvation bug is invisible to a random
    walk and routine for PCT."""
    random_finds = find_count(starvation, None)
    pct_finds = find_count(starvation, PCT)
    assert random_finds <= RUNS // 50, (
        f"random walk found starvation {random_finds}/{RUNS} — "
        f"the bug is shallower than the benchmark assumes"
    )
    assert pct_finds >= RUNS // 10, f"PCT only found starvation {pct_finds}/{RUNS}"
    assert pct_finds > random_finds


def test_random_walk_beats_pct_on_shallow_races() -> None:
    """The honest counterpart, recorded so nobody oversells PCT: shallow
    races belong to the random walk."""
    random_finds = find_count(shallow_race, None)
    pct_finds = find_count(shallow_race, PCT)
    assert random_finds > pct_finds
    assert random_finds >= RUNS // 2
