"""The Phase C gate (demo #3): the toy Raft torture suite.

The buggy variant (votes granted without consulting votedFor) must be caught
by seed exploration — two leaders in one term — and the failure must replay
exactly. The correct variant must survive the same torture, and the torture
must demonstrably exercise its fault paths (coverage, the sometimes-assertion
pattern).
"""

from __future__ import annotations

import os

from toy_raft import raft_world

import simloom

EXPLORE_CAP = int(os.environ.get("SIMLOOM_RAFT_EXPLORE_CAP", "60"))
SURVIVAL_SEEDS = int(os.environ.get("SIMLOOM_RAFT_SURVIVAL_SEEDS", "15"))


def violations(value: dict[int, list[str]]) -> dict[int, list[str]]:
    return {term: who for term, who in value.items() if len(who) > 1}


def test_buggy_raft_is_caught_and_replays() -> None:
    found = None
    for seed in range(EXPLORE_CAP):
        result = simloom.run(raft_world(buggy=True), seed=seed)
        if violations(result.value):
            found = (seed, result)
            break
    assert found is not None, f"no election-safety violation in {EXPLORE_CAP} seeds"

    seed, result = found
    # The artifact story at Raft scale: the recorded tape replays the exact
    # violating universe, byte for byte.
    replayed = simloom.replay(raft_world(buggy=True), tape=result)
    assert replayed.digest == result.digest
    assert violations(replayed.value) == violations(result.value)


def test_correct_raft_survives_the_torture() -> None:
    elected = 0
    fault_kinds: set[str] = set()
    for seed in range(SURVIVAL_SEEDS):
        result = simloom.run(raft_world(buggy=False), seed=seed)
        assert violations(result.value) == {}, (
            f"seed {seed}: election safety violated: {violations(result.value)}"
        )
        elected += result.coverage.get("raft.leader_elected", 0)
        fault_kinds |= {k for k in result.coverage if k.startswith("chaos.")}
    # Sometimes-assertions over the corpus: the torture must actually elect
    # leaders and actually inject faults, or surviving it proves nothing.
    assert elected > 0, "no leader was ever elected across the corpus"
    assert {"chaos.partition", "chaos.crash", "chaos.restart"} <= fault_kinds, fault_kinds
