"""Phase H — soak testing: shardable, resumable, continuous exploration.

Gate: shards are provably disjoint and complete, and a killed-then-resumed
soak covers exactly the remaining seeds — none skipped, none run twice."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import simloom
from simloom import soak
from simloom._soak import shard_seed


async def ok() -> None:
    await asyncio.sleep(0)


async def flaky() -> None:
    # fails on odd seeds (deterministic, easy to reason about)
    n = simloom.draw("n", 2)
    await asyncio.sleep(0)
    assert n == 0, "flaky failed"


class TestSharding:
    def test_shards_are_disjoint_and_complete(self) -> None:
        count, shards = 25, 4
        covered: list[int] = []
        for sh in range(shards):
            report = soak(ok, count=count, start=0, shards=shards, shard=sh)
            covered.extend(report.seeds)
        # a partition of exactly [0, shards*count): no gaps (complete), no dups (disjoint)
        assert sorted(covered) == list(range(shards * count))

    def test_shard_seed_formula(self) -> None:
        # shard 2 of 5 from start 100: 102, 107, 112, ...
        assert [shard_seed(100, 5, 2, i) for i in range(3)] == [102, 107, 112]

    def test_seeds_belong_to_the_shard(self) -> None:
        report = soak(ok, count=10, start=3, shards=4, shard=1)
        assert all((seed - 3 - 1) % 4 == 0 for seed in report.seeds)


class TestResume:
    def test_killed_then_resumed_covers_exactly_the_rest(self, tmp_path: Path) -> None:
        cp = tmp_path / "soak.json"
        # "kill" after 4 seeds
        first = soak(ok, count=4, checkpoint=cp)
        assert first.next_index == 4
        # resume toward 10: must cover exactly indices 4..9
        second = soak(ok, count=10, checkpoint=cp)
        assert second.next_index == 10
        combined = list(first.seeds) + list(second.seeds)
        assert combined == list(range(10))  # exact coverage, no gaps, no overlap

    def test_resume_is_a_noop_when_already_complete(self, tmp_path: Path) -> None:
        cp = tmp_path / "soak.json"
        soak(ok, count=8, checkpoint=cp)
        again = soak(ok, count=8, checkpoint=cp)
        assert again.seeds == ()
        assert again.next_index == 8

    def test_checkpoint_mismatch_is_rejected(self, tmp_path: Path) -> None:
        cp = tmp_path / "soak.json"
        soak(ok, count=4, start=0, shards=1, checkpoint=cp)
        with pytest.raises(ValueError, match="checkpoint"):
            soak(ok, count=4, start=0, shards=2, shard=0, checkpoint=cp)

    def test_resume_across_shards_shares_one_checkpoint(self, tmp_path: Path) -> None:
        cp = tmp_path / "soak.json"
        soak(ok, count=5, shards=3, shard=0, checkpoint=cp)
        soak(ok, count=5, shards=3, shard=1, checkpoint=cp)
        # both shards' cursors persisted independently in the same file
        r0 = soak(ok, count=5, shards=3, shard=0, checkpoint=cp)
        r1 = soak(ok, count=5, shards=3, shard=1, checkpoint=cp)
        assert r0.seeds == ()  # shard 0 already complete
        assert r1.seeds == ()  # shard 1 already complete


class TestFindingAndControl:
    def test_finds_failures_and_calls_back(self) -> None:
        seen: list[int] = []
        report = soak(flaky, count=20, on_failure=lambda f, r: seen.append(f.seed))
        assert report.failed
        assert seen == [f.seed for f in report.failures]
        # each reported failure reproduces from its seed
        for f in report.failures:
            assert simloom.run(flaky, seed=f.seed, raise_on_error=False).outcome == "error"

    def test_stop_on_failure(self) -> None:
        report = soak(flaky, count=100, stop_on_failure=True)
        assert report.stopped_early
        assert len(report.failures) == 1
        assert report.seeds_run < 100

    def test_validation(self) -> None:
        with pytest.raises(ValueError, match="shards"):
            soak(ok, count=5, shards=0)
        with pytest.raises(ValueError, match="shard"):
            soak(ok, count=5, shards=2, shard=2)
