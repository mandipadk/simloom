"""Phase H — interleaving fingerprints and the regression corpus (the feedback
substrate for coverage-guided exploration), plus the signal pre-gate."""

from __future__ import annotations

import asyncio
import os

import simloom
from simloom import InterleavingCorpus, fingerprint, interleaving_edges


async def workload() -> None:
    shared = {"v": 0}

    async def worker(k: int) -> None:
        for _ in range(k % 3 + 1):
            cur = shared["v"]
            await asyncio.sleep(0)
            shared["v"] = cur + 1

    await asyncio.gather(*(worker(i) for i in range(5)))


class TestEdges:
    def test_consecutive_step_pairs(self) -> None:
        events = [
            {"kind": "run_start", "t": 0.0},
            {"kind": "step", "ran": "a"},
            {"kind": "clock_jump", "t": 1.0},
            {"kind": "step", "ran": "b"},
            {"kind": "step", "ran": "a"},
        ]
        assert interleaving_edges(events) == {("a", "b"), ("b", "a")}

    def test_fingerprint_is_hashable_and_deterministic(self) -> None:
        r = simloom.run(workload, seed=3)
        fp1 = fingerprint(r.log.events)
        fp2 = fingerprint(simloom.replay(workload, tape=r).log.events)
        assert fp1 == fp2
        assert isinstance(hash(fp1), int)


class TestCorpus:
    def test_only_novel_runs_are_kept(self) -> None:
        corpus = InterleavingCorpus()
        r0 = simloom.run(workload, seed=0)
        first = corpus.observe(r0.log.events, r0.tape, seed=0)
        assert first > 0  # the first run is all-new
        assert len(corpus) == 1
        # the same run again contributes nothing
        again = corpus.observe(r0.log.events, r0.tape, seed=0)
        assert again == 0
        assert len(corpus) == 1

    def test_coverage_is_monotonic(self) -> None:
        corpus = InterleavingCorpus()
        prev = 0
        for seed in range(30):
            r = simloom.run(workload, seed=seed)
            corpus.observe(r.log.events, r.tape, seed=seed)
            assert corpus.coverage >= prev
            prev = corpus.coverage


class TestSignal:
    """The fingerprint must give a usable steering signal: it discriminates
    (coverage > 1, runs differ), and novelty is sparse (most runs add nothing,
    so steering toward the novel ones is worthwhile)."""

    N = int(os.environ.get("SIMLOOM_FINGERPRINT_SEEDS", "300"))

    def test_signal(self) -> None:
        corpus = InterleavingCorpus()
        fps = set()
        early_novel = 0
        for seed in range(self.N):
            r = simloom.run(workload, seed=seed)
            fps.add(fingerprint(r.log.events))
            new = corpus.observe(r.log.events, r.tape, seed=seed)
            if seed < self.N // 4 and new:
                early_novel += 1

        # discrimination: more than one structural outcome, real coverage
        assert len(fps) > 1
        assert corpus.coverage > 5
        # sparse novelty: only a small fraction of runs add edges (steering pays)
        assert 0 < len(corpus) < self.N // 4
        # the corpus grows early (novelty is discovered, not absent)
        assert early_novel > 0
