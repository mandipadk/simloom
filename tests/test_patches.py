"""Phase G — the determinism boundary: virtual wall clock + tape-seeded
randomness. An unmodified asyncio program that reads ``time.time()`` and rolls
``random``/``uuid4`` must replay byte-for-byte."""

from __future__ import annotations

import asyncio
import os
import random
import secrets
import time
import uuid

import pytest

import simloom
from simloom._patches import DEFAULT_WALL_EPOCH


# --------------------------------------------------------------------------
# the shared workload: time + random + uuid all drive scheduling-visible state
# --------------------------------------------------------------------------
async def clocked_workload() -> dict[str, object]:
    results: list[tuple[int, float, str]] = []

    async def worker(n: int) -> None:
        await asyncio.sleep(random.random())  # tape-seeded delay -> deterministic schedule
        results.append((n, round(time.monotonic(), 6), uuid.uuid4().hex[:8]))

    await asyncio.gather(*(worker(i) for i in range(5)))
    branch = "low" if random.random() < 0.5 else "high"
    return {
        "results": results,
        "branch": branch,
        "wall": time.time(),
        "token": secrets.token_bytes(4).hex(),
        "raw": os.urandom(4).hex(),
    }


class TestVirtualClock:
    def test_time_functions_are_virtual(self) -> None:
        async def main() -> dict[str, float]:
            await asyncio.sleep(7.5)
            return {
                "time": time.time(),
                "monotonic": time.monotonic(),
                "perf": time.perf_counter(),
                "time_ns": float(time.time_ns()),
                "monotonic_ns": float(time.monotonic_ns()),
            }

        v = simloom.run(main, seed=0, virtual_time=True).value
        assert v["time"] == DEFAULT_WALL_EPOCH + 7.5
        assert v["monotonic"] == 7.5
        assert v["perf"] == 7.5
        assert v["time_ns"] == (DEFAULT_WALL_EPOCH + 7.5) * 1e9
        assert v["monotonic_ns"] == 7.5e9

    def test_custom_wall_epoch(self) -> None:
        async def main() -> float:
            await asyncio.sleep(1.0)
            return time.time()

        assert simloom.run(main, seed=0, virtual_time=True, wall_epoch=1000.0).value == 1001.0

    def test_clock_only_does_not_draw_entropy(self) -> None:
        async def main() -> None:
            await asyncio.sleep(1.0)

        r = simloom.run(main, seed=0, virtual_time=True)
        assert all(d.label != "entropy.seed" for d in r.tape)


class TestSeededRandomness:
    def test_random_uuid_secrets_urandom_are_deterministic(self) -> None:
        async def main() -> tuple[float, str, str, str]:
            return (
                random.random(),
                uuid.uuid4().hex,
                secrets.token_bytes(8).hex(),
                os.urandom(8).hex(),
            )

        a = simloom.run(main, seed=3, seed_randomness=True).value
        b = simloom.run(main, seed=3, seed_randomness=True).value
        assert a == b  # same seed -> identical entropy

    def test_entropy_is_tape_coupled(self) -> None:
        async def main() -> float:
            return random.random()

        seeds = {simloom.run(main, seed=s, seed_randomness=True).value for s in range(20)}
        assert len(seeds) > 10  # different seeds -> different entropy

    def test_entropy_seed_is_the_first_draw(self) -> None:
        async def main() -> None:
            random.random()
            await asyncio.sleep(0)

        r = simloom.run(main, seed=0, seed_randomness=True)
        assert r.tape[0].label == "entropy.seed"


class TestRestoration:
    def test_real_time_and_random_restored_after_ok_run(self) -> None:
        real_time = time.time
        real_urandom = os.urandom
        state = random.getstate()

        async def main() -> None:
            await asyncio.sleep(1.0)

        simloom.run(main, seed=0, virtual_time=True, seed_randomness=True)
        assert time.time is real_time
        assert os.urandom is real_urandom
        # random stream restored (not stuck on the seeded sequence)
        random.setstate(state)
        assert time.time() > DEFAULT_WALL_EPOCH  # real wall clock, present day

    def test_patches_restored_even_when_the_run_crashes(self) -> None:
        real_time = time.time
        real_urandom = os.urandom

        async def boom() -> None:
            await asyncio.sleep(1.0)
            raise RuntimeError("boom")

        r = simloom.run(
            boom, seed=0, virtual_time=True, seed_randomness=True, raise_on_error=False
        )
        assert r.outcome == "error"
        assert time.time is real_time  # restored despite the crash
        assert os.urandom is real_urandom


class TestGate:
    """The Phase G gate: a workload mixing time + random + uuid replays
    byte-for-byte across the seed corpus."""

    SEEDS = int(os.environ.get("SIMLOOM_PATCH_SEEDS", "200"))

    def test_record_rerecord_replay_byte_identical(self) -> None:
        digests: set[str] = set()
        branches: set[str] = set()
        for seed in range(self.SEEDS):
            first = simloom.run(
                clocked_workload, seed=seed, virtual_time=True, seed_randomness=True
            )
            again = simloom.run(
                clocked_workload, seed=seed, virtual_time=True, seed_randomness=True
            )
            replayed = simloom.replay(
                clocked_workload, tape=first, virtual_time=True, seed_randomness=True
            )
            assert again.digest == first.digest, f"seed {seed}: re-run diverged"
            assert replayed.digest == first.digest, f"seed {seed}: replay diverged"
            assert again.value == first.value
            assert replayed.value == first.value
            digests.add(first.digest)
            branches.add(first.value["branch"])  # type: ignore[index]
        # the entropy genuinely varies the universe, and the random branch flips
        assert len(digests) > self.SEEDS // 2
        assert branches == {"low", "high"}

    def test_replay_without_patches_diverges_loudly(self) -> None:
        # A universe recorded WITH seeded randomness must not be replayed without
        # it: the entropy.seed draw is missing, so strict replay misaligns.
        recorded = simloom.run(clocked_workload, seed=0, virtual_time=True, seed_randomness=True)
        mismatched = simloom.replay(clocked_workload, tape=recorded, raise_on_error=False)
        assert mismatched.outcome == "error"
        assert isinstance(mismatched.error, simloom.TapeMisalignmentError)


class TestPlugin:
    def test_simloom_test_is_deterministic_by_default(self, pytester: pytest.Pytester) -> None:
        pytester.makepyfile(
            """
            import time, random, asyncio
            import simloom

            @simloom.test(runs=50)
            async def test_clock_and_random_are_virtual():
                # default: virtual_time + seed_randomness are ON
                await asyncio.sleep(2.0)
                assert time.time() == 1_704_067_200.0 + 2.0   # virtual wall clock
                assert time.monotonic() == 2.0
                random.random()  # deterministic; no escape, no flake
            """
        )
        result = pytester.runpytest("-p", "no:cacheprovider")
        result.assert_outcomes(passed=1)


pytest_plugins = ["pytester"]
