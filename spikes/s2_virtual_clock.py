"""S2 — virtual clock spike.

Claim under test: with a virtual clock that jumps to the next timer whenever
nothing is ready, one hour of simulated `asyncio.sleep` traffic completes in
well under one second of wall time — and stays deterministic.

Run:    uv run python spikes/s2_virtual_clock.py
Pass:   prints "S2 PASS" and exits 0.

Reuses SpikeLoop from S1 (same directory).
"""

from __future__ import annotations

import asyncio
import time

from s1_seeded_scheduler import SpikeLoop


async def one_hour_of_traffic() -> dict[str, int]:
    """50 long-period workers + a 1 Hz ticker, all across one simulated hour."""
    stats = {"worker_wakeups": 0, "ticks": 0}
    horizon = 3600.0
    loop = asyncio.get_running_loop()

    async def worker(period: float) -> None:
        deadline = loop.time() + horizon
        while loop.time() < deadline:
            await asyncio.sleep(period)
            stats["worker_wakeups"] += 1

    async def ticker() -> None:
        for _ in range(int(horizon)):
            await asyncio.sleep(1.0)
            stats["ticks"] += 1

    tasks = [loop.create_task(worker(30.0 + i)) for i in range(50)]
    tasks.append(loop.create_task(ticker()))
    for t in tasks:
        await t
    return stats


def run(seed: int) -> tuple[dict[str, int], float, float]:
    loop = SpikeLoop(seed)
    wall_start = time.perf_counter()
    stats = loop.run_until_complete(one_hour_of_traffic())
    wall = time.perf_counter() - wall_start
    return stats, loop.time(), wall


def main() -> None:
    stats, vtime, wall = run(seed=42)

    assert vtime >= 3600.0, f"virtual clock only reached {vtime}s"
    assert wall < 1.0, f"took {wall:.3f}s wall — virtual clock is not jumping"
    assert stats["ticks"] == 3600
    # 50 workers with periods 30..79s over 3600s: sum(3600 // p) wakeups, ±1
    # per worker for the final partial period.
    expected = sum(int(3600 // (30 + i)) for i in range(50))
    assert abs(stats["worker_wakeups"] - expected) <= 50, stats

    stats2, vtime2, _ = run(seed=42)
    assert (stats2, vtime2) == (stats, vtime), "same seed, different universe"

    ratio = vtime / wall
    print(f"simulated {vtime:,.0f}s in {wall * 1000:.1f}ms wall ({ratio:,.0f}x real time)")
    print(f"events: {stats['ticks']} ticks + {stats['worker_wakeups']} worker wakeups")
    print("\nS2 PASS — one simulated hour in well under a second, deterministically.")


if __name__ == "__main__":
    main()
