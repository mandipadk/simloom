"""S1 — seeded scheduler spike.

Claim under test: a custom event loop in which a *seeded RNG* picks which ready
callback runs next gives us (a) different interleavings of the same unmodified
program for different seeds, and (b) byte-identical event logs when the same
seed runs twice.

Run:    uv run python spikes/s1_seeded_scheduler.py
Pass:   prints "S1 PASS" and exits 0; any assertion failure exits non-zero.

The loop here is deliberately minimal — just enough surface for asyncio.Task,
asyncio.Future, and asyncio.sleep to function. Production SimLoop is Phase A.
"""

from __future__ import annotations

import asyncio
import hashlib
import heapq
import json
import random
from collections import Counter
from collections.abc import Callable, Coroutine
from contextvars import Context
from typing import Any


def _describe(handle: asyncio.Handle) -> str:
    """Address-free, deterministic description of what a handle will run."""
    callback = handle._callback
    owner = getattr(callback, "__self__", None)
    if isinstance(owner, asyncio.Task):
        kind = getattr(callback, "__name__", type(callback).__name__)
        return f"{owner.get_name()}:{kind}"
    return getattr(callback, "__qualname__", type(callback).__name__)


class SpikeLoop(asyncio.AbstractEventLoop):
    """Minimal deterministic event loop: the seeded RNG owns every scheduling pick.

    Anything asyncio needs that is not implemented below raises
    NotImplementedError from AbstractEventLoop — a crude preview of escape
    detection: nothing touches a selector, a socket, or real time.
    """

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)
        self._ready: list[asyncio.Handle] = []
        self._scheduled: list[tuple[float, int, asyncio.TimerHandle]] = []
        self._time = 0.0
        self._timer_tiebreak = 0
        self._task_counter = 0
        self._step_counter = 0
        self._loop_exc: BaseException | None = None
        self.events: list[dict[str, Any]] = []

    # --- introspection the asyncio machinery relies on ---

    def time(self) -> float:
        return self._time

    def get_debug(self) -> bool:
        return False

    def is_running(self) -> bool:
        return True

    def is_closed(self) -> bool:
        return False

    def close(self) -> None:
        pass

    # --- scheduling primitives: every path into the loop lands here ---

    def call_soon(
        self, callback: Callable[..., object], *args: object, context: Context | None = None
    ) -> asyncio.Handle:
        handle = asyncio.Handle(callback, args, self, context)
        self._ready.append(handle)
        return handle

    def call_later(
        self,
        delay: float,
        callback: Callable[..., object],
        *args: object,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        return self.call_at(self._time + delay, callback, *args, context=context)

    def call_at(
        self,
        when: float,
        callback: Callable[..., object],
        *args: object,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        handle = asyncio.TimerHandle(when, callback, args, self, context)
        self._timer_tiebreak += 1
        heapq.heappush(self._scheduled, (when, self._timer_tiebreak, handle))
        return handle

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        pass  # cancelled timers are dropped lazily when they pop

    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def create_task(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> asyncio.Task[Any]:
        # Deterministic naming: asyncio's default Task-N counter is process-global,
        # which would make logs differ across runs in the same process.
        if name is None:
            name = f"task-{self._task_counter}"
        self._task_counter += 1
        return asyncio.Task(coro, loop=self, name=name, context=context)

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        exc = context.get("exception")
        self._loop_exc = exc if isinstance(exc, BaseException) else RuntimeError(str(context))

    # --- the spike's heart ---

    def run_until_complete(self, coro: Coroutine[Any, Any, Any]) -> Any:
        asyncio.events._set_running_loop(self)
        try:
            main = self.create_task(coro)
            while not main.done():
                self._run_once()
                if self._loop_exc is not None:
                    raise self._loop_exc
            return main.result()
        finally:
            asyncio.events._set_running_loop(None)

    def _run_once(self) -> None:
        # If nothing is ready, jump virtual time to the next live timer.
        if not self._ready:
            while self._scheduled and self._scheduled[0][2].cancelled():
                heapq.heappop(self._scheduled)
            if not self._scheduled:
                raise RuntimeError("quiescent: no ready callbacks and no timers (deadlock)")
            self._time = max(self._time, self._scheduled[0][0])

        # Pump every due timer into the ready set.
        while self._scheduled and self._scheduled[0][0] <= self._time:
            _, _, timer = heapq.heappop(self._scheduled)
            if not timer.cancelled():
                self._ready.append(timer)

        # THE CHOICE: the seeded RNG — not arrival order — picks what runs next.
        index = self._rng.randrange(len(self._ready))
        handle = self._ready.pop(index)
        self.events.append(
            {
                "step": self._step_counter,
                "vtime": round(self._time, 9),
                "ready": len(self._ready) + 1,
                "choice": index,
                "ran": _describe(handle),
            }
        )
        self._step_counter += 1
        if not handle.cancelled():
            handle._run()


# --- the unmodified program under test: a classic read-modify-write race ---


async def racy_program(workers: int = 3, increments: int = 3) -> int:
    """Each worker does non-atomic `value = value + 1` straddling an await."""
    state = {"value": 0}

    async def worker() -> None:
        for _ in range(increments):
            current = state["value"]
            await asyncio.sleep(0)  # schedule point between read and write
            state["value"] = current + 1

    loop = asyncio.get_running_loop()
    tasks = [loop.create_task(worker()) for _ in range(workers)]
    for t in tasks:
        await t
    return state["value"]


def run_seed(seed: int) -> tuple[int, str, list[dict[str, Any]]]:
    """One fresh simulated universe; returns (final_value, log_sha256, events)."""
    loop = SpikeLoop(seed)
    final = loop.run_until_complete(racy_program())
    raw = "\n".join(json.dumps(e, sort_keys=True) for e in loop.events).encode()
    return final, hashlib.sha256(raw).hexdigest(), loop.events


def main() -> None:
    seeds = range(20)
    results = {}
    for seed in seeds:
        first = run_seed(seed)
        second = run_seed(seed)
        # (b) Replay: same seed, byte-identical event log and same result.
        assert first[1] == second[1], f"seed {seed}: log hashes differ across identical runs"
        assert first[0] == second[0], f"seed {seed}: results differ across identical runs"
        results[seed] = first

    finals = [r[0] for r in results.values()]
    hashes = [r[1] for r in results.values()]

    # (a) Exploration: different seeds produce different interleavings...
    assert len(set(hashes)) >= 2, "all seeds produced identical schedules"
    # ...and the differences are semantically visible (lost updates vary).
    assert len(set(finals)) >= 2, "schedules differed but outcomes did not"

    print("seed  final  log-sha256[:12]  steps")
    for seed, (final, digest, events) in results.items():
        print(f"{seed:>4}  {final:>5}  {digest[:12]}      {len(events)}")
    print(f"\ndistinct interleavings: {len(set(hashes))}/{len(hashes)}")
    print(f"final-value distribution: {dict(sorted(Counter(finals).items()))}")
    print("(correct, race-free answer would always be 9)")

    sample_seed = next(s for s, r in results.items() if r[0] != max(finals))
    print(f"\nfirst 6 events of seed {sample_seed} (lost updates present):")
    for event in results[sample_seed][2][:6]:
        print("  ", json.dumps(event, sort_keys=True))

    print("\nS1 PASS — seeded scheduling explores; same seed replays byte-identically.")


if __name__ == "__main__":
    main()
