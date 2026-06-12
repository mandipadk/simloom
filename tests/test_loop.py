"""SimLoop behavior: virtual time, scheduling, lifecycle, teardown."""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from typing import Any

import pytest

import simloom
from simloom import (
    EscapedSimulationError,
    MisalignmentPolicy,
    SimDeadlockError,
    TapeMisalignmentError,
    UnhandledExceptionError,
)


class TestBasics:
    def test_returns_value(self) -> None:
        async def main() -> str:
            return "hello"

        assert simloom.run(main, seed=0).value == "hello"

    def test_exceptions_raise_by_default(self) -> None:
        async def main() -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            simloom.run(main, seed=0)

    def test_exceptions_captured_on_request(self) -> None:
        async def main() -> None:
            raise RuntimeError("boom")

        result = simloom.run(main, seed=0, raise_on_error=False)
        assert result.outcome == "error"
        assert isinstance(result.error, RuntimeError)
        assert result.log.events[-1]["kind"] == "run_end"
        assert result.log.events[-1]["error"] == "RuntimeError"

    def test_rejects_coroutine_object(self) -> None:
        async def main() -> None:
            pass

        coro = main()
        with pytest.raises(TypeError, match=r"not a.*coroutine object"):
            simloom.run(coro, seed=0)  # type: ignore[arg-type]
        coro.close()

    def test_rejects_non_callable(self) -> None:
        with pytest.raises(TypeError, match="callable"):
            simloom.run("nope", seed=0)  # type: ignore[arg-type]

    def test_nested_run_rejected(self) -> None:
        async def main() -> None:
            async def inner() -> None:
                pass

            simloom.run(inner, seed=0)

        with pytest.raises(RuntimeError, match=r"another (event )?loop"):
            simloom.run(main, seed=0)


class TestVirtualTime:
    def test_sleep_advances_exactly(self) -> None:
        async def main() -> float:
            loop = asyncio.get_running_loop()
            start = loop.time()
            await asyncio.sleep(123.456)
            return loop.time() - start

        assert simloom.run(main, seed=0).value == pytest.approx(123.456)

    def test_simulated_hours_in_wall_milliseconds(self) -> None:
        async def main() -> float:
            for _ in range(100):
                await asyncio.sleep(36.0)
            return asyncio.get_running_loop().time()

        wall_start = time.perf_counter()
        result = simloom.run(main, seed=0)
        wall = time.perf_counter() - wall_start
        assert result.value == pytest.approx(3600.0)
        assert wall < 1.0

    def test_epoch_respected(self) -> None:
        async def main() -> float:
            return asyncio.get_running_loop().time()

        assert simloom.run(main, seed=0, epoch=1_000_000.0).value == 1_000_000.0

    def test_clock_jumps_logged(self) -> None:
        async def main() -> None:
            await asyncio.sleep(5)

        result = simloom.run(main, seed=0)
        jumps = [e for e in result.log.events if e["kind"] == "clock_jump"]
        assert jumps
        assert jumps[0]["t"] == pytest.approx(5.0)

    def test_timers_fire_in_order_along_virtual_time(self) -> None:
        async def main() -> list[int]:
            fired: list[int] = []
            loop = asyncio.get_running_loop()
            loop.call_later(3.0, fired.append, 3)
            loop.call_later(1.0, fired.append, 1)
            loop.call_later(2.0, fired.append, 2)
            await asyncio.sleep(10)
            return fired

        assert simloom.run(main, seed=0).value == [1, 2, 3]


class TestDeterministicNaming:
    def test_loop_owned_names(self) -> None:
        async def main() -> list[str]:
            async def child() -> None:
                await asyncio.sleep(0)

            tasks = [asyncio.get_running_loop().create_task(child()) for _ in range(3)]
            names = [t.get_name() for t in tasks]
            await asyncio.gather(*tasks)
            return names

        names = simloom.run(main, seed=0).value
        # task-0 is the main task itself.
        assert names == ["task-1", "task-2", "task-3"]

    def test_explicit_names_kept(self) -> None:
        async def main() -> str:
            async def child() -> None:
                pass

            task = asyncio.get_running_loop().create_task(child(), name="Task-mine")
            await task
            return task.get_name()

        assert simloom.run(main, seed=0).value == "Task-mine"


class TestDeadlock:
    def test_bare_future_deadlocks(self) -> None:
        async def main() -> None:
            await asyncio.get_running_loop().create_future()

        with pytest.raises(SimDeadlockError, match="task-0"):
            simloom.run(main, seed=0)

    def test_deadlock_lists_all_waiters(self) -> None:
        async def main() -> None:
            forever = asyncio.get_running_loop().create_future()

            async def waiter() -> None:
                await forever

            task = asyncio.get_running_loop().create_task(waiter(), name="stuck-waiter")
            del task
            await forever

        with pytest.raises(SimDeadlockError, match="stuck-waiter") as info:
            simloom.run(main, seed=0)
        assert "quiescent" in str(info.value)

    def test_deadlock_event_logged(self) -> None:
        async def main() -> None:
            await asyncio.get_running_loop().create_future()

        result = simloom.run(main, seed=0, raise_on_error=False)
        assert result.outcome == "error"
        assert any(e["kind"] == "deadlock" for e in result.log.events)


class TestTeardown:
    def test_leftover_tasks_cancelled_with_finally(self) -> None:
        cleaned: list[str] = []

        async def main() -> None:
            async def background(name: str) -> None:
                try:
                    await asyncio.sleep(10_000)
                finally:
                    cleaned.append(name)

            loop = asyncio.get_running_loop()
            keep = [
                loop.create_task(background("b"), name="bg-b"),
                loop.create_task(background("a"), name="bg-a"),
            ]
            await asyncio.sleep(0)
            assert len(keep) == 2  # alive at main exit; teardown cancels them

        simloom.run(main, seed=0)
        # Both finally blocks ran; their order is tape-chosen, so the
        # guarantee is determinism, not sortedness.
        assert set(cleaned) == {"a", "b"}
        first_order = list(cleaned)
        cleaned.clear()
        simloom.run(main, seed=0)
        assert cleaned == first_order

    def test_asyncgen_closed_at_shutdown(self) -> None:
        closed: list[bool] = []

        async def main() -> None:
            async def generator() -> Any:
                try:
                    while True:
                        yield 1
                finally:
                    closed.append(True)

            agen = generator()
            assert await anext(agen) == 1
            # left open deliberately

        simloom.run(main, seed=0)
        assert closed == [True]


class TestUnhandledExceptions:
    def test_orphaned_failure_fails_the_run(self) -> None:
        async def main() -> None:
            async def doomed() -> None:
                raise ValueError("orphaned")

            asyncio.get_running_loop().create_task(doomed())
            await asyncio.sleep(1)
            # the task object is unreferenced and its exception never retrieved

        with pytest.raises(UnhandledExceptionError) as info:
            simloom.run(main, seed=0)
        assert isinstance(info.value.__cause__, ValueError)

    def test_record_mode_keeps_outcome_ok(self) -> None:
        async def main() -> str:
            async def doomed() -> None:
                raise ValueError("orphaned")

            asyncio.get_running_loop().create_task(doomed())
            await asyncio.sleep(1)
            return "done"

        result = simloom.run(main, seed=0, on_unhandled="record")
        assert result.outcome == "ok"
        assert result.value == "done"
        assert any(e["kind"] == "unhandled_exception" for e in result.log.events)

    def test_awaited_failure_is_not_unhandled(self) -> None:
        async def main() -> str:
            async def doomed() -> None:
                raise ValueError("caught")

            task = asyncio.get_running_loop().create_task(doomed())
            try:
                await task
            except ValueError:
                return "caught it"
            return "missed"

        assert simloom.run(main, seed=0).value == "caught it"


class TestExecutor:
    def test_runs_inline(self) -> None:
        async def main() -> int:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: 6 * 7)

        assert simloom.run(main, seed=0).value == 42

    def test_exceptions_propagate(self) -> None:
        def explode() -> None:
            raise OSError("disk on fire")

        async def main() -> None:
            await asyncio.get_running_loop().run_in_executor(None, explode)

        with pytest.raises(OSError, match="disk on fire"):
            simloom.run(main, seed=0)


class TestThreadBoundary:
    def test_same_thread_call_soon_threadsafe_allowed(self) -> None:
        async def main() -> int:
            box: list[int] = []
            asyncio.get_running_loop().call_soon_threadsafe(box.append, 1)
            while not box:  # noqa: ASYNC110 — the tape decides when it runs
                await asyncio.sleep(0)
            return box[0]

        assert simloom.run(main, seed=0).value == 1

    def test_foreign_thread_injection_escapes(self) -> None:
        captured: list[BaseException] = []

        async def main() -> None:
            loop = asyncio.get_running_loop()

            def from_thread() -> None:
                try:
                    loop.call_soon_threadsafe(print)
                except BaseException as exc:
                    captured.append(exc)

            thread = threading.Thread(target=from_thread)
            thread.start()
            thread.join()  # blocking join: brief and bounded, test-only

        simloom.run(main, seed=0)
        assert len(captured) == 1
        assert isinstance(captured[0], EscapedSimulationError)


class TestStdlibCompat:
    def test_event_queue_lock(self) -> None:
        async def main() -> list[int]:
            queue: asyncio.Queue[int] = asyncio.Queue(maxsize=2)
            gate = asyncio.Event()
            lock = asyncio.Lock()
            consumed: list[int] = []

            async def producer() -> None:
                for i in range(5):
                    await queue.put(i)
                gate.set()

            async def consumer() -> None:
                for _ in range(5):
                    item = await queue.get()
                    async with lock:
                        consumed.append(item)

            await asyncio.gather(producer(), consumer())
            await gate.wait()
            return consumed

        assert simloom.run(main, seed=3).value == [0, 1, 2, 3, 4]

    def test_wait_for_times_out_in_virtual_time(self) -> None:
        async def main() -> str:
            try:
                await asyncio.wait_for(asyncio.sleep(100), timeout=5)
            except TimeoutError:
                return f"timed out at t={asyncio.get_running_loop().time()}"
            return "no timeout"

        assert simloom.run(main, seed=0).value == "timed out at t=5.0"

    def test_timeout_context_manager(self) -> None:
        async def main() -> bool:
            try:
                async with asyncio.timeout(1.5):
                    await asyncio.sleep(10)
            except TimeoutError:
                return True
            return False

        assert simloom.run(main, seed=0).value is True

    def test_shield(self) -> None:
        async def main() -> str:
            inner = asyncio.get_running_loop().create_task(asyncio.sleep(2, "survived"))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.shield(inner), timeout=1)
            return await inner

        assert simloom.run(main, seed=0).value == "survived"

    def test_taskgroup(self) -> None:
        async def main() -> list[int]:
            results: list[int] = []

            async def job(n: int) -> None:
                await asyncio.sleep(n * 0.1)
                results.append(n)

            async with asyncio.TaskGroup() as group:
                for n in (3, 1, 2):
                    group.create_task(job(n))
            return results

        assert simloom.run(main, seed=0).value == [1, 2, 3]


class TestReplayDivergence:
    def test_replay_against_different_program_errors(self) -> None:
        def fan_out(width: int) -> Any:
            async def main() -> None:
                async def child() -> None:
                    await asyncio.sleep(0)
                    await asyncio.sleep(0)

                await asyncio.gather(*(child() for _ in range(width)))

            return main

        recorded = simloom.run(fan_out(6), seed=11)
        with pytest.raises(TapeMisalignmentError):
            simloom.replay(fan_out(2), tape=recorded)

    def test_fallback_policy_completes(self) -> None:
        def fan_out(width: int) -> Any:
            async def main() -> int:
                async def child() -> None:
                    await asyncio.sleep(0)

                await asyncio.gather(*(child() for _ in range(width)))
                return width

            return main

        recorded = simloom.run(fan_out(6), seed=11)
        result = simloom.replay(fan_out(2), tape=recorded, policy=MisalignmentPolicy.FALLBACK)
        assert result.value == 2


class TestEventLogContents:
    def test_expected_kinds_present(self) -> None:
        async def main() -> None:
            async def child() -> None:
                await asyncio.sleep(1)

            await asyncio.get_running_loop().create_task(child())

        result = simloom.run(main, seed=0)
        kinds = {e["kind"] for e in result.log.events}
        assert {"run_start", "task_created", "step", "clock_jump", "task_done", "run_end"} <= kinds

    def test_seq_monotone_and_time_non_decreasing(self) -> None:
        async def main() -> None:
            for _ in range(10):
                await asyncio.sleep(0.5)

        result = simloom.run(main, seed=0)
        events = result.log.events
        assert [e["seq"] for e in events] == list(range(len(events)))
        times = [float(e["t"]) for e in events]
        assert times == sorted(times)

    def test_no_memory_addresses_in_log(self) -> None:
        async def main() -> None:
            async def child() -> None:
                await asyncio.sleep(0)

            await asyncio.gather(*(child() for _ in range(3)))

        result = simloom.run(main, seed=0)
        assert "0x" not in result.log.to_jsonl()

    def test_metadata_in_header_not_digest(self) -> None:
        async def main() -> None:
            await asyncio.sleep(0)

        a = simloom.run(main, seed=0)
        b = simloom.run(main, seed=0)
        b.log.metadata["extra"] = "different"
        assert a.digest == b.digest
        assert a.log.header()["seed"] == 0
