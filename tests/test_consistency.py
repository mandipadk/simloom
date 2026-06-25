"""Phase K — consistency checking: the headline "finds *wrong answers*".

An Elle-style list-append serializability checker over a recorded history. A
store with a lost update (a read-modify-write that isn't atomic) produces a
non-serializable history under some schedules; the checker reports it as a
dependency cycle, a correct (locked) store always passes, and the violating
history shrinks to a minimal two-step cycle."""

from __future__ import annotations

import asyncio

import simloom
from simloom import ConsistencyViolation, History, check_serializable


class TestChecker:
    def test_serial_history_passes(self) -> None:
        h = History()
        h.record([("read", "x", []), ("append", "x", "a")])
        h.record([("read", "x", ["a"]), ("append", "x", "b")])
        h.record([("read", "x", ["a", "b"])])
        assert check_serializable(h).ok

    def test_write_skew_is_a_cycle(self) -> None:
        h = History()
        h.record([("read", "x", []), ("append", "x", "a")])  # T0 reads stale
        h.record([("read", "x", []), ("append", "x", "b")])  # T1 reads stale
        h.record([("read", "x", ["a", "b"])])  # establishes the version order
        result = check_serializable(h)
        assert not result.ok
        assert set(result.cycle) == {0, 1}  # the two conflicting transactions
        assert "rw" in result.edge_types  # the anti-dependency that can't serialize

    def test_empty_history_passes(self) -> None:
        assert check_serializable(History()).ok

    def test_cycle_is_minimal(self) -> None:
        # even with a third transaction in the mix, the witness is the 2-cycle
        h = History()
        h.record([("read", "x", []), ("append", "x", "a")])
        h.record([("read", "x", []), ("append", "x", "b")])
        h.record([("read", "x", ["a"]), ("append", "x", "c")])
        h.record([("read", "x", ["a", "b", "c"])])
        result = check_serializable(h)
        assert not result.ok
        assert len(result.cycle) == 2


# --- a real workload: read-modify-write with and without atomicity -----------


def _make_workload(*, atomic: bool, writers: int = 2):
    async def workload(world: simloom.World) -> None:
        store: dict[str, list[str]] = {"x": []}
        lock = asyncio.Lock()

        async def rmw(value: str) -> list[str]:
            await asyncio.sleep(0)  # vary when the read happens
            observed = list(store["x"])
            await asyncio.sleep(0)  # the window a lost update needs
            store["x"].append(value)  # the append itself is atomic; the read isn't
            return observed

        async def writer(value: str, process: int) -> None:
            observed = await (_locked(lock, value, store) if atomic else rmw(value))
            with world.history.transaction(process=process) as txn:
                txn.read("x", observed)
                txn.append("x", value)

        await asyncio.gather(*(writer(chr(ord("a") + i), i) for i in range(writers)))
        with world.history.transaction(process=99) as txn:
            txn.read("x", list(store["x"]))  # final read -> version order
        world.assert_serializable()

    return workload


async def _locked(lock: asyncio.Lock, value: str, store: dict[str, list[str]]) -> list[str]:
    async with lock:  # the whole read-modify-write is atomic -> serializable
        await asyncio.sleep(0)
        observed = list(store["x"])
        await asyncio.sleep(0)
        store["x"].append(value)
        return observed


class TestGate:
    def test_correct_store_is_always_serializable(self) -> None:
        assert not simloom.explore(_make_workload(atomic=True), runs=120).failed

    def test_buggy_store_is_caught_as_a_violation(self) -> None:
        exploration = simloom.explore(_make_workload(atomic=False), runs=120)
        assert exploration.failed
        failure = exploration.first_failure
        assert failure is not None
        assert isinstance(failure.error, ConsistencyViolation)
        assert failure.error.cycle  # the cyclic read/write dependency
        assert "rw" in failure.error.edge_types

    def test_violation_replays_and_shrinks_to_a_minimal_cycle(self) -> None:
        workload = _make_workload(atomic=False)
        exploration = simloom.explore(workload, runs=120, stop_on_failure=True)
        failure = exploration.first_failure
        assert failure is not None

        replayed = simloom.replay(workload, tape=failure, raise_on_error=False)
        assert isinstance(replayed.error, ConsistencyViolation)
        assert replayed.digest == failure.digest

        shrunk = simloom.shrink(workload, failure)
        assert len(shrunk.tape) <= len(failure.tape)
        minimized = simloom.replay(workload, tape=shrunk.tape, raise_on_error=False)
        assert isinstance(minimized.error, ConsistencyViolation)
        assert len(minimized.error.cycle) == 2  # a minimal two-step cycle


class TestWorldApi:
    def test_check_serializable_returns_a_result(self) -> None:
        async def main(world: simloom.World) -> bool:
            with world.history.transaction() as t:
                t.read("x", [])
                t.append("x", "v")
            return world.check_serializable().ok

        assert simloom.run(main, seed=0).value is True
