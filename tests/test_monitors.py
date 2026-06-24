"""Phase F — property monitors: safety (always), liveness (eventually /
leads_to), the livelock oracle, and convergence. Edge cases included."""

from __future__ import annotations

import asyncio

import pytest

import simloom
from oracle_zoo import livelock_spin, mutex_correct, mutex_safety
from simloom import InvariantViolation, SimDeadlockError, SimLivelockError


# --------------------------------------------------------------------------
# always — safety
# --------------------------------------------------------------------------
class TestAlways:
    def test_violation_is_caught_with_kind_and_time(self) -> None:
        result = None
        for seed in range(200):
            r = simloom.run(mutex_safety, seed=seed, raise_on_error=False)
            if r.outcome == "error":
                result = r
                break
        assert result is not None, "no schedule violated the mutex in 200 seeds"
        assert isinstance(result.error, InvariantViolation)
        assert result.error.kind == "safety"
        assert result.error.label == "mutex"

    def test_verdict_is_in_the_event_log(self) -> None:
        r = next(
            r
            for s in range(200)
            if (r := simloom.run(mutex_safety, seed=s, raise_on_error=False)).outcome == "error"
        )
        events = list(r.log.events)
        verdicts = [e for e in events if e["kind"] == "invariant"]
        assert len(verdicts) == 1
        verdict = verdicts[0]
        assert verdict["label"] == "mutex"
        assert verdict["category"] == "safety"
        # the verdict is emitted mid-run, with a step before it (the step that
        # broke the invariant) — teardown's cleanup events come after.
        verdict_seq = verdict["seq"]
        assert any(e["kind"] == "step" and e["seq"] < verdict_seq for e in events)

    def test_violation_replays_byte_identically(self) -> None:
        r = next(
            r
            for s in range(200)
            if (r := simloom.run(mutex_safety, seed=s, raise_on_error=False)).outcome == "error"
        )
        replayed = simloom.replay(mutex_safety, tape=r, raise_on_error=False)
        assert replayed.digest == r.digest
        assert replayed.outcome == "error"
        assert isinstance(replayed.error, InvariantViolation)

    def test_passing_monitor_is_zero_perturbation(self) -> None:
        # Same program, toggling only whether a never-failing monitor exists.
        register = {"on": False}

        async def main() -> None:
            if register["on"]:
                simloom.always("ok", lambda: True)
            box = {"v": 0}

            async def w() -> None:
                for _ in range(3):
                    c = box["v"]
                    await asyncio.sleep(0)
                    box["v"] = c + 1

            await asyncio.gather(*(w() for _ in range(3)))

        register["on"] = False
        without = simloom.run(main, seed=11)
        register["on"] = True
        withmon = simloom.run(main, seed=11)
        assert withmon.digest == without.digest  # byte-identical

    def test_correct_implementation_never_fires(self) -> None:
        for seed in range(100):
            assert simloom.run(mutex_correct, seed=seed).outcome == "ok"

    def test_first_of_several_monitors_wins_deterministically(self) -> None:
        async def main() -> None:
            state = {"a": True, "b": True}
            simloom.always("a", lambda: state["a"])
            simloom.always("b", lambda: state["b"])
            state["a"] = False
            state["b"] = False
            await asyncio.sleep(0)

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, InvariantViolation)
        assert r.error.label == "a"  # registration order

    def test_async_predicate_rejected(self) -> None:
        async def main() -> None:
            async def pred() -> bool:
                return True

            simloom.always("bad", pred)  # type: ignore[arg-type]

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, TypeError)

    def test_empty_label_rejected(self) -> None:
        async def main() -> None:
            simloom.always("", lambda: True)

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, ValueError)

    def test_predicate_cannot_draw_from_the_tape(self) -> None:
        async def main() -> None:
            simloom.always("bad", lambda: bool(simloom.sometimes("x")) or True)
            await asyncio.sleep(0)

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, RuntimeError)
        assert "monitor" in str(r.error)

    def test_raising_predicate_surfaces_and_does_not_wedge(self) -> None:
        async def main() -> None:
            simloom.always("boom", lambda: 1 // 0 == 0)
            await asyncio.sleep(0)

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, ZeroDivisionError)

    def test_world_method_and_module_function_are_equivalent(self) -> None:
        async def via_world(world: simloom.World) -> None:
            world.always("w", lambda: False)
            await asyncio.sleep(0)

        async def via_module() -> None:
            simloom.always("m", lambda: False)
            await asyncio.sleep(0)

        rw = simloom.run(via_world, seed=0, raise_on_error=False)
        rm = simloom.run(via_module, seed=0, raise_on_error=False)
        assert isinstance(rw.error, InvariantViolation)
        assert rw.error.label == "w"
        assert isinstance(rm.error, InvariantViolation)
        assert rm.error.label == "m"


# --------------------------------------------------------------------------
# eventually — liveness
# --------------------------------------------------------------------------
class TestEventually:
    def test_never_satisfied_fires_at_exact_deadline(self) -> None:
        async def main() -> None:
            simloom.eventually("done", lambda: False, within=5.0)
            for _ in range(20):
                await asyncio.sleep(1.0)  # busy past the deadline

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, InvariantViolation)
        assert r.error.kind == "liveness"
        assert r.error.t == 5.0
        # replays identically
        assert simloom.replay(main, tape=r, raise_on_error=False).digest == r.digest

    def test_satisfied_before_deadline_passes(self) -> None:
        async def main() -> None:
            flag = {"v": False}
            simloom.eventually("done", lambda: flag["v"], within=100.0)

            async def setter() -> None:
                await asyncio.sleep(3.0)
                flag["v"] = True

            asyncio.get_running_loop().create_task(setter())
            await asyncio.sleep(10.0)

        assert simloom.run(main, seed=0).outcome == "ok"

    def test_satisfied_exactly_at_deadline_passes(self) -> None:
        # Inclusive `within`: an event at the deadline that satisfies the goal
        # runs before the deadline is enforced.
        async def main() -> None:
            flag = {"v": False}
            simloom.eventually("done", lambda: flag["v"], within=5.0)

            async def setter() -> None:
                await asyncio.sleep(5.0)
                flag["v"] = True

            asyncio.get_running_loop().create_task(setter())
            await asyncio.sleep(10.0)

        assert simloom.run(main, seed=0).outcome == "ok"

    def test_unmet_at_run_end_is_a_violation(self) -> None:
        async def main() -> None:
            simloom.eventually("done", lambda: False, within=100.0)
            await asyncio.sleep(5.0)  # run ends at 5, well before the deadline

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, InvariantViolation)
        assert r.error.kind == "liveness"
        assert r.error.t == 5.0

    def test_quiescence_before_deadline_surfaces_deadlock_not_liveness(self) -> None:
        async def main() -> None:
            simloom.eventually("done", lambda: False, within=100.0)
            await asyncio.get_running_loop().create_future()  # block forever at t=0

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, SimDeadlockError)  # not masked by the monitor

    def test_within_must_be_positive(self) -> None:
        async def main() -> None:
            simloom.eventually("x", lambda: True, within=0.0)

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, ValueError)

    def test_earliest_deadline_fires_first(self) -> None:
        async def main() -> None:
            simloom.eventually("late", lambda: False, within=50.0)
            simloom.eventually("early", lambda: False, within=5.0)
            for _ in range(100):
                await asyncio.sleep(1.0)

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, InvariantViolation)
        assert r.error.label == "early"
        assert r.error.t == 5.0


# --------------------------------------------------------------------------
# leads_to — response
# --------------------------------------------------------------------------
class TestLeadsTo:
    def test_response_within_deadline_passes(self) -> None:
        async def main() -> None:
            st = {"req": False, "resp": False}
            simloom.leads_to("r", lambda: st["req"], lambda: st["resp"], within=10.0)
            st["req"] = True

            async def respond() -> None:
                await asyncio.sleep(3.0)
                st["resp"] = True

            asyncio.get_running_loop().create_task(respond())
            await asyncio.sleep(20.0)

        assert simloom.run(main, seed=0).outcome == "ok"

    def test_no_response_fires_at_deadline(self) -> None:
        async def main() -> None:
            st = {"req": False, "resp": False}
            simloom.leads_to("r", lambda: st["req"], lambda: st["resp"], within=5.0)
            st["req"] = True
            for _ in range(20):
                await asyncio.sleep(1.0)

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, InvariantViolation)
        assert r.error.kind == "liveness"
        assert r.error.t == 5.0

    def test_trigger_with_immediate_response_opens_no_obligation(self) -> None:
        async def main() -> None:
            st = {"on": True}
            simloom.leads_to("r", lambda: st["on"], lambda: st["on"], within=1.0)
            await asyncio.sleep(100.0)

        assert simloom.run(main, seed=0).outcome == "ok"

    def test_within_must_be_positive(self) -> None:
        async def main() -> None:
            simloom.leads_to("r", lambda: True, lambda: True, within=-1.0)

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, ValueError)


# --------------------------------------------------------------------------
# livelock
# --------------------------------------------------------------------------
class TestLivelock:
    def test_spin_is_detected(self) -> None:
        r = simloom.run(livelock_spin, seed=0, raise_on_error=False, max_steps_per_instant=2000)
        assert isinstance(r.error, SimLivelockError)

    def test_legitimate_large_fanout_is_not_flagged(self) -> None:
        async def main() -> int:
            # 500 tasks all at t=0 — many steps at one instant, but finite.
            async def w(n: int) -> int:
                await asyncio.sleep(0)
                return n

            return sum(await asyncio.gather(*(w(i) for i in range(500))))

        assert simloom.run(main, seed=0, max_steps_per_instant=100_000).value == sum(range(500))

    def test_threshold_is_configurable_and_deterministic(self) -> None:
        a = simloom.run(livelock_spin, seed=0, raise_on_error=False, max_steps_per_instant=500)
        b = simloom.run(livelock_spin, seed=0, raise_on_error=False, max_steps_per_instant=500)
        assert isinstance(a.error, SimLivelockError)
        assert a.digest == b.digest


# --------------------------------------------------------------------------
# convergence
# --------------------------------------------------------------------------
class TestConvergence:
    def test_divergent_hosts_fire(self) -> None:
        async def main(world: simloom.World) -> None:
            a, b = world.host("a"), world.host("b")
            a.disk.write("k", b"x")
            a.disk.fsync()
            b.disk.write("k", b"y")
            b.disk.fsync()
            world.assert_converged([a, b], key=lambda h: h.disk.read("k"))

        r = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(r.error, InvariantViolation)
        assert r.error.kind == "convergence"

    def test_converged_hosts_pass(self) -> None:
        async def main(world: simloom.World) -> None:
            hosts = [world.host(f"n{i}") for i in range(3)]
            for h in hosts:
                h.disk.write("k", b"same")
                h.disk.fsync()
            world.assert_converged(hosts, key=lambda h: h.disk.read("k"))

        assert simloom.run(main, seed=0).outcome == "ok"

    def test_fewer_than_two_hosts_trivially_converge(self) -> None:
        async def main(world: simloom.World) -> None:
            world.assert_converged([world.host("solo")], key=lambda h: h.name)

        assert simloom.run(main, seed=0).outcome == "ok"


# --------------------------------------------------------------------------
# integration with explore / shrink / @simloom.test
# --------------------------------------------------------------------------
class TestIntegration:
    def test_explore_finds_and_shrink_minimizes_a_safety_violation(self) -> None:
        exploration = simloom.explore(mutex_safety, runs=300)
        assert exploration.failed
        first = exploration.first_failure
        assert first is not None
        assert isinstance(first.error, InvariantViolation)
        shrunk = simloom.shrink(mutex_safety, first)
        assert shrunk.result.outcome == "error"
        assert isinstance(shrunk.result.error, InvariantViolation)
        # the shrunk artifact still reproduces under strict replay
        replayed = simloom.replay(mutex_safety, tape=shrunk.result, raise_on_error=False)
        assert replayed.digest == shrunk.result.digest

    def test_pytest_plugin_reports_a_monitor_violation(self, pytester: pytest.Pytester) -> None:
        pytester.makepyfile(
            """
            import asyncio
            import simloom

            @simloom.test(runs=300)
            async def test_mutex():
                state = {"in_cs": 0, "locked": False}
                simloom.always("mutex", lambda: state["in_cs"] <= 1)

                async def contender(stagger):
                    for _ in range(stagger + 1):
                        await asyncio.sleep(0)
                    if not state["locked"]:
                        await asyncio.sleep(0)
                        state["locked"] = True
                        state["in_cs"] += 1
                        await asyncio.sleep(0)
                        state["in_cs"] -= 1
                        state["locked"] = False

                await asyncio.gather(*(contender(i) for i in range(4)))
            """
        )
        result = pytester.runpytest("-p", "no:cacheprovider")
        result.assert_outcomes(failed=1)
        assert "simloom found a failing universe" in result.stdout.str()
        assert "InvariantViolation" in result.stdout.str()


pytest_plugins = ["pytester"]
