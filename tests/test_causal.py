"""Phase J — causal event log v2: every step records *why* it woke (the
happens-before edge to the step that scheduled it). Opt-in (``causal=True``),
so the default path is byte-identical."""

from __future__ import annotations

import asyncio

import simloom


async def producer_consumer_net(world: simloom.World) -> None:
    """A producer/consumer over a queue, a sleep, and one connection — the
    program the gate names."""
    queue: asyncio.Queue[int] = asyncio.Queue()

    async def producer() -> None:
        for i in range(3):
            await asyncio.sleep(0.05)  # timer-caused wakeups
            await queue.put(i)

    async def consumer() -> None:
        for _ in range(3):
            await queue.get()  # woken by the producer's put

    class Echo(asyncio.Protocol):
        def __init__(self) -> None:
            self.transport: asyncio.Transport | None = None

        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            assert isinstance(transport, asyncio.Transport)
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            if self.transport is not None:
                self.transport.write(b"echo:" + data)  # delivery is network-caused

    (client_t, _), (_st, _sp) = world.connected_pair(Echo, Echo)
    client_t.write(b"hello")
    await asyncio.gather(producer(), consumer())
    await world.sleep(0.5)
    client_t.close()


def _steps(result: simloom.RunResult) -> list[dict]:
    return [e for e in result.log.events if e["kind"] == "step"]


class TestCausalLog:
    def test_off_is_byte_identical(self) -> None:
        a = simloom.run(producer_consumer_net, seed=0)
        b = simloom.run(producer_consumer_net, seed=0, causal=False)
        assert a.digest == b.digest
        assert all("woke_by" not in e for e in _steps(a))

    def test_on_annotates_every_step(self) -> None:
        r = simloom.run(producer_consumer_net, seed=0, causal=True)
        steps = _steps(r)
        assert steps
        assert all("via" in e and "woke_by" in e for e in steps)
        assert {"soon", "timer", "root"} >= {e["via"] for e in steps}

    def test_on_is_deterministic_and_replayable(self) -> None:
        a = simloom.run(producer_consumer_net, seed=1, causal=True)
        b = simloom.run(producer_consumer_net, seed=1, causal=True)
        replay = simloom.replay(producer_consumer_net, tape=a, causal=True)
        assert a.digest == b.digest == replay.digest


class TestHappensBeforeOracle:
    """The recorded `woke_by` must satisfy an independent happens-before oracle:
    causes precede effects, every step traces back to a root, and asyncio's own
    temporal law holds — an immediate (`soon`) wakeup runs at the *same* virtual
    instant as its cause, while a `timer` wakeup runs at or after it."""

    def test_causes_precede_effects(self) -> None:
        steps = _steps(simloom.run(producer_consumer_net, seed=0, causal=True))
        for index, event in enumerate(steps):
            if event["woke_by"] is not None:
                assert event["woke_by"] < index  # acyclic: the cause is earlier

    def test_every_step_traces_to_a_root(self) -> None:
        steps = _steps(simloom.run(producer_consumer_net, seed=0, causal=True))
        for start in range(len(steps)):
            seen = set()
            cursor: int | None = start
            while cursor is not None:
                assert cursor not in seen  # no cycles on the way to a root
                seen.add(cursor)
                cursor = steps[cursor]["woke_by"]
            # terminates at a root (woke_by is None)

    def test_temporal_law_of_wakeups(self) -> None:
        steps = _steps(simloom.run(producer_consumer_net, seed=0, causal=True))
        for event in steps:
            cause = event["woke_by"]
            if cause is None:
                continue
            cause_t = steps[cause]["t"]
            if event["via"] == "soon":
                # An immediate callback runs at the same virtual instant.
                assert event["t"] == cause_t
            else:  # timer
                assert event["t"] >= cause_t

    def test_specific_causal_edges_exist(self) -> None:
        steps = _steps(simloom.run(producer_consumer_net, seed=0, causal=True))
        # the sleeps produce timer-caused wakeups whose cause is an earlier step
        assert any(e["via"] == "timer" and e["woke_by"] is not None for e in steps)
        # the network delivery is a cross-instant (timer) edge: data arrives later
        deliveries = [e for e in steps if "echo" not in e["ran"] and e["via"] == "timer"]
        assert deliveries
        # the first step is a root (nothing earlier scheduled it)
        assert steps[0]["woke_by"] is None
        # every root has no cause; roots are scheduled before the loop runs
        assert all(e["woke_by"] is None for e in steps if e["via"] == "root")
