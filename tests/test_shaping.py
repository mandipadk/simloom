"""Phase I — per-link / per-node shaping: directional latency and loss, and
ingress/egress clogging. Real links are asymmetric; the fault matrix should be."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

import simloom


async def _rtt(world: simloom.World, shape: Callable[[simloom.World], None] | None) -> float:
    a = world.host("a")
    b = world.host("b")
    if shape is not None:
        shape(world)

    async def server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        line = await reader.readline()
        writer.write(b"r:" + line)
        await writer.drain()
        writer.close()

    await b.spawn(asyncio.start_server(server, "b.local", 80))

    result = {"rtt": 0.0}

    async def client() -> None:
        start = world.time
        reader, writer = await asyncio.open_connection("b.local", 80)
        writer.write(b"x\n")
        await writer.drain()
        await reader.readline()
        writer.close()
        result["rtt"] = world.time - start

    await a.spawn(client())
    await world.sleep(30.0)
    return result["rtt"]


def _run(shape: Callable[[simloom.World], None] | None, seed: int = 0) -> float:
    return simloom.run(lambda w: _rtt(w, shape), seed=seed).value


class TestShaping:
    def test_baseline_is_fast(self) -> None:
        assert _run(None) < 0.1

    def test_link_latency_is_directional(self) -> None:
        # slowing a->b adds ~1s one way; the b->a reply stays fast
        def slow_forward(w: simloom.World) -> None:
            w.net.set_link_latency(w.host("a"), w.host("b"), 1.0, 1.0)

        rtt = _run(slow_forward)
        assert 1.0 < rtt < 1.3  # one slow leg, one fast

        # the reverse direction is independent
        def slow_back(w: simloom.World) -> None:
            w.net.set_link_latency(w.host("b"), w.host("a"), 1.0, 1.0)

        assert 1.0 < _run(slow_back) < 1.3

    def test_clog_node_egress_and_ingress(self) -> None:
        def egress(w: simloom.World) -> None:
            w.net.clog_node_out(w.host("a"), latency=(0.5, 0.5))

        def ingress(w: simloom.World) -> None:
            w.net.clog_node_in(w.host("a"), latency=(0.5, 0.5))

        # egress slows the request leg; ingress slows the reply leg — both add ~0.5s
        assert _run(egress) > 0.4
        assert _run(ingress) > 0.4

    def test_per_link_loss_adds_retransmit_delay(self) -> None:
        def lossy(w: simloom.World) -> None:
            w.net.set_link_loss(w.host("a"), w.host("b"), 90)

        assert _run(lossy) > _run(None)

    def test_explicit_link_overrides_node_clog(self) -> None:
        # node-out says slow, but the explicit a->b link says fast: link wins
        def both(w: simloom.World) -> None:
            w.net.clog_node_out(w.host("a"), latency=(2.0, 2.0))
            w.net.set_link_latency(w.host("a"), w.host("b"), 0.0, 0.0)

        # a->b is fast (explicit), only the b->a reply rides the (default) fast path
        assert _run(both) < 0.5

    def test_validation(self) -> None:
        result = simloom.run(_validation_main, seed=0, raise_on_error=False)
        assert result.outcome == "ok"


async def _validation_main(world: simloom.World) -> None:
    a, b = world.host("a"), world.host("b")
    with pytest.raises(ValueError, match="latency range"):
        world.net.set_link_latency(a, b, 1.0, 0.5)  # min > max
    with pytest.raises(ValueError, match="loss percent"):
        world.net.set_link_loss(a, b, 101)
    with pytest.raises(ValueError, match="loss percent"):
        world.net.clog_node_out(a, loss=200)
    with pytest.raises(ValueError, match="latency range"):
        world.net.clog_node_in(a, latency=(2.0, 1.0))
