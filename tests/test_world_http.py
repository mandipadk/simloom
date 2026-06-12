"""The Phase B gate (demo #2): real aiohttp serving a real httpx client,
unmodified, in-process, on the simulated network — with latency and loss
injected — and the whole thing replayable from the tape.
"""

from __future__ import annotations

from typing import Any

import pytest

import simloom
from simloom import World

pytest.importorskip("aiohttp")
pytest.importorskip("httpx")


async def http_workload(world: World) -> dict[str, Any]:
    import httpx
    from aiohttp import web

    hits = {"count": 0}

    async def hello(request: web.Request) -> web.Response:
        hits["count"] += 1
        return web.json_response({"message": "hello from in-sim aiohttp", "hit": hits["count"]})

    async def echo(request: web.Request) -> web.Response:
        body = await request.read()
        return web.Response(body=body[::-1])

    async def serve() -> None:
        app = web.Application()
        app.router.add_get("/hello", hello)
        app.router.add_post("/echo", echo)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host="app.sim", port=8080)
        await site.start()
        await world.sleep(1_000_000)

    world.net.set_latency(0.001, 0.030)
    world.net.set_loss(20)  # observed as retransmission delay, never corruption
    world.host("app").spawn(lambda: serve())
    await world.sleep(0.2)

    results: dict[str, Any] = {"t_start": world.time}
    async with httpx.AsyncClient() as client:
        first = await client.get("http://app.sim:8080/hello")
        second = await client.get("http://app.sim:8080/hello")
        echoed = await client.post("http://app.sim:8080/echo", content=b"simloom")
    results["hello1"] = (first.status_code, first.json())
    results["hello2"] = (second.status_code, second.json())
    results["echo"] = (echoed.status_code, echoed.content)
    results["duration"] = world.time - results.pop("t_start")
    results["loss_delays"] = world.net.chunks_delayed_by_loss
    return results


def test_demo2_unmodified_http_stack_with_faults() -> None:
    result = simloom.run(http_workload, seed=42)
    value = result.value
    assert value["hello1"] == (200, {"message": "hello from in-sim aiohttp", "hit": 1})
    assert value["hello2"] == (200, {"message": "hello from in-sim aiohttp", "hit": 2})
    assert value["echo"] == (200, b"moolmis")
    assert value["duration"] > 0  # virtual time passed; wall time is ~nothing


def test_demo2_replays_byte_identically() -> None:
    first = simloom.run(http_workload, seed=42)
    again = simloom.run(http_workload, seed=42)
    replayed = simloom.replay(http_workload, tape=first)
    assert first.digest == again.digest == replayed.digest
    assert replayed.value["echo"] == first.value["echo"]


def test_demo2_different_seeds_different_universes() -> None:
    a = simloom.run(http_workload, seed=1)
    b = simloom.run(http_workload, seed=2)
    assert a.digest != b.digest
    assert a.value["echo"] == b.value["echo"] == (200, b"moolmis")


def test_demo2_loss_actually_fires() -> None:
    delayed = [simloom.run(http_workload, seed=s).value["loss_delays"] for s in range(3)]
    assert any(d > 0 for d in delayed), "20% loss never triggered a retransmit delay"
