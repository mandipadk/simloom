"""Phase I — world-on-by-default: the network is simulated even when main takes
no World parameter; world=False restores escape-on-real-network."""

from __future__ import annotations

import asyncio

import pytest

import simloom
from simloom import EscapedSimulationError


async def _serve_and_connect() -> bytes:
    """A no-arg main (no World parameter) that still uses the network."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.readline()
        writer.write(b"pong:" + data)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "svc.local", 8080)
    async with server:
        reader, writer = await asyncio.open_connection("svc.local", 8080)
        writer.write(b"ping\n")
        await writer.drain()
        reply = await asyncio.wait_for(reader.readline(), timeout=2.0)
        writer.close()
        return reply


class TestWorldByDefault:
    def test_no_arg_main_gets_a_simulated_network(self) -> None:
        # main() takes no World parameter, yet the network is simulated — no
        # arity trap, no escape.
        result = simloom.run(_serve_and_connect, seed=0)
        assert result.outcome == "ok"
        assert result.value == b"pong:ping\n"

    def test_deterministic_without_a_world_param(self) -> None:
        a = simloom.run(_serve_and_connect, seed=1)
        b = simloom.run(_serve_and_connect, seed=1)
        assert a.digest == b.digest

    def test_world_false_restores_escape(self) -> None:
        async def main() -> None:
            await asyncio.open_connection("svc.local", 8080)

        result = simloom.run(main, seed=0, world=False, raise_on_error=False)
        assert isinstance(result.error, EscapedSimulationError)
        assert result.error.api == "loop.create_connection"
        assert "world=False" in str(result.error)

    def test_world_false_with_a_world_param_is_an_error(self) -> None:
        async def main(world: simloom.World) -> None:
            await world.sleep(0)

        with pytest.raises(TypeError, match="world=False"):
            simloom.run(main, seed=0, world=False)

    def test_world_param_still_works(self) -> None:
        async def main(world: simloom.World) -> float:
            await world.sleep(1.0)
            return world.time

        assert simloom.run(main, seed=0).value == 1.0
