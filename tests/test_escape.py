"""Escape detection: the real world is unreachable from inside the sim."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

import simloom
from simloom import EscapedSimulationError

SYNC_ESCAPES: list[tuple[str, Callable[[asyncio.AbstractEventLoop], Any]]] = [
    ("add_reader", lambda loop: loop.add_reader(0, print)),
    ("remove_reader", lambda loop: loop.remove_reader(0)),
    ("add_writer", lambda loop: loop.add_writer(0, print)),
    ("remove_writer", lambda loop: loop.remove_writer(0)),
    ("add_signal_handler", lambda loop: loop.add_signal_handler(2, print)),
    ("remove_signal_handler", lambda loop: loop.remove_signal_handler(2)),
]

ASYNC_ESCAPES: list[tuple[str, Callable[[asyncio.AbstractEventLoop], Awaitable[Any]]]] = [
    ("getaddrinfo", lambda loop: loop.getaddrinfo("example.com", 80)),
    ("getnameinfo", lambda loop: loop.getnameinfo(("127.0.0.1", 80))),
    ("create_connection", lambda loop: loop.create_connection(object, "h", 80)),
    ("create_server", lambda loop: loop.create_server(object, "h", 80)),
    ("create_datagram_endpoint", lambda loop: loop.create_datagram_endpoint(object)),
    ("sock_connect", lambda loop: loop.sock_connect(None, ("h", 80))),  # type: ignore[arg-type]
    ("sock_recv", lambda loop: loop.sock_recv(None, 1)),  # type: ignore[arg-type]
    ("sock_sendall", lambda loop: loop.sock_sendall(None, b"x")),  # type: ignore[arg-type]
    ("subprocess_shell", lambda loop: loop.subprocess_shell(object, "true")),
    ("subprocess_exec", lambda loop: loop.subprocess_exec(object, "true")),
    ("connect_read_pipe", lambda loop: loop.connect_read_pipe(object, None)),
    ("start_tls", lambda loop: loop.start_tls(None, None, None)),  # type: ignore[arg-type]
]


@pytest.mark.parametrize(("api", "call"), SYNC_ESCAPES, ids=[a for a, _ in SYNC_ESCAPES])
def test_sync_apis_escape(api: str, call: Callable[[asyncio.AbstractEventLoop], Any]) -> None:
    async def main() -> None:
        call(asyncio.get_running_loop())

    with pytest.raises(EscapedSimulationError, match=api):
        simloom.run(main, seed=0, world=False)


@pytest.mark.parametrize(("api", "call"), ASYNC_ESCAPES, ids=[a for a, _ in ASYNC_ESCAPES])
def test_async_apis_escape(
    api: str, call: Callable[[asyncio.AbstractEventLoop], Awaitable[Any]]
) -> None:
    async def main() -> None:
        await call(asyncio.get_running_loop())

    with pytest.raises(EscapedSimulationError, match=api):
        simloom.run(main, seed=0, world=False)


def test_escape_carries_api_and_hint() -> None:
    async def main() -> None:
        await asyncio.get_running_loop().getaddrinfo("example.com", 80)

    result = simloom.run(main, seed=0, raise_on_error=False, world=False)
    assert isinstance(result.error, EscapedSimulationError)
    assert result.error.api == "loop.getaddrinfo"
    assert "determinism.md" in str(result.error)
    assert any(e["kind"] == "escape" for e in result.log.events)


def test_open_connection_helper_escapes() -> None:
    """The high-level helper bottoms out in loop.create_connection."""

    async def main() -> None:
        await asyncio.open_connection("example.com", 80)

    with pytest.raises(EscapedSimulationError, match="create_connection"):
        simloom.run(main, seed=0, world=False)
