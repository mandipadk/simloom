"""Demo #4: reproducing a real, historical CPython race under simloom.

The bug: **asyncio.wait_for can hide cancellation in a rare race condition**
— bpo-42130, github.com/python/cpython/issues/86296. If the inner future
completes in the same event-loop window in which the outer task is
cancelled, the pre-3.12 ``wait_for`` catches the ``CancelledError`` raised
out of its waiter, sees the future done, and *returns the result* — the
outer task keeps running although it was successfully cancelled. The race
sat in CPython for years and was reported from production systems; fixed by
gh-26097 and ultimately by the 3.12 rewrite on ``asyncio.timeouts``.

Below, ``buggy_wait_for`` is the pre-fix implementation, vendored verbatim
in structure from CPython 3.10's ``Lib/asyncio/tasks.py`` (PSF license).
The invariant test says: if ``task.cancel()`` returned True, awaiting the
task must raise ``CancelledError``. simloom explores schedules around the
timeout boundary and finds the swallowing interleaving from a seed; the
modern stdlib ``asyncio.wait_for`` survives the identical torture.

Run:  uv run python examples/bpo42130.py
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import simloom

# --------------------------------------------------------------------------
# The historical implementation (CPython 3.10, Lib/asyncio/tasks.py, PSF
# license; comments trimmed). The bug is the marked branch.
# --------------------------------------------------------------------------


def _release_waiter(waiter: asyncio.Future[None], *args: object) -> None:
    if not waiter.done():
        waiter.set_result(None)


async def _cancel_and_wait(fut: asyncio.Future[Any], loop: asyncio.AbstractEventLoop) -> None:
    waiter = loop.create_future()
    cb = functools.partial(_release_waiter, waiter)
    fut.add_done_callback(cb)
    try:
        fut.cancel()
        await waiter
    finally:
        fut.remove_done_callback(cb)


async def buggy_wait_for(fut: Awaitable[Any], timeout: float) -> Any:
    """``asyncio.wait_for`` as shipped before the bpo-42130 fix."""
    loop = asyncio.get_running_loop()
    waiter = loop.create_future()
    timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    cb = functools.partial(_release_waiter, waiter)
    fut = asyncio.ensure_future(fut)
    fut.add_done_callback(cb)
    try:
        try:
            await waiter
        except asyncio.CancelledError:
            if fut.done():
                return fut.result()  # <-- bpo-42130: the cancellation vanishes
            fut.remove_done_callback(cb)
            await _cancel_and_wait(fut, loop=loop)
            raise
        if fut.done():
            return fut.result()
        fut.remove_done_callback(cb)
        await _cancel_and_wait(fut, loop=loop)
        try:
            return fut.result()
        except asyncio.CancelledError as exc:
            raise TimeoutError() from exc
    finally:
        timeout_handle.cancel()


# --------------------------------------------------------------------------
# The invariant harness: cancellation, once delivered, must not be swallowed.
# --------------------------------------------------------------------------

WaitFor = Callable[..., Awaitable[Any]]


def cancellation_invariant(wait_for: WaitFor) -> Callable[[], Coroutine[Any, Any, str]]:
    """A simloom test program: a victim awaits work under ``wait_for`` while
    a controller cancels it right around the moment the work completes."""

    async def main() -> str:
        loop = asyncio.get_running_loop()

        async def work() -> int:
            await asyncio.sleep(1.0)  # completes exactly at the timeout boundary
            return 42

        async def victim() -> int:
            value = await wait_for(work(), timeout=1.0)
            return int(value)

        task = loop.create_task(victim())
        await asyncio.sleep(1.0)  # the same virtual moment, tape-ordered
        delivered = task.cancel()
        try:
            result: int | None = await task
        except (asyncio.CancelledError, TimeoutError):
            result = None
        if delivered and result is not None:
            raise AssertionError(
                f"bpo-42130 reproduced: task.cancel() was delivered but the "
                f"task returned {result!r} — wait_for swallowed the cancellation"
            )
        return "cancelled-cleanly" if delivered else "completed-before-cancel"

    return main


if __name__ == "__main__":
    print("hunting the bpo-42130 interleaving in the historical wait_for ...")
    exploration = simloom.explore(cancellation_invariant(buggy_wait_for), runs=500)
    print(f"  {exploration.summary()}")
    failure = exploration.first_failure
    assert failure is not None
    replayed = simloom.replay(
        cancellation_invariant(buggy_wait_for), tape=failure, raise_on_error=False
    )
    print(f"  replays byte-identically: {replayed.digest == failure.digest}")

    print("the modern asyncio.wait_for under the identical torture ...")
    modern = simloom.explore(
        cancellation_invariant(asyncio.wait_for), runs=500, stop_on_failure=False
    )
    print(f"  {modern.summary()}")
