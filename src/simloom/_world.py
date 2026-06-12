"""The simulated world: hosts, their disks, and the network between them.

A ``World`` is created per run and handed to the program under test. Hosts
are containers for tasks with crash/restart semantics and a durable disk;
the network (``world.net``) wires unmodified asyncio clients and servers
together with tape-driven latency and loss.

Crash semantics (the place naive implementations go wrong): ``crash()`` is a
power cut, not a shutdown. The host's tasks are never scheduled again — no
``CancelledError``, no ``finally`` blocks, no context-manager ``__aexit__``
during the simulation. The loop holds strong references to the abandoned
tasks so garbage collection cannot run their cleanup mid-run; when the
simulated universe has ended, teardown revives and cancels them at a
deterministic point. Unsynced disk writes are lost; the peer of every open
connection observes a reset.
"""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from ._context import current_host
from ._net import SimNetwork
from ._tape import Tape

if TYPE_CHECKING:
    from ._loop import SimLoop


class SimDisk:
    """A host's durable storage, with honest fsync semantics.

    ``write()`` lands in the buffer cache; only ``fsync()`` makes it
    durable. A crash drops everything unsynced — exactly the discipline real
    storage demands and tests almost never exercise. Reads see the buffered
    view (the OS page cache), so a program can be consistent with itself
    while being one power cut away from losing data.

    The API is deliberately explicit (no ``open()`` interception): code under
    test talks to ``host.disk``. Real file I/O bypasses the simulation
    undetectably — see docs/determinism.md.
    """

    def __init__(self) -> None:
        self._synced: dict[str, bytes] = {}
        self._buffered: dict[str, bytes | None] = {}  # None marks a pending delete

    def write(self, path: str, data: bytes) -> None:
        self._buffered[path] = bytes(data)

    def read(self, path: str) -> bytes:
        if path in self._buffered:
            buffered = self._buffered[path]
            if buffered is None:
                raise FileNotFoundError(path)
            return buffered
        if path in self._synced:
            return self._synced[path]
        raise FileNotFoundError(path)

    def exists(self, path: str) -> bool:
        if path in self._buffered:
            return self._buffered[path] is not None
        return path in self._synced

    def delete(self, path: str) -> None:
        if not self.exists(path):
            raise FileNotFoundError(path)
        self._buffered[path] = None

    def fsync(self, path: str | None = None) -> None:
        """Make ``path`` (or everything) durable."""
        if path is not None:
            if path in self._buffered:
                self._commit(path, self._buffered.pop(path))
            return
        for name, data in list(self._buffered.items()):
            self._commit(name, data)
        self._buffered.clear()

    def _commit(self, path: str, data: bytes | None) -> None:
        if data is None:
            self._synced.pop(path, None)
        else:
            self._synced[path] = data

    def files(self) -> list[str]:
        visible = set(self._synced)
        for path, data in self._buffered.items():
            if data is None:
                visible.discard(path)
            else:
                visible.add(path)
        return sorted(visible)

    def drop_unsynced(self) -> None:
        """Power cut, worst case: the entire buffer cache is gone."""
        self._buffered.clear()

    def _power_cut(self, tape: Tape) -> None:
        """Power cut with honest physics: each buffered write independently
        turns out lost, torn (a prefix reached the platter), or flushed —
        the storage bug class FoundationDB's simulator is famous for."""
        for path in sorted(self._buffered):
            data = self._buffered[path]
            if data is None:
                if tape.draw("disk.fate", 2) == 1:
                    self._synced.pop(path, None)  # the delete had hit disk
            else:
                fate = tape.draw("disk.fate", 3)  # 0 lost, 1 torn, 2 flushed
                if fate == 1 and len(data) > 1:
                    cut = 1 + tape.draw("disk.tear", len(data) - 1)
                    self._synced[path] = data[:cut]
                elif fate == 2:
                    self._synced[path] = data
        self._buffered.clear()


class Host:
    """A container of tasks with its own disk and a power switch."""

    def __init__(self, world: World, name: str) -> None:
        self.name = name
        self._world = world
        self._loop = world._loop
        self.disk = SimDisk()
        self.crashed = False
        self.generation = 0
        self._entries: list[Callable[[], Coroutine[Any, Any, Any]] | None] = []
        self._tasks: dict[int, weakref.ref[asyncio.Task[Any]]] = {}  # insertion-ordered
        self._task_seq = 0

    def __repr__(self) -> str:
        state = "crashed" if self.crashed else "up"
        return f"<Host {self.name} {state} gen={self.generation}>"

    # --- running code on the host ---

    def spawn(
        self,
        target: Callable[[], Coroutine[Any, Any, Any]] | Coroutine[Any, Any, Any],
        *,
        name: str | None = None,
    ) -> asyncio.Task[Any]:
        """Run a coroutine on this host.

        Prefer passing a *factory* (``lambda: run_node(...)``): factories are
        re-run by ``restart()``. A raw coroutine object works but makes the
        host unrestartable.
        """
        if self.crashed:
            raise RuntimeError(f"host {self.name!r} is crashed; restart() it first")
        if callable(target):
            factory = target
            coro = target()
        else:
            factory = None
            coro = target
        self._entries.append(factory)
        return self._spawn(coro, name=name)

    def _spawn(self, coro: Coroutine[Any, Any, Any], *, name: str | None) -> asyncio.Task[Any]:
        host = self

        async def hosted() -> Any:
            # Set, never reset: the variable lives in this task's own context,
            # which dies with the task (a reset token would be invalid when a
            # crashed task's coroutine is closed from another context).
            current_host.set(host)
            return await coro

        if name is None:
            name = f"{self.name}/entry-{self._task_seq}"
            self._task_seq += 1
        task = self._loop.create_task(hosted(), name=name)
        self._register(task)
        return task

    def _register(self, task: asyncio.Task[Any]) -> None:
        key = id(task)

        def _remove(_: weakref.ref[asyncio.Task[Any]]) -> None:
            self._tasks.pop(key, None)

        self._tasks[key] = weakref.ref(task, _remove)

    def _live_tasks(self) -> list[asyncio.Task[Any]]:
        alive = []
        for ref in self._tasks.values():
            task = ref()
            if task is not None and not task.done():
                alive.append(task)
        return alive

    # --- the power switch ---

    def crash(self) -> None:
        """Power cut. Tasks stop forever (no cleanup runs during the sim),
        unsynced disk state is lost, peers see connection resets."""
        if self.crashed:
            return
        self.crashed = True
        self._loop.log.emit("host_crash", t=self._loop.time(), host=self.name)
        self._loop._crash_tasks(self._live_tasks())
        self._tasks.clear()
        net = self._world.net
        for transport in list(net._live_transports.values()):
            if transport.owner is self:
                transport._drop_on_crash()
        for server in list(net._listeners.values()):
            if server.owner is self:
                server.close()
        self.disk._power_cut(self._loop.tape)

    def restart(self) -> None:
        """Power back on: re-run every entry factory against the surviving
        (fsynced) disk state."""
        if not self.crashed:
            raise RuntimeError(f"host {self.name!r} is not crashed")
        if any(entry is None for entry in self._entries):
            raise RuntimeError(
                f"host {self.name!r} was spawned with raw coroutine objects, "
                f"which cannot be re-run; spawn factories (lambda: main(...)) "
                f"to make a host restartable"
            )
        self.crashed = False
        self.generation += 1
        entries, self._entries = self._entries, []
        self._loop.log.emit(
            "host_restart", t=self._loop.time(), host=self.name, generation=self.generation
        )
        for factory in entries:
            assert factory is not None
            self.spawn(factory)


class World:
    """The simulated universe handed to the program under test."""

    def __init__(self, loop: SimLoop) -> None:
        self._loop = loop
        self.net = SimNetwork(loop)
        loop.attach_network(self.net)
        self._hosts: dict[str, Host] = {}

    @property
    def time(self) -> float:
        return self._loop.time()

    def host(self, name: str) -> Host:
        """Create (or fetch) the named host."""
        if name not in self._hosts:
            self._hosts[name] = Host(self, name)
        return self._hosts[name]

    @property
    def hosts(self) -> list[Host]:
        return list(self._hosts.values())

    async def sleep(self, seconds: float) -> None:
        """Virtual-time sleep (wall time ~0)."""
        await asyncio.sleep(seconds)

    async def until(
        self,
        predicate: Callable[[], bool],
        *,
        timeout: float,  # noqa: ASYNC109 — virtual-time timeout is the point
        poll: float = 0.05,
    ) -> None:
        """Wait until ``predicate()`` holds, polling in virtual time.

        Raises ``TimeoutError`` after ``timeout`` virtual seconds. The poll
        interval bounds how often the condition is sampled; it costs nothing
        in wall time.
        """
        deadline = self._loop.time() + timeout
        while True:
            if predicate():
                return
            remaining = deadline - self._loop.time()
            if remaining <= 0:
                raise TimeoutError(f"condition not reached within {timeout} virtual seconds")
            await asyncio.sleep(min(poll, remaining))
