"""SimLoop: a deterministic asyncio event loop driven by the choice tape.

Design rules (earned in the Phase 0 spikes, enforced here):

- The tape picks which ready callback runs next; nothing else does. A step
  with a single ready callback is forced and consumes no draw.
- Virtual time only: ``time()`` never touches a real clock; when nothing is
  ready the clock jumps to the next timer. Wall time per simulated hour is
  milliseconds.
- Task names are loop-owned. asyncio's default ``Task-N`` counter is
  process-global and leaks run-to-run nondeterminism into logs, so every
  task scheduled on this loop gets a deterministic name unless the caller
  chose one explicitly.
- Event-log labels are address-free; nothing derived from ``id()`` or
  ``repr`` with memory addresses may reach the log.
- Anything that would touch the real world — selectors, sockets, pipes,
  signals, subprocesses, real DNS — raises ``EscapedSimulationError`` at the
  call site instead of silently reintroducing nondeterminism.
- The garbage collector is disabled while the loop runs and invoked at fixed
  step intervals, so finalizers and weakref callbacks land at deterministic
  points.
"""

from __future__ import annotations

import asyncio
import asyncio.events
import gc
import heapq
import sys
import threading
import weakref
from collections.abc import Callable, Coroutine, Generator
from concurrent.futures import Executor
from contextvars import Context
from typing import Any, NoReturn, Protocol, TypeVar, TypeVarTuple, cast

from ._errors import EscapedSimulationError, SimDeadlockError
from ._eventlog import EventLog
from ._tape import Tape

_T = TypeVar("_T")
_Ts = TypeVarTuple("_Ts")


# Mirror typeshed's private types structurally so overrides type-check exactly.
class _TaskFactory(Protocol):
    def __call__(
        self, loop: asyncio.AbstractEventLoop, factory: Coroutine[Any, Any, _T], /
    ) -> asyncio.Future[_T]: ...


type _ExceptionHandler = Callable[[asyncio.AbstractEventLoop, dict[str, Any]], object]

#: Tape label for scheduler picks.
SCHED_PICK = "sched.pick"


def _handle_callback(handle: asyncio.Handle) -> Callable[..., object]:
    """The callable a Handle wraps (private attribute, stable since 3.0)."""
    return cast("Callable[..., object]", getattr(handle, "_callback", None) or (lambda: None))


def describe_callback(callback: Callable[..., object]) -> str:
    """Address-free, deterministic description of a callback for the log."""
    # Unwrap functools.partial chains.
    while True:
        inner = getattr(callback, "func", None)
        if inner is None or not callable(inner):
            break
        callback = inner
    owner = getattr(callback, "__self__", None)
    if isinstance(owner, asyncio.Task):
        raw = getattr(callback, "__name__", type(callback).__name__).lower()
        kind = "step" if "step" in raw else ("wakeup" if "wakeup" in raw else raw)
        return f"{_task_label(owner)}.{kind}"
    name = getattr(callback, "__qualname__", None)
    if isinstance(name, str):
        return name
    return type(callback).__name__


def _task_label(task: asyncio.Task[Any]) -> str:
    """A task's log label. Tasks born outside this loop's ``create_task``
    carry asyncio's process-global default name, which differs run to run;
    fall back to the coroutine's qualname, which is stable."""
    name = task.get_name()
    if name.startswith("Task-"):
        coro = task.get_coro()
        qualname = getattr(coro, "__qualname__", None)
        if isinstance(qualname, str):
            return qualname
    return name


class SimLoop(asyncio.AbstractEventLoop):
    """A deterministic event loop. All nondeterminism flows through ``tape``."""

    def __init__(
        self,
        tape: Tape,
        *,
        log: EventLog | None = None,
        epoch: float = 0.0,
        gc_interval: int = 1009,
    ) -> None:
        self._tape = tape
        self._log = log if log is not None else EventLog()
        self._now = float(epoch)
        self._ready: list[asyncio.Handle] = []
        self._scheduled: list[tuple[float, int, asyncio.TimerHandle]] = []
        self._timer_seq = 0
        self._task_name_seq = 0
        self._step_seq = 0
        self._gc_interval = gc_interval
        self._closed = False
        self._running = False
        self._stopping = False
        self._debug = False
        self._thread: threading.Thread | None = None
        self._task_factory: _TaskFactory | None = None
        self._exception_handler: _ExceptionHandler | None = None
        self._unhandled: list[dict[str, Any]] = []
        # Weak creation-order registry: sorting tasks by label alone would
        # tie-break duplicates by set-iteration order, which is address-based
        # and nondeterministic. Weak so it never delays task finalization.
        self._task_order: weakref.WeakKeyDictionary[asyncio.Task[Any], int] = (
            weakref.WeakKeyDictionary()
        )
        self._task_creation_seq = 0
        self._asyncgens: dict[int, weakref.ref[Any]] = {}
        self._asyncgens_shutdown_called = False

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------

    @property
    def tape(self) -> Tape:
        return self._tape

    @property
    def log(self) -> EventLog:
        return self._log

    @property
    def unhandled_exceptions(self) -> list[dict[str, Any]]:
        """Exception contexts that reached the loop with no handler set."""
        return list(self._unhandled)

    def time(self) -> float:
        return self._now

    def is_running(self) -> bool:
        return self._running

    def is_closed(self) -> bool:
        return self._closed

    def get_debug(self) -> bool:
        return self._debug

    def set_debug(self, enabled: bool) -> None:
        self._debug = enabled

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    def _check_closed(self) -> None:
        if self._closed:
            raise RuntimeError("event loop is closed")

    def _check_running(self) -> None:
        if self._running:
            raise RuntimeError("this event loop is already running")
        if asyncio.events._get_running_loop() is not None:
            raise RuntimeError("cannot run a SimLoop while another event loop is running")

    def run_forever(self) -> None:
        self._check_closed()
        self._check_running()
        self._thread = threading.current_thread()
        old_agen_hooks = sys.get_asyncgen_hooks()
        sys.set_asyncgen_hooks(
            firstiter=self._asyncgen_firstiter_hook,
            finalizer=self._asyncgen_finalizer_hook,
        )
        asyncio.events._set_running_loop(self)
        self._running = True
        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            while not self._stopping:
                self._run_once()
        finally:
            if gc_was_enabled:
                gc.enable()
            self._stopping = False
            self._running = False
            self._thread = None
            asyncio.events._set_running_loop(None)
            sys.set_asyncgen_hooks(*old_agen_hooks)

    def run_until_complete(self, future: Any) -> Any:
        self._check_closed()
        self._check_running()
        task = asyncio.ensure_future(future, loop=self)

        def _on_done(_: asyncio.Future[Any]) -> None:
            self.stop()

        task.add_done_callback(_on_done)
        try:
            self.run_forever()
        finally:
            task.remove_done_callback(_on_done)
        if not task.done():
            raise RuntimeError("event loop stopped before the future completed")
        return task.result()

    def stop(self) -> None:
        self._stopping = True

    def close(self) -> None:
        if self._running:
            raise RuntimeError("cannot close a running event loop")
        self._closed = True
        self._ready.clear()
        self._scheduled.clear()

    # ------------------------------------------------------------------
    # the deterministic heart
    # ------------------------------------------------------------------

    def _run_once(self) -> None:
        # Deterministic compaction: drop cancelled work before choosing.
        while self._scheduled and self._scheduled[0][2].cancelled():
            heapq.heappop(self._scheduled)
        if any(h.cancelled() for h in self._ready):
            self._ready = [h for h in self._ready if not h.cancelled()]

        if not self._ready:
            if not self._scheduled:
                self._report_deadlock()
            target = self._scheduled[0][0]
            if target > self._now:
                self._now = target
                self._log.emit("clock_jump", t=self._now)

        while self._scheduled and self._scheduled[0][0] <= self._now:
            _, _, timer = heapq.heappop(self._scheduled)
            if not timer.cancelled():
                self._ready.append(timer)

        if not self._ready:
            return  # everything due was cancelled; re-evaluate

        count = len(self._ready)
        index = self._tape.draw(SCHED_PICK, count) if count > 1 else 0
        handle = self._ready.pop(index)
        self._log.emit(
            "step",
            t=self._now,
            choice=index,
            ready=count,
            ran=describe_callback(_handle_callback(handle)),
        )
        self._step_seq += 1
        if self._gc_interval and self._step_seq % self._gc_interval == 0:
            self._collect_garbage()
        handle._run()

    def _collect_garbage(self) -> None:
        collected = gc.collect()
        self._log.emit("gc_collect", t=self._now, collected=collected)

    def _drain_ready(self, limit: int = 100_000) -> None:
        """Run pending ready callbacks (no clock jumps) until none remain.

        Used during teardown so finalizer-scheduled callbacks execute at a
        deterministic point instead of vanishing.
        """
        steps = 0
        while self._ready:
            if any(h.cancelled() for h in self._ready):
                self._ready = [h for h in self._ready if not h.cancelled()]
                continue
            count = len(self._ready)
            index = self._tape.draw(SCHED_PICK, count) if count > 1 else 0
            handle = self._ready.pop(index)
            self._log.emit(
                "step",
                t=self._now,
                choice=index,
                ready=count,
                ran=describe_callback(_handle_callback(handle)),
            )
            handle._run()
            steps += 1
            if steps >= limit:
                raise RuntimeError("teardown drain exceeded its step limit")

    def _task_sort_key(self, task: asyncio.Task[Any]) -> tuple[str, int]:
        """Deterministic ordering for task collections (label, creation order)."""
        return (_task_label(task), self._task_order.get(task, -1))

    def _report_deadlock(self) -> NoReturn:
        pending = sorted(
            (t for t in asyncio.all_tasks(self) if not t.done()),
            key=self._task_sort_key,
        )
        lines = []
        for task in pending:
            frames = task.get_stack(limit=1)
            if frames:
                frame = frames[0]
                where = f"{frame.f_code.co_filename}:{frame.f_lineno}"
            else:
                where = "<no frame>"
            coro = task.get_coro()
            qualname = getattr(coro, "__qualname__", "<coroutine>")
            lines.append(f"  - {_task_label(task)} ({qualname}) suspended at {where}")
        detail = "\n".join(lines) if lines else "  (no tasks registered)"
        self._log.emit("deadlock", t=self._now, pending=[_task_label(t) for t in pending])
        raise SimDeadlockError(
            f"simulated universe is quiescent at t={self._now}: no runnable "
            f"callbacks and no timers, but {len(pending)} task(s) are still "
            f"waiting:\n{detail}"
        )

    # ------------------------------------------------------------------
    # scheduling primitives
    # ------------------------------------------------------------------

    def call_soon(
        self,
        callback: Callable[[*_Ts], object],
        *args: *_Ts,
        context: Context | None = None,
    ) -> asyncio.Handle:
        self._check_closed()
        _check_callback(callback, "call_soon")
        handle = asyncio.Handle(callback, args, self, context)
        self._ready.append(handle)
        return handle

    def call_later(
        self,
        delay: float,
        callback: Callable[[*_Ts], object],
        *args: *_Ts,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        return self.call_at(self._now + delay, callback, *args, context=context)

    def call_at(
        self,
        when: float,
        callback: Callable[[*_Ts], object],
        *args: *_Ts,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        self._check_closed()
        _check_callback(callback, "call_at")
        handle = asyncio.TimerHandle(float(when), callback, args, self, context)
        self._timer_seq += 1
        heapq.heappush(self._scheduled, (float(when), self._timer_seq, handle))
        return handle

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        pass  # cancelled timers are dropped lazily in _run_once

    def call_soon_threadsafe(
        self,
        callback: Callable[[*_Ts], object],
        *args: *_Ts,
        context: Context | None = None,
    ) -> asyncio.Handle:
        if self._running and threading.current_thread() is not self._thread:
            self._escape(
                "call_soon_threadsafe",
                "a foreign thread tried to inject work into the simulation; "
                "real threads run outside virtual time",
            )
        return self.call_soon(callback, *args, context=context)

    # ------------------------------------------------------------------
    # futures and tasks
    # ------------------------------------------------------------------

    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def create_task(
        self,
        coro: Coroutine[Any, Any, _T] | Generator[Any, None, _T],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> asyncio.Task[_T]:
        self._check_closed()
        task: asyncio.Task[_T]
        if self._task_factory is None:
            task = asyncio.Task(
                cast("Coroutine[Any, Any, _T]", coro), loop=self, name=name, context=context
            )
        else:
            factory = cast("Callable[..., asyncio.Task[_T]]", self._task_factory)
            if context is None:  # noqa: SIM108 — mirrors BaseEventLoop's branching
                task = factory(self, coro)
            else:
                task = factory(self, coro, context=context)
            if name is not None:
                task.set_name(name)
        if name is None and task.get_name().startswith("Task-"):
            # Replace asyncio's process-global default name with a loop-owned,
            # deterministic one. Explicit names — even "Task-…" — are kept.
            task.set_name(f"task-{self._task_name_seq}")
            self._task_name_seq += 1
        self._task_order[task] = self._task_creation_seq
        self._task_creation_seq += 1
        coro_name = getattr(task.get_coro(), "__qualname__", None)
        self._log.emit(
            "task_created",
            t=self._now,
            task=task.get_name(),
            coro=coro_name if isinstance(coro_name, str) else "<coroutine>",
        )
        task.add_done_callback(self._on_task_done)
        return task

    def _on_task_done(self, task: asyncio.Future[Any]) -> None:
        # Never call task.exception() here: that would mark the exception as
        # retrieved and silence asyncio's never-retrieved detection, which the
        # unhandled-exception policy depends on.
        outcome = "cancelled" if task.cancelled() else "finished"
        label = _task_label(task) if isinstance(task, asyncio.Task) else "<future>"
        self._log.emit("task_done", t=self._now, task=label, outcome=outcome)

    def set_task_factory(self, factory: _TaskFactory | None) -> None:
        self._task_factory = factory

    def get_task_factory(self) -> _TaskFactory | None:
        return self._task_factory

    # ------------------------------------------------------------------
    # executor surface: functions run inline at a tape-chosen point
    # ------------------------------------------------------------------

    def run_in_executor(
        self, executor: Executor | None, func: Callable[[*_Ts], _T], *args: *_Ts
    ) -> asyncio.Future[_T]:
        """Run ``func`` inline at a tape-chosen scheduler point.

        Thread pools do not exist inside the simulation; the ``executor``
        argument is ignored. CPU-bound work behaves as expected. Blocking
        real I/O inside ``func`` escapes the simulation undetectably — mock
        at that boundary instead (see docs/determinism.md).
        """
        self._check_closed()
        future: asyncio.Future[_T] = self.create_future()

        def _invoke() -> None:
            if future.cancelled():
                return
            try:
                result = func(*args)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)

        self.call_soon(_invoke)
        return future

    def set_default_executor(self, executor: Any) -> None:
        pass  # executors are simulated away; nothing to store

    async def shutdown_default_executor(
        self,
        timeout: float | None = None,  # noqa: ASYNC109 — asyncio API parity
    ) -> None:
        return

    # ------------------------------------------------------------------
    # async generator lifecycle (deterministically ordered)
    # ------------------------------------------------------------------

    def _asyncgen_firstiter_hook(self, agen: Any) -> None:
        if self._asyncgens_shutdown_called:
            return
        key = id(agen)

        def _remove(_: weakref.ref[Any]) -> None:
            self._asyncgens.pop(key, None)

        # Insertion-ordered dict, not a WeakSet: shutdown must close async
        # generators in a deterministic order.
        self._asyncgens[key] = weakref.ref(agen, _remove)

    def _asyncgen_finalizer_hook(self, agen: Any) -> None:
        self._asyncgens.pop(id(agen), None)
        if not self._closed:
            self.create_task(agen.aclose())  # loop-owned deterministic name

    async def shutdown_asyncgens(self) -> None:
        self._asyncgens_shutdown_called = True
        if not self._asyncgens:
            return
        generators = [ref() for ref in self._asyncgens.values()]
        self._asyncgens.clear()
        live = [agen for agen in generators if agen is not None]
        results = await asyncio.gather(*(agen.aclose() for agen in live), return_exceptions=True)
        for agen, result in zip(live, results, strict=True):
            if isinstance(result, Exception):
                self.call_exception_handler(
                    {
                        "message": "error closing async generator during shutdown",
                        "exception": result,
                        "asyncgen": agen,
                    }
                )

    # ------------------------------------------------------------------
    # exception handling: nothing passes silently
    # ------------------------------------------------------------------

    def set_exception_handler(self, handler: _ExceptionHandler | None) -> None:
        self._exception_handler = handler

    def get_exception_handler(self) -> _ExceptionHandler | None:
        return self._exception_handler

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        self._unhandled.append(dict(context))

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        exc = context.get("exception")
        # Only the exception type reaches the log: context messages can embed
        # file paths and reprs, which must never leak into the digest.
        self._log.emit(
            "unhandled_exception",
            t=self._now,
            error=type(exc).__name__ if isinstance(exc, BaseException) else None,
        )
        if self._exception_handler is not None:
            self._exception_handler(self, context)
        else:
            self.default_exception_handler(context)

    # ------------------------------------------------------------------
    # the simulation boundary: real-world APIs raise, loudly
    # ------------------------------------------------------------------

    def _escape(self, api: str, hint: str) -> NoReturn:
        self._log.emit("escape", t=self._now, api=api)
        raise EscapedSimulationError(api=api, hint=hint)

    def _escape_network(self, api: str) -> NoReturn:
        self._escape(
            api,
            "real network access from inside the simulation; the simulated "
            "network (SimWorld) arrives in Phase B",
        )

    def _escape_fd(self, api: str) -> NoReturn:
        self._escape(
            api,
            "file-descriptor readiness callbacks require a real selector, "
            "which the simulation never touches",
        )

    # -- DNS / sockets / servers --

    async def getaddrinfo(self, host: Any, port: Any, **kwargs: Any) -> Any:
        self._escape_network("loop.getaddrinfo")

    async def getnameinfo(self, sockaddr: Any, flags: int = 0) -> Any:
        self._escape_network("loop.getnameinfo")

    async def create_connection(self, *args: Any, **kwargs: Any) -> Any:
        self._escape_network("loop.create_connection")

    async def create_server(self, *args: Any, **kwargs: Any) -> Any:
        self._escape_network("loop.create_server")

    async def create_datagram_endpoint(self, *args: Any, **kwargs: Any) -> Any:
        self._escape_network("loop.create_datagram_endpoint")

    async def create_unix_connection(self, *args: Any, **kwargs: Any) -> Any:
        self._escape_network("loop.create_unix_connection")

    async def create_unix_server(self, *args: Any, **kwargs: Any) -> Any:
        self._escape_network("loop.create_unix_server")

    async def start_tls(self, *args: Any, **kwargs: Any) -> Any:
        self._escape_network("loop.start_tls")

    async def sendfile(self, *args: Any, **kwargs: Any) -> Any:
        self._escape_network("loop.sendfile")

    # -- raw socket operations --

    async def sock_recv(self, sock: Any, nbytes: int) -> Any:
        self._escape_network("loop.sock_recv")

    async def sock_recv_into(self, sock: Any, buf: Any) -> Any:
        self._escape_network("loop.sock_recv_into")

    async def sock_recvfrom(self, sock: Any, bufsize: int) -> Any:
        self._escape_network("loop.sock_recvfrom")

    async def sock_recvfrom_into(self, sock: Any, buf: Any, nbytes: int = 0) -> Any:
        self._escape_network("loop.sock_recvfrom_into")

    async def sock_sendall(self, sock: Any, data: Any) -> None:
        self._escape_network("loop.sock_sendall")

    async def sock_sendto(self, sock: Any, data: Any, address: Any) -> Any:
        self._escape_network("loop.sock_sendto")

    async def sock_connect(self, sock: Any, address: Any) -> None:
        self._escape_network("loop.sock_connect")

    async def sock_accept(self, sock: Any) -> Any:
        self._escape_network("loop.sock_accept")

    async def sock_sendfile(self, *args: Any, **kwargs: Any) -> Any:
        self._escape_network("loop.sock_sendfile")

    # -- file descriptors --

    def add_reader(self, fd: Any, callback: Callable[[*_Ts], Any], *args: *_Ts) -> None:
        self._escape_fd("loop.add_reader")

    def remove_reader(self, fd: Any) -> bool:
        self._escape_fd("loop.remove_reader")

    def add_writer(self, fd: Any, callback: Callable[[*_Ts], Any], *args: *_Ts) -> None:
        self._escape_fd("loop.add_writer")

    def remove_writer(self, fd: Any) -> bool:
        self._escape_fd("loop.remove_writer")

    # -- pipes / subprocesses / signals --

    async def connect_read_pipe(self, protocol_factory: Any, pipe: Any) -> Any:
        self._escape("loop.connect_read_pipe", "real pipes live outside the simulation")

    async def connect_write_pipe(self, protocol_factory: Any, pipe: Any) -> Any:
        self._escape("loop.connect_write_pipe", "real pipes live outside the simulation")

    async def subprocess_shell(self, *args: Any, **kwargs: Any) -> Any:
        self._escape(
            "loop.subprocess_shell",
            "real subprocesses run outside virtual time and cannot be replayed",
        )

    async def subprocess_exec(self, *args: Any, **kwargs: Any) -> Any:
        self._escape(
            "loop.subprocess_exec",
            "real subprocesses run outside virtual time and cannot be replayed",
        )

    def add_signal_handler(
        self, sig: int, callback: Callable[[*_Ts], object], *args: *_Ts
    ) -> None:
        self._escape(
            "loop.add_signal_handler",
            "OS signals arrive from outside the simulation at nondeterministic times",
        )

    def remove_signal_handler(self, sig: int) -> bool:
        self._escape(
            "loop.remove_signal_handler",
            "OS signals arrive from outside the simulation at nondeterministic times",
        )


def _check_callback(callback: object, method: str) -> None:
    if asyncio.iscoroutine(callback) or asyncio.iscoroutinefunction(callback):
        raise TypeError(f"coroutines cannot be used with {method}(): use create_task()")
    if not callable(callback):
        raise TypeError(f"{method}() expected a callable, got {callback!r}")
