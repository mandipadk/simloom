"""Top-level entry points: run a coroutine in a fresh simulated universe.

``run()`` explores a new universe from a seed; ``replay()`` re-executes a
recorded one. Both perform asyncio.run-parity teardown, deterministically:
leftover tasks are cancelled in sorted order, async generators are closed,
the garbage collector flushes finalizers at a known point, and any exception
that reached the loop unhandled fails the run (a testing harness must not
let failures pass silently).
"""

from __future__ import annotations

import asyncio
import faulthandler
import gc
import os
import platform
import sys
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass, field
from typing import Any, Literal

from ._errors import TapeMisalignmentError, UnhandledExceptionError
from ._eventlog import EventLog
from ._loop import SimLoop
from ._tape import Draw, MisalignmentPolicy, Tape
from ._version import __version__


@dataclass(frozen=True, slots=True)
class RunResult:
    """The complete record of one simulated universe."""

    outcome: Literal["ok", "error"]
    value: Any
    error: BaseException | None
    seed: int | None
    tape: tuple[Draw, ...]
    log: EventLog = field(repr=False)

    @property
    def digest(self) -> str:
        """sha256 of the event sequence — the universe's fingerprint."""
        return self.log.digest()

    def __post_init__(self) -> None:
        if (self.outcome == "error") != (self.error is not None):
            raise ValueError("outcome and error are inconsistent")


def run(
    main: Callable[[], Coroutine[Any, Any, Any]],
    *,
    seed: int,
    raise_on_error: bool = True,
    epoch: float = 0.0,
    gc_interval: int = 1009,
    on_unhandled: Literal["raise", "record"] = "raise",
    watchdog: float | None = None,
) -> RunResult:
    """Run ``main()`` in a fresh simulated universe generated from ``seed``.

    ``main`` must be a callable returning a new coroutine (not a coroutine
    object): exploration and replay re-execute it from scratch.

    ``watchdog`` arms a wall-clock guard (seconds) against hot loops that
    never ``await``: cooperative scheduling cannot preempt them and virtual
    time never advances, so the guard dumps every thread's stack to stderr
    and exits the process. It observes only; it cannot perturb the schedule.
    """
    return _execute(
        main,
        tape=Tape.generate(seed),
        seed=seed,
        raise_on_error=raise_on_error,
        epoch=epoch,
        gc_interval=gc_interval,
        on_unhandled=on_unhandled,
        watchdog=watchdog,
    )


def replay(
    main: Callable[[], Coroutine[Any, Any, Any]],
    *,
    tape: Tape | RunResult | Iterable[Draw],
    policy: MisalignmentPolicy = MisalignmentPolicy.STRICT,
    fallback_seed: int = 0,
    raise_on_error: bool = True,
    epoch: float = 0.0,
    gc_interval: int = 1009,
    on_unhandled: Literal["raise", "record"] = "raise",
    watchdog: float | None = None,
) -> RunResult:
    """Re-execute ``main()`` against a recorded universe."""
    recorded = tape.tape if isinstance(tape, RunResult) else tape
    return _execute(
        main,
        tape=Tape.replay(recorded, policy=policy, fallback_seed=fallback_seed),
        seed=None,
        raise_on_error=raise_on_error,
        epoch=epoch,
        gc_interval=gc_interval,
        on_unhandled=on_unhandled,
        watchdog=watchdog,
    )


def _execute(
    main: Callable[[], Coroutine[Any, Any, Any]],
    *,
    tape: Tape,
    seed: int | None,
    raise_on_error: bool,
    epoch: float,
    gc_interval: int,
    on_unhandled: Literal["raise", "record"],
    watchdog: float | None,
) -> RunResult:
    if asyncio.iscoroutine(main):
        raise TypeError(
            "run()/replay() take a callable returning a coroutine, not a "
            "coroutine object: a universe must be re-executable from scratch. "
            "Pass `main`, not `main()`."
        )
    if not callable(main):
        raise TypeError(f"main must be callable, got {main!r}")
    if asyncio.events._get_running_loop() is not None:
        raise RuntimeError(
            "simloom.run()/replay() cannot be called while another event loop is running"
        )

    loop = SimLoop(tape, epoch=epoch, gc_interval=gc_interval)
    log = loop.log
    log.metadata.update(
        {
            "simloom": __version__,
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "seed": seed,
            "epoch": epoch,
            "hash_randomization_pinned": _hash_randomization_pinned(),
        }
    )
    log.emit("run_start", t=epoch)

    if watchdog is not None:
        faulthandler.dump_traceback_later(watchdog, exit=True)
    asyncio.set_event_loop(loop)
    outcome: Literal["ok", "error"] = "ok"
    value: Any = None
    error: BaseException | None = None
    try:
        try:
            value = loop.run_until_complete(main())
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            outcome, error = "error", exc
            if isinstance(exc, TapeMisalignmentError):
                # The recording is already known not to match; teardown still
                # needs draws, so it gets deterministic fallback ones rather
                # than a second misalignment error mid-cleanup.
                tape.force_fallback()
        teardown_error = _teardown(loop)
        if outcome == "ok" and teardown_error is not None:
            outcome, error = "error", teardown_error
        if outcome == "ok" and on_unhandled == "raise" and loop.unhandled_exceptions:
            outcome, error = "error", _unhandled_error(loop)
        if (
            outcome == "ok"
            and tape.is_replay
            and tape.policy is MisalignmentPolicy.STRICT
            and not tape.replay_exact
        ):
            # Requesting a draw the tape cannot satisfy raises mid-run; the
            # mirror-image divergence — finishing without consuming the whole
            # recording — is only visible here.
            outcome, error = (
                "error",
                TapeMisalignmentError(
                    f"replay finished after consuming {tape.position} of "
                    f"{tape.recorded_length} recorded draws: the execution "
                    f"diverged from the recording"
                ),
            )
        log.emit(
            "run_end",
            t=loop.time(),
            outcome=outcome,
            error=type(error).__name__ if error is not None else None,
        )
    finally:
        if watchdog is not None:
            faulthandler.cancel_dump_traceback_later()
        asyncio.set_event_loop(None)
        loop.close()

    result = RunResult(
        outcome=outcome,
        value=value,
        error=error,
        seed=seed,
        tape=tape.draws,
        log=log,
    )
    if raise_on_error and error is not None:
        raise error
    return result


def _teardown(loop: SimLoop) -> BaseException | None:
    """asyncio.run-parity cleanup, deterministically ordered.

    Returns the first teardown failure instead of raising so the caller can
    decide whether it outranks the main outcome.
    """
    try:
        pending = sorted(
            (t for t in asyncio.all_tasks(loop) if not t.done()),
            key=loop._task_sort_key,
        )
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        # Flush finalizers and weakref callbacks at a deterministic point,
        # then run anything they scheduled.
        gc.collect()
        loop._drain_ready()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:
        return exc
    return None


def _unhandled_error(loop: SimLoop) -> UnhandledExceptionError:
    contexts = loop.unhandled_exceptions
    first = contexts[0]
    message = str(first.get("message", "unhandled exception in the event loop"))
    error = UnhandledExceptionError(
        f"{len(contexts)} exception(s) reached the loop unhandled; first: {message}"
    )
    exc = first.get("exception")
    if isinstance(exc, BaseException):
        error.__cause__ = exc
    return error


def _hash_randomization_pinned() -> bool:
    """Whether PYTHONHASHSEED is pinned (required for cross-process replay)."""
    if sys.flags.hash_randomization == 0:
        return True  # PYTHONHASHSEED=0
    env = os.environ.get("PYTHONHASHSEED", "")
    return env not in ("", "random")
