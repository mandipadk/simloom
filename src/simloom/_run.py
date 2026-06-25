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
from dataclasses import dataclass, field, replace
from typing import Any, Literal

from ._errors import (
    SimloomNondeterminismError,
    TapeMisalignmentError,
    UnhandledExceptionError,
)
from ._eventlog import EventLog
from ._loop import SimLoop
from ._patches import DEFAULT_WALL_EPOCH, patched_environment
from ._sched import SchedulerFactory, auto_pct_depth, resolve_scheduler
from ._tape import Draw, MisalignmentPolicy, Tape
from ._version import __version__
from ._world import World


@dataclass(frozen=True, slots=True)
class RunResult:
    """The complete record of one simulated universe."""

    outcome: Literal["ok", "error"]
    value: Any
    error: BaseException | None
    seed: int | None
    tape: tuple[Draw, ...]
    log: EventLog = field(repr=False)
    #: Buggify/reached counters (label -> hits); see simloom.sometimes/reached.
    coverage: dict[str, int] = field(default_factory=dict)
    #: Which scheduling strategy produced this universe (replay must match).
    scheduler: str = "random"

    @property
    def digest(self) -> str:
        """sha256 of the event sequence — the universe's fingerprint."""
        return self.log.digest()

    def __post_init__(self) -> None:
        if (self.outcome == "error") != (self.error is not None):
            raise ValueError("outcome and error are inconsistent")


def run(
    main: Callable[..., Coroutine[Any, Any, Any]],
    *,
    seed: int,
    raise_on_error: bool = True,
    epoch: float = 0.0,
    gc_interval: int = 1009,
    on_unhandled: Literal["raise", "record"] = "raise",
    watchdog: float | None = None,
    scheduler: str | SchedulerFactory | None = None,
    max_steps_per_instant: int = 1_000_000,
    virtual_time: bool = False,
    seed_randomness: bool = False,
    wall_epoch: float = DEFAULT_WALL_EPOCH,
    check_determinism: bool = False,
    world: bool = True,
) -> RunResult:
    """Run ``main()`` in a fresh simulated universe generated from ``seed``.

    ``main`` must be a callable returning a new coroutine (not a coroutine
    object): exploration and replay re-execute it from scratch.

    ``watchdog`` arms a wall-clock guard (seconds) against hot loops that
    never ``await``: cooperative scheduling cannot preempt them and virtual
    time never advances, so the guard dumps every thread's stack to stderr
    and exits the process. It observes only; it cannot perturb the schedule.

    ``check_determinism`` runs the seed twice and, if the two event logs
    differ, raises ``SimloomNondeterminismError`` locating the first diverging
    event — catching nondeterminism the tape does not control (identity-ordered
    iteration, a stray real clock/RNG, threads doing real work).
    """

    scheduler = resolve_auto_horizon(
        main,
        scheduler,
        seed=seed,
        epoch=epoch,
        gc_interval=gc_interval,
        max_steps_per_instant=max_steps_per_instant,
        virtual_time=virtual_time,
        seed_randomness=seed_randomness,
        wall_epoch=wall_epoch,
        world=world,
    )

    def once(rerr: bool) -> RunResult:
        return _execute(
            main,
            tape=Tape.generate(seed),
            seed=seed,
            raise_on_error=rerr,
            epoch=epoch,
            gc_interval=gc_interval,
            on_unhandled=on_unhandled,
            watchdog=watchdog,
            scheduler=scheduler,
            max_steps_per_instant=max_steps_per_instant,
            virtual_time=virtual_time,
            seed_randomness=seed_randomness,
            wall_epoch=wall_epoch,
            world=world,
        )

    if check_determinism:
        first = once(False)
        second = once(False)
        if first.digest != second.digest:
            error = _nondeterminism_error(seed, first, second)
            if raise_on_error:
                raise error
            return replace(first, outcome="error", error=error)
        if raise_on_error and first.error is not None:
            raise first.error
        return first
    return _execute(
        main,
        tape=Tape.generate(seed),
        seed=seed,
        raise_on_error=raise_on_error,
        epoch=epoch,
        gc_interval=gc_interval,
        on_unhandled=on_unhandled,
        watchdog=watchdog,
        scheduler=scheduler,
        max_steps_per_instant=max_steps_per_instant,
        virtual_time=virtual_time,
        seed_randomness=seed_randomness,
        wall_epoch=wall_epoch,
        world=world,
    )


def replay(
    main: Callable[..., Coroutine[Any, Any, Any]],
    *,
    tape: Tape | RunResult | Iterable[Draw],
    policy: MisalignmentPolicy = MisalignmentPolicy.STRICT,
    fallback_seed: int = 0,
    fallback: str = "rng",
    raise_on_error: bool = True,
    epoch: float = 0.0,
    gc_interval: int = 1009,
    on_unhandled: Literal["raise", "record"] = "raise",
    watchdog: float | None = None,
    scheduler: str | SchedulerFactory | None = None,
    max_steps_per_instant: int = 1_000_000,
    virtual_time: bool = False,
    seed_randomness: bool = False,
    wall_epoch: float = DEFAULT_WALL_EPOCH,
    world: bool = True,
) -> RunResult:
    """Re-execute ``main()`` against a recorded universe.

    When ``tape`` is a RunResult and no scheduler is given, the recording's
    own strategy is used — a PCT universe replays under PCT.
    """
    recorded = tape.tape if isinstance(tape, RunResult) else tape
    if scheduler is None and isinstance(tape, RunResult):
        scheduler = tape.scheduler
    return _execute(
        main,
        tape=Tape.replay(recorded, policy=policy, fallback_seed=fallback_seed, fallback=fallback),
        seed=None,
        raise_on_error=raise_on_error,
        epoch=epoch,
        gc_interval=gc_interval,
        on_unhandled=on_unhandled,
        watchdog=watchdog,
        scheduler=scheduler,
        max_steps_per_instant=max_steps_per_instant,
        virtual_time=virtual_time,
        seed_randomness=seed_randomness,
        wall_epoch=wall_epoch,
        world=world,
    )


def _execute(
    main: Callable[..., Coroutine[Any, Any, Any]],
    *,
    tape: Tape,
    seed: int | None,
    raise_on_error: bool,
    epoch: float,
    gc_interval: int,
    on_unhandled: Literal["raise", "record"],
    watchdog: float | None,
    scheduler: str | SchedulerFactory | None = None,
    max_steps_per_instant: int = 1_000_000,
    virtual_time: bool = False,
    seed_randomness: bool = False,
    wall_epoch: float = DEFAULT_WALL_EPOCH,
    world: bool = True,
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

    factory = resolve_scheduler(scheduler)
    loop = SimLoop(
        tape,
        epoch=epoch,
        gc_interval=gc_interval,
        scheduler=factory,
        max_steps_per_instant=max_steps_per_instant,
    )
    wants_world = _wants_world(main)
    if world:
        sim_world: World | None = World(loop)
    elif wants_world:
        raise TypeError(
            "main declares a World parameter but world=False; pass world=True "
            "(the default) or remove the parameter"
        )
    else:
        sim_world = None
    log = loop.log
    log.metadata.update(
        {
            "simloom": __version__,
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "seed": seed,
            "epoch": epoch,
            "scheduler": loop._scheduler.descriptor,
            "virtual_time": virtual_time,
            "seed_randomness": seed_randomness,
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
    # Virtual clock + tape-seeded randomness are installed for the whole run
    # (including teardown, so a finally block that reads the clock stays
    # deterministic) and always restored below. __enter__ draws the entropy
    # seed, so it is the run's first tape draw.
    env = patched_environment(
        loop,
        virtual_time=virtual_time,
        seed_randomness=seed_randomness,
        wall_epoch=wall_epoch,
    )
    env.__enter__()
    try:
        try:
            coro = main(sim_world) if wants_world else main()
            value = loop.run_until_complete(coro)
            # A liveness goal never satisfied during the run is a violation —
            # checked after the main coroutine returns, before teardown.
            loop.finalize_monitors()
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
        env.__exit__(None, None, None)  # restore real time/random before close
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
        coverage=dict(loop.coverage),
        scheduler=loop._scheduler.descriptor,
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
        # Monitors are off during teardown: cancellation interrupts tasks
        # mid-flight (a violating state may persist), and re-checking would
        # abort cleanup and leak the interrupted coroutines.
        loop._monitoring_enabled = False
        crashed_ids = set(loop._crashed_ids)
        pending = sorted(
            (t for t in asyncio.all_tasks(loop) if not t.done() and id(t) not in crashed_ids),
            key=loop._task_sort_key,
        )
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        # The universe has ended: crashed-host tasks may now run their
        # cleanup, deterministically, where it can no longer affect the sim.
        revived = loop._revive_crashed()
        for task in revived:
            task.cancel()
        if revived:
            loop.run_until_complete(asyncio.gather(*revived, return_exceptions=True))
        # Close connections still open at the universe's end (deterministic
        # order), so a wrapping SSL transport (asyncio's SSLProtocol) is told
        # connection_lost and does not warn about an unclosed transport at GC.
        network = loop._network
        if network is not None and network._live_transports:
            for transport in list(network._live_transports.values()):
                transport._force_close(None)
            loop._drain_ready()
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


def resolve_auto_horizon(
    main: Callable[..., Coroutine[Any, Any, Any]],
    scheduler: str | SchedulerFactory | None,
    *,
    seed: int,
    **probe_kwargs: Any,
) -> str | SchedulerFactory | None:
    """Resolve a ``pct:auto`` request to a concrete ``pct:d=D,k=K`` spec by
    measuring the step count of one probe run (under random walk). This kills
    the hand-tuned horizon: PCT's default ``k=4096`` is far larger than a small
    test's step count, so its change points land past the run's end and never
    fire — degrading PCT to a fixed priority schedule.

    The returned descriptor is concrete and picklable, so it flows through the
    multiprocess explorer and is recorded for exact replay.
    """
    depth = auto_pct_depth(scheduler)
    if depth is None:
        return scheduler
    probe = _execute(
        main,
        tape=Tape.generate(seed),
        seed=seed,
        raise_on_error=False,
        on_unhandled="record",
        watchdog=None,
        scheduler="random",
        epoch=probe_kwargs.get("epoch", 0.0),
        gc_interval=probe_kwargs.get("gc_interval", 1009),
        max_steps_per_instant=probe_kwargs.get("max_steps_per_instant", 1_000_000),
        virtual_time=probe_kwargs.get("virtual_time", False),
        seed_randomness=probe_kwargs.get("seed_randomness", False),
        wall_epoch=probe_kwargs.get("wall_epoch", DEFAULT_WALL_EPOCH),
        world=probe_kwargs.get("world", True),
    )
    steps = sum(1 for event in probe.log.events if event.get("kind") == "step")
    return f"pct:d={depth},k={max(1, steps)}"


def _nondeterminism_error(
    seed: int, first: RunResult, second: RunResult
) -> SimloomNondeterminismError:
    a = list(first.log.events)
    b = list(second.log.events)
    index = next(
        (i for i, (ea, eb) in enumerate(zip(a, b, strict=False)) if ea != eb),
        min(len(a), len(b)),  # one log is a strict prefix of the other
    )
    ea = a[index] if index < len(a) else None
    eb = b[index] if index < len(b) else None
    return SimloomNondeterminismError(
        f"seed {seed} produced two different universes — the test is nondeterministic "
        f"in a way the choice tape does not control (iterating a set/dict keyed by "
        f"object identity, a stray real time.time()/random not routed through simloom, "
        f"or threads doing real work). First divergence at event #{index}:\n"
        f"  run A: {ea}\n  run B: {eb}"
    )


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


def _wants_world(main: Callable[..., object]) -> bool:
    """True iff ``main`` declares a positional parameter to receive the World
    (the ``async def test(world)`` shape from the north-star API)."""
    import inspect

    try:
        signature = inspect.signature(main)
    except (TypeError, ValueError):
        return False
    required = [
        p
        for p in signature.parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        and p.default is inspect.Parameter.empty
    ]
    if len(required) > 1:
        raise TypeError(
            f"main may take at most one positional parameter (the World); got {len(required)}"
        )
    return len(required) == 1


def _hash_randomization_pinned() -> bool:
    """Whether PYTHONHASHSEED is pinned (required for cross-process replay)."""
    if sys.flags.hash_randomization == 0:
        return True  # PYTHONHASHSEED=0
    env = os.environ.get("PYTHONHASHSEED", "")
    return env not in ("", "random")
