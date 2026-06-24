"""Closing the determinism boundary: opt-in patching of the stdlib's two
silent value-divergence sources so an *unmodified* asyncio library replays
byte-for-byte.

1. **Virtual wall clock** — ``time.time``/``monotonic``/``perf_counter`` (and
   their ``_ns`` variants) are redirected to the loop's virtual clock. A
   library that timestamps with ``time.time()`` or measures elapsed time with
   ``time.monotonic()`` now gets deterministic, replayable values instead of
   real wall time.

2. **Tape-seeded randomness** — the global ``random`` module, ``os.urandom``
   (used by ``uuid.uuid4``), and ``random._urandom`` (the bound alias used by
   ``SystemRandom``/``secrets``) are seeded from a single tape draw at run
   start, so a library that picks a random election timeout or a request id
   produces the same sequence on replay.

Both are installed for the duration of a run and **always restored** in a
``finally`` — even if the run crashes — so the real ``time``/``random`` are
never left patched.

Honest boundary (recorded in docs/determinism.md): this patches the ``time``,
``random``, and ``os`` *module* functions. Code that bound a local alias before
the patch (``from time import monotonic as _m``) or the C-accelerated
``datetime.now()`` is not redirected — those remain a documented escape until a
module-walking patcher lands.
"""

from __future__ import annotations

import contextlib
import os
import random
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._loop import SimLoop

#: Tape label for the per-run entropy seed (one draw, at run start).
ENTROPY_SEED = "entropy.seed"
_ENTROPY_BOUND = 2**31 - 1

#: A fixed simulated wall-clock epoch (2024-01-01T00:00:00Z) so ``time.time()``
#: returns deterministic, plausible timestamps. Overridable per run.
DEFAULT_WALL_EPOCH = 1_704_067_200.0


class _SeededEntropy:
    """A deterministic byte source for ``os.urandom``/``random._urandom``,
    derived from the tape seed but on its own generator so it never disturbs
    the ``random`` stream a library reads through ``random.random()``."""

    def __init__(self, seed: int) -> None:
        # XOR with a constant so the urandom stream differs from the Mersenne
        # stream even though both derive from the same tape seed.
        self._rng = random.Random(seed ^ 0x5DEECE66D)

    def urandom(self, n: int) -> bytes:
        return self._rng.randbytes(n)


@contextlib.contextmanager
def patched_environment(
    loop: SimLoop,
    *,
    virtual_time: bool,
    seed_randomness: bool,
    wall_epoch: float,
) -> Iterator[None]:
    """Install the requested patches for the duration of a run, restoring the
    real implementations on exit (including on exception)."""
    if not (virtual_time or seed_randomness):
        yield
        return

    restores: list[tuple[object, str, object]] = []

    def patch(module: object, name: str, value: object) -> None:
        restores.append((module, name, getattr(module, name)))
        setattr(module, name, value)

    saved_random_state: object | None = None
    try:
        if virtual_time:
            patch(time, "time", lambda: wall_epoch + loop.time())
            patch(time, "time_ns", lambda: int((wall_epoch + loop.time()) * 1e9))
            patch(time, "monotonic", loop.time)
            patch(time, "monotonic_ns", lambda: int(loop.time() * 1e9))
            patch(time, "perf_counter", loop.time)
            patch(time, "perf_counter_ns", lambda: int(loop.time() * 1e9))

        if seed_randomness:
            seed = loop.tape.draw(ENTROPY_SEED, _ENTROPY_BOUND)
            saved_random_state = random.getstate()
            random.seed(seed)
            entropy = _SeededEntropy(seed)
            patch(os, "urandom", entropy.urandom)  # uuid.uuid4, direct os.urandom
            if hasattr(random, "_urandom"):
                patch(random, "_urandom", entropy.urandom)  # SystemRandom, secrets
        yield
    finally:
        for module, name, original in reversed(restores):
            setattr(module, name, original)
        if saved_random_state is not None:
            random.setstate(saved_random_state)  # type: ignore[arg-type]
