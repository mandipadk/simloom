"""PYTHONHASHSEED pinning — the linchpin of *cross-process* reproducibility.

A seed or tape reproduces within one process unconditionally, but across
processes only if hash randomization is pinned: set/dict iteration order for
hash-randomized types differs per process otherwise, and any program that
iterates a set of strings makes hash-order-dependent decisions. ``PYTHONHASHSEED``
can only be fixed at interpreter start, so the only way to *guarantee* it is to
re-exec the process with it set.

``pin_hashseed()`` does exactly that, guarded by a sentinel so it re-execs at
most once. Call it at the very top of a ``conftest.py`` (or a script's
``__main__``) — before any imports that might hash-iterate — so a failing seed
shipped to a colleague reproduces on their machine.

Limitation: a ``python -c '...'`` invocation cannot be re-exec'd faithfully
(``-c`` code is not present in ``sys.argv``); use a script file or ``-m``.
"""

from __future__ import annotations

import os
import sys

_SENTINEL = "_SIMLOOM_HASHSEED_REEXEC"


def is_pinned() -> bool:
    """True iff this interpreter's hash randomization is fixed — either started
    with ``PYTHONHASHSEED=0`` (``sys.flags.hash_randomization == 0``) or with the
    env var set to a concrete seed."""
    if sys.flags.hash_randomization == 0:
        return True
    env = os.environ.get("PYTHONHASHSEED", "")
    return env not in ("", "random")


def pin_hashseed(value: str = "0") -> None:
    """If hash randomization is not pinned, re-exec this process with
    ``PYTHONHASHSEED`` set, so cross-process replay is sound. No-op if already
    pinned, or if a previous call already re-exec'd (the sentinel prevents an
    infinite loop). When it re-execs, this call does not return — the process is
    replaced.
    """
    if is_pinned() or os.environ.get(_SENTINEL):
        return
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = value
    env[_SENTINEL] = "1"
    os.execve(sys.executable, [sys.executable, *sys.argv], env)
