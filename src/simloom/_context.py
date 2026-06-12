"""Context variables shared across simloom modules (no internal imports,
so anything may import this without cycles)."""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

#: The simulated host whose code is currently executing, if any. Set by
#: Host.spawn's wrapper coroutine; inherited by child tasks via contextvars.
current_host: ContextVar[Any] = ContextVar("simloom_current_host", default=None)
