"""The event log: a versioned, public record of everything a run did.

One JSONL document per run: a header object carrying metadata, then one
object per event in execution order. The format is a public contract —
failure artifacts ship it, tooling consumes it, and the planned time-travel
debugger replays from it — so schema changes bump the version. The schema is
documented in docs/event-log.md.

The digest covers the *events only*, not the header: two runs are the same
universe iff their event sequences are byte-identical, regardless of which
machine or interpreter produced them.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

EVENT_LOG_FORMAT = "simloom-events"
EVENT_LOG_FORMAT_VERSION = 1


def _canonical(obj: Mapping[str, Any]) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


class EventLog:
    """An append-only sequence of events with canonical serialization."""

    __slots__ = ("_events", "metadata")

    def __init__(self) -> None:
        self._events: list[dict[str, Any]] = []
        #: Run metadata for the header line; never part of the digest.
        self.metadata: dict[str, Any] = {}

    def emit(self, kind: str, t: float, **fields: Any) -> None:
        """Append one event at virtual time ``t``.

        Field values must be JSON-serializable and deterministic — no memory
        addresses, no wall-clock times, no process-global counters.
        """
        event: dict[str, Any] = {"seq": len(self._events), "kind": kind, "t": t}
        for key, value in fields.items():
            if key in event:
                raise ValueError(f"reserved event field: {key}")
            event[key] = value
        self._events.append(event)

    # --- reading ---

    @property
    def events(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self._events)

    def __len__(self) -> int:
        return len(self._events)

    def __iter__(self) -> Iterator[Mapping[str, Any]]:
        return iter(self._events)

    def __repr__(self) -> str:
        return f"<EventLog {len(self._events)} events digest={self.digest()[:12]}>"

    # --- serialization ---

    def header(self) -> dict[str, Any]:
        return {
            "format": EVENT_LOG_FORMAT,
            "version": EVENT_LOG_FORMAT_VERSION,
            **self.metadata,
        }

    def event_lines(self) -> Iterator[str]:
        for event in self._events:
            yield _canonical(event)

    def to_jsonl(self) -> str:
        lines = [_canonical(self.header())]
        lines.extend(self.event_lines())
        return "\n".join(lines) + "\n"

    def write_to(self, path: str | Path) -> None:
        Path(path).write_text(self.to_jsonl(), encoding="utf-8")

    def digest(self) -> str:
        """sha256 over the canonical event lines (header excluded)."""
        h = hashlib.sha256()
        for line in self.event_lines():
            h.update(line.encode("utf-8"))
            h.update(b"\n")
        return h.hexdigest()
