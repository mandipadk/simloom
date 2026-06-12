"""The event log: canonical serialization, digests, the format contract."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from simloom import EventLog


def test_seq_assigned_in_order() -> None:
    log = EventLog()
    log.emit("a", t=0.0)
    log.emit("b", t=1.0, detail="x")
    assert [e["seq"] for e in log.events] == [0, 1]
    assert log.events[1]["detail"] == "x"


def test_reserved_fields_rejected() -> None:
    log = EventLog()
    with pytest.raises(ValueError, match="reserved"):
        log.emit("a", t=0.0, seq=99)
    with pytest.raises(TypeError):
        # `kind` and `t` collide at the signature level, which is fine too.
        log.emit("a", t=0.0, kind="sneaky")


def test_digest_covers_events_not_metadata() -> None:
    a, b = EventLog(), EventLog()
    for log in (a, b):
        log.emit("step", t=0.0, choice=1)
    a.metadata["python"] = "3.12"
    b.metadata["python"] = "3.99"
    assert a.digest() == b.digest()

    c = EventLog()
    c.emit("step", t=0.0, choice=2)
    assert c.digest() != a.digest()


def test_jsonl_round_trip(tmp_path: Path) -> None:
    log = EventLog()
    log.metadata["seed"] = 7
    log.emit("run_start", t=0.0)
    log.emit("step", t=0.5, choice=0, ready=2, ran="task-0.step")

    path = tmp_path / "events.jsonl"
    log.write_to(path)
    lines = path.read_text().splitlines()

    header = json.loads(lines[0])
    assert header["format"] == "simloom-events"
    assert header["version"] == 1
    assert header["seed"] == 7

    events = [json.loads(line) for line in lines[1:]]
    assert [e["kind"] for e in events] == ["run_start", "step"]
    assert events[1]["ran"] == "task-0.step"


def test_canonical_lines_are_key_sorted() -> None:
    log = EventLog()
    log.emit("z", t=0.0, zeta=1, alpha=2)
    line = next(iter(log.event_lines()))
    assert line.index('"alpha"') < line.index('"zeta"')
