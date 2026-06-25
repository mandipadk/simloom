"""``simloom`` command-line debugging tools over a causal event log.

A run recorded with ``causal=True`` (``result.log.write_to("trace.jsonl")``)
carries the happens-before edges; these commands consume that artifact:

- ``simloom trace LOG --step N`` reconstructs the state at step N — the virtual
  clock, what ran, and the *causal stack* (the chain of steps that woke it, back
  to a root).
- ``simloom trace LOG --grep REGEX`` / ``--changed NAME`` are omniscient queries:
  every step whose callback matches, or every step where an ``observe``-d value
  changed.
- ``simloom diff A B`` reports the first diverging event between two universes,
  with a cause classification.
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

Event = dict[str, Any]


def load_log(path: str | Path) -> tuple[Event, list[Event]]:
    """Read a serialized event log (JSONL: a header line, then events)."""
    lines = [ln for ln in Path(path).read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not lines:
        return {}, []
    return json.loads(lines[0]), [json.loads(ln) for ln in lines[1:]]


def steps_of(events: Sequence[Event]) -> list[Event]:
    return [e for e in events if e.get("kind") == "step"]


def causal_stack(steps: Sequence[Event], index: int) -> list[int]:
    """The chain of step indices from ``index`` back to a root (woke_by=None)."""
    chain: list[int] = []
    cursor: int | None = index
    seen: set[int] = set()
    while cursor is not None and cursor not in seen and 0 <= cursor < len(steps):
        seen.add(cursor)
        chain.append(cursor)
        cursor = steps[cursor].get("woke_by")
    return chain


def changed_steps(events: Sequence[Event], name: str) -> list[Event]:
    """Exactly the ``observe`` events for ``name`` where its value changed."""
    out: list[Event] = []
    previous: object = object()  # sentinel: the first observation always "changes"
    for event in events:
        if (
            event.get("kind") == "observe"
            and event.get("name") == name
            and event.get("value") != previous
        ):
            out.append(event)
            previous = event.get("value")
    return out


def classify_divergence(a: Event, b: Event) -> str:
    if a.get("kind") != b.get("kind"):
        return f"event kind differs: {a.get('kind')} vs {b.get('kind')}"
    if a.get("kind") == "step":
        if a.get("ran") != b.get("ran"):
            return f"a different callback ran: {a.get('ran')} vs {b.get('ran')}"
        if a.get("choice") != b.get("choice"):
            return f"the scheduler picked differently: {a.get('choice')} vs {b.get('choice')}"
        if a.get("t") != b.get("t"):
            return f"the virtual clock diverged: t={a.get('t')} vs t={b.get('t')}"
    return "field-level difference within the same event kind"


def first_divergence(a: Sequence[Event], b: Sequence[Event]) -> int | None:
    """Index of the first differing event, or None if one is a prefix of the
    other and they are otherwise identical."""
    for i, (x, y) in enumerate(zip(a, b, strict=False)):
        if x != y:
            return i
    if len(a) != len(b):
        return min(len(a), len(b))
    return None


def _cmd_trace(args: argparse.Namespace) -> int:
    _header, events = load_log(args.log)
    steps = steps_of(events)
    if args.grep is not None:
        pattern = re.compile(args.grep)
        for i, s in enumerate(steps):
            if pattern.search(str(s.get("ran", ""))):
                print(f"#{i} t={s['t']} {s.get('ran')} (via {s.get('via')} <- {s.get('woke_by')})")
        return 0
    if args.changed is not None:
        for event in changed_steps(events, args.changed):
            print(f"step {event.get('step')} t={event['t']} {args.changed}={event.get('value')}")
        return 0
    if args.step is not None:
        if not 0 <= args.step < len(steps):
            print(f"step {args.step} is out of range (0..{len(steps) - 1})")
            return 2
        s = steps[args.step]
        print(f"=== state at step {args.step} ===")
        print(f"virtual clock : t={s['t']}")
        print(
            f"running       : {s.get('ran')} (woke via {s.get('via')} by step {s.get('woke_by')})"
        )
        print("causal stack (newest first, back to a root):")
        for c in causal_stack(steps, args.step):
            print(f"  #{c} t={steps[c]['t']} {steps[c].get('ran')}")
        return 0
    for i, s in enumerate(steps):
        print(f"#{i} t={s['t']} {s.get('ran')} (via {s.get('via')} <- {s.get('woke_by')})")
    return 0


def _cmd_diff(args: argparse.Namespace) -> int:
    _ha, a = load_log(args.a)
    _hb, b = load_log(args.b)
    index = first_divergence(a, b)
    if index is None:
        print("no divergence: the two universes are identical")
        return 0
    print(f"first divergence at event #{index}:")
    ax = a[index] if index < len(a) else None
    bx = b[index] if index < len(b) else None
    print(f"  A: {ax}")
    print(f"  B: {bx}")
    if ax is not None and bx is not None:
        print(f"  cause: {classify_divergence(ax, bx)}")
    else:
        print("  cause: one universe has more events (it ran longer)")
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="simloom", description="simloom debugging tools")
    sub = parser.add_subparsers(dest="command", required=True)

    trace = sub.add_parser("trace", help="inspect a causal event log")
    trace.add_argument("log", help="a JSONL event log recorded with causal=True")
    trace.add_argument("--step", type=int, default=None, help="reconstruct state at step N")
    trace.add_argument("--grep", default=None, help="steps whose callback matches a regex")
    trace.add_argument("--changed", default=None, help="steps where an observed value changed")
    trace.set_defaults(func=_cmd_trace)

    diff = sub.add_parser("diff", help="first divergence between two event logs")
    diff.add_argument("a", help="first JSONL event log")
    diff.add_argument("b", help="second JSONL event log")
    diff.set_defaults(func=_cmd_diff)

    args = parser.parse_args(argv)
    result: int = args.func(args)
    return result


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
