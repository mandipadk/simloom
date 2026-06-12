"""S4 — choice tape spike: replay equality under fault injection.

Claim under test: if *every* source of nondeterminism — scheduler picks, packet
drops, packet delays — is a labeled draw from one recorded tape, then replaying
the tape (no PRNG at all) reproduces the run byte-for-byte: same event log,
same fault decisions, same outcome. And editing one draw on the tape yields a
*different but valid* universe — the tape IS the universe.

Run:    uv run python spikes/s4_tape_replay.py
Pass:   prints "S4 PASS" and exits 0.

The workload is a retry protocol over a lossy datagram link: client sends 5
pings, retries on a 200ms (virtual) timeout; server acks; both directions drop
~25% of packets and delay the rest by a tape-drawn amount.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
from typing import Any

from s1_seeded_scheduler import SpikeLoop


class Tape:
    """A sequence of labeled integer draws — generate, record, replay, edit.

    Recording: values come from the seeded RNG and are appended to the tape.
    Replaying: values come from the recorded sequence; a label mismatch or an
    exhausted tape (possible after edits change control flow) falls back to a
    fixed-seed RNG for the remainder, Hypothesis-style.
    """

    def __init__(self, rng: random.Random, recorded: list[tuple[str, int]] | None = None):
        self._rng = rng
        self._recorded = recorded
        self._position = 0
        self._misaligned = False
        self.draws: list[tuple[str, int]] = []

    def draw(self, label: str, n: int) -> int:
        """A labeled draw of an int in [0, n)."""
        value: int | None = None
        if self._recorded is not None and not self._misaligned:
            if self._position < len(self._recorded):
                rec_label, rec_value = self._recorded[self._position]
                if rec_label == label and 0 <= rec_value < n:
                    value = rec_value
                else:
                    self._misaligned = True
            else:
                self._misaligned = True
        if value is None:
            value = self._rng.randrange(n)
        self._position += 1
        self.draws.append((label, value))
        return value

    def draw_bool(self, label: str, percent_true: int) -> bool:
        return self.draw(label, 100) < percent_true


class TapeLoop(SpikeLoop):
    """SpikeLoop whose every scheduling pick is a labeled tape draw."""

    def __init__(self, tape: Tape) -> None:
        super().__init__(seed=0)  # the inherited RNG is never used...
        self._rng = _ForbiddenRandom()  # ...and we make sure of it
        self.tape = tape

    def _choose(self, n: int) -> int:
        return self.tape.draw("sched.pick", n)


class _ForbiddenRandom(random.Random):
    """Trips if anything reaches for loop randomness behind the tape's back."""

    def random(self) -> float:
        raise AssertionError("nondeterminism outside the tape")

    def randrange(self, *args: object, **kwargs: object) -> int:  # type: ignore[override]
        raise AssertionError("nondeterminism outside the tape")


class LossyLink:
    """Unidirectional datagram link; drop and delay are tape draws."""

    def __init__(self, loop: TapeLoop, name: str, drop_percent: int) -> None:
        self._loop = loop
        self._name = name
        self._drop_percent = drop_percent
        self._inbox: asyncio.Queue[str] = asyncio.Queue()
        self.sent = 0
        self.dropped = 0

    def send(self, message: str) -> None:
        self.sent += 1
        if self._loop.tape.draw_bool(f"{self._name}.drop", self._drop_percent):
            self.dropped += 1
            return
        delay_ms = 1 + self._loop.tape.draw(f"{self._name}.delay_ms", 50)
        self._loop.call_later(delay_ms / 1000.0, self._inbox.put_nowait, message)

    async def recv(self) -> str:
        return await self._inbox.get()


async def retry_protocol(loop: TapeLoop) -> dict[str, Any]:
    """Client pings 5 times over lossy links, retrying each ping until acked."""
    to_server = LossyLink(loop, "c2s", drop_percent=25)
    to_client = LossyLink(loop, "s2c", drop_percent=25)
    acked: set[int] = set()

    async def server() -> None:
        while len(acked) < 5:
            message = await to_server.recv()
            ping_id = int(message.removeprefix("ping-"))
            to_client.send(f"ack-{ping_id}")

    async def client() -> None:
        for ping_id in range(5):
            while ping_id not in acked:
                to_server.send(f"ping-{ping_id}")
                deadline = loop.time() + 0.2
                while loop.time() < deadline and ping_id not in acked:
                    await asyncio.sleep(0.02)

    async def ack_reader() -> None:
        while len(acked) < 5:
            message = await to_client.recv()
            acked.add(int(message.removeprefix("ack-")))

    reader_task = loop.create_task(ack_reader())
    server_task = loop.create_task(server())
    await client()
    server_task.cancel()
    reader_task.cancel()
    return {
        "acked": sorted(acked),
        "pings_sent": to_server.sent,
        "pings_dropped": to_server.dropped,
        "acks_sent": to_client.sent,
        "acks_dropped": to_client.dropped,
        "virtual_duration": round(loop.time(), 6),
    }


def execute(tape: Tape) -> tuple[dict[str, Any], str]:
    loop = TapeLoop(tape)
    result = loop.run_until_complete(retry_protocol(loop))
    raw = "\n".join(json.dumps(e, sort_keys=True) for e in loop.events).encode()
    return result, hashlib.sha256(raw).hexdigest()


def main() -> None:
    # 1. Generate: a fresh universe from a seed; every decision lands on the tape.
    recording = Tape(random.Random(1234))
    result1, digest1 = execute(recording)
    assert result1["acked"] == [0, 1, 2, 3, 4]
    assert result1["pings_dropped"] + result1["acks_dropped"] > 0, (
        "want faults in this universe; pick a different seed"
    )

    # 2. Replay: the recorded tape, PRNG forbidden -> byte-identical universe.
    replay = Tape(_ForbiddenRandom(), recorded=list(recording.draws))
    result2, digest2 = execute(replay)
    assert result2 == result1, f"replay result diverged: {result2} != {result1}"
    assert digest2 == digest1, "replay event log diverged"
    assert replay.draws == recording.draws, "replay consumed different draws"

    # 3. Edit one fault decision (a kept packet becomes dropped) -> a different
    #    but still-valid universe, no other code change.
    edited = list(recording.draws)
    flip_at = next(
        i
        for i, (label, value) in enumerate(edited)
        if label == "c2s.drop" and value >= 25  # this packet was NOT dropped...
    )
    edited[flip_at] = ("c2s.drop", 0)  # ...now it is.
    mutated = Tape(random.Random(99), recorded=edited)
    result3, digest3 = execute(mutated)
    assert result3["acked"] == [0, 1, 2, 3, 4], "protocol should still survive"
    assert digest3 != digest1, "flipping a fault decision changed nothing?"

    print(f"recorded universe: {result1}")
    print(f"  tape: {len(recording.draws)} draws, log sha256 {digest1[:16]}")
    print(f"replayed universe: identical ({digest2[:16]})")
    print(
        f"edited universe (packet {flip_at} force-dropped): "
        f"{result3['pings_sent']} pings sent vs {result1['pings_sent']}, "
        f"duration {result3['virtual_duration']}s vs {result1['virtual_duration']}s"
    )
    print("\nS4 PASS — the tape is the universe: replayable byte-for-byte, editable.")


if __name__ == "__main__":
    main()
