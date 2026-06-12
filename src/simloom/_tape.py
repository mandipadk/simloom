"""The choice tape: simloom's single source of nondeterminism.

Every decision the simulation makes — which ready callback runs next, whether
a packet drops, how long a link delays — is a labeled, bounded integer draw
from one tape. While exploring, draws come from a seeded PRNG and are
recorded; a recorded tape replays the identical universe, byte for byte, and
is the artifact a failure ships as. The design follows Hypothesis's
conjecture choice sequence: a flat list of bounded integer draws is trivially
serializable and shrinkable.

There is deliberately no second randomness source anywhere in simloom.
"""

from __future__ import annotations

import enum
import json
import random
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from ._errors import TapeMisalignmentError

TAPE_FORMAT = "simloom-tape"


class _ZeroRandom(random.Random):
    """A fallback that always answers 0: misaligned tails complete along
    the canonical (FIFO) path instead of a random one. The shrinker's
    refill of choice."""

    def randrange(self, *args: Any, **kwargs: Any) -> int:
        return 0


TAPE_FORMAT_VERSION = 1


@dataclass(frozen=True, slots=True)
class Draw:
    """One recorded decision: ``value`` was drawn from ``[0, bound)`` at the
    draw site identified by ``label``."""

    label: str
    value: int
    bound: int

    def __post_init__(self) -> None:
        if self.bound < 1:
            raise ValueError(f"draw bound must be >= 1, got {self.bound}")
        if not 0 <= self.value < self.bound:
            raise ValueError(f"draw value {self.value} outside [0, {self.bound})")


class MisalignmentPolicy(enum.Enum):
    """What a replaying tape does when execution diverges from the recording.

    STRICT is for replaying failure artifacts: divergence means the recording
    no longer matches the program, and silently improvising would undermine
    the reproducibility claim. FALLBACK is for tape *editing* (mutation,
    shrinking): an edited prefix legitimately changes the control flow that
    follows, and the remainder of the universe is filled in deterministically
    from a fixed-seed PRNG.
    """

    STRICT = "strict"
    FALLBACK = "fallback"


class Tape:
    """A sequence of labeled bounded draws; generate, record, replay.

    Use the constructors:

    - :meth:`Tape.generate` — fresh universe from a seed; draws are recorded.
    - :meth:`Tape.replay` — re-feed recorded draws to a re-execution.

    The consumed draws are always available as :attr:`draws`, so a replayed
    or fallback-extended tape re-records itself exactly.
    """

    __slots__ = (
        "_draws",
        "_fallback_seed",
        "_misaligned_at",
        "_policy",
        "_position",
        "_recorded",
        "_rng",
        "_seed",
    )

    def __init__(
        self,
        *,
        _rng: random.Random,
        _seed: int | None,
        _recorded: tuple[Draw, ...] | None,
        _policy: MisalignmentPolicy,
        _fallback_seed: int,
    ) -> None:
        self._rng = _rng
        self._seed = _seed
        self._recorded = _recorded
        self._policy = _policy
        self._fallback_seed = _fallback_seed
        self._position = 0
        self._misaligned_at: int | None = None
        self._draws: list[Draw] = []

    # --- constructors ---

    @classmethod
    def generate(cls, seed: int) -> Tape:
        """A fresh universe: draws come from a PRNG seeded with ``seed``."""
        return cls(
            _rng=random.Random(seed),
            _seed=seed,
            _recorded=None,
            _policy=MisalignmentPolicy.STRICT,
            _fallback_seed=0,
        )

    @classmethod
    def replay(
        cls,
        recorded: Tape | Iterable[Draw],
        *,
        policy: MisalignmentPolicy = MisalignmentPolicy.STRICT,
        fallback_seed: int = 0,
        fallback: str = "rng",
    ) -> Tape:
        """Re-feed a recorded universe to a re-execution of the program.

        ``fallback`` selects what fills draws after a FALLBACK divergence:
        ``"rng"`` (seeded PRNG) or ``"zero"`` (every draw answers 0 — the
        canonical FIFO completion, which the shrinker wants).
        """
        if fallback not in ("rng", "zero"):
            raise ValueError(f"unknown fallback {fallback!r}")
        draws = recorded.draws if isinstance(recorded, Tape) else tuple(recorded)
        rng = _ZeroRandom() if fallback == "zero" else random.Random(fallback_seed)
        return cls(
            _rng=rng,
            _seed=None,
            _recorded=draws,
            _policy=policy,
            _fallback_seed=fallback_seed,
        )

    # --- drawing ---

    def draw(self, label: str, bound: int) -> int:
        """Draw an integer in ``[0, bound)`` at the draw site ``label``."""
        if bound < 1:
            raise ValueError(f"draw bound must be >= 1, got {bound}")
        value = self._next_value(label, bound)
        self._draws.append(Draw(label, value, bound))
        self._position += 1
        return value

    def _next_value(self, label: str, bound: int) -> int:
        if self._recorded is None or self._misaligned_at is not None:
            return self._rng.randrange(bound)
        if self._position >= len(self._recorded):
            self._diverge(
                f"tape exhausted: draw #{self._position} ({label!r}, bound {bound}) "
                f"requested but only {len(self._recorded)} draws were recorded"
            )
            return self._rng.randrange(bound)
        recorded = self._recorded[self._position]
        if recorded.label != label or recorded.bound != bound:
            self._diverge(
                f"draw #{self._position} requested ({label!r}, bound {bound}) but the "
                f"tape recorded ({recorded.label!r}, bound {recorded.bound})"
            )
            return self._rng.randrange(bound)
        return recorded.value

    def _diverge(self, detail: str) -> None:
        if self._policy is MisalignmentPolicy.STRICT:
            raise TapeMisalignmentError(
                f"{detail}. The execution diverged from the recording — the code "
                f"under test changed, hash randomization is unpinned across "
                f"processes, or nondeterminism escaped the simulation."
            )
        self._misaligned_at = self._position

    def force_fallback(self) -> None:
        """Stop consulting the recording; draw from the fallback PRNG from
        here on, regardless of policy.

        Used after a strict replay raises ``TapeMisalignmentError``: the
        recording is already known not to match, but loop teardown still
        needs draws, and it should get deterministic ones instead of a
        second error mid-cleanup.
        """
        if self._recorded is not None and self._misaligned_at is None:
            self._misaligned_at = self._position

    # --- introspection ---

    @property
    def seed(self) -> int | None:
        """The generating seed, or None for a replayed tape."""
        return self._seed

    @property
    def is_replay(self) -> bool:
        return self._recorded is not None

    @property
    def policy(self) -> MisalignmentPolicy:
        return self._policy

    @property
    def recorded_length(self) -> int | None:
        return None if self._recorded is None else len(self._recorded)

    @property
    def draws(self) -> tuple[Draw, ...]:
        """Every draw consumed so far — the exact, re-recordable universe."""
        return tuple(self._draws)

    @property
    def position(self) -> int:
        return self._position

    @property
    def misaligned_at(self) -> int | None:
        """Index of the first diverging draw under FALLBACK, else None."""
        return self._misaligned_at

    @property
    def replay_exact(self) -> bool:
        """True iff this tape replayed its full recording without divergence."""
        return (
            self._recorded is not None
            and self._misaligned_at is None
            and self._position == len(self._recorded)
        )

    def __len__(self) -> int:
        return len(self._draws)

    def __repr__(self) -> str:
        mode = "generate" if self._recorded is None else "replay"
        return f"<Tape {mode} seed={self._seed} draws={len(self._draws)}>"

    # --- serialization (versioned, self-describing; see docs/event-log.md) ---

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": TAPE_FORMAT,
            "version": TAPE_FORMAT_VERSION,
            "seed": self._seed,
            "draws": [[d.label, d.value, d.bound] for d in self._draws],
        }

    def dumps(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @staticmethod
    def draws_from_dict(data: dict[str, Any]) -> tuple[Draw, ...]:
        """Validate a serialized tape and return its draws (for replay)."""
        if data.get("format") != TAPE_FORMAT:
            raise ValueError(f"not a simloom tape: format={data.get('format')!r}")
        if data.get("version") != TAPE_FORMAT_VERSION:
            raise ValueError(
                f"unsupported tape version {data.get('version')!r} "
                f"(this simloom reads version {TAPE_FORMAT_VERSION})"
            )
        raw = data["draws"]
        if not isinstance(raw, list):
            raise ValueError("tape draws must be a list")
        draws = []
        for entry in raw:
            match entry:
                case [str(label), int(value), int(bound)]:
                    draws.append(Draw(label, value, bound))
                case _:
                    raise ValueError(f"malformed tape entry: {entry!r}")
        return tuple(draws)

    @classmethod
    def loads(
        cls,
        serialized: str,
        *,
        policy: MisalignmentPolicy = MisalignmentPolicy.STRICT,
        fallback_seed: int = 0,
    ) -> Tape:
        """Deserialize a tape straight into replay mode."""
        data = json.loads(serialized)
        if not isinstance(data, dict):
            raise ValueError("serialized tape must be a JSON object")
        draws = cls.draws_from_dict(data)
        return cls.replay(draws, policy=policy, fallback_seed=fallback_seed)


def replay_draws(draws: Sequence[Draw]) -> Tape:
    """Shorthand for :meth:`Tape.replay` with strict policy."""
    return Tape.replay(tuple(draws))
