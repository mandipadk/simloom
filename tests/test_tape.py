"""The choice tape: generation, replay, misalignment, serialization."""

from __future__ import annotations

import json

import pytest
from hypothesis import given
from hypothesis import strategies as st

from simloom import Draw, MisalignmentPolicy, Tape, TapeMisalignmentError

LABELS = st.text(min_size=1, max_size=20)
BOUNDS = st.integers(min_value=1, max_value=1000)
SITES = st.lists(st.tuples(LABELS, BOUNDS), min_size=1, max_size=200)


class TestGenerate:
    def test_same_seed_same_draws(self) -> None:
        a, b = Tape.generate(42), Tape.generate(42)
        for tape in (a, b):
            for i in range(100):
                tape.draw(f"site-{i % 7}", (i % 13) + 1)
        assert a.draws == b.draws

    def test_different_seeds_differ(self) -> None:
        a, b = Tape.generate(1), Tape.generate(2)
        values_a = [a.draw("x", 1000) for _ in range(20)]
        values_b = [b.draw("x", 1000) for _ in range(20)]
        assert values_a != values_b

    def test_values_within_bounds(self) -> None:
        tape = Tape.generate(7)
        for bound in (1, 2, 3, 100):
            for _ in range(50):
                assert 0 <= tape.draw("x", bound) < bound

    def test_bound_one_is_forced(self) -> None:
        tape = Tape.generate(0)
        assert tape.draw("forced", 1) == 0

    def test_invalid_bound_rejected(self) -> None:
        tape = Tape.generate(0)
        with pytest.raises(ValueError, match="bound"):
            tape.draw("x", 0)
        with pytest.raises(ValueError, match="bound"):
            tape.draw("x", -3)

    def test_seed_exposed(self) -> None:
        assert Tape.generate(99).seed == 99
        assert Tape.replay(()).seed is None


class TestReplay:
    def make_recording(self) -> Tape:
        tape = Tape.generate(123)
        tape.draw("a", 10)
        tape.draw("b", 5)
        tape.draw("a", 10)
        return tape

    def test_exact_replay(self) -> None:
        recording = self.make_recording()
        replayed = Tape.replay(recording)
        values = [replayed.draw("a", 10), replayed.draw("b", 5), replayed.draw("a", 10)]
        assert values == [d.value for d in recording.draws]
        assert replayed.replay_exact
        assert replayed.draws == recording.draws

    def test_label_mismatch_strict(self) -> None:
        replayed = Tape.replay(self.make_recording())
        replayed.draw("a", 10)
        with pytest.raises(TapeMisalignmentError, match="'wrong'"):
            replayed.draw("wrong", 5)

    def test_bound_mismatch_strict(self) -> None:
        replayed = Tape.replay(self.make_recording())
        with pytest.raises(TapeMisalignmentError, match="bound 11"):
            replayed.draw("a", 11)

    def test_exhaustion_strict(self) -> None:
        replayed = Tape.replay(self.make_recording())
        replayed.draw("a", 10)
        replayed.draw("b", 5)
        replayed.draw("a", 10)
        with pytest.raises(TapeMisalignmentError, match="exhausted"):
            replayed.draw("a", 10)

    def test_fallback_continues_deterministically(self) -> None:
        recording = self.make_recording()

        def diverge() -> list[int]:
            tape = Tape.replay(recording, policy=MisalignmentPolicy.FALLBACK, fallback_seed=7)
            values = [tape.draw("a", 10), tape.draw("DIVERGED", 99), tape.draw("z", 50)]
            assert tape.misaligned_at == 1
            assert not tape.replay_exact
            return values

        assert diverge() == diverge()

    def test_not_misaligned_until_divergence(self) -> None:
        replayed = Tape.replay(self.make_recording())
        replayed.draw("a", 10)
        assert replayed.misaligned_at is None
        assert not replayed.replay_exact  # not finished yet

    def test_replay_accepts_draw_iterable(self) -> None:
        draws = (Draw("x", 3, 5), Draw("y", 0, 1))
        replayed = Tape.replay(draws)
        assert replayed.draw("x", 5) == 3
        assert replayed.draw("y", 1) == 0
        assert replayed.replay_exact


class TestDraw:
    def test_validation(self) -> None:
        with pytest.raises(ValueError, match="outside"):
            Draw("x", 5, 5)
        with pytest.raises(ValueError, match="outside"):
            Draw("x", -1, 5)
        with pytest.raises(ValueError, match="bound"):
            Draw("x", 0, 0)

    def test_frozen(self) -> None:
        draw = Draw("x", 1, 2)
        with pytest.raises(AttributeError):
            draw.value = 0  # type: ignore[misc]


class TestSerialization:
    def test_round_trip(self) -> None:
        recording = Tape.generate(55)
        for i in range(30):
            recording.draw(f"site-{i % 3}", (i % 9) + 1)
        replayed = Tape.loads(recording.dumps())
        for draw in recording.draws:
            assert replayed.draw(draw.label, draw.bound) == draw.value
        assert replayed.replay_exact

    def test_header_fields(self) -> None:
        data = json.loads(Tape.generate(9).dumps())
        assert data["format"] == "simloom-tape"
        assert data["version"] == 1
        assert data["seed"] == 9

    def test_rejects_wrong_format(self) -> None:
        with pytest.raises(ValueError, match="not a simloom tape"):
            Tape.loads('{"format": "something-else", "version": 1, "draws": []}')

    def test_rejects_wrong_version(self) -> None:
        with pytest.raises(ValueError, match="version"):
            Tape.loads('{"format": "simloom-tape", "version": 999, "draws": []}')

    def test_rejects_malformed_draws(self) -> None:
        with pytest.raises(ValueError, match="malformed"):
            Tape.loads('{"format": "simloom-tape", "version": 1, "draws": [[1, 2]]}')

    def test_rejects_non_object(self) -> None:
        with pytest.raises(ValueError, match="JSON object"):
            Tape.loads("[1, 2, 3]")


class TestProperties:
    @given(seed=st.integers(min_value=0, max_value=2**63), sites=SITES)
    def test_generate_replay_equality(self, seed: int, sites: list[tuple[str, int]]) -> None:
        recording = Tape.generate(seed)
        values = [recording.draw(label, bound) for label, bound in sites]
        replayed = Tape.replay(recording)
        assert [replayed.draw(label, bound) for label, bound in sites] == values
        assert replayed.replay_exact

    @given(seed=st.integers(min_value=0, max_value=2**63), sites=SITES)
    def test_serialization_round_trip(self, seed: int, sites: list[tuple[str, int]]) -> None:
        recording = Tape.generate(seed)
        for label, bound in sites:
            recording.draw(label, bound)
        restored = Tape.loads(recording.dumps())
        for draw in recording.draws:
            assert restored.draw(draw.label, draw.bound) == draw.value

    @given(seed=st.integers(min_value=0, max_value=2**63), sites=SITES)
    def test_draws_always_within_bounds(self, seed: int, sites: list[tuple[str, int]]) -> None:
        tape = Tape.generate(seed)
        for label, bound in sites:
            assert 0 <= tape.draw(label, bound) < bound
