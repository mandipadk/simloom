"""The pytest half of ``@simloom.test``: options, fixture, hashseed warning.

Registered via the ``pytest11`` entry point; loads automatically wherever
simloom is installed.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest

from ._run import _hash_randomization_pinned
from ._testing import Settings


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("simloom", "deterministic simulation testing")
    group.addoption(
        "--simloom-seed",
        type=int,
        default=None,
        help="run @simloom.test tests under exactly this seed (replay a failure)",
    )
    group.addoption(
        "--simloom-runs",
        type=int,
        default=None,
        help="override the number of universes each @simloom.test explores",
    )
    group.addoption(
        "--simloom-tape",
        default=None,
        help="replay a recorded tape artifact (.tape.json) against the test",
    )
    group.addoption(
        "--simloom-no-shrink",
        action="store_true",
        help="skip shrinking when a failure is found",
    )
    group.addoption(
        "--simloom-check-determinism",
        action="store_true",
        help="run each explored seed twice and fail if the two universes differ "
        "(catches nondeterminism the tape does not control)",
    )


def pytest_configure(config: pytest.Config) -> None:
    if not _hash_randomization_pinned() and (
        config.getoption("--simloom-seed") is not None
        or config.getoption("--simloom-tape") is not None
    ):
        warnings.warn(
            "PYTHONHASHSEED is not pinned: a seed or tape recorded in another "
            "process may not replay if the program iterates hash-randomized "
            "sets/dicts. Run with PYTHONHASHSEED=0.",
            stacklevel=1,
        )


@pytest.fixture
def simloom_settings(request: pytest.FixtureRequest) -> Settings:
    config = request.config
    return Settings(
        seed_override=config.getoption("--simloom-seed"),
        runs_override=config.getoption("--simloom-runs"),
        tape_path=config.getoption("--simloom-tape"),
        artifact_dir=Path(config.rootpath) / ".sim" / "failures",
        shrink_enabled=not config.getoption("--simloom-no-shrink"),
        force_check_determinism=bool(config.getoption("--simloom-check-determinism")),
    )
