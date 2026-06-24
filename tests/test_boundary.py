"""Phase G — the boundary registry (the enforced honesty contract) and the
PYTHONHASHSEED auto-pin."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import simloom
from simloom import BoundaryStatus, boundary
from simloom._boundary import detected_apis

_SRC = Path(simloom.__file__).parent


def _escape_apis_in_source() -> set[str]:
    """Every API string actually passed to an escape at a call site."""
    apis: set[str] = set()
    loop_src = (_SRC / "_loop.py").read_text()
    apis |= set(re.findall(r'self\._escape(?:_network|_fd)?\(\s*"([^"]+)"', loop_src))
    net_src = (_SRC / "_net.py").read_text()
    apis |= set(re.findall(r'api="([^"]+)"', net_src))
    return apis


class TestRegistry:
    def test_every_escape_site_is_registered_as_detected(self) -> None:
        registered = detected_apis()
        for api in _escape_apis_in_source():
            assert api in registered, f"escape site {api!r} missing from the boundary registry"

    def test_every_detected_entry_has_a_real_escape_site(self) -> None:
        in_source = _escape_apis_in_source()
        for api in detected_apis():
            assert api in in_source, (
                f"registry lists {api!r} as DETECTED but no escape site raises it"
            )

    def test_lookup_and_statuses(self) -> None:
        assert simloom.boundary.__doc__  # public
        entry = simloom._boundary.lookup("time.time")
        assert entry is not None
        assert entry.status is BoundaryStatus.PATCHED
        assert simloom._boundary.lookup("loop.add_reader").status is BoundaryStatus.DETECTED  # type: ignore[union-attr]
        assert simloom._boundary.lookup("loop.time").status is BoundaryStatus.SIMULATED  # type: ignore[union-attr]
        assert simloom._boundary.lookup("datetime.now").status is BoundaryStatus.DOCUMENTED  # type: ignore[union-attr]
        assert simloom._boundary.lookup("nonexistent.api") is None

    def test_registry_is_nonempty_and_every_status_used(self) -> None:
        entries = boundary()
        assert len(entries) > 20
        used = {e.status for e in entries}
        assert used == set(BoundaryStatus)  # every status category is represented


_PROBE = (
    "import os, sys, simloom\n"
    "simloom.pin_hashseed()\n"
    "print(sys.flags.hash_randomization, os.environ.get('_SIMLOOM_HASHSEED_REEXEC', 'no'))\n"
)


class TestHashseedPinning:
    def _probe(self, tmp_path: Path, *, pinned: bool) -> str:
        # A real script file (not `python -c`), since `-c` code is not in argv
        # and so cannot be re-exec'd.
        script = tmp_path / "probe.py"
        script.write_text(_PROBE)
        env = {k: v for k, v in os.environ.items() if k != "PYTHONHASHSEED"}
        if pinned:
            env["PYTHONHASHSEED"] = "0"
        out = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        return out.stdout.strip()

    def test_unpinned_interpreter_is_re_execd_and_pinned(self, tmp_path: Path) -> None:
        # starts unpinned -> pin_hashseed re-execs with PYTHONHASHSEED=0
        assert self._probe(tmp_path, pinned=False) == "0 1"

    def test_already_pinned_is_a_noop(self, tmp_path: Path) -> None:
        # starts pinned -> no re-exec (sentinel absent)
        assert self._probe(tmp_path, pinned=True) == "0 no"

    def test_is_pinned_reflects_the_flag(self) -> None:
        # in this test process, is_pinned matches the interpreter flag/env
        assert simloom.is_pinned() == (
            sys.flags.hash_randomization == 0
            or os.environ.get("PYTHONHASHSEED", "") not in ("", "random")
        )
