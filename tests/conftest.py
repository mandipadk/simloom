"""Make the examples importable from tests (they double as test fixtures)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))
