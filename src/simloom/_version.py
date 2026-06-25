"""Package version, importable without cycles.

Single source of truth: pyproject.toml reads ``__version__`` from this file
(``[tool.hatch.version]``), and the release workflow checks that a release
tag matches it. Bump here, tag ``vX.Y.Z``, publish.
"""

__version__ = "0.2.0"
