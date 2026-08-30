"""Canonical command-line entry for the external semantic execution provider."""

from __future__ import annotations

import sys
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[2]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.execution_provider.server import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
