"""Compatibility import for the canonical frozen recording wire contract."""

from __future__ import annotations

import sys
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[3]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.execution_provider.adapters.recording import run_recording_session  # noqa: E402
from src.execution_provider.wire.v2 import *  # noqa: E402,F403
from src.execution_provider.wire.v2 import __all__ as _WIRE_V2_PUBLIC  # noqa: E402


__all__ = [*_WIRE_V2_PUBLIC, "run_recording_session"]
