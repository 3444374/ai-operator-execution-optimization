"""CLI entrypoint for native-framework single/multi-job characterization."""

from __future__ import annotations

import sys
from pathlib import Path


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.text.orchestration.native_multijob import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
