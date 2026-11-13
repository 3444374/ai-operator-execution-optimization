#!/usr/bin/env python3
"""Run complete original LOTUS queries against a declared native model service."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.experiments.sembench_lotus_query import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
