#!/usr/bin/env python3
"""Prepare or run the complete original LOTUS MMQA Q5 aggregation."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.experiments.sembench_lotus_mmqa_aggregate import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
