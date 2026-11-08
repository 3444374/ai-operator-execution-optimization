#!/usr/bin/env python3
"""Check original visual-query inputs and budgets; never start a model."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.experiments.sembench_image_query import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
