#!/usr/bin/env python3
"""Run the pinned original Movie methods on explicitly selected LOTUS arms."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.postgresql.sembench_query_cli import main

if __name__ == "__main__":
    raise SystemExit(main())
