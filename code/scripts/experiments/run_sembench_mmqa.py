#!/usr/bin/env python3
"""Prepare, verify, or observe the original finite MMQA Q2b query."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.sembench_mmqa import main

if __name__ == "__main__":
    raise SystemExit(main())
