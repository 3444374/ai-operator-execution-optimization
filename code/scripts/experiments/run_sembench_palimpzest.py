#!/usr/bin/env python3
"""Run original SemBench PZ or its explicitly selected fixed-plan backend."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.palimpzest.runner import main

if __name__ == '__main__':
    raise SystemExit(main())
