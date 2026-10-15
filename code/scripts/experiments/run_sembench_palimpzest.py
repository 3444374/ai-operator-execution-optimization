#!/usr/bin/env python3
"""Run fixed-source SemBench Movie Q3 (optional Q2) on original parallel PZ."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.palimpzest.native import main

if __name__ == '__main__':
    raise SystemExit(main())
