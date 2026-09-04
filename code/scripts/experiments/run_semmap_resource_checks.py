#!/usr/bin/env python3
"""Run fixture-only SemMap resource qualification (metric schema v2) on Linux."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.postgresql.semmap_resource_runner import main

if __name__ == '__main__':
    main()
