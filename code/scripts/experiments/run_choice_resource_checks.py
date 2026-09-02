#!/usr/bin/env python3
"""Run fixture-only PostgreSQL choice resource qualification on Linux."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.choice_resource_checks import main

if __name__ == '__main__':
    main()
