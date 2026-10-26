#!/usr/bin/env python3
"""Check/run a sealed Movie matrix; no historical launcher or SSH dependency."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.experiments.movie_records_matrix import main

if __name__ == '__main__':
    raise SystemExit(main())
