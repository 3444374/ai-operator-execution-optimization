#!/usr/bin/env python3
"""Bounded full-query option validation; all site configuration stays outside Git."""
import time
PROCESS_STARTED_NS = time.monotonic_ns()
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.record_chain_validation_run import main

if __name__ == '__main__':
    raise SystemExit(main(process_started_ns=PROCESS_STARTED_NS))
