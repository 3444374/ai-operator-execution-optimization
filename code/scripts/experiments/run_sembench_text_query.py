#!/usr/bin/env python3
"""Original SemBench single-operator text queries; native owners retain execution."""
import time
CONTROLLER_ENTRY_NS = time.monotonic_ns()

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.experiments.sembench_text_query import main  # noqa: E402


if __name__ == '__main__':
    raise SystemExit(main(controller_entry_ns=CONTROLLER_ENTRY_NS))
