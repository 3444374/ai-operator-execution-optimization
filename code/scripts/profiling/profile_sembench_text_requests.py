#!/usr/bin/env python3
"""Profile original SemBench requests with a local tokenizer; never run a model."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.experiments.sembench_request_profile import main


if __name__ == '__main__':
    raise SystemExit(main())
