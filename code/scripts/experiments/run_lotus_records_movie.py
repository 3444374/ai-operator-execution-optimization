#!/usr/bin/env python3
"""Ordinary Movie Q3 records/local-model qualification entry."""

import os
from pathlib import Path
import sys

os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")
os.environ.setdefault("VLLM_WORKER_MULTIPROC_METHOD", "spawn")
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.experiments.lotus_records_movie import main

if __name__ == "__main__":
    raise SystemExit(main())
