#!/usr/bin/env python3
"""Check or execute a separately authorized finite records/local-model Q3 matrix."""
import os
from pathlib import Path
import sys

for key, value in {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
        "LITELLM_LOCAL_MODEL_COST_MAP": "true", "VLLM_WORKER_MULTIPROC_METHOD": "spawn"}.items():
    os.environ[key] = value
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.experiments.sembench_records_formal import main

if __name__ == "__main__":
    raise SystemExit(main())
