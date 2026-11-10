#!/usr/bin/env python3
"""Run a finite, CPU-only original-LOTUS media preparation study."""
import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.infrastructure.private_artifacts import read_json
from src.experiments.media_retention_study import run_study


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    os.environ.setdefault('HF_HUB_OFFLINE', '1')
    os.environ.setdefault('LITELLM_LOCAL_MODEL_COST_MAP', 'True')
    result = run_study(read_json(args.config), args.output)
    print(json.dumps({key: result[key] for key in ('input_kind', 'model_calls', 'gpu', 'matched_outputs', 'rows', 'passes', 'repeats')}))


if __name__ == '__main__':
    main()
