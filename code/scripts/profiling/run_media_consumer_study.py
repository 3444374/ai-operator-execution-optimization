#!/usr/bin/env python3
"""Compare image retention using explicit complete/abandoned consumer traces."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.media_consumer_study import run_study
from src.infrastructure.private_artifacts import read_json


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True); parser.add_argument('--output', required=True)
    args=parser.parse_args()
    report=run_study(read_json(args.config), args.output)
    print(json.dumps(dict(arms=len(report['arms']), capacities=report['capacities'], model_calls=0, gpu=False)))


if __name__ == '__main__':
    main()
