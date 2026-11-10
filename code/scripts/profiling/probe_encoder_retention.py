#!/usr/bin/env python3
"""Probe consumer-value hints against the selected native encoder-cache source."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.encoder_retention_probe import probe
from src.infrastructure.private_artifacts import write_private_json


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True); parser.add_argument('--sha256', required=True)
    parser.add_argument('--output', required=True)
    args=parser.parse_args()
    result=probe(args.source, args.sha256)
    write_private_json(Path(args.output), result)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
