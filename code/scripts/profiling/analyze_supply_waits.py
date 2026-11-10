#!/usr/bin/env python3
"""Analyze legal ready waits against delivered native queue snapshots."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.supply_diagnostics import analyze_directory
from src.infrastructure.private_artifacts import write_private_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events-dir', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--max-events', type=int, default=1_000_000)
    args = parser.parse_args()
    if args.max_events <= 0:
        parser.error('--max-events must be positive')
    report = analyze_directory(args.events_dir, max_events=args.max_events)
    write_private_json(Path(args.output), report)
    print(json.dumps(dict(capture_status=report['capture_status'], summary=report['summary'])))


if __name__ == '__main__':
    main()
