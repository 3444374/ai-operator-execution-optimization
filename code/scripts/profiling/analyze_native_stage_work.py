#!/usr/bin/env python3
"""Report observed model-stage positions and encoder cache transitions."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.native_stage_diagnostics import analyze_directory
from src.infrastructure.private_artifacts import write_private_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events-dir', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    report = analyze_directory(args.events_dir)
    write_private_json(Path(args.output), report)
    print(json.dumps(dict(capture_status=report['capture_status'], iterations=len(report['iterations']),
                          cache_observations=len(report['encoder_cache_changes']))))


if __name__ == '__main__':
    main()
