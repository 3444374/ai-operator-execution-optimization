"""Recover original experiment files from a retained storage manifest."""

import argparse
import json
from pathlib import Path

from src.experiments.evidence_storage import restore_result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--repository', type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    print(json.dumps(restore_result(args.manifest, args.output, repository=args.repository)))


if __name__ == '__main__':
    main()
