#!/usr/bin/env python3
"""Summarize explicit retained observations without running experiments."""

import argparse
import json
from pathlib import Path
import sys

CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
sys.path.insert(0, str(CODE_ROOT))

from src.observability.metrics.offline_results import SCHEMAS, analyze


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--schema", choices=SCHEMAS, dest="input_schema")
    parser.add_argument("--comparison-scope")
    parser.add_argument("--root", type=Path, help="base for explicit relative input paths")
    for name in ("queries", "api-samples", "lifecycles", "results"):
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--output", type=Path, help="new JSON file; defaults to stdout")
    args = vars(parser.parse_args())
    output = args.pop("output")
    try:
        result = analyze(**args)
        payload = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
        if output:
            with output.open("x", encoding="utf-8") as stream:
                stream.write(payload)
        else:
            print(payload, end="")
    except (ValueError, KeyError, TypeError, OSError) as error:
        parser.exit(2, "offline analysis failed: " + str(error) + "\n")


if __name__ == "__main__":
    main()
