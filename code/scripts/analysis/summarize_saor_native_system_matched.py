#!/usr/bin/env python3
"""Validate stored SAOR matched-system evidence and emit frozen summaries."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.saor.native_system_summary import summarize_matched_system


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--formal-authorization", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = _args()
    return 0 if summarize_matched_system(
        args.matrix_root,
        args.output_dir,
        formal_authorization_path=args.formal_authorization,
    ) else 2


if __name__ == "__main__":
    raise SystemExit(main())
