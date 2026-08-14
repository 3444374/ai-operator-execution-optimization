#!/usr/bin/env python3
"""Validate one matched direct ceiling against the sealed SAOR rehearsal."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.saor.feeding_ceiling import (  # noqa: E402
    summarize_feeding_ceiling,
    write_summary,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--ceiling-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = summarize_feeding_ceiling(
        args.project_root.resolve(),
        args.ceiling_root.resolve(),
    )
    write_summary(args.output.resolve(), result)
    return 0 if result["evidence_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
