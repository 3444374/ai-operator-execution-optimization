#!/usr/bin/env python3
"""Freeze the shared short/three-long image multi-job PostgreSQL slices."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.image_multijob.manifest import build_image_job_manifest  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--workload-name", default="coco_train2017_60k")
    parser.add_argument("--short-rows", type=int, default=2000)
    parser.add_argument("--long-rows", type=int, default=3000)
    parser.add_argument("--late-offset-s", type=float, default=0.5)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest = build_image_job_manifest(
        database_url=args.database_url,
        workload_name=args.workload_name,
        short_rows=args.short_rows,
        long_rows=args.long_rows,
        late_offset_s=args.late_offset_s,
        output_path=args.output,
    )
    print(json.dumps({"status": "ready", "path": str(manifest.path), "sha256": manifest.sha256}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
