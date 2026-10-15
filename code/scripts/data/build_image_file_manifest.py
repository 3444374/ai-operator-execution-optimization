#!/usr/bin/env python3
"""Prepare a fixed COCO JPEG file selection once; no database, model or GPU."""

import argparse
import json
import sys
from pathlib import Path

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.image.file_source import build_image_file_manifest  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image-root", required=True, type=Path)
    parser.add_argument("--workload-name", default="coco_val2017")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest = build_image_file_manifest(root=args.image_root, workload_name=args.workload_name,
                                         output=args.output, limit=args.limit, offset=args.offset)
    print(json.dumps({"source_kind": "files", "manifest_sha256": manifest.sha256,
                      "rows": len(manifest.entries), "preparation": manifest.preparation}, sort_keys=True))


if __name__ == "__main__":
    main()
