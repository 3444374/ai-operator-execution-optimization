#!/usr/bin/env python3
"""Validate one completed five-arm rehearsal and seal its formal input identity."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.redact import redact_text  # noqa: E402
from src.experiments.saor.native_system_matched import (  # noqa: E402
    build_rehearsal_validation_payload,
    load_matched_system_config,
)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--repository-commit", required=True)
    parser.add_argument("--rehearsal-root", required=True, type=Path)
    parser.add_argument("--rehearsal-archive", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = _args()
    try:
        if args.output.exists():
            raise FileExistsError("rehearsal validation output already exists")
        config = load_matched_system_config(
            args.config, allow_existing_matrix_output_root=True
        )
        payload = build_rehearsal_validation_payload(
            args.config,
            config,
            args.repository_commit,
            args.rehearsal_root,
            args.rehearsal_archive,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except (OSError, RuntimeError, ValueError) as exc:
        print(json.dumps({
            "status": "failed",
            "error": redact_text(f"{type(exc).__name__}: {exc}"),
        }))
        return 2
    print(json.dumps({"status": "passed", "output": str(args.output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
