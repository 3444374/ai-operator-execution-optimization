#!/usr/bin/env python3
"""Write exact-hash installed-source evidence for the frozen vLLM service."""

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

from src.experiments.saor.vllm_0251_source_audit import (  # noqa: E402
    audit_installed_vllm_0251,
    load_expected_service_identity,
)
from src.baselines.common.redact import redact_text  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    output = parser.add_mutually_exclusive_group(required=True)
    output.add_argument("--output", type=Path)
    output.add_argument("--stdout", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        expected = load_expected_service_identity(args.config)
        result = audit_installed_vllm_0251(expected)
    except (OSError, ValueError) as exc:
        result = {
            "schema_version": 1,
            "status": "failed",
            "errors": [redact_text(str(exc))],
        }
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.stdout:
        sys.stdout.write(encoded)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    return 0 if result["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
