#!/usr/bin/env python3
"""Validate or run the native Daft/Ray Data image multi-job contract."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.image_multijob.native import (  # noqa: E402
    load_native_image_multijob_config,
    run_native_image_multijob,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate-config", "gate", "run"))
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = load_native_image_multijob_config(args.config)
    if args.command == "validate-config":
        print(f"validated {len(config.arms)} native image arms")
        return 0
    return run_native_image_multijob(config, gate_only=args.command == "gate")


if __name__ == "__main__":
    raise SystemExit(main())
