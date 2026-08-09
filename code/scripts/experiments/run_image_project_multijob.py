#!/usr/bin/env python3
"""Validate or run the project image single/static/shared multi-job matrix."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.image_multijob.project import (  # noqa: E402
    load_project_image_multijob_config,
    run_project_image_multijob,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate-config", "run"))
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = load_project_image_multijob_config(args.config)
    if args.command == "validate-config":
        print(f"validated {len(config.scenarios)} project image scenarios")
        return 0
    return run_project_image_multijob(config)


if __name__ == "__main__":
    raise SystemExit(main())
