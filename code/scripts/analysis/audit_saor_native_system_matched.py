"""Write a read-only SAOR matched-system readiness report; never executes a workload."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.experiments.saor.native_system_matched import (
    audit_matched_system_config,
    load_matched_system_config,
)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    """Audit only local config/manifest evidence and return fail-closed status."""

    args = _args()
    try:
        result = audit_matched_system_config(load_matched_system_config(args.config))
    except (OSError, ValueError) as exc:
        result = {"schema_version": 1, "status": "failed", "errors": [str(exc)]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 0 if result["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
