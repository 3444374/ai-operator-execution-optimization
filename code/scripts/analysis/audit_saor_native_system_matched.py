"""Write a read-only SAOR matched-system readiness report; never executes a workload."""

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

from src.experiments.saor.native_system_readiness import (  # noqa: E402
    audit_readiness,
)


def _args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--native-config", type=Path, required=True)
    parser.add_argument("--project-config", type=Path, required=True)
    parser.add_argument("--installed-source-audit", type=Path)
    parser.add_argument("--live-service", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Audit configs statically or opt into full read-only live readiness."""

    args = _args(argv)
    try:
        result = audit_readiness(
            args.config,
            args.native_config,
            args.project_config,
            live_service=args.live_service,
            installed_source_audit=args.installed_source_audit,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        result = {"schema_version": 1, "status": "failed", "errors": [str(exc)]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 0 if result["status"] in {"passed", "static_config_passed"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
