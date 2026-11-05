#!/usr/bin/env python3
"""Run read-only SAOR system probes and seal evidence for correctness smoke."""

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
from src.experiments.saor.native_system_preflight import (  # noqa: E402
    build_system_preflight_payload,
)
from src.experiments.saor.native_system_readiness import (  # noqa: E402
    audit_readiness,
    load_and_validate_static_readiness,
)


def _args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the three-config scope and live preflight inputs."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--native-config", required=True, type=Path)
    parser.add_argument("--project-config", required=True, type=Path)
    parser.add_argument("--vllm-runtime-identity", action="append", required=True, type=Path)
    parser.add_argument("--ray-address", required=True)
    parser.add_argument("--bounded-baseline-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Probe live services once and write a fresh, non-overwriting JSON seal."""

    args = _args(argv)
    try:
        if args.output.exists():
            raise FileExistsError("system preflight output already exists")
        matched, _identity = load_and_validate_static_readiness(
            args.config, args.native_config, args.project_config
        )
        static = audit_readiness(args.config, args.native_config, args.project_config)
        runtime_paths = tuple(args.vllm_runtime_identity)
        not_before = max(path.stat().st_mtime_ns for path in runtime_paths)
        runtime_records = [
            json.loads(path.read_text(encoding="utf-8")) for path in runtime_paths
        ]
        if any(not isinstance(record, dict) for record in runtime_records):
            raise RuntimeError("vLLM runtime sidecar must be a JSON object")
        allowed_pids = tuple(int(record["pid"]) for record in runtime_records)
        payload = build_system_preflight_payload(
            static["binding"],
            matched,
            ray_address=args.ray_address,
            bounded_baseline_root=args.bounded_baseline_root,
            not_before_mtime_ns=not_before,
            allowed_vllm_root_pids=allowed_pids,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except Exception as exc:  # CLI boundary: redact third-party PG/Ray/HTTP errors.
        print(json.dumps({
            "status": "failed",
            "error": redact_text(f"{type(exc).__name__}: {exc}"),
        }))
        return 2
    print(json.dumps({"status": "passed", "output": str(args.output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
