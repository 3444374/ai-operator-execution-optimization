#!/usr/bin/env python3
"""Parse CLI options and invoke the five-arm matched-system executor."""

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

from src.baselines.common.database_identity import (  # noqa: E402
    consistent_database_versions as _consistent_database_versions,
)
from src.baselines.common.redact import redact_text  # noqa: E402
from src.experiments.saor.native_system_execution import (  # noqa: E402
    MatchedExecutionOptions,
    execute_matched_system,
    normalize_native_evidence as _normalize_native,
    normalize_project_evidence as _normalize_project,
)

CliOptions = MatchedExecutionOptions


def parse_args(argv: list[str] | None = None) -> CliOptions:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--native-config", required=True, type=Path)
    parser.add_argument("--project-config", required=True, type=Path)
    parser.add_argument("--native-runner", required=True, type=Path)
    parser.add_argument("--profiler", required=True, type=Path)
    parser.add_argument(
        "--driver-python", "--python-executable", dest="python_executable",
        required=True, type=Path,
    )
    parser.add_argument("--health-url", required=True)
    parser.add_argument("--metrics-urls", required=True)
    parser.add_argument("--ray-address", required=True)
    parser.add_argument("--idle-timeout-s", type=float, default=60.0)
    parser.add_argument("--start-delay-s", type=float, default=15.0)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--correctness-smoke", action="store_true")
    mode.add_argument("--rehearsal", action="store_true")
    parser.add_argument("--vllm-python", required=True, type=Path)
    parser.add_argument(
        "--vllm-runtime-identity", action="append", required=True, type=Path
    )
    parser.add_argument("--installed-source-audit", required=True, type=Path)
    parser.add_argument("--system-preflight-evidence", required=True, type=Path)
    parser.add_argument("--correctness-smoke-evidence", type=Path)
    parser.add_argument("--correctness-smoke-root", type=Path)
    parser.add_argument("--formal-authorization", type=Path)
    parser.add_argument("--rehearsal-validation", type=Path)
    parser.add_argument("--rehearsal-root", type=Path)
    parser.add_argument("--rehearsal-archive", type=Path)
    args = parser.parse_args(argv)
    if not args.correctness_smoke and args.correctness_smoke_evidence is None:
        parser.error("matrix execution requires --correctness-smoke-evidence")
    if args.correctness_smoke and args.correctness_smoke_root is None:
        parser.error("--correctness-smoke requires --correctness-smoke-root")
    if not args.correctness_smoke and args.correctness_smoke_root is not None:
        parser.error("--correctness-smoke-root is valid only with --correctness-smoke")
    metrics_urls = tuple(
        item.strip() for item in args.metrics_urls.split(",") if item.strip()
    )
    if not metrics_urls:
        parser.error("--metrics-urls must contain at least one URL")
    return CliOptions(
        config=args.config.resolve(),
        native_config=args.native_config.resolve(),
        project_config=args.project_config.resolve(),
        native_runner=args.native_runner.resolve(),
        profiler=args.profiler.resolve(),
        python_executable=args.python_executable.absolute(),
        health_url=args.health_url,
        metrics_urls=metrics_urls,
        ray_address=args.ray_address,
        idle_timeout_s=args.idle_timeout_s,
        start_delay_s=args.start_delay_s,
        rehearsal=args.rehearsal,
        correctness_smoke=args.correctness_smoke,
        correctness_smoke_root=(
            args.correctness_smoke_root.resolve()
            if args.correctness_smoke_root is not None else None
        ),
        vllm_python=args.vllm_python.absolute(),
        runtime_identity_paths=tuple(
            item.resolve() for item in args.vllm_runtime_identity
        ),
        installed_source_audit=args.installed_source_audit.resolve(),
        system_preflight_evidence=args.system_preflight_evidence.resolve(),
        correctness_smoke_evidence=(
            args.correctness_smoke_evidence.resolve()
            if args.correctness_smoke_evidence is not None else None
        ),
        formal_authorization=(
            args.formal_authorization.resolve()
            if args.formal_authorization is not None else None
        ),
        rehearsal_validation=(
            args.rehearsal_validation.resolve()
            if args.rehearsal_validation is not None else None
        ),
        rehearsal_root=(
            args.rehearsal_root.resolve()
            if args.rehearsal_root is not None else None
        ),
        rehearsal_archive=(
            args.rehearsal_archive.resolve()
            if args.rehearsal_archive is not None else None
        ),
    )


def run(options: CliOptions) -> dict[str, object]:
    return execute_matched_system(options)


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(parse_args(argv))
    except Exception as exc:
        print(json.dumps({
            "status": "failed",
            "error": redact_text(f"{type(exc).__name__}: {exc}"),
        }))
        return 1
    print(json.dumps({"status": result["status"], "cells": len(result["cells"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
