#!/usr/bin/env python3
"""Run one validated project-derived phase-change experiment matrix."""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.experiments.run_ai_operator_scenarios import wait_for_idle  # noqa: E402
from src.experiments.phase_change import load_contract, runner_environment  # noqa: E402
from src.experiments.shared_vllm import RunnerOptions, run_experiment  # noqa: E402


def _args() -> tuple[RunnerOptions, Path]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract-dir", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--profiler", required=True, type=Path)
    parser.add_argument("--python-executable", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--health-url", required=True)
    parser.add_argument("--metrics-urls", required=True)
    parser.add_argument("--ray-address", required=True)
    parser.add_argument("--idle-timeout-s", type=float, default=60.0)
    parser.add_argument("--start-delay-s", type=float, default=15.0)
    parser.add_argument("--max-start-lateness-s", type=float, default=2.0)
    parser.add_argument("--max-start-skew-s", type=float, default=0.5)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--recover-stale-lease", action="store_true")
    args = parser.parse_args()
    metrics_urls = tuple(
        item.strip() for item in args.metrics_urls.split(",") if item.strip()
    )
    timing = (
        args.idle_timeout_s,
        args.start_delay_s,
        args.max_start_lateness_s,
        args.max_start_skew_s,
    )
    if not metrics_urls or any(not math.isfinite(value) or value < 0 for value in timing):
        parser.error("metrics URLs and timing bounds must be valid")
    if args.idle_timeout_s <= 0 or args.start_delay_s <= 0:
        parser.error("idle timeout and start delay must be positive")
    if args.recover_stale_lease and not args.resume:
        parser.error("--recover-stale-lease requires --resume")
    return (
        RunnerOptions(
            config_path=args.config.resolve(),
            profiler_path=args.profiler.resolve(),
            python_executable=args.python_executable.resolve(),
            output_dir=args.output_dir.resolve(),
            health_url=args.health_url,
            metrics_urls=metrics_urls,
            ray_address=args.ray_address,
            idle_timeout_s=args.idle_timeout_s,
            start_delay_s=args.start_delay_s,
            max_start_lateness_s=args.max_start_lateness_s,
            max_start_skew_s=args.max_start_skew_s,
            resume=args.resume,
            recover_stale_lease=args.recover_stale_lease,
        ),
        args.contract_dir.resolve(),
    )


def main() -> int:
    options, contract_dir = _args()
    audit = load_contract(contract_dir)
    os.environ.update(runner_environment(audit, contract_dir))
    return run_experiment(options, idle_gate=wait_for_idle)


if __name__ == "__main__":
    raise SystemExit(main())
