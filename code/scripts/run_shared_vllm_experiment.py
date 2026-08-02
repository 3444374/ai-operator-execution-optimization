#!/usr/bin/env python3
"""Run preregistered shared-vLLM multi-job scenario groups."""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.run_ai_operator_scenarios import wait_for_idle  # noqa: E402
from src.experiments.shared_vllm.core import (  # noqa: E402
    RunnerOptions,
    run_experiment,
)


def parse_args(argv: list[str] | None = None) -> RunnerOptions:
    parser = argparse.ArgumentParser(
        description="Run concurrent jobs against shared vLLM endpoints."
    )
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
    args = parser.parse_args(argv)
    metrics_urls = tuple(
        item.strip()
        for item in args.metrics_urls.split(",")
        if item.strip()
    )
    if not metrics_urls:
        parser.error("--metrics-urls must contain at least one URL")
    if (
        not math.isfinite(args.idle_timeout_s)
        or args.idle_timeout_s <= 0
        or not math.isfinite(args.start_delay_s)
        or args.start_delay_s <= 0
        or not math.isfinite(args.max_start_lateness_s)
        or args.max_start_lateness_s < 0
        or not math.isfinite(args.max_start_skew_s)
        or args.max_start_skew_s < 0
    ):
        parser.error("runner timing bounds must be finite and valid")
    if args.recover_stale_lease and not args.resume:
        parser.error("--recover-stale-lease requires --resume")
    return RunnerOptions(
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
    )


def main() -> None:
    raise SystemExit(
        run_experiment(
            parse_args(),
            idle_gate=wait_for_idle,
        )
    )


if __name__ == "__main__":
    main()
