#!/usr/bin/env python3
"""Write CPU-only control-path overhead; never infer GPU performance."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from src.experiments.saor.control_benchmark import run_control_benchmark


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-counts", required=True)
    parser.add_argument("--iterations", type=int, required=True)
    parser.add_argument("--warmup-iterations", type=int, required=True)
    parser.add_argument("--repeats", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    job_counts = tuple(int(value) for value in args.job_counts.split(","))
    rows = run_control_benchmark(
        job_counts=job_counts,
        iterations=args.iterations,
        warmup_iterations=args.warmup_iterations,
        repeats=args.repeats,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].to_dict()))
        writer.writeheader()
        writer.writerows(row.to_dict() for row in rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
