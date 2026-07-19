#!/usr/bin/env python3
"""Smoke test for the Daft text DataOrganizer path.

This script intentionally stays below the full PostgreSQL/Ray/vLLM pipeline.
It verifies the first Daft-specific step the project now needs:

    rows -> Arrow Table -> Daft DataFrame -> into_batches / optional repartition

The output is a CSV row with enough metrics to tell whether Daft was actually
used and whether repartition was effective in the selected runner.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.organizers import OrganizerConfig, batch_metrics, make_organizer


@dataclass(frozen=True)
class TextRow:
    doc_id: int
    tenant_id: int
    category: str
    prompt: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a minimal Daft text organizer smoke test.")
    parser.add_argument("--organizer", choices=["arrow", "daft"], default="daft")
    parser.add_argument("--rows", type=int, default=1024)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--partitions", type=int, default=0, help="0 disables repartition/into_partitions.")
    parser.add_argument(
        "--partition-mode",
        choices=["none", "into_partitions", "repartition"],
        default="none",
        help="Use into_partitions for local splitting; repartition requires Ray runner to be effective.",
    )
    parser.add_argument("--runner", choices=["native", "ray"], default="native")
    parser.add_argument("--experiment-id", default="daft_text_smoke")
    parser.add_argument("--output", default="tmp/daft_text_organizer_smoke.csv")
    return parser.parse_args()


def synthetic_rows(row_count: int) -> list[TextRow]:
    if row_count < 0:
        raise ValueError("--rows must be non-negative")
    rows = []
    for i in range(row_count):
        token_repeats = 8 + (i % 64)
        prefix = "shared_prefix_alpha" if i % 2 == 0 else "shared_prefix_beta"
        prompt = f"{prefix} document {i} tenant {i % 16} " + ("token " * token_repeats)
        rows.append(TextRow(i, i % 16, f"cat_{i % 8}", prompt.strip()))
    return rows


def rows_to_arrow(rows: list[TextRow]) -> pa.Table:
    return pa.table(
        {
            "doc_id": [row.doc_id for row in rows],
            "tenant_id": [row.tenant_id for row in rows],
            "category": [row.category for row in rows],
            "prompt": [row.prompt for row in rows],
        }
    )


def append_csv(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def run(args: argparse.Namespace) -> dict:
    rows = synthetic_rows(args.rows)

    arrow_start = time.perf_counter()
    table = rows_to_arrow(rows)
    arrow_build_s = time.perf_counter() - arrow_start

    config = OrganizerConfig(
        batch_size=args.batch_size,
        partition_mode=args.partition_mode,
        partitions=args.partitions,
        runner=args.runner,
    )
    organizer = make_organizer(args.organizer, config)
    organized = organizer.organize(table)

    return {
        "status": "ok",
        "experiment_id": args.experiment_id,
        "row_count": args.rows,
        "batch_size": args.batch_size,
        "arrow_build_s": round(arrow_build_s, 6),
        **organized.metrics,
        **batch_metrics(organized.batches),
    }


def main() -> None:
    args = parse_args()
    row = run(args)
    append_csv(Path(args.output), row)
    print(row)


if __name__ == "__main__":
    main()
