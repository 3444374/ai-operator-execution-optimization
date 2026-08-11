#!/usr/bin/env python3
"""Replay paired aggregate traces; output is not online performance evidence."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from src.experiments.saor.trace_replay import (
    load_paired_capacity_replay_config,
    replay_paired_capacity_trace,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary-output", type=Path, required=True)
    args = parser.parse_args()
    with args.input.open(encoding="utf-8", newline="") as handle:
        source_rows = list(csv.DictReader(handle))
    config = load_paired_capacity_replay_config(args.config)
    rows = replay_paired_capacity_trace(source_rows, config)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].to_dict()))
        writer.writeheader()
        writer.writerows(row.to_dict() for row in rows)
    summary = {
        "schema_version": 1,
        "claim_scope": "paired_aggregate_trace_noncausal",
        "sample_count": len(rows),
        "heldout_sample_count": sum(row.regret_eligible for row in rows),
        "switch_count": sum(row.switched for row in rows),
        "oracle_match_count": sum(
            row.regret_eligible and row.selected_arm == row.oracle_arm
            for row in rows
        ),
        "cumulative_regret": sum(
            row.regret for row in rows if row.regret_eligible
        ),
    }
    args.summary_output.parent.mkdir(parents=True, exist_ok=True)
    args.summary_output.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
