"""Atomic publication primitives for matched-system summary generations."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from src.baselines.common.redact import redact_text


RANKING_OUTPUT_NAMES = (
    "system_summary.csv",
    "job_summary.csv",
    "resource_summary.csv",
)


def publish_failed_generation(
    output_dir: Path,
    audit_rows: list[dict[str, object]],
    errors: list[str],
    validation_payload: dict[str, object],
) -> None:
    """Keep all recorded cells, but delete every rankable output on failure."""

    if not audit_rows:
        raise ValueError("failed generation requires at least one audit row")
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in RANKING_OUTPUT_NAMES:
        path = output_dir / name
        if path.is_file():
            path.unlink()
    audit_path = output_dir / ".all_runs.csv.failed.tmp"
    with audit_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(audit_rows[0]))
        writer.writeheader()
        writer.writerows(audit_rows)
    audit_path.replace(output_dir / "all_runs.csv")
    payload = {
        **validation_payload,
        "status": "failed",
        "errors": [redact_text(str(error)) for error in errors],
    }
    validation_path = output_dir / ".validation.json.failed.tmp"
    validation_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    validation_path.replace(output_dir / "validation.json")
