#!/usr/bin/env python3
"""Backfill scale-aware image metrics into an existing run CSV copy.

The command never edits raw experiment data in place.  It derives only values
that are algebraically available from the existing timing and resource totals,
so historical schema-v11 runs do not need to be repeated just to obtain these
fields.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.image.metrics import (  # noqa: E402
    IMAGE_METRIC_DEFINITIONS,
    image_run_derived_metrics,
)


DERIVED_FIELDS = (
    "image_derived_metrics_status",
    "post_first_output_s",
    "first_output_fraction_of_e2e",
    "post_first_output_fraction_of_e2e",
    "first_output_cross_scale_semantics",
    "steady_state_min_s",
    "steady_state_duration_gate_met",
    "throughput_cross_scale_semantics",
    "joules_per_1k_images",
    "gpu_seconds_per_image",
    "images_per_cpu_core_second",
    "host_disk_read_bytes_per_image",
    "host_disk_write_bytes_per_image",
    "host_net_recv_bytes_per_image",
    "host_net_sent_bytes_per_image",
)


def _required_number(row: dict[str, str], field: str, cast=float):
    value = row.get(field, "")
    if value in ("", None):
        raise ValueError(f"missing required field {field}")
    return cast(value)


def augment_row(row: dict[str, str]) -> dict[str, object]:
    """Return one copied CSV row with deterministic derived image metrics."""
    augmented: dict[str, object] = dict(row)
    try:
        metrics = image_run_derived_metrics(
            rows=_required_number(row, "rows", int),
            operator_e2e_s=_required_number(row, "operator_e2e_s"),
            first_output_s=_required_number(row, "first_output_s"),
            cpu_core_seconds=_required_number(row, "cpu_core_seconds_estimate"),
            gpu_seconds=_required_number(row, "gpu_seconds"),
            gpu_energy_j=_required_number(row, "gpu_energy_estimate_j"),
            host_disk_read_bytes=_required_number(row, "host_disk_read_bytes", int),
            host_disk_write_bytes=_required_number(row, "host_disk_write_bytes", int),
            host_net_recv_bytes=_required_number(row, "host_net_recv_bytes", int),
            host_net_sent_bytes=_required_number(row, "host_net_sent_bytes", int),
        )
    except (TypeError, ValueError) as error:
        metrics = {field: "" for field in DERIVED_FIELDS}
        metrics["image_derived_metrics_status"] = f"unavailable:{error}"
    augmented.update(metrics)
    return augmented


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    definitions_path = args.output_csv.with_suffix(args.output_csv.suffix + ".metrics.json")
    if args.output_csv.exists():
        raise FileExistsError(f"output already exists: {args.output_csv}")
    if definitions_path.exists():
        raise FileExistsError(f"metric definitions already exist: {definitions_path}")
    with args.input_csv.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("input CSV has no header")
        rows = [augment_row(row) for row in reader]
        fieldnames = list(reader.fieldnames)
    fieldnames.extend(field for field in DERIVED_FIELDS if field not in fieldnames)
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)
    definitions_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "csv": args.output_csv.name,
                "metric_definitions": IMAGE_METRIC_DEFINITIONS,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
