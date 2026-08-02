"""Fail-closed CSV schema preflight and append helpers."""

from __future__ import annotations

import csv
from pathlib import Path


def preflight_metrics_schema(
    path: Path,
    fieldnames,
    *,
    allow_additional_fields: bool = False,
) -> None:
    expected = list(fieldnames)
    has_content = path.exists() and path.stat().st_size > 0
    if has_content:
        with path.open(newline="", encoding="utf-8") as existing:
            header = next(csv.reader(existing), [])
        if allow_additional_fields:
            expected_set = set(expected)
            matches = [
                field for field in header if field in expected_set
            ] == expected
        else:
            matches = header == expected
        if not matches:
            raise ValueError(
                "CSV schema mismatch: "
                f"existing header {header!r} != row keys {expected!r}"
            )

def append_metrics(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(row.keys())
    preflight_metrics_schema(path, fieldnames)
    has_content = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not has_content:
            writer.writeheader()
        writer.writerow(row)
