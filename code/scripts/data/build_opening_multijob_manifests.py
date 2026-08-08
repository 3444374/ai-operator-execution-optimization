#!/usr/bin/env python3
"""Build disjoint endpoint-balanced short/long manifests for opening multi-job evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from pathlib import Path


def _load(path: Path) -> list[dict[str, object]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    required = {"doc_id", "endpoint_index", "prompt_tokens", "source_row_hash"}
    if not rows or any(not required.issubset(row) for row in rows):
        raise ValueError("source manifest is empty or missing required fields")
    doc_ids = [int(row["doc_id"]) for row in rows]
    if len(set(doc_ids)) != len(doc_ids):
        raise ValueError("source manifest contains duplicate doc_id values")
    return rows


def _select(
    rows: list[dict[str, object]],
    *,
    rows_per_job: int,
    endpoint_count: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    if rows_per_job <= 0 or endpoint_count <= 0 or rows_per_job % endpoint_count:
        raise ValueError("rows_per_job must be positive and divisible by endpoint_count")
    quota = rows_per_job // endpoint_count
    short: list[dict[str, object]] = []
    long: list[dict[str, object]] = []
    for endpoint in range(endpoint_count):
        bucket = [row for row in rows if int(row["endpoint_index"]) == endpoint]
        if len(bucket) < 2 * quota:
            raise ValueError(f"endpoint {endpoint} lacks disjoint short/long rows")
        ordered = sorted(bucket, key=lambda row: (int(row["prompt_tokens"]), int(row["doc_id"])))
        short.extend(ordered[:quota])
        short_ids = {int(row["doc_id"]) for row in ordered[:quota]}
        remaining = [row for row in reversed(ordered) if int(row["doc_id"]) not in short_ids]
        long.extend(remaining[:quota])
    short.sort(key=lambda row: (float(row.get("arrival_time_s", 0.0)), int(row["doc_id"])))
    long.sort(key=lambda row: (float(row.get("arrival_time_s", 0.0)), int(row["doc_id"])))
    if {int(row["doc_id"]) for row in short} & {int(row["doc_id"]) for row in long}:
        raise RuntimeError("short and long manifests overlap")
    return short, long


def _encode(rows: list[dict[str, object]]) -> bytes:
    return ("\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) for row in rows) + "\n").encode("utf-8")


def _summary(rows: list[dict[str, object]], payload: bytes) -> dict[str, object]:
    tokens = [int(row["prompt_tokens"]) for row in rows]
    endpoint_counts: dict[str, int] = {}
    for row in rows:
        key = str(int(row["endpoint_index"]))
        endpoint_counts[key] = endpoint_counts.get(key, 0) + 1
    ordered = sorted(tokens)
    return {
        "rows": len(rows),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "endpoint_counts": endpoint_counts,
        "prompt_tokens_min": min(tokens),
        "prompt_tokens_median": statistics.median(tokens),
        "prompt_tokens_p95": ordered[max(0, int(0.95 * len(ordered)) - 1)],
        "prompt_tokens_max": max(tokens),
        "prompt_tokens_total": sum(tokens),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--short-output", required=True, type=Path)
    parser.add_argument("--long-output", required=True, type=Path)
    parser.add_argument("--audit-output", required=True, type=Path)
    parser.add_argument("--rows-per-job", type=int, default=512)
    parser.add_argument("--endpoint-count", type=int, default=2)
    args = parser.parse_args()

    rows = _load(args.source)
    short, long = _select(rows, rows_per_job=args.rows_per_job, endpoint_count=args.endpoint_count)
    short_payload = _encode(short)
    long_payload = _encode(long)
    for path in (args.short_output, args.long_output, args.audit_output):
        path.parent.mkdir(parents=True, exist_ok=True)
    args.short_output.write_bytes(short_payload)
    args.long_output.write_bytes(long_payload)
    audit = {
        "schema_version": 1,
        "status": "ready",
        "source": str(args.source.resolve()),
        "source_sha256": hashlib.sha256(args.source.read_bytes()).hexdigest(),
        "selection": "per-endpoint shortest/longest disjoint prompt-token work",
        "short": _summary(short, short_payload),
        "long": _summary(long, long_payload),
        "doc_id_overlap": 0,
    }
    args.audit_output.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
