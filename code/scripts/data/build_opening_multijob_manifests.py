#!/usr/bin/env python3
"""Build disjoint endpoint-balanced short and matched-long job manifests."""

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
    long_job_count: int,
) -> tuple[list[dict[str, object]], list[list[dict[str, object]]]]:
    if rows_per_job <= 0 or endpoint_count <= 0 or rows_per_job % endpoint_count:
        raise ValueError("rows_per_job must be positive and divisible by endpoint_count")
    if long_job_count <= 0:
        raise ValueError("long_job_count must be positive")
    quota = rows_per_job // endpoint_count
    short: list[dict[str, object]] = []
    long_jobs: list[list[dict[str, object]]] = [[] for _ in range(long_job_count)]
    for endpoint in range(endpoint_count):
        bucket = [row for row in rows if int(row["endpoint_index"]) == endpoint]
        required = (1 + long_job_count) * quota
        if len(bucket) < required:
            raise ValueError(
                f"endpoint {endpoint} lacks rows for one short and "
                f"{long_job_count} disjoint long jobs: need {required}, got {len(bucket)}"
            )
        ordered = sorted(bucket, key=lambda row: (int(row["prompt_tokens"]), int(row["doc_id"])))
        short.extend(ordered[:quota])
        short_ids = {int(row["doc_id"]) for row in ordered[:quota]}
        remaining = [
            row for row in reversed(ordered) if int(row["doc_id"]) not in short_ids
        ][: long_job_count * quota]
        assignments: list[list[dict[str, object]]] = [
            [] for _ in range(long_job_count)
        ]
        assigned_work = [0] * long_job_count
        for row in remaining:
            candidates = [
                index for index, assigned in enumerate(assignments)
                if len(assigned) < quota
            ]
            target = min(candidates, key=lambda index: (assigned_work[index], index))
            assignments[target].append(row)
            assigned_work[target] += int(row["prompt_tokens"])
        for index, assigned in enumerate(assignments):
            long_jobs[index].extend(assigned)
    short.sort(key=lambda row: (float(row.get("arrival_time_s", 0.0)), int(row["doc_id"])))
    for job in long_jobs:
        job.sort(key=lambda row: (float(row.get("arrival_time_s", 0.0)), int(row["doc_id"])))
    id_sets = [{int(row["doc_id"]) for row in job} for job in [short, *long_jobs]]
    if sum(len(ids) for ids in id_sets) != len(set().union(*id_sets)):
        raise RuntimeError("short/long manifests overlap")
    return short, long_jobs


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
    parser.add_argument(
        "--long-output",
        type=Path,
        help="Legacy single long output; mutually exclusive with --long-outputs",
    )
    parser.add_argument(
        "--long-outputs",
        nargs="+",
        type=Path,
        help="One output path per disjoint matched long job",
    )
    parser.add_argument("--audit-output", required=True, type=Path)
    parser.add_argument("--rows-per-job", type=int, default=512)
    parser.add_argument("--endpoint-count", type=int, default=2)
    args = parser.parse_args()

    rows = _load(args.source)
    if (args.long_output is None) == (args.long_outputs is None):
        parser.error("provide exactly one of --long-output or --long-outputs")
    long_outputs = [args.long_output] if args.long_output is not None else args.long_outputs
    short, long_jobs = _select(
        rows,
        rows_per_job=args.rows_per_job,
        endpoint_count=args.endpoint_count,
        long_job_count=len(long_outputs),
    )
    short_payload = _encode(short)
    long_payloads = [_encode(job) for job in long_jobs]
    for path in (args.short_output, *long_outputs, args.audit_output):
        path.parent.mkdir(parents=True, exist_ok=True)
    args.short_output.write_bytes(short_payload)
    for path, payload in zip(long_outputs, long_payloads):
        path.write_bytes(payload)
    long_summaries = [
        {"job_id": f"long{index}", **_summary(job, payload)}
        for index, (job, payload) in enumerate(zip(long_jobs, long_payloads), start=1)
    ]
    long_token_totals = [int(item["prompt_tokens_total"]) for item in long_summaries]
    audit = {
        "schema_version": 1,
        "status": "ready",
        "source": str(args.source.resolve()),
        "source_sha256": hashlib.sha256(args.source.read_bytes()).hexdigest(),
        "selection": (
            "per-endpoint shortest short job; remaining highest-work rows greedily "
            "balanced across disjoint long jobs"
        ),
        "short": _summary(short, short_payload),
        "long_jobs": long_summaries,
        "long_job_prompt_token_skew": (
            (max(long_token_totals) - min(long_token_totals)) / max(long_token_totals)
        ),
        "doc_id_overlap": 0,
    }
    if len(long_summaries) == 1:
        audit["long"] = {
            key: value for key, value in long_summaries[0].items() if key != "job_id"
        }
    args.audit_output.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
