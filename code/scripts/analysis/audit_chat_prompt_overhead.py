#!/usr/bin/env python3
"""Audit per-request chat-template prompt overhead from raw request evidence.

The audit joins completed request and submission records by submission ID and
recomputes ``service total - raw prompt - actual output``.  It passes only when
the evidence is one-request-per-submission, complete, non-negative, uniform,
and equal to the optional pre-registered value and request count.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_rows(paths: Iterable[Path]) -> Iterable[tuple[Path, dict[str, str]]]:
    for path in paths:
        with path.open(newline="", encoding="utf-8") as source:
            yield from ((path, row) for row in csv.DictReader(source))


def audit_prompt_overhead(
    matrix_root: Path,
    *,
    expected_overhead: int | None = None,
    expected_requests: int | None = None,
) -> dict[str, object]:
    """Return a fail-closed audit of service-side prompt-token overhead."""

    root = matrix_root.resolve()
    request_paths = sorted((root / "jobs").glob("*.requests.csv"))
    submission_paths = sorted((root / "jobs").glob("*.submissions.csv"))
    errors: list[str] = []
    if not request_paths:
        errors.append("no request evidence found under jobs/")
    if not submission_paths:
        errors.append("no submission evidence found under jobs/")

    requests: dict[str, list[tuple[Path, dict[str, str]]]] = defaultdict(list)
    submissions: dict[str, list[tuple[Path, dict[str, str]]]] = defaultdict(list)
    for path, row in _read_rows(request_paths):
        submission_id = row.get("submission_id", "").strip()
        if not submission_id:
            errors.append(f"request row without submission_id in {path.name}")
            continue
        requests[submission_id].append((path, row))
    for path, row in _read_rows(submission_paths):
        submission_id = row.get("submission_id", "").strip()
        if not submission_id:
            errors.append(f"submission row without submission_id in {path.name}")
            continue
        submissions[submission_id].append((path, row))

    request_ids = set(requests)
    submission_ids = set(submissions)
    missing_submissions = sorted(request_ids - submission_ids)
    missing_requests = sorted(submission_ids - request_ids)
    if missing_submissions:
        errors.append(
            f"{len(missing_submissions)} request IDs lack submission evidence"
        )
    if missing_requests:
        errors.append(
            f"{len(missing_requests)} submission IDs lack request evidence"
        )

    overheads: list[int] = []
    scenario_counts: Counter[str] = Counter()
    for submission_id in sorted(request_ids & submission_ids):
        request_group = requests[submission_id]
        submission_group = submissions[submission_id]
        if len(request_group) != 1 or len(submission_group) != 1:
            errors.append(
                f"{submission_id}: expected one request and one submission, got "
                f"{len(request_group)} and {len(submission_group)}"
            )
            continue
        request_path, request = request_group[0]
        submission_path, submission = submission_group[0]
        if request.get("status") != "completed":
            errors.append(f"{submission_id}: request status is not completed")
            continue
        if submission.get("status") != "completed":
            errors.append(f"{submission_id}: submission status is not completed")
            continue
        try:
            rows = int(submission["rows"])
            service_total = int(submission["token_count"])
            raw_prompt = int(request["prompt_tokens"])
            actual_output = int(request["actual_output_tokens"])
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(
                f"{submission_id}: invalid numeric evidence in "
                f"{request_path.name}/{submission_path.name}: {exc}"
            )
            continue
        if rows != 1:
            errors.append(f"{submission_id}: submission rows={rows}, expected 1")
            continue
        overhead = service_total - raw_prompt - actual_output
        if overhead < 0:
            errors.append(f"{submission_id}: negative prompt overhead {overhead}")
        overheads.append(overhead)
        scenario_counts[request.get("scenario_id", "")] += 1

    distribution = Counter(overheads)
    if overheads and len(distribution) != 1:
        errors.append("prompt overhead is not uniform across completed requests")
    observed = overheads[0] if overheads and len(distribution) == 1 else None
    if expected_overhead is not None and observed != expected_overhead:
        errors.append(
            f"uniform overhead {observed!r} does not match expected "
            f"{expected_overhead}"
        )
    if expected_requests is not None and len(overheads) != expected_requests:
        errors.append(
            f"audited request count {len(overheads)} does not match expected "
            f"{expected_requests}"
        )

    input_paths = request_paths + submission_paths
    return {
        "schema_version": 1,
        "status": "passed" if overheads and not errors else "failed",
        "matrix_root": str(root),
        "calibration_method": (
            "submission.token_count_minus_request.raw_prompt_tokens_minus_"
            "request.actual_output_tokens"
        ),
        "request_granularity_required": True,
        "audited_requests": len(overheads),
        "expected_requests": expected_requests,
        "expected_overhead_tokens_per_request": expected_overhead,
        "observed_overhead_tokens_per_request": observed,
        "observed_min_tokens": min(overheads) if overheads else None,
        "observed_max_tokens": max(overheads) if overheads else None,
        "overhead_distribution": {
            str(value): count for value, count in sorted(distribution.items())
        },
        "scenario_request_counts": dict(sorted(scenario_counts.items())),
        "missing_submission_count": len(missing_submissions),
        "missing_request_count": len(missing_requests),
        "input_files": [
            {
                "path": str(path.relative_to(root)),
                "sha256": _sha256(path),
            }
            for path in input_paths
        ],
        "errors": errors,
    }


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--expected-overhead", type=int)
    parser.add_argument("--expected-requests", type=int)
    return parser.parse_args()


def main() -> int:
    args = _args()
    for value, name in (
        (args.expected_overhead, "expected overhead"),
        (args.expected_requests, "expected requests"),
    ):
        if value is not None and value < 0:
            raise SystemExit(f"{name} must be non-negative")
    result = audit_prompt_overhead(
        args.matrix_root,
        expected_overhead=args.expected_overhead,
        expected_requests=args.expected_requests,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
