#!/usr/bin/env python3
"""Build an audited all-at-t0 variant of an immutable request manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def build_eager_variant(source: Path, output: Path, audit_output: Path) -> dict[str, object]:
    """Set only ``arrival_time_s`` to zero and preserve request identities/work."""

    source_payload = source.read_bytes()
    rows = [
        json.loads(line)
        for line in source_payload.decode("utf-8").splitlines()
        if line.strip()
    ]
    required = {"doc_id", "endpoint_index", "prompt_tokens", "source_row_hash"}
    if not rows or any(not required.issubset(row) for row in rows):
        raise ValueError("source manifest is empty or missing required fields")
    doc_ids = [int(row["doc_id"]) for row in rows]
    if len(set(doc_ids)) != len(doc_ids):
        raise ValueError("source manifest contains duplicate doc_id values")

    eager_rows = [{**row, "arrival_time_s": 0.0} for row in rows]
    eager_rows.sort(key=lambda row: int(row["doc_id"]))
    encoded = (
        "\n".join(
            json.dumps(
                row,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            for row in eager_rows
        )
        + "\n"
    ).encode("utf-8")
    output.parent.mkdir(parents=True, exist_ok=True)
    audit_output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(encoded)

    endpoint_counts: dict[str, int] = {}
    for row in eager_rows:
        endpoint = str(int(row["endpoint_index"]))
        endpoint_counts[endpoint] = endpoint_counts.get(endpoint, 0) + 1
    audit = {
        "schema_version": 1,
        "status": "ready",
        "transformation": "arrival_time_s_only_to_zero_then_doc_id_sort",
        "source": str(source.resolve()),
        "source_sha256": _sha256(source_payload),
        "output": str(output.resolve()),
        "output_sha256": _sha256(encoded),
        "rows": len(eager_rows),
        "unique_doc_ids": len(set(doc_ids)),
        "endpoint_counts": endpoint_counts,
        "arrival_time_min_s": 0.0,
        "arrival_time_max_s": 0.0,
        "prompt_tokens_total": sum(int(row["prompt_tokens"]) for row in eager_rows),
    }
    audit_output.write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--audit-output", required=True, type=Path)
    args = parser.parse_args()
    audit = build_eager_variant(args.source, args.output, args.audit_output)
    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
