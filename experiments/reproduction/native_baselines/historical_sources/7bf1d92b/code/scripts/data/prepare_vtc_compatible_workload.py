#!/usr/bin/env python3
"""Prepare PostgreSQL rows and manifests for VTC-compatible multi-job runs."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from dataclasses import asdict
from pathlib import Path

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.manifests import write_manifest  # noqa: E402
from src.experiments.vtc_compatible import (  # noqa: E402
    OFFICIAL_VTC_ARTIFACT_COMMIT,
    VtcSourceRow,
    build_suite,
    suite_spec,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument(
        "--source-workload",
        default="squad_v11_dev_short_answer",
    )
    parser.add_argument("--target-workload", required=True)
    parser.add_argument("--suite", choices=("on_off_overload", "overload_multi"), required=True)
    parser.add_argument("--duration-s", type=float)
    parser.add_argument("--doc-id-base", type=int, required=True)
    parser.add_argument("--endpoint-count", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260810)
    parser.add_argument("--max-prompt-token-distance", type=int, default=64)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Insert the prepared rows into PostgreSQL",
    )
    return parser.parse_args()


def _load_source(connection, workload: str, target: int, distance: int) -> tuple[VtcSourceRow, ...]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT doc_id, tenant_id, category, text, prompt_tokens,
                   COALESCE(session_id, ''), COALESCE(prefix_key, '')
            FROM documents
            WHERE workload_name = %s
              AND prompt_tokens BETWEEN %s AND %s
            ORDER BY ABS(prompt_tokens - %s), doc_id
            """,
            (workload, target - distance, target + distance, target),
        )
        return tuple(VtcSourceRow(*row) for row in cursor.fetchall())


def _insert_rows(connection, target_workload: str, jobs) -> int:
    rows = [row for job in jobs for row in job]
    doc_ids = [row.doc_id for row in rows]
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM documents WHERE workload_name = %s",
            (target_workload,),
        )
        if int(cursor.fetchone()[0]) != 0:
            raise ValueError("target workload already exists; choose a new immutable name")
        cursor.execute("SELECT doc_id FROM documents WHERE doc_id = ANY(%s) LIMIT 1", (doc_ids,))
        if cursor.fetchone() is not None:
            raise ValueError("prepared doc_id range collides with existing documents")
        cursor.executemany(
            """
            INSERT INTO documents (
                doc_id, tenant_id, category, text, workload_name, prompt_tokens,
                target_output_tokens, arrival_time_s, session_id, prefix_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    row.doc_id, row.tenant_id, row.category, row.text,
                    row.workload_name, row.prompt_tokens, row.target_output_tokens,
                    row.arrival_time_s, row.session_id, row.prefix_key,
                )
                for row in rows
            ],
        )
    connection.commit()
    return len(rows)


def main() -> int:
    args = _parse_args()
    if args.max_prompt_token_distance < 0:
        raise ValueError("max prompt-token distance must be non-negative")
    destination = args.output_dir.resolve()
    expected_paths = [destination / "audit.json", destination / "workload.jsonl"]
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("output directory must be absent or empty")
    spec = suite_spec(args.suite, duration_s=args.duration_s)
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg is required for workload preparation") from exc
    with psycopg.connect(args.database_url) as connection:
        source_rows = _load_source(
            connection,
            args.source_workload,
            spec.input_tokens,
            args.max_prompt_token_distance,
        )
        jobs, manifests = build_suite(
            source_rows,
            spec=spec,
            workload_name=args.target_workload,
            doc_id_base=args.doc_id_base,
            seed=args.seed,
            endpoint_count=args.endpoint_count,
        )
        destination.mkdir(parents=True, exist_ok=True)
        workload_payload = "".join(
            json.dumps(
                asdict(row),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for job in jobs for row in job
        )
        expected_paths[1].write_text(workload_payload, encoding="utf-8")
        manifest_metadata = []
        for index, manifest in enumerate(manifests):
            path = destination / f"client_{index}.jsonl"
            metadata = write_manifest(path, manifest)
            manifest_metadata.append({"client_index": index, "path": str(path), **asdict(metadata)})
        prompt_tokens = [row.prompt_tokens for job in jobs for row in job]
        audit = {
            "schema_version": 1,
            "status": "prepared",
            "label": "VTC-compatible upstream evaluation; not official VTC reproduction",
            "official_artifact": "Ying1123/VTC-artifact",
            "official_artifact_commit": OFFICIAL_VTC_ARTIFACT_COMMIT,
            "source_workload": args.source_workload,
            "target_workload": args.target_workload,
            "suite": asdict(spec),
            "seed": args.seed,
            "poisson_arrivals": True,
            "job_row_counts": [len(job) for job in jobs],
            "job_first_arrival_s": [job[0].arrival_time_s for job in jobs],
            "total_rows": sum(len(job) for job in jobs),
            "prompt_tokens_min": min(prompt_tokens),
            "prompt_tokens_median": statistics.median(prompt_tokens),
            "prompt_tokens_max": max(prompt_tokens),
            "doc_id_overlap": 0,
            "workload_sha256": hashlib.sha256(workload_payload.encode()).hexdigest(),
            "manifests": manifest_metadata,
        }
        expected_paths[0].write_text(
            json.dumps(audit, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        if args.apply:
            inserted = _insert_rows(connection, args.target_workload, jobs)
            receipt = {"schema_version": 1, "status": "imported", "inserted_rows": inserted}
            (destination / "database_import_receipt.json").write_text(
                json.dumps(receipt, indent=2) + "\n", encoding="utf-8"
            )
    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
