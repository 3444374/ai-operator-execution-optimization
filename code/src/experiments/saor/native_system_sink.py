"""Unified PostgreSQL completion sink adapter for DB-E2E matched cells."""

from __future__ import annotations

import csv
import hashlib
import json
import time
from pathlib import Path


def collect_completion_rows(output_dir: Path) -> list[tuple[int, str]]:
    """Collect exactly one completed output per doc_id from executor traces."""

    paths = sorted((output_dir / "jobs").glob("**/*.requests.csv"))
    if not paths:
        raise RuntimeError("completion sink cannot find request traces")
    by_doc: dict[int, str] = {}
    for path in paths:
        with path.open(encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                if row.get("status") != "completed":
                    raise RuntimeError("completion sink observed a failed request")
                doc_id = int(row["doc_id"])
                if doc_id in by_doc:
                    raise RuntimeError("completion sink observed duplicate doc_id")
                by_doc[doc_id] = str(row.get("output_text", "") or "")
    if not by_doc:
        raise RuntimeError("completion sink collected no rows")
    return sorted(by_doc.items())


def materialize_and_verify_postgres_sink(
    database_url: str,
    workload_name: str,
    rows: list[tuple[int, str]],
    *,
    write: bool,
) -> dict[str, object]:
    """Write native results or verify Project results, then digest readback."""

    import psycopg

    started = time.time()
    doc_ids = [doc_id for doc_id, _text in rows]
    expected_digest = _pairs_digest(rows)
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            if write:
                cursor.execute(
                    "SELECT doc_id, tenant_id, category FROM documents "
                    "WHERE workload_name = %s AND doc_id = ANY(%s)",
                    (workload_name, doc_ids),
                )
                sidecar = {
                    int(doc_id): (int(tenant_id), str(category))
                    for doc_id, tenant_id, category in cursor.fetchall()
                }
                if set(sidecar) != set(doc_ids):
                    raise RuntimeError("completion sink source sidecar is incomplete")
                cursor.execute(
                    "DELETE FROM document_completions WHERE doc_id = ANY(%s)",
                    (doc_ids,),
                )
                cursor.executemany(
                    "INSERT INTO document_completions "
                    "(doc_id, tenant_id, category, completion_text, completion_json) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    [
                        (
                            doc_id,
                            sidecar[doc_id][0],
                            sidecar[doc_id][1],
                            text,
                            json.dumps({"text": text}, ensure_ascii=False, separators=(",", ":")),
                        )
                        for doc_id, text in rows
                    ],
                )
                connection.commit()
            cursor.execute(
                "SELECT doc_id, completion_text FROM document_completions "
                "WHERE doc_id = ANY(%s) ORDER BY doc_id",
                (doc_ids,),
            )
            observed = [(int(doc_id), str(text)) for doc_id, text in cursor.fetchall()]
    observed_digest = _pairs_digest(observed)
    matched = len(observed) == len(rows) and observed_digest == expected_digest
    if not matched:
        raise RuntimeError("PostgreSQL completion sink readback mismatch")
    return {
        "status": "passed",
        "mode": "json_text",
        "table": "document_completions",
        "written_by": "matrix_adapter" if write else "project_profiler",
        "expected_rows": len(rows),
        "observed_rows": len(observed),
        "expected_digest": expected_digest,
        "observed_digest": observed_digest,
        "exactly_once": True,
        "sink_wall_s": time.time() - started,
        "verified_epoch_s": time.time(),
    }


def _pairs_digest(rows: list[tuple[int, str]]) -> str:
    return hashlib.sha256(json.dumps(
        sorted(rows), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")).hexdigest()
