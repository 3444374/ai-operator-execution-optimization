#!/usr/bin/env python3
"""Profile a database-triggered AI operator external execution path.

This is a PostgreSQL 18.3-compatible harness. It uses a job table as the
database trigger substitute so the same worker path can later be connected to
an internal UDF/table-function trigger without changing Ray/Arrow profiling.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pyarrow as pa
import ray


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS documents (
  doc_id BIGINT PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  text TEXT NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ai_operator_jobs (
  job_id BIGSERIAL PRIMARY KEY,
  operator_name TEXT NOT NULL,
  input_table TEXT NOT NULL,
  output_table TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  started_at TIMESTAMP,
  finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS document_embeddings (
  doc_id BIGINT PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  embedding_json TEXT NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


@dataclass
class StageTimer:
    name: str
    start_s: float
    elapsed_s: float = 0.0

    @classmethod
    def start(cls, name: str) -> "StageTimer":
        return cls(name=name, start_s=time.perf_counter())

    def stop(self) -> float:
        self.elapsed_s = time.perf_counter() - self.start_s
        return self.elapsed_s


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile PostgreSQL-triggered AI_EMBED external execution with Ray and Arrow."
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--setup", action="store_true", help="Create required tables before running.")
    parser.add_argument("--seed-rows", type=int, default=0, help="Insert synthetic documents if table is empty.")
    parser.add_argument("--total-rows", type=int, default=10000)
    parser.add_argument("--db-fetch-rows", type=int, default=1024)
    parser.add_argument("--ray-batch-rows", type=int, default=1024)
    parser.add_argument("--embedding-dim", type=int, default=128)
    parser.add_argument("--model-workers", type=int, default=2)
    parser.add_argument("--max-inflight", type=int, default=8)
    parser.add_argument("--strategy", choices=["fine", "coalesced"], default="coalesced")
    parser.add_argument("--output", default="validation/results/postgres_ai_operator_profile.csv")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without connecting to DB.")
    return parser.parse_args()


def require_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: psycopg. Install with `.venv/bin/python -m pip install \"psycopg[binary]\"`."
        ) from exc
    return psycopg


def connect(database_url: str):
    psycopg = require_psycopg()
    return psycopg.connect(database_url)


def setup_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def count_documents(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM documents")
        return int(cur.fetchone()[0])


def seed_documents(conn, rows: int) -> None:
    if rows <= 0 or count_documents(conn) > 0:
        return
    values = [
        (
            i,
            i % 16,
            f"cat_{i % 8}",
            f"document {i} tenant {i % 16} category {i % 8} " + ("token " * 32),
        )
        for i in range(rows)
    ]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO documents (doc_id, tenant_id, category, text) VALUES (%s, %s, %s, %s)",
            values,
        )
    conn.commit()


def create_job(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ai_operator_jobs (operator_name, input_table, output_table, status, started_at)
            VALUES ('AI_EMBED', 'documents', 'document_embeddings', 'running', CURRENT_TIMESTAMP)
            RETURNING job_id
            """
        )
        job_id = int(cur.fetchone()[0])
    conn.commit()
    return job_id


def finish_job(conn, job_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE ai_operator_jobs SET status = 'finished', finished_at = CURRENT_TIMESTAMP WHERE job_id = %s",
            (job_id,),
        )
    conn.commit()


def fetch_record_batch(conn, limit: int, offset: int) -> pa.RecordBatch | None:
    timer = StageTimer.start("db_fetch")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT doc_id, tenant_id, category, text
            FROM documents
            ORDER BY doc_id
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
    db_fetch_s = timer.stop()
    if not rows:
        return None
    columns = list(zip(*rows, strict=True))
    batch = pa.record_batch(
        [
            pa.array(columns[0], type=pa.int64()),
            pa.array(columns[1], type=pa.int32()),
            pa.array(columns[2], type=pa.string()),
            pa.array(columns[3], type=pa.string()),
        ],
        names=["doc_id", "tenant_id", "category", "text"],
    )
    batch = batch.append_column("db_fetch_s", pa.array([db_fetch_s] * batch.num_rows, type=pa.float64()))
    return batch


def split_batch(batch: pa.RecordBatch, rows_per_batch: int) -> list[pa.RecordBatch]:
    if rows_per_batch <= 0 or rows_per_batch >= batch.num_rows:
        return [batch]
    return [batch.slice(start, rows_per_batch) for start in range(0, batch.num_rows, rows_per_batch)]


@ray.remote
class FakeEmbeddingActor:
    def __init__(self, embedding_dim: int, service_tokens_per_s: float = 50000.0):
        self.embedding_dim = embedding_dim
        self.service_tokens_per_s = service_tokens_per_s

    def embed(self, batch: pa.RecordBatch) -> dict:
        service_start = time.perf_counter()
        texts = batch.column("text").to_pylist()
        token_count = sum(max(1, len(text.split())) for text in texts)
        target_s = token_count / self.service_tokens_per_s
        if target_s > 0:
            time.sleep(target_s)
        vectors = np.empty((batch.num_rows, self.embedding_dim), dtype=np.float32)
        for i, text in enumerate(texts):
            seed = hash(text) & 0xFFFFFFFF
            rng = np.random.default_rng(seed)
            vectors[i, :] = rng.random(self.embedding_dim, dtype=np.float32)
        service_s = time.perf_counter() - service_start
        return {
            "doc_id": batch.column("doc_id").to_pylist(),
            "tenant_id": batch.column("tenant_id").to_pylist(),
            "category": batch.column("category").to_pylist(),
            "embedding": vectors,
            "rows": batch.num_rows,
            "token_count": token_count,
            "service_s": service_s,
        }


def submit_with_backpressure(
    actors: list,
    batches: Iterable[pa.RecordBatch],
    max_inflight: int,
) -> tuple[list[dict], dict]:
    pending = []
    results = []
    submit_count = 0
    max_seen_inflight = 0
    queue_wait_samples = []
    fanin_s = 0.0
    submit_start_by_ref = {}

    for batch in batches:
        while len(pending) >= max_inflight:
            wait_timer = StageTimer.start("bounded_wait")
            ready, pending = ray.wait(pending, num_returns=1)
            queue_wait_samples.append(wait_timer.stop())
            fanin_timer = StageTimer.start("ray_get")
            results.extend(ray.get(ready))
            fanin_s += fanin_timer.stop()
        actor = actors[submit_count % len(actors)]
        ref = actor.embed.remote(batch)
        pending.append(ref)
        submit_start_by_ref[ref] = time.perf_counter()
        submit_count += 1
        max_seen_inflight = max(max_seen_inflight, len(pending))

    while pending:
        ready, pending = ray.wait(pending, num_returns=1)
        fanin_timer = StageTimer.start("ray_get")
        results.extend(ray.get(ready))
        fanin_s += fanin_timer.stop()

    return results, {
        "operator_invocations": submit_count,
        "max_inflight": max_seen_inflight,
        "bounded_wait_s": sum(queue_wait_samples),
        "avg_bounded_wait_s": statistics.mean(queue_wait_samples) if queue_wait_samples else 0.0,
        "fanin_s": fanin_s,
    }


def write_embeddings(conn, results: list[dict]) -> int:
    rows = []
    for result in results:
        vectors = result["embedding"]
        for i, doc_id in enumerate(result["doc_id"]):
            rows.append(
                (
                    doc_id,
                    result["tenant_id"][i],
                    result["category"][i],
                    json.dumps(vectors[i].tolist(), separators=(",", ":")),
                )
            )
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO document_embeddings (doc_id, tenant_id, category, embedding_json)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (doc_id) DO UPDATE
            SET tenant_id = EXCLUDED.tenant_id,
                category = EXCLUDED.category,
                embedding_json = EXCLUDED.embedding_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def append_metrics(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def run(args: argparse.Namespace) -> dict:
    if args.dry_run:
        return {
            "status": "dry_run",
            "database_trigger": "job_table",
            "strategy": args.strategy,
            "total_rows": args.total_rows,
            "db_fetch_rows": args.db_fetch_rows,
            "ray_batch_rows": args.ray_batch_rows,
            "model_workers": args.model_workers,
            "max_inflight_limit": args.max_inflight,
        }
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL.")

    conn = connect(args.database_url)
    try:
        if args.setup:
            setup_schema(conn)
            seed_documents(conn, args.seed_rows)

        job_id = create_job(conn)
        ray.init(ignore_reinit_error=True)
        actors = [FakeEmbeddingActor.remote(args.embedding_dim) for _ in range(args.model_workers)]

        e2e_timer = StageTimer.start("e2e")
        processed_rows = 0
        object_count = 0
        arrow_build_s = 0.0
        db_fetch_s = 0.0
        operator_results = []
        submit_metrics = {
            "operator_invocations": 0,
            "max_inflight": 0,
            "bounded_wait_s": 0.0,
            "avg_bounded_wait_s": 0.0,
            "fanin_s": 0.0,
        }

        offset = 0
        while processed_rows < args.total_rows:
            fetch_timer = StageTimer.start("fetch_to_arrow")
            batch = fetch_record_batch(conn, args.db_fetch_rows, offset)
            arrow_build_s += fetch_timer.stop()
            if batch is None:
                break
            db_fetch_s += float(batch.column("db_fetch_s")[0].as_py())
            offset += batch.num_rows
            remaining = args.total_rows - processed_rows
            if batch.num_rows > remaining:
                batch = batch.slice(0, remaining)
            ray_batches = split_batch(batch, 1 if args.strategy == "fine" else args.ray_batch_rows)
            object_count += len(ray_batches)
            results, metrics = submit_with_backpressure(actors, ray_batches, args.max_inflight)
            operator_results.extend(results)
            for key in submit_metrics:
                if key == "max_inflight":
                    submit_metrics[key] = max(submit_metrics[key], metrics[key])
                else:
                    submit_metrics[key] += metrics[key]
            processed_rows += batch.num_rows

        write_timer = StageTimer.start("writeback")
        written_rows = write_embeddings(conn, operator_results)
        writeback_s = write_timer.stop()
        finish_job(conn, job_id)
        e2e_s = e2e_timer.stop()
        service_s = sum(float(result["service_s"]) for result in operator_results)
        token_count = sum(int(result["token_count"]) for result in operator_results)

        return {
            "status": "ok",
            "database_trigger": "job_table",
            "job_id": job_id,
            "strategy": args.strategy,
            "total_rows": processed_rows,
            "written_rows": written_rows,
            "db_fetch_rows": args.db_fetch_rows,
            "ray_batch_rows": args.ray_batch_rows,
            "embedding_dim": args.embedding_dim,
            "model_workers": args.model_workers,
            "max_inflight_limit": args.max_inflight,
            "object_count": object_count,
            "operator_invocations": submit_metrics["operator_invocations"],
            "max_inflight_seen": submit_metrics["max_inflight"],
            "token_count": token_count,
            "db_fetch_s": round(db_fetch_s, 6),
            "arrow_build_s": round(arrow_build_s, 6),
            "model_service_s": round(service_s, 6),
            "bounded_wait_s": round(submit_metrics["bounded_wait_s"], 6),
            "avg_bounded_wait_s": round(submit_metrics["avg_bounded_wait_s"], 6),
            "fanin_s": round(submit_metrics["fanin_s"], 6),
            "writeback_s": round(writeback_s, 6),
            "e2e_s": round(e2e_s, 6),
            "rows_per_s": round(processed_rows / e2e_s, 3) if e2e_s else 0.0,
        }
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    row = run(args)
    append_metrics(Path(args.output), row)
    print(json.dumps(row, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
