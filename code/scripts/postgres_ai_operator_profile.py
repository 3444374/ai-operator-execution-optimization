#!/usr/bin/env python3
"""Profile a PostgreSQL 18-compatible AI operator external execution path.

The local rehearsal environment currently runs PostgreSQL 18.4. The target
production validation platform is PostgreSQL 18.3. The script records the
actual server and pgvector versions in every non-dry-run CSV row so results do
not conflate the two environments.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib import error, request

import numpy as np
import pyarrow as pa


SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;

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
  embedding_vector vector(128),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE document_embeddings
ADD COLUMN IF NOT EXISTS embedding_vector vector(128);
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
    parser.add_argument("--model-backend", choices=["fake", "http_openai"], default="fake")
    parser.add_argument("--embedding-endpoint-url", default=os.environ.get("EMBEDDING_ENDPOINT_URL"))
    parser.add_argument(
        "--embedding-endpoint-urls",
        default=os.environ.get("EMBEDDING_ENDPOINT_URLS"),
        help="Comma-separated OpenAI-compatible embedding endpoint URLs for round-robin routing.",
    )
    parser.add_argument("--embedding-model", default=os.environ.get("EMBEDDING_MODEL", "local-embedding"))
    parser.add_argument("--embedding-api-key", default=os.environ.get("EMBEDDING_API_KEY"))
    parser.add_argument("--embedding-request-timeout-s", type=float, default=120.0)
    parser.add_argument("--model-workers", type=int, default=2)
    parser.add_argument("--max-inflight", type=int, default=8)
    parser.add_argument("--strategy", choices=["fine", "coalesced"], default="coalesced")
    parser.add_argument("--executor", choices=["ray_actor", "ray_task", "python"], default="ray_actor")
    parser.add_argument("--writeback-mode", choices=["json_text", "pgvector"], default="json_text")
    parser.add_argument("--write-batch-rows", type=int, default=0)
    parser.add_argument("--warmup-runs", type=int, default=0)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--experiment-id", default="manual")
    parser.add_argument("--output", default="feasibility/results/postgres_ai_operator_profile.csv")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without connecting to DB.")
    return parser.parse_args()


def embedding_endpoint_urls(args: argparse.Namespace) -> list[str]:
    if args.embedding_endpoint_urls:
        return [url.strip() for url in args.embedding_endpoint_urls.split(",") if url.strip()]
    if args.embedding_endpoint_url:
        return [args.embedding_endpoint_url]
    return []


def require_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: psycopg. Install with `.venv/bin/python -m pip install \"psycopg[binary]\"`."
        ) from exc
    return psycopg


def require_ray():
    try:
        import ray
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: ray. Install project dependencies before using --executor ray_actor."
        ) from exc
    return ray


def connect(database_url: str):
    psycopg = require_psycopg()
    return psycopg.connect(database_url)


def database_metadata(conn) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute("SHOW server_version")
        server_version = str(cur.fetchone()[0])
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        row = cur.fetchone()
    return {
        "server_version": server_version,
        "pgvector_version": str(row[0]) if row else "not_installed",
    }


def setup_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def count_documents(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM documents")
        return int(cur.fetchone()[0])


def seed_documents(conn, rows: int) -> None:
    existing_rows = count_documents(conn)
    if rows <= existing_rows:
        return
    values = [
        (
            i,
            i % 16,
            f"cat_{i % 8}",
            f"document {i} tenant {i % 16} category {i % 8} " + ("token " * 32),
        )
        for i in range(existing_rows, rows)
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


def fetch_record_batch(conn, limit: int, offset: int) -> tuple[pa.RecordBatch | None, dict[str, float]]:
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
        return None, {"db_fetch_s": db_fetch_s, "arrow_build_s": 0.0}
    arrow_timer = StageTimer.start("arrow_build")
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
    arrow_build_s = arrow_timer.stop()
    return batch, {"db_fetch_s": db_fetch_s, "arrow_build_s": arrow_build_s}


def split_batch(batch: pa.RecordBatch, rows_per_batch: int) -> list[pa.RecordBatch]:
    if rows_per_batch <= 0 or rows_per_batch >= batch.num_rows:
        return [batch]
    return [batch.slice(start, rows_per_batch) for start in range(0, batch.num_rows, rows_per_batch)]


def call_openai_embedding_endpoint(
    endpoint_url: str,
    model_name: str,
    texts: list[str],
    api_key: str | None,
    timeout_s: float,
) -> tuple[np.ndarray, int | None]:
    payload = json.dumps({"model": model_name, "input": texts}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = request.Request(endpoint_url, data=payload, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=timeout_s) as response:
            body = response.read()
    except error.URLError as exc:
        raise RuntimeError(f"Embedding endpoint request failed: {exc}") from exc
    decoded = json.loads(body.decode("utf-8"))
    data = sorted(decoded["data"], key=lambda item: item.get("index", 0))
    vectors = np.asarray([item["embedding"] for item in data], dtype=np.float32)
    usage = decoded.get("usage") or {}
    total_tokens = usage.get("total_tokens")
    return vectors, int(total_tokens) if total_tokens is not None else None


class FakeEmbeddingActor:
    def __init__(self, embedding_dim: int, service_tokens_per_s: float = 50000.0):
        self.embedding_dim = embedding_dim
        self.service_tokens_per_s = service_tokens_per_s

    def embed(self, batch: pa.RecordBatch) -> dict:
        service_start = time.perf_counter()
        service_start_epoch = time.time()
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
        service_end_epoch = time.time()
        return {
            "doc_id": batch.column("doc_id").to_pylist(),
            "tenant_id": batch.column("tenant_id").to_pylist(),
            "category": batch.column("category").to_pylist(),
            "embedding": vectors,
            "rows": batch.num_rows,
            "token_count": token_count,
            "service_s": service_s,
            "service_start_epoch_s": service_start_epoch,
            "service_end_epoch_s": service_end_epoch,
        }


class OpenAIEmbeddingActor:
    def __init__(self, endpoint_url: str, model_name: str, api_key: str | None, timeout_s: float):
        self.endpoint_url = endpoint_url
        self.model_name = model_name
        self.api_key = api_key
        self.timeout_s = timeout_s

    def embed(self, batch: pa.RecordBatch) -> dict:
        service_start = time.perf_counter()
        service_start_epoch = time.time()
        texts = batch.column("text").to_pylist()
        vectors, endpoint_tokens = call_openai_embedding_endpoint(
            self.endpoint_url,
            self.model_name,
            texts,
            self.api_key,
            self.timeout_s,
        )
        token_count = endpoint_tokens
        if token_count is None:
            token_count = sum(max(1, len(text.split())) for text in texts)
        service_s = time.perf_counter() - service_start
        service_end_epoch = time.time()
        return {
            "doc_id": batch.column("doc_id").to_pylist(),
            "tenant_id": batch.column("tenant_id").to_pylist(),
            "category": batch.column("category").to_pylist(),
            "embedding": vectors,
            "rows": batch.num_rows,
            "token_count": token_count,
            "service_s": service_s,
            "service_start_epoch_s": service_start_epoch,
            "service_end_epoch_s": service_end_epoch,
        }


def fake_embed_batch(batch: pa.RecordBatch, embedding_dim: int, service_tokens_per_s: float = 50000.0) -> dict:
    service_start = time.perf_counter()
    service_start_epoch = time.time()
    texts = batch.column("text").to_pylist()
    token_count = sum(max(1, len(text.split())) for text in texts)
    target_s = token_count / service_tokens_per_s
    if target_s > 0:
        time.sleep(target_s)
    vectors = np.empty((batch.num_rows, embedding_dim), dtype=np.float32)
    for i, text in enumerate(texts):
        seed = hash(text) & 0xFFFFFFFF
        rng = np.random.default_rng(seed)
        vectors[i, :] = rng.random(embedding_dim, dtype=np.float32)
    service_s = time.perf_counter() - service_start
    service_end_epoch = time.time()
    return {
        "doc_id": batch.column("doc_id").to_pylist(),
        "tenant_id": batch.column("tenant_id").to_pylist(),
        "category": batch.column("category").to_pylist(),
        "embedding": vectors,
        "rows": batch.num_rows,
        "token_count": token_count,
        "service_s": service_s,
        "service_start_epoch_s": service_start_epoch,
        "service_end_epoch_s": service_end_epoch,
    }


def http_openai_embed_batch(
    batch: pa.RecordBatch,
    endpoint_url: str,
    model_name: str,
    api_key: str | None,
    timeout_s: float,
) -> dict:
    return OpenAIEmbeddingActor(endpoint_url, model_name, api_key, timeout_s).embed(batch)


def submit_with_backpressure(
    ray_module,
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
    submit_s = 0.0

    for batch in batches:
        while len(pending) >= max_inflight:
            wait_timer = StageTimer.start("bounded_wait")
            ready, pending = ray_module.wait(pending, num_returns=1)
            queue_wait_samples.append(wait_timer.stop())
            fanin_timer = StageTimer.start("ray_get")
            results.extend(ray_module.get(ready))
            fanin_s += fanin_timer.stop()
        actor = actors[submit_count % len(actors)]
        submit_timer = StageTimer.start("submit")
        ref = actor.embed.remote(batch)
        submit_s += submit_timer.stop()
        pending.append(ref)
        submit_count += 1
        max_seen_inflight = max(max_seen_inflight, len(pending))

    while pending:
        ready, pending = ray_module.wait(pending, num_returns=1)
        fanin_timer = StageTimer.start("ray_get")
        results.extend(ray_module.get(ready))
        fanin_s += fanin_timer.stop()

    return results, {
        "operator_invocations": submit_count,
        "max_inflight": max_seen_inflight,
        "bounded_wait_s": sum(queue_wait_samples),
        "avg_bounded_wait_s": statistics.mean(queue_wait_samples) if queue_wait_samples else 0.0,
        "fanin_s": fanin_s,
        "submit_s": submit_s,
    }


def submit_ray_tasks(
    ray_module,
    remote_embed,
    batches: Iterable[pa.RecordBatch],
    max_inflight: int,
    embedding_dim: int,
    model_backend: str,
    endpoint_urls: list[str],
    model_name: str,
    api_key: str | None,
    timeout_s: float,
) -> tuple[list[dict], dict]:
    pending = []
    results = []
    submit_count = 0
    max_seen_inflight = 0
    queue_wait_samples = []
    fanin_s = 0.0
    submit_s = 0.0

    for batch in batches:
        while len(pending) >= max_inflight:
            wait_timer = StageTimer.start("bounded_wait")
            ready, pending = ray_module.wait(pending, num_returns=1)
            queue_wait_samples.append(wait_timer.stop())
            fanin_timer = StageTimer.start("ray_get")
            results.extend(ray_module.get(ready))
            fanin_s += fanin_timer.stop()
        if model_backend == "fake":
            submit_timer = StageTimer.start("submit")
            pending.append(remote_embed.remote(batch, embedding_dim))
            submit_s += submit_timer.stop()
        else:
            endpoint_url = endpoint_urls[submit_count % len(endpoint_urls)]
            submit_timer = StageTimer.start("submit")
            pending.append(remote_embed.remote(batch, endpoint_url, model_name, api_key, timeout_s))
            submit_s += submit_timer.stop()
        submit_count += 1
        max_seen_inflight = max(max_seen_inflight, len(pending))

    while pending:
        ready, pending = ray_module.wait(pending, num_returns=1)
        fanin_timer = StageTimer.start("ray_get")
        results.extend(ray_module.get(ready))
        fanin_s += fanin_timer.stop()

    return results, {
        "operator_invocations": submit_count,
        "max_inflight": max_seen_inflight,
        "bounded_wait_s": sum(queue_wait_samples),
        "avg_bounded_wait_s": statistics.mean(queue_wait_samples) if queue_wait_samples else 0.0,
        "fanin_s": fanin_s,
        "submit_s": submit_s,
    }


def submit_python_batches(batches: Iterable[pa.RecordBatch], embedding_dim: int) -> tuple[list[dict], dict]:
    results = []
    invocation_count = 0
    for batch in batches:
        results.append(fake_embed_batch(batch, embedding_dim))
        invocation_count += 1
    return results, {
        "operator_invocations": invocation_count,
        "max_inflight": 1 if invocation_count else 0,
        "bounded_wait_s": 0.0,
        "avg_bounded_wait_s": 0.0,
        "fanin_s": 0.0,
        "submit_s": 0.0,
    }


def submit_python_http_openai_batches(
    batches: Iterable[pa.RecordBatch],
    endpoint_urls: list[str],
    model_name: str,
    api_key: str | None,
    timeout_s: float,
) -> tuple[list[dict], dict]:
    results = []
    invocation_count = 0
    for batch in batches:
        endpoint_url = endpoint_urls[invocation_count % len(endpoint_urls)]
        results.append(http_openai_embed_batch(batch, endpoint_url, model_name, api_key, timeout_s))
        invocation_count += 1
    return results, {
        "operator_invocations": invocation_count,
        "max_inflight": 1 if invocation_count else 0,
        "bounded_wait_s": 0.0,
        "avg_bounded_wait_s": 0.0,
        "fanin_s": 0.0,
        "submit_s": 0.0,
    }


def vector_to_pg_literal(vector: np.ndarray) -> str:
    return json.dumps(vector.tolist(), separators=(",", ":"))


def batched_rows(rows: list[tuple], batch_rows: int) -> Iterable[list[tuple]]:
    if batch_rows <= 0:
        yield rows
        return
    for start in range(0, len(rows), batch_rows):
        yield rows[start : start + batch_rows]


def write_embeddings(conn, results: list[dict], writeback_mode: str, write_batch_rows: int) -> int:
    rows = []
    for result in results:
        vectors = result["embedding"]
        for i, doc_id in enumerate(result["doc_id"]):
            if writeback_mode == "json_text":
                rows.append(
                    (
                        doc_id,
                        result["tenant_id"][i],
                        result["category"][i],
                        vector_to_pg_literal(vectors[i]),
                    )
                )
            elif writeback_mode == "pgvector":
                rows.append(
                    (
                        doc_id,
                        result["tenant_id"][i],
                        result["category"][i],
                        vector_to_pg_literal(vectors[i]),
                    )
                )
            else:
                raise ValueError(f"Unsupported writeback mode: {writeback_mode}")
    with conn.cursor() as cur:
        if writeback_mode == "json_text":
            statement = """
                INSERT INTO document_embeddings (doc_id, tenant_id, category, embedding_json)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (doc_id) DO UPDATE
                SET tenant_id = EXCLUDED.tenant_id,
                    category = EXCLUDED.category,
                    embedding_json = EXCLUDED.embedding_json,
                    updated_at = CURRENT_TIMESTAMP
                """
        else:
            statement = """
                INSERT INTO document_embeddings (doc_id, tenant_id, category, embedding_json, embedding_vector)
                VALUES (%s, %s, %s, '[]', %s::vector)
                ON CONFLICT (doc_id) DO UPDATE
                SET tenant_id = EXCLUDED.tenant_id,
                    category = EXCLUDED.category,
                    embedding_json = EXCLUDED.embedding_json,
                    embedding_vector = EXCLUDED.embedding_vector,
                    updated_at = CURRENT_TIMESTAMP
                """
        for chunk in batched_rows(rows, write_batch_rows):
            cur.executemany(statement, chunk)
    conn.commit()
    return len(rows)


def model_request_wall_time(results: list[dict]) -> float:
    starts = [float(result["service_start_epoch_s"]) for result in results if "service_start_epoch_s" in result]
    ends = [float(result["service_end_epoch_s"]) for result in results if "service_end_epoch_s" in result]
    if not starts or not ends:
        return 0.0
    return max(ends) - min(starts)


def append_metrics(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def gpu_metadata() -> dict[str, str]:
    try:
        completed = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {
            "gpu_metrics_status": f"unavailable:{type(exc).__name__}",
            "gpu_name": "",
            "gpu_utilization_pct": "",
            "gpu_memory_used_mib": "",
            "gpu_memory_total_mib": "",
        }
    first_line = completed.stdout.strip().splitlines()[0] if completed.stdout.strip() else ""
    parts = [part.strip() for part in first_line.split(",")]
    if len(parts) != 4:
        return {
            "gpu_metrics_status": "unavailable:unexpected_nvidia_smi_output",
            "gpu_name": "",
            "gpu_utilization_pct": "",
            "gpu_memory_used_mib": "",
            "gpu_memory_total_mib": "",
        }
    return {
        "gpu_metrics_status": "snapshot",
        "gpu_name": parts[0],
        "gpu_utilization_pct": parts[1],
        "gpu_memory_used_mib": parts[2],
        "gpu_memory_total_mib": parts[3],
    }


def run_once(args: argparse.Namespace, phase: str, repeat_index: int) -> dict:
    endpoint_urls = embedding_endpoint_urls(args)
    endpoint_url_label = ";".join(endpoint_urls)
    if args.dry_run:
        return {
            "status": "dry_run",
            "experiment_id": args.experiment_id,
            "phase": phase,
            "repeat_index": repeat_index,
            "database_trigger": "job_table",
            "executor": args.executor,
            "strategy": args.strategy,
            "model_backend": args.model_backend,
            "embedding_endpoint_url": endpoint_url_label,
            "embedding_model": args.embedding_model,
            "total_rows": args.total_rows,
            "db_fetch_rows": args.db_fetch_rows,
            "ray_batch_rows": args.ray_batch_rows,
            "model_workers": args.model_workers,
            "max_inflight_limit": args.max_inflight,
            "writeback_mode": args.writeback_mode,
            "write_batch_rows": args.write_batch_rows,
        }
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL.")
    if args.model_backend == "http_openai" and not endpoint_urls:
        raise SystemExit(
            "Missing --embedding-endpoint-url, --embedding-endpoint-urls, "
            "EMBEDDING_ENDPOINT_URL or EMBEDDING_ENDPOINT_URLS for http_openai backend."
        )
    if args.writeback_mode == "pgvector" and args.model_backend == "http_openai" and args.embedding_dim != 128:
        raise SystemExit("The current schema uses vector(128); use --writeback-mode json_text or a 128-dim model.")

    conn = connect(args.database_url)
    try:
        db_metadata = database_metadata(conn)
        gpu_snapshot = gpu_metadata()
        if args.setup:
            setup_schema(conn)
            seed_documents(conn, args.seed_rows)

        job_id = create_job(conn)
        actors = []
        ray_module = None
        remote_embed = None
        if args.executor in {"ray_actor", "ray_task"}:
            ray_module = require_ray()
            ray_module.init(ignore_reinit_error=True)
            if args.executor == "ray_actor":
                if args.model_backend == "fake":
                    RayEmbeddingActor = ray_module.remote(FakeEmbeddingActor)
                    actors = [RayEmbeddingActor.remote(args.embedding_dim) for _ in range(args.model_workers)]
                else:
                    RayEmbeddingActor = ray_module.remote(OpenAIEmbeddingActor)
                    actors = [
                        RayEmbeddingActor.remote(
                            endpoint_urls[index % len(endpoint_urls)],
                            args.embedding_model,
                            args.embedding_api_key,
                            args.embedding_request_timeout_s,
                        )
                        for index in range(args.model_workers)
                    ]
            else:
                remote_embed = ray_module.remote(
                    fake_embed_batch if args.model_backend == "fake" else http_openai_embed_batch
                )

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
            "submit_s": 0.0,
        }

        operator_wall_s = 0.0
        offset = 0
        while processed_rows < args.total_rows:
            batch, fetch_metrics = fetch_record_batch(conn, args.db_fetch_rows, offset)
            if batch is None:
                break
            db_fetch_s += fetch_metrics["db_fetch_s"]
            arrow_build_s += fetch_metrics["arrow_build_s"]
            offset += batch.num_rows
            remaining = args.total_rows - processed_rows
            if batch.num_rows > remaining:
                batch = batch.slice(0, remaining)
            ray_batches = split_batch(batch, 1 if args.strategy == "fine" else args.ray_batch_rows)
            object_count += len(ray_batches)
            operator_timer = StageTimer.start("operator_wall")
            if args.executor == "ray_actor":
                results, metrics = submit_with_backpressure(ray_module, actors, ray_batches, args.max_inflight)
            elif args.executor == "ray_task":
                results, metrics = submit_ray_tasks(
                    ray_module,
                    remote_embed,
                    ray_batches,
                    args.max_inflight,
                    args.embedding_dim,
                    args.model_backend,
                    endpoint_urls,
                    args.embedding_model,
                    args.embedding_api_key,
                    args.embedding_request_timeout_s,
                )
            else:
                if args.model_backend == "fake":
                    results, metrics = submit_python_batches(ray_batches, args.embedding_dim)
                else:
                    results, metrics = submit_python_http_openai_batches(
                        ray_batches,
                        endpoint_urls,
                        args.embedding_model,
                        args.embedding_api_key,
                        args.embedding_request_timeout_s,
                    )
            operator_wall_s += operator_timer.stop()
            operator_results.extend(results)
            for key in submit_metrics:
                if key == "max_inflight":
                    submit_metrics[key] = max(submit_metrics[key], metrics[key])
                else:
                    submit_metrics[key] += metrics[key]
            processed_rows += batch.num_rows

        write_timer = StageTimer.start("writeback")
        written_rows = write_embeddings(
            conn,
            operator_results,
            args.writeback_mode,
            args.write_batch_rows,
        )
        writeback_s = write_timer.stop()
        finish_job(conn, job_id)
        e2e_s = e2e_timer.stop()
        service_s = sum(float(result["service_s"]) for result in operator_results)
        request_wall_s = model_request_wall_time(operator_results)
        token_count = sum(int(result["token_count"]) for result in operator_results)

        return {
            "status": "ok",
            "experiment_id": args.experiment_id,
            "phase": phase,
            "repeat_index": repeat_index,
            **db_metadata,
            **gpu_snapshot,
            "database_trigger": "job_table",
            "job_id": job_id,
            "executor": args.executor,
            "strategy": args.strategy,
            "model_backend": args.model_backend,
            "embedding_endpoint_url": endpoint_url_label,
            "embedding_model": args.embedding_model,
            "total_rows": processed_rows,
            "written_rows": written_rows,
            "db_fetch_rows": args.db_fetch_rows,
            "ray_batch_rows": args.ray_batch_rows,
            "embedding_dim": args.embedding_dim,
            "model_workers": args.model_workers,
            "max_inflight_limit": args.max_inflight,
            "writeback_mode": args.writeback_mode,
            "write_batch_rows": args.write_batch_rows,
            "object_count": object_count,
            "operator_invocations": submit_metrics["operator_invocations"],
            "max_inflight_seen": submit_metrics["max_inflight"],
            "token_count": token_count,
            "db_fetch_s": round(db_fetch_s, 6),
            "arrow_build_s": round(arrow_build_s, 6),
            "model_service_s": round(service_s, 6),
            "model_request_wall_s": round(request_wall_s, 6),
            "operator_wall_s": round(operator_wall_s, 6),
            "submit_s": round(submit_metrics["submit_s"], 6),
            "bounded_wait_s": round(submit_metrics["bounded_wait_s"], 6),
            "avg_bounded_wait_s": round(submit_metrics["avg_bounded_wait_s"], 6),
            "fanin_s": round(submit_metrics["fanin_s"], 6),
            "writeback_s": round(writeback_s, 6),
            "e2e_s": round(e2e_s, 6),
            "rows_per_s": round(processed_rows / e2e_s, 3) if e2e_s else 0.0,
        }
    finally:
        conn.close()


def iter_run_phases(warmup_runs: int, repeats: int) -> Iterable[tuple[str, int]]:
    for repeat_index in range(1, warmup_runs + 1):
        yield "warmup", repeat_index
    for repeat_index in range(1, repeats + 1):
        yield "formal", repeat_index


def main() -> None:
    args = parse_args()
    for phase, repeat_index in iter_run_phases(args.warmup_runs, args.repeats):
        row = run_once(args, phase, repeat_index)
        append_metrics(Path(args.output), row)
        print(json.dumps(row, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
