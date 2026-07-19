#!/usr/bin/env python3
"""Profile a PostgreSQL 18-compatible AI operator external execution path.

The local rehearsal environment currently runs PostgreSQL 18.4. The target
production validation platform is PostgreSQL 18.3. The script records the
actual server and pgvector versions in every non-dry-run CSV row so results do
not conflate the two environments.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Iterable

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.metrics import (
    StageTimer,
    append_metrics,
    batch_result_stats,
    gpu_metadata,
    scrape_prometheus_metrics,
    vllm_metric_delta_stats,
)
from src.model_backends import (
    CompatibleHTTPCompletionActor,
    CompatibleHTTPEmbeddingActor,
    FakeCompletionActor,
    FakeEmbeddingActor,
    OllamaCompletionActor,
    compatible_http_complete_batch,
    compatible_http_embed_batch,
    fake_complete_batch,
    fake_embed_batch,
    model_request_wall_time,
    normalize_completion_backend,
    normalize_embedding_backend,
    ollama_complete_batch,
)
from src.organizers import OrganizerConfig, configure_daft_runner, make_organizer
from src.sinks import write_completions, write_embeddings
from src.sources import SourceConfig, make_source
from src.workloads import WORKLOAD_NAMES, generate_document_rows


SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS documents (
  doc_id BIGINT PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  text TEXT NOT NULL,
  workload_name TEXT NOT NULL DEFAULT 'synthetic',
  prompt_tokens INTEGER,
  target_output_tokens INTEGER,
  arrival_time_s DOUBLE PRECISION,
  session_id TEXT,
  prefix_key TEXT,
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

CREATE TABLE IF NOT EXISTS document_completions (
  doc_id BIGINT PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  completion_text TEXT NOT NULL,
  completion_json TEXT NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile PostgreSQL-triggered AI_EMBED external execution with Ray and Arrow."
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--setup", action="store_true", help="Create required tables before running.")
    parser.add_argument("--seed-rows", type=int, default=0, help="Insert workload documents if table is short.")
    parser.add_argument("--seed-workload", choices=WORKLOAD_NAMES, default="synthetic")
    parser.add_argument(
        "--reset-documents",
        action="store_true",
        help="Delete existing seeded documents before inserting --seed-rows rows.",
    )
    parser.add_argument("--total-rows", type=int, default=10000)
    parser.add_argument("--db-fetch-rows", type=int, default=1024)
    parser.add_argument("--data-source", choices=["arrow_postgres", "daft_postgres"], default="arrow_postgres")
    parser.add_argument("--source-workload-name", default=None)
    parser.add_argument(
        "--source-order",
        choices=["doc_id", "arrival_time"],
        default="doc_id",
        help=(
            "PostgreSQL read order: doc_id for offline throughput scans, "
            "arrival_time for arrival-aware service scheduling experiments."
        ),
    )
    parser.add_argument("--operator", choices=["ai_embed", "ai_complete"], default="ai_embed")
    parser.add_argument("--ray-batch-rows", type=int, default=1024)
    parser.add_argument(
        "--batching-policy",
        choices=[
            "fixed_rows",
            "token_budget",
            "length_align_fixed_rows",
            "length_align_token_budget",
            "prefix_aware_fixed_rows",
            "prefix_aware_token_budget",
        ],
        default="fixed_rows",
        help="Upstream batch construction policy before Ray submission.",
    )
    parser.add_argument(
        "--token-budget",
        type=int,
        default=0,
        help="Estimated prompt+completion token budget per Ray submission when --batching-policy token_budget.",
    )
    parser.add_argument("--embedding-dim", type=int, default=128)
    parser.add_argument("--model-backend", choices=["fake", "compatible_http", "http_openai", "ollama"], default="fake")
    parser.add_argument("--embedding-endpoint-url", default=os.environ.get("EMBEDDING_ENDPOINT_URL"))
    parser.add_argument(
        "--embedding-endpoint-urls",
        default=os.environ.get("EMBEDDING_ENDPOINT_URLS"),
        help="Comma-separated OpenAI-compatible embedding endpoint URLs for round-robin routing.",
    )
    parser.add_argument("--embedding-model", default=os.environ.get("EMBEDDING_MODEL", "local-embedding"))
    parser.add_argument("--embedding-api-key", default=os.environ.get("EMBEDDING_API_KEY"))
    parser.add_argument("--embedding-request-timeout-s", type=float, default=120.0)
    parser.add_argument("--completion-endpoint-url", default=os.environ.get("COMPLETION_ENDPOINT_URL"))
    parser.add_argument(
        "--completion-endpoint-urls",
        default=os.environ.get("COMPLETION_ENDPOINT_URLS"),
        help="Comma-separated OpenAI-compatible completion endpoint URLs for round-robin routing.",
    )
    parser.add_argument("--completion-model", default=os.environ.get("COMPLETION_MODEL", "local-completion"))
    parser.add_argument("--completion-api-key", default=os.environ.get("COMPLETION_API_KEY"))
    parser.add_argument("--completion-request-timeout-s", type=float, default=120.0)
    parser.add_argument("--completion-max-tokens", type=int, default=128)
    parser.add_argument("--model-metrics-url", default=os.environ.get("MODEL_METRICS_URL"))
    parser.add_argument("--model-workers", type=int, default=2)
    parser.add_argument("--max-inflight", type=int, default=8)
    parser.add_argument("--scheduling-policy", choices=["static", "queue_adaptive"], default="static")
    parser.add_argument("--adaptive-min-inflight", type=int, default=2)
    parser.add_argument("--adaptive-max-inflight", type=int, default=16)
    parser.add_argument("--adaptive-queue-threshold", type=int, default=0)
    parser.add_argument("--adaptive-running-threshold", type=int, default=128)
    parser.add_argument("--adaptive-kv-threshold", type=float, default=0.85)
    parser.add_argument("--adaptive-poll-interval-s", type=float, default=0.05)
    parser.add_argument("--strategy", choices=["fine", "coalesced"], default="coalesced")
    parser.add_argument("--organizer", choices=["arrow", "daft"], default="arrow")
    parser.add_argument(
        "--organizer-partition-mode",
        choices=["none", "into_partitions", "repartition"],
        default="none",
    )
    parser.add_argument("--organizer-partitions", type=int, default=0)
    parser.add_argument("--daft-runner", choices=["native", "ray"], default="native")
    parser.add_argument("--executor", choices=["ray_actor", "ray_task", "python"], default="ray_actor")
    parser.add_argument("--writeback-mode", choices=["none", "json_text", "pgvector"], default="json_text")
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


def completion_endpoint_urls(args: argparse.Namespace) -> list[str]:
    if args.completion_endpoint_urls:
        return [url.strip() for url in args.completion_endpoint_urls.split(",") if url.strip()]
    if args.completion_endpoint_url:
        return [args.completion_endpoint_url]
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


def ray_runtime_env() -> dict[str, dict[str, str]]:
    pythonpath = str(CODE_ROOT)
    existing_pythonpath = os.environ.get("PYTHONPATH")
    if existing_pythonpath:
        pythonpath = os.pathsep.join([pythonpath, existing_pythonpath])
    return {"env_vars": {"PYTHONPATH": pythonpath}}


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


def embedding_vector_column_dim(conn) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = current_schema()
              AND c.relname = 'document_embeddings'
              AND a.attname = 'embedding_vector'
              AND NOT a.attisdropped
            """
        )
        row = cur.fetchone()
    if not row:
        return None
    type_name = str(row[0])
    if not type_name.startswith("vector(") or not type_name.endswith(")"):
        return None
    return int(type_name.removeprefix("vector(").removesuffix(")"))


def ensure_embedding_vector_column(conn, embedding_dim: int) -> None:
    if embedding_dim <= 0:
        raise ValueError("--embedding-dim must be positive")
    current_dim = embedding_vector_column_dim(conn)
    if current_dim == embedding_dim:
        return
    with conn.cursor() as cur:
        if current_dim is not None:
            cur.execute("ALTER TABLE document_embeddings DROP COLUMN embedding_vector")
        cur.execute(f"ALTER TABLE document_embeddings ADD COLUMN embedding_vector vector({embedding_dim})")
    conn.commit()


def setup_schema(conn, embedding_dim: int) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS workload_name TEXT NOT NULL DEFAULT 'synthetic'")
        cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS prompt_tokens INTEGER")
        cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS target_output_tokens INTEGER")
        cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS arrival_time_s DOUBLE PRECISION")
        cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS session_id TEXT")
        cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS prefix_key TEXT")
    conn.commit()
    ensure_embedding_vector_column(conn, embedding_dim)


def count_documents(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM documents")
        return int(cur.fetchone()[0])


def reset_documents(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE documents CASCADE")
    conn.commit()


def seed_documents(conn, rows: int, workload: str) -> None:
    existing_rows = count_documents(conn)
    if rows <= existing_rows:
        return
    values = [
        (row.doc_id, row.tenant_id, row.category, row.text, workload)
        for row in generate_document_rows(existing_rows, rows, workload)
    ]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO documents (doc_id, tenant_id, category, text, workload_name) VALUES (%s, %s, %s, %s, %s)",
            values,
        )
    conn.commit()


def create_job(conn, operator_name: str, output_table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ai_operator_jobs (operator_name, input_table, output_table, status, started_at)
            VALUES (%s, 'documents', %s, 'running', CURRENT_TIMESTAMP)
            RETURNING job_id
            """,
            (operator_name, output_table),
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


def submit_with_backpressure(
    ray_module,
    actors: list,
    batches: Iterable[pa.RecordBatch | pa.Table],
    max_inflight: int,
    method_name: str,
    adaptive_config: dict | None = None,
) -> tuple[list[dict], dict]:
    pending = []
    results = []
    submit_count = 0
    max_seen_inflight = 0
    queue_wait_samples = []
    fanin_s = 0.0
    submit_s = 0.0
    adaptive_downshifts = 0
    adaptive_upshifts = 0
    adaptive_limit_sum = 0
    adaptive_limit_samples = 0

    for batch in batches:
        current_limit, decision = adaptive_inflight_limit(max_inflight, adaptive_config)
        adaptive_downshifts += 1 if decision == "down" else 0
        adaptive_upshifts += 1 if decision == "up" else 0
        adaptive_limit_sum += current_limit
        adaptive_limit_samples += 1
        while len(pending) >= current_limit:
            wait_timer = StageTimer.start("bounded_wait")
            ready, pending = ray_module.wait(pending, num_returns=1)
            queue_wait_samples.append(wait_timer.stop())
            fanin_timer = StageTimer.start("ray_get")
            results.extend(ray_module.get(ready))
            fanin_s += fanin_timer.stop()
            current_limit, decision = adaptive_inflight_limit(max_inflight, adaptive_config)
            adaptive_downshifts += 1 if decision == "down" else 0
            adaptive_upshifts += 1 if decision == "up" else 0
            adaptive_limit_sum += current_limit
            adaptive_limit_samples += 1
        actor = actors[submit_count % len(actors)]
        submit_timer = StageTimer.start("submit")
        ref = getattr(actor, method_name).remote(batch)
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
        "adaptive_downshifts": adaptive_downshifts,
        "adaptive_upshifts": adaptive_upshifts,
        "adaptive_limit_mean": adaptive_limit_sum / adaptive_limit_samples if adaptive_limit_samples else max_inflight,
    }


def submit_ray_tasks(
    ray_module,
    remote_embed,
    batches: Iterable[pa.RecordBatch | pa.Table],
    max_inflight: int,
    operator: str,
    embedding_dim: int,
    model_backend: str,
    endpoint_urls: list[str],
    model_name: str,
    api_key: str | None,
    timeout_s: float,
    completion_max_tokens: int,
    adaptive_config: dict | None = None,
) -> tuple[list[dict], dict]:
    pending = []
    results = []
    submit_count = 0
    max_seen_inflight = 0
    queue_wait_samples = []
    fanin_s = 0.0
    submit_s = 0.0
    adaptive_downshifts = 0
    adaptive_upshifts = 0
    adaptive_limit_sum = 0
    adaptive_limit_samples = 0

    for batch in batches:
        current_limit, decision = adaptive_inflight_limit(max_inflight, adaptive_config)
        adaptive_downshifts += 1 if decision == "down" else 0
        adaptive_upshifts += 1 if decision == "up" else 0
        adaptive_limit_sum += current_limit
        adaptive_limit_samples += 1
        while len(pending) >= current_limit:
            wait_timer = StageTimer.start("bounded_wait")
            ready, pending = ray_module.wait(pending, num_returns=1)
            queue_wait_samples.append(wait_timer.stop())
            fanin_timer = StageTimer.start("ray_get")
            results.extend(ray_module.get(ready))
            fanin_s += fanin_timer.stop()
            current_limit, decision = adaptive_inflight_limit(max_inflight, adaptive_config)
            adaptive_downshifts += 1 if decision == "down" else 0
            adaptive_upshifts += 1 if decision == "up" else 0
            adaptive_limit_sum += current_limit
            adaptive_limit_samples += 1
        if model_backend == "fake":
            submit_timer = StageTimer.start("submit")
            if operator == "ai_embed":
                pending.append(remote_embed.remote(batch, embedding_dim))
            else:
                pending.append(remote_embed.remote(batch, completion_max_tokens))
            submit_s += submit_timer.stop()
        else:
            endpoint_url = endpoint_urls[submit_count % len(endpoint_urls)]
            submit_timer = StageTimer.start("submit")
            if operator == "ai_embed":
                pending.append(remote_embed.remote(batch, endpoint_url, model_name, api_key, timeout_s))
            else:
                pending.append(
                    remote_embed.remote(batch, endpoint_url, model_name, api_key, timeout_s, completion_max_tokens)
                )
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
        "adaptive_downshifts": adaptive_downshifts,
        "adaptive_upshifts": adaptive_upshifts,
        "adaptive_limit_mean": adaptive_limit_sum / adaptive_limit_samples if adaptive_limit_samples else max_inflight,
    }


def adaptive_inflight_limit(static_limit: int, adaptive_config: dict | None) -> tuple[int, str]:
    if not adaptive_config:
        return static_limit, "static"
    metrics_url = adaptive_config.get("metrics_url")
    if not metrics_url:
        return static_limit, "static"
    metrics = scrape_prometheus_metrics(metrics_url, timeout_s=1.0)
    if not metrics:
        return static_limit, "static"
    min_limit = max(1, int(adaptive_config["min_inflight"]))
    max_limit = max(min_limit, int(adaptive_config["max_inflight"]))
    waiting = metrics.get("vllm:num_requests_waiting", 0.0)
    running = metrics.get("vllm:num_requests_running", 0.0)
    kv_usage = metrics.get("vllm:kv_cache_usage_perc", 0.0)
    if (
        waiting > float(adaptive_config["queue_threshold"])
        or running >= float(adaptive_config["running_threshold"])
        or kv_usage >= float(adaptive_config["kv_threshold"])
    ):
        time.sleep(float(adaptive_config["poll_interval_s"]))
        return min_limit, "down"
    return max_limit, "up"


def submit_python_batches(batches: Iterable[pa.RecordBatch | pa.Table], embedding_dim: int) -> tuple[list[dict], dict]:
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
        "adaptive_downshifts": 0,
        "adaptive_upshifts": 0,
        "adaptive_limit_mean": 1 if invocation_count else 0,
    }


def submit_python_completion_batches(
    batches: Iterable[pa.RecordBatch | pa.Table],
    completion_max_tokens: int,
) -> tuple[list[dict], dict]:
    results = []
    invocation_count = 0
    for batch in batches:
        results.append(fake_complete_batch(batch, completion_max_tokens))
        invocation_count += 1
    return results, {
        "operator_invocations": invocation_count,
        "max_inflight": 1 if invocation_count else 0,
        "bounded_wait_s": 0.0,
        "avg_bounded_wait_s": 0.0,
        "fanin_s": 0.0,
        "submit_s": 0.0,
        "adaptive_downshifts": 0,
        "adaptive_upshifts": 0,
        "adaptive_limit_mean": 1 if invocation_count else 0,
    }


def submit_python_compatible_http_batches(
    batches: Iterable[pa.RecordBatch | pa.Table],
    operator: str,
    endpoint_urls: list[str],
    model_name: str,
    api_key: str | None,
    timeout_s: float,
    completion_max_tokens: int,
    model_backend: str,
) -> tuple[list[dict], dict]:
    results = []
    invocation_count = 0
    for batch in batches:
        endpoint_url = endpoint_urls[invocation_count % len(endpoint_urls)]
        if operator == "ai_embed":
            results.append(compatible_http_embed_batch(batch, endpoint_url, model_name, api_key, timeout_s))
        elif model_backend == "ollama":
            results.append(
                ollama_complete_batch(batch, endpoint_url, model_name, api_key, timeout_s, completion_max_tokens)
            )
        else:
            results.append(
                compatible_http_complete_batch(batch, endpoint_url, model_name, api_key, timeout_s, completion_max_tokens)
            )
        invocation_count += 1
    return results, {
        "operator_invocations": invocation_count,
        "max_inflight": 1 if invocation_count else 0,
        "bounded_wait_s": 0.0,
        "avg_bounded_wait_s": 0.0,
        "fanin_s": 0.0,
        "submit_s": 0.0,
        "adaptive_downshifts": 0,
        "adaptive_upshifts": 0,
        "adaptive_limit_mean": 1 if invocation_count else 0,
    }


def run_once(args: argparse.Namespace, phase: str, repeat_index: int) -> dict:
    endpoint_urls = completion_endpoint_urls(args) if args.operator == "ai_complete" else embedding_endpoint_urls(args)
    endpoint_url_label = ";".join(endpoint_urls)
    if args.operator == "ai_embed" and args.model_backend == "ollama":
        raise SystemExit("Ollama backend is only supported for --operator ai_complete.")
    model_backend = (
        normalize_completion_backend(args.model_backend)
        if args.operator == "ai_complete"
        else normalize_embedding_backend(args.model_backend)
    )
    if args.operator == "ai_complete" and model_backend == "ollama" and not endpoint_urls:
        endpoint_urls = ["http://localhost:11434"]
        endpoint_url_label = ";".join(endpoint_urls)
    model_name = args.completion_model if args.operator == "ai_complete" else args.embedding_model
    api_key = args.completion_api_key if args.operator == "ai_complete" else args.embedding_api_key
    request_timeout_s = (
        args.completion_request_timeout_s if args.operator == "ai_complete" else args.embedding_request_timeout_s
    )
    if args.dry_run:
        return {
            "status": "dry_run",
            "experiment_id": args.experiment_id,
            "phase": phase,
            "repeat_index": repeat_index,
            "database_trigger": "job_table",
            "operator": args.operator,
            "seed_workload": args.seed_workload,
            "executor": args.executor,
            "strategy": args.strategy,
            "data_source": args.data_source,
            "source_workload_name": args.source_workload_name or "",
            "source_order": args.source_order,
            "organizer": args.organizer,
            "organizer_partition_mode": args.organizer_partition_mode,
            "organizer_partitions": args.organizer_partitions,
            "daft_runner": args.daft_runner,
            "model_backend": model_backend,
            "model_endpoint_url": endpoint_url_label,
            "model_name": model_name,
            "model_request_timeout_s": request_timeout_s,
            "total_rows": args.total_rows,
            "db_fetch_rows": args.db_fetch_rows,
            "ray_batch_rows": args.ray_batch_rows,
            "batching_policy": args.batching_policy,
            "token_budget": args.token_budget,
            "completion_max_tokens": args.completion_max_tokens if args.operator == "ai_complete" else "",
            "model_workers": args.model_workers,
            "max_inflight_limit": args.max_inflight,
            "scheduling_policy": args.scheduling_policy,
            "adaptive_min_inflight": args.adaptive_min_inflight,
            "adaptive_max_inflight": args.adaptive_max_inflight,
            "writeback_mode": args.writeback_mode,
            "write_batch_rows": args.write_batch_rows,
        }
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL.")
    if model_backend in {"compatible_http", "ollama"} and not endpoint_urls:
        raise SystemExit(
            "Missing endpoint URL. Use embedding endpoint args for ai_embed or completion endpoint args for ai_complete."
        )
    if args.operator == "ai_complete" and args.writeback_mode == "pgvector":
        raise SystemExit("AI_COMPLETE does not support --writeback-mode pgvector.")
    conn = connect(args.database_url)
    try:
        gpu_snapshot = gpu_metadata()
        if args.setup:
            setup_schema(conn, args.embedding_dim)
            if args.reset_documents:
                reset_documents(conn)
            seed_documents(conn, args.seed_rows, args.seed_workload)
        if args.writeback_mode == "pgvector":
            current_dim = embedding_vector_column_dim(conn)
            if current_dim != args.embedding_dim:
                raise SystemExit(
                    "document_embeddings.embedding_vector is "
                    f"vector({current_dim}); rerun with --setup or choose --embedding-dim {current_dim}."
                )
        db_metadata = database_metadata(conn)
        current_vector_dim = embedding_vector_column_dim(conn)

        operator_sql_name = "AI_COMPLETE" if args.operator == "ai_complete" else "AI_EMBED"
        output_table = "document_completions" if args.operator == "ai_complete" else "document_embeddings"
        job_id = create_job(conn, operator_sql_name, output_table)
        actors = []
        ray_module = None
        remote_embed = None
        if args.executor in {"ray_actor", "ray_task"}:
            ray_module = require_ray()
            ray_module.init(ignore_reinit_error=True, runtime_env=ray_runtime_env())
            if args.executor == "ray_actor":
                if args.operator == "ai_complete" and model_backend == "fake":
                    RayCompletionActor = ray_module.remote(FakeCompletionActor)
                    actors = [RayCompletionActor.remote(args.completion_max_tokens) for _ in range(args.model_workers)]
                elif args.operator == "ai_complete":
                    actor_cls = OllamaCompletionActor if model_backend == "ollama" else CompatibleHTTPCompletionActor
                    RayCompletionActor = ray_module.remote(actor_cls)
                    actors = [
                        RayCompletionActor.remote(
                            endpoint_urls[index % len(endpoint_urls)],
                            model_name,
                            api_key,
                            request_timeout_s,
                            args.completion_max_tokens,
                        )
                        for index in range(args.model_workers)
                    ]
                elif model_backend == "fake":
                    RayEmbeddingActor = ray_module.remote(FakeEmbeddingActor)
                    actors = [RayEmbeddingActor.remote(args.embedding_dim) for _ in range(args.model_workers)]
                else:
                    RayEmbeddingActor = ray_module.remote(CompatibleHTTPEmbeddingActor)
                    actors = [
                        RayEmbeddingActor.remote(
                            endpoint_urls[index % len(endpoint_urls)],
                            model_name,
                            api_key,
                            request_timeout_s,
                        )
                        for index in range(args.model_workers)
                    ]
            else:
                if args.operator == "ai_complete" and model_backend == "fake":
                    remote_embed = ray_module.remote(fake_complete_batch)
                elif args.operator == "ai_complete" and model_backend == "ollama":
                    remote_embed = ray_module.remote(ollama_complete_batch)
                elif args.operator == "ai_complete":
                    remote_embed = ray_module.remote(compatible_http_complete_batch)
                elif model_backend == "fake":
                    remote_embed = ray_module.remote(fake_embed_batch)
                else:
                    remote_embed = ray_module.remote(compatible_http_embed_batch)

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
            "adaptive_downshifts": 0,
            "adaptive_upshifts": 0,
            "adaptive_limit_mean": 0.0,
        }

        operator_wall_s = 0.0
        vllm_metrics_before = scrape_prometheus_metrics(args.model_metrics_url) if args.model_metrics_url else {}
        if args.data_source == "daft_postgres" or args.organizer == "daft":
            configure_daft_runner(args.daft_runner)
        source = make_source(args.data_source)
        organizer_config = OrganizerConfig(
            batch_size=1 if args.strategy == "fine" else args.ray_batch_rows,
            partition_mode=args.organizer_partition_mode,
            partitions=args.organizer_partitions,
            runner=args.daft_runner,
            batching_policy=args.batching_policy,
            token_budget=args.token_budget,
            completion_max_tokens=args.completion_max_tokens if args.operator == "ai_complete" else 0,
        )
        organizer = make_organizer(args.organizer, organizer_config)
        organizer_metrics = {
            "organizer_from_arrow_s": 0.0,
            "organizer_plan_s": 0.0,
            "organizer_collect_s": 0.0,
            "partition_effective": True,
            "organization_policy_family": "none",
            "batch_prompt_token_spread_mean": 0.0,
            "prefix_group_ratio": 0.0,
        }
        adaptive_config = None
        if args.scheduling_policy == "queue_adaptive":
            adaptive_config = {
                "metrics_url": args.model_metrics_url,
                "min_inflight": args.adaptive_min_inflight,
                "max_inflight": args.adaptive_max_inflight,
                "queue_threshold": args.adaptive_queue_threshold,
                "running_threshold": args.adaptive_running_threshold,
                "kv_threshold": args.adaptive_kv_threshold,
                "poll_interval_s": args.adaptive_poll_interval_s,
            }
        organizer_warnings = []
        offset = 0
        while processed_rows < args.total_rows:
            source_config = SourceConfig(
                limit=args.db_fetch_rows,
                offset=offset,
                workload_name=args.source_workload_name,
                order=args.source_order,
            )
            if args.data_source == "arrow_postgres":
                source_batch = source.fetch(conn, source_config)
            else:
                source_batch = source.fetch(args.database_url, source_config)
            table = source_batch.table
            fetch_metrics = source_batch.metrics
            if table is None:
                break
            db_fetch_s += fetch_metrics["db_fetch_s"]
            arrow_build_s += fetch_metrics["arrow_build_s"]
            offset += table.num_rows
            remaining = args.total_rows - processed_rows
            if table.num_rows > remaining:
                table = table.slice(0, remaining)
            organized = organizer.organize(table)
            ray_batches = organized.batches
            organizer_metrics["organizer_from_arrow_s"] += float(organized.metrics["organizer_from_arrow_s"])
            organizer_metrics["organizer_plan_s"] += float(organized.metrics["organizer_plan_s"])
            organizer_metrics["organizer_collect_s"] += float(organized.metrics["organizer_collect_s"])
            organizer_metrics["organization_policy_family"] = str(
                organized.metrics.get("organization_policy_family", "none")
            )
            organizer_metrics["batch_prompt_token_spread_mean"] += float(
                organized.metrics.get("batch_prompt_token_spread_mean", 0.0)
            )
            organizer_metrics["prefix_group_ratio"] = max(
                float(organizer_metrics["prefix_group_ratio"]),
                float(organized.metrics.get("prefix_group_ratio", 0.0)),
            )
            organizer_metrics["partition_effective"] = (
                bool(organizer_metrics["partition_effective"])
                and str(organized.metrics["partition_effective"]) == "true"
            )
            if organized.metrics["warnings"]:
                organizer_warnings.append(str(organized.metrics["warnings"]))
            object_count += len(ray_batches)
            operator_timer = StageTimer.start("operator_wall")
            if args.executor == "ray_actor":
                method_name = "complete" if args.operator == "ai_complete" else "embed"
                results, metrics = submit_with_backpressure(
                    ray_module, actors, ray_batches, args.max_inflight, method_name, adaptive_config
                )
            elif args.executor == "ray_task":
                results, metrics = submit_ray_tasks(
                    ray_module,
                    remote_embed,
                    ray_batches,
                    args.max_inflight,
                    args.operator,
                    args.embedding_dim,
                    model_backend,
                    endpoint_urls,
                    model_name,
                    api_key,
                    request_timeout_s,
                    args.completion_max_tokens,
                    adaptive_config,
                )
            else:
                if model_backend == "fake":
                    if args.operator == "ai_complete":
                        results, metrics = submit_python_completion_batches(ray_batches, args.completion_max_tokens)
                    else:
                        results, metrics = submit_python_batches(ray_batches, args.embedding_dim)
                else:
                    results, metrics = submit_python_compatible_http_batches(
                        ray_batches,
                        args.operator,
                        endpoint_urls,
                        model_name,
                        api_key,
                        request_timeout_s,
                        args.completion_max_tokens,
                        model_backend,
                    )
            operator_wall_s += operator_timer.stop()
            operator_results.extend(results)
            for key in submit_metrics:
                if key == "max_inflight":
                    submit_metrics[key] = max(submit_metrics[key], metrics[key])
                elif key in {"adaptive_limit_mean"}:
                    submit_metrics[key] = max(submit_metrics[key], metrics[key])
                else:
                    submit_metrics[key] += metrics[key]
            processed_rows += table.num_rows

        vllm_metrics_after = scrape_prometheus_metrics(args.model_metrics_url) if args.model_metrics_url else {}
        write_timer = StageTimer.start("writeback")
        if args.operator == "ai_complete":
            written_rows = write_completions(conn, operator_results, args.writeback_mode, args.write_batch_rows)
        else:
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
        batch_stats = batch_result_stats(operator_results)
        vllm_stats = vllm_metric_delta_stats(vllm_metrics_before, vllm_metrics_after)

        return {
            "status": "ok",
            "experiment_id": args.experiment_id,
            "phase": phase,
            "repeat_index": repeat_index,
            **db_metadata,
            **gpu_snapshot,
            "database_trigger": "job_table",
            "job_id": job_id,
            "operator": args.operator,
            "seed_workload": args.seed_workload,
            "executor": args.executor,
            "strategy": args.strategy,
            "data_source": args.data_source,
            "source_workload_name": args.source_workload_name or "",
            "source_order": args.source_order,
            "organizer": args.organizer,
            "organizer_partition_mode": args.organizer_partition_mode,
            "organizer_partitions": args.organizer_partitions,
            "daft_runner": args.daft_runner if args.organizer == "daft" else "",
            "organizer_partition_effective": str(organizer_metrics["partition_effective"]).lower(),
            "model_backend": model_backend,
            "model_endpoint_url": endpoint_url_label,
            "model_name": model_name,
            "model_request_timeout_s": request_timeout_s,
            "total_rows": processed_rows,
            "written_rows": written_rows,
            "db_fetch_rows": args.db_fetch_rows,
            "ray_batch_rows": args.ray_batch_rows,
            "batching_policy": args.batching_policy,
            "token_budget": args.token_budget,
            "embedding_dim": args.embedding_dim,
            "embedding_vector_dim": current_vector_dim if current_vector_dim is not None else "",
            "completion_max_tokens": args.completion_max_tokens if args.operator == "ai_complete" else "",
            "model_workers": args.model_workers,
            "max_inflight_limit": args.max_inflight,
            "scheduling_policy": args.scheduling_policy,
            "adaptive_min_inflight": args.adaptive_min_inflight if args.scheduling_policy == "queue_adaptive" else 0,
            "adaptive_max_inflight": args.adaptive_max_inflight if args.scheduling_policy == "queue_adaptive" else 0,
            "adaptive_downshifts": int(submit_metrics["adaptive_downshifts"]),
            "adaptive_upshifts": int(submit_metrics["adaptive_upshifts"]),
            "adaptive_limit_mean": round(float(submit_metrics["adaptive_limit_mean"]), 3),
            "writeback_mode": args.writeback_mode,
            "write_batch_rows": args.write_batch_rows,
            "object_count": object_count,
            "operator_invocations": submit_metrics["operator_invocations"],
            "max_inflight_seen": submit_metrics["max_inflight"],
            "token_count": token_count,
            "batch_rows_min": batch_stats["batch_rows_min"],
            "batch_rows_max": batch_stats["batch_rows_max"],
            "batch_rows_mean": round(float(batch_stats["batch_rows_mean"]), 6),
            "batch_tokens_min": batch_stats["batch_tokens_min"],
            "batch_tokens_max": batch_stats["batch_tokens_max"],
            "batch_tokens_mean": round(float(batch_stats["batch_tokens_mean"]), 6),
            "batch_tokens_p50": round(float(batch_stats["batch_tokens_p50"]), 6),
            "batch_tokens_p95": round(float(batch_stats["batch_tokens_p95"]), 6),
            "batch_service_s_p50": round(float(batch_stats["batch_service_s_p50"]), 6),
            "batch_service_s_p95": round(float(batch_stats["batch_service_s_p95"]), 6),
            "batch_service_s_p99": round(float(batch_stats["batch_service_s_p99"]), 6),
            "vllm_metrics_status": vllm_stats["vllm_metrics_status"],
            "vllm_prompt_tokens_delta": vllm_stats["vllm_prompt_tokens_delta"],
            "vllm_generation_tokens_delta": vllm_stats["vllm_generation_tokens_delta"],
            "vllm_request_success_delta": vllm_stats["vllm_request_success_delta"],
            "vllm_e2e_request_latency_mean_s": round(float(vllm_stats["vllm_e2e_request_latency_mean_s"]), 6),
            "vllm_request_queue_time_mean_s": round(float(vllm_stats["vllm_request_queue_time_mean_s"]), 6),
            "vllm_request_inference_time_mean_s": round(
                float(vllm_stats["vllm_request_inference_time_mean_s"]), 6
            ),
            "vllm_request_prefill_time_mean_s": round(float(vllm_stats["vllm_request_prefill_time_mean_s"]), 6),
            "vllm_request_decode_time_mean_s": round(float(vllm_stats["vllm_request_decode_time_mean_s"]), 6),
            "vllm_num_requests_running_after": vllm_stats["vllm_num_requests_running_after"],
            "vllm_num_requests_waiting_after": vllm_stats["vllm_num_requests_waiting_after"],
            "vllm_kv_cache_usage_perc_after": round(float(vllm_stats["vllm_kv_cache_usage_perc_after"]), 6),
            "db_fetch_s": round(db_fetch_s, 6),
            "arrow_build_s": round(arrow_build_s, 6),
            "source_fetch_s": round(db_fetch_s + arrow_build_s, 6),
            "organizer_from_arrow_s": round(float(organizer_metrics["organizer_from_arrow_s"]), 6),
            "organizer_plan_s": round(float(organizer_metrics["organizer_plan_s"]), 6),
            "organizer_collect_s": round(float(organizer_metrics["organizer_collect_s"]), 6),
            "organization_policy_family": organizer_metrics["organization_policy_family"],
            "batch_prompt_token_spread_mean": round(float(organizer_metrics["batch_prompt_token_spread_mean"]), 3),
            "prefix_group_ratio": round(float(organizer_metrics["prefix_group_ratio"]), 6),
            "organizer_warnings": " | ".join(organizer_warnings),
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
