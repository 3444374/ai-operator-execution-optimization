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
import math
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
    PeriodicSampler,
    StageTimer,
    append_metrics,
    batch_result_stats,
    gpu_metadata,
    percentile,
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
    text_token_count,
)
from src.organizers import OrganizerConfig, configure_daft_runner, make_organizer
from src.scheduling.adaptive_admission import (
    AimdAdmissionController,
    AimdConfig,
    EwmaAimdAdmissionController,
)
from src.scheduling.admission import DynamicAdmissionGate, StaticAdmissionController
from src.scheduling.batching import (
    ArrivalReplayBatcher,
    PendingBatch,
    PendingBatchBuilder,
    ReplayServiceObservation,
    RowArrival,
    SystemReplayClock,
)
from src.scheduling.flush import FixedTimeoutFlush, ImmediateFlush, QueueAdaptiveFlush
from src.scheduling.lifecycle import (
    MonotonicEpochClock,
    RequestLifecycleSeed,
    RequestTraceRow,
    SubmissionServiceTiming,
    build_request_trace_rows,
)
from src.scheduling.models import (
    BatchRequest,
    EndpointSnapshot,
    PayloadEnvelope,
    SubmissionLifecycleEvent,
    TopologySnapshot,
)
from src.scheduling.ray_adapter import RaySubmissionAdapter
from src.scheduling.observations import (
    CachedMetricsObservationProvider,
    NonBlockingMetricsObservationProvider,
    ServiceMetricsSnapshot,
)
from src.scheduling.pid_admission import PidAdmissionController, PidConfig
from src.scheduling.routing import (
    LeastQueuedEndpointRouter,
    PrefixAffinityEndpointRouter,
    RequestPoolRouter,
    RoundRobinEndpointRouter,
)
from src.scheduling.scheduler import SchedulerResult, SynchronousScheduler
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
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
    parser.add_argument(
        "--endpoint-routing",
        choices=["round_robin", "least_queued", "prefix_affinity"],
        default="round_robin",
    )
    parser.add_argument(
        "--pool-routing",
        choices=["none", "request_cost"],
        default="none",
    )
    parser.add_argument(
        "--endpoint-pool-ids",
        default=None,
        help="Comma-separated pool ID per Ray actor or task endpoint.",
    )
    parser.add_argument(
        "--endpoint-gpu-ids",
        default=None,
        help="Comma-separated GPU ID per Ray actor or task endpoint.",
    )
    parser.add_argument(
        "--long-request-token-threshold",
        type=int,
        default=0,
        help="Resolved tuning-workload P75 token cost; required by request_cost pool routing.",
    )
    parser.add_argument(
        "--scheduling-policy",
        choices=["static", "queue_adaptive", "aimd", "ewma_aimd", "pid"],
        default="static",
    )
    parser.add_argument("--adaptive-min-inflight", type=int, default=2)
    parser.add_argument("--adaptive-max-inflight", type=int, default=16)
    parser.add_argument("--adaptive-queue-threshold", type=int, default=0)
    parser.add_argument("--adaptive-running-threshold", type=int, default=128)
    parser.add_argument("--adaptive-kv-threshold", type=float, default=0.85)
    parser.add_argument("--adaptive-poll-interval-s", type=float, default=0.05)
    parser.add_argument("--controller-min-window", type=int, default=None)
    parser.add_argument("--controller-max-window", type=int, default=16)
    parser.add_argument("--controller-initial-window", type=int, default=None)
    parser.add_argument("--adaptive-sample-interval-s", type=float, default=0.25)
    parser.add_argument("--ewma-alpha", type=float, default=0.3)
    parser.add_argument("--pid-proportional-gain", type=float, default=0.5)
    parser.add_argument("--pid-integral-gain", type=float, default=0.1)
    parser.add_argument("--pid-derivative-gain", type=float, default=0.05)
    parser.add_argument(
        "--control-trace-output",
        default=None,
        help="Optional typed adaptive control-trace CSV path.",
    )
    parser.add_argument(
        "--arrival-replay",
        action="store_true",
        help="Replay source arrival_time_s values before Ray submission.",
    )
    parser.add_argument(
        "--arrival-time-scale",
        type=float,
        default=1.0,
        help="Positive multiplier applied to normalized arrival replay offsets.",
    )
    parser.add_argument(
        "--flush-policy",
        choices=["immediate", "fixed_timeout", "queue_adaptive"],
        default="immediate",
    )
    parser.add_argument("--flush-timeout-ms", type=float, default=25.0)
    parser.add_argument("--flush-max-wait-ms", type=float, default=50.0)
    parser.add_argument(
        "--flush-trace-output",
        default=None,
        help="Optional arrival replay flush-trace CSV path.",
    )
    parser.add_argument("--submission-trace-output", default=None)
    parser.add_argument("--resource-trace-output", default=None)
    parser.add_argument("--request-trace-output", default=None)
    parser.add_argument("--request-slo-ms", type=float, default=0.0)
    parser.add_argument("--scenario-id", default="manual")
    parser.add_argument("--random-seed", type=int, default=0)
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
    parser.add_argument(
        "--run-phase",
        choices=["warmup", "formal"],
        default=None,
    )
    parser.add_argument("--run-repeat-index", type=int, default=None)
    parser.add_argument("--experiment-id", default="manual")
    parser.add_argument("--output", default="feasibility/results/postgres_ai_operator_profile.csv")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without connecting to DB.")
    return parser.parse_args(argv)


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


def _batch_envelopes(
    batches: Iterable[pa.RecordBatch | pa.Table],
    job_id: str,
    operator: str,
    completion_max_tokens: int,
) -> list[PayloadEnvelope]:
    envelopes = []
    for index, batch in enumerate(batches):
        request_id = f"{job_id}:batch:{index}"
        prompt_tokens = 0
        if "prompt_tokens" in batch.column_names:
            prompt_tokens = sum(
                int(value.as_py() or 0) for value in batch.column("prompt_tokens")
            )
        prefix_key = ""
        if "prefix_key" in batch.column_names and batch.num_rows:
            prefix_values = {
                str(value.as_py() or "") for value in batch.column("prefix_key")
            }
            if len(prefix_values) == 1:
                prefix_key = prefix_values.pop()
        arrival_times = []
        if "arrival_time_s" in batch.column_names:
            arrival_times = [
                float(value.as_py())
                for value in batch.column("arrival_time_s")
                if value.as_py() is not None
            ]
        oldest_arrival_s = min(arrival_times, default=0.0)
        request = BatchRequest(
            request_id=request_id,
            job_id=job_id,
            operator=operator,
            row_count=batch.num_rows,
            prompt_tokens=prompt_tokens,
            estimated_output_tokens=max(0, completion_max_tokens) * batch.num_rows,
            prefix_key=prefix_key,
            first_arrival_s=oldest_arrival_s,
            oldest_arrival_s=oldest_arrival_s,
            payload_id=request_id,
        )
        envelopes.append(PayloadEnvelope(request=request, payload=batch))
    return envelopes


def _row_arrivals(
    table: pa.Table | pa.RecordBatch,
    completion_max_tokens: int,
) -> list[RowArrival]:
    if "arrival_time_s" not in table.column_names:
        raise ValueError("arrival_time_s column is required for arrival replay")
    previous_arrival_s: float | None = None
    arrivals = []
    for index in range(table.num_rows):
        arrival_value = table.column("arrival_time_s")[index].as_py()
        if (
            not isinstance(arrival_value, (int, float))
            or isinstance(arrival_value, bool)
            or not math.isfinite(arrival_value)
            or arrival_value < 0
        ):
            raise ValueError(
                "arrival_time_s must be present, finite, and non-negative"
            )
        arrival_s = float(arrival_value)
        if previous_arrival_s is not None and arrival_s < previous_arrival_s:
            raise ValueError("arrival_time_s values must be non-decreasing")
        previous_arrival_s = arrival_s

        prompt_tokens = 0
        if "prompt_tokens" in table.column_names:
            prompt_value = table.column("prompt_tokens")[index].as_py()
            prompt_tokens = int(prompt_value or 0)
        prefix_key = ""
        if "prefix_key" in table.column_names:
            prefix_value = table.column("prefix_key")[index].as_py()
            prefix_key = str(prefix_value or "")
        row_value = (
            table.column("doc_id")[index].as_py()
            if "doc_id" in table.column_names
            else index
        )
        arrivals.append(
            RowArrival(
                row_id=str(row_value),
                arrival_s=arrival_s,
                prompt_tokens=prompt_tokens,
                estimated_output_tokens=max(0, completion_max_tokens),
                prefix_key=prefix_key,
                payload_ref=table.slice(index, 1),
            )
        )
    return arrivals


def _arrow_envelope(
    pending: PendingBatch,
    batch_index: int,
    job_id: str,
    operator: str,
) -> PayloadEnvelope:
    payloads = [row.payload_ref for row in pending.rows]
    if not all(
        isinstance(payload, (pa.Table, pa.RecordBatch)) and payload.num_rows == 1
        for payload in payloads
    ):
        raise ValueError("each replay payload_ref must be a one-row Arrow payload")
    payload = pa.concat_tables(
        [
            item
            if isinstance(item, pa.Table)
            else pa.Table.from_batches([item])
            for item in payloads
        ]
    )
    prefix_values = {row.prefix_key for row in pending.rows}
    prefix_key = prefix_values.pop() if len(prefix_values) == 1 else ""
    request_id = f"{job_id}:batch:{batch_index}"
    return PayloadEnvelope(
        request=BatchRequest(
            request_id=request_id,
            job_id=job_id,
            operator=operator,
            row_count=pending.row_count,
            prompt_tokens=pending.prompt_tokens,
            estimated_output_tokens=pending.estimated_output_tokens,
            prefix_key=prefix_key,
            first_arrival_s=pending.rows[0].arrival_s,
            oldest_arrival_s=pending.oldest_arrival_s,
            payload_id=request_id,
        ),
        payload=payload,
    )


def _arrival_replay_envelopes(
    tables: Iterable[pa.Table | pa.RecordBatch],
    args: argparse.Namespace,
    job_id: str,
    operator: str,
    service_observation,
    trace_sink,
    lifecycle_seed_sink=None,
    epoch_clock=None,
) -> Iterable[PayloadEnvelope]:
    completion_max_tokens = (
        args.completion_max_tokens if operator == "ai_complete" else 0
    )

    first_source_arrival_s: float | None = None
    replay_start_epoch_s: float | None = None
    arrival_time_scale = getattr(args, "arrival_time_scale", 1.0)
    replay_clock = getattr(args, "_replay_clock", None) or SystemReplayClock()
    lifecycle_epoch_clock = epoch_clock or MonotonicEpochClock()

    def rows() -> Iterable[RowArrival]:
        nonlocal first_source_arrival_s
        previous_arrival_s: float | None = None
        for table in tables:
            for arrival in _row_arrivals(table, completion_max_tokens):
                if (
                    previous_arrival_s is not None
                    and arrival.arrival_s < previous_arrival_s
                ):
                    raise ValueError(
                        "arrival_time_s values must be non-decreasing across fetch chunks"
                    )
                previous_arrival_s = arrival.arrival_s
                if first_source_arrival_s is None:
                    first_source_arrival_s = arrival.arrival_s
                yield arrival

    policies = {
        "immediate": lambda: ImmediateFlush(),
        "fixed_timeout": lambda: FixedTimeoutFlush(
            timeout_s=args.flush_timeout_ms / 1000.0
        ),
        "queue_adaptive": lambda: QueueAdaptiveFlush(
            min_wait_s=args.flush_timeout_ms / 1000.0,
            max_wait_s=args.flush_max_wait_ms / 1000.0,
            pressure_running=args.max_inflight,
        ),
    }
    try:
        flush_policy = policies[args.flush_policy]()
    except KeyError as exc:
        raise ValueError(f"unsupported flush policy: {args.flush_policy}") from exc

    def observe() -> ReplayServiceObservation:
        if args.flush_policy != "queue_adaptive":
            return ReplayServiceObservation(
                fresh=False,
                running=None,
                waiting=None,
                kv_usage=None,
            )
        if hasattr(service_observation, "latest"):
            observation = service_observation.latest(0)
            return ReplayServiceObservation(
                fresh=observation.fresh,
                running=observation.running,
                waiting=observation.waiting,
                kv_usage=observation.kv_usage,
            )
        return service_observation()

    batch_index = 0

    def close_batch(pending: PendingBatch) -> PayloadEnvelope:
        nonlocal batch_index
        envelope = _arrow_envelope(
            pending,
            batch_index=batch_index,
            job_id=str(job_id),
            operator=operator,
        )
        if lifecycle_seed_sink is not None:
            if replay_start_epoch_s is None or first_source_arrival_s is None:
                raise RuntimeError("replay epoch origin is not initialized")
            flush_epoch_s = lifecycle_epoch_clock()
            seeds = [
                RequestLifecycleSeed(
                    request_id=f"{job_id}:row:{row.row_id}",
                    submission_id=envelope.request.request_id,
                    doc_id=row.row_id,
                    prompt_tokens=row.prompt_tokens,
                    estimated_output_tokens=row.estimated_output_tokens,
                    prefix_key=row.prefix_key,
                    arrival_epoch_s=(
                        replay_start_epoch_s
                        + (row.arrival_s - first_source_arrival_s)
                        * arrival_time_scale
                    ),
                    flush_epoch_s=flush_epoch_s,
                )
                for row in pending.rows
            ]
            for seed in seeds:
                if callable(lifecycle_seed_sink):
                    lifecycle_seed_sink(seed)
                else:
                    lifecycle_seed_sink.append(seed)
        batch_index += 1
        return envelope

    token_budget = args.token_budget if args.batching_policy == "token_budget" else 0
    max_rows = (
        1
        if getattr(args, "strategy", "coalesced") == "fine"
        else args.ray_batch_rows
    )
    batcher = ArrivalReplayBatcher(
        rows=rows(),
        builder_factory=lambda: PendingBatchBuilder(
            max_rows=max_rows,
            token_budget=token_budget,
        ),
        flush_policy=flush_policy,
        close_batch=close_batch,
        service_observation=observe,
        clock=replay_clock,
        arrival_time_scale=arrival_time_scale,
    )

    def replay() -> Iterable[PayloadEnvelope]:
        nonlocal replay_start_epoch_s
        replay_start_epoch_s = lifecycle_epoch_clock()
        try:
            yield from batcher
        finally:
            for event in batcher.trace:
                if callable(trace_sink):
                    trace_sink(event)
                else:
                    trace_sink.append(event)

    return replay()


def _endpoint_topology(
    endpoint_ids: list[str],
    endpoint_urls: list[str],
    *,
    pool_ids: list[str] | None = None,
    gpu_ids: list[str] | None = None,
) -> TopologySnapshot:
    if len(endpoint_ids) != len(endpoint_urls):
        raise ValueError("endpoint_ids and endpoint_urls must have the same length")
    resolved_pool_ids = pool_ids or ["default"] * len(endpoint_ids)
    resolved_gpu_ids = gpu_ids or ["0"] * len(endpoint_ids)
    if len(resolved_pool_ids) != len(endpoint_ids):
        raise ValueError("pool_ids and endpoint_ids must have the same length")
    if len(resolved_gpu_ids) != len(endpoint_ids):
        raise ValueError("gpu_ids and endpoint_ids must have the same length")
    observed_at_s = time.monotonic()
    endpoints = tuple(
        EndpointSnapshot(
            endpoint_id=endpoint_id,
            url=endpoint_url,
            pool_id=pool_id,
            gpu_id=gpu_id,
            healthy=True,
            running=0,
            waiting=0,
            kv_usage=None,
            observed_at_s=observed_at_s,
        )
        for endpoint_id, endpoint_url, pool_id, gpu_id in zip(
            endpoint_ids,
            endpoint_urls,
            resolved_pool_ids,
            resolved_gpu_ids,
        )
    )
    return TopologySnapshot(endpoints=endpoints, observed_at_s=observed_at_s)


def _scheduler_metrics(result: SchedulerResult) -> dict:
    return {
        "operator_invocations": result.operator_invocations,
        "max_inflight": result.max_inflight_seen,
        "bounded_wait_s": result.bounded_wait_s,
        "avg_bounded_wait_s": result.avg_bounded_wait_s,
        "fanin_s": result.fanin_s,
        "submit_s": result.submit_s,
        "adaptive_downshifts": 0,
        "adaptive_upshifts": 0,
        "adaptive_limit_mean": result.applied_limit,
    }


def _service_metrics_snapshot(
    metrics_url: str,
) -> ServiceMetricsSnapshot | None:
    metrics = scrape_prometheus_metrics(metrics_url, timeout_s=1.0)
    if not metrics:
        return None

    def optional_int(name: str) -> int | None:
        return int(metrics[name]) if name in metrics else None

    return ServiceMetricsSnapshot(
        running=optional_int("vllm:num_requests_running"),
        waiting=optional_int("vllm:num_requests_waiting"),
        kv_usage=metrics.get("vllm:kv_cache_usage_perc"),
    )


def _build_adaptive_config(
    *,
    scheduling_policy: str,
    metrics_url: str | None,
    trace_events: list,
    min_window: int,
    max_window: int,
    initial_window: int,
    sample_interval_s: float,
    ewma_alpha: float,
    pid_proportional_gain: float,
    pid_integral_gain: float,
    pid_derivative_gain: float,
) -> dict:
    if not metrics_url:
        raise ValueError("adaptive scheduling requires a model metrics URL")
    if scheduling_policy in {"aimd", "ewma_aimd"}:
        config = AimdConfig(min_window=min_window, max_window=max_window)
        if scheduling_policy == "aimd":
            controller = AimdAdmissionController(config, initial_window)
        else:
            controller = EwmaAimdAdmissionController(
                config,
                initial_window,
                alpha=ewma_alpha,
            )
    elif scheduling_policy == "pid":
        controller = PidAdmissionController(
            PidConfig(
                min_window=min_window,
                max_window=max_window,
                proportional_gain=pid_proportional_gain,
                integral_gain=pid_integral_gain,
                derivative_gain=pid_derivative_gain,
            ),
            initial_window,
        )
    else:
        raise ValueError(f"unsupported typed adaptive policy: {scheduling_policy}")
    provider = CachedMetricsObservationProvider(
        lambda: _service_metrics_snapshot(metrics_url),
        min_sample_interval_s=sample_interval_s,
    )
    gate = DynamicAdmissionGate(
        controller,
        provider,
        trace_sink=trace_events.append,
    )
    return {
        "admission_gate": gate,
        "trace_events": trace_events,
        "controller_name": scheduling_policy,
        "min_window": min_window,
        "max_window": max_window,
    }


def _build_routing_config(
    *,
    endpoint_count: int,
    endpoint_routing: str,
    pool_routing: str,
    pool_ids_text: str | None,
    gpu_ids_text: str | None,
    long_request_tokens: int,
) -> dict:
    if endpoint_count <= 0:
        raise ValueError("endpoint_count must be positive")

    def assignments(
        text: str | None,
        default: str,
        label: str,
    ) -> list[str]:
        values = (
            [value.strip() for value in text.split(",") if value.strip()]
            if text
            else [default] * endpoint_count
        )
        if len(values) != endpoint_count:
            raise ValueError(
                f"{label} count must equal endpoint/actor count {endpoint_count}"
            )
        return values

    endpoint_routers = {
        "round_robin": RoundRobinEndpointRouter,
        "least_queued": LeastQueuedEndpointRouter,
        "prefix_affinity": PrefixAffinityEndpointRouter,
    }
    if endpoint_routing not in endpoint_routers:
        raise ValueError(f"unsupported endpoint routing: {endpoint_routing}")
    pool_ids = assignments(pool_ids_text, "default", "pool IDs")
    gpu_ids = assignments(gpu_ids_text, "0", "GPU IDs")
    if pool_routing == "none":
        if any(pool_id != "default" for pool_id in pool_ids):
            raise ValueError(
                "non-default pool IDs require request_cost pool routing"
            )
        pool_router = None
    elif pool_routing == "request_cost":
        pool_router = RequestPoolRouter(long_request_tokens)
    else:
        raise ValueError(f"unsupported pool routing: {pool_routing}")
    return {
        "endpoint_router": endpoint_routers[endpoint_routing](),
        "pool_router": pool_router,
        "pool_ids": pool_ids,
        "gpu_ids": gpu_ids,
        "endpoint_routing": endpoint_routing,
        "pool_routing": pool_routing,
        "long_request_tokens": long_request_tokens,
    }


def _write_control_trace(
    output_path: Path,
    *,
    experiment_id: str,
    phase: str,
    repeat_index: int,
    job_id: int,
    server_version: str,
    pgvector_version: str,
    controller_name: str,
    trace_events: list,
) -> None:
    first_observed_at_s = (
        trace_events[0].observed_at_s if trace_events else 0.0
    )
    for trace_index, event in enumerate(trace_events):
        append_metrics(
            output_path,
            {
                "schema_version": 1,
                "experiment_id": experiment_id,
                "phase": phase,
                "repeat_index": repeat_index,
                "job_id": job_id,
                "server_version": server_version,
                "pgvector_version": pgvector_version,
                "controller": controller_name,
                "trace_index": trace_index,
                "elapsed_s": event.observed_at_s - first_observed_at_s,
                "fresh": event.fresh,
                "inflight": event.inflight,
                "k_max": event.window,
                "running": event.running if event.running is not None else "",
                "waiting": event.waiting if event.waiting is not None else "",
                "kv_usage": event.kv_usage if event.kv_usage is not None else "",
                "controller_action": event.controller_action,
                "reason": event.reason,
                "allowed": event.allowed,
            },
        )


def _write_flush_trace(
    output_path: Path,
    *,
    experiment_id: str,
    phase: str,
    repeat_index: int,
    job_id: int,
    server_version: str,
    pgvector_version: str,
    flush_policy: str,
    flush_timeout_ms: float,
    flush_max_wait_ms: float,
    arrival_time_scale: float,
    trace_events: list,
) -> None:
    for trace_index, event in enumerate(trace_events):
        append_metrics(
            output_path,
            {
                "schema_version": 2,
                "experiment_id": experiment_id,
                "phase": phase,
                "repeat_index": repeat_index,
                "job_id": job_id,
                "server_version": server_version,
                "pgvector_version": pgvector_version,
                "flush_policy": flush_policy,
                "flush_timeout_ms": flush_timeout_ms,
                "flush_max_wait_ms": flush_max_wait_ms,
                "arrival_time_scale": arrival_time_scale,
                "trace_index": trace_index,
                "elapsed_s": event.elapsed_s,
                "pending_rows": event.pending_rows,
                "pending_tokens": event.pending_tokens,
                "oldest_age_s": event.oldest_age_s,
                "action": event.action,
                "reason": event.reason,
                "selected_wait_s": event.selected_wait_s,
                "window_reason": event.window_reason,
            },
        )


def _write_submission_trace(
    output_path: Path,
    *,
    experiment_id: str,
    phase: str,
    repeat_index: int,
    job_id: int,
    server_version: str,
    pgvector_version: str,
    results: list[dict],
) -> None:
    for submission_index, result in enumerate(results):
        append_metrics(
            output_path,
            {
                "schema_version": 1,
                "experiment_id": experiment_id,
                "phase": phase,
                "repeat_index": repeat_index,
                "job_id": job_id,
                "server_version": server_version,
                "pgvector_version": pgvector_version,
                "submission_index": submission_index,
                "doc_ids": ";".join(str(item) for item in result.get("doc_id", [])),
                "rows": result.get("rows", 0),
                "token_count": result.get("token_count", 0),
                "input_token_count": result.get("input_token_count", 0),
                "output_token_count": result.get("output_token_count", 0),
                "service_s": result.get("service_s", 0.0),
                "service_start_epoch_s": result.get("service_start_epoch_s", 0.0),
                "service_end_epoch_s": result.get("service_end_epoch_s", 0.0),
            },
        )


def _write_request_trace(
    output_path: Path,
    *,
    experiment_id: str,
    phase: str,
    repeat_index: int,
    scenario_id: str,
    random_seed: int,
    job_id: int,
    server_version: str,
    pgvector_version: str,
    rows: list[RequestTraceRow] | tuple[RequestTraceRow, ...],
) -> None:
    for request_index, row in enumerate(rows):
        append_metrics(
            output_path,
            {
                "schema_version": 1,
                "experiment_id": experiment_id,
                "phase": phase,
                "repeat_index": repeat_index,
                "scenario_id": scenario_id,
                "random_seed": random_seed,
                "job_id": job_id,
                "server_version": server_version,
                "pgvector_version": pgvector_version,
                "request_index": request_index,
                "request_id": row.request_id,
                "submission_id": row.submission_id,
                "doc_id": row.doc_id,
                "pool_id": row.pool_id,
                "endpoint_id": row.endpoint_id,
                "gpu_id": row.gpu_id,
                "prompt_tokens": row.prompt_tokens,
                "estimated_output_tokens": row.estimated_output_tokens,
                "client_estimated_output_tokens": (
                    row.client_estimated_output_tokens
                    if row.client_estimated_output_tokens is not None
                    else ""
                ),
                "actual_output_tokens": (
                    row.actual_output_tokens
                    if row.actual_output_tokens is not None
                    else ""
                ),
                "output_token_source": row.output_token_source,
                "total_tokens": (
                    row.total_tokens if row.total_tokens is not None else ""
                ),
                "prefix_key": row.prefix_key,
                "status": row.status,
                "error_type": row.error_type,
                "arrival_epoch_s": row.arrival_epoch_s,
                "flush_epoch_s": row.flush_epoch_s,
                "submit_epoch_s": row.submit_epoch_s,
                "service_start_epoch_s": (
                    row.service_start_epoch_s
                    if row.service_start_epoch_s is not None
                    else ""
                ),
                "completion_epoch_s": row.completion_epoch_s,
                "buffer_s": row.buffer_s,
                "submit_to_service_s": (
                    row.submit_to_service_s
                    if row.submit_to_service_s is not None
                    else ""
                ),
                "service_s": row.service_s if row.service_s is not None else "",
                "service_clock_domain": row.service_clock_domain,
                "e2e_s": row.e2e_s,
                "latency_granularity": row.latency_granularity,
                "slo_target_s": (
                    row.slo_target_s if row.slo_target_s is not None else ""
                ),
                "slo_met": row.slo_met if row.slo_met is not None else "",
            },
        )


def _build_profiler_request_rows(
    seeds: list[RequestLifecycleSeed],
    submission_events: list[SubmissionLifecycleEvent],
    results: list[dict],
    *,
    operator: str,
    slo_target_s: float | None,
) -> tuple[RequestTraceRow, ...]:
    if len(results) != len(submission_events):
        raise ValueError(
            "operator results and submission lifecycle events must align"
        )

    service_by_submission_id = {}
    client_estimated_output_tokens_by_doc_id = {}
    for event, result in zip(submission_events, results):
        if event.status == "failed":
            service_by_submission_id[event.submission_id] = (
                SubmissionServiceTiming(event.submission_id, None, None)
            )
            continue
        if not isinstance(result, dict):
            raise ValueError("completed submission result must be a mapping")
        service_by_submission_id[event.submission_id] = SubmissionServiceTiming(
            event.submission_id,
            float(result["service_start_epoch_s"]),
            float(result["service_end_epoch_s"]),
        )
        doc_ids = [str(value) for value in result.get("doc_id", [])]
        if operator == "ai_complete":
            outputs = result.get("output_text", [])
            if len(outputs) != len(doc_ids):
                raise ValueError(
                    "backend output count must match doc_id count"
                )
            output_counts = [
                text_token_count(str(output)) if str(output).strip() else 0
                for output in outputs
            ]
        else:
            output_counts = [0] * len(doc_ids)
        for doc_id, output_count in zip(doc_ids, output_counts):
            if doc_id in client_estimated_output_tokens_by_doc_id:
                raise ValueError(f"duplicate backend doc_id: {doc_id}")
            client_estimated_output_tokens_by_doc_id[doc_id] = output_count

    return build_request_trace_rows(
        seeds,
        submission_events,
        service_by_submission_id,
        client_estimated_output_tokens_by_doc_id,
        {},
        slo_target_s=slo_target_s,
    )


def _request_trace_metrics(
    rows: tuple[RequestTraceRow, ...] | list[RequestTraceRow],
    *,
    e2e_s: float,
) -> dict[str, float | str]:
    successful_e2e = [
        row.e2e_s for row in rows if row.status == "completed"
    ]
    slo_enabled = any(row.slo_target_s is not None for row in rows)
    slo_met_count = sum(row.slo_met is True for row in rows)
    violation_ratio = (
        sum(row.slo_met is not True for row in rows) / len(rows)
        if rows and slo_enabled
        else 0.0
    )
    granularities = {row.latency_granularity for row in rows}
    if len(granularities) > 1:
        raise ValueError("request trace contains mixed latency granularities")
    return {
        "request_e2e_s_p50": percentile(successful_e2e, 50),
        "request_e2e_s_p95": percentile(successful_e2e, 95),
        "request_e2e_s_p99": percentile(successful_e2e, 99),
        "request_slo_violation_ratio": violation_ratio,
        "request_slo_goodput_per_s": (
            slo_met_count / e2e_s if slo_enabled and e2e_s > 0 else 0.0
        ),
        "latency_granularity": next(iter(granularities), ""),
    }


def _write_resource_trace(
    output_path: Path,
    *,
    experiment_id: str,
    phase: str,
    repeat_index: int,
    job_id: int,
    server_version: str,
    pgvector_version: str,
    samples: list[dict],
) -> None:
    for sample in samples:
        append_metrics(
            output_path,
            {
                "schema_version": 1,
                "experiment_id": experiment_id,
                "phase": phase,
                "repeat_index": repeat_index,
                "job_id": job_id,
                "server_version": server_version,
                "pgvector_version": pgvector_version,
                **sample,
            },
        )


def _resource_snapshot(metrics_url: str | None) -> dict[str, object]:
    gpu = gpu_metadata()
    metrics = (
        scrape_prometheus_metrics(metrics_url, timeout_s=0.5)
        if metrics_url
        else {}
    )
    return {
        **gpu,
        "vllm_metrics_status": "ok" if metrics else "unavailable",
        "vllm_num_requests_running": int(
            metrics.get("vllm:num_requests_running", 0.0)
        ),
        "vllm_num_requests_waiting": int(
            metrics.get("vllm:num_requests_waiting", 0.0)
        ),
        "vllm_kv_cache_usage_perc": metrics.get(
            "vllm:kv_cache_usage_perc", 0.0
        ),
    }


def _run_static_scheduler(
    ray_module,
    envelopes: Iterable[PayloadEnvelope],
    topology: TopologySnapshot,
    submitters: dict,
    max_inflight: int,
    routing_config: dict | None = None,
    submission_lifecycle_sink: list[SubmissionLifecycleEvent] | None = None,
    epoch_clock=None,
) -> tuple[list[dict], dict]:
    return _run_scheduler(
        ray_module,
        envelopes,
        topology,
        submitters,
        StaticAdmissionController(max_inflight),
        routing_config,
        submission_lifecycle_sink,
        epoch_clock,
    )


def _run_scheduler(
    ray_module,
    envelopes: Iterable[PayloadEnvelope],
    topology: TopologySnapshot,
    submitters: dict,
    admission,
    routing_config: dict | None = None,
    submission_lifecycle_sink: list[SubmissionLifecycleEvent] | None = None,
    epoch_clock=None,
) -> tuple[list[dict], dict]:
    routing_config = routing_config or {}
    scheduler = SynchronousScheduler(
        admission=admission,
        router=routing_config.get("endpoint_router", RoundRobinEndpointRouter()),
        adapter=RaySubmissionAdapter(ray_module, submitters),
        pool_id="default",
        pool_router=routing_config.get("pool_router"),
        epoch_clock=epoch_clock or time.time,
    )
    result = scheduler.run(envelopes, topology)
    if submission_lifecycle_sink is not None:
        submission_lifecycle_sink.extend(result.submission_events)
    return [completion.result for completion in result.completions], _scheduler_metrics(result)


def _run_dynamic_scheduler(
    ray_module,
    envelopes: Iterable[PayloadEnvelope],
    topology: TopologySnapshot,
    submitters: dict,
    adaptive_config: dict,
    routing_config: dict | None = None,
    submission_lifecycle_sink: list[SubmissionLifecycleEvent] | None = None,
    epoch_clock=None,
) -> tuple[list[dict], dict]:
    trace_events = adaptive_config["trace_events"]
    trace_start = len(trace_events)
    results, metrics = _run_scheduler(
        ray_module,
        envelopes,
        topology,
        submitters,
        adaptive_config["admission_gate"],
        routing_config,
        submission_lifecycle_sink,
        epoch_clock,
    )
    new_events = trace_events[trace_start:]
    metrics["adaptive_downshifts"] = sum(
        event.controller_action == "decrease" for event in new_events
    )
    metrics["adaptive_upshifts"] = sum(
        event.controller_action == "increase" for event in new_events
    )
    metrics["adaptive_limit_mean"] = (
        statistics.mean(event.window for event in new_events)
        if new_events
        else adaptive_config["admission_gate"].limit
    )
    return results, metrics


def submit_with_backpressure(
    ray_module,
    actors: list,
    batches: Iterable[pa.RecordBatch | pa.Table],
    max_inflight: int,
    method_name: str,
    adaptive_config: dict | None = None,
    routing_config: dict | None = None,
    replay_envelopes: Iterable[PayloadEnvelope] | None = None,
    submission_lifecycle_sink: list[SubmissionLifecycleEvent] | None = None,
    epoch_clock=None,
) -> tuple[list[dict], dict]:
    typed_adaptive = (
        adaptive_config is not None and "admission_gate" in adaptive_config
    )
    if adaptive_config is not None and not typed_adaptive:
        if submission_lifecycle_sink is not None:
            raise ValueError("request tracing requires the typed scheduler")
        replay_batches = (
            (envelope.payload for envelope in replay_envelopes)
            if replay_envelopes is not None
            else batches
        )
        return _submit_with_backpressure_legacy_adaptive(
            ray_module,
            actors,
            replay_batches,
            max_inflight,
            method_name,
            adaptive_config,
        )
    if not actors:
        raise ValueError("actors must not be empty")

    operator = "ai_complete" if "complete" in method_name else "ai_embed"
    envelopes = (
        replay_envelopes
        if replay_envelopes is not None
        else _batch_envelopes(
            batches,
            job_id="ray-actor",
            operator=operator,
            completion_max_tokens=0,
        )
    )
    endpoint_ids = [f"actor-{index}" for index in range(len(actors))]
    topology = _endpoint_topology(
        endpoint_ids,
        [f"ray://actor/{index}" for index in range(len(actors))],
        pool_ids=(
            routing_config.get("pool_ids") if routing_config is not None else None
        ),
        gpu_ids=(
            routing_config.get("gpu_ids") if routing_config is not None else None
        ),
    )
    submitters = {
        endpoint_id: (
            lambda payload, actor_handle=actor: getattr(
                actor_handle, method_name
            ).remote(payload)
        )
        for endpoint_id, actor in zip(endpoint_ids, actors)
    }
    if typed_adaptive:
        return _run_dynamic_scheduler(
            ray_module,
            envelopes,
            topology,
            submitters,
            adaptive_config,
            routing_config,
            submission_lifecycle_sink,
            epoch_clock,
        )
    return _run_static_scheduler(
        ray_module,
        envelopes,
        topology,
        submitters,
        max_inflight,
        routing_config,
        submission_lifecycle_sink,
        epoch_clock,
    )


def _submit_with_backpressure_legacy_adaptive(
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
    routing_config: dict | None = None,
    replay_envelopes: Iterable[PayloadEnvelope] | None = None,
    submission_lifecycle_sink: list[SubmissionLifecycleEvent] | None = None,
    epoch_clock=None,
) -> tuple[list[dict], dict]:
    typed_adaptive = (
        adaptive_config is not None and "admission_gate" in adaptive_config
    )
    if adaptive_config is not None and not typed_adaptive:
        if submission_lifecycle_sink is not None:
            raise ValueError("request tracing requires the typed scheduler")
        replay_batches = (
            (envelope.payload for envelope in replay_envelopes)
            if replay_envelopes is not None
            else batches
        )
        return _submit_ray_tasks_legacy_adaptive(
            ray_module,
            remote_embed,
            replay_batches,
            max_inflight,
            operator,
            embedding_dim,
            model_backend,
            endpoint_urls,
            model_name,
            api_key,
            timeout_s,
            completion_max_tokens,
            adaptive_config,
        )

    envelopes = (
        replay_envelopes
        if replay_envelopes is not None
        else _batch_envelopes(
            batches,
            job_id="ray-task",
            operator=operator,
            completion_max_tokens=completion_max_tokens
            if operator == "ai_complete"
            else 0,
        )
    )
    if model_backend == "fake":
        endpoint_ids = ["task-0"]
        endpoint_urls_for_topology = ["ray://task/fake"]
        if operator == "ai_embed":
            submitters = {
                "task-0": lambda payload: remote_embed.remote(payload, embedding_dim)
            }
        else:
            submitters = {
                "task-0": lambda payload: remote_embed.remote(payload, completion_max_tokens)
            }
    else:
        if not endpoint_urls:
            raise ValueError("endpoint_urls must not be empty for an HTTP model backend")
        endpoint_ids = [f"task-{index}" for index in range(len(endpoint_urls))]
        endpoint_urls_for_topology = endpoint_urls
        submitters = {}
        for endpoint_id, endpoint_url in zip(endpoint_ids, endpoint_urls):
            if operator == "ai_embed":
                submitters[endpoint_id] = (
                    lambda payload, url=endpoint_url: remote_embed.remote(
                        payload, url, model_name, api_key, timeout_s
                    )
                )
            else:
                submitters[endpoint_id] = (
                    lambda payload, url=endpoint_url: remote_embed.remote(
                        payload,
                        url,
                        model_name,
                        api_key,
                        timeout_s,
                        completion_max_tokens,
                    )
                )
    topology = _endpoint_topology(
        endpoint_ids,
        endpoint_urls_for_topology,
        pool_ids=(
            routing_config.get("pool_ids") if routing_config is not None else None
        ),
        gpu_ids=(
            routing_config.get("gpu_ids") if routing_config is not None else None
        ),
    )
    if typed_adaptive:
        return _run_dynamic_scheduler(
            ray_module,
            envelopes,
            topology,
            submitters,
            adaptive_config,
            routing_config,
            submission_lifecycle_sink,
            epoch_clock,
        )
    return _run_static_scheduler(
        ray_module,
        envelopes,
        topology,
        submitters,
        max_inflight,
        routing_config,
        submission_lifecycle_sink,
        epoch_clock,
    )


def _submit_ray_tasks_legacy_adaptive(
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


def _validate_arrival_replay_args(args: argparse.Namespace) -> None:
    if not args.arrival_replay:
        return
    if (
        isinstance(args.arrival_time_scale, bool)
        or not math.isfinite(args.arrival_time_scale)
        or args.arrival_time_scale <= 0
    ):
        raise SystemExit("--arrival-time-scale must be finite and positive")
    if args.data_source != "daft_postgres":
        raise SystemExit("arrival replay requires --data-source daft_postgres")
    if args.source_order != "arrival_time":
        raise SystemExit("arrival replay requires --source-order arrival_time")
    if args.executor not in {"ray_actor", "ray_task"}:
        raise SystemExit("arrival replay requires a Ray executor")
    if args.batching_policy not in {"fixed_rows", "token_budget"}:
        raise SystemExit(
            "arrival replay rejects offline reordering batching policies"
        )
    if not args.dry_run and args.model_backend == "fake":
        raise SystemExit("arrival replay formal runs require a real model backend")
    if (
        isinstance(args.flush_timeout_ms, bool)
        or not math.isfinite(args.flush_timeout_ms)
        or args.flush_timeout_ms < 0
    ):
        raise SystemExit("--flush-timeout-ms must be finite and non-negative")
    if (
        isinstance(args.flush_max_wait_ms, bool)
        or not math.isfinite(args.flush_max_wait_ms)
        or args.flush_max_wait_ms <= 0
    ):
        raise SystemExit("--flush-max-wait-ms must be finite and positive")
    if (
        args.flush_policy == "queue_adaptive"
        and args.flush_timeout_ms <= 0
    ):
        raise SystemExit(
            "queue-adaptive flush requires --flush-timeout-ms > 0"
        )
    if (
        args.flush_policy == "queue_adaptive"
        and args.flush_max_wait_ms < args.flush_timeout_ms
    ):
        raise SystemExit(
            "queue-adaptive flush requires "
            "--flush-max-wait-ms >= --flush-timeout-ms"
        )


def _validate_request_trace_args(args: argparse.Namespace) -> None:
    if (
        isinstance(args.request_slo_ms, bool)
        or not math.isfinite(args.request_slo_ms)
        or args.request_slo_ms < 0
    ):
        raise SystemExit("--request-slo-ms must be non-negative")
    if not args.scenario_id or not args.scenario_id.strip():
        raise SystemExit("--scenario-id must be non-empty")
    if not args.request_trace_output:
        return
    if not args.arrival_replay:
        raise SystemExit("request tracing requires --arrival-replay")
    if args.scheduling_policy == "queue_adaptive":
        raise SystemExit("request tracing requires the typed scheduler")


def _vllm_tokens_per_second(vllm_stats: dict, e2e_s: float) -> float:
    """Return observed vLLM token throughput for one end-to-end run."""
    if e2e_s <= 0:
        return 0.0
    observed_tokens = (
        float(vllm_stats["vllm_prompt_tokens_delta"])
        + float(vllm_stats["vllm_generation_tokens_delta"])
    )
    return observed_tokens / e2e_s


def run_once(args: argparse.Namespace, phase: str, repeat_index: int) -> dict:
    _validate_request_trace_args(args)
    _validate_arrival_replay_args(args)
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
    typed_adaptive_policies = {"aimd", "ewma_aimd", "pid"}
    if args.scheduling_policy in typed_adaptive_policies:
        if args.executor not in {"ray_actor", "ray_task"}:
            raise SystemExit("typed adaptive scheduling requires a Ray executor")
        if not args.model_metrics_url:
            raise SystemExit("typed adaptive scheduling requires --model-metrics-url")
    if args.executor == "python" and (
        args.endpoint_routing != "round_robin"
        or args.pool_routing != "none"
        or args.endpoint_pool_ids
        or args.endpoint_gpu_ids
    ):
        raise SystemExit("endpoint and pool routing require a Ray executor")
    routing_endpoint_count = (
        args.model_workers
        if args.executor == "ray_actor"
        else (1 if model_backend == "fake" else max(1, len(endpoint_urls)))
    )
    routing_config = None
    if args.executor in {"ray_actor", "ray_task"}:
        try:
            routing_config = _build_routing_config(
                endpoint_count=routing_endpoint_count,
                endpoint_routing=args.endpoint_routing,
                pool_routing=args.pool_routing,
                pool_ids_text=args.endpoint_pool_ids,
                gpu_ids_text=args.endpoint_gpu_ids,
                long_request_tokens=args.long_request_token_threshold,
            )
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
    if args.dry_run:
        return {
            "status": "dry_run",
            "experiment_id": args.experiment_id,
            "phase": phase,
            "repeat_index": repeat_index,
            "scenario_id": args.scenario_id,
            "random_seed": args.random_seed,
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
            "endpoint_routing": args.endpoint_routing,
            "pool_routing": args.pool_routing,
            "endpoint_pool_ids": args.endpoint_pool_ids or "",
            "endpoint_gpu_ids": args.endpoint_gpu_ids or "",
            "long_request_token_threshold": args.long_request_token_threshold,
            "scheduling_policy": args.scheduling_policy,
            "adaptive_min_inflight": args.adaptive_min_inflight,
            "adaptive_max_inflight": args.adaptive_max_inflight,
            "controller_min_window": args.controller_min_window
            if args.controller_min_window is not None
            else (2 if args.scheduling_policy == "pid" else 4),
            "controller_max_window": args.controller_max_window,
            "controller_initial_window": args.controller_initial_window
            if args.controller_initial_window is not None
            else (
                args.controller_min_window
                if args.controller_min_window is not None
                else (2 if args.scheduling_policy == "pid" else 4)
            ),
            "adaptive_sample_interval_s": args.adaptive_sample_interval_s,
            "control_trace_output": args.control_trace_output or "",
            "arrival_replay": args.arrival_replay,
            "arrival_time_scale": args.arrival_time_scale,
            "arrival_replay_preload": (
                "bounded_requested_workload" if args.arrival_replay else ""
            ),
            "flush_policy": args.flush_policy,
            "flush_timeout_ms": args.flush_timeout_ms,
            "flush_max_wait_ms": args.flush_max_wait_ms,
            "flush_trace_output": args.flush_trace_output or "",
            "flush_trace_path": (
                args.flush_trace_output
                or str(
                    Path(args.output).with_name(
                        f"{Path(args.output).stem}_flush_trace.csv"
                    )
                )
                if args.arrival_replay
                else ""
            ),
            "flush_trace_events": 0,
            "submission_trace_path": args.submission_trace_output or "",
            "submission_trace_events": 0,
            "resource_trace_path": args.resource_trace_output or "",
            "resource_trace_events": 0,
            "request_trace_path": args.request_trace_output or "",
            "request_trace_events": 0,
            "request_e2e_s_p50": 0.0,
            "request_e2e_s_p95": 0.0,
            "request_e2e_s_p99": 0.0,
            "request_slo_target_ms": args.request_slo_ms,
            "request_slo_violation_ratio": 0.0,
            "request_slo_goodput_per_s": 0.0,
            "latency_granularity": (
                "submission" if args.request_trace_output else ""
            ),
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
    resource_sampler = None
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
        request_lifecycle_seeds: list[RequestLifecycleSeed] = []
        submission_lifecycle_events: list[SubmissionLifecycleEvent] = []
        request_trace_rows: tuple[RequestTraceRow, ...] = ()
        lifecycle_epoch_clock = (
            MonotonicEpochClock() if args.request_trace_output else None
        )
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
        if args.resource_trace_output:
            resource_sampler = PeriodicSampler(
                lambda: _resource_snapshot(args.model_metrics_url),
                interval_s=0.25,
            )
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
        organizer = (
            None
            if args.arrival_replay
            else make_organizer(args.organizer, organizer_config)
        )
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
        control_trace_events = []
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
        elif args.scheduling_policy in typed_adaptive_policies:
            default_min_window = 2 if args.scheduling_policy == "pid" else 4
            min_window = (
                args.controller_min_window
                if args.controller_min_window is not None
                else default_min_window
            )
            initial_window = (
                args.controller_initial_window
                if args.controller_initial_window is not None
                else min_window
            )
            try:
                adaptive_config = _build_adaptive_config(
                    scheduling_policy=args.scheduling_policy,
                    metrics_url=args.model_metrics_url,
                    trace_events=control_trace_events,
                    min_window=min_window,
                    max_window=args.controller_max_window,
                    initial_window=initial_window,
                    sample_interval_s=args.adaptive_sample_interval_s,
                    ewma_alpha=args.ewma_alpha,
                    pid_proportional_gain=args.pid_proportional_gain,
                    pid_integral_gain=args.pid_integral_gain,
                    pid_derivative_gain=args.pid_derivative_gain,
                )
            except ValueError as exc:
                raise SystemExit(str(exc)) from exc

        def submit_operator_batches(
            batches: Iterable[pa.RecordBatch | pa.Table],
            replay_envelopes: Iterable[PayloadEnvelope] | None = None,
        ) -> tuple[list[dict], dict]:
            if args.executor == "ray_actor":
                method_name = (
                    "complete" if args.operator == "ai_complete" else "embed"
                )
                return submit_with_backpressure(
                    ray_module,
                    actors,
                    batches,
                    args.max_inflight,
                    method_name,
                    adaptive_config,
                    routing_config,
                    replay_envelopes=replay_envelopes,
                    submission_lifecycle_sink=(
                        submission_lifecycle_events
                        if args.request_trace_output
                        else None
                    ),
                    epoch_clock=lifecycle_epoch_clock,
                )
            if args.executor == "ray_task":
                return submit_ray_tasks(
                    ray_module,
                    remote_embed,
                    batches,
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
                    routing_config,
                    replay_envelopes=replay_envelopes,
                    submission_lifecycle_sink=(
                        submission_lifecycle_events
                        if args.request_trace_output
                        else None
                    ),
                    epoch_clock=lifecycle_epoch_clock,
                )
            if replay_envelopes is not None:
                raise RuntimeError("arrival replay requires a Ray executor")
            if model_backend == "fake":
                if args.operator == "ai_complete":
                    return submit_python_completion_batches(
                        batches, args.completion_max_tokens
                    )
                return submit_python_batches(batches, args.embedding_dim)
            return submit_python_compatible_http_batches(
                batches,
                args.operator,
                endpoint_urls,
                model_name,
                api_key,
                request_timeout_s,
                args.completion_max_tokens,
                model_backend,
            )

        organizer_warnings = []
        replay_tables: list[pa.Table] = []
        flush_trace_events = []
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
            if args.arrival_replay:
                replay_tables.append(table)
                processed_rows += table.num_rows
                continue
            if organizer is None:
                raise RuntimeError("non-replay execution requires an organizer")
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
            results, metrics = submit_operator_batches(ray_batches)
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

        if args.arrival_replay:
            flush_observation_provider = None
            if args.flush_policy == "queue_adaptive":
                flush_observation_provider = NonBlockingMetricsObservationProvider(
                    lambda: (
                        _service_metrics_snapshot(args.model_metrics_url)
                        if args.model_metrics_url
                        else None
                    ),
                    poll_interval_s=0.25,
                    stale_after_s=0.5,
                )
            service_observation = flush_observation_provider or (
                lambda: ReplayServiceObservation(
                    fresh=False,
                    running=None,
                    waiting=None,
                    kv_usage=None,
                )
            )
            try:
                replay_envelopes = _arrival_replay_envelopes(
                    replay_tables,
                    args,
                    job_id=str(job_id),
                    operator=args.operator,
                    service_observation=service_observation,
                    trace_sink=flush_trace_events,
                    lifecycle_seed_sink=(
                        request_lifecycle_seeds
                        if args.request_trace_output
                        else None
                    ),
                    epoch_clock=lifecycle_epoch_clock,
                )
                operator_timer = StageTimer.start("operator_wall")
                results, metrics = submit_operator_batches(
                    (),
                    replay_envelopes=replay_envelopes,
                )
                operator_wall_s += operator_timer.stop()
            finally:
                if flush_observation_provider is not None:
                    flush_observation_provider.close()
            operator_results.extend(results)
            object_count = metrics["operator_invocations"]
            for key in submit_metrics:
                if key == "max_inflight":
                    submit_metrics[key] = max(submit_metrics[key], metrics[key])
                elif key == "adaptive_limit_mean":
                    submit_metrics[key] = max(submit_metrics[key], metrics[key])
                else:
                    submit_metrics[key] += metrics[key]

        request_trace_path = args.request_trace_output or ""
        if request_trace_path:
            request_trace_rows = _build_profiler_request_rows(
                request_lifecycle_seeds,
                submission_lifecycle_events,
                operator_results,
                operator=args.operator,
                slo_target_s=(
                    args.request_slo_ms / 1000.0
                    if args.request_slo_ms > 0
                    else None
                ),
            )
            _write_request_trace(
                Path(request_trace_path),
                experiment_id=args.experiment_id,
                phase=phase,
                repeat_index=repeat_index,
                scenario_id=args.scenario_id,
                random_seed=args.random_seed,
                job_id=job_id,
                server_version=db_metadata["server_version"],
                pgvector_version=db_metadata["pgvector_version"],
                rows=request_trace_rows,
            )

        resource_samples = []
        if resource_sampler is not None:
            resource_sampler.close()
            resource_samples = list(resource_sampler.samples)

        submission_trace_path = args.submission_trace_output or ""
        if submission_trace_path:
            _write_submission_trace(
                Path(submission_trace_path),
                experiment_id=args.experiment_id,
                phase=phase,
                repeat_index=repeat_index,
                job_id=job_id,
                server_version=db_metadata["server_version"],
                pgvector_version=db_metadata["pgvector_version"],
                results=operator_results,
            )
        resource_trace_path = args.resource_trace_output or ""
        if resource_trace_path:
            _write_resource_trace(
                Path(resource_trace_path),
                experiment_id=args.experiment_id,
                phase=phase,
                repeat_index=repeat_index,
                job_id=job_id,
                server_version=db_metadata["server_version"],
                pgvector_version=db_metadata["pgvector_version"],
                samples=resource_samples,
            )

        flush_trace_path = ""
        if args.arrival_replay:
            flush_trace_path = args.flush_trace_output
            if not flush_trace_path:
                main_output = Path(args.output)
                flush_trace_path = str(
                    main_output.with_name(
                        f"{main_output.stem}_flush_trace.csv"
                    )
                )
            _write_flush_trace(
                Path(flush_trace_path),
                experiment_id=args.experiment_id,
                phase=phase,
                repeat_index=repeat_index,
                job_id=job_id,
                server_version=db_metadata["server_version"],
                pgvector_version=db_metadata["pgvector_version"],
                flush_policy=args.flush_policy,
                flush_timeout_ms=args.flush_timeout_ms,
                flush_max_wait_ms=args.flush_max_wait_ms,
                arrival_time_scale=args.arrival_time_scale,
                trace_events=flush_trace_events,
            )

        control_trace_path = ""
        if (
            adaptive_config is not None
            and "admission_gate" in adaptive_config
        ):
            trace_events = adaptive_config["trace_events"]
            submit_metrics["adaptive_downshifts"] = sum(
                event.controller_action == "decrease" for event in trace_events
            )
            submit_metrics["adaptive_upshifts"] = sum(
                event.controller_action == "increase" for event in trace_events
            )
            submit_metrics["adaptive_limit_mean"] = (
                statistics.mean(event.window for event in trace_events)
                if trace_events
                else adaptive_config["admission_gate"].limit
            )
            control_trace_path = args.control_trace_output
            if not control_trace_path:
                main_output = Path(args.output)
                control_trace_path = str(
                    main_output.with_name(
                        f"{main_output.stem}_control_trace.csv"
                    )
                )
            _write_control_trace(
                Path(control_trace_path),
                experiment_id=args.experiment_id,
                phase=phase,
                repeat_index=repeat_index,
                job_id=job_id,
                server_version=db_metadata["server_version"],
                pgvector_version=db_metadata["pgvector_version"],
                controller_name=adaptive_config["controller_name"],
                trace_events=trace_events,
            )

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
        request_metrics = _request_trace_metrics(
            request_trace_rows,
            e2e_s=e2e_s,
        )
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
            "scenario_id": args.scenario_id,
            "random_seed": args.random_seed,
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
            "endpoint_routing": args.endpoint_routing,
            "pool_routing": args.pool_routing,
            "endpoint_pool_ids": ";".join(routing_config["pool_ids"])
            if routing_config
            else "",
            "endpoint_gpu_ids": ";".join(routing_config["gpu_ids"])
            if routing_config
            else "",
            "long_request_token_threshold": args.long_request_token_threshold,
            "scheduling_policy": args.scheduling_policy,
            "adaptive_min_inflight": args.adaptive_min_inflight if args.scheduling_policy == "queue_adaptive" else 0,
            "adaptive_max_inflight": args.adaptive_max_inflight if args.scheduling_policy == "queue_adaptive" else 0,
            "controller_min_window": adaptive_config.get("min_window", 0)
            if adaptive_config
            else 0,
            "controller_max_window": adaptive_config.get("max_window", 0)
            if adaptive_config
            else 0,
            "adaptive_sample_interval_s": args.adaptive_sample_interval_s
            if args.scheduling_policy in typed_adaptive_policies
            else 0,
            "adaptive_downshifts": int(submit_metrics["adaptive_downshifts"]),
            "adaptive_upshifts": int(submit_metrics["adaptive_upshifts"]),
            "adaptive_limit_mean": round(float(submit_metrics["adaptive_limit_mean"]), 3),
            "control_trace_path": control_trace_path,
            "control_trace_events": len(control_trace_events),
            "arrival_replay": args.arrival_replay,
            "arrival_time_scale": args.arrival_time_scale,
            "arrival_replay_preload": (
                "bounded_requested_workload" if args.arrival_replay else ""
            ),
            "flush_policy": args.flush_policy,
            "flush_timeout_ms": args.flush_timeout_ms,
            "flush_max_wait_ms": args.flush_max_wait_ms,
            "flush_trace_output": args.flush_trace_output or "",
            "flush_trace_path": flush_trace_path,
            "flush_trace_events": len(flush_trace_events),
            "submission_trace_path": submission_trace_path,
            "submission_trace_events": len(operator_results),
            "resource_trace_path": resource_trace_path,
            "resource_trace_events": len(resource_samples),
            "request_trace_path": request_trace_path,
            "request_trace_events": len(request_trace_rows),
            "request_e2e_s_p50": round(
                float(request_metrics["request_e2e_s_p50"]), 6
            ),
            "request_e2e_s_p95": round(
                float(request_metrics["request_e2e_s_p95"]), 6
            ),
            "request_e2e_s_p99": round(
                float(request_metrics["request_e2e_s_p99"]), 6
            ),
            "request_slo_target_ms": args.request_slo_ms,
            "request_slo_violation_ratio": round(
                float(request_metrics["request_slo_violation_ratio"]), 6
            ),
            "request_slo_goodput_per_s": round(
                float(request_metrics["request_slo_goodput_per_s"]), 6
            ),
            "latency_granularity": request_metrics["latency_granularity"],
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
            "tokens_per_s": round(_vllm_tokens_per_second(vllm_stats, e2e_s), 3),
        }
    finally:
        if resource_sampler is not None and resource_sampler.is_running:
            resource_sampler.close()
        conn.close()


def iter_run_phases(warmup_runs: int, repeats: int) -> Iterable[tuple[str, int]]:
    for repeat_index in range(1, warmup_runs + 1):
        yield "warmup", repeat_index
    for repeat_index in range(1, repeats + 1):
        yield "formal", repeat_index


def iter_requested_runs(
    args: argparse.Namespace,
) -> Iterable[tuple[str, int]]:
    supplied = (
        args.run_phase is not None,
        args.run_repeat_index is not None,
    )
    if supplied[0] != supplied[1]:
        raise SystemExit(
            "single-run mode requires --run-phase and --run-repeat-index"
        )
    if supplied[0]:
        if args.run_repeat_index < 1:
            raise SystemExit("single-run repeat index must be positive")
        yield args.run_phase, args.run_repeat_index
        return
    yield from iter_run_phases(args.warmup_runs, args.repeats)


def main() -> None:
    args = parse_args()
    for phase, repeat_index in iter_requested_runs(args):
        row = run_once(args, phase, repeat_index)
        append_metrics(Path(args.output), row)
        print(json.dumps(row, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
