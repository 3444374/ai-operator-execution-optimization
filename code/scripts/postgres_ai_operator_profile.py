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
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.metrics import (
    PeriodicSampler,
    StageTimer,
    aggregate_model_metric_snapshots,
    append_metrics,
    batch_result_stats,
    estimate_mfu,
    gpu_metadata,
    percentile,
    preflight_metrics_schema,
    resource_sample_stats,
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
from src.organizers import (
    OrganizerConfig,
    configure_daft_runner,
    make_organizer,
    packing_algorithm_name,
)
from src.packing import summarize_packing
from src.profile_cli import parse_args
from src.profile_config import (
    completion_endpoint_urls,
    embedding_endpoint_urls,
    model_metrics_urls,
    ray_worker_options as _ray_worker_options,
    resolve_actor_workers_per_endpoint as _resolve_actor_workers_per_endpoint,
)
from src.profile_traces import (
    write_control_trace as _write_control_trace,
    write_flush_trace as _write_flush_trace,
    write_request_trace as _write_request_trace,
    write_resource_trace as _write_resource_trace,
    write_submission_trace as _write_submission_trace,
)
from src.profile_schema import (
    FORMAL_RESULT_FIELDS,
    GPU_METADATA_DEFAULTS,
    validated_formal_result_row as _validated_formal_result_row,
)
from src.profile_replay import (
    _arrival_replay_envelopes,
    _offline_batch_envelopes,
)
from src.profile_ray import (
    submit_ray_tasks,
    submit_with_backpressure,
)
from src.request_costs import (
    output_cost_source,
)
from src.scheduling.adaptive_admission import (
    AimdAdmissionController,
    AimdConfig,
    EwmaAimdAdmissionController,
    HolAgeAimdAdmissionController,
    HolAgeAimdConfig,
)
from src.scheduling.admission import DynamicAdmissionGate
from src.scheduling.batching import ReplayServiceObservation
from src.scheduling.lifecycle import (
    MonotonicEpochClock,
    RequestLifecycleSeed,
    RequestTraceRow,
    SubmissionServiceTiming,
    build_request_trace_rows,
)
from src.scheduling.models import (
    PayloadEnvelope,
    SubmissionLifecycleEvent,
)
from src.scheduling.ray_adapter import (
    ActorSubmissionState,
)
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
from src.sinks import write_completions, write_embeddings
from src.sources import SourceConfig, make_source
from src.workloads import generate_document_rows


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



def _remote_actor_class(ray_module, actor_cls, worker_options):
    return ray_module.remote(actor_cls).options(
        **worker_options.actor_options()
    )


def _remote_task(ray_module, task_fn, worker_options):
    return ray_module.remote(task_fn).options(
        **worker_options.task_options()
    )


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


def _sum_semicolon_counts(current: str, addition: str) -> str:
    if not current:
        return addition
    if not addition:
        return current
    current_counts = [int(value) for value in current.split(";")]
    addition_counts = [int(value) for value in addition.split(";")]
    if len(current_counts) != len(addition_counts):
        raise ValueError("actor worker submission count widths must match")
    return ";".join(
        str(left + right)
        for left, right in zip(current_counts, addition_counts)
    )


def _merge_submit_metrics(
    aggregate: dict,
    addition: Mapping[str, object],
) -> None:
    maximum_fields = {
        "max_inflight",
        "adaptive_limit_mean",
        "endpoint_count",
        "actor_worker_count",
    }
    for key in aggregate:
        if key == "actor_worker_submission_counts":
            aggregate[key] = _sum_semicolon_counts(
                str(aggregate[key]),
                str(addition.get(key, "")),
            )
        elif key in maximum_fields:
            aggregate[key] = max(aggregate[key], addition.get(key, 0))
        else:
            aggregate[key] += addition.get(key, 0)


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


def fail_job(conn, job_id: int) -> None:
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE ai_operator_jobs SET status = 'failed', finished_at = CURRENT_TIMESTAMP WHERE job_id = %s",
            (job_id,),
        )
    conn.commit()



def _packing_run_metrics(
    batch_cost_units: list[int],
    batch_row_counts: list[int],
    *,
    capacity: int,
    packing_scope: str,
    packing_algorithm: str,
) -> dict[str, float | int | str]:
    summary = summarize_packing(
        batch_cost_units,
        batch_row_counts,
        capacity=capacity,
    )
    return {
        "packing_algorithm": packing_algorithm,
        "packing_scope": packing_scope,
        "packing_budget_utilization_mean": round(
            summary.utilization_mean,
            6,
        ),
        "packing_budget_utilization_p95": round(
            summary.utilization_p95,
            6,
        ),
        "packing_oversized_rows": summary.oversized_rows,
        "packing_input_rows": summary.input_rows,
        "packing_batch_count": summary.batch_count,
        "batch_estimated_cost_units_p50": summary.cost_units_p50,
        "batch_estimated_cost_units_p95": summary.cost_units_p95,
        "batch_estimated_cost_units_p99": summary.cost_units_p99,
        "batch_estimated_cost_units_max": summary.cost_units_max,
    }




def _service_metrics_snapshot(
    metrics_urls: Sequence[str],
) -> ServiceMetricsSnapshot | None:
    metrics = _scrape_model_metrics(metrics_urls, timeout_s=1.0)
    if not metrics:
        return None

    def optional_int(name: str) -> int | None:
        return int(metrics[name]) if name in metrics else None

    return ServiceMetricsSnapshot(
        running=optional_int("vllm:num_requests_running"),
        waiting=optional_int("vllm:num_requests_waiting"),
        kv_usage=metrics.get("vllm:kv_cache_usage_perc"),
    )


def _scrape_model_metrics(
    metrics_urls: Sequence[str],
    *,
    timeout_s: float = 5.0,
) -> dict[str, float]:
    snapshots = [
        scrape_prometheus_metrics(url, timeout_s=timeout_s)
        for url in metrics_urls
    ]
    return aggregate_model_metric_snapshots(snapshots)


def _build_adaptive_config(
    *,
    scheduling_policy: str,
    metrics_urls: Sequence[str],
    trace_events: list,
    min_window: int,
    max_window: int,
    initial_window: int,
    sample_interval_s: float,
    ewma_alpha: float,
    pid_proportional_gain: float,
    pid_integral_gain: float,
    pid_derivative_gain: float,
    hol_age_congestion_s: float,
    hol_age_low_load_s: float,
) -> dict:
    # aimd_hol keys on Ray-side head-of-line age and ignores service metrics,
    # so it (alone) can run without a model metrics URL; the service-metric
    # controllers (aimd/ewma_aimd/pid) still require one.
    if scheduling_policy != "aimd_hol" and not metrics_urls:
        raise ValueError("service-metric adaptive scheduling requires a model metrics URL")
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
    elif scheduling_policy == "aimd_hol":
        controller = HolAgeAimdAdmissionController(
            HolAgeAimdConfig(
                min_window=min_window,
                max_window=max_window,
                congestion_hol_age_s=hol_age_congestion_s,
                low_load_hol_age_s=hol_age_low_load_s,
            ),
            initial_window,
        )
    else:
        raise ValueError(f"unsupported typed adaptive policy: {scheduling_policy}")
    if scheduling_policy == "aimd_hol":
        provider = CachedMetricsObservationProvider(
            lambda: None,
            min_sample_interval_s=sample_interval_s,
        )
    else:
        sampler = lambda: _service_metrics_snapshot(metrics_urls)
        provider = NonBlockingMetricsObservationProvider(
            sampler,
            poll_interval_s=sample_interval_s,
            stale_after_s=max(0.5, sample_interval_s * 2),
            close_timeout_s=2.0,
        )
    gate = DynamicAdmissionGate(
        controller,
        provider,
        trace_sink=trace_events.append,
    )
    return {
        "admission_gate": gate,
        "observation_provider": provider,
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
                f"{label} count must equal service endpoint count {endpoint_count}"
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
    gpu_ids = (
        assignments(gpu_ids_text, "0", "GPU IDs")
        if gpu_ids_text
        else [str(index) for index in range(endpoint_count)]
    )
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
    actual_output_tokens_by_doc_id = {}
    finish_reason_by_doc_id = {}
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
            actual_output_counts = result.get("output_token_counts", [])
            if actual_output_counts and len(actual_output_counts) != len(doc_ids):
                raise ValueError(
                    "backend output token count must match doc_id count"
                )
            finish_reasons = result.get("finish_reasons", [])
            if finish_reasons and len(finish_reasons) != len(doc_ids):
                raise ValueError(
                    "backend finish reason count must match doc_id count"
                )
        else:
            output_counts = [0] * len(doc_ids)
            actual_output_counts = []
            finish_reasons = []
        for doc_id, output_count in zip(doc_ids, output_counts):
            if doc_id in client_estimated_output_tokens_by_doc_id:
                raise ValueError(f"duplicate backend doc_id: {doc_id}")
            client_estimated_output_tokens_by_doc_id[doc_id] = output_count
        for doc_id, output_count in zip(doc_ids, actual_output_counts):
            if output_count is not None:
                actual_output_tokens_by_doc_id[doc_id] = int(output_count)
        for doc_id, finish_reason in zip(doc_ids, finish_reasons):
            if finish_reason is not None:
                finish_reason_by_doc_id[doc_id] = str(finish_reason)

    return build_request_trace_rows(
        seeds,
        submission_events,
        service_by_submission_id,
        client_estimated_output_tokens_by_doc_id,
        actual_output_tokens_by_doc_id,
        finish_reason_by_doc_id=finish_reason_by_doc_id,
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
    actual_output_tokens = [
        row.actual_output_tokens
        for row in rows
        if row.actual_output_tokens is not None
    ]
    finish_reasons = [
        row.finish_reason for row in rows if row.finish_reason is not None
    ]
    finish_reason_count = len(finish_reasons)
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
        "request_actual_output_tokens_observed": len(actual_output_tokens),
        "request_actual_output_tokens_p50": percentile(
            actual_output_tokens,
            50,
        ),
        "request_actual_output_tokens_p95": percentile(
            actual_output_tokens,
            95,
        ),
        "request_actual_output_tokens_p99": percentile(
            actual_output_tokens,
            99,
        ),
        "request_finish_reason_observed": finish_reason_count,
        "request_finish_reason_stop_ratio": (
            sum(reason == "stop" for reason in finish_reasons)
            / finish_reason_count
            if finish_reason_count
            else 0.0
        ),
        "request_finish_reason_length_ratio": (
            sum(reason == "length" for reason in finish_reasons)
            / finish_reason_count
            if finish_reason_count
            else 0.0
        ),
        "latency_granularity": next(iter(granularities), ""),
    }


def _resource_snapshot(
    metrics_urls: Sequence[str],
    gpu_ids: Sequence[str] | None = None,
) -> dict[str, object]:
    gpu = gpu_metadata(gpu_ids)
    metrics = _scrape_model_metrics(metrics_urls, timeout_s=0.5)
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
    completion_return_token_ids: bool = False,
    completion_prompt_format: str = "raw",
    completion_temperature: float | None = None,
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
                compatible_http_complete_batch(
                    batch,
                    endpoint_url,
                    model_name,
                    api_key,
                    timeout_s,
                    completion_max_tokens,
                    completion_return_token_ids,
                    completion_prompt_format,
                    completion_temperature,
                )
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
    if args.submission_granularity == "request" and not args.arrival_replay:
        raise SystemExit(
            "--submission-granularity request requires --arrival-replay"
        )
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
        if args.batching_policy in {
            "best_fit_token_budget",
            "row_cap_aware_token_budget",
        }:
            raise SystemExit(
                f"arrival replay does not support {args.batching_policy}"
            )
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
    if args.executor not in {"ray_actor", "ray_task"}:
        raise SystemExit("request tracing requires a Ray executor")
    if args.scheduling_policy == "queue_adaptive":
        raise SystemExit("request tracing requires the typed scheduler")


def _validate_resource_efficiency_args(args: argparse.Namespace) -> None:
    if (
        isinstance(args.resource_sample_interval_s, bool)
        or not math.isfinite(args.resource_sample_interval_s)
        or args.resource_sample_interval_s <= 0
    ):
        raise SystemExit(
            "--resource-sample-interval-s must be finite and positive"
        )
    for name, value in (
        ("model-flops-per-token", args.model_flops_per_token),
        ("gpu-peak-tflops", args.gpu_peak_tflops),
    ):
        if (
            isinstance(value, bool)
            or not math.isfinite(value)
            or value < 0
        ):
            raise SystemExit(f"--{name} must be finite and non-negative")
    if (
        args.model_flops_per_token > 0
        or args.gpu_peak_tflops > 0
    ) and not args.mfu_precision.strip():
        raise SystemExit(
            "--mfu-precision is required when MFU inputs are configured"
        )


def _validate_completion_observation_args(args: argparse.Namespace) -> None:
    if (
        args.completion_temperature is not None
        and (
            isinstance(args.completion_temperature, bool)
            or not math.isfinite(args.completion_temperature)
            or args.completion_temperature < 0
        )
    ):
        raise SystemExit(
            "--completion-temperature must be finite and non-negative"
        )
    if (
        args.source_max_prompt_tokens is not None
        and args.source_max_prompt_tokens <= 0
    ):
        raise SystemExit("--source-max-prompt-tokens must be positive")
    uses_compatible_completion_options = (
        args.completion_return_token_ids
        or args.completion_prompt_format != "raw"
        or args.completion_temperature is not None
    )
    if uses_compatible_completion_options and (
        args.operator != "ai_complete"
        or args.model_backend not in {"compatible_http", "http_openai"}
    ):
        raise SystemExit(
            "completion token IDs, prompt format, and temperature require "
            "--operator ai_complete with a compatible HTTP backend"
        )


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
    worker_options = _ray_worker_options(args)
    reported_ray_actor_max_concurrency = (
        worker_options.actor_max_concurrency
        if args.executor == "ray_actor" and worker_options is not None
        else 0
    )
    ray_worker_num_cpus = (
        worker_options.num_cpus
        if worker_options is not None
        else 0.0
    )
    ray_worker_num_gpus = 0
    _validate_request_trace_args(args)
    _validate_arrival_replay_args(args)
    _validate_resource_efficiency_args(args)
    _validate_completion_observation_args(args)
    endpoint_urls = completion_endpoint_urls(args) if args.operator == "ai_complete" else embedding_endpoint_urls(args)
    endpoint_url_label = ";".join(endpoint_urls)
    resolved_metrics_urls = model_metrics_urls(args)
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
    if model_backend in {"compatible_http", "ollama"} and not endpoint_urls:
        raise SystemExit(
            "Missing endpoint URL. Use embedding endpoint args for ai_embed "
            "or completion endpoint args for ai_complete."
        )
    actor_workers_per_endpoint = 0
    if args.executor == "ray_actor":
        actor_endpoint_count = (
            1 if model_backend == "fake" else max(1, len(endpoint_urls))
        )
        actor_workers_per_endpoint = _resolve_actor_workers_per_endpoint(
            args,
            actor_endpoint_count,
        )
    model_name = args.completion_model if args.operator == "ai_complete" else args.embedding_model
    api_key = args.completion_api_key if args.operator == "ai_complete" else args.embedding_api_key
    request_timeout_s = (
        args.completion_request_timeout_s if args.operator == "ai_complete" else args.embedding_request_timeout_s
    )
    typed_adaptive_policies = {"aimd", "ewma_aimd", "pid", "aimd_hol"}
    if args.scheduling_policy in typed_adaptive_policies:
        if args.executor not in {"ray_actor", "ray_task"}:
            raise SystemExit("typed adaptive scheduling requires a Ray executor")
        if args.scheduling_policy != "aimd_hol" and not resolved_metrics_urls:
            raise SystemExit(
                "typed adaptive scheduling requires model metrics URL(s) "
                "(aimd_hol keys on Ray-side head-of-line age and does not)"
            )
    if args.executor == "python" and (
        args.endpoint_routing != "round_robin"
        or args.pool_routing != "none"
        or args.endpoint_pool_ids
        or args.endpoint_gpu_ids
    ):
        raise SystemExit("endpoint and pool routing require a Ray executor")
    routing_endpoint_count = (
        actor_endpoint_count
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
    if args.max_inflight <= 0:
        raise SystemExit("--max-inflight must be positive")
    if args.admission_scope == "per_endpoint":
        if args.executor not in {"ray_actor", "ray_task"}:
            raise SystemExit("per-endpoint admission requires a Ray executor")
        if args.scheduling_policy != "static":
            raise SystemExit(
                "per-endpoint admission currently supports --scheduling-policy "
                "static only"
            )
    per_endpoint_inflight_limit = (
        args.max_inflight if args.admission_scope == "per_endpoint" else None
    )
    effective_global_inflight_limit = (
        args.max_inflight * routing_endpoint_count
        if per_endpoint_inflight_limit is not None
        else args.max_inflight
    )
    sampled_gpu_ids = routing_config["gpu_ids"] if routing_config else None
    if args.dry_run:
        dry_packing_algorithm = (
            "sequential_pending"
            if args.arrival_replay
            else packing_algorithm_name(args.batching_policy)
        )
        dry_packing_metrics = _packing_run_metrics(
            [],
            [],
            capacity=(
                args.token_budget
                if args.batching_policy.endswith("token_budget")
                else 0
            ),
            packing_scope=(
                "arrival_order"
                if args.arrival_replay
                else "organizer_input"
            ),
            packing_algorithm=dry_packing_algorithm,
        )
        dry_resource_metrics = resource_sample_stats(
            [],
            observed_tokens=0,
        )
        dry_mfu_metrics = estimate_mfu(
            estimated_flops=0.0,
            observed_tokens=0,
            operator_wall_s=0.0,
            model_flops_per_token=args.model_flops_per_token,
            gpu_peak_tflops=args.gpu_peak_tflops,
            precision=args.mfu_precision,
        )
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
            "source_max_prompt_tokens": (
                args.source_max_prompt_tokens
                if args.source_max_prompt_tokens is not None
                else ""
            ),
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
            "completion_return_token_ids": args.completion_return_token_ids,
            "completion_prompt_format": args.completion_prompt_format,
            "completion_temperature": (
                args.completion_temperature
                if args.completion_temperature is not None
                else ""
            ),
            "output_cost_mode": args.output_cost_mode,
            "output_cost_source": output_cost_source(args.output_cost_mode),
            "packing_cost_unit": "tokens",
            "cost_model_id": args.cost_model_id,
            "cost_tokenizer_id": args.cost_tokenizer_id,
            **dry_packing_metrics,
            "model_workers": args.model_workers,
            "ray_version": "",
            "actor_workers_per_endpoint": actor_workers_per_endpoint,
            "ray_actor_max_concurrency": reported_ray_actor_max_concurrency,
            "ray_worker_num_cpus": ray_worker_num_cpus,
            "ray_worker_num_gpus": ray_worker_num_gpus,
            "endpoint_count": routing_endpoint_count,
            "actor_worker_count": (
                routing_endpoint_count * actor_workers_per_endpoint
                if args.executor == "ray_actor"
                else 0
            ),
            "actor_worker_submission_counts": "",
            "max_inflight_limit": args.max_inflight,
            "admission_scope": args.admission_scope,
            "per_endpoint_inflight_limit": per_endpoint_inflight_limit or 0,
            "effective_global_inflight_limit": effective_global_inflight_limit,
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
            "submission_granularity": args.submission_granularity,
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
            "resource_sample_interval_s": args.resource_sample_interval_s,
            **dry_resource_metrics,
            **dry_mfu_metrics,
            "request_trace_path": args.request_trace_output or "",
            "request_trace_events": 0,
            "request_e2e_s_p50": 0.0,
            "request_e2e_s_p95": 0.0,
            "request_e2e_s_p99": 0.0,
            "request_slo_target_ms": args.request_slo_ms,
            "request_slo_violation_ratio": 0.0,
            "request_slo_goodput_per_s": 0.0,
            "request_actual_output_tokens_observed": 0,
            "request_actual_output_tokens_p50": 0.0,
            "request_actual_output_tokens_p95": 0.0,
            "request_actual_output_tokens_p99": 0.0,
            "request_finish_reason_observed": 0,
            "request_finish_reason_stop_ratio": 0.0,
            "request_finish_reason_length_ratio": 0.0,
            "latency_granularity": (
                "request"
                if (
                    args.request_trace_output
                    and args.arrival_replay
                    and args.submission_granularity == "request"
                )
                else "submission"
                if args.request_trace_output
                else ""
            ),
            "writeback_mode": args.writeback_mode,
            "write_batch_rows": args.write_batch_rows,
        }
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL.")
    if args.operator == "ai_complete" and args.writeback_mode == "pgvector":
        raise SystemExit("AI_COMPLETE does not support --writeback-mode pgvector.")
    resource_sampler = None
    adaptive_observation_provider = None
    conn = connect(args.database_url)
    job_id = None
    try:
        gpu_snapshot = {
            **GPU_METADATA_DEFAULTS,
            **gpu_metadata(sampled_gpu_ids),
        }
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
        actor_pools: dict[str, list[object]] = {}
        actor_endpoint_urls: dict[str, str] = {}
        ray_module = None
        ray_version = ""
        remote_embed = None
        if args.executor in {"ray_actor", "ray_task"}:
            ray_module = require_ray()
            ray_version = str(getattr(ray_module, "__version__", ""))
            ray_module.init(ignore_reinit_error=True, runtime_env=ray_runtime_env())
            if args.executor == "ray_actor":
                actor_endpoint_urls = {
                    f"endpoint-{index}": endpoint_url
                    for index, endpoint_url in enumerate(
                        ["ray://actor/fake"]
                        if model_backend == "fake"
                        else endpoint_urls
                    )
                }
                if args.operator == "ai_complete" and model_backend == "fake":
                    RayCompletionActor = _remote_actor_class(
                        ray_module,
                        FakeCompletionActor,
                        worker_options,
                    )
                    actor_pools = {
                        endpoint_id: [
                            RayCompletionActor.remote(args.completion_max_tokens)
                            for _ in range(actor_workers_per_endpoint)
                        ]
                        for endpoint_id in actor_endpoint_urls
                    }
                elif args.operator == "ai_complete":
                    actor_cls = OllamaCompletionActor if model_backend == "ollama" else CompatibleHTTPCompletionActor
                    RayCompletionActor = _remote_actor_class(
                        ray_module,
                        actor_cls,
                        worker_options,
                    )
                    actor_pools = {}
                    for endpoint_id, endpoint_url in actor_endpoint_urls.items():
                        actor_pools[endpoint_id] = [
                            RayCompletionActor.remote(
                                endpoint_url,
                                model_name,
                                api_key,
                                request_timeout_s,
                                args.completion_max_tokens,
                                *(
                                    [
                                        args.completion_return_token_ids,
                                        args.completion_prompt_format,
                                        args.completion_temperature,
                                    ]
                                    if (
                                        model_backend == "compatible_http"
                                        and (
                                            args.completion_return_token_ids
                                            or args.completion_prompt_format
                                            != "raw"
                                            or args.completion_temperature
                                            is not None
                                        )
                                    )
                                    else []
                                ),
                            )
                            for _ in range(actor_workers_per_endpoint)
                        ]
                elif model_backend == "fake":
                    RayEmbeddingActor = _remote_actor_class(
                        ray_module,
                        FakeEmbeddingActor,
                        worker_options,
                    )
                    actor_pools = {
                        endpoint_id: [
                            RayEmbeddingActor.remote(args.embedding_dim)
                            for _ in range(actor_workers_per_endpoint)
                        ]
                        for endpoint_id in actor_endpoint_urls
                    }
                else:
                    RayEmbeddingActor = _remote_actor_class(
                        ray_module,
                        CompatibleHTTPEmbeddingActor,
                        worker_options,
                    )
                    actor_pools = {
                        endpoint_id: [
                            RayEmbeddingActor.remote(
                                endpoint_url,
                                model_name,
                                api_key,
                                request_timeout_s,
                            )
                            for _ in range(actor_workers_per_endpoint)
                        ]
                        for endpoint_id, endpoint_url in actor_endpoint_urls.items()
                    }
            else:
                if args.operator == "ai_complete" and model_backend == "fake":
                    remote_embed = _remote_task(
                        ray_module,
                        fake_complete_batch,
                        worker_options,
                    )
                elif args.operator == "ai_complete" and model_backend == "ollama":
                    remote_embed = _remote_task(
                        ray_module,
                        ollama_complete_batch,
                        worker_options,
                    )
                elif args.operator == "ai_complete":
                    remote_embed = _remote_task(
                        ray_module,
                        compatible_http_complete_batch,
                        worker_options,
                    )
                elif model_backend == "fake":
                    remote_embed = _remote_task(
                        ray_module,
                        fake_embed_batch,
                        worker_options,
                    )
                else:
                    remote_embed = _remote_task(
                        ray_module,
                        compatible_http_embed_batch,
                        worker_options,
                    )

        actor_submission_state = (
            ActorSubmissionState(
                actor_pools,
                "complete" if args.operator == "ai_complete" else "embed",
            )
            if args.executor == "ray_actor"
            else None
        )
        e2e_timer = StageTimer.start("e2e")
        processed_rows = 0
        object_count = 0
        arrow_build_s = 0.0
        db_fetch_s = 0.0
        operator_results = []
        request_lifecycle_seeds: list[RequestLifecycleSeed] = []
        submission_lifecycle_events: list[SubmissionLifecycleEvent] = []
        request_trace_rows: tuple[RequestTraceRow, ...] = ()
        packing_batch_cost_units: list[int] = []
        packing_batch_row_counts: list[int] = []
        organizer_calls = 0
        organizer_packing_scopes: list[str] = []
        replay_packing: list[tuple[int, int]] = []
        lifecycle_epoch_clock = (
            MonotonicEpochClock() if args.request_trace_output else None
        )
        offline_job_start_epoch_s = (
            lifecycle_epoch_clock()
            if lifecycle_epoch_clock is not None and not args.arrival_replay
            else None
        )
        offline_batch_index = 0
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
            "endpoint_count": 0,
            "actor_worker_count": 0,
            "actor_worker_submission_counts": "",
        }

        operator_wall_s = 0.0
        vllm_metrics_before = _scrape_model_metrics(resolved_metrics_urls)
        if args.resource_trace_output:
            resource_sampler = PeriodicSampler(
                lambda: _resource_snapshot(
                    resolved_metrics_urls,
                    sampled_gpu_ids,
                ),
                interval_s=args.resource_sample_interval_s,
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
            output_cost_mode=args.output_cost_mode,
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
                "metrics_url": (
                    resolved_metrics_urls[0]
                    if resolved_metrics_urls
                    else None
                ),
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
                    metrics_urls=resolved_metrics_urls,
                    trace_events=control_trace_events,
                    min_window=min_window,
                    max_window=args.controller_max_window,
                    initial_window=initial_window,
                    sample_interval_s=args.adaptive_sample_interval_s,
                    ewma_alpha=args.ewma_alpha,
                    pid_proportional_gain=args.pid_proportional_gain,
                    pid_integral_gain=args.pid_integral_gain,
                    pid_derivative_gain=args.pid_derivative_gain,
                    hol_age_congestion_s=args.hol_age_congestion_s,
                    hol_age_low_load_s=args.hol_age_low_load_s,
                )
                adaptive_observation_provider = adaptive_config[
                    "observation_provider"
                ]
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
                    ray_module=ray_module,
                    actor_pools=actor_pools,
                    endpoint_urls=actor_endpoint_urls,
                    batches=batches,
                    max_inflight=effective_global_inflight_limit,
                    method_name=method_name,
                    adaptive_config=adaptive_config,
                    routing_config=routing_config,
                    replay_envelopes=replay_envelopes,
                    submission_lifecycle_sink=(
                        submission_lifecycle_events
                        if (
                            args.request_trace_output
                            or args.submission_trace_output
                        )
                        else None
                    ),
                    epoch_clock=lifecycle_epoch_clock,
                    output_cost_mode=args.output_cost_mode,
                    completion_max_tokens=args.completion_max_tokens,
                    submission_state=actor_submission_state,
                    per_endpoint_limit=per_endpoint_inflight_limit,
                )
            if args.executor == "ray_task":
                return submit_ray_tasks(
                    ray_module,
                    remote_embed,
                    batches,
                    effective_global_inflight_limit,
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
                        if (
                            args.request_trace_output
                            or args.submission_trace_output
                        )
                        else None
                    ),
                    epoch_clock=lifecycle_epoch_clock,
                    per_endpoint_limit=per_endpoint_inflight_limit,
                    output_cost_mode=args.output_cost_mode,
                    completion_return_token_ids=(
                        args.completion_return_token_ids
                    ),
                    completion_prompt_format=args.completion_prompt_format,
                    completion_temperature=args.completion_temperature,
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
                args.completion_return_token_ids,
                args.completion_prompt_format,
                args.completion_temperature,
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
                max_prompt_tokens=args.source_max_prompt_tokens,
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
            organizer_calls += 1
            ray_batches = organized.batches
            packing_batch_cost_units.extend(organized.batch_cost_units)
            packing_batch_row_counts.extend(organized.batch_row_counts)
            organizer_packing_scopes.append(
                str(organized.metrics.get("packing_scope", "organizer_input"))
            )
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
            offline_envelopes = None
            if args.request_trace_output:
                if (
                    lifecycle_epoch_clock is None
                    or offline_job_start_epoch_s is None
                ):
                    raise RuntimeError(
                        "offline lifecycle clock is not initialized"
                    )
                offline_envelopes, offline_seeds = (
                    _offline_batch_envelopes(
                        ray_batches,
                        job_id=str(job_id),
                        operator=args.operator,
                        completion_max_tokens=(
                            args.completion_max_tokens
                            if args.operator == "ai_complete"
                            else 0
                        ),
                        output_cost_mode=args.output_cost_mode,
                        batch_index_start=offline_batch_index,
                        job_start_epoch_s=offline_job_start_epoch_s,
                        ready_epoch_s=lifecycle_epoch_clock(),
                    )
                )
                request_lifecycle_seeds.extend(offline_seeds)
                offline_batch_index += len(offline_envelopes)
            operator_timer = StageTimer.start("operator_wall")
            results, metrics = submit_operator_batches(
                ray_batches,
                replay_envelopes=offline_envelopes,
            )
            operator_wall_s += operator_timer.stop()
            operator_results.extend(results)
            _merge_submit_metrics(submit_metrics, metrics)
            processed_rows += table.num_rows

        if args.arrival_replay:
            flush_observation_provider = None
            if args.flush_policy == "queue_adaptive":
                flush_observation_provider = NonBlockingMetricsObservationProvider(
                    lambda: (
                        _service_metrics_snapshot(resolved_metrics_urls)
                        if resolved_metrics_urls
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
                    packing_sink=replay_packing,
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
            _merge_submit_metrics(submit_metrics, metrics)

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
                submission_events=(
                    submission_lifecycle_events
                    if submission_lifecycle_events
                    else None
                ),
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

        vllm_metrics_after = _scrape_model_metrics(resolved_metrics_urls)
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
        observed_tokens = (
            int(vllm_stats["vllm_prompt_tokens_delta"])
            + int(vllm_stats["vllm_generation_tokens_delta"])
        )
        resource_metrics = resource_sample_stats(
            resource_samples,
            observed_tokens=observed_tokens,
        )
        mfu_metrics = estimate_mfu(
            estimated_flops=float(
                vllm_stats["vllm_estimated_flops_per_gpu_delta"]
            ),
            observed_tokens=observed_tokens,
            operator_wall_s=operator_wall_s,
            model_flops_per_token=args.model_flops_per_token,
            gpu_peak_tflops=args.gpu_peak_tflops,
            precision=args.mfu_precision,
        )
        if args.arrival_replay:
            packing_batch_cost_units = [
                cost for cost, _ in replay_packing
            ]
            packing_batch_row_counts = [
                row_count for _, row_count in replay_packing
            ]
            packing_scope = "arrival_order"
            packing_algorithm = "sequential_pending"
        else:
            packing_scope = (
                "fetch_chunk_local"
                if organizer_calls > 1
                else "partition_local"
                if "partition_local" in organizer_packing_scopes
                else "organizer_input"
            )
            packing_algorithm = packing_algorithm_name(
                args.batching_policy
            )
        packing_metrics = _packing_run_metrics(
            packing_batch_cost_units,
            packing_batch_row_counts,
            capacity=(
                args.token_budget
                if args.batching_policy.endswith("token_budget")
                else 0
            ),
            packing_scope=packing_scope,
            packing_algorithm=packing_algorithm,
        )

        return _validated_formal_result_row({
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
            "source_max_prompt_tokens": (
                args.source_max_prompt_tokens
                if args.source_max_prompt_tokens is not None
                else ""
            ),
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
            "completion_return_token_ids": args.completion_return_token_ids,
            "completion_prompt_format": args.completion_prompt_format,
            "completion_temperature": (
                args.completion_temperature
                if args.completion_temperature is not None
                else ""
            ),
            "output_cost_mode": args.output_cost_mode,
            "output_cost_source": output_cost_source(args.output_cost_mode),
            "packing_cost_unit": "tokens",
            "cost_model_id": args.cost_model_id,
            "cost_tokenizer_id": args.cost_tokenizer_id,
            **packing_metrics,
            "model_workers": args.model_workers,
            "ray_version": ray_version,
            "actor_workers_per_endpoint": actor_workers_per_endpoint,
            "ray_actor_max_concurrency": reported_ray_actor_max_concurrency,
            "ray_worker_num_cpus": ray_worker_num_cpus,
            "ray_worker_num_gpus": ray_worker_num_gpus,
            "endpoint_count": max(
                routing_endpoint_count,
                int(submit_metrics["endpoint_count"]),
            ),
            "actor_worker_count": int(submit_metrics["actor_worker_count"]),
            "actor_worker_submission_counts": submit_metrics[
                "actor_worker_submission_counts"
            ],
            "max_inflight_limit": args.max_inflight,
            "admission_scope": args.admission_scope,
            "per_endpoint_inflight_limit": per_endpoint_inflight_limit or 0,
            "effective_global_inflight_limit": effective_global_inflight_limit,
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
            "submission_granularity": args.submission_granularity,
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
            "resource_sample_interval_s": args.resource_sample_interval_s,
            **resource_metrics,
            **mfu_metrics,
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
            "request_actual_output_tokens_observed": int(
                request_metrics["request_actual_output_tokens_observed"]
            ),
            "request_actual_output_tokens_p50": round(
                float(request_metrics["request_actual_output_tokens_p50"]),
                6,
            ),
            "request_actual_output_tokens_p95": round(
                float(request_metrics["request_actual_output_tokens_p95"]),
                6,
            ),
            "request_actual_output_tokens_p99": round(
                float(request_metrics["request_actual_output_tokens_p99"]),
                6,
            ),
            "request_finish_reason_observed": int(
                request_metrics["request_finish_reason_observed"]
            ),
            "request_finish_reason_stop_ratio": round(
                float(request_metrics["request_finish_reason_stop_ratio"]),
                6,
            ),
            "request_finish_reason_length_ratio": round(
                float(request_metrics["request_finish_reason_length_ratio"]),
                6,
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
            "vllm_estimated_flops_per_gpu_delta": vllm_stats[
                "vllm_estimated_flops_per_gpu_delta"
            ],
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
        })
    except BaseException as original_error:
        if job_id is not None:
            try:
                fail_job(conn, job_id)
            except BaseException as fail_error:
                note = (
                    "Failed to mark ai_operator_job "
                    f"{job_id} failed: {fail_error!r}"
                )
                if hasattr(original_error, "add_note"):
                    original_error.add_note(note)
                else:
                    original_error.__notes__ = [
                        *getattr(original_error, "__notes__", ()),
                        note,
                    ]
        raise
    finally:
        if resource_sampler is not None and resource_sampler.is_running:
            resource_sampler.close()
        if adaptive_observation_provider is not None:
            adaptive_observation_provider.close()
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
    requested_runs = list(iter_requested_runs(args))
    if requested_runs and not args.dry_run:
        phase, repeat_index = requested_runs[0]
        dry_args = argparse.Namespace(**{**vars(args), "dry_run": True})
        run_once(dry_args, phase, repeat_index)
        preflight_metrics_schema(Path(args.output), FORMAL_RESULT_FIELDS)
    for phase, repeat_index in requested_runs:
        row = run_once(args, phase, repeat_index)
        append_metrics(Path(args.output), row)
        print(json.dumps(row, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
