#!/usr/bin/env python3
"""Profile a PostgreSQL 18-compatible AI operator external execution path.

The local rehearsal environment currently runs PostgreSQL 18.4, and the
semantic-operator implementation is now locked to REL_18_4. The script records
the actual server and pgvector versions in every non-dry-run CSV row so external
path rehearsals are not confused with planner-visible operator qualification.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pyarrow as pa

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.observability.metrics import (
    PeriodicSampler,
    StageTimer,
    aggregate_model_metric_snapshots,
    append_metrics,
    batch_result_stats,
    estimate_mfu,
    gpu_metadata,
    observed_slo_scale_metrics,
    percentile,
    preflight_metrics_schema,
    resource_sample_stats,
    scrape_prometheus_metrics,
    token_cost_metrics,
    vllm_metric_delta_stats,
)
from src.serving.backends import (
    CompatibleAsyncHTTPCompletionActor,
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
from src.data.materializers.text import (
    OrganizerConfig,
    configure_daft_runner,
    make_organizer,
    packing_algorithm_name,
)
from src.planning.packing.scalar import summarize_packing
from src.observability.profiling.cli import parse_args
from src.observability.profiling.config import (
    completion_endpoint_urls,
    embedding_endpoint_urls,
    model_metrics_urls,
    ray_worker_options as _ray_worker_options,
    resolve_actor_workers_per_endpoint as _resolve_actor_workers_per_endpoint,
    validate_shared_credit_policy_args,
)
from src.observability.profiling.traces import (
    source_scan_fingerprint_rows as _source_scan_fingerprint_rows,
    write_completion_evidence as _write_completion_evidence,
    write_control_trace as _write_control_trace,
    write_flush_trace as _write_flush_trace,
    write_request_trace as _write_request_trace,
    write_resource_trace as _write_resource_trace,
    write_submission_trace as _write_submission_trace,
    write_source_scan_evidence as _write_source_scan_evidence,
)
from src.observability.profiling.schema import (
    FORMAL_RESULT_FIELDS,
    GPU_METADATA_DEFAULTS,
    validated_formal_result_row as _validated_formal_result_row,
)
from src.observability.profiling.replay import (
    _arrival_replay_envelopes,
    _offline_batch_envelopes,
    _requires_replay_feedback,
)
from src.observability.profiling.manifest_guard import (
    ProfileManifestGuard,
    validate_profile_manifest_contract,
)
from src.observability.profiling.ray import (
    submit_ray_tasks,
    submit_with_backpressure,
)
from src.modalities.text.costs import (
    output_cost_source,
)
from src.infrastructure.runtime_env import ray_runtime_env as _shared_ray_runtime_env
from src.scheduling.submission_control.adaptive import (
    AimdAdmissionController,
    AimdConfig,
    EwmaAimdAdmissionController,
    HolAgeAimdAdmissionController,
    HolAgeAimdConfig,
)
from src.scheduling.submission_control.admission import DynamicAdmissionGate
from src.scheduling.organization.batching import ReplayServiceObservation
from src.scheduling.core.lifecycle import (
    MonotonicEpochClock,
    RequestLifecycleSeed,
    RequestTraceRow,
    SubmissionServiceTiming,
    build_request_trace_rows,
)
from src.scheduling.core.models import (
    PayloadEnvelope,
    SubmissionLifecycleEvent,
)
from src.scheduling.runtime.ray_adapter import (
    ActorSubmissionState,
)
from src.scheduling.runtime.observations import (
    CachedMetricsObservationProvider,
    NonBlockingMetricsObservationProvider,
    ServiceMetricsSnapshot,
)
from src.scheduling.submission_control.pid import PidAdmissionController, PidConfig
from src.scheduling.endpoint_routing.policies import (
    LeastQueuedEndpointRouter,
    LeastWorkEndpointRouter,
    PinnedEndpointRouter,
    PrefixAffinityEndpointRouter,
    RequestPoolRouter,
    RoundRobinEndpointRouter,
)
from src.data.sinks import (
    execute_write_plan,
    prepare_completion_write,
    prepare_embedding_write,
    # Preserve the script module's historical patch/import surface.
    write_completions,
    write_embeddings,
)  # noqa: F401
from src.data.sources import SourceConfig, make_source
from src.data.workloads import generate_document_rows


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
    sample_fields = {
        "ready_requests_transition_samples",
        "ready_work_transition_samples",
        "ready_payload_bytes_transition_samples",
    }
    maximum_fields = {
        "max_inflight",
        "max_active_work_per_endpoint_seen",
        "max_ready_requests_seen",
        "max_ready_work_seen",
        "max_ready_payload_bytes_seen",
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
        elif key in sample_fields:
            aggregate[key].extend(addition.get(key, ()))
        elif key in maximum_fields:
            aggregate[key] = max(aggregate[key], addition.get(key, 0))
        else:
            aggregate[key] += addition.get(key, 0)


def ray_runtime_env() -> dict[str, dict[str, str]]:
    return _shared_ray_runtime_env(CODE_ROOT)


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
    row_cap: int,
    packing_scope: str,
    packing_algorithm: str,
    padding_slots: int = 0,
    padding_capacity_slots: int = 0,
    padding_observed: bool = False,
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
        "packing_padding_waste_status": (
            "ok" if padding_observed else "unavailable:row_lengths_not_captured"
        ),
        "packing_padding_waste_ratio": round(
            padding_slots / padding_capacity_slots
            if padding_capacity_slots > 0
            else 0.0,
            6,
        ),
        "packing_oversized_rows": summary.oversized_rows,
        "packing_input_rows": summary.input_rows,
        "packing_batch_count": summary.batch_count,
        "batch_estimated_cost_units_p50": summary.cost_units_p50,
        "batch_estimated_cost_units_p95": summary.cost_units_p95,
        "batch_estimated_cost_units_p99": summary.cost_units_p99,
        "batch_estimated_cost_units_max": summary.cost_units_max,
        "organization_batch_count": len(batch_row_counts),
        "organization_batch_rows_mean": round(
            (
                sum(batch_row_counts) / len(batch_row_counts)
                if batch_row_counts
                else 0.0
            ),
            6,
        ),
        "organization_batch_rows_max": max(batch_row_counts, default=0),
        "organization_batch_cost_units_mean": round(
            (
                sum(batch_cost_units) / len(batch_cost_units)
                if batch_cost_units
                else 0.0
            ),
            6,
        ),
        "organization_batch_cost_units_p95": percentile(
            batch_cost_units,
            95,
        ),
        "organization_row_cap_hit_ratio": round(
            (
                sum(rows >= row_cap for rows in batch_row_counts)
                / len(batch_row_counts)
                if row_cap > 0 and batch_row_counts
                else 0.0
            ),
            6,
        ),
    }


def _service_quantum_run_metrics(
    quanta: Sequence[tuple[int, int, bool]],
    *,
    target_tokens: int,
) -> dict[str, float | int]:
    work = [item[0] for item in quanta]
    rows = [item[1] for item in quanta]
    return {
        "service_quantum_tokens": target_tokens,
        "service_quantum_count": len(quanta),
        "service_quantum_rows_mean": round(
            sum(rows) / len(rows) if rows else 0.0,
            6,
        ),
        "service_quantum_work_mean": round(
            sum(work) / len(work) if work else 0.0,
            6,
        ),
        "service_quantum_work_p95": percentile(work, 95),
        "service_quantum_oversized_rows": sum(
            row_count
            for _, row_count, oversized in quanta
            if oversized
        ),
    }


def _actor_worker_run_metrics(
    submission_state: ActorSubmissionState | None,
    *,
    routing_policy: str,
    actor_ready_s: float,
    slots_per_endpoint: int,
    operator_wall_s: float,
) -> dict[str, float | int | str]:
    snapshots = (
        tuple(
            snapshot
            for submitter in submission_state.pool_submitters.values()
            for snapshot in submitter.snapshots()
        )
        if submission_state is not None
        else ()
    )
    total_slots = (
        slots_per_endpoint * len(submission_state.pool_submitters)
        if submission_state
        else 0
    )
    utilization = (
        sum(snapshot.slot_held_s for snapshot in snapshots)
        / (operator_wall_s * total_slots)
        if operator_wall_s > 0 and total_slots > 0
        else 0.0
    )
    return {
        "actor_worker_routing": routing_policy,
        "actor_ready_s": round(actor_ready_s, 6),
        "actor_pool_slots_per_endpoint": slots_per_endpoint,
        "actor_worker_max_running": ";".join(
            str(snapshot.max_running) for snapshot in snapshots
        ),
        "actor_worker_max_active_work": ";".join(
            str(snapshot.max_active_work) for snapshot in snapshots
        ),
        "actor_worker_failures": ";".join(
            str(snapshot.failed) for snapshot in snapshots
        ),
        "actor_worker_slot_held_utilization": round(utilization, 6),
    }


def _http_transport_metrics(results: Sequence[dict]) -> dict[str, float]:
    """Summarize only explicitly observed non-streaming HTTP intervals."""

    headers_wait = [
        float(result["http_headers_wait_s"])
        for result in results
        if result.get("http_headers_wait_s") is not None
    ]
    body_read = [
        float(result["http_body_read_s"])
        for result in results
        if result.get("http_body_read_s") is not None
    ]

    def observed_percentile(values: list[float], quantile: float) -> float:
        return percentile(values, quantile) if values else 0.0

    return {
        "http_headers_wait_s_p50": observed_percentile(headers_wait, 50),
        "http_headers_wait_s_p95": observed_percentile(headers_wait, 95),
        "http_headers_wait_s_p99": observed_percentile(headers_wait, 99),
        "http_body_read_s_p50": observed_percentile(body_read, 50),
        "http_body_read_s_p95": observed_percentile(body_read, 95),
        "http_body_read_s_p99": observed_percentile(body_read, 99),
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
        completed_tokens_total=(
            metrics.get("vllm:prompt_tokens_total", 0.0)
            + metrics.get("vllm:generation_tokens_total", 0.0)
            if (
                "vllm:prompt_tokens_total" in metrics
                or "vllm:generation_tokens_total" in metrics
            )
            else None
        ),
        endpoint_count=len(metrics_urls),
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
    endpoint_id: str | None = None,
    additive_increase: int = 2,
    multiplicative_decrease: float = 0.5,
    congestion_kv_usage: float = 0.85,
    low_load_kv_usage: float = 0.50,
    low_load_running: int = 64,
) -> dict:
    # aimd_hol keys on Ray-side head-of-line age and ignores service metrics,
    # so it (alone) can run without a model metrics URL; the service-metric
    # controllers (aimd/ewma_aimd/pid) still require one.
    if scheduling_policy != "aimd_hol" and not metrics_urls:
        raise ValueError("service-metric adaptive scheduling requires a model metrics URL")
    if scheduling_policy in {"aimd", "ewma_aimd"}:
        config = AimdConfig(
            min_window=min_window,
            max_window=max_window,
            additive_increase=additive_increase,
            multiplicative_decrease=multiplicative_decrease,
            congestion_kv_usage=congestion_kv_usage,
            low_load_kv_usage=low_load_kv_usage,
            low_load_running=low_load_running,
        )
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
                additive_increase=additive_increase,
                multiplicative_decrease=multiplicative_decrease,
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
        endpoint_id=endpoint_id,
    )
    return {
        "admission_gate": gate,
        "observation_provider": provider,
        "trace_events": trace_events,
        "controller_name": scheduling_policy,
        "min_window": min_window,
        "max_window": max_window,
    }


def _build_per_endpoint_adaptive_config(
    *,
    scheduling_policy: str,
    endpoint_ids: Sequence[str],
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
    additive_increase: int = 2,
    multiplicative_decrease: float = 0.5,
    congestion_kv_usage: float = 0.85,
    low_load_kv_usage: float = 0.50,
    low_load_running: int = 64,
) -> dict:
    if not endpoint_ids:
        raise ValueError("per-endpoint adaptive scheduling requires endpoints")
    if scheduling_policy != "aimd_hol" and (
        len(metrics_urls) != len(endpoint_ids)
    ):
        raise ValueError(
            "per-endpoint adaptive scheduling requires exactly one metrics "
            "URL per service endpoint"
        )
    endpoint_configs = {}
    for index, endpoint_id in enumerate(endpoint_ids):
        endpoint_metrics_urls = (
            []
            if scheduling_policy == "aimd_hol"
            else [metrics_urls[index]]
        )
        endpoint_configs[endpoint_id] = _build_adaptive_config(
            scheduling_policy=scheduling_policy,
            metrics_urls=endpoint_metrics_urls,
            trace_events=trace_events,
            min_window=min_window,
            max_window=max_window,
            initial_window=initial_window,
            sample_interval_s=sample_interval_s,
            ewma_alpha=ewma_alpha,
            pid_proportional_gain=pid_proportional_gain,
            pid_integral_gain=pid_integral_gain,
            pid_derivative_gain=pid_derivative_gain,
            hol_age_congestion_s=hol_age_congestion_s,
            hol_age_low_load_s=hol_age_low_load_s,
            endpoint_id=endpoint_id,
            additive_increase=additive_increase,
            multiplicative_decrease=multiplicative_decrease,
            congestion_kv_usage=congestion_kv_usage,
            low_load_kv_usage=low_load_kv_usage,
            low_load_running=low_load_running,
        )
    return {
        "per_endpoint_gates": {
            endpoint_id: config["admission_gate"]
            for endpoint_id, config in endpoint_configs.items()
        },
        "observation_providers": [
            config["observation_provider"]
            for config in endpoint_configs.values()
        ],
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
        "least_work": LeastWorkEndpointRouter,
        "manifest_pinned": PinnedEndpointRouter,
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


def _validate_operator_result_visibility(
    results: Sequence[object],
    processed_rows: int,
) -> None:
    """Require complete, non-duplicate row visibility across batched results."""

    visible_rows = 0
    visible_doc_ids: list[str] = []
    for result in results:
        if not isinstance(result, dict):
            raise RuntimeError(
                "complete correct result visibility requires one result per source row"
            )
        raw_doc_ids = result.get("doc_id")
        declared_rows = result.get("rows")
        if raw_doc_ids is None:
            result_rows = int(declared_rows) if declared_rows is not None else 1
        else:
            if isinstance(raw_doc_ids, (str, bytes)):
                raise RuntimeError("operator result doc_id must be a row collection")
            doc_ids = [str(value) for value in raw_doc_ids]
            result_rows = len(doc_ids)
            if declared_rows is not None and int(declared_rows) != result_rows:
                raise RuntimeError(
                    "operator result rows must match its doc_id count"
                )
            visible_doc_ids.extend(doc_ids)
        if result_rows < 0:
            raise RuntimeError("operator result rows must be non-negative")
        visible_rows += result_rows
    if len(visible_doc_ids) != len(set(visible_doc_ids)):
        raise RuntimeError("operator results contain duplicate doc_id values")
    if visible_rows != processed_rows:
        raise RuntimeError(
            "complete correct result visibility requires one result per source row"
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
    slo_met_rows = [row for row in rows if row.slo_met is True]
    slo_input_tokens = sum(row.prompt_tokens for row in slo_met_rows)
    slo_output_tokens = sum(
        row.actual_output_tokens or 0 for row in slo_met_rows
    )
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
        "request_slo_input_tokens_goodput_per_s": (
            slo_input_tokens / e2e_s if slo_enabled and e2e_s > 0 else 0.0
        ),
        "request_slo_output_tokens_goodput_per_s": (
            slo_output_tokens / e2e_s if slo_enabled and e2e_s > 0 else 0.0
        ),
        "request_slo_total_tokens_goodput_per_s": (
            (slo_input_tokens + slo_output_tokens) / e2e_s
            if slo_enabled and e2e_s > 0
            else 0.0
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
    completion_protocol: str = "completions",
    completion_ignore_eos: bool = False,
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
                    completion_protocol,
                    completion_ignore_eos,
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
    if args.endpoint_routing == "manifest_pinned" and not args.request_manifest:
        raise SystemExit(
            "manifest_pinned routing requires --request-manifest"
        )
    if (
        isinstance(args.arrival_replay_start_epoch_s, bool)
        or not math.isfinite(args.arrival_replay_start_epoch_s)
        or args.arrival_replay_start_epoch_s < 0
    ):
        raise SystemExit(
            "--arrival-replay-start-epoch-s must be finite and non-negative"
        )
    if args.arrival_replay_start_epoch_s > 0 and not args.arrival_replay:
        raise SystemExit(
            "--arrival-replay-start-epoch-s requires --arrival-replay"
        )
    if args.submission_granularity == "service_quantum":
        if args.service_quantum_tokens <= 0:
            raise SystemExit(
                "--service-quantum-tokens must be positive for service_quantum"
            )
    elif args.service_quantum_tokens != 0:
        raise SystemExit(
            "--service-quantum-tokens requires service_quantum granularity"
        )
    if not args.arrival_replay:
        if args.token_budget_policy != "static":
            raise SystemExit(
                "dynamic token-budget policy requires --arrival-replay"
            )
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
    if (
        args.token_budget_policy != "static"
        and args.batching_policy != "token_budget"
    ):
        raise SystemExit(
            "dynamic token-budget policy requires "
            "--batching-policy token_budget"
        )
    if args.token_budget_policy == "service_quantum":
        try:
            candidates = tuple(
                int(value.strip())
                for value in args.token_budget_candidates.split(",")
                if value.strip()
            )
        except ValueError as exc:
            raise SystemExit(
                "--token-budget-candidates must be comma-separated integers"
            ) from exc
        if (
            not candidates
            or any(candidate <= 0 for candidate in candidates)
            or args.token_budget not in candidates
        ):
            raise SystemExit(
                "--token-budget-candidates must contain positive values and "
                "include --token-budget as the fallback"
            )
        if (
            not math.isfinite(args.token_budget_target_service_ms)
            or args.token_budget_target_service_ms <= 0
        ):
            raise SystemExit(
                "--token-budget-target-service-ms must be finite and positive"
            )
        if (
            not math.isfinite(args.token_budget_arrival_ewma_alpha)
            or not 0 < args.token_budget_arrival_ewma_alpha <= 1
        ):
            raise SystemExit(
                "--token-budget-arrival-ewma-alpha must be in (0, 1]"
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
    adaptive_flush_policies = {"queue_adaptive", "slo_ewma"}
    if (
        args.flush_policy in adaptive_flush_policies
        and args.flush_timeout_ms <= 0
    ):
        raise SystemExit(
            "adaptive flush requires --flush-timeout-ms > 0"
        )
    if (
        args.flush_policy in adaptive_flush_policies
        and args.flush_max_wait_ms < args.flush_timeout_ms
    ):
        raise SystemExit(
            "adaptive flush requires "
            "--flush-max-wait-ms >= --flush-timeout-ms"
        )
    if (
        not math.isfinite(args.flush_ewma_alpha)
        or not 0 < args.flush_ewma_alpha <= 1
    ):
        raise SystemExit("--flush-ewma-alpha must be in (0, 1]")
    if (
        not math.isfinite(args.flush_deadband_ratio)
        or not 0 <= args.flush_deadband_ratio <= 1
    ):
        raise SystemExit("--flush-deadband-ratio must be in [0, 1]")
    if args.flush_policy == "slo_ewma" and args.request_slo_ms <= 0:
        raise SystemExit("slo-ewma flush requires --request-slo-ms > 0")
    if (
        not math.isfinite(
            args.flush_service_capacity_tokens_s_per_endpoint
        )
        or args.flush_service_capacity_tokens_s_per_endpoint < 0
    ):
        raise SystemExit(
            "--flush-service-capacity-tokens-s-per-endpoint must be "
            "finite and non-negative"
        )
    if (
        args.flush_policy == "slo_ewma"
        and args.flush_service_capacity_tokens_s_per_endpoint <= 0
    ):
        raise SystemExit(
            "slo-ewma flush requires calibrated "
            "--flush-service-capacity-tokens-s-per-endpoint > 0"
        )


def _wait_for_replay_start(
    target_epoch_s: float,
    *,
    wall_clock=time.time,
    sleeper=time.sleep,
) -> float:
    if target_epoch_s > 0:
        remaining_s = target_epoch_s - wall_clock()
        if remaining_s > 0:
            sleeper(remaining_s)
    return wall_clock()


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
    if args.completion_evidence_output and not args.request_trace_output:
        raise SystemExit(
            "--completion-evidence-output requires --request-trace-output"
        )
    for name, value in (
        ("ttft-slo-ms", args.ttft_slo_ms),
        ("itl-slo-ms", args.itl_slo_ms),
        (
            "input-cost-per-million-tokens-usd",
            args.input_cost_per_million_tokens_usd,
        ),
        (
            "output-cost-per-million-tokens-usd",
            args.output_cost_per_million_tokens_usd,
        ),
    ):
        if value is not None and (
            isinstance(value, bool) or not math.isfinite(value) or value < 0
        ):
            raise SystemExit(f"--{name} must be finite and non-negative")
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
    if args.source_row_offset < 0:
        raise SystemExit("--source-row-offset must be non-negative")
    if (
        not isinstance(args.completion_prompt_token_overhead, int)
        or isinstance(args.completion_prompt_token_overhead, bool)
        or args.completion_prompt_token_overhead < 0
    ):
        raise SystemExit(
            "--completion-prompt-token-overhead must be a non-negative integer"
        )
    if args.completion_prompt_token_overhead and (
        args.operator != "ai_complete"
        or args.completion_protocol != "chat_completions"
    ):
        raise SystemExit(
            "non-zero completion prompt overhead requires "
            "--operator ai_complete and --completion-protocol chat_completions"
        )
    uses_compatible_completion_options = (
        args.completion_return_token_ids
        or args.completion_ignore_eos
        or args.completion_prompt_format != "raw"
        or args.completion_temperature is not None
    )
    if uses_compatible_completion_options and (
        args.operator != "ai_complete"
        or args.model_backend not in {"compatible_http", "http_openai"}
    ):
        raise SystemExit(
            "completion token IDs, ignore-EOS, prompt format, and temperature require "
            "--operator ai_complete with a compatible HTTP backend"
        )
    if args.completion_http_transport == "httpx_async" and (
        args.operator != "ai_complete"
        or args.model_backend not in {"compatible_http", "http_openai"}
        or args.executor != "ray_actor"
    ):
        raise SystemExit(
            "httpx_async completion transport requires "
            "--operator ai_complete, a compatible HTTP backend, "
            "and --executor ray_actor"
        )
    if (
        not math.isfinite(args.completion_http_keepalive_expiry_s)
        or args.completion_http_keepalive_expiry_s <= 0
    ):
        raise SystemExit(
            "--completion-http-keepalive-expiry-s must be finite and positive"
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

@dataclass(frozen=True)
class _DryRunContext:
    actor_pool_slots_per_endpoint: int
    actor_workers_per_endpoint: int
    effective_global_inflight_limit: int
    endpoint_url_label: str
    model_backend: str
    model_name: str
    per_endpoint_inflight_limit: int | None
    per_endpoint_work_limit: int | None
    ray_worker_num_cpus: float
    ray_worker_num_gpus: int
    reported_ray_actor_max_concurrency: int
    request_manifest_guard: ProfileManifestGuard | None
    request_manifest_path: str
    request_manifest_rows: int
    request_manifest_sha256: str
    request_timeout_s: float
    routing_endpoint_count: int


def _dry_run_result(
    args: argparse.Namespace,
    phase: str,
    repeat_index: int,
    context: _DryRunContext,
) -> dict[str, object]:
    actor_pool_slots_per_endpoint = context.actor_pool_slots_per_endpoint
    actor_workers_per_endpoint = context.actor_workers_per_endpoint
    effective_global_inflight_limit = context.effective_global_inflight_limit
    endpoint_url_label = context.endpoint_url_label
    model_backend = context.model_backend
    model_name = context.model_name
    per_endpoint_inflight_limit = context.per_endpoint_inflight_limit
    per_endpoint_work_limit = context.per_endpoint_work_limit
    ray_worker_num_cpus = context.ray_worker_num_cpus
    ray_worker_num_gpus = context.ray_worker_num_gpus
    reported_ray_actor_max_concurrency = (
        context.reported_ray_actor_max_concurrency
    )
    request_manifest_guard = context.request_manifest_guard
    request_manifest_path = context.request_manifest_path
    request_manifest_rows = context.request_manifest_rows
    request_manifest_sha256 = context.request_manifest_sha256
    request_timeout_s = context.request_timeout_s
    routing_endpoint_count = context.routing_endpoint_count

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
        row_cap=args.ray_batch_rows,
        packing_scope=(
            "arrival_order"
            if args.arrival_replay
            else "organizer_input"
        ),
        packing_algorithm=dry_packing_algorithm,
    )
    dry_service_quantum_metrics = _service_quantum_run_metrics(
        [],
        target_tokens=args.service_quantum_tokens,
    )
    dry_actor_worker_metrics = _actor_worker_run_metrics(
        None,
        routing_policy=(
            args.actor_worker_routing
            if args.executor == "ray_actor"
            else ""
        ),
        actor_ready_s=0.0,
        slots_per_endpoint=actor_pool_slots_per_endpoint,
        operator_wall_s=0.0,
    )
    dry_http_transport_metrics = _http_transport_metrics([])
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
        "source_row_offset": args.source_row_offset,
        "source_max_prompt_tokens": (
            args.source_max_prompt_tokens
            if args.source_max_prompt_tokens is not None
            else ""
        ),
        "request_manifest_path": request_manifest_path,
        "request_manifest_sha256": request_manifest_sha256,
        "request_manifest_rows": request_manifest_rows,
        "request_manifest_validated_rows": 0,
        "request_manifest_validation_status": (
            "not_executed"
            if request_manifest_guard is not None
            else "disabled"
        ),
        "organizer": args.organizer,
        "organizer_partition_mode": args.organizer_partition_mode,
        "organizer_partitions": args.organizer_partitions,
        "daft_runner": args.daft_runner,
        "model_backend": model_backend,
        "model_endpoint_url": endpoint_url_label,
        "model_name": model_name,
        "model_request_timeout_s": request_timeout_s,
        "service_prefix_caching": args.service_prefix_caching,
        "total_rows": args.total_rows,
        "db_fetch_rows": args.db_fetch_rows,
        "ray_batch_rows": args.ray_batch_rows,
        "batching_policy": args.batching_policy,
        "token_budget": args.token_budget,
        "token_budget_policy": args.token_budget_policy,
        "token_budget_candidates": args.token_budget_candidates,
        "token_budget_target_service_ms": (
            args.token_budget_target_service_ms
        ),
        "token_budget_arrival_ewma_alpha": (
            args.token_budget_arrival_ewma_alpha
        ),
        "completion_max_tokens": args.completion_max_tokens if args.operator == "ai_complete" else "",
        "completion_return_token_ids": args.completion_return_token_ids,
        "completion_ignore_eos": args.completion_ignore_eos,
        "completion_prompt_format": args.completion_prompt_format,
        "completion_protocol": args.completion_protocol,
        "completion_prompt_token_overhead": (
            args.completion_prompt_token_overhead
            if args.operator == "ai_complete"
            else ""
        ),
        "completion_http_transport": (
            args.completion_http_transport
            if args.operator == "ai_complete"
            else ""
        ),
        "completion_http_keepalive_expiry_s": (
            args.completion_http_keepalive_expiry_s
            if args.operator == "ai_complete"
            and args.completion_http_transport == "httpx_async"
            else ""
        ),
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
        **dry_service_quantum_metrics,
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
        **dry_actor_worker_metrics,
        "max_inflight_limit": args.max_inflight,
        "admission_scope": args.admission_scope,
        "per_endpoint_inflight_limit": per_endpoint_inflight_limit or 0,
        "max_active_work_per_endpoint": per_endpoint_work_limit or 0,
        "max_active_work_per_endpoint_seen": 0,
        "max_ready_requests_seen": 0,
        "max_ready_work_seen": 0,
        "max_ready_payload_bytes_seen": 0,
        "ready_requests_transition_samples": [],
        "ready_work_transition_samples": [],
        "ready_payload_bytes_transition_samples": [],
        "shared_credit_coordinator_name": (
            args.shared_credit_coordinator_name
        ),
        "shared_credit_request_limit": (
            args.shared_credit_request_limit
        ),
        "shared_credit_work_limit": args.shared_credit_work_limit,
        "shared_credit_quantum": args.shared_credit_quantum,
        "shared_credit_job_weight": args.shared_credit_job_weight,
        "shared_credit_job_priority": args.shared_credit_job_priority,
        "shared_credit_job_slo_ms": args.shared_credit_job_slo_ms,
        "shared_credit_priority_window_ms": (
            args.shared_credit_priority_window_ms
        ),
        "shared_credit_job_debt_cap_work": (
            args.shared_credit_job_debt_cap_work
        ),
        "shared_credit_policy": (
            args.shared_credit_policy
            if args.shared_credit_coordinator_name
            else ""
        ),
        "shared_ready_observation_contract": (
            args.shared_ready_observation_contract
            if args.shared_credit_coordinator_name
            else ""
        ),
        "shared_ready_payload_bytes_limit": (
            args.shared_ready_payload_bytes_limit
            if args.shared_credit_coordinator_name
            else 0
        ),
        "saor_entitlement_weight": (
            args.saor_entitlement_weight
            if args.shared_credit_policy in {
                "saor",
                "saor_bounded_priority",
                "saor_bounded_ready",
            }
            else 0.0
        ),
        "saor_queue_weight": (
            args.saor_queue_weight
            if args.shared_credit_policy in {
                "saor",
                "saor_bounded_priority",
                "saor_bounded_ready",
            }
            else 0.0
        ),
        "saor_fairness_weight": (
            args.saor_fairness_weight
            if args.shared_credit_policy in {
                "saor",
                "saor_bounded_priority",
                "saor_bounded_ready",
            }
            else 0.0
        ),
        "saor_slo_weight": (
            args.saor_slo_weight
            if args.shared_credit_policy in {
                "saor",
                "saor_bounded_priority",
                "saor_bounded_ready",
            }
            else 0.0
        ),
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
        "controller_additive_increase": (
            args.controller_additive_increase
        ),
        "controller_multiplicative_decrease": (
            args.controller_multiplicative_decrease
        ),
        "controller_congestion_kv_usage": (
            args.controller_congestion_kv_usage
        ),
        "controller_low_load_kv_usage": (
            args.controller_low_load_kv_usage
        ),
        "controller_low_load_running": (
            args.controller_low_load_running
        ),
        "adaptive_sample_interval_s": args.adaptive_sample_interval_s,
        "control_trace_output": args.control_trace_output or "",
        "arrival_replay": args.arrival_replay,
        "arrival_time_scale": args.arrival_time_scale,
        "arrival_replay_preload": (
            "bounded_requested_workload" if args.arrival_replay else ""
        ),
        "arrival_replay_start_epoch_s": (
            args.arrival_replay_start_epoch_s
        ),
        "arrival_replay_observed_start_epoch_s": 0.0,
        "submission_granularity": args.submission_granularity,
        "flush_policy": args.flush_policy,
        "flush_timeout_ms": args.flush_timeout_ms,
        "flush_max_wait_ms": args.flush_max_wait_ms,
        "flush_ewma_alpha": args.flush_ewma_alpha,
        "flush_deadband_ratio": args.flush_deadband_ratio,
        "flush_service_capacity_tokens_s_per_endpoint": (
            args.flush_service_capacity_tokens_s_per_endpoint
        ),
        "flush_trace_output": args.flush_trace_output or "",
        "flush_trace_status": (
            "requested" if args.arrival_replay else "not_applicable_non_replay"
        ),
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
                and args.submission_granularity == "request"
            )
            else "submission"
            if args.request_trace_output
            else ""
        ),
        "writeback_mode": args.writeback_mode,
        "write_batch_rows": args.write_batch_rows,
        **dry_http_transport_metrics,
    }

@dataclass(frozen=True)
class _ResolvedProfileRun:
    worker_options: object | None
    reported_ray_actor_max_concurrency: int
    ray_worker_num_cpus: float
    ray_worker_num_gpus: int
    endpoint_urls: list[str]
    endpoint_url_label: str
    resolved_metrics_urls: list[str]
    model_backend: str
    request_manifest_path: str
    request_manifest_sha256: str
    request_manifest_rows: int
    request_manifest_validation_status: str
    request_manifest_guard: ProfileManifestGuard | None
    request_manifest_doc_ids: tuple[int, ...] | None
    actor_workers_per_endpoint: int
    actor_pool_slots_per_endpoint: int
    model_name: str
    api_key: str
    request_timeout_s: float
    typed_adaptive_policies: frozenset[str]
    routing_endpoint_count: int
    routing_config: dict[str, object] | None
    per_endpoint_inflight_limit: int | None
    per_endpoint_work_limit: int | None
    shared_credit_config: dict[str, object] | None
    ray_address: str | None
    effective_global_inflight_limit: int
    sampled_gpu_ids: object

    def dry_run_context(self) -> _DryRunContext:
        return _DryRunContext(
            actor_pool_slots_per_endpoint=self.actor_pool_slots_per_endpoint,
            actor_workers_per_endpoint=self.actor_workers_per_endpoint,
            effective_global_inflight_limit=self.effective_global_inflight_limit,
            endpoint_url_label=self.endpoint_url_label,
            model_backend=self.model_backend,
            model_name=self.model_name,
            per_endpoint_inflight_limit=self.per_endpoint_inflight_limit,
            per_endpoint_work_limit=self.per_endpoint_work_limit,
            ray_worker_num_cpus=self.ray_worker_num_cpus,
            ray_worker_num_gpus=self.ray_worker_num_gpus,
            reported_ray_actor_max_concurrency=(
                self.reported_ray_actor_max_concurrency
            ),
            request_manifest_guard=self.request_manifest_guard,
            request_manifest_path=self.request_manifest_path,
            request_manifest_rows=self.request_manifest_rows,
            request_manifest_sha256=self.request_manifest_sha256,
            request_timeout_s=self.request_timeout_s,
            routing_endpoint_count=self.routing_endpoint_count,
        )


def _resolve_profile_run(args: argparse.Namespace) -> _ResolvedProfileRun:
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
    validate_shared_credit_policy_args(args)
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
    request_manifest_path = args.request_manifest or ""
    request_manifest_sha256 = ""
    request_manifest_rows = 0
    request_manifest_validated_rows = 0
    request_manifest_validation_status = "disabled"
    request_manifest_guard = None
    request_manifest_doc_ids: tuple[int, ...] | None = None
    if request_manifest_path:
        endpoint_ids = tuple(
            f"endpoint-{index}" for index in range(len(endpoint_urls))
        )
        request_manifest_guard = ProfileManifestGuard.from_path(
            request_manifest_path,
            endpoint_ids,
            output_cost_mode=args.output_cost_mode,
        )
        validate_profile_manifest_contract(
            request_manifest_guard.requests,
            total_rows=args.total_rows,
            operator=args.operator,
            model_backend=model_backend,
            endpoint_count=len(endpoint_urls),
            completion_protocol=args.completion_protocol,
            completion_prompt_format=args.completion_prompt_format,
            completion_temperature=args.completion_temperature,
            completion_max_tokens=args.completion_max_tokens,
            output_cost_mode=args.output_cost_mode,
            source_order=args.source_order,
            executor=args.executor,
            submission_granularity=args.submission_granularity,
            endpoint_routing=args.endpoint_routing,
            arrival_replay=args.arrival_replay,
        )
        request_manifest_sha256 = request_manifest_guard.manifest_sha256
        request_manifest_rows = len(request_manifest_guard.requests)
        request_manifest_doc_ids = tuple(
            request.doc_id for request in request_manifest_guard.requests
        )
        request_manifest_validation_status = "pending"
    actor_workers_per_endpoint = 0
    if args.executor == "ray_actor":
        actor_endpoint_count = (
            1 if model_backend == "fake" else max(1, len(endpoint_urls))
        )
        actor_workers_per_endpoint = _resolve_actor_workers_per_endpoint(
            args,
            actor_endpoint_count,
        )
    actor_pool_slots_per_endpoint = (
        actor_workers_per_endpoint * reported_ray_actor_max_concurrency
        if args.executor == "ray_actor"
        else 0
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
        if (
            args.scheduling_policy != "static"
            and args.scheduling_policy not in typed_adaptive_policies
        ):
            raise SystemExit(
                "per-endpoint admission supports static or typed adaptive "
                "scheduling; legacy queue_adaptive is global-only"
            )
        if (
            args.scheduling_policy in typed_adaptive_policies
            and args.controller_max_window > args.max_inflight
        ):
            raise SystemExit(
                "per-endpoint controller max window must not exceed "
                "--max-inflight"
            )
        if (
            args.scheduling_policy in typed_adaptive_policies
            and args.scheduling_policy != "aimd_hol"
            and len(resolved_metrics_urls) != routing_endpoint_count
        ):
            raise SystemExit(
                "per-endpoint adaptive scheduling requires exactly one "
                "model metrics URL per service endpoint"
            )
    if args.max_active_work_per_endpoint < 0:
        raise SystemExit(
            "--max-active-work-per-endpoint must be non-negative"
        )
    if (
        args.max_active_work_per_endpoint > 0
        and args.scheduling_policy != "static"
    ):
        raise SystemExit(
            "active-work admission currently supports "
            "--scheduling-policy static only"
        )
    configured_per_endpoint_limit = (
        args.max_inflight if args.admission_scope == "per_endpoint" else None
    )
    per_endpoint_inflight_limit = configured_per_endpoint_limit
    if args.executor == "ray_actor":
        per_endpoint_inflight_limit = (
            min(configured_per_endpoint_limit, actor_pool_slots_per_endpoint)
            if configured_per_endpoint_limit is not None
            else actor_pool_slots_per_endpoint
        )
    per_endpoint_work_limit = (
        args.max_active_work_per_endpoint
        if args.max_active_work_per_endpoint > 0
        else None
    )
    shared_credit_config = None
    ray_address = args.ray_address or os.environ.get("RAY_ADDRESS")
    if args.shared_credit_coordinator_name:
        if not ray_address:
            raise SystemExit(
                "shared credit requires an explicit Ray address via "
                "--ray-address or RAY_ADDRESS"
            )
        if args.executor not in {"ray_actor", "ray_task"}:
            raise SystemExit("shared credit requires a Ray executor")
        if args.scheduling_policy != "static":
            raise SystemExit(
                "shared credit currently requires static job-local scheduling"
            )
        if (
            args.shared_credit_request_limit <= 0
            or args.shared_credit_work_limit <= 0
            or args.shared_credit_quantum <= 0
            or args.shared_credit_job_weight <= 0
            or args.shared_credit_job_priority < 0
        ):
            raise SystemExit(
                "shared credit request/work limits, quantum, and job weight "
                "must be positive; job priority must be non-negative"
            )
        if args.shared_credit_policy == "saor" and args.saor_slo_weight > 0:
            raise SystemExit(
                "SAOR SLO-weighted release is not executable yet; use "
                "--saor-slo-weight 0 until per-Job SLO debt is connected"
            )
        shared_credit_config = {
            "name": args.shared_credit_coordinator_name,
            "namespace": args.shared_credit_namespace,
            "request_limit": args.shared_credit_request_limit,
            "work_limit": args.shared_credit_work_limit,
            "quantum": args.shared_credit_quantum,
            "job_weight": args.shared_credit_job_weight,
            "job_priority": args.shared_credit_job_priority,
            "job_slo_target_s": (
                args.shared_credit_job_slo_ms / 1000.0
                if args.shared_credit_job_slo_ms > 0
                else None
            ),
            "job_priority_window_s": (
                args.shared_credit_priority_window_ms / 1000.0
                if args.shared_credit_priority_window_ms > 0
                else None
            ),
            "job_fairness_debt_cap": (
                args.shared_credit_job_debt_cap_work
                if args.shared_credit_job_debt_cap_work > 0
                else None
            ),
            "acquire_timeout_s": request_timeout_s,
            "policy": args.shared_credit_policy,
            "ready_observation_contract": (
                args.shared_ready_observation_contract
            ),
            "ready_payload_bytes_limit": (
                args.shared_ready_payload_bytes_limit
            ),
            "saor_release": (
                {
                    "entitlement_weight": args.saor_entitlement_weight,
                    "queue_weight": args.saor_queue_weight,
                    "fairness_weight": args.saor_fairness_weight,
                    "slo_weight": args.saor_slo_weight,
                }
                if args.shared_credit_policy in {
                    "saor",
                    "saor_bounded_priority",
                    "saor_bounded_ready",
                }
                else None
            ),
        }
    effective_global_inflight_limit = (
        per_endpoint_inflight_limit * routing_endpoint_count
        if args.admission_scope == "per_endpoint"
        and per_endpoint_inflight_limit is not None
        else min(
            args.max_inflight,
            actor_pool_slots_per_endpoint * routing_endpoint_count,
        )
        if args.executor == "ray_actor"
        else args.max_inflight
    )
    sampled_gpu_ids = routing_config["gpu_ids"] if routing_config else None
    return _ResolvedProfileRun(
        worker_options=worker_options,
        reported_ray_actor_max_concurrency=reported_ray_actor_max_concurrency,
        ray_worker_num_cpus=ray_worker_num_cpus,
        ray_worker_num_gpus=ray_worker_num_gpus,
        endpoint_urls=endpoint_urls,
        endpoint_url_label=endpoint_url_label,
        resolved_metrics_urls=resolved_metrics_urls,
        model_backend=model_backend,
        request_manifest_path=request_manifest_path,
        request_manifest_sha256=request_manifest_sha256,
        request_manifest_rows=request_manifest_rows,
        request_manifest_validation_status=request_manifest_validation_status,
        request_manifest_guard=request_manifest_guard,
        request_manifest_doc_ids=request_manifest_doc_ids,
        actor_workers_per_endpoint=actor_workers_per_endpoint,
        actor_pool_slots_per_endpoint=actor_pool_slots_per_endpoint,
        model_name=model_name,
        api_key=api_key,
        request_timeout_s=request_timeout_s,
        typed_adaptive_policies=frozenset(typed_adaptive_policies),
        routing_endpoint_count=routing_endpoint_count,
        routing_config=routing_config,
        per_endpoint_inflight_limit=per_endpoint_inflight_limit,
        per_endpoint_work_limit=per_endpoint_work_limit,
        shared_credit_config=shared_credit_config,
        ray_address=ray_address,
        effective_global_inflight_limit=effective_global_inflight_limit,
        sampled_gpu_ids=sampled_gpu_ids,
    )


@dataclass(frozen=True)
class _ProfileResultData:
    phase: str
    repeat_index: int
    adaptive_config: object
    arrival_replay_observed_start_epoch_s: float
    arrow_build_s: float
    batch_stats: dict
    control_trace_events: list
    control_trace_path: str
    cost_metrics: dict
    current_vector_dim: object
    db_fetch_s: float
    db_metadata: dict
    e2e_s: float
    first_batch_ready_epoch_s: float
    flush_trace_events: list
    flush_trace_path: str
    gpu_snapshot: dict
    http_transport_metrics: dict
    job_id: object
    mfu_metrics: dict
    object_count: int
    operator_results: list
    operator_wall_s: float
    organizer_metrics: dict
    organizer_warnings: list[str]
    packing_metrics: dict
    processed_rows: int
    ray_version: str
    request_manifest_validated_rows: int
    request_manifest_validation_status: str
    request_metrics: dict
    request_trace_path: str
    request_trace_rows: tuple
    request_wall_s: float
    resource_metrics: dict
    resource_samples: list
    resource_trace_path: str
    result_visible_epoch_s: float
    service_quantum_metrics: dict
    service_s: float
    slo_scale_metrics: dict
    submission_trace_path: str
    submit_metrics: dict
    token_count: int
    vllm_stats: dict
    writeback_s: float
    written_rows: int
    actor_worker_metrics: dict


def _build_profile_result_row(
    args: argparse.Namespace,
    resolved: _ResolvedProfileRun,
    data: _ProfileResultData,
) -> dict:
    phase = data.phase
    repeat_index = data.repeat_index
    adaptive_config = data.adaptive_config
    arrival_replay_observed_start_epoch_s = (
        data.arrival_replay_observed_start_epoch_s
    )
    arrow_build_s = data.arrow_build_s
    batch_stats = data.batch_stats
    control_trace_events = data.control_trace_events
    control_trace_path = data.control_trace_path
    cost_metrics = data.cost_metrics
    current_vector_dim = data.current_vector_dim
    db_fetch_s = data.db_fetch_s
    db_metadata = data.db_metadata
    e2e_s = data.e2e_s
    first_batch_ready_epoch_s = data.first_batch_ready_epoch_s
    flush_trace_events = data.flush_trace_events
    flush_trace_path = data.flush_trace_path
    gpu_snapshot = data.gpu_snapshot
    http_transport_metrics = data.http_transport_metrics
    job_id = data.job_id
    mfu_metrics = data.mfu_metrics
    object_count = data.object_count
    operator_results = data.operator_results
    operator_wall_s = data.operator_wall_s
    organizer_metrics = data.organizer_metrics
    organizer_warnings = data.organizer_warnings
    packing_metrics = data.packing_metrics
    processed_rows = data.processed_rows
    ray_version = data.ray_version
    request_manifest_validated_rows = data.request_manifest_validated_rows
    request_manifest_validation_status = data.request_manifest_validation_status
    request_metrics = data.request_metrics
    request_trace_path = data.request_trace_path
    request_trace_rows = data.request_trace_rows
    request_wall_s = data.request_wall_s
    resource_metrics = data.resource_metrics
    resource_samples = data.resource_samples
    resource_trace_path = data.resource_trace_path
    result_visible_epoch_s = data.result_visible_epoch_s
    service_quantum_metrics = data.service_quantum_metrics
    service_s = data.service_s
    slo_scale_metrics = data.slo_scale_metrics
    submission_trace_path = data.submission_trace_path
    submit_metrics = data.submit_metrics
    token_count = data.token_count
    vllm_stats = data.vllm_stats
    writeback_s = data.writeback_s
    written_rows = data.written_rows
    actor_worker_metrics = data.actor_worker_metrics
    actor_workers_per_endpoint = resolved.actor_workers_per_endpoint
    effective_global_inflight_limit = resolved.effective_global_inflight_limit
    endpoint_url_label = resolved.endpoint_url_label
    model_backend = resolved.model_backend
    model_name = resolved.model_name
    per_endpoint_inflight_limit = resolved.per_endpoint_inflight_limit
    per_endpoint_work_limit = resolved.per_endpoint_work_limit
    ray_worker_num_cpus = resolved.ray_worker_num_cpus
    ray_worker_num_gpus = resolved.ray_worker_num_gpus
    reported_ray_actor_max_concurrency = (
        resolved.reported_ray_actor_max_concurrency
    )
    request_manifest_path = resolved.request_manifest_path
    request_manifest_rows = resolved.request_manifest_rows
    request_manifest_sha256 = resolved.request_manifest_sha256
    request_timeout_s = resolved.request_timeout_s
    routing_config = resolved.routing_config
    routing_endpoint_count = resolved.routing_endpoint_count
    typed_adaptive_policies = resolved.typed_adaptive_policies

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
        "source_row_offset": args.source_row_offset,
        "source_max_prompt_tokens": (
            args.source_max_prompt_tokens
            if args.source_max_prompt_tokens is not None
            else ""
        ),
        "request_manifest_path": request_manifest_path,
        "request_manifest_sha256": request_manifest_sha256,
        "request_manifest_rows": request_manifest_rows,
        "request_manifest_validated_rows": (
            request_manifest_validated_rows
        ),
        "request_manifest_validation_status": (
            request_manifest_validation_status
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
        "service_prefix_caching": args.service_prefix_caching,
        "total_rows": processed_rows,
        "written_rows": written_rows,
        "db_fetch_rows": args.db_fetch_rows,
        "ray_batch_rows": args.ray_batch_rows,
        "batching_policy": args.batching_policy,
        "token_budget": args.token_budget,
        "token_budget_policy": args.token_budget_policy,
        "token_budget_candidates": args.token_budget_candidates,
        "token_budget_target_service_ms": (
            args.token_budget_target_service_ms
        ),
        "token_budget_arrival_ewma_alpha": (
            args.token_budget_arrival_ewma_alpha
        ),
        "embedding_dim": args.embedding_dim,
        "embedding_vector_dim": current_vector_dim if current_vector_dim is not None else "",
        "completion_max_tokens": args.completion_max_tokens if args.operator == "ai_complete" else "",
        "completion_return_token_ids": args.completion_return_token_ids,
        "completion_ignore_eos": args.completion_ignore_eos,
        "completion_prompt_format": args.completion_prompt_format,
        "completion_protocol": args.completion_protocol,
        "completion_prompt_token_overhead": (
            args.completion_prompt_token_overhead
            if args.operator == "ai_complete"
            else ""
        ),
        "completion_http_transport": (
            args.completion_http_transport
            if args.operator == "ai_complete"
            else ""
        ),
        "completion_http_keepalive_expiry_s": (
            args.completion_http_keepalive_expiry_s
            if args.operator == "ai_complete"
            and args.completion_http_transport == "httpx_async"
            else ""
        ),
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
        **service_quantum_metrics,
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
        **actor_worker_metrics,
        "max_inflight_limit": args.max_inflight,
        "admission_scope": args.admission_scope,
        "per_endpoint_inflight_limit": per_endpoint_inflight_limit or 0,
        "max_active_work_per_endpoint": per_endpoint_work_limit or 0,
        "max_active_work_per_endpoint_seen": int(
            submit_metrics["max_active_work_per_endpoint_seen"]
        ),
        "max_ready_requests_seen": int(
            submit_metrics["max_ready_requests_seen"]
        ),
        "max_ready_work_seen": int(
            submit_metrics["max_ready_work_seen"]
        ),
        "max_ready_payload_bytes_seen": int(
            submit_metrics["max_ready_payload_bytes_seen"]
        ),
        "ready_requests_transition_mean": (
            statistics.mean(
                submit_metrics["ready_requests_transition_samples"]
            )
            if submit_metrics["ready_requests_transition_samples"]
            else 0.0
        ),
        "ready_requests_transition_p95": percentile(
            submit_metrics["ready_requests_transition_samples"], 95
        ),
        "ready_work_transition_mean": (
            statistics.mean(
                submit_metrics["ready_work_transition_samples"]
            )
            if submit_metrics["ready_work_transition_samples"]
            else 0.0
        ),
        "ready_work_transition_p95": percentile(
            submit_metrics["ready_work_transition_samples"], 95
        ),
        "ready_payload_bytes_transition_mean": (
            statistics.mean(
                submit_metrics[
                    "ready_payload_bytes_transition_samples"
                ]
            )
            if submit_metrics[
                "ready_payload_bytes_transition_samples"
            ]
            else 0.0
        ),
        "ready_payload_bytes_transition_p95": percentile(
            submit_metrics["ready_payload_bytes_transition_samples"],
            95,
        ),
        "shared_credit_coordinator_name": (
            args.shared_credit_coordinator_name
        ),
        "shared_credit_request_limit": (
            args.shared_credit_request_limit
        ),
        "shared_credit_work_limit": args.shared_credit_work_limit,
        "shared_credit_quantum": args.shared_credit_quantum,
        "shared_credit_job_weight": args.shared_credit_job_weight,
        "shared_credit_job_priority": args.shared_credit_job_priority,
        "shared_credit_job_slo_ms": args.shared_credit_job_slo_ms,
        "shared_credit_priority_window_ms": (
            args.shared_credit_priority_window_ms
        ),
        "shared_credit_job_debt_cap_work": (
            args.shared_credit_job_debt_cap_work
        ),
        "shared_credit_policy": (
            args.shared_credit_policy
            if args.shared_credit_coordinator_name
            else ""
        ),
        "shared_ready_observation_contract": (
            args.shared_ready_observation_contract
            if args.shared_credit_coordinator_name
            else ""
        ),
        "shared_ready_payload_bytes_limit": (
            args.shared_ready_payload_bytes_limit
            if args.shared_credit_coordinator_name
            else 0
        ),
        "saor_entitlement_weight": (
            args.saor_entitlement_weight
            if args.shared_credit_policy in {
                "saor",
                "saor_bounded_priority",
                "saor_bounded_ready",
            }
            else 0.0
        ),
        "saor_queue_weight": (
            args.saor_queue_weight
            if args.shared_credit_policy in {
                "saor",
                "saor_bounded_priority",
                "saor_bounded_ready",
            }
            else 0.0
        ),
        "saor_fairness_weight": (
            args.saor_fairness_weight
            if args.shared_credit_policy in {
                "saor",
                "saor_bounded_priority",
                "saor_bounded_ready",
            }
            else 0.0
        ),
        "saor_slo_weight": (
            args.saor_slo_weight
            if args.shared_credit_policy in {
                "saor",
                "saor_bounded_priority",
                "saor_bounded_ready",
            }
            else 0.0
        ),
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
        "controller_initial_window": (
            args.controller_initial_window
            if (
                args.scheduling_policy in typed_adaptive_policies
                and args.controller_initial_window is not None
            )
            else (
                args.controller_min_window
                if (
                    args.scheduling_policy in typed_adaptive_policies
                    and args.controller_min_window is not None
                )
                else (
                    2
                    if args.scheduling_policy == "pid"
                    else 4
                    if args.scheduling_policy in typed_adaptive_policies
                    else 0
                )
            )
        ),
        "controller_additive_increase": (
            args.controller_additive_increase
            if args.scheduling_policy in typed_adaptive_policies
            else 0
        ),
        "controller_multiplicative_decrease": (
            args.controller_multiplicative_decrease
            if args.scheduling_policy in typed_adaptive_policies
            else 0
        ),
        "controller_congestion_kv_usage": (
            args.controller_congestion_kv_usage
            if args.scheduling_policy in typed_adaptive_policies
            else 0
        ),
        "controller_low_load_kv_usage": (
            args.controller_low_load_kv_usage
            if args.scheduling_policy in typed_adaptive_policies
            else 0
        ),
        "controller_low_load_running": (
            args.controller_low_load_running
            if args.scheduling_policy in typed_adaptive_policies
            else 0
        ),
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
        "arrival_replay_start_epoch_s": (
            args.arrival_replay_start_epoch_s
        ),
        "arrival_replay_observed_start_epoch_s": (
            arrival_replay_observed_start_epoch_s
        ),
        "submission_granularity": args.submission_granularity,
        "flush_policy": args.flush_policy,
        "flush_timeout_ms": args.flush_timeout_ms,
        "flush_max_wait_ms": args.flush_max_wait_ms,
        "flush_ewma_alpha": args.flush_ewma_alpha,
        "flush_deadband_ratio": args.flush_deadband_ratio,
        "flush_service_capacity_tokens_s_per_endpoint": (
            args.flush_service_capacity_tokens_s_per_endpoint
        ),
        "flush_trace_output": args.flush_trace_output or "",
        "flush_trace_status": (
            "ok" if args.arrival_replay else "not_applicable_non_replay"
        ),
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
        "request_slo_input_tokens_goodput_per_s": round(
            float(
                request_metrics[
                    "request_slo_input_tokens_goodput_per_s"
                ]
            ),
            6,
        ),
        "request_slo_output_tokens_goodput_per_s": round(
            float(
                request_metrics[
                    "request_slo_output_tokens_goodput_per_s"
                ]
            ),
            6,
        ),
        "request_slo_total_tokens_goodput_per_s": round(
            float(
                request_metrics[
                    "request_slo_total_tokens_goodput_per_s"
                ]
            ),
            6,
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
        "http_headers_wait_s_p50": round(
            float(http_transport_metrics["http_headers_wait_s_p50"]),
            6,
        ),
        "http_headers_wait_s_p95": round(
            float(http_transport_metrics["http_headers_wait_s_p95"]),
            6,
        ),
        "http_headers_wait_s_p99": round(
            float(http_transport_metrics["http_headers_wait_s_p99"]),
            6,
        ),
        "http_body_read_s_p50": round(
            float(http_transport_metrics["http_body_read_s_p50"]),
            6,
        ),
        "http_body_read_s_p95": round(
            float(http_transport_metrics["http_body_read_s_p95"]),
            6,
        ),
        "http_body_read_s_p99": round(
            float(http_transport_metrics["http_body_read_s_p99"]),
            6,
        ),
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
        "vllm_prefix_cache_queries_delta": vllm_stats["vllm_prefix_cache_queries_delta"],
        "vllm_prefix_cache_hits_delta": vllm_stats["vllm_prefix_cache_hits_delta"],
        "vllm_prefix_cache_hit_rate": round(float(vllm_stats["vllm_prefix_cache_hit_rate"]), 6),
        "vllm_latency_histogram_status": vllm_stats[
            "vllm_latency_histogram_status"
        ],
        "vllm_ttft_histogram_status": vllm_stats[
            "vllm_ttft_histogram_status"
        ],
        "vllm_itl_histogram_status": vllm_stats[
            "vllm_itl_histogram_status"
        ],
        "vllm_time_to_first_token_mean_s": round(float(vllm_stats["vllm_time_to_first_token_mean_s"]), 6),
        "vllm_time_to_first_token_p50_s": round(
            float(vllm_stats["vllm_time_to_first_token_p50_s"]), 6
        ),
        "vllm_time_to_first_token_p95_s": round(
            float(vllm_stats["vllm_time_to_first_token_p95_s"]), 6
        ),
        "vllm_time_to_first_token_p99_s": round(
            float(vllm_stats["vllm_time_to_first_token_p99_s"]), 6
        ),
        "vllm_inter_token_latency_mean_s": round(
            float(vllm_stats["vllm_inter_token_latency_mean_s"]), 6
        ),
        "vllm_inter_token_latency_p50_s": round(
            float(vllm_stats["vllm_inter_token_latency_p50_s"]), 6
        ),
        "vllm_inter_token_latency_p95_s": round(
            float(vllm_stats["vllm_inter_token_latency_p95_s"]), 6
        ),
        "vllm_inter_token_latency_p99_s": round(
            float(vllm_stats["vllm_inter_token_latency_p99_s"]), 6
        ),
        "ttft_slo_target_ms": args.ttft_slo_ms,
        "itl_slo_target_ms": args.itl_slo_ms,
        **slo_scale_metrics,
        **cost_metrics,
        "first_batch_ready_epoch_s": first_batch_ready_epoch_s,
        "result_visible_epoch_s": result_visible_epoch_s,
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
        "scheduling_control_overhead_s": round(
            float(organizer_metrics["organizer_plan_s"])
            + float(submit_metrics["submit_s"]),
            6,
        ),
        "scheduling_control_overhead_pct": round(
            100.0
            * (
                float(organizer_metrics["organizer_plan_s"])
                + float(submit_metrics["submit_s"])
            )
            / operator_wall_s
            if operator_wall_s > 0
            else 0.0,
            6,
        ),
        "writeback_s": round(writeback_s, 6),
        "e2e_s": round(e2e_s, 6),
        "rows_per_s": round(processed_rows / e2e_s, 3) if e2e_s else 0.0,
        "tokens_per_s": round(_vllm_tokens_per_second(vllm_stats, e2e_s), 3),
        "model_request_tokens_per_s": round(
            _vllm_tokens_per_second(vllm_stats, request_wall_s),
            3,
        ),
        "operator_tokens_per_s": round(
            _vllm_tokens_per_second(vllm_stats, operator_wall_s),
            3,
        ),
    })


@dataclass(frozen=True)
class _ProfileRayRuntime:
    actor_pools: dict[str, list[object]]
    actor_endpoint_urls: dict[str, str]
    ray_module: object | None
    ray_version: str
    remote_embed: object | None
    actor_submission_state: ActorSubmissionState | None
    actor_ready_s: float


def _initialize_profile_ray_runtime(
    args: argparse.Namespace,
    resolved: _ResolvedProfileRun,
) -> _ProfileRayRuntime:
    actor_pools: dict[str, list[object]] = {}
    actor_endpoint_urls: dict[str, str] = {}
    ray_module = None
    ray_version = ""
    remote_embed = None
    if args.executor in {"ray_actor", "ray_task"}:
        ray_module = require_ray()
        ray_version = str(getattr(ray_module, "__version__", ""))
        ray_init_options = {
            "ignore_reinit_error": True,
            "runtime_env": ray_runtime_env(),
        }
        if resolved.ray_address:
            ray_init_options["address"] = resolved.ray_address
        ray_module.init(**ray_init_options)
        if args.executor == "ray_actor":
            actor_endpoint_urls = {
                f"endpoint-{index}": endpoint_url
                for index, endpoint_url in enumerate(
                    ["ray://actor/fake"]
                    if resolved.model_backend == "fake"
                    else resolved.endpoint_urls
                )
            }
            if args.operator == "ai_complete" and resolved.model_backend == "fake":
                remote_actor = _remote_actor_class(
                    ray_module,
                    FakeCompletionActor,
                    resolved.worker_options,
                )
                actor_pools = {
                    endpoint_id: [
                        remote_actor.remote(args.completion_max_tokens)
                        for _ in range(resolved.actor_workers_per_endpoint)
                    ]
                    for endpoint_id in actor_endpoint_urls
                }
            elif args.operator == "ai_complete":
                if resolved.model_backend == "ollama":
                    actor_cls = OllamaCompletionActor
                elif args.completion_http_transport == "httpx_async":
                    actor_cls = CompatibleAsyncHTTPCompletionActor
                else:
                    actor_cls = CompatibleHTTPCompletionActor
                remote_actor = _remote_actor_class(
                    ray_module,
                    actor_cls,
                    resolved.worker_options,
                )
                for endpoint_id, endpoint_url in actor_endpoint_urls.items():
                    actor_args = [
                        endpoint_url,
                        resolved.model_name,
                        resolved.api_key,
                        resolved.request_timeout_s,
                        args.completion_max_tokens,
                    ]
                    if resolved.model_backend == "compatible_http":
                        actor_args.extend([
                            args.completion_return_token_ids,
                            args.completion_prompt_format,
                            args.completion_temperature,
                            args.completion_protocol,
                        ])
                        if args.completion_http_transport == "httpx_async":
                            actor_args.extend([
                                resolved.reported_ray_actor_max_concurrency,
                                args.completion_ignore_eos,
                                args.completion_http_keepalive_expiry_s,
                            ])
                        else:
                            actor_args.append(args.completion_ignore_eos)
                    actor_pools[endpoint_id] = [
                        remote_actor.remote(*actor_args)
                        for _ in range(resolved.actor_workers_per_endpoint)
                    ]
            elif resolved.model_backend == "fake":
                remote_actor = _remote_actor_class(
                    ray_module,
                    FakeEmbeddingActor,
                    resolved.worker_options,
                )
                actor_pools = {
                    endpoint_id: [
                        remote_actor.remote(args.embedding_dim)
                        for _ in range(resolved.actor_workers_per_endpoint)
                    ]
                    for endpoint_id in actor_endpoint_urls
                }
            else:
                remote_actor = _remote_actor_class(
                    ray_module,
                    CompatibleHTTPEmbeddingActor,
                    resolved.worker_options,
                )
                actor_pools = {
                    endpoint_id: [
                        remote_actor.remote(
                            endpoint_url,
                            resolved.model_name,
                            resolved.api_key,
                            resolved.request_timeout_s,
                        )
                        for _ in range(resolved.actor_workers_per_endpoint)
                    ]
                    for endpoint_id, endpoint_url in actor_endpoint_urls.items()
                }
        elif args.operator == "ai_complete" and resolved.model_backend == "fake":
            remote_embed = _remote_task(
                ray_module,
                fake_complete_batch,
                resolved.worker_options,
            )
        elif args.operator == "ai_complete" and resolved.model_backend == "ollama":
            remote_embed = _remote_task(
                ray_module,
                ollama_complete_batch,
                resolved.worker_options,
            )
        elif args.operator == "ai_complete":
            remote_embed = _remote_task(
                ray_module,
                compatible_http_complete_batch,
                resolved.worker_options,
            )
        elif resolved.model_backend == "fake":
            remote_embed = _remote_task(
                ray_module,
                fake_embed_batch,
                resolved.worker_options,
            )
        else:
            remote_embed = _remote_task(
                ray_module,
                compatible_http_embed_batch,
                resolved.worker_options,
            )
    actor_submission_state = (
        ActorSubmissionState(
            actor_pools,
            "complete" if args.operator == "ai_complete" else "embed",
            max_concurrency_per_worker=(
                resolved.reported_ray_actor_max_concurrency
            ),
            routing_policy=args.actor_worker_routing,
        )
        if args.executor == "ray_actor"
        else None
    )
    actor_ready_s = 0.0
    if actor_submission_state is not None:
        actor_ready_s, _ = actor_submission_state.wait_until_ready(ray_module)
    return _ProfileRayRuntime(
        actor_pools=actor_pools,
        actor_endpoint_urls=actor_endpoint_urls,
        ray_module=ray_module,
        ray_version=ray_version,
        remote_embed=remote_embed,
        actor_submission_state=actor_submission_state,
        actor_ready_s=actor_ready_s,
    )


def _initialize_profile_adaptive_config(
    args: argparse.Namespace,
    resolved: _ResolvedProfileRun,
    ray_runtime: _ProfileRayRuntime,
    control_trace_events: list,
    observation_providers: list,
) -> dict[str, object] | None:
    if args.scheduling_policy == "queue_adaptive":
        return {
            "metrics_url": (
                resolved.resolved_metrics_urls[0]
                if resolved.resolved_metrics_urls
                else None
            ),
            "min_inflight": args.adaptive_min_inflight,
            "max_inflight": args.adaptive_max_inflight,
            "queue_threshold": args.adaptive_queue_threshold,
            "running_threshold": args.adaptive_running_threshold,
            "kv_threshold": args.adaptive_kv_threshold,
            "poll_interval_s": args.adaptive_poll_interval_s,
        }
    if args.scheduling_policy not in resolved.typed_adaptive_policies:
        return None
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
    adaptive_kwargs = {
        "scheduling_policy": args.scheduling_policy,
        "metrics_urls": resolved.resolved_metrics_urls,
        "trace_events": control_trace_events,
        "min_window": min_window,
        "max_window": args.controller_max_window,
        "initial_window": initial_window,
        "sample_interval_s": args.adaptive_sample_interval_s,
        "ewma_alpha": args.ewma_alpha,
        "pid_proportional_gain": args.pid_proportional_gain,
        "pid_integral_gain": args.pid_integral_gain,
        "pid_derivative_gain": args.pid_derivative_gain,
        "hol_age_congestion_s": args.hol_age_congestion_s,
        "hol_age_low_load_s": args.hol_age_low_load_s,
        "additive_increase": args.controller_additive_increase,
        "multiplicative_decrease": args.controller_multiplicative_decrease,
        "congestion_kv_usage": args.controller_congestion_kv_usage,
        "low_load_kv_usage": args.controller_low_load_kv_usage,
        "low_load_running": args.controller_low_load_running,
    }
    try:
        if args.admission_scope == "per_endpoint":
            endpoint_ids = (
                list(ray_runtime.actor_endpoint_urls)
                if args.executor == "ray_actor"
                else [
                    f"task-{index}"
                    for index in range(resolved.routing_endpoint_count)
                ]
            )
            config = _build_per_endpoint_adaptive_config(
                endpoint_ids=endpoint_ids,
                **adaptive_kwargs,
            )
            observation_providers.extend(config["observation_providers"])
            return config
        config = _build_adaptive_config(**adaptive_kwargs)
        observation_providers.append(config["observation_provider"])
        return config
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def _submit_profile_batches(
    args: argparse.Namespace,
    resolved: _ResolvedProfileRun,
    ray_runtime: _ProfileRayRuntime,
    adaptive_config: dict[str, object] | None,
    submission_lifecycle_events: list[SubmissionLifecycleEvent],
    lifecycle_epoch_clock: MonotonicEpochClock | None,
    batches: Iterable[pa.RecordBatch | pa.Table],
    replay_envelopes: Iterable[PayloadEnvelope] | None = None,
) -> tuple[list[dict], dict]:
    lifecycle_sink = (
        submission_lifecycle_events
        if args.request_trace_output or args.submission_trace_output
        else None
    )
    if args.executor == "ray_actor":
        return submit_with_backpressure(
            ray_module=ray_runtime.ray_module,
            actor_pools=ray_runtime.actor_pools,
            endpoint_urls=ray_runtime.actor_endpoint_urls,
            batches=batches,
            max_inflight=resolved.effective_global_inflight_limit,
            method_name=(
                "complete" if args.operator == "ai_complete" else "embed"
            ),
            adaptive_config=adaptive_config,
            routing_config=resolved.routing_config,
            replay_envelopes=replay_envelopes,
            submission_lifecycle_sink=lifecycle_sink,
            epoch_clock=lifecycle_epoch_clock,
            output_cost_mode=args.output_cost_mode,
            completion_max_tokens=args.completion_max_tokens,
            completion_prompt_token_overhead=(
                args.completion_prompt_token_overhead
            ),
            submission_state=ray_runtime.actor_submission_state,
            per_endpoint_limit=resolved.per_endpoint_inflight_limit,
            per_endpoint_work_limit=resolved.per_endpoint_work_limit,
            shared_credit_config=resolved.shared_credit_config,
        )
    if args.executor == "ray_task":
        return submit_ray_tasks(
            ray_runtime.ray_module,
            ray_runtime.remote_embed,
            batches,
            resolved.effective_global_inflight_limit,
            args.operator,
            args.embedding_dim,
            resolved.model_backend,
            resolved.endpoint_urls,
            resolved.model_name,
            resolved.api_key,
            resolved.request_timeout_s,
            args.completion_max_tokens,
            adaptive_config,
            resolved.routing_config,
            replay_envelopes=replay_envelopes,
            submission_lifecycle_sink=lifecycle_sink,
            epoch_clock=lifecycle_epoch_clock,
            per_endpoint_limit=resolved.per_endpoint_inflight_limit,
            per_endpoint_work_limit=resolved.per_endpoint_work_limit,
            shared_credit_config=resolved.shared_credit_config,
            output_cost_mode=args.output_cost_mode,
            completion_return_token_ids=args.completion_return_token_ids,
            completion_prompt_format=args.completion_prompt_format,
            completion_temperature=args.completion_temperature,
            completion_protocol=args.completion_protocol,
            completion_ignore_eos=args.completion_ignore_eos,
            completion_prompt_token_overhead=(
                args.completion_prompt_token_overhead
            ),
        )
    if replay_envelopes is not None:
        raise RuntimeError("arrival replay requires a Ray executor")
    if resolved.model_backend == "fake":
        if args.operator == "ai_complete":
            return submit_python_completion_batches(
                batches,
                args.completion_max_tokens,
            )
        return submit_python_batches(batches, args.embedding_dim)
    return submit_python_compatible_http_batches(
        batches,
        args.operator,
        resolved.endpoint_urls,
        resolved.model_name,
        resolved.api_key,
        resolved.request_timeout_s,
        args.completion_max_tokens,
        resolved.model_backend,
        args.completion_return_token_ids,
        args.completion_prompt_format,
        args.completion_temperature,
        args.completion_protocol,
        args.completion_ignore_eos,
    )


class _ProfileOperatorRun:
    """Own one connected profiler run and its external resource lifecycle."""

    def __init__(
        self,
        args: argparse.Namespace,
        resolved: _ResolvedProfileRun,
        ray_runtime: _ProfileRayRuntime,
        *,
        conn,
        job_id: object,
        phase: str,
        repeat_index: int,
        db_metadata: dict,
        gpu_snapshot: dict,
        current_vector_dim: object,
    ) -> None:
        self.args = args
        self.resolved = resolved
        self.ray_runtime = ray_runtime
        self.conn = conn
        self.job_id = job_id
        self.phase = phase
        self.repeat_index = repeat_index
        self.db_metadata = db_metadata
        self.gpu_snapshot = gpu_snapshot
        self.current_vector_dim = current_vector_dim
        self.resource_sampler = None
        self.adaptive_observation_providers: list[object] = []

    def _initialize(self) -> None:
        args = self.args
        self.e2e_timer = StageTimer.start("e2e")
        self.processed_rows = 0
        self.object_count = 0
        self.arrow_build_s = 0.0
        self.db_fetch_s = 0.0
        self.first_batch_ready_epoch_s: float | None = None
        self.operator_results: list[dict] = []
        self.source_scan_evidence_rows: list[dict] = []
        self.request_lifecycle_seeds: list[RequestLifecycleSeed] = []
        self.submission_lifecycle_events: list[SubmissionLifecycleEvent] = []
        self.request_trace_rows: tuple[RequestTraceRow, ...] = ()
        self.packing_batch_cost_units: list[int] = []
        self.packing_batch_row_counts: list[int] = []
        self.organizer_calls = 0
        self.organizer_packing_scopes: list[str] = []
        self.replay_packing: list[tuple[int, int]] = []
        self.service_quanta: list[tuple[int, int, bool]] = []
        self.lifecycle_epoch_clock = (
            MonotonicEpochClock() if args.request_trace_output else None
        )
        self.offline_job_start_epoch_s = (
            self.lifecycle_epoch_clock()
            if self.lifecycle_epoch_clock is not None and not args.arrival_replay
            else None
        )
        self.offline_batch_index = 0
        self.submit_metrics = {
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
            "max_active_work_per_endpoint_seen": 0,
            "max_ready_requests_seen": 0,
            "max_ready_work_seen": 0,
            "max_ready_payload_bytes_seen": 0,
            "ready_requests_transition_samples": [],
            "ready_work_transition_samples": [],
            "ready_payload_bytes_transition_samples": [],
        }
        self.operator_wall_s = 0.0
        self.vllm_metrics_before = _scrape_model_metrics(
            self.resolved.resolved_metrics_urls
        )
        if args.resource_trace_output:
            self.resource_sampler = PeriodicSampler(
                lambda: _resource_snapshot(
                    self.resolved.resolved_metrics_urls,
                    self.resolved.sampled_gpu_ids,
                ),
                interval_s=args.resource_sample_interval_s,
            )
        if args.data_source == "daft_postgres" or args.organizer == "daft":
            configure_daft_runner(args.daft_runner)
        self.source = make_source(args.data_source)
        organizer_config = OrganizerConfig(
            batch_size=1 if args.strategy == "fine" else args.ray_batch_rows,
            partition_mode=args.organizer_partition_mode,
            partitions=args.organizer_partitions,
            runner=args.daft_runner,
            batching_policy=args.batching_policy,
            token_budget=args.token_budget,
            completion_max_tokens=(
                args.completion_max_tokens
                if args.operator == "ai_complete"
                else 0
            ),
            output_cost_mode=args.output_cost_mode,
        )
        self.organizer = (
            None
            if args.arrival_replay
            else make_organizer(args.organizer, organizer_config)
        )
        self.organizer_metrics = {
            "organizer_from_arrow_s": 0.0,
            "organizer_plan_s": 0.0,
            "organizer_collect_s": 0.0,
            "partition_effective": True,
            "organization_policy_family": "none",
            "batch_prompt_token_spread_mean": 0.0,
            "prefix_group_ratio": 0.0,
            "packing_padding_slots": 0,
            "packing_padding_capacity_slots": 0,
        }
        self.control_trace_events: list = []
        self.adaptive_config = _initialize_profile_adaptive_config(
            args,
            self.resolved,
            self.ray_runtime,
            self.control_trace_events,
            self.adaptive_observation_providers,
        )
        self.organizer_warnings: list[str] = []
        self.replay_tables: list[pa.Table] = []
        self.flush_trace_events: list = []
        self.arrival_replay_observed_start_epoch_s = 0.0

    def _submit_operator_batches(
        self,
        batches: Iterable[pa.RecordBatch | pa.Table],
        replay_envelopes: Iterable[PayloadEnvelope] | None = None,
    ) -> tuple[list[dict], dict]:
        return _submit_profile_batches(
            self.args,
            self.resolved,
            self.ray_runtime,
            self.adaptive_config,
            self.submission_lifecycle_events,
            self.lifecycle_epoch_clock,
            batches,
            replay_envelopes,
        )

    def _record_organized_batch(self, table: pa.Table) -> None:
        args = self.args
        if self.organizer is None:
            raise RuntimeError("non-replay execution requires an organizer")
        organized = self.organizer.organize(table)
        self.organizer_calls += 1
        ray_batches = organized.batches
        self.packing_batch_cost_units.extend(organized.batch_cost_units)
        self.packing_batch_row_counts.extend(organized.batch_row_counts)
        self.organizer_packing_scopes.append(
            str(organized.metrics.get("packing_scope", "organizer_input"))
        )
        for field in (
            "organizer_from_arrow_s",
            "organizer_plan_s",
            "organizer_collect_s",
        ):
            self.organizer_metrics[field] += float(organized.metrics[field])
        self.organizer_metrics["organization_policy_family"] = str(
            organized.metrics.get("organization_policy_family", "none")
        )
        self.organizer_metrics["batch_prompt_token_spread_mean"] += float(
            organized.metrics.get("batch_prompt_token_spread_mean", 0.0)
        )
        self.organizer_metrics["prefix_group_ratio"] = max(
            float(self.organizer_metrics["prefix_group_ratio"]),
            float(organized.metrics.get("prefix_group_ratio", 0.0)),
        )
        for field in (
            "packing_padding_slots",
            "packing_padding_capacity_slots",
        ):
            self.organizer_metrics[field] += int(organized.metrics.get(field, 0))
        self.organizer_metrics["partition_effective"] = (
            bool(self.organizer_metrics["partition_effective"])
            and str(organized.metrics["partition_effective"]) == "true"
        )
        if organized.metrics["warnings"]:
            self.organizer_warnings.append(str(organized.metrics["warnings"]))
        self.object_count += len(ray_batches)
        offline_envelopes = None
        if (
            args.request_trace_output
            or args.submission_granularity in {"request", "service_quantum"}
        ):
            if args.request_trace_output and (
                self.lifecycle_epoch_clock is None
                or self.offline_job_start_epoch_s is None
            ):
                raise RuntimeError("offline lifecycle clock is not initialized")
            offline_envelopes, offline_seeds = _offline_batch_envelopes(
                ray_batches,
                job_id=str(self.job_id),
                operator=args.operator,
                completion_max_tokens=(
                    args.completion_max_tokens
                    if args.operator == "ai_complete"
                    else 0
                ),
                output_cost_mode=args.output_cost_mode,
                batch_index_start=self.offline_batch_index,
                job_start_epoch_s=self.offline_job_start_epoch_s or 0.0,
                ready_epoch_s=(
                    self.lifecycle_epoch_clock()
                    if self.lifecycle_epoch_clock is not None
                    else 0.0
                ),
                submission_granularity=args.submission_granularity,
                service_quantum_tokens=args.service_quantum_tokens,
                prompt_token_overhead_per_request=(
                    args.completion_prompt_token_overhead
                ),
                quantum_sink=self.service_quanta,
            )
            if args.request_trace_output:
                self.request_lifecycle_seeds.extend(offline_seeds)
            if args.submission_granularity in {"request", "service_quantum"}:
                self.object_count += len(offline_envelopes) - len(ray_batches)
            self.offline_batch_index += len(ray_batches)
        operator_timer = StageTimer.start("operator_wall")
        results, metrics = self._submit_operator_batches(
            ray_batches,
            replay_envelopes=offline_envelopes,
        )
        self.operator_wall_s += operator_timer.stop()
        self.operator_results.extend(results)
        _merge_submit_metrics(self.submit_metrics, metrics)

    def _run_source(self) -> None:
        args = self.args
        offset = args.source_row_offset
        while self.processed_rows < args.total_rows:
            source_config = SourceConfig(
                limit=args.db_fetch_rows,
                offset=offset,
                workload_name=args.source_workload_name,
                order=args.source_order,
                max_prompt_tokens=args.source_max_prompt_tokens,
                doc_ids=self.resolved.request_manifest_doc_ids,
            )
            source_batch = (
                self.source.fetch(self.conn, source_config)
                if args.data_source == "arrow_postgres"
                else self.source.fetch(args.database_url, source_config)
            )
            table = source_batch.table
            if table is None:
                break
            self.db_fetch_s += source_batch.metrics["db_fetch_s"]
            self.arrow_build_s += source_batch.metrics["arrow_build_s"]
            offset += table.num_rows
            remaining = args.total_rows - self.processed_rows
            if table.num_rows > remaining:
                table = table.slice(0, remaining)
            if args.source_scan_evidence_output:
                self.source_scan_evidence_rows.extend(
                    _source_scan_fingerprint_rows(table)
                )
            if self.resolved.request_manifest_guard is not None:
                table = self.resolved.request_manifest_guard.validate_and_annotate(
                    table
                )
            if self.first_batch_ready_epoch_s is None:
                self.first_batch_ready_epoch_s = time.time()
            if args.arrival_replay:
                self.replay_tables.append(table)
            else:
                self._record_organized_batch(table)
            self.processed_rows += table.num_rows
        if args.arrival_replay:
            self._run_replay()

    def _run_replay(self) -> None:
        args = self.args
        _wait_for_replay_start(args.arrival_replay_start_epoch_s)
        observation_provider = None
        if _requires_replay_feedback(args):
            if not self.resolved.resolved_metrics_urls and not args.dry_run:
                raise SystemExit(
                    "feedback-driven flush or token budget requires "
                    "--model-metrics-url(s)"
                )
            observation_provider = NonBlockingMetricsObservationProvider(
                lambda: (
                    _service_metrics_snapshot(self.resolved.resolved_metrics_urls)
                    if self.resolved.resolved_metrics_urls
                    else None
                ),
                poll_interval_s=0.25,
                stale_after_s=0.5,
            )
        service_observation = observation_provider or (
            lambda: ReplayServiceObservation(
                fresh=False,
                running=None,
                waiting=None,
                kv_usage=None,
            )
        )
        try:
            self.arrival_replay_observed_start_epoch_s = time.time()
            replay_envelopes = _arrival_replay_envelopes(
                self.replay_tables,
                args,
                job_id=str(self.job_id),
                operator=args.operator,
                service_observation=service_observation,
                trace_sink=self.flush_trace_events,
                lifecycle_seed_sink=(
                    self.request_lifecycle_seeds
                    if args.request_trace_output
                    else None
                ),
                packing_sink=self.replay_packing,
                quantum_sink=self.service_quanta,
                epoch_clock=self.lifecycle_epoch_clock,
                service_endpoint_count=len(self.resolved.endpoint_urls),
                replay_origin_epoch_s=(
                    args.arrival_replay_start_epoch_s
                    if args.arrival_replay_start_epoch_s > 0
                    else None
                ),
            )
            operator_timer = StageTimer.start("operator_wall")
            results, metrics = self._submit_operator_batches(
                (),
                replay_envelopes=replay_envelopes,
            )
            self.operator_wall_s += operator_timer.stop()
        finally:
            if observation_provider is not None:
                observation_provider.close()
        self.operator_results.extend(results)
        self.object_count = metrics["operator_invocations"]
        _merge_submit_metrics(self.submit_metrics, metrics)

    def _raise_on_missing_results(self) -> None:
        missing_result_count = sum(
            not isinstance(result, dict) for result in self.operator_results
        )
        if not missing_result_count:
            return
        lifecycle_errors = [
            event.error for event in self.submission_lifecycle_events
            if event.error
        ]
        detail = lifecycle_errors[0] if lifecycle_errors else "unavailable"
        raise RuntimeError(
            "model submission produced "
            f"{missing_result_count} missing result(s); "
            f"first lifecycle error: {detail}"
        )

    def _write_results(self) -> tuple[int, float]:
        timer = StageTimer.start("writeback")
        if self.args.operator == "ai_complete":
            plan = prepare_completion_write(
                self.operator_results,
                self.args.writeback_mode,
            )
        else:
            plan = prepare_embedding_write(
                self.operator_results,
                self.args.writeback_mode,
            )
        written_rows = execute_write_plan(
            self.conn,
            plan,
            self.args.write_batch_rows,
        )
        if plan is not None:
            self.conn.commit()
        return written_rows, timer.stop()

    def _validate_result_visibility(self) -> None:
        guard = self.resolved.request_manifest_guard
        self.request_manifest_validated_rows = 0
        self.request_manifest_validation_status = (
            self.resolved.request_manifest_validation_status
        )
        if guard is not None:
            evidence = guard.finish()
            self.request_manifest_validated_rows = evidence.validated_rows
            self.request_manifest_validation_status = "ok"
        if self.first_batch_ready_epoch_s is None:
            raise RuntimeError("source produced no first batch")
        _validate_operator_result_visibility(
            self.operator_results,
            self.processed_rows,
        )
        self.result_visible_epoch_s = time.time()
        self.database_e2e_boundary_complete = False
        if self.args.database_e2e_timing_boundary:
            self._raise_on_missing_results()
            if self.resource_sampler is not None:
                self.resource_sampler.close()
            self.written_rows, self.writeback_s = self._write_results()
            self.e2e_s = self.e2e_timer.stop()
            self.database_e2e_boundary_complete = True

    def _write_request_and_resource_evidence(self) -> None:
        args = self.args
        self.request_trace_path = args.request_trace_output or ""
        if self.request_trace_path:
            self.request_trace_rows = _build_profiler_request_rows(
                self.request_lifecycle_seeds,
                self.submission_lifecycle_events,
                self.operator_results,
                operator=args.operator,
                slo_target_s=(
                    args.request_slo_ms / 1000.0
                    if args.request_slo_ms > 0
                    else None
                ),
            )
            _write_request_trace(
                Path(self.request_trace_path),
                experiment_id=args.experiment_id,
                phase=self.phase,
                repeat_index=self.repeat_index,
                scenario_id=args.scenario_id,
                random_seed=args.random_seed,
                job_id=self.job_id,
                server_version=self.db_metadata["server_version"],
                pgvector_version=self.db_metadata["pgvector_version"],
                rows=self.request_trace_rows,
            )
            if args.completion_evidence_output:
                _write_completion_evidence(
                    Path(args.completion_evidence_output),
                    rows=self.request_trace_rows,
                    operator_results=self.operator_results,
                )
        if args.source_scan_evidence_output:
            _write_source_scan_evidence(
                Path(args.source_scan_evidence_output),
                rows=self.source_scan_evidence_rows,
            )
        self.resource_samples: list = []
        if self.resource_sampler is not None:
            self.resource_sampler.close()
            self.resource_samples = list(self.resource_sampler.samples)
        self.submission_trace_path = args.submission_trace_output or ""
        if self.submission_trace_path:
            _write_submission_trace(
                Path(self.submission_trace_path),
                experiment_id=args.experiment_id,
                phase=self.phase,
                repeat_index=self.repeat_index,
                job_id=self.job_id,
                server_version=self.db_metadata["server_version"],
                pgvector_version=self.db_metadata["pgvector_version"],
                results=self.operator_results,
                submission_events=(
                    self.submission_lifecycle_events
                    if self.submission_lifecycle_events
                    else None
                ),
            )
        self._raise_on_missing_results()
        self.resource_trace_path = args.resource_trace_output or ""
        if self.resource_trace_path:
            _write_resource_trace(
                Path(self.resource_trace_path),
                experiment_id=args.experiment_id,
                phase=self.phase,
                repeat_index=self.repeat_index,
                job_id=self.job_id,
                server_version=self.db_metadata["server_version"],
                pgvector_version=self.db_metadata["pgvector_version"],
                samples=self.resource_samples,
            )

    def _write_control_evidence(self) -> None:
        args = self.args
        self.flush_trace_path = ""
        if args.arrival_replay:
            self.flush_trace_path = args.flush_trace_output
            if not self.flush_trace_path:
                main_output = Path(args.output)
                self.flush_trace_path = str(
                    main_output.with_name(
                        f"{main_output.stem}_flush_trace.csv"
                    )
                )
            _write_flush_trace(
                Path(self.flush_trace_path),
                experiment_id=args.experiment_id,
                phase=self.phase,
                repeat_index=self.repeat_index,
                job_id=self.job_id,
                server_version=self.db_metadata["server_version"],
                pgvector_version=self.db_metadata["pgvector_version"],
                flush_policy=args.flush_policy,
                flush_timeout_ms=args.flush_timeout_ms,
                flush_max_wait_ms=args.flush_max_wait_ms,
                arrival_time_scale=args.arrival_time_scale,
                trace_events=self.flush_trace_events,
            )
        self.control_trace_path = ""
        if self.adaptive_config is None or not (
            "admission_gate" in self.adaptive_config
            or "per_endpoint_gates" in self.adaptive_config
        ):
            return
        trace_events = self.adaptive_config["trace_events"]
        self.submit_metrics["adaptive_downshifts"] = sum(
            event.controller_action == "decrease" for event in trace_events
        )
        self.submit_metrics["adaptive_upshifts"] = sum(
            event.controller_action == "increase" for event in trace_events
        )
        self.submit_metrics["adaptive_limit_mean"] = (
            statistics.mean(event.window for event in trace_events)
            if trace_events
            else self.adaptive_config["admission_gate"].limit
            if "admission_gate" in self.adaptive_config
            else statistics.mean(
                gate.limit
                for gate in self.adaptive_config["per_endpoint_gates"].values()
            )
        )
        self.control_trace_path = args.control_trace_output
        if not self.control_trace_path:
            main_output = Path(args.output)
            self.control_trace_path = str(
                main_output.with_name(
                    f"{main_output.stem}_control_trace.csv"
                )
            )
        _write_control_trace(
            Path(self.control_trace_path),
            experiment_id=args.experiment_id,
            phase=self.phase,
            repeat_index=self.repeat_index,
            job_id=self.job_id,
            server_version=self.db_metadata["server_version"],
            pgvector_version=self.db_metadata["pgvector_version"],
            controller_name=self.adaptive_config["controller_name"],
            trace_events=trace_events,
        )

    def _packing_metrics(self) -> tuple[dict, dict, dict]:
        args = self.args
        if args.arrival_replay:
            batch_cost_units = [cost for cost, _ in self.replay_packing]
            batch_row_counts = [rows for _, rows in self.replay_packing]
            packing_scope = "arrival_order"
            packing_algorithm = "sequential_pending"
        else:
            batch_cost_units = self.packing_batch_cost_units
            batch_row_counts = self.packing_batch_row_counts
            packing_scope = (
                "fetch_chunk_local"
                if self.organizer_calls > 1
                else "partition_local"
                if "partition_local" in self.organizer_packing_scopes
                else "organizer_input"
            )
            packing_algorithm = packing_algorithm_name(args.batching_policy)
        packing = _packing_run_metrics(
            batch_cost_units,
            batch_row_counts,
            capacity=(
                args.token_budget
                if args.batching_policy.endswith("token_budget")
                else 0
            ),
            row_cap=args.ray_batch_rows,
            packing_scope=packing_scope,
            packing_algorithm=packing_algorithm,
            padding_slots=int(self.organizer_metrics["packing_padding_slots"]),
            padding_capacity_slots=int(
                self.organizer_metrics["packing_padding_capacity_slots"]
            ),
            padding_observed=not args.arrival_replay,
        )
        quantum = _service_quantum_run_metrics(
            self.service_quanta,
            target_tokens=args.service_quantum_tokens,
        )
        worker = _actor_worker_run_metrics(
            self.ray_runtime.actor_submission_state,
            routing_policy=(
                args.actor_worker_routing
                if args.executor == "ray_actor"
                else ""
            ),
            actor_ready_s=self.ray_runtime.actor_ready_s,
            slots_per_endpoint=self.resolved.actor_pool_slots_per_endpoint,
            operator_wall_s=self.operator_wall_s,
        )
        return packing, quantum, worker

    def _finish_and_build_result(self) -> dict:
        args = self.args
        vllm_metrics_after = _scrape_model_metrics(
            self.resolved.resolved_metrics_urls
        )
        if not self.database_e2e_boundary_complete:
            self.written_rows, self.writeback_s = self._write_results()
        finish_job(self.conn, self.job_id)
        if not self.database_e2e_boundary_complete:
            self.e2e_s = self.e2e_timer.stop()
        request_metrics = _request_trace_metrics(
            self.request_trace_rows,
            e2e_s=self.e2e_s,
        )
        service_s = sum(
            float(result["service_s"]) for result in self.operator_results
        )
        request_wall_s = model_request_wall_time(self.operator_results)
        token_count = sum(
            int(result["token_count"]) for result in self.operator_results
        )
        batch_stats = batch_result_stats(self.operator_results)
        http_transport_metrics = _http_transport_metrics(self.operator_results)
        vllm_stats = vllm_metric_delta_stats(
            self.vllm_metrics_before,
            vllm_metrics_after,
        )
        slo_scale_metrics = observed_slo_scale_metrics(
            vllm_stats,
            ttft_target_ms=args.ttft_slo_ms,
            itl_target_ms=args.itl_slo_ms,
        )
        cost_metrics = token_cost_metrics(
            vllm_stats,
            input_price=args.input_cost_per_million_tokens_usd,
            output_price=args.output_cost_per_million_tokens_usd,
        )
        observed_tokens = (
            int(vllm_stats["vllm_prompt_tokens_delta"])
            + int(vllm_stats["vllm_generation_tokens_delta"])
        )
        resource_metrics = resource_sample_stats(
            self.resource_samples,
            observed_tokens=observed_tokens,
        )
        mfu_metrics = estimate_mfu(
            estimated_flops=float(
                vllm_stats["vllm_estimated_flops_per_gpu_delta"]
            ),
            observed_tokens=observed_tokens,
            operator_wall_s=self.operator_wall_s,
            model_flops_per_token=args.model_flops_per_token,
            gpu_peak_tflops=args.gpu_peak_tflops,
            precision=args.mfu_precision,
        )
        packing_metrics, quantum_metrics, worker_metrics = self._packing_metrics()
        data = _ProfileResultData(
            phase=self.phase,
            repeat_index=self.repeat_index,
            adaptive_config=self.adaptive_config,
            arrival_replay_observed_start_epoch_s=(
                self.arrival_replay_observed_start_epoch_s
            ),
            arrow_build_s=self.arrow_build_s,
            batch_stats=batch_stats,
            control_trace_events=self.control_trace_events,
            control_trace_path=self.control_trace_path,
            cost_metrics=cost_metrics,
            current_vector_dim=self.current_vector_dim,
            db_fetch_s=self.db_fetch_s,
            db_metadata=self.db_metadata,
            e2e_s=self.e2e_s,
            first_batch_ready_epoch_s=self.first_batch_ready_epoch_s,
            flush_trace_events=self.flush_trace_events,
            flush_trace_path=self.flush_trace_path,
            gpu_snapshot=self.gpu_snapshot,
            http_transport_metrics=http_transport_metrics,
            job_id=self.job_id,
            mfu_metrics=mfu_metrics,
            object_count=self.object_count,
            operator_results=self.operator_results,
            operator_wall_s=self.operator_wall_s,
            organizer_metrics=self.organizer_metrics,
            organizer_warnings=self.organizer_warnings,
            packing_metrics=packing_metrics,
            processed_rows=self.processed_rows,
            ray_version=self.ray_runtime.ray_version,
            request_manifest_validated_rows=(
                self.request_manifest_validated_rows
            ),
            request_manifest_validation_status=(
                self.request_manifest_validation_status
            ),
            request_metrics=request_metrics,
            request_trace_path=self.request_trace_path,
            request_trace_rows=self.request_trace_rows,
            request_wall_s=request_wall_s,
            resource_metrics=resource_metrics,
            resource_samples=self.resource_samples,
            resource_trace_path=self.resource_trace_path,
            result_visible_epoch_s=self.result_visible_epoch_s,
            service_quantum_metrics=quantum_metrics,
            service_s=service_s,
            slo_scale_metrics=slo_scale_metrics,
            submission_trace_path=self.submission_trace_path,
            submit_metrics=self.submit_metrics,
            token_count=token_count,
            vllm_stats=vllm_stats,
            writeback_s=self.writeback_s,
            written_rows=self.written_rows,
            actor_worker_metrics=worker_metrics,
        )
        return _build_profile_result_row(args, self.resolved, data)

    def run(self) -> dict:
        self._initialize()
        self._run_source()
        self._validate_result_visibility()
        self._write_request_and_resource_evidence()
        self._write_control_evidence()
        return self._finish_and_build_result()

    def close(self) -> None:
        if self.resource_sampler is not None and self.resource_sampler.is_running:
            self.resource_sampler.close()
        for provider in self.adaptive_observation_providers:
            provider.close()


def run_once(args: argparse.Namespace, phase: str, repeat_index: int) -> dict:
    resolved = _resolve_profile_run(args)
    if args.dry_run:
        return _dry_run_result(
            args,
            phase,
            repeat_index,
            resolved.dry_run_context(),
        )
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL.")
    if args.operator == "ai_complete" and args.writeback_mode == "pgvector":
        raise SystemExit("AI_COMPLETE does not support --writeback-mode pgvector.")

    conn = connect(args.database_url)
    job_id = None
    operator_run: _ProfileOperatorRun | None = None
    try:
        gpu_snapshot = {
            **GPU_METADATA_DEFAULTS,
            **gpu_metadata(resolved.sampled_gpu_ids),
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
                    f"vector({current_dim}); rerun with --setup or choose "
                    f"--embedding-dim {current_dim}."
                )
        db_metadata = database_metadata(conn)
        current_vector_dim = embedding_vector_column_dim(conn)
        operator_sql_name = (
            "AI_COMPLETE" if args.operator == "ai_complete" else "AI_EMBED"
        )
        output_table = (
            "document_completions"
            if args.operator == "ai_complete"
            else "document_embeddings"
        )
        job_id = create_job(conn, operator_sql_name, output_table)
        ray_runtime = _initialize_profile_ray_runtime(args, resolved)
        operator_run = _ProfileOperatorRun(
            args,
            resolved,
            ray_runtime,
            conn=conn,
            job_id=job_id,
            phase=phase,
            repeat_index=repeat_index,
            db_metadata=db_metadata,
            gpu_snapshot=gpu_snapshot,
            current_vector_dim=current_vector_dim,
        )
        return operator_run.run()
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
        if operator_run is not None:
            operator_run.close()
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
