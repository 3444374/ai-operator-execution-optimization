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
from typing import Callable, Iterable, Mapping, Sequence

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
from src.profile_traces import (
    write_control_trace as _write_control_trace,
    write_flush_trace as _write_flush_trace,
    write_request_trace as _write_request_trace,
    write_resource_trace as _write_resource_trace,
    write_submission_trace as _write_submission_trace,
)
from src.request_costs import (
    OutputCostMode,
    output_cost_source,
    resolve_output_tokens,
)
from src.scheduling.adaptive_admission import (
    AimdAdmissionController,
    AimdConfig,
    EwmaAimdAdmissionController,
    HolAgeAimdAdmissionController,
    HolAgeAimdConfig,
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
from src.scheduling.ray_adapter import (
    ActorSubmissionState,
    RaySubmissionAdapter,
)
from src.scheduling.ray_runtime import RayWorkerOptions
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


FORMAL_RESULT_FIELDS = tuple(
    """
    status experiment_id phase repeat_index scenario_id random_seed
    server_version pgvector_version gpu_metrics_status gpu_name
    gpu_utilization_pct gpu_memory_used_mib gpu_memory_total_mib gpu_power_w
    database_trigger job_id operator seed_workload executor strategy data_source
    source_workload_name source_order source_max_prompt_tokens organizer
    organizer_partition_mode organizer_partitions daft_runner
    organizer_partition_effective model_backend model_endpoint_url model_name
    model_request_timeout_s total_rows written_rows db_fetch_rows ray_batch_rows
    batching_policy token_budget embedding_dim embedding_vector_dim
    completion_max_tokens completion_return_token_ids completion_prompt_format
    completion_temperature output_cost_mode output_cost_source packing_cost_unit
    cost_model_id cost_tokenizer_id packing_algorithm packing_scope
    packing_budget_utilization_mean packing_budget_utilization_p95
    packing_oversized_rows packing_input_rows packing_batch_count
    batch_estimated_cost_units_p50 batch_estimated_cost_units_p95
    batch_estimated_cost_units_p99 batch_estimated_cost_units_max model_workers
    ray_version actor_workers_per_endpoint ray_actor_max_concurrency
    ray_worker_num_cpus ray_worker_num_gpus endpoint_count actor_worker_count
    actor_worker_submission_counts max_inflight_limit endpoint_routing
    pool_routing endpoint_pool_ids endpoint_gpu_ids long_request_token_threshold
    scheduling_policy adaptive_min_inflight adaptive_max_inflight
    controller_min_window controller_max_window adaptive_sample_interval_s
    adaptive_downshifts adaptive_upshifts adaptive_limit_mean control_trace_path
    control_trace_events arrival_replay arrival_time_scale arrival_replay_preload
    submission_granularity
    flush_policy flush_timeout_ms flush_max_wait_ms flush_trace_output
    flush_trace_path flush_trace_events submission_trace_path
    submission_trace_events resource_trace_path resource_trace_events
    resource_sample_interval_s resource_metrics_status gpu_utilization_pct_mean
    gpu_utilization_pct_p50 gpu_utilization_pct_p95 gpu_utilization_pct_max
    gpu_utilization_below_10pct_ratio gpu_memory_used_mib_mean
    gpu_memory_used_mib_max gpu_memory_utilization_pct_mean
    gpu_memory_utilization_pct_max gpu_power_w_mean gpu_power_w_max gpu_energy_j
    energy_j_per_1k_observed_tokens vllm_running_mean vllm_running_p50
    vllm_running_p95 vllm_running_max vllm_waiting_mean vllm_waiting_p50
    vllm_waiting_p95 vllm_waiting_max vllm_kv_cache_usage_mean
    vllm_kv_cache_usage_p50 vllm_kv_cache_usage_p95 vllm_kv_cache_usage_max
    mfu_estimation_method mfu_time_basis model_flops_per_token gpu_peak_tflops
    mfu_precision mfu_status mfu_estimate request_trace_path request_trace_events
    request_e2e_s_p50 request_e2e_s_p95 request_e2e_s_p99
    request_slo_target_ms request_slo_violation_ratio request_slo_goodput_per_s
    request_actual_output_tokens_observed request_actual_output_tokens_p50
    request_actual_output_tokens_p95 request_actual_output_tokens_p99
    request_finish_reason_observed request_finish_reason_stop_ratio
    request_finish_reason_length_ratio latency_granularity writeback_mode
    write_batch_rows object_count operator_invocations max_inflight_seen
    token_count batch_rows_min batch_rows_max batch_rows_mean batch_tokens_min
    batch_tokens_max batch_tokens_mean batch_tokens_p50 batch_tokens_p95
    batch_service_s_p50 batch_service_s_p95 batch_service_s_p99
    vllm_metrics_status vllm_prompt_tokens_delta vllm_generation_tokens_delta
    vllm_request_success_delta vllm_estimated_flops_per_gpu_delta
    vllm_e2e_request_latency_mean_s vllm_request_queue_time_mean_s
    vllm_request_inference_time_mean_s vllm_request_prefill_time_mean_s
    vllm_request_decode_time_mean_s vllm_num_requests_running_after
    vllm_num_requests_waiting_after vllm_kv_cache_usage_perc_after db_fetch_s
    arrow_build_s source_fetch_s organizer_from_arrow_s organizer_plan_s
    organizer_collect_s organization_policy_family
    batch_prompt_token_spread_mean prefix_group_ratio organizer_warnings
    model_service_s model_request_wall_s operator_wall_s submit_s bounded_wait_s
    avg_bounded_wait_s fanin_s writeback_s e2e_s rows_per_s tokens_per_s
    """.split()
)

GPU_METADATA_DEFAULTS = {
    "gpu_metrics_status": "unavailable",
    "gpu_name": "",
    "gpu_utilization_pct": "",
    "gpu_memory_used_mib": "",
    "gpu_memory_total_mib": "",
    "gpu_power_w": "",
}


def _validated_formal_result_row(row: dict) -> dict:
    actual_fields = tuple(row)
    if actual_fields != FORMAL_RESULT_FIELDS:
        raise RuntimeError(
            "formal result schema drift: "
            f"actual fields {actual_fields!r} != "
            f"expected fields {FORMAL_RESULT_FIELDS!r}"
        )
    return row


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Profile PostgreSQL-triggered AI operator execution through "
            "Daft, Ray, and an external model service."
        )
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
        "--source-max-prompt-tokens",
        type=int,
        default=None,
        help=(
            "Exclude, rather than truncate, rows whose recorded prompt "
            "tokens exceed this context-safety threshold."
        ),
    )
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
            "best_fit_token_budget",
            "row_cap_aware_token_budget",
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
    parser.add_argument("--embedding-endpoint-url")
    parser.add_argument(
        "--embedding-endpoint-urls",
        default=None,
        help="Comma-separated OpenAI-compatible embedding endpoint URLs for round-robin routing.",
    )
    parser.add_argument("--embedding-model", default=os.environ.get("EMBEDDING_MODEL", "local-embedding"))
    parser.add_argument("--embedding-api-key", default=os.environ.get("EMBEDDING_API_KEY"))
    parser.add_argument("--embedding-request-timeout-s", type=float, default=120.0)
    parser.add_argument("--completion-endpoint-url")
    parser.add_argument(
        "--completion-endpoint-urls",
        default=None,
        help="Comma-separated OpenAI-compatible completion endpoint URLs for round-robin routing.",
    )
    parser.add_argument("--completion-model", default=os.environ.get("COMPLETION_MODEL", "local-completion"))
    parser.add_argument("--completion-api-key", default=os.environ.get("COMPLETION_API_KEY"))
    parser.add_argument("--completion-request-timeout-s", type=float, default=120.0)
    parser.add_argument("--completion-max-tokens", type=int, default=128)
    parser.add_argument(
        "--completion-return-token-ids",
        action="store_true",
        help=(
            "Request vLLM per-choice token IDs for exact output-token and "
            "finish-reason tracing. Disabled for generic compatible servers."
        ),
    )
    parser.add_argument(
        "--completion-prompt-format",
        choices=["raw", "chatml"],
        default="raw",
        help=(
            "Prompt envelope sent to compatible completion endpoints. "
            "chatml preserves row content and adds user/assistant delimiters."
        ),
    )
    parser.add_argument(
        "--completion-temperature",
        type=float,
        default=None,
        help=(
            "Optional sampling temperature. Use 0 for deterministic "
            "cross-policy comparisons."
        ),
    )
    parser.add_argument(
        "--output-cost-mode",
        choices=[
            "prompt_only",
            "fixed_output_cap",
            "trace_target_output",
        ],
        default="fixed_output_cap",
        help="Output-token estimate used only for organization and scheduling cost.",
    )
    parser.add_argument(
        "--cost-model-id",
        default="",
        help="Model identifier used to produce or calibrate the cost estimate.",
    )
    parser.add_argument(
        "--cost-tokenizer-id",
        default="",
        help="Tokenizer identifier used to produce prompt/output cost units.",
    )
    parser.add_argument("--model-metrics-url")
    parser.add_argument(
        "--model-metrics-urls",
        default=None,
        help=(
            "Comma-separated Prometheus endpoints. Counters and request "
            "gauges are summed; KV-cache usage uses the maximum."
        ),
    )
    parser.add_argument("--model-workers", type=int, default=2)
    parser.add_argument(
        "--actor-workers-per-endpoint",
        type=int,
        default=0,
        help="Number of Ray HTTP client actor workers per service endpoint.",
    )
    parser.add_argument("--ray-actor-max-concurrency", type=int, default=1)
    parser.add_argument("--ray-worker-num-cpus", type=float, default=0.25)
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
        help="Comma-separated pool ID per service endpoint.",
    )
    parser.add_argument(
        "--endpoint-gpu-ids",
        default=None,
        help="Comma-separated GPU ID per service endpoint.",
    )
    parser.add_argument(
        "--long-request-token-threshold",
        type=int,
        default=0,
        help="Resolved tuning-workload P75 token cost; required by request_cost pool routing.",
    )
    parser.add_argument(
        "--scheduling-policy",
        choices=["static", "queue_adaptive", "aimd", "ewma_aimd", "pid", "aimd_hol"],
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
        "--hol-age-congestion-s",
        type=float,
        default=2.0,
        help="aimd_hol multiplicative-decrease threshold on Ray-side head-of-line age (s).",
    )
    parser.add_argument(
        "--hol-age-low-load-s",
        type=float,
        default=0.5,
        help="aimd_hol additive-increase threshold on Ray-side head-of-line age (s).",
    )
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
        "--submission-granularity",
        choices=["batch", "request"],
        default="batch",
        help=(
            "Submit each closed replay group as one multi-row HTTP call, or "
            "expand it into complete one-row requests for continuous replenishment."
        ),
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
    parser.add_argument("--resource-sample-interval-s", type=float, default=0.25)
    parser.add_argument(
        "--model-flops-per-token",
        type=float,
        default=0.0,
        help="Reviewed model FLOP estimate per observed token; zero disables MFU.",
    )
    parser.add_argument(
        "--gpu-peak-tflops",
        type=float,
        default=0.0,
        help="Peak TFLOP/s for the recorded GPU and --mfu-precision.",
    )
    parser.add_argument(
        "--mfu-precision",
        default="",
        help="Precision label matching --gpu-peak-tflops, for example bf16.",
    )
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
    return _configured_urls(
        plural_cli=args.embedding_endpoint_urls,
        single_cli=args.embedding_endpoint_url,
        plural_env=os.environ.get("EMBEDDING_ENDPOINT_URLS"),
        single_env=os.environ.get("EMBEDDING_ENDPOINT_URL"),
    )


def completion_endpoint_urls(args: argparse.Namespace) -> list[str]:
    return _configured_urls(
        plural_cli=args.completion_endpoint_urls,
        single_cli=args.completion_endpoint_url,
        plural_env=os.environ.get("COMPLETION_ENDPOINT_URLS"),
        single_env=os.environ.get("COMPLETION_ENDPOINT_URL"),
    )


def model_metrics_urls(args: argparse.Namespace) -> list[str]:
    return _configured_urls(
        plural_cli=getattr(args, "model_metrics_urls", None),
        single_cli=getattr(args, "model_metrics_url", None),
        plural_env=os.environ.get("MODEL_METRICS_URLS"),
        single_env=os.environ.get("MODEL_METRICS_URL"),
    )


def _configured_urls(
    *,
    plural_cli: str | None,
    single_cli: str | None,
    plural_env: str | None,
    single_env: str | None,
) -> list[str]:
    """Resolve explicit CLI values before plural/single environment defaults."""

    if plural_cli is not None:
        text = plural_cli
    elif single_cli is not None:
        text = single_cli
    elif plural_env is not None:
        text = plural_env
    elif single_env is not None:
        text = single_env
    else:
        return []
    return [value.strip() for value in text.split(",") if value.strip()]


def _resolve_actor_workers_per_endpoint(
    args: argparse.Namespace,
    endpoint_count: int,
) -> int:
    if endpoint_count <= 0:
        raise SystemExit("endpoint_count must be positive")
    if args.actor_workers_per_endpoint < 0:
        raise SystemExit("--actor-workers-per-endpoint must be non-negative")
    if args.actor_workers_per_endpoint:
        return args.actor_workers_per_endpoint
    if endpoint_count > 1:
        raise SystemExit(
            "multi-endpoint ray_actor requires --actor-workers-per-endpoint"
        )
    if args.model_workers <= 0:
        raise SystemExit("--model-workers must be positive")
    return args.model_workers


def _validate_ray_worker_resources(args: argparse.Namespace) -> None:
    if args.executor not in {"ray_actor", "ray_task"}:
        return
    if (
        not math.isfinite(args.ray_worker_num_cpus)
        or args.ray_worker_num_cpus <= 0
    ):
        raise SystemExit("--ray-worker-num-cpus must be finite and positive")
    if args.executor == "ray_actor" and args.ray_actor_max_concurrency <= 0:
        raise SystemExit("--ray-actor-max-concurrency must be positive")


def _ray_worker_options(
    args: argparse.Namespace,
) -> RayWorkerOptions | None:
    if args.executor not in {"ray_actor", "ray_task"}:
        return None
    _validate_ray_worker_resources(args)
    return RayWorkerOptions(
        num_cpus=args.ray_worker_num_cpus,
        actor_max_concurrency=(
            args.ray_actor_max_concurrency
            if args.executor == "ray_actor"
            else 1
        ),
    )


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


def _batch_envelopes(
    batches: Iterable[pa.RecordBatch | pa.Table],
    job_id: str,
    operator: str,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode = "fixed_output_cap",
    batch_index_start: int = 0,
) -> list[PayloadEnvelope]:
    envelopes = []
    for index, batch in enumerate(batches):
        request_id = f"{job_id}:batch:{batch_index_start + index}"
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
            estimated_output_tokens=sum(
                _row_output_tokens(
                    batch,
                    row_index,
                    output_cost_mode=output_cost_mode,
                    completion_max_tokens=completion_max_tokens,
                )
                for row_index in range(batch.num_rows)
            ),
            prefix_key=prefix_key,
            first_arrival_s=oldest_arrival_s,
            oldest_arrival_s=oldest_arrival_s,
            payload_id=request_id,
        )
        envelopes.append(PayloadEnvelope(request=request, payload=batch))
    return envelopes


def _offline_batch_envelopes(
    batches: Iterable[pa.Table | pa.RecordBatch],
    *,
    job_id: str,
    operator: str,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode,
    batch_index_start: int,
    job_start_epoch_s: float,
    ready_epoch_s: float,
) -> tuple[list[PayloadEnvelope], list[RequestLifecycleSeed]]:
    materialized_batches = list(batches)
    envelopes = _batch_envelopes(
        materialized_batches,
        job_id=job_id,
        operator=operator,
        completion_max_tokens=completion_max_tokens,
        output_cost_mode=output_cost_mode,
        batch_index_start=batch_index_start,
    )
    seeds = []
    for batch, envelope in zip(materialized_batches, envelopes):
        if "doc_id" not in batch.column_names:
            raise ValueError("doc_id column is required for request tracing")
        for row_index in range(batch.num_rows):
            doc_value = batch.column("doc_id")[row_index].as_py()
            if doc_value is None:
                raise ValueError(
                    "doc_id must be non-null for request tracing"
                )
            prompt_tokens = (
                int(
                    batch.column("prompt_tokens")[row_index].as_py()
                    or 0
                )
                if "prompt_tokens" in batch.column_names
                else 0
            )
            prefix_key = (
                str(batch.column("prefix_key")[row_index].as_py() or "")
                if "prefix_key" in batch.column_names
                else ""
            )
            seeds.append(
                RequestLifecycleSeed(
                    request_id=f"{job_id}:row:{doc_value}",
                    submission_id=envelope.request.request_id,
                    doc_id=str(doc_value),
                    prompt_tokens=prompt_tokens,
                    estimated_output_tokens=_row_output_tokens(
                        batch,
                        row_index,
                        output_cost_mode=output_cost_mode,
                        completion_max_tokens=completion_max_tokens,
                    ),
                    prefix_key=prefix_key,
                    arrival_epoch_s=job_start_epoch_s,
                    flush_epoch_s=ready_epoch_s,
                    request_time_origin="offline_job_start",
                )
            )
    return envelopes, seeds


def _row_output_tokens(
    table: pa.Table | pa.RecordBatch,
    row_index: int,
    *,
    output_cost_mode: OutputCostMode,
    completion_max_tokens: int,
) -> int:
    target_value = (
        table.column("target_output_tokens")[row_index].as_py()
        if "target_output_tokens" in table.column_names
        else None
    )
    return resolve_output_tokens(
        output_cost_mode,
        completion_max_tokens=completion_max_tokens,
        target_output_tokens=target_value,
    )


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


def _row_arrivals(
    table: pa.Table | pa.RecordBatch,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode = "fixed_output_cap",
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
                estimated_output_tokens=_row_output_tokens(
                    table,
                    index,
                    output_cost_mode=output_cost_mode,
                    completion_max_tokens=completion_max_tokens,
                ),
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


def _request_envelopes(
    pending: PendingBatch,
    *,
    job_id: str,
    operator: str,
) -> tuple[PayloadEnvelope, ...]:
    envelopes = []
    for row in pending.rows:
        request_id = f"{job_id}:request:{row.row_id}"
        envelopes.append(
            PayloadEnvelope(
                request=BatchRequest(
                    request_id=request_id,
                    job_id=job_id,
                    operator=operator,
                    row_count=1,
                    prompt_tokens=row.prompt_tokens,
                    estimated_output_tokens=row.estimated_output_tokens,
                    prefix_key=row.prefix_key,
                    first_arrival_s=row.arrival_s,
                    oldest_arrival_s=row.arrival_s,
                    payload_id=request_id,
                ),
                payload=row.payload_ref,
            )
        )
    return tuple(envelopes)


def _arrival_replay_envelopes(
    tables: Iterable[pa.Table | pa.RecordBatch],
    args: argparse.Namespace,
    job_id: str,
    operator: str,
    service_observation,
    trace_sink,
    lifecycle_seed_sink=None,
    packing_sink=None,
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
            for arrival in _row_arrivals(
                table,
                completion_max_tokens,
                output_cost_mode=getattr(
                    args,
                    "output_cost_mode",
                    "fixed_output_cap",
                ),
            ):
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

    submission_granularity = getattr(args, "submission_granularity", "batch")

    def close_batch(pending: PendingBatch) -> tuple[PayloadEnvelope, ...]:
        nonlocal batch_index
        if packing_sink is not None:
            packing_sink.append(
                (pending.estimated_total_tokens, pending.row_count)
            )
        envelope = _arrow_envelope(
            pending,
            batch_index=batch_index,
            job_id=str(job_id),
            operator=operator,
        )
        closed_envelopes = (
            _request_envelopes(
                pending,
                job_id=str(job_id),
                operator=operator,
            )
            if submission_granularity == "request"
            else (envelope,)
        )
        if lifecycle_seed_sink is not None:
            if replay_start_epoch_s is None or first_source_arrival_s is None:
                raise RuntimeError("replay epoch origin is not initialized")
            intended_arrival_epochs = [
                replay_start_epoch_s
                + (row.arrival_s - first_source_arrival_s)
                * arrival_time_scale
                for row in pending.rows
            ]
            flush_epoch_s = lifecycle_epoch_clock()
            # The replay clock and epoch-shaped lifecycle clock are separate
            # monotonic domains. Scheduler jitter can make an intended replay
            # deadline a few milliseconds later than the epoch observed when
            # the batch actually closes. Request traces record observed
            # lifecycle times, so clamp such intended arrivals at the observed
            # flush boundary instead of pushing flush into the future and
            # making the subsequent submit timestamp appear to precede it.
            arrival_epochs = [
                min(arrival_epoch_s, flush_epoch_s)
                for arrival_epoch_s in intended_arrival_epochs
            ]
            seeds = [
                RequestLifecycleSeed(
                    request_id=f"{job_id}:row:{row.row_id}",
                    submission_id=(
                        f"{job_id}:request:{row.row_id}"
                        if submission_granularity == "request"
                        else envelope.request.request_id
                    ),
                    doc_id=row.row_id,
                    prompt_tokens=row.prompt_tokens,
                    estimated_output_tokens=row.estimated_output_tokens,
                    prefix_key=row.prefix_key,
                    arrival_epoch_s=arrival_epoch_s,
                    flush_epoch_s=flush_epoch_s,
                    request_time_origin="replayed_arrival",
                    latency_granularity=(
                        "request"
                        if submission_granularity == "request"
                        else "submission"
                    ),
                )
                for row, arrival_epoch_s in zip(
                    pending.rows,
                    arrival_epochs,
                )
            ]
            for seed in seeds:
                if callable(lifecycle_seed_sink):
                    lifecycle_seed_sink(seed)
                else:
                    lifecycle_seed_sink.append(seed)
        batch_index += 1
        return closed_envelopes

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
            for closed_envelopes in batcher:
                yield from closed_envelopes
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


def _resource_snapshot(metrics_urls: Sequence[str]) -> dict[str, object]:
    gpu = gpu_metadata()
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
    actor_pools: Mapping[str, Sequence[object]] | None = None,
    endpoint_urls: Mapping[str, str] | None = None,
    batches: Iterable[pa.RecordBatch | pa.Table] = (),
    max_inflight: int = 1,
    method_name: str = "",
    adaptive_config: dict | None = None,
    routing_config: dict | None = None,
    replay_envelopes: Iterable[PayloadEnvelope] | None = None,
    submission_lifecycle_sink: list[SubmissionLifecycleEvent] | None = None,
    epoch_clock=None,
    output_cost_mode: OutputCostMode = "fixed_output_cap",
    completion_max_tokens: int = 0,
    actors: Sequence[object] | None = None,
    submission_state: ActorSubmissionState | None = None,
) -> tuple[list[dict], dict]:
    if actor_pools is None:
        if actors is None:
            raise ValueError("actor_pools must not be empty")
        actor_pools = {
            f"actor-{index}": [actor]
            for index, actor in enumerate(actors)
        }
        endpoint_urls = {
            endpoint_id: f"ray://actor/{index}"
            for index, endpoint_id in enumerate(actor_pools)
        }
    if not actor_pools:
        raise ValueError("actor_pools must not be empty")
    if not endpoint_urls:
        raise ValueError("endpoint_urls must not be empty")
    if set(actor_pools) != set(endpoint_urls):
        raise ValueError(
            "actor_pools and endpoint_urls must have identical "
            "service endpoint IDs"
        )

    endpoint_ids = list(actor_pools)
    state = submission_state or ActorSubmissionState(actor_pools, method_name)
    state.validate(actor_pools, method_name)
    pool_submitters = state.pool_submitters
    counts_before = {
        endpoint_id: submitter.submission_counts
        for endpoint_id, submitter in pool_submitters.items()
    }
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
        results, metrics = _submit_with_backpressure_legacy_adaptive(
            ray_module,
            state.legacy_endpoint_submitter,
            replay_batches,
            max_inflight,
            adaptive_config,
        )
    else:
        operator = "ai_complete" if "complete" in method_name else "ai_embed"
        envelopes = (
            replay_envelopes
            if replay_envelopes is not None
            else _batch_envelopes(
                batches,
                job_id="ray-actor",
                operator=operator,
                completion_max_tokens=(
                    completion_max_tokens
                    if operator == "ai_complete"
                    else 0
                ),
                output_cost_mode=output_cost_mode,
            )
        )
        topology = _endpoint_topology(
            endpoint_ids,
            [endpoint_urls[item] for item in endpoint_ids],
            pool_ids=(
                routing_config.get("pool_ids")
                if routing_config is not None
                else None
            ),
            gpu_ids=(
                routing_config.get("gpu_ids")
                if routing_config is not None
                else None
            ),
        )
        submitters = {
            endpoint_id: submitter
            for endpoint_id, submitter in pool_submitters.items()
        }
        if typed_adaptive:
            results, metrics = _run_dynamic_scheduler(
                ray_module,
                envelopes,
                topology,
                submitters,
                adaptive_config,
                routing_config,
                submission_lifecycle_sink,
                epoch_clock,
            )
        else:
            results, metrics = _run_static_scheduler(
                ray_module,
                envelopes,
                topology,
                submitters,
                max_inflight,
                routing_config,
                submission_lifecycle_sink,
                epoch_clock,
            )
    metrics.update(
        {
            "endpoint_count": len(endpoint_ids),
            "actor_worker_count": sum(
                submitter.worker_count
                for submitter in pool_submitters.values()
            ),
            "actor_worker_submission_counts": ";".join(
                str(after - before)
                for endpoint_id, submitter in pool_submitters.items()
                for before, after in zip(
                    counts_before[endpoint_id],
                    submitter.submission_counts,
                )
            ),
        }
    )
    return results, metrics


def _submit_with_backpressure_legacy_adaptive(
    ray_module,
    endpoint_submitter: Callable[[object], object],
    batches: Iterable[pa.RecordBatch | pa.Table],
    max_inflight: int,
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
        submit_timer = StageTimer.start("submit")
        ref = endpoint_submitter(batch)
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
    output_cost_mode: OutputCostMode = "fixed_output_cap",
    completion_return_token_ids: bool = False,
    completion_prompt_format: str = "raw",
    completion_temperature: float | None = None,
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
            completion_return_token_ids,
            completion_prompt_format,
            completion_temperature,
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
            output_cost_mode=output_cost_mode,
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
            elif (
                model_backend != "ollama"
                and (
                    completion_return_token_ids
                    or completion_prompt_format != "raw"
                    or completion_temperature is not None
                )
            ):
                submitters[endpoint_id] = (
                    lambda payload, url=endpoint_url: remote_embed.remote(
                        payload,
                        url,
                        model_name,
                        api_key,
                        timeout_s,
                        completion_max_tokens,
                        completion_return_token_ids,
                        completion_prompt_format,
                        completion_temperature,
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
    completion_return_token_ids: bool = False,
    completion_prompt_format: str = "raw",
    completion_temperature: float | None = None,
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
            elif (
                model_backend != "ollama"
                and (
                    completion_return_token_ids
                    or completion_prompt_format != "raw"
                    or completion_temperature is not None
                )
            ):
                pending.append(
                    remote_embed.remote(
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
        gpu_snapshot = {**GPU_METADATA_DEFAULTS, **gpu_metadata()}
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
                lambda: _resource_snapshot(resolved_metrics_urls),
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
                    max_inflight=args.max_inflight,
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
                        if (
                            args.request_trace_output
                            or args.submission_trace_output
                        )
                        else None
                    ),
                    epoch_clock=lifecycle_epoch_clock,
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
