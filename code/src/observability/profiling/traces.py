"""Versioned CSV serializers for profiler trace artifacts."""

from __future__ import annotations

import csv
from collections.abc import Sequence
from pathlib import Path

from src.observability.metrics import append_metrics
from src.scheduling.core.lifecycle import RequestTraceRow
from src.scheduling.core.models import SubmissionLifecycleEvent


def write_control_trace(
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
                "schema_version": 2,
                "experiment_id": experiment_id,
                "phase": phase,
                "repeat_index": repeat_index,
                "job_id": job_id,
                "server_version": server_version,
                "pgvector_version": pgvector_version,
                "controller": controller_name,
                "endpoint_id": event.endpoint_id or "",
                "trace_index": trace_index,
                "elapsed_s": event.observed_at_s - first_observed_at_s,
                "fresh": event.fresh,
                "inflight": event.inflight,
                "k_max": event.window,
                "running": event.running if event.running is not None else "",
                "waiting": event.waiting if event.waiting is not None else "",
                "kv_usage": (
                    event.kv_usage if event.kv_usage is not None else ""
                ),
                "sample_age_s": (
                    event.sample_age_s
                    if event.sample_age_s is not None
                    else ""
                ),
                "hol_age_s": (
                    event.hol_age_s if event.hol_age_s is not None else ""
                ),
                "controller_action": event.controller_action,
                "reason": event.reason,
                "allowed": event.allowed,
            },
        )


def write_flush_trace(
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
                "schema_version": 3,
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
                "selected_token_budget": event.selected_token_budget,
                "token_budget_reason": event.token_budget_reason,
                "arrival_rate_tokens_s": (
                    event.arrival_rate_tokens_s
                    if event.arrival_rate_tokens_s is not None
                    else ""
                ),
                "service_rate_tokens_s_per_endpoint": (
                    event.service_rate_tokens_s_per_endpoint
                    if event.service_rate_tokens_s_per_endpoint is not None
                    else ""
                ),
            },
        )


def write_submission_trace(
    output_path: Path,
    *,
    experiment_id: str,
    phase: str,
    repeat_index: int,
    job_id: int,
    server_version: str,
    pgvector_version: str,
    results: list[dict | None],
    submission_events: Sequence[SubmissionLifecycleEvent] | None = None,
) -> None:
    if submission_events is not None and len(submission_events) != len(results):
        raise ValueError(
            "submission lifecycle events and results must align"
        )
    for submission_index, result in enumerate(results):
        resolved_result = result or {}
        event = (
            submission_events[submission_index]
            if submission_events is not None
            else None
        )
        append_metrics(
            output_path,
            {
                "schema_version": 5,
                "experiment_id": experiment_id,
                "phase": phase,
                "repeat_index": repeat_index,
                "job_id": job_id,
                "server_version": server_version,
                "pgvector_version": pgvector_version,
                "submission_index": submission_index,
                "submission_id": (
                    event.submission_id
                    if event is not None
                    else f"{job_id}:batch:{submission_index}"
                ),
                "planning_batch_id": (
                    event.planning_batch_id if event is not None else ""
                ),
                "service_quantum_index": (
                    event.service_quantum_index if event is not None else -1
                ),
                "service_quantum_oversized": (
                    event.service_quantum_oversized if event is not None else False
                ),
                "actor_worker_id": (
                    event.actor_worker_id if event is not None else ""
                ),
                "actor_worker_index": (
                    event.actor_worker_index if event is not None else -1
                ),
                "actor_worker_pid": (
                    event.actor_worker_pid if event is not None else 0
                ),
                "pool_id": event.pool_id if event is not None else "",
                "endpoint_id": event.endpoint_id if event is not None else "",
                "gpu_id": event.gpu_id if event is not None else "",
                "status": event.status if event is not None else "",
                "error": (
                    (event.error or "") if event is not None else ""
                ),
                "credit_held_s": (
                    event.completion_epoch_s - event.submit_epoch_s
                    if event is not None
                    else 0.0
                ),
                "ray_to_service_s": (
                    max(
                        0.0,
                        float(
                            resolved_result.get(
                                "service_start_epoch_s",
                                0.0,
                            )
                        )
                        - event.submit_epoch_s,
                    )
                    if event is not None
                    else 0.0
                ),
                "doc_ids": ";".join(
                    str(item)
                    for item in resolved_result.get("doc_id", [])
                ),
                "rows": resolved_result.get("rows", 0),
                "token_count": resolved_result.get("token_count", 0),
                "input_token_count": resolved_result.get(
                    "input_token_count",
                    0,
                ),
                "output_token_count": resolved_result.get(
                    "output_token_count",
                    0,
                ),
                "service_s": resolved_result.get("service_s", 0.0),
                "service_start_epoch_s": resolved_result.get(
                    "service_start_epoch_s",
                    0.0,
                ),
                "service_end_epoch_s": resolved_result.get(
                    "service_end_epoch_s",
                    0.0,
                ),
                "http_request_start_epoch_s": resolved_result.get(
                    "http_request_start_epoch_s",
                    "",
                ),
                "http_response_headers_epoch_s": resolved_result.get(
                    "http_response_headers_epoch_s",
                    "",
                ),
                "http_response_body_epoch_s": resolved_result.get(
                    "http_response_body_epoch_s",
                    "",
                ),
                "http_headers_wait_s": resolved_result.get(
                    "http_headers_wait_s",
                    "",
                ),
                "http_body_read_s": resolved_result.get(
                    "http_body_read_s",
                    "",
                ),
            },
        )


def write_request_trace(
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
    rows: Sequence[RequestTraceRow],
) -> None:
    for request_index, row in enumerate(rows):
        append_metrics(
            output_path,
            {
                "schema_version": 3,
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
                "finish_reason": row.finish_reason or "",
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
                "service_s": (
                    row.service_s if row.service_s is not None else ""
                ),
                "service_clock_domain": row.service_clock_domain,
                "e2e_s": row.e2e_s,
                "request_time_origin": row.request_time_origin,
                "latency_granularity": row.latency_granularity,
                "slo_target_s": (
                    row.slo_target_s if row.slo_target_s is not None else ""
                ),
                "slo_met": row.slo_met if row.slo_met is not None else "",
            },
        )


_COMPLETION_EVIDENCE_FIELDS = (
    "doc_id", "prompt_tokens", "output_tokens", "output_text",
    "status", "error_type", "finish_reason",
    "submit_epoch_s", "service_start_epoch_s", "completion_epoch_s",
)


def write_completion_evidence(
    output_path: Path,
    *,
    rows: Sequence[RequestTraceRow],
    operator_results: Sequence[dict],
) -> None:
    """Write a run-scoped per-doc completion evidence CSV (``output_text`` included).

    Independent of the ``document_completions`` sink: ``output_text`` is flattened
    from the in-process ``operator_results`` (per-batch ``doc_id``/``output_text``
    lists, mirroring ``_build_profiler_request_rows``) so a downstream reader can
    compare this file against the sink to detect stale residual rows -- breaking
    the circular self-reference of reading output_text FROM the sink and then
    digesting the sink against itself. One row per doc; doc_id is unique across
    the trace rows (enforced by ``build_request_trace_rows``).
    """

    output_text_by_doc_id: dict[str, str] = {}
    for result in operator_results:
        doc_ids = [str(value) for value in result.get("doc_id", [])]
        outputs = result.get("output_text", [])
        if len(outputs) != len(doc_ids):
            continue
        for doc_id, output in zip(doc_ids, outputs):
            output_text_by_doc_id[doc_id] = "" if output is None else str(output)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_COMPLETION_EVIDENCE_FIELDS))
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "doc_id": row.doc_id,
                "prompt_tokens": row.prompt_tokens,
                "output_tokens": (
                    row.actual_output_tokens
                    if row.actual_output_tokens is not None else 0
                ),
                "output_text": output_text_by_doc_id.get(str(row.doc_id), ""),
                "status": row.status,
                "error_type": row.error_type or "",
                "finish_reason": row.finish_reason or "",
                "submit_epoch_s": row.submit_epoch_s,
                "service_start_epoch_s": (
                    row.service_start_epoch_s
                    if row.service_start_epoch_s is not None else ""
                ),
                "completion_epoch_s": row.completion_epoch_s,
            })


def write_resource_trace(
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
