"""Assemble profiler routing, admission and scheduler result metrics."""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from src.modalities.text.costs import extract_completed_token_work
from src.scheduling.core.models import (
    EndpointSnapshot,
    PayloadEnvelope,
    SubmissionLifecycleEvent,
    TopologySnapshot,
)
from src.scheduling.core.scheduler import (
    EndpointCapacityConfig,
    SchedulerConfig,
    SchedulerResult,
)
from src.scheduling.runtime.execution import SynchronousExecutionEngine
from src.scheduling.runtime.ray_adapter import RaySubmissionAdapter
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.submission_control.admission import StaticAdmissionController


DEFAULT_POOL_ID = "default"


@dataclass(frozen=True)
class _SchedulerOptions:
    """Optional scheduler wiring kept behind one internal interface."""

    routing_config: Mapping[str, object] | None = None
    submission_lifecycle_sink: list[SubmissionLifecycleEvent] | None = None
    epoch_clock: Callable[[], float] | None = None
    per_endpoint_limit: int | None = None
    per_endpoint_work_limit: int | None = None
    per_endpoint_admission: Mapping[str, object] | None = None


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
        "max_active_work_per_endpoint_seen": (
            result.max_active_work_per_endpoint_seen
        ),
        "max_ready_requests_seen": result.max_ready_requests_seen,
        "max_ready_work_seen": result.max_ready_work_seen,
        "max_ready_payload_bytes_seen": (
            result.max_ready_payload_bytes_seen
        ),
        "ready_requests_transition_samples": (
            result.ready_requests_transition_samples
        ),
        "ready_work_transition_samples": result.ready_work_transition_samples,
        "ready_payload_bytes_transition_samples": (
            result.ready_payload_bytes_transition_samples
        ),
        "bounded_wait_s": result.bounded_wait_s,
        "avg_bounded_wait_s": result.avg_bounded_wait_s,
        "fanin_s": result.fanin_s,
        "submit_s": result.submit_s,
        "adaptive_downshifts": 0,
        "adaptive_upshifts": 0,
        "adaptive_limit_mean": result.applied_limit,
    }


def _run_static_scheduler(
    ray_module,
    envelopes: Iterable[PayloadEnvelope],
    topology: TopologySnapshot,
    submitters: dict,
    max_inflight: int,
    options: _SchedulerOptions,
) -> tuple[list[dict], dict]:
    return _run_scheduler_with_options(
        ray_module,
        envelopes,
        topology,
        submitters,
        StaticAdmissionController(max_inflight),
        options,
    )


def _run_scheduler_with_options(
    ray_module,
    envelopes: Iterable[PayloadEnvelope],
    topology: TopologySnapshot,
    submitters: dict,
    admission,
    options: _SchedulerOptions,
) -> tuple[list[dict], dict]:
    routing_config = options.routing_config or {}
    execution = SynchronousExecutionEngine(
        admission=admission,
        router=routing_config.get("endpoint_router", RoundRobinEndpointRouter()),
        adapter=RaySubmissionAdapter(ray_module, submitters),
        pool_id=DEFAULT_POOL_ID,
        pool_router=routing_config.get("pool_router"),
        epoch_clock=options.epoch_clock or time.time,
        config=SchedulerConfig(
            endpoint_capacity=EndpointCapacityConfig(
                request_limit=options.per_endpoint_limit,
                work_limit=options.per_endpoint_work_limit,
                admission_by_endpoint=options.per_endpoint_admission or {},
            ),
            actual_work_extractor=extract_completed_token_work,
        ),
    )
    result = execution.execute(envelopes, topology)
    if options.submission_lifecycle_sink is not None:
        options.submission_lifecycle_sink.extend(result.submission_events)
    return [completion.result for completion in result.completions], _scheduler_metrics(result)


def _run_dynamic_scheduler(
    ray_module,
    envelopes: Iterable[PayloadEnvelope],
    topology: TopologySnapshot,
    submitters: dict,
    adaptive_config: dict,
    options: _SchedulerOptions,
) -> tuple[list[dict], dict]:
    trace_events = adaptive_config["trace_events"]
    trace_start = len(trace_events)
    results, metrics = _run_scheduler_with_options(
        ray_module,
        envelopes,
        topology,
        submitters,
        adaptive_config["admission_gate"],
        options,
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


def _run_per_endpoint_dynamic_scheduler(
    ray_module,
    envelopes: Iterable[PayloadEnvelope],
    topology: TopologySnapshot,
    submitters: dict,
    adaptive_config: dict,
    options: _SchedulerOptions,
) -> tuple[list[dict], dict]:
    endpoint_gates = adaptive_config["per_endpoint_gates"]
    trace_events = adaptive_config["trace_events"]
    trace_start = len(trace_events)
    max_window = int(adaptive_config["max_window"])
    global_safety_limit = max_window * len(endpoint_gates)
    per_endpoint_options = _SchedulerOptions(
        routing_config=options.routing_config,
        submission_lifecycle_sink=options.submission_lifecycle_sink,
        epoch_clock=options.epoch_clock,
        per_endpoint_limit=options.per_endpoint_limit,
        per_endpoint_admission=endpoint_gates,
    )
    results, metrics = _run_scheduler_with_options(
        ray_module,
        envelopes,
        topology,
        submitters,
        StaticAdmissionController(global_safety_limit),
        per_endpoint_options,
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
        else statistics.mean(gate.limit for gate in endpoint_gates.values())
    )
    return results, metrics
