"""Ray task/actor submission and typed scheduler wiring."""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field

import pyarrow as pa

from src.observability.metrics import StageTimer, scrape_prometheus_metrics
from src.modalities.text.costs import (
    OutputCostMode,
    extract_completed_token_work,
)
from src.scheduling.core.models import (
    EndpointSnapshot,
    PayloadEnvelope,
    SubmissionLifecycleEvent,
    TopologySnapshot,
)
from src.scheduling.core.scheduler import (
    EndpointCapacityConfig,
    JobSchedulingContract,
    ReadyWindowConfig,
    SchedulerConfig,
    SchedulerResult,
    SharedCreditConfig,
)
from src.scheduling.runtime.execution import SynchronousExecutionEngine
from src.scheduling.runtime.ray_adapter import (
    ActorSubmissionState,
    RaySubmissionAdapter,
)
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.submission_control.admission import StaticAdmissionController
from src.scheduling.submission_control.saor import SaorReleaseConfig
from src.scheduling.runtime.shared_credit_ray import (
    get_or_create_shared_credit_client,
)
from .replay import _batch_envelopes


DEFAULT_POOL_ID = "default"
DEFAULT_METRICS_TIMEOUT_S = 1.0
FAKE_TASK_ENDPOINT_ID = "task-0"
RAW_PROMPT_FORMAT = "raw"
COMPLETIONS_PROTOCOL = "completions"


@dataclass(frozen=True)
class _SchedulerOptions:
    """Optional scheduler wiring kept behind one internal interface."""

    routing_config: Mapping[str, object] | None = None
    submission_lifecycle_sink: list[SubmissionLifecycleEvent] | None = None
    epoch_clock: Callable[[], float] | None = None
    per_endpoint_limit: int | None = None
    per_endpoint_work_limit: int | None = None
    shared_credit: object | None = None
    job_weight: int = 1
    job_priority: int = 0
    per_endpoint_admission: Mapping[str, object] | None = None
    job_slo_target_s: float | None = None
    job_priority_window_s: float | None = None
    job_fairness_debt_cap: float | None = None
    shared_credit_acquire_timeout_s: float | None = None
    shared_ready_request_limit: int = 1
    shared_ready_work_limit: int | None = None
    shared_ready_payload_bytes_limit: int | None = None


@dataclass
class _LegacyAdaptiveStats:
    """Mutable counters shared by the two retained legacy submission paths."""

    max_inflight_seen: int = 0
    submit_count: int = 0
    fanin_s: float = 0.0
    submit_s: float = 0.0
    adaptive_downshifts: int = 0
    adaptive_upshifts: int = 0
    adaptive_limit_sum: int = 0
    adaptive_limit_samples: int = 0
    queue_wait_samples: list[float] = field(default_factory=list)

    def observe_limit(self, static_limit: int, adaptive_config: dict | None) -> int:
        current_limit, decision = adaptive_inflight_limit(static_limit, adaptive_config)
        self.adaptive_downshifts += int(decision == "down")
        self.adaptive_upshifts += int(decision == "up")
        self.adaptive_limit_sum += current_limit
        self.adaptive_limit_samples += 1
        return current_limit

    def as_metrics(self, static_limit: int) -> dict[str, float | int]:
        queue_wait_samples = self.queue_wait_samples
        adaptive_limit_mean = (
            self.adaptive_limit_sum / self.adaptive_limit_samples
            if self.adaptive_limit_samples
            else static_limit
        )
        return {
            "operator_invocations": self.submit_count,
            "max_inflight": self.max_inflight_seen,
            "bounded_wait_s": sum(queue_wait_samples),
            "avg_bounded_wait_s": (
                statistics.mean(queue_wait_samples) if queue_wait_samples else 0.0
            ),
            "fanin_s": self.fanin_s,
            "submit_s": self.submit_s,
            "adaptive_downshifts": self.adaptive_downshifts,
            "adaptive_upshifts": self.adaptive_upshifts,
            "adaptive_limit_mean": adaptive_limit_mean,
        }


@dataclass(frozen=True)
class _RemoteTaskConfig:
    """Model invocation values that otherwise travel as a large parameter clump."""

    operator: str
    embedding_dim: int
    model_backend: str
    endpoint_urls: tuple[str, ...]
    model_name: str
    api_key: str | None
    timeout_s: float
    completion_max_tokens: int
    completion_return_token_ids: bool = False
    completion_prompt_format: str = RAW_PROMPT_FORMAT
    completion_temperature: float | None = None
    completion_protocol: str = COMPLETIONS_PROTOCOL
    completion_ignore_eos: bool = False

    @property
    def uses_extended_completion_call(self) -> bool:
        return self.model_backend != "ollama" and (
            self.completion_return_token_ids
            or self.completion_prompt_format != RAW_PROMPT_FORMAT
            or self.completion_temperature is not None
            or self.completion_protocol != COMPLETIONS_PROTOCOL
            or self.completion_ignore_eos
        )


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


def _shared_credit_client(
    ray_module,
    endpoint_ids: list[str],
    config: dict | None,
):
    if not config:
        return None
    capacities = {
        endpoint_id: (
            config["request_limit"],
            config["work_limit"],
        )
        for endpoint_id in endpoint_ids
    }
    return get_or_create_shared_credit_client(
        ray_module,
        name=config["name"],
        namespace=config["namespace"],
        capacities=capacities,
        quantum=config["quantum"],
        policy=config.get("policy", "drr"),
        saor_release_config=(
            SaorReleaseConfig(**config["saor_release"])
            if config.get("saor_release") is not None
            else None
        ),
        record_ready_lifecycle_events=(
            config.get("ready_observation_contract")
            == "bounded_concrete_pre_registration"
        ),
    )


def _shared_ready_window_limits(
    max_inflight: int,
    endpoint_ids: Sequence[str],
    config: Mapping[str, object] | None,
) -> tuple[int, int | None, int | None]:
    """Derive the ready window from the frozen Job K and endpoint W."""

    if not config or config.get("ready_observation_contract") != (
        "bounded_concrete_pre_registration"
    ):
        return 1, None, None
    return (
        max_inflight,
        int(config["work_limit"]) * len(endpoint_ids),
        int(config["ready_payload_bytes_limit"]),
    )


def _static_scheduler_options(
    ray_module,
    endpoint_ids: Sequence[str],
    max_inflight: int,
    base_options: _SchedulerOptions,
    shared_credit_config: Mapping[str, object] | None,
) -> _SchedulerOptions:
    shared_credit = _shared_credit_client(
        ray_module,
        list(endpoint_ids),
        dict(shared_credit_config) if shared_credit_config is not None else None,
    )
    ready_request_limit, ready_work_limit, ready_payload_bytes_limit = (
        _shared_ready_window_limits(
            max_inflight,
            endpoint_ids,
            shared_credit_config,
        )
    )
    config = shared_credit_config or {}
    return _SchedulerOptions(
        routing_config=base_options.routing_config,
        submission_lifecycle_sink=base_options.submission_lifecycle_sink,
        epoch_clock=base_options.epoch_clock,
        per_endpoint_limit=base_options.per_endpoint_limit,
        per_endpoint_work_limit=base_options.per_endpoint_work_limit,
        shared_credit=shared_credit,
        job_weight=config.get("job_weight", 1),
        job_priority=config.get("job_priority", 0),
        job_slo_target_s=config.get("job_slo_target_s"),
        job_priority_window_s=config.get("job_priority_window_s"),
        job_fairness_debt_cap=config.get("job_fairness_debt_cap"),
        shared_credit_acquire_timeout_s=config.get("acquire_timeout_s"),
        shared_ready_request_limit=ready_request_limit,
        shared_ready_work_limit=ready_work_limit,
        shared_ready_payload_bytes_limit=ready_payload_bytes_limit,
    )


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


def _run_scheduler(
    ray_module,
    envelopes: Iterable[PayloadEnvelope],
    topology: TopologySnapshot,
    submitters: dict,
    admission,
    routing_config: dict | None = None,
    submission_lifecycle_sink: list[SubmissionLifecycleEvent] | None = None,
    epoch_clock=None,
    per_endpoint_limit: int | None = None,
    per_endpoint_work_limit: int | None = None,
    shared_credit=None,
    job_weight: int = 1,
    job_priority: int = 0,
    per_endpoint_admission: Mapping[str, object] | None = None,
    job_slo_target_s: float | None = None,
    job_priority_window_s: float | None = None,
    job_fairness_debt_cap: float | None = None,
    shared_credit_acquire_timeout_s: float | None = None,
    shared_ready_request_limit: int = 1,
    shared_ready_work_limit: int | None = None,
    shared_ready_payload_bytes_limit: int | None = None,
) -> tuple[list[dict], dict]:
    """Compatibility wrapper for callers using the former parameter list."""

    return _run_scheduler_with_options(
        ray_module,
        envelopes,
        topology,
        submitters,
        admission,
        _SchedulerOptions(
            routing_config=routing_config,
            submission_lifecycle_sink=submission_lifecycle_sink,
            epoch_clock=epoch_clock,
            per_endpoint_limit=per_endpoint_limit,
            per_endpoint_work_limit=per_endpoint_work_limit,
            shared_credit=shared_credit,
            job_weight=job_weight,
            job_priority=job_priority,
            per_endpoint_admission=per_endpoint_admission,
            job_slo_target_s=job_slo_target_s,
            job_priority_window_s=job_priority_window_s,
            job_fairness_debt_cap=job_fairness_debt_cap,
            shared_credit_acquire_timeout_s=shared_credit_acquire_timeout_s,
            shared_ready_request_limit=shared_ready_request_limit,
            shared_ready_work_limit=shared_ready_work_limit,
            shared_ready_payload_bytes_limit=shared_ready_payload_bytes_limit,
        ),
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
            shared_credit=SharedCreditConfig(
                policy=options.shared_credit,
                acquire_timeout_s=(
                    options.shared_credit_acquire_timeout_s
                ),
                ready_window=ReadyWindowConfig(
                    request_limit=options.shared_ready_request_limit,
                    work_limit=options.shared_ready_work_limit,
                    payload_bytes_limit=(
                        options.shared_ready_payload_bytes_limit
                    ),
                ),
                job=JobSchedulingContract(
                    weight=options.job_weight,
                    priority=options.job_priority,
                    slo_target_s=options.job_slo_target_s,
                    priority_window_s=options.job_priority_window_s,
                    fairness_debt_cap=options.job_fairness_debt_cap,
                ),
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


def _resolve_actor_pool_inputs(
    actor_pools: Mapping[str, Sequence[object]] | None,
    endpoint_urls: Mapping[str, str] | None,
    actors: Sequence[object] | None,
) -> tuple[Mapping[str, Sequence[object]], Mapping[str, str]]:
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
    return actor_pools, endpoint_urls


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
    completion_prompt_token_overhead: int = 0,
    actors: Sequence[object] | None = None,
    submission_state: ActorSubmissionState | None = None,
    per_endpoint_limit: int | None = None,
    per_endpoint_work_limit: int | None = None,
    shared_credit_config: dict | None = None,
) -> tuple[list[dict], dict]:
    actor_pools, endpoint_urls = _resolve_actor_pool_inputs(
        actor_pools,
        endpoint_urls,
        actors,
    )

    endpoint_ids = list(actor_pools)
    state = submission_state or ActorSubmissionState(actor_pools, method_name)
    state.validate(actor_pools, method_name)
    pool_submitters = state.pool_submitters
    counts_before = {
        endpoint_id: submitter.submission_counts
        for endpoint_id, submitter in pool_submitters.items()
    }
    typed_adaptive = adaptive_config is not None and (
        "admission_gate" in adaptive_config
        or "per_endpoint_gates" in adaptive_config
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
                prompt_token_overhead_per_request=(
                    completion_prompt_token_overhead
                ),
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
        scheduler_options = _SchedulerOptions(
            routing_config=routing_config,
            submission_lifecycle_sink=submission_lifecycle_sink,
            epoch_clock=epoch_clock,
            per_endpoint_limit=per_endpoint_limit,
            per_endpoint_work_limit=per_endpoint_work_limit,
        )
        if typed_adaptive:
            if per_endpoint_work_limit is not None:
                raise ValueError(
                    "dynamic active-work admission is not implemented; "
                    "keep the work-credit arm static for an independent "
                    "ablation"
                )
            if "per_endpoint_gates" in adaptive_config:
                results, metrics = _run_per_endpoint_dynamic_scheduler(
                    ray_module,
                    envelopes,
                    topology,
                    submitters,
                    adaptive_config,
                    scheduler_options,
                )
            else:
                if per_endpoint_limit is not None:
                    raise ValueError(
                        "global adaptive admission cannot be combined with "
                        "an endpoint-local limit"
                    )
                results, metrics = _run_dynamic_scheduler(
                    ray_module,
                    envelopes,
                    topology,
                    submitters,
                    adaptive_config,
                    scheduler_options,
                )
        else:
            static_options = _static_scheduler_options(
                ray_module,
                endpoint_ids,
                max_inflight,
                scheduler_options,
                shared_credit_config,
            )
            results, metrics = _run_static_scheduler(
                ray_module,
                envelopes,
                topology,
                submitters,
                max_inflight,
                static_options,
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
    def record_completion(handles: Sequence[object], *, failed: bool) -> None:
        if not hasattr(endpoint_submitter, "complete"):
            return
        for handle in handles:
            endpoint_submitter.complete(handle, failed=failed)

    return _run_legacy_adaptive_batches(
        ray_module,
        batches,
        max_inflight,
        adaptive_config,
        submit_batch=lambda batch, _index: endpoint_submitter(batch),
        completion_recorder=record_completion,
    )


def _collect_legacy_results(
    ray_module,
    ready: Sequence[object],
    results: list[dict],
    stats: _LegacyAdaptiveStats,
    completion_recorder: Callable[..., None] | None,
) -> None:
    fanin_timer = StageTimer.start("ray_get")
    try:
        results.extend(ray_module.get(ready))
    except Exception:
        if completion_recorder is not None:
            completion_recorder(ready, failed=True)
        raise
    else:
        if completion_recorder is not None:
            completion_recorder(ready, failed=False)
    stats.fanin_s += fanin_timer.stop()


def _run_legacy_adaptive_batches(
    ray_module,
    batches: Iterable[pa.RecordBatch | pa.Table],
    max_inflight: int,
    adaptive_config: dict | None,
    *,
    submit_batch: Callable[[object, int], object],
    completion_recorder: Callable[..., None] | None = None,
) -> tuple[list[dict], dict]:
    """Run the retained polling controller without duplicating fan-in logic."""

    pending: list[object] = []
    results: list[dict] = []
    stats = _LegacyAdaptiveStats()

    for batch in batches:
        current_limit = stats.observe_limit(max_inflight, adaptive_config)
        while len(pending) >= current_limit:
            wait_timer = StageTimer.start("bounded_wait")
            ready, pending = ray_module.wait(pending, num_returns=1)
            stats.queue_wait_samples.append(wait_timer.stop())
            _collect_legacy_results(
                ray_module,
                ready,
                results,
                stats,
                completion_recorder,
            )
            current_limit = stats.observe_limit(max_inflight, adaptive_config)

        submit_timer = StageTimer.start("submit")
        pending.append(submit_batch(batch, stats.submit_count))
        stats.submit_s += submit_timer.stop()
        stats.submit_count += 1
        stats.max_inflight_seen = max(stats.max_inflight_seen, len(pending))

    while pending:
        ready, pending = ray_module.wait(pending, num_returns=1)
        _collect_legacy_results(
            ray_module,
            ready,
            results,
            stats,
            completion_recorder,
        )

    return results, stats.as_metrics(max_inflight)


def _submit_remote_task(
    remote_embed,
    payload: object,
    config: _RemoteTaskConfig,
    endpoint_url: str | None = None,
):
    if config.model_backend == "fake":
        if config.operator == "ai_embed":
            return remote_embed.remote(payload, config.embedding_dim)
        return remote_embed.remote(payload, config.completion_max_tokens)

    if config.operator == "ai_embed":
        return remote_embed.remote(
            payload,
            endpoint_url,
            config.model_name,
            config.api_key,
            config.timeout_s,
        )
    if config.uses_extended_completion_call:
        return remote_embed.remote(
            payload,
            endpoint_url,
            config.model_name,
            config.api_key,
            config.timeout_s,
            config.completion_max_tokens,
            config.completion_return_token_ids,
            config.completion_prompt_format,
            config.completion_temperature,
            config.completion_protocol,
            config.completion_ignore_eos,
        )
    return remote_embed.remote(
        payload,
        endpoint_url,
        config.model_name,
        config.api_key,
        config.timeout_s,
        config.completion_max_tokens,
    )


def _task_submitters(
    remote_embed,
    config: _RemoteTaskConfig,
) -> tuple[list[str], list[str], dict[str, Callable[[object], object]]]:
    if config.model_backend == "fake":
        return (
            [FAKE_TASK_ENDPOINT_ID],
            ["ray://task/fake"],
            {
                FAKE_TASK_ENDPOINT_ID: lambda payload: _submit_remote_task(
                    remote_embed,
                    payload,
                    config,
                )
            },
        )
    if not config.endpoint_urls:
        raise ValueError("endpoint_urls must not be empty for an HTTP model backend")

    endpoint_ids = [f"task-{index}" for index in range(len(config.endpoint_urls))]
    submitters = {
        endpoint_id: (
            lambda payload, url=endpoint_url: _submit_remote_task(
                remote_embed,
                payload,
                config,
                url,
            )
        )
        for endpoint_id, endpoint_url in zip(endpoint_ids, config.endpoint_urls)
    }
    return endpoint_ids, list(config.endpoint_urls), submitters


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
    completion_prompt_format: str = RAW_PROMPT_FORMAT,
    completion_temperature: float | None = None,
    completion_protocol: str = COMPLETIONS_PROTOCOL,
    completion_ignore_eos: bool = False,
    completion_prompt_token_overhead: int = 0,
    per_endpoint_limit: int | None = None,
    per_endpoint_work_limit: int | None = None,
    shared_credit_config: dict | None = None,
) -> tuple[list[dict], dict]:
    task_config = _RemoteTaskConfig(
        operator=operator,
        embedding_dim=embedding_dim,
        model_backend=model_backend,
        endpoint_urls=tuple(endpoint_urls),
        model_name=model_name,
        api_key=api_key,
        timeout_s=timeout_s,
        completion_max_tokens=completion_max_tokens,
        completion_return_token_ids=completion_return_token_ids,
        completion_prompt_format=completion_prompt_format,
        completion_temperature=completion_temperature,
        completion_protocol=completion_protocol,
        completion_ignore_eos=completion_ignore_eos,
    )
    typed_adaptive = adaptive_config is not None and (
        "admission_gate" in adaptive_config
        or "per_endpoint_gates" in adaptive_config
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
            task_config,
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
            output_cost_mode=output_cost_mode,
            prompt_token_overhead_per_request=(
                completion_prompt_token_overhead
            ),
        )
    )
    endpoint_ids, endpoint_urls_for_topology, submitters = _task_submitters(
        remote_embed,
        task_config,
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
    scheduler_options = _SchedulerOptions(
        routing_config=routing_config,
        submission_lifecycle_sink=submission_lifecycle_sink,
        epoch_clock=epoch_clock,
        per_endpoint_limit=per_endpoint_limit,
        per_endpoint_work_limit=per_endpoint_work_limit,
    )
    if typed_adaptive:
        if per_endpoint_work_limit is not None:
            raise ValueError(
                "dynamic active-work admission is not implemented; keep "
                "the work-credit arm static for an independent ablation"
            )
        if "per_endpoint_gates" in adaptive_config:
            return _run_per_endpoint_dynamic_scheduler(
                ray_module,
                envelopes,
                topology,
                submitters,
                adaptive_config,
                scheduler_options,
            )
        if per_endpoint_limit is not None:
            raise ValueError(
                "global adaptive admission cannot be combined with an "
                "endpoint-local limit"
            )
        return _run_dynamic_scheduler(
            ray_module,
            envelopes,
            topology,
            submitters,
            adaptive_config,
            scheduler_options,
        )
    static_options = _static_scheduler_options(
        ray_module,
        endpoint_ids,
        max_inflight,
        scheduler_options,
        shared_credit_config,
    )
    return _run_static_scheduler(
        ray_module,
        envelopes,
        topology,
        submitters,
        max_inflight,
        static_options,
    )


def _submit_ray_tasks_legacy_adaptive(
    ray_module,
    remote_embed,
    batches: Iterable[pa.RecordBatch | pa.Table],
    max_inflight: int,
    task_config: _RemoteTaskConfig,
    adaptive_config: dict | None = None,
) -> tuple[list[dict], dict]:
    def submit_batch(batch: object, submit_index: int):
        endpoint_url = None
        if task_config.model_backend != "fake":
            endpoint_url = task_config.endpoint_urls[
                submit_index % len(task_config.endpoint_urls)
            ]
        return _submit_remote_task(
            remote_embed,
            batch,
            task_config,
            endpoint_url,
        )

    return _run_legacy_adaptive_batches(
        ray_module,
        batches,
        max_inflight,
        adaptive_config,
        submit_batch=submit_batch,
    )


def adaptive_inflight_limit(static_limit: int, adaptive_config: dict | None) -> tuple[int, str]:
    if not adaptive_config:
        return static_limit, "static"
    metrics_url = adaptive_config.get("metrics_url")
    if not metrics_url:
        return static_limit, "static"
    metrics = scrape_prometheus_metrics(
        metrics_url,
        timeout_s=DEFAULT_METRICS_TIMEOUT_S,
    )
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
