"""Ray task/actor submission and typed scheduler wiring."""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable, Iterable, Mapping, Sequence

import pyarrow as pa

from ..metrics import StageTimer, scrape_prometheus_metrics
from ..request_costs import OutputCostMode
from ..scheduling.models import (
    EndpointSnapshot,
    PayloadEnvelope,
    SubmissionLifecycleEvent,
    TopologySnapshot,
)
from ..scheduling.runtime.ray_adapter import (
    ActorSubmissionState,
    RaySubmissionAdapter,
)
from ..scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from ..scheduling.scheduler import SchedulerResult, SynchronousScheduler
from ..scheduling.submission_control.admission import StaticAdmissionController
from ..scheduling.runtime.shared_credit_ray import (
    get_or_create_shared_credit_client,
)
from .replay import _batch_envelopes

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
    )


def _run_static_scheduler(
    ray_module,
    envelopes: Iterable[PayloadEnvelope],
    topology: TopologySnapshot,
    submitters: dict,
    max_inflight: int,
    routing_config: dict | None = None,
    submission_lifecycle_sink: list[SubmissionLifecycleEvent] | None = None,
    epoch_clock=None,
    per_endpoint_limit: int | None = None,
    per_endpoint_work_limit: int | None = None,
    shared_credit=None,
    job_weight: int = 1,
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
        per_endpoint_limit,
        per_endpoint_work_limit,
        shared_credit,
        job_weight,
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
) -> tuple[list[dict], dict]:
    routing_config = routing_config or {}
    scheduler = SynchronousScheduler(
        admission=admission,
        router=routing_config.get("endpoint_router", RoundRobinEndpointRouter()),
        adapter=RaySubmissionAdapter(ray_module, submitters),
        pool_id="default",
        pool_router=routing_config.get("pool_router"),
        epoch_clock=epoch_clock or time.time,
        per_endpoint_limit=per_endpoint_limit,
        per_endpoint_work_limit=per_endpoint_work_limit,
        shared_credit=shared_credit,
        job_weight=job_weight,
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
    per_endpoint_limit: int | None = None,
    per_endpoint_work_limit: int | None = None,
    shared_credit_config: dict | None = None,
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
            if (
                per_endpoint_limit is not None
                or per_endpoint_work_limit is not None
            ):
                raise ValueError(
                    "endpoint-local admission currently supports static "
                    "scheduling only"
                )
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
            shared_credit = _shared_credit_client(
                ray_module,
                endpoint_ids,
                shared_credit_config,
            )
            results, metrics = _run_static_scheduler(
                ray_module,
                envelopes,
                topology,
                submitters,
                max_inflight,
                routing_config,
                submission_lifecycle_sink,
                epoch_clock,
                per_endpoint_limit,
                per_endpoint_work_limit,
                shared_credit,
                (
                    shared_credit_config.get("job_weight", 1)
                    if shared_credit_config
                    else 1
                ),
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
    per_endpoint_limit: int | None = None,
    per_endpoint_work_limit: int | None = None,
    shared_credit_config: dict | None = None,
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
        if (
            per_endpoint_limit is not None
            or per_endpoint_work_limit is not None
        ):
            raise ValueError(
                "endpoint-local admission currently supports static "
                "scheduling only"
            )
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
    shared_credit = _shared_credit_client(
        ray_module,
        endpoint_ids,
        shared_credit_config,
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
        per_endpoint_limit,
        per_endpoint_work_limit,
        shared_credit,
        (
            shared_credit_config.get("job_weight", 1)
            if shared_credit_config
            else 1
        ),
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
