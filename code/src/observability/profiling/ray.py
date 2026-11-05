"""Ray task and actor submission for profiler workloads."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa

from src.modalities.text.costs import OutputCostMode
from src.scheduling.core.models import PayloadEnvelope, SubmissionLifecycleEvent
from src.scheduling.runtime.ray_adapter import ActorSubmissionState
from .ray_scheduler import (
    DEFAULT_POOL_ID as DEFAULT_POOL_ID,
    _SchedulerOptions,
    _endpoint_topology,
    _run_dynamic_scheduler,
    _run_per_endpoint_dynamic_scheduler,
    _run_static_scheduler,
    _static_scheduler_options,
)
from .batch_envelopes import _batch_envelopes


FAKE_TASK_ENDPOINT_ID = "task-0"
RAW_PROMPT_FORMAT = "raw"
COMPLETIONS_PROTOCOL = "completions"


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
        raise ValueError(
            "adaptive_config requires admission_gate or per_endpoint_gates; "
            "HTTP metrics polling is retired"
        )
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
        raise ValueError(
            "adaptive_config requires admission_gate or per_endpoint_gates; "
            "HTTP metrics polling is retired"
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
