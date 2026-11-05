"""Records execution selection and compatibility profiles, without actor allocation."""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import TYPE_CHECKING

from .records_capacity import CoreRecordsCapacity, independent_default_capacity, resolve_records_async_limits

if TYPE_CHECKING:
    from src.serving.backends.vllm_async_engine import AsyncEngineLimits

from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig, ResourceRequestConfig


def expand_core_profile(profile):
    """Retain named legacy experiment settings; ordinary defaults stay fixed."""
    if type(profile) is not str:
        return profile
    if profile not in ("delivered-work-scan", "computed-work-local-pairs"):
        raise ValueError("unknown Core query feedback profile")
    admission = dict(kind="legacy_resource_feedback", search_mode="geometric",
        poll_interval_s=0.1, window_s=0.25, min_samples=16, min_remaining=64,
        exploration_s=10.0, max_changes=8, allow_exploration=True,
        min_gain=0.03, work_tolerance=0.03, max_candidate_attempts=2,
        memory_floor_bytes=0, pressure_response="window")
    if profile == "computed-work-local-pairs":
        admission.update(work_basis="computed", work_comparison="local_pairs")
    return dict(mode="async", max_inflight=8, stage_timing=True, admission=admission)



def resolve_core_profile(engine, resources, profile=None, *, rolling_refill=False, capacity=None,
                         derived_capacity=False):
    """Resolve an explicit execution profile, without probing or allocating hardware.

    Auto is a bound on caller-declared capacity, not a throughput tuner. Device
    allocation and machine preflight remain the caller's existing responsibility.
    """
    if type(derived_capacity) is not bool:
        raise TypeError('derived_capacity must describe a boolean resolver choice')
    profile = {} if profile is None else expand_core_profile(profile)
    if type(profile) is not dict or set(profile) - {"mode", "max_inflight", "stage_timing", "admission"}:
        raise ValueError("Core profile accepts mode, max_inflight, stage_timing and optional admission only")
    mode = profile.get("mode", "sync")
    request_feedback = rolling_refill and profile.get('admission') is not None
    admission = profile.get('admission')
    modern_feedback = type(admission) is dict and admission.get('kind') == 'resource_feedback'
    budget = None if capacity is None else capacity.budget
    legacy_request_budget = resources.max_active_batches
    if budget is not None:
        if mode != 'async' or 'max_inflight' not in profile and admission is None and not derived_capacity:
            raise ValueError('explicit resource budgets require async execution and an explicit C or rolling admission')
        resources = replace(resources, max_active_batches=budget.active_requests)
    if resources.max_active_batches is None:
        raise ValueError('request capacity is unresolved; a complete resource budget is required')
    requested = profile.get("max_inflight", 1 if request_feedback or modern_feedback else resources.max_active_batches)
    timing = profile.get("stage_timing", False)
    if mode not in ("sync", "async") or type(timing) is not bool:
        raise ValueError("Core requires sync/async mode and boolean stage_timing")
    if requested == "auto":
        if mode != "async":
            raise ValueError("automatic in-flight capacity requires the existing async owner")
        maximum = resources.max_active_batches
    elif type(requested) is not int or requested < 1 or requested > resources.max_active_batches:
        raise ValueError("Core in-flight limit exceeds its declared request capacity")
    else:
        maximum = requested
    result = dict(requested=dict(profile), mode=mode, max_inflight=maximum, stage_timing=timing,
                capacity_source="Core request budget"
                if requested == "auto" else "explicit Core request budget",
                declared_request_budget=resources.max_active_batches,
                engine_max_num_seqs=engine.max_num_seqs,
                model_batch_rows=1,
                model_batch_mode="independent_final_results" if mode == "async" else "finite_synchronous_single_row",
                performance_tuned=False)
    admission = profile.get("admission")
    if admission is not None:
        if (mode != "async" or type(admission) is not dict
                or admission.get("kind") not in ('resource_feedback', 'legacy_resource_feedback')):
            raise ValueError("resource feedback requires explicit async Core admission")
        gpu_ids = admission.get("gpu_ids", ())
        if not isinstance(gpu_ids, (list, tuple)) or any(not isinstance(v, str) or not v.startswith("GPU-") for v in gpu_ids):
            raise ValueError("feedback GPU IDs must be physical UUIDs when supplied")
        options = {k: v for k, v in admission.items() if k not in ("kind", "gpu_ids")}
        if modern_feedback:
            if gpu_ids:
                raise ValueError('resource requests sample the actual model actor allocation')
            if maximum != 1:
                raise ValueError('resource requests start at one without cross-query hints')
            try:
                config = ResourceRequestConfig(**options)
            except TypeError as error:
                raise ValueError('unsupported resource request option; historical policies require explicit legacy_resource_feedback') from error
        elif request_feedback:
            from src.scheduling.submission_control.stage_work import default_request_feedback_options
            if gpu_ids:
                raise ValueError('rolling request feedback uses native stats; physical GPU UUID sampling is not connected')
            if maximum != 1:
                raise ValueError('rolling request feedback starts at one without a historical search hint')
            config = replace(default_request_feedback_options(), **options)
            if config.memory_floor_bytes:
                raise ValueError('rolling memory_floor_bytes requires a memory signal source; none is connected')
            if config.poll_interval_s > config.window_s:
                raise ValueError('request feedback poll interval must fit the measurement window')
        else:
            config = ResourceFeedbackConfig(**options)
            if config.search_policy != 'reference':
                raise ValueError('ascending request search requires rolling_refill')
        result.update(owner_max_inflight=resources.max_active_batches,
            initial_window=maximum, admission=dict(kind=admission['kind'], gpu_ids=list(gpu_ids), **asdict(config)))
    if budget is not None:
        result.update(owner_max_inflight=budget.active_requests,
            declared_legacy_request_budget=legacy_request_budget,
            resource_budget=asdict(budget), resource_budget_source='explicit deployment declaration; logical bounds, not measured RSS')
        if derived_capacity:
            result['resource_budget_source'] = 'existing IPC byte declarations and single-stage Method envelopes; logical allocations, not measured memory'
    return result


@dataclass(frozen=True)
class RecordsRuntimeConfig:
    capacity: CoreRecordsCapacity
    profile: dict
    async_limits: AsyncEngineLimits | None


def resolve_records_runtime(engine, resources, profile=None, *, capacity=None, async_limits=None,
                            prepared_batching=None, rolling_refill=False):
    """Resolve the same actual capacities before preflight and actor construction."""
    if type(rolling_refill) is not bool:
        raise ValueError('rolling_refill must be boolean')
    capacity = CoreRecordsCapacity() if capacity is None else capacity
    if type(capacity) is not CoreRecordsCapacity:
        raise TypeError('Core records capacity must be CoreRecordsCapacity')
    expanded = expand_core_profile(profile)
    mode = expanded.get('mode', 'sync') if type(expanded) is dict else 'sync'
    admission = expanded.get('admission') if type(expanded) is dict else None
    historical = type(admission) is dict and admission.get('kind') == 'legacy_resource_feedback'
    derived = (mode == 'async' and not historical and capacity.budget is None
               and (prepared_batching is None or rolling_refill))
    if derived:
        capacity, async_limits = independent_default_capacity(resources, capacity, async_limits)
    resolved = resolve_core_profile(engine, resources, expanded, rolling_refill=rolling_refill,
                                    capacity=capacity, derived_capacity=derived)
    effective = replace(resources, max_active_batches=resolved.get('owner_max_inflight', resolved['max_inflight']))
    if resolved['mode'] == 'async':
        async_limits = resolve_records_async_limits(effective, capacity, async_limits)
    elif async_limits is not None:
        raise ValueError('async_limits requires async Core execution')
    return RecordsRuntimeConfig(capacity, resolved, async_limits)



def checked_preparation_options(batching, *, interleaved_preparation=False, event_driven_preparation=False):
    """Validate existing preparation choices without allocating an execution owner."""
    if type(interleaved_preparation) is not bool:
        raise TypeError('interleaved_preparation must be boolean')
    if type(event_driven_preparation) is not bool:
        raise TypeError('event_driven_preparation must be boolean')
    if event_driven_preparation and not interleaved_preparation:
        raise ValueError('event preparation requires explicit interleaved preparation')
    if interleaved_preparation and (batching is None or not batching.get('rolling_refill')
            or 'feedback' in batching or 'request_feedback' in batching):
        raise ValueError('interleaved preparation requires fixed rolling token requests')
    return dict(**(dict(interleaved_preparation=True) if interleaved_preparation else {}),
                **(dict(event_driven_preparation=True) if event_driven_preparation else {}))


def resolve_record_batching(resolved, prepared_batching=None, batch_concurrency=None, batch_feedback=None,
                            rolling_refill=False, experimental_skip_token_packing=False, *,
                            interleaved_preparation=False, event_driven_preparation=False):
    """Validate the optional fixed token batch path before allocating an owner."""
    if type(rolling_refill) is not bool:
        raise ValueError('rolling_refill must be boolean')
    if type(experimental_skip_token_packing) is not bool:
        raise TypeError('experimental_skip_token_packing must be boolean')
    if experimental_skip_token_packing and not (prepared_batching is not None and rolling_refill):
        raise ValueError('experimental token packing bypass requires rolling prepared batches')
    if prepared_batching is None:
        if batch_concurrency is not None or batch_feedback is not None or rolling_refill:
            raise ValueError('batch controls require prepared_batching')
        checked_preparation_options(None, interleaved_preparation=interleaved_preparation,
                                    event_driven_preparation=event_driven_preparation)
        return None
    from src.semantic_methods.lotus.prepared_batching import PreparedBatching
    if (type(prepared_batching) is not PreparedBatching or prepared_batching.policy != 'token_budget'
            or prepared_batching.max_rows is not None):
        raise ValueError('record batching requires token_budget with max_rows=None')
    if resolved['mode'] != 'async' or ('admission' in resolved and not rolling_refill) or resolved['stage_timing']:
        raise ValueError('record batching requires async execution; admission requires rolling_refill, stage_timing is unsupported')
    if 'resource_budget' in resolved and not rolling_refill:
        raise ValueError('explicit resource budgets currently support ordinary requests or rolling token requests')
    if experimental_skip_token_packing and 'admission' in resolved:
        raise ValueError('experimental token packing bypass requires fixed rolling request capacity')
    requests = resolved.get('owner_max_inflight', resolved['max_inflight'])
    if rolling_refill and (batch_concurrency is not None or batch_feedback is not None):
        raise ValueError('rolling_refill uses request R, not batch_concurrency or batch_feedback')
    concurrency = None if rolling_refill else 1 if batch_concurrency is None else batch_concurrency
    if not rolling_refill and (type(concurrency) is not int or not 1 <= concurrency <= requests):
        raise ValueError('batch_concurrency must fit the request grant R')
    feedback = {}
    if batch_feedback is not None:
        from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
        if (type(batch_feedback) is not SupplyFeedbackConfig or concurrency != 1
                or batch_feedback.max_batches > requests):
            raise ValueError('batch feedback requires SupplyFeedbackConfig, initial one and a ceiling within R')
        feedback['feedback'] = asdict(batch_feedback)
    if rolling_refill:
        feedback.update(rolling_refill=True, C_request=resolved['max_inflight'], admission_unit='single_model_requests')
        if 'admission' in resolved:
            if resolved['max_inflight'] != 1:
                raise ValueError('rolling request feedback starts at one')
            feedback['request_feedback'] = {k: v for k, v in resolved['admission'].items()
                                            if k not in ('kind', 'gpu_ids')}
            feedback['request_feedback_kind'] = resolved['admission']['kind']
    batching = dict(prepared_batching=asdict(prepared_batching), batch_concurrency=concurrency,
        request_limit=requests, B=prepared_batching.token_budget, C_batch=concurrency, R=requests,
        row_cap=None, W=None, batch_unit='application transport batch with unsettled compute members',
        request_unit=('single complete model request; C gates new admission; explicit independent storage budgets'
                      if 'resource_budget' in resolved else
                      'single complete model request; soft C_request, independent resident R'
                      if 'request_feedback' in feedback else
                      'single complete model request; also the inherited resident-row limit'), **feedback,
        **({} if 'resource_budget' not in resolved else dict(resource_budget=resolved['resource_budget'])),
        **(dict(experimental_skip_token_packing=True) if experimental_skip_token_packing else {}))
    batching.update(checked_preparation_options(batching, interleaved_preparation=interleaved_preparation,
                                               event_driven_preparation=event_driven_preparation))
    return batching
