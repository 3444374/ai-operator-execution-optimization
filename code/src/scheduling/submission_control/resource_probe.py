"""Source-qualified opportunities for tentative probes, not resource grants."""

from __future__ import annotations

import math
from copy import deepcopy
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Literal

from ..core.models import AdmissionObservation

if TYPE_CHECKING:
    from .stage_work import SupplyState


@dataclass(frozen=True)
class ResourceProbeView:
    """Actor sampling enclosed by one driver's snapshot RPC issue/receive pair.

    source_elapsed_s is an actor-local duration, never an absolute cross-host
    clock. GPU busy is the maximum over the bound physical devices; host peak
    CPU is source-qualified cgroup utilization per effective CPU, or an affinity
    CPU peak for an unscoped host. It is not an actor's own CPU time.
    """
    engine_uid: str | None
    session_id: int | None
    request_index: int | None
    requested_at_s: float | None
    received_at_s: float | None
    source_elapsed_s: float | None
    gpu_uuids: tuple[str, ...]
    gpu_busy_pct: float | None
    host_peak_cpu_pct: float | None


@dataclass(frozen=True)
class ResourceOpportunity:
    kind: Literal["none", "native_headroom", "idle_device_headroom"]
    probe_target_window: int | None
    reason: str
    condition: dict
    evidence: dict


@dataclass(frozen=True)
class ResourceRequestPermit:
    """Adapter-attested operational permission for one validated native frame.

    The adapter obtains request_limit from the actual owner/IPC/storage limits,
    retains their enforcement and validates engine/session identity before
    constructing this object. backend_owns_memory means the selected backend
    owns allocation, queueing and allocation failure reporting. Neither flag
    asserts a measured GPU request capacity. observed_at_s uses the driver
    clock; native_sequence identifies the source sample, sample_sequence the
    accepted observation. A new RPC alone cannot refresh native_sequence.
    A separate guard uses its real safety_sequence, memory_pressure and its own
    safety_observed_at_s in the driver clock. observed_at_s describes current
    operational permission only. Guard source time stays unchanged in cached samples. Missing
    native/sample sequences are allowed for protection alone, never growth.
    Native memory/rejection values share the native source identity. Values
    sampled independently require their actual safety_sequence, not an RPC or
    consumer sequence. A bound safety source cannot be omitted to clear it.
    """
    engine_uid: str
    session_id: int
    sample_sequence: int | None
    native_sequence: int | None
    observed_at_s: float
    request_limit: int
    local_limits_enforced: bool
    backend_owns_memory: bool
    rejection_feedback_required: bool = False
    rejected_requests_total: int | None = None
    safety_sequence: int | None = None
    memory_pressure: bool | None = None
    safety_observed_at_s: float | None = None

    def __post_init__(self):
        if not isinstance(self.engine_uid, str) or not self.engine_uid:
            raise ValueError('engine_uid must identify the validated owner')
        for name in ('session_id', 'sample_sequence', 'native_sequence', 'request_limit', 'safety_sequence'):
            value = getattr(self, name)
            if value is None and name in ('sample_sequence', 'native_sequence', 'safety_sequence'):
                continue
            if type(value) is not int or value < (1 if name.endswith('sequence') else 0):
                raise ValueError(name+' must be a valid non-negative integer')
        if not _finite(self.observed_at_s) or self.observed_at_s < 0:
            raise ValueError('permit time must use a non-negative driver clock')
        for name in ('local_limits_enforced', 'backend_owns_memory', 'rejection_feedback_required'):
            if type(getattr(self, name)) is not bool:
                raise ValueError(name+' must be boolean')
        if self.rejected_requests_total is not None and (
                type(self.rejected_requests_total) is not int or self.rejected_requests_total < 0):
            raise ValueError('rejection total must be non-negative or unknown')
        if self.memory_pressure is not None and (
                type(self.memory_pressure) is not bool or self.safety_sequence is None):
            raise ValueError('independent memory pressure requires a boolean and its safety source sequence')
        if self.safety_sequence is not None and (
                not _finite(self.safety_observed_at_s) or self.safety_observed_at_s < 0):
            raise ValueError('an independent safety source needs its own driver-mapped sample time')
        if self.safety_sequence is None and self.safety_observed_at_s is not None:
            raise ValueError('a safety sample time needs its source sequence')


def request_supply_blocked(supply):
    """Applied request-count saturation is not downstream consumer pressure.

    Core still enforces that request maximum. All actual storage, transport and
    consumer blocks remain effective; full request occupancy may be measured
    and may take a smaller trial without pretending it permits more requests.
    """
    return supply.capacity_blocked not in (None, 'member_R')


def resource_request_opportunity(observation, supply, *, current_window, hard_limit,
        max_age_s, expected_engine_uid, expected_session_id, permit,
        physical_samples=(), resource_step=None, require_memory_feedback=False):
    """Choose a reversible position without the historical KV/busy cutoffs.

    Missing physical data uses a bounded native trial, never an idle-device
    assertion. Two qualified physical samples continuously position the trial
    within the actually ready, operationally permitted prefix. This is an
    engineering search rule, not a sustainable-capacity or benefit estimate.
    """
    if (type(current_window) is not int or type(hard_limit) is not int
            or not 1 <= current_window <= hard_limit
            or not _finite(max_age_s) or max_age_s <= 0
            or type(require_memory_feedback) is not bool):
        raise ValueError('valid current window, maximum, age and memory choice required')
    o, s = observation, supply
    evidence = dict(tentative=True, physical_capacity_known=False,
        full_query_benefit_proven=False, memory_allocation_owner='backend',
        KV_usage_is_capacity=False, free_VRAM_to_request_conversion=False)

    def result(reason, *, target=None, physical=False, devices=(), idle_share=None):
        kind = 'none' if target is None else 'idle_device_headroom' if physical else 'native_headroom'
        return ResourceOpportunity(kind, target, reason,
            dict(kind=kind, gpu_uuids=list(devices), idle_share=idle_share), dict(evidence))

    if (not isinstance(permit, ResourceRequestPermit)
            or not isinstance(expected_engine_uid, str) or not expected_engine_uid
            or type(expected_session_id) is not int
            or (permit.engine_uid, permit.session_id) != (expected_engine_uid, expected_session_id)
            or permit.sample_sequence != o.sample_sequence or permit.native_sequence is None
            or not 0 <= o.observed_at_s-permit.observed_at_s <= max_age_s):
        return result('operational_permit_unknown_or_unbound')
    evidence.update(operational_request_limit=permit.request_limit,
        source=dict(engine_uid=permit.engine_uid, session_id=permit.session_id,
                    native_sequence=permit.native_sequence, sample_sequence=permit.sample_sequence),
        scope='enforced owner/IPC/storage permission; not physical request capacity')
    if (not permit.local_limits_enforced or not permit.backend_owns_memory
            or permit.rejection_feedback_required and permit.rejected_requests_total is None):
        return result('required_safety_information_unknown')
    if (o.fresh is not True or o.preemptions_total is None
            or type(o.sample_sequence) is not int or not _finite(o.sample_age_s)
            or not 0 <= o.sample_age_s <= max_age_s
            or not 0 <= s.observed_at_s-o.observed_at_s <= max_age_s-o.sample_age_s
            or any(type(v) is not int or v < 0 for v in (o.running, o.waiting, o.inflight))
            or not _finite(o.kv_usage) or not 0 <= o.kv_usage <= 1):
        return result('native_feedback_unknown_or_stale')
    memory = permit.memory_pressure if permit.safety_sequence is not None else o.memory_pressure
    if o.memory_pressure is True or memory is True or require_memory_feedback and memory is not False:
        return result('memory_feedback_not_clear')
    if o.waiting or o.consumer_backpressure or request_supply_blocked(s):
        return result('downstream_or_native_queue_present')
    if (s.exhausted or not s.ready_items or o.pending_items is None
            or o.pending_items <= 0):
        return result('no_known_ready_pending_work')
    if (o.transition_pending or o.inflight < current_window
            or type(s.active_requests) is not int or s.active_requests < current_window):
        return result('current_window_not_saturated')
    ends = dict(owner=permit.request_limit, ready=current_window+s.ready_items,
                pending=current_window+o.pending_items)
    end = min(hard_limit, *ends.values())
    native_target = min(hard_limit, current_window*2)
    evidence.update(interval_end=end, measurable_end=end,
        native_trial_target=min(end, native_target),
        requested_native_target=native_target,
        clip_causes=[name for name, value in ends.items() if value < native_target])
    if end <= current_window:
        return result('operational_request_limit')
    from .resource_step import ResourceStep
    if isinstance(resource_step, ResourceStep) and type(resource_step.target_window) is int:
        cost = resource_step.evidence
        if (cost.get('engine_uid') == permit.engine_uid
                and cost.get('session_id') == permit.session_id
                and cost.get('sequence') == permit.native_sequence
                and cost.get('cost_model') == 'single_pool_token_blocks'
                and cost.get('fallback_required') is False
                and _finite(cost.get('sample_age_s'))
                and 0 <= cost['sample_age_s'] <= max_age_s
                and current_window < resource_step.target_window <= hard_limit):
            target = min(end, resource_step.target_window)
            evidence['costed_resource_step'] = deepcopy(cost)
            evidence['measurable_end'] = target
            if target < native_target:
                evidence['clip_causes'].append('costed_ready_prefix')
            return result('costed_ready_prefix_trial', target=target)
    fallback = min(end, current_window*2)
    if type(physical_samples) is not tuple or not physical_samples:
        return result('protected_native_trial', target=fallback)
    indexed = {}
    for view in physical_samples:
        if (not isinstance(view, ResourceProbeView) or type(view.request_index) is not int
                or view.request_index < 1
                or view.request_index in indexed and indexed[view.request_index] != view):
            return result('physical_identity_invalid_native_trial', target=fallback)
        indexed[view.request_index] = view
    if len(indexed) < 2:
        return result('physical_pair_missing_native_trial', target=fallback)
    pair = [indexed[key] for key in sorted(indexed)[-2:]]
    devices = None
    for view in pair:
        if ((view.engine_uid, view.session_id) != (permit.engine_uid, permit.session_id)
                or type(view.session_id) is not int
                or type(view.gpu_uuids) is not tuple or not view.gpu_uuids
                or any(not isinstance(uid, str) or not uid.startswith('GPU-')
                       or len(uid) <= 4 for uid in view.gpu_uuids)
                or len(set(view.gpu_uuids)) != len(view.gpu_uuids)):
            return result('physical_identity_invalid_native_trial', target=fallback)
        bound = tuple(sorted(view.gpu_uuids))
        if devices is not None and bound != devices:
            return result('physical_devices_changed_native_trial', target=fallback)
        devices = bound
        if (not all(_finite(v) for v in (view.requested_at_s, view.received_at_s, view.source_elapsed_s))
                or not 0 <= view.requested_at_s <= view.received_at_s <= s.observed_at_s
                or s.observed_at_s-view.requested_at_s > max_age_s
                or not 0 <= view.source_elapsed_s <= view.received_at_s-view.requested_at_s
                or not all(_finite(v) and 0 <= v <= 100
                    for v in (view.gpu_busy_pct, view.host_peak_cpu_pct))):
            return result('physical_time_or_values_invalid_native_trial', target=fallback)
    idle = min(1-max(v.gpu_busy_pct for v in pair)/100,
               1-max(v.host_peak_cpu_pct for v in pair)/100)
    physical_target = current_window+max(1, math.floor((end-current_window)*idle))
    target = max(fallback, physical_target)
    evidence.update(physical_samples=[asdict(v) for v in pair], joint_idle_share=idle,
        physical_target=physical_target,
        rule='max(native_trial_target, C + max(1, floor((permitted_ready_end-C)*joint_idle_share)))',
        interpretation='tentative search position; utilization is not MFU or a resource grant')
    return result('continuous_resource_trial', target=target, physical=True, devices=devices, idle_share=idle)


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def resource_probe_opportunity(observation: AdmissionObservation, supply: SupplyState, *,
        current_window, hard_limit, max_age_s, expected_engine_uid, expected_session_id,
        physical_samples: tuple[ResourceProbeView, ...] = (),
        preemption_increased=False, low_kv_usage=.5,
        physical_probe_policy="idle_majority") -> ResourceOpportunity:
    """Authorize a *trial opportunity*, leaving legal candidates to the caller.

    Native headroom permits geometric probing without a cost estimate. Two
    qualified physical reads choose a tentative point within the ready-work/R
    interval, further along when the joint observed idle share is larger. This
    search-position heuristic is not a request-capacity estimate or 1/util
    scaling. Benefit/pressure validation and existing ownership gates still win.
    """
    if (type(current_window) is not int or type(hard_limit) is not int
            or not 1 <= current_window <= hard_limit
            or not _finite(max_age_s) or max_age_s <= 0
            or not _finite(low_kv_usage) or not 0 <= low_kv_usage <= 1
            or type(preemption_increased) is not bool
            or physical_probe_policy not in ("idle_majority", "native_only")):
        raise ValueError("valid request/age/KV bounds and preemption flag required")
    o, s = observation, supply
    evidence = dict(tentative=True, cost_model_required=False,
        physical_probe_policy=physical_probe_policy,
        observed_request_window=current_window,
        full_query_benefit_proven=False, capacity_estimate=False,
        physical_scope="UUID-bound device peak busy and host-wide per-CPU peak busy",
        GPU_busy_is_MFU=False, free_VRAM_to_request_conversion=False,
        stage_supply=dict(ready_work=[asdict(w) for w in s.ready_work],
            local_capacity=deepcopy(s.local_capacity),
            capacity_blocked=s.capacity_blocked, upstream_pending=s.upstream_pending,
            source_exhausted=s.source_exhausted,
            scope='declared stage units and local ownership, not calibrated physical demand'),
        action_scope='request permits; not CPU/GPU placement, I/O scheduling or a joint resource allocator',
        native=dict(running=o.running, waiting=o.waiting, kv_usage=o.kv_usage,
            memory_pressure=o.memory_pressure, preemptions_total=o.preemptions_total,
            preemption_increased=preemption_increased),
        physical_samples_supplied=len(physical_samples) if type(physical_samples) is tuple else None)

    def decision(kind, reason, devices=(), *, target=None, condition=None):
        return ResourceOpportunity(kind, target, reason,
            condition or dict(kind=kind, native="uncongested" if kind != "none" else reason,
                              gpu_uuids=list(devices)), evidence)

    if (not isinstance(expected_engine_uid, str) or not expected_engine_uid
            or type(expected_session_id) is not int or expected_session_id < 0):
        return decision("none", "native_identity_unknown")
    if (o.fresh is not True or not o.has_service_metrics
            or any(type(v) is not int or v < 0 for v in (o.running, o.waiting, o.inflight))
            or not _finite(o.kv_usage) or not 0 <= o.kv_usage <= 1
            or not _finite(o.sample_age_s) or o.sample_age_s < 0
            or not 0 <= s.observed_at_s-o.observed_at_s
                    <= max_age_s-o.sample_age_s):
        return decision("none", "native_feedback_not_fresh")
    if o.waiting:
        return decision("none", "native_queue_present")
    if o.memory_pressure is True or preemption_increased or o.kv_usage > low_kv_usage:
        return decision("none", "native_pressure_or_kv_not_low")
    if o.consumer_backpressure or s.capacity_blocked:
        return decision("none", "consumer_or_capacity_blocked")
    if s.exhausted or not s.ready_items:
        return decision("none", "no_ready_supply")
    if current_window == hard_limit or o.inflight >= hard_limit or (
            s.active_requests is not None and s.active_requests >= hard_limit):
        return decision("none", "request_capacity_boundary")
    if (o.transition_pending or o.inflight < current_window
            or s.active_requests is None or s.active_requests < current_window):
        return decision("none", "current_window_not_fully_inflight")

    def regular(reason):
        evidence["physical_qualification"] = reason
        return decision("native_headroom", reason)

    if physical_probe_policy == "native_only":
        return regular("physical_acceleration_disabled")
    if type(physical_samples) is not tuple or not physical_samples:
        return regular("physical_samples_unavailable")
    indexed = {}
    for view in physical_samples:
        if (not isinstance(view, ResourceProbeView) or type(view.request_index) is not int
                or view.request_index < 1):
            return regular("physical_source_identity_invalid")
        if view.request_index in indexed and indexed[view.request_index] != view:
            return regular("physical_request_index_conflict")
        indexed[view.request_index] = view
    if len(indexed) < 2:
        return regular("two_distinct_physical_samples_required")
    # Inspect the latest two, not a hand-picked idle subset of the history.
    pair = [indexed[key] for key in sorted(indexed)[-2:]]
    evidence['physical_considered'] = [asdict(v) for v in pair]
    devices = None
    for view in pair:
        if (view.engine_uid != expected_engine_uid or type(view.session_id) is not int
                or view.session_id != expected_session_id
                or type(view.gpu_uuids) is not tuple or not view.gpu_uuids
                or any(not isinstance(uid, str) or not uid.startswith("GPU-")
                       or len(uid) <= 4 for uid in view.gpu_uuids)
                or len(set(view.gpu_uuids)) != len(view.gpu_uuids)):
            return regular("physical_source_identity_invalid")
        bound = tuple(sorted(view.gpu_uuids))
        if devices is not None and bound != devices:
            return regular("physical_device_binding_changed")
        devices = bound
        if (not all(_finite(v) for v in (view.requested_at_s, view.received_at_s, view.source_elapsed_s))
                or not 0 <= view.requested_at_s <= view.received_at_s <= s.observed_at_s
                or s.observed_at_s-view.requested_at_s > max_age_s
                or not 0 <= view.source_elapsed_s <= view.received_at_s-view.requested_at_s):
            return regular("physical_RPC_time_bounds_invalid_or_stale")
        if (not all(_finite(v) and 0 <= v <= 100
                    for v in (view.gpu_busy_pct, view.host_peak_cpu_pct))):
            return regular("physical_busy_metrics_unknown_or_invalid")
    gpu_busy = max(v.gpu_busy_pct for v in pair)
    host_busy = max(v.host_peak_cpu_pct for v in pair)
    if gpu_busy >= 50 or host_busy >= 50:
        # This is an observed busy condition, unlike a missing/invalid read.
        # Remembering that distinction permits a genuine later busy->idle
        # transition without turning every coverage gap into a new opportunity.
        evidence['physical_qualification'] = 'physical_idle_majority_not_observed'
        return decision('native_headroom', 'physical_idle_majority_not_observed', devices,
            condition=dict(kind='physical_busy', native='uncongested', gpu_uuids=list(devices)))
    idle_share = min(1-gpu_busy/100, 1-host_busy/100, 1-o.kv_usage)
    endpoint = min(hard_limit, current_window+s.ready_items)
    target = current_window+max(1, math.floor((endpoint-current_window)*idle_share))
    evidence.update(physical_qualification="two_fresh_idle_majority_samples",
        physical_request_indices=[v.request_index for v in pair],
        physical_considered=[asdict(v) for v in pair],
        idle_majority_rule="device and host peak busy < 50 percent; not a hardware capacity boundary",
        probe_position=dict(rule='C + max(1, floor((min(R,C+ready)-C)*joint_idle_share))',
            current_window=current_window, hard_limit=hard_limit, ready_items=s.ready_items,
            interval_end=endpoint, gpu_busy_pct=gpu_busy, host_peak_cpu_pct=host_busy,
            kv_usage=o.kv_usage, joint_idle_share=idle_share, target=target,
            scope='reversible search position, not predicted sustainable request capacity'))
    return decision("idle_device_headroom", "two_fresh_idle_majority_samples", devices, target=target,
        condition=dict(kind='idle_device_headroom', native='uncongested',
                       gpu_uuids=list(devices), idle_share=idle_share))
