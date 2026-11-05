"""Source-qualified opportunities for tentative probes, not resource grants."""

from __future__ import annotations

import math
from copy import deepcopy
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Literal

from ..core.models import AdmissionObservation

if TYPE_CHECKING:
    from .feedback_work import SupplyState


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
        physical_samples=(), resource_step=None, require_memory_feedback=False, after_s=None):
    """Choose a reversible position without the historical KV/busy cutoffs.

    Missing physical data uses a bounded native trial, never an idle-device
    assertion. Qualified physical observations continuously position the trial
    within the actually ready, operationally permitted prefix. This is an
    engineering search rule, not a sustainable-capacity or benefit estimate.
    """
    if (type(current_window) is not int or type(hard_limit) is not int
            or not 1 <= current_window <= hard_limit
            or not _finite(max_age_s) or max_age_s <= 0
            or type(require_memory_feedback) is not bool
            or after_s is not None and (not _finite(after_s) or after_s < 0)):
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
    if (o.inflight != current_window
            or type(s.active_requests) is not int or s.active_requests != current_window):
        return result('current_window_not_saturated')
    if after_s is not None and o.observed_at_s-o.sample_age_s < after_s:
        return result('native_predates_current_window')
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
    fallback = None if o.transition_pending else min(end, current_window*2)
    if type(physical_samples) is not tuple or not physical_samples:
        return result('protected_native_trial', target=fallback)
    indexed = {}
    for view in physical_samples:
        if (not isinstance(view, ResourceProbeView) or type(view.request_index) is not int
                or view.request_index < 1
                or view.request_index in indexed and indexed[view.request_index] != view):
            return result('physical_identity_invalid_native_trial', target=fallback)
        indexed[view.request_index] = view
    pair = [indexed[key] for key in sorted(indexed)[-2:]]
    if after_s is not None:
        pair = [view for view in pair if not (
            _finite(view.requested_at_s) and view.requested_at_s < after_s)]
        if not pair:
            return result('physical_predates_current_window_native_trial', target=fallback)
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
    physical_target = current_window+math.floor((end-current_window)*idle)
    if physical_target <= current_window:
        return result('no_observed_joint_spare_native_trial', target=fallback)
    target = physical_target
    evidence.update(physical_samples=[asdict(v) for v in pair], joint_idle_share=idle,
        physical_target=physical_target,
        rule='C + floor((permitted_ready_end-C)*joint_idle_share)',
        interpretation='tentative search position; utilization is not MFU or a resource grant')
    return result('continuous_resource_trial', target=target, physical=True, devices=devices, idle_share=idle)


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)
