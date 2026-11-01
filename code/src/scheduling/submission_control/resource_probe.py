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
    CPU is host-wide per-CPU peak busy, not process CPU or controller CPU time.
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


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def resource_probe_opportunity(observation: AdmissionObservation, supply: SupplyState, *,
        current_window, hard_limit, max_age_s, expected_engine_uid, expected_session_id,
        physical_samples: tuple[ResourceProbeView, ...] = (),
        preemption_increased=False, low_kv_usage=.5) -> ResourceOpportunity:
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
            or type(preemption_increased) is not bool):
        raise ValueError("valid request/age/KV bounds and preemption flag required")
    o, s = observation, supply
    evidence = dict(tentative=True, cost_model_required=False,
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
