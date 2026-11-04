"""Pure resource observations and snapshot report fields for records feedback.

The existing feedback owner retains locks, clocks, sampling and decisions.
These functions only interpret supplied values and format copied state.
"""
from dataclasses import asdict
import math


def resource_probe_view(packet, *, request_index, requested_at_s, received_at_s):
    from src.scheduling.submission_control.resource_probe import ResourceProbeView
    packet = packet if isinstance(packet, dict) else {}
    sample = packet.get('sample')
    sample = sample if isinstance(sample, dict) else {}
    values, sources = sample.get('values'), sample.get('sources')
    values = values if isinstance(values, dict) else {}
    sources = sources if isinstance(sources, dict) else {}
    host = sources.get('host')
    host = host if isinstance(host, dict) else {}
    devices = packet.get('gpu_uuids') or ()
    devices = (tuple(devices) if isinstance(devices, (tuple, list))
               and all(isinstance(uid, str) for uid in devices) else ())
    def number(value):
        return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None
    return ResourceProbeView(packet.get('engine_uid'), packet.get('session_id'), request_index,
        requested_at_s, received_at_s, number(packet.get('source_elapsed_s')), devices,
        # The shared reader aggregates multiple devices. Its mean cannot
        # establish that every device is idle; this owner uses one GPU.
        number(values.get('gpu_utilization_pct')) if len(devices) == 1 else None,
        number(host.get('cpu_allocation_utilization_pct')
               if host.get('cpu_observation_scope') == 'current_cgroup_per_effective_CPU'
               else values.get('host_cpu_per_core_max_pct'))
        if host.get('cpu_percent_interval_s') is not None else None)


def qualify_resource_observation(packet, view, *, window_s):
    # Retain source evidence even when the CPU interval is not
    # eligible for the unchanged feedback measurement window.
    packet = packet if isinstance(packet, dict) else {}
    sample = packet.get('sample')
    sample = sample if isinstance(sample, dict) else {}
    sources = sample.get('sources')
    sources = sources if isinstance(sources, dict) else {}
    host = sources.get('host')
    host = host if isinstance(host, dict) else {}
    interval = host.get('cpu_percent_interval_s')
    raw_interval = interval if type(interval) in (int, float) and math.isfinite(interval) else None
    cpu_reject = host.get('cpu_metrics_status')
    if cpu_reject == 'observed':
        cpu_reject = None
    if raw_interval is None:
        cpu_reject = cpu_reject or 'cpu_interval_missing_or_invalid'
    elif raw_interval <= 0:
        cpu_reject = 'cpu_interval_nonpositive'
    elif raw_interval > window_s:
        cpu_reject = 'cpu_interval_exceeds_feedback_window'
    else:
        cpu_reject = None if view.host_peak_cpu_pct is not None else (cpu_reject or 'cpu_value_unavailable')
    if raw_interval is None or not 0 < raw_interval <= window_s:
        from dataclasses import replace
        view = replace(view, host_peak_cpu_pct=None)
        interval = None
    return view, dict(view=asdict(view),
        status=packet.get('status', 'unavailable'), missing=sample.get('missing') or {},
        error_type=packet.get('error_type'), observed_values=sample.get('values') or {},
        host_cpu_interval_s=interval,
        host_cpu_raw_interval_s=raw_interval, host_cpu_reject_reason=cpu_reject,
        host_cpu_interval_type=type(host.get('cpu_percent_interval_s')).__name__,
        host_cpu_source={name: host[name] for name in ('cpu_metrics_status',
            'cpu_baseline_scope', 'cpu_counter_sample_sequence', 'cpu_sample_thread_id',
            'cpu_observation_scope', 'cpu_effective_slots', 'cpu_quota_slots',
            'cpu_allocation_busy_cores', 'cpu_allocation_utilization_pct', 'cpu_allocation_status',
            'cpu_counter_read_start_monotonic_ns', 'cpu_counter_read_end_monotonic_ns')
            if name in host and (type(host[name]) in (str, int) or
                (type(host[name]) is float and math.isfinite(host[name])))},
        busy_period_scope='NVML internal period; distinct RPC reads need not be independent samples')


def audit_integer(value, *, minimum=0):
    return value if type(value) is int and minimum <= value < 1 << 64 else None


def snapshot_response_header(value):
    ipc = value.get('ipc') if isinstance(value, dict) else None
    native = ipc.get('native_feedback') if isinstance(ipc, dict) else None
    native = native if isinstance(native, dict) else {}
    session = value.get('session_id') if isinstance(value, dict) else None
    uid, sequence, at = (native.get(n) for n in ('engine_uid', 'sequence', 'observed_at_s'))
    clock_error = None
    if type(at) in (int, float):
        try:
            if not math.isfinite(at):raise ValueError('non-finite source timestamp')
        except (OverflowError, ValueError) as error:
            at, clock_error = None, type(error).__name__
    else:
        at = None
    return dict(response_session_id=session if type(session) is int else None,
        response_engine_uid=uid if type(uid) is str else None,
        response_source_sequence=sequence if type(sequence) is int else None,
        response_source_observed_at_s=at, response_source_observed_at_error_type=clock_error)


def snapshot_rpc_report(*, request, receipts, requests, errors, closed, ref_retained,
                        future_retained, sampler_stopped):
    pending = (None if request is None else
               dict(request, rpc_terminal_status='unknown',
                    unresolved_reason=('client_future_cancelled_no_rpc_result'
                        if request['client_future_cancelled'] else 'terminal_not_collected')))
    complete = (closed and sampler_stopped and pending is None and not ref_retained
                and not future_retained and len(receipts) == requests)
    return dict(snapshot_requests=requests, sample_errors=errors,
        snapshot_ref_retained=ref_retained, sampler_stopped=sampler_stopped,
        snapshot_rpc_cleanup_status=('open' if not closed else 'closed' if complete else 'unknown'),
        snapshot_rpc_terminal_evidence_complete=complete,
        snapshot_rpc_receipts=[dict(row) for row in receipts], snapshot_rpc_pending=pending,
        snapshot_rpc_clock='driver monotonic seconds; response source timestamp is engine-supplied')
