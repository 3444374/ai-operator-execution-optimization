"""Interval and contained-sample summaries for offline record observations."""
import math


def is_number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _union(intervals):
    result = []
    for a, b in sorted(intervals):
        if result and a <= result[-1][1]:
            result[-1][1] = max(b, result[-1][1])
        else:
            result.append([a, b])
    return result


def interval_duration(intervals):
    return sum(b - a for a, b in _union(intervals))


def _overlap(left, right):
    return interval_duration([(max(a, c), min(b, d)) for a, b in _union(left)
                              for c, d in _union(right) if max(a, c) < min(b, d)])


def interval_peak(intervals):
    points = sorted([(a, 1) for a, b in intervals if a < b] +
                    [(b, -1) for a, b in intervals if a < b])
    active = peak = 0
    for _, delta in points:
        active += delta
        peak = max(peak, active)
    return peak


def summarize_windows(intervals, samples, *, source_paths=(), gpu_uuids=None):
    """Measure shared sample overlap without assigning exclusive stage demand."""
    sample_groups = {}
    for sample in samples:
        sample_groups.setdefault((sample['_source'], sample['_domain']), []).append(sample)
    stages = {}
    for stage, domains in intervals.items():
        observation = dict(windows=[], resources=[])
        for domain, windows in domains.items():
            merged = _union(windows)
            observation['windows'].append(dict(clock_domain=domain, union_ns=interval_duration(merged),
                envelope_ns=max(b for a, b in merged) - min(a for a, b in merged),
                peak_observed_API_intervals=interval_peak(windows), kind='API_or_operator_window_not_GPU_time'))
            for path in source_paths:
                selected = [value for value in sample_groups.get((path, domain), ())
                    if is_number(value.get('sample_start_monotonic_ns')) and is_number(value.get('sample_end_monotonic_ns'))
                    and any(a <= value['sample_start_monotonic_ns'] <= value['sample_end_monotonic_ns'] <= b for a, b in merged)]
                values = {}
                for metric in ('gpu_utilization_pct', 'gpu_memory_used_mib', 'host_cpu_busy_cores',
                               'host_memory_available_mib', 'vllm_num_requests_running', 'vllm_num_requests_waiting',
                               'vllm_kv_cache_usage_perc'):
                    measured = []
                    for value in selected:
                        group = 'gpu' if metric.startswith('gpu_') else 'host' if metric.startswith('host_') else 'native'
                        source = value.get('sources', {}).get(group, {})
                        status = source.get('status')
                        unavailable = isinstance(status, str) and status.split(':', 1)[0] in ('unavailable', 'unknown', 'unqualified')
                        valid = metric not in value.get('missing', {}) and not unavailable
                        if metric.startswith('gpu_'):
                            actual = source.get('physical_gpu_uuids')
                            valid = valid and bool(gpu_uuids) and actual == gpu_uuids and bool(status) and not status.startswith('unavailable')
                        measured.append(value.get('values', {}).get(metric) if valid else None)
                    known = [value for value in measured if is_number(value)]
                    values[metric] = dict(observed=len(known), missing=len(measured)-len(known),
                        minimum=min(known) if known else None, maximum=max(known) if known else None,
                        unavailable_reason=None if known else 'metric_missing_or_unbound_device' if selected
                            else 'no_same_domain_contained_sample')
                observation['resources'].append(dict(source=path, sample_count=len(selected), values=values,
                    attribution='shared_source_samples_inside_window_not_stage_exclusive',
                    status='observed_sample_overlap' if selected else 'unknown:no_qualified_samples'))
        stages[stage] = observation
    overlaps = []
    for a in sorted(intervals):
        for b in sorted(intervals):
            if a >= b:
                continue
            common = set(intervals[a]) & set(intervals[b])
            overlaps.append(dict(stages=[a, b], status='observed' if common else 'unknown:clock_domains_unjoined',
                by_domain=[dict(clock_domain=domain, overlap_ns=_overlap(intervals[a][domain], intervals[b][domain]))
                           for domain in sorted(common)],
                meaning='API_or_operator_overlap_not_proof_of_concurrent_GPU_kernels'))
    return stages, overlaps
