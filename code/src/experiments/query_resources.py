"""Audit observed logical reservations throughout a completed query, not only EOF."""

RESOURCE_KEYS = ('held_tasks', 'input_bytes', 'result_bytes', 'active_requests', 'active_work')


def verify_logical_resources(events, limits, *, expected_jobs=None, require_usage=True):
    if set(limits) != set(RESOURCE_KEYS) or any(type(v) is not int or v < 1 for v in limits.values()):
        raise ValueError('complete positive logical resource limits required')
    peaks = dict.fromkeys(RESOURCE_KEYS, 0)
    observations = 0
    drained = []
    for event in events:
        if 'usage' not in event:
            if event['event'] == 'core_job_drained':
                raise ValueError('missing final logical resource observation')
            continue
        usage = event['usage']
        if not isinstance(usage, dict) or not set(RESOURCE_KEYS) <= usage.keys():
            raise ValueError('incomplete logical resource observation')
        for key, limit in limits.items():
            value = usage[key]
            if type(value) is not int or not 0 <= value <= limit:
                raise ValueError('logical resource limit exceeded: ' + key)
            peaks[key] = max(peaks[key], value)
        observations += 1
        if event['event'] == 'core_job_drained':
            if any(usage.values()):
                raise ValueError('PG logical resources did not drain')
            drained.append(usage)
    if require_usage and (not observations or not peaks['held_tasks'] or not peaks['active_requests']):
        raise ValueError('missing runtime logical resource observations')
    if expected_jobs is not None and len(drained) != expected_jobs:
        raise ValueError('PG logical resources did not drain')
    if require_usage and not drained:
        raise ValueError('missing final logical resource observation')
    return dict(logical_peaks=peaks if observations else None, limits=limits,
                observations=observations, drained_jobs=len(drained),
                result_bytes_kind='reserved capacity',
                scope='all recorded responsibility transitions; physical RSS reported separately')
