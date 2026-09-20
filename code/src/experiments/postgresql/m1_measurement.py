"""Offline M1 output, token, and residence diagnostics from existing recorders."""
import hashlib
import json


def output_identity(path):
    values = {}
    with path.open() as stream:
        for line in stream:
            identity, text = json.loads(line)['row']
            if identity in values or not isinstance(text, str):
                raise ValueError('duplicate or malformed output identity')
            values[identity] = hashlib.sha256(text.encode()).hexdigest()
    return values


def token_usage(events, expected_rows):
    # PG observer and the direct completion event both retain model-reported usage.
    rows = [e for e in events if e['event'] in ('core_map_completion', 'direct_completion')]
    if len(rows) != expected_rows or any(type(e.get(k)) is not int or e[k] < 0
            for e in rows for k in ('prompt_tokens', 'output_tokens')):
        return dict(status='unavailable', reason='complete model-reported token usage absent from this observation mode')
    return dict(status='observed', prompt_tokens=sum(e['prompt_tokens'] for e in rows),
                output_tokens=sum(e['output_tokens'] for e in rows),
                output_token_lengths=[e['output_tokens'] for e in rows])


def retained_resource_areas(events, rss_path):
    """Integrate each domain separately; resource reservations are not physical RSS."""
    logical = [e for e in events if 'usage' in e]
    areas = {}
    for a, b in zip(logical, logical[1:]):
        delta = (b['monotonic_ns']-a['monotonic_ns'])/1e9
        if delta < 0:
            raise ValueError('logical resource observations are out of order')
        for key, value in a['usage'].items():
            areas[key] = areas.get(key, 0) + value*delta
    processes = []
    if rss_path.exists():
        with rss_path.open() as stream:
            processes = [json.loads(line) for line in stream]
    rss_area, cpu = {}, {}
    for a, b in zip(processes, processes[1:]):
        delta = (b['monotonic_ns']-a['monotonic_ns'])/1e9
        if delta < 0:
            raise ValueError('process observations are out of order')
        for key, value in a['rss_bytes'].items():
            if value is not None and b['rss_bytes'].get(key) is not None:
                rss_area[key] = rss_area.get(key, 0) + value*delta
        for key, value in a['cpu_seconds'].items():
            if key in b['cpu_seconds']:
                cpu[key] = cpu.get(key, 0) + max(0, b['cpu_seconds'][key]-value)
    return dict(logical_unit_seconds=areas or None, rss_byte_seconds=rss_area or None,
        sampled_cpu_seconds=cpu or None, process_samples=len(processes),
        scope='left-held values over observed intervals only; missing intervals excluded; per-domain values never summed as simultaneous peaks',
        logical_result_kind='reserved bytes, not actual result size',
        pg_residence_area=dict(status='unavailable', reason='PG recorder retains peaks, not a complete byte timeline'))
