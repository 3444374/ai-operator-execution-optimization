"""Decompose the retained Q2 observations; stdlib only, no network or GPU access."""
import argparse
import hashlib
import json
import math
import re
import tarfile
from collections import defaultdict
from pathlib import Path

SOURCE_HEAD = '3bd5358706679b639e790585347133cd8d60b836'
ARMS = ['lotus-adapted-native', 'lotus-method-native-daft', 'lotus-method-native-ray-data',
        'lotus-method-semloom', 'lotus-method-native-imlane', 'lotus-method-native-imlane-ray']
HTTP_STAGES = {
    'entry_to_dispatch': ('received_monotonic_ns', 'upstream_dispatch_started_monotonic_ns'),
    'dispatch_to_headers': ('upstream_dispatch_started_monotonic_ns', 'upstream_headers_send_started_monotonic_ns'),
    'headers_to_body': ('upstream_headers_send_started_monotonic_ns', 'upstream_response_body_read_completed_monotonic_ns'),
    'body_to_eof': ('upstream_response_body_read_completed_monotonic_ns', 'request_terminal_monotonic_ns'),
}
METRICS = {key: 'vllm:'+name+'_seconds' for key, name in {
    'service_e2e': 'e2e_request_latency', 'queue': 'request_queue_time',
    'inference': 'request_inference_time', 'prefill': 'request_prefill_time',
    'decode': 'request_decode_time', 'ttft': 'time_to_first_token',
}.items()}


def archive(path, digest):
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, path.name
    with tarfile.open(path) as source:
        return {member.name: source.extractfile(member).read() for member in source if member.isfile()}


def json_rows(raw, name):
    return [json.loads(line) for line in raw[name].splitlines() if line.strip()]


def interval(a, b):
    assert isinstance(a, int) and isinstance(b, int) and b >= a, (a, b)
    return (b-a)/1e9


def covered(intervals):
    end = None
    total = 0
    for start, stop in sorted(intervals):
        assert stop >= start
        total += max(0, stop-max(start, end if end is not None else start))
        end = max(end, stop) if end is not None else stop
    return total/1e9


def http(rows):
    assert len(rows) == 120
    assert all(row['timing_clock'] == 'time.monotonic_ns' and row['status'] == 'completed'
               and row['retry_count'] == 0 for row in rows)
    sums = {key: sum(interval(row[a], row[b]) for row in rows) for key, (a, b) in HTTP_STAGES.items()}
    total = sum(interval(row['received_monotonic_ns'], row['request_terminal_monotonic_ns']) for row in rows)
    assert math.isclose(total, sum(sums.values()), abs_tol=1e-9)
    return {'request_n': 120, 'request_e2e_sum_s': total, 'stage_sums_s': sums,
            'connections_created': sum(row['upstream_connection_create_completed_count'] for row in rows),
            'connections_reused': sum(row['upstream_connection_reused_count'] for row in rows),
            'connector_queue_observations': sum(row['upstream_connection_queued_completed_count'] for row in rows)}


def prometheus(text):
    values = defaultdict(float)
    for line in text.splitlines():
        match = re.match(r'^([^\s{]+)(?:\{[^}]*\})?\s+([-+0-9.eE]+)(?:\s|$)', line)
        if match:
            values[match[1]] += float(match[2])
    return values


def model_sums(record):
    before = {part['endpoint']: prometheus(part['text']) for part in record['before']}
    after = {part['endpoint']: prometheus(part['text']) for part in record['after']}
    assert before.keys() == after.keys()
    values = defaultdict(float)
    counts = defaultdict(float)
    for endpoint, final in after.items():
        for key, metric in METRICS.items():
            delta = final[metric+'_sum']-before[endpoint][metric+'_sum']
            assert delta >= -1e-9
            values[key] += delta
            counts[key] += final[metric+'_count']-before[endpoint][metric+'_count']
    assert all(count == 120 for count in counts.values()), counts
    return {'request_n': 120, 'sums_s': dict(values), 'scope': 'matched query counter deltas, cumulative request spans; not GPU kernel wall'}


def compact_spans(value, prefix=''):
    spans = {}
    if not isinstance(value, dict):
        return spans
    if isinstance(value.get('samples'), list):
        samples = value['samples']
        assert len(samples) == value['count']
        spans[prefix] = {'n': len(samples), 'sum_s': sum(samples), 'status': value['status']}
    else:
        for key, item in value.items():
            if isinstance(item, dict):
                spans.update(compact_spans(item, (prefix+'.'+key).strip('.')))
    return spans


def query_fact(raw, routers, gpu, cohort, unit):
    path = unit['path']
    summary = json.loads(raw[path+'summary.json'])
    execution = summary['execution']
    release = summary['query_preparation_started_ns']
    submit = execution['t_submit_ns']
    terminal = execution['t_query_terminal_ns']
    traces = json_rows(raw, path+'http-trace.jsonl')
    first = min(row['received_monotonic_ns'] for row in traces)
    last = max(row['request_terminal_monotonic_ns'] for row in traces)
    assert release <= submit <= first <= last <= terminal
    selected = [row for row in routers if first <= row['received_monotonic_ns'] <= last]
    assert len(selected) == 120
    assert all(first <= row['received_monotonic_ns'] <= row['request_terminal_monotonic_ns'] <= last for row in selected)
    span = covered((row['received_monotonic_ns'], row['request_terminal_monotonic_ns']) for row in traces)
    wall = {'query_preparation': interval(release, submit),
            'submit_to_first_http': interval(submit, first), 'http_covered': span,
            'http_uncovered_gaps': interval(first, last)-span,
            'last_http_to_consumer_eof': interval(last, terminal)}
    assert math.isclose(sum(wall.values()), unit['full_query_seconds'], abs_tol=1e-8)
    assert math.isclose(sum(v for k,v in wall.items() if k != 'query_preparation'), unit['ready_query_seconds'], abs_tol=1e-8)
    slot = path.split('/output/')[0]
    observer = json.loads(raw[slot+'/query-metrics/'+path.rstrip('/').split('/')[-1]+'-metrics.json'])
    assert observer['started_ns'] <= release and observer['ended_ns'] >= summary['ended_ns']
    metrics_get = sum(interval(part['started_ns'], part['ended_ns']) for key in ('before', 'after') for part in observer[key])
    gpu_values = defaultdict(list)
    for sample in gpu:
        if release <= sample['started_ns'] < terminal and sample['returncode'] == 0:
            for line in sample['rows']:
                columns = [value.strip() for value in line.split(',')]
                gpu_values[columns[0]].append((float(columns[2]), float(columns[4]), float(columns[5])))
    gpu_facts = {index: {'samples': len(values), 'util_mean_percent': sum(v[0] for v in values)/len(values),
                         'util_max_percent': max(v[0] for v in values), 'memory_max_mib': max(v[1] for v in values),
                         'power_mean_w': sum(v[2] for v in values)/len(values)} for index, values in gpu_values.items()}
    return {'cohort': cohort, 'topology': unit.get('topology', 'dual'), 'arm': unit['arm'],
            'round': unit['round'], 'position': unit['position'], 'phase': unit['phase'], 'path': path,
            'full_query_s': unit['full_query_seconds'], 'ready_query_s': unit['ready_query_seconds'],
            'wall_parts_s': wall, 'source_s': summary['stages']['source_seconds'],
            'official_evaluation_s': summary['stages']['evaluation_seconds'],
            'stream_cleanup_after_eof_s': interval(terminal, execution['t_stream_cleanup_ns']),
            'results_durable_after_eof_s': interval(terminal, execution['t_results_durable_ns']),
            'post_eof_summary_s': interval(terminal, summary['ended_ns']),
            'summary_to_observer_return_s': interval(summary['ended_ns'], observer['ended_ns']),
            'metrics_get_outside_query_s': metrics_get,
            'inline_recording_s': execution['inline_recording_seconds'],
            'owner_http': http(traces), 'router_http': http(selected), 'vllm': model_sums(observer),
            'observed_peak_http': summary['observed_peak_http'],
            'gpu_query_samples': gpu_facts,
            'executor_cumulative_spans': compact_spans(summary['executor_phases'])}


def mean(values):
    assert values
    return sum(values)/len(values)


def analyse(root):
    original = root.parent/'native_sembench_model_20261012'
    proofs = [json.loads((directory/name).read_text()) for directory, name in [
        (original, 'performance-verification.json'), (root, 'supplement-verification.json'), (root, 'sr-verification.json')]]
    names = ['original', 'first_supplement', 'sr_supplement']
    paths = [original/'performance-observations.tar.gz', root/'supplement-observations.tar.gz', root/'sr-observations.tar.gz']
    raws = [archive(path, proof['public_archive']['sha256']) for path, proof in zip(paths, proofs)]
    physical_counts = [len(raw) for raw in raws]
    manifest = json.loads((root/'sr-members-sha256.json').read_text())
    assert manifest['reused_archive']['sha256'] == proofs[1]['public_archive']['sha256']
    for name, item in manifest['members'].items():
        storage = item['storage']
        if storage['kind'] == 'reuse':
            raws[2][name] = raws[1][storage['member']]
        elif storage['kind'] == 'concat':
            raws[2][name] = b''.join(raws[2][part] for part in storage['parts'])
    assert set(raws[2]) == set(manifest['members'])
    assert all(hashlib.sha256(raws[2][name]).hexdigest() == item['public_sha256'] for name, item in manifest['members'].items())
    facts, slots, services, sources, seriality = [], [], [], [], []
    for cohort, raw, proof, path in zip(names, raws, proofs, paths):
        assert proof['source_head'] == SOURCE_HEAD
        prefix = 'model-resume' if cohort == 'original' else 'model-supplement'
        routers = json_rows(raw, prefix+'/router-http-trace.jsonl')
        gpu = json_rows(raw, prefix+'/gpu-timeseries.jsonl')
        units = proof['units']
        assert len(units) == {'original': 58, 'first_supplement': 10, 'sr_supplement': 4}[cohort]
        query_intervals = sorted((json.loads(raw[unit['path']+'summary.json'])['query_preparation_started_ns'],
                                  json.loads(raw[unit['path']+'summary.json'])['execution']['t_query_terminal_ns']) for unit in units)
        assert all(previous[1] <= following[0] for previous, following in zip(query_intervals, query_intervals[1:]))
        seriality.append({'cohort': cohort, 'queries': len(query_intervals), 'overlapping_query_intervals': 0,
                          'scope': 'query preparation through consumer EOF, within each same-host cohort'})
        facts.extend(query_fact(raw, routers, gpu, cohort, unit) for unit in units)
        result = json.loads(raw[prefix+'/result.json'])
        for service in result['services']:
            services.append({key: service.get(key) for key in ('phase', 'replica', 'started_ns', 'ready_ns', 'readiness_seconds', 'service_history_before', 'api_exit') } | {'cohort': cohort})
        for topology, phase in result['phases'].items():
            for case in phase['cases']:
                slot = prefix+f"/{topology}/round-{case['round']}-position-{case['position']}-{case['arm']}/"
                members = [unit for unit in facts if unit['cohort'] == cohort and unit['path'].startswith(slot)]
                assert len(members) == 2
                startup = json.loads(raw[slot+'output/owner-0/startup.json'])
                group = json.loads(raw[slot+'output/owner-0/group-summary.json'])
                preflights = {name: json.loads(raw[slot+name+'-command.json'])['seconds'] for name in ('data-preflight', 'preflight')}
                query_total = sum(unit['full_query_s'] for unit in members)
                config = json.loads(raw[slot+'config.json'])
                assert config['lotus_observation_mode'] == 'performance'
                assert config['arms'] == [case['arm']] and config['warmups'] == config['repeats'] == 1
                slots.append({'cohort': cohort, 'topology': topology, 'arm': case['arm'], 'round': case['round'],
                              'position': case['position'], 'path': slot, 'full_slot_s': case['full_slot_seconds'],
                              'zero_model_preflight_s': sum(preflights.values()), 'owner_startup_s': startup['startup_seconds'],
                              'queries_full_s_sum': query_total,
                              'slot_other_s': case['full_slot_seconds']-sum(preflights.values())-startup['startup_seconds']-query_total,
                              'owner_group_closed_ns': group['ended_ns'],
                              'ledger_rows_before_slot': case['count_before']['total'],
                              'ledger_rows_after_slot': case['count_after']['total'],
                              'native_options': config['native_options']})
                assert slots[-1]['slot_other_s'] >= 0
        sources.append({'cohort': cohort, 'archive': path.name, 'sha256': proof['public_archive']['sha256'],
                        'physical_members': physical_counts[names.index(cohort)],
                        'logical_members': len(raw), 'verification_sha256': hashlib.sha256(json.dumps(proof, sort_keys=True).encode()).hexdigest()})
    assert len(facts) == 72 and len(slots) == 36
    groups = []
    for cohort, topology in [('original','single'),('original','dual'),('first_supplement','dual'),('sr_supplement','dual')]:
        for arm in ARMS:
            measurements = [unit for unit in facts if (unit['cohort'], unit['topology'], unit['arm'], unit['phase']) == (cohort,topology,arm,'measurement')]
            if not measurements:
                continue
            warmups = [unit for unit in facts if (unit['cohort'], unit['topology'], unit['arm'], unit['phase']) == (cohort,topology,arm,'warmup')]
            matching_slots = [slot for slot in slots if (slot['cohort'],slot['topology'],slot['arm']) == (cohort,topology,arm)]
            item = {'cohort': cohort, 'topology': topology, 'arm': arm, 'query_n': len(measurements), 'request_n': 120*len(measurements),
                    'warmup_full_values_s': [unit['full_query_s'] for unit in warmups],
                    'query_preparation_mean_s': mean([unit['wall_parts_s']['query_preparation'] for unit in measurements]),
                    'source_mean_s': mean([unit['source_s'] for unit in measurements]),
                    'http_covered_mean_s': mean([unit['wall_parts_s']['http_covered'] for unit in measurements]),
                    'ready_query_mean_s': mean([unit['ready_query_s'] for unit in measurements]),
                    'ready_outside_http_mean_s': mean([unit['ready_query_s']-unit['wall_parts_s']['http_covered'] for unit in measurements]),
                    'evaluation_mean_s': mean([unit['official_evaluation_s'] for unit in measurements]),
                    'post_eof_summary_mean_s': mean([unit['post_eof_summary_s'] for unit in measurements]),
                    'stream_cleanup_mean_ms': mean([unit['stream_cleanup_after_eof_s'] for unit in measurements])*1000,
                    'owner_startup_values_s': [slot['owner_startup_s'] for slot in matching_slots],
                    'slot_other_values_s': [slot['slot_other_s'] for slot in matching_slots]}
            for hop in ('owner_http','router_http'):
                item[hop+'_mean_ms'] = {key: sum(unit[hop]['stage_sums_s'][key] for unit in measurements)/item['request_n']*1000 for key in HTTP_STAGES}
                item[hop+'_e2e_mean_ms'] = sum(unit[hop]['request_e2e_sum_s'] for unit in measurements)/item['request_n']*1000
            item['vllm_mean_ms'] = {key: sum(unit['vllm']['sums_s'][key] for unit in measurements)/item['request_n']*1000 for key in METRICS}
            groups.append(item)
    return {'schema': 'semloom.sembench_q2_retained_phase_analysis.v1', 'source_head': SOURCE_HEAD,
            'passed': True, 'new_model_or_network_calls': 0, 'units': facts, 'slots': slots, 'services': services,
            'groups': groups, 'sources': sources, 'query_seriality': seriality,
            'limits': {'request_cumulative_is_wall_time': False, 'nested_proxy_intervals_are_added': False,
                       'cross_machine_clocks_subtracted': False, 'gpu_temperature_or_clock_collected': False,
                       'model_per_call_identity_in_metrics': False, 'sdk_parse_and_aggregate_separately_measured': False,
                       'native_method_call_e2e_is_independent_request_sample': False}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--result-dir', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--write', type=Path)
    args = parser.parse_args()
    actual = analyse(args.result_dir)
    if args.write:
        args.write.write_text(json.dumps(actual, ensure_ascii=False, indent=2)+'\n')
    else:
        assert actual == json.loads((args.result_dir/'stage-analysis.json').read_text())
    print(json.dumps({'passed': True, 'retained_queries': len(actual['units']), 'warmups': 36,
                      'measurements': 36, 'slots': len(actual['slots']), 'cohorts': 3,
                      'new_model_or_network_calls': 0, 'source_head': SOURCE_HEAD}))


if __name__ == '__main__':
    main()
