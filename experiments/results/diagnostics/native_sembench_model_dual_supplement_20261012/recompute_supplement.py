"""Recompute five completed slots and two unrun slots; stdlib only."""
import hashlib
import json
import tarfile
from collections import Counter, defaultdict
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from retained_evidence import read_bytes, read_json, open_archive

SOURCE_HEAD = '3bd5358706679b639e790585347133cd8d60b836'
EXPECTED = [(1, 5, 5), (2, 0, 1), (2, 1, 0), (2, 2, 5), (2, 3, 4), (2, 4, 3), (2, 5, 2)]
ARMS = ['lotus-adapted-native', 'lotus-method-native-daft', 'lotus-method-native-ray-data',
        'lotus-method-semloom', 'lotus-method-native-imlane', 'lotus-method-native-imlane-ray']


def stats(values):
    values = sorted(values)
    n = len(values)
    assert n
    def quantile(p):
        position = (n - 1) * p
        low = int(position)
        high = min(low + 1, n - 1)
        return values[low] + (values[high] - values[low]) * (position - low)
    return {'n': n, 'p50': quantile(.5), 'p95': quantile(.95), 'p99': quantile(.99),
            'max': values[-1], 'min': values[0], 'mean': sum(values) / n,
            'percentile': 'linear interpolation; descriptive'}


def metric(name, text):
    return sum(float(line.split()[-1]) for line in text.splitlines() if line.startswith(name + '{'))


def audit(raw, original=False):
    load = lambda name: json.loads(raw[name])
    rows = lambda name: [json.loads(line) for line in raw[name].splitlines()]
    result = load('model-supplement/result.json')
    watch = load('model-supplement-watchdog.json')
    plan = load('supplement-plan.json')
    clock = load('model-start.json')
    outer_elapsed = load('model-supplement-launch.json')['started_monotonic'] - clock['started_monotonic_ns'] / 1e9 + watch['elapsed_since_supervisor_launch']
    routes = load('model-preparation/route-manifest.json')
    postflight = load('independent-postflight.json')
    quality = load('reference-quality.json')
    assert result['source_head'] == plan['source_head'] == SOURCE_HEAD
    assert not result['passed'] and not watch['passed'] and watch['returncode'] == 1
    assert result['error_type'] == 'RuntimeError'
    assert result['error'] == 'original public CLI --preflight failed for lotus-method-semloom'
    assert not watch['stop_sent'] and not watch['forced_cleanup'] and not result['forced_service_cleanup']
    for cleanup in (result['cleanup'], watch['cleanup']):
        assert not cleanup['owned_survivors'] and not cleanup['affinity_violations']
        assert all(set(row['affinity']) <= set(range(72, 98)) for row in cleanup['processes'])
    assert not result['gpu_after'] and not postflight['gpu_compute_after']
    assert not postflight['registered_live_processes'] and postflight['outer_controller_exited'] and postflight['supervisor_exited']
    assert postflight['source_files_verified'] == 1001
    assert postflight['original_counts'] == {'gateway': 7680, 'router': 7680, 'model': 7680}
    assert result['independent_clock_unchanged'] and result['original_first_clock_unchanged'] and postflight['original_first_clock_unchanged']
    assert not clock['original_window_clock_reset']
    assert clock['deadline_monotonic_ns'] - clock['started_monotonic_ns'] == 1800_000_000_000
    assert result['elapsed_seconds'] <= 1800 and result['startup_seconds_used'] <= 900
    assert outer_elapsed <= 1800
    assert (plan['planned_logical'], plan['planned_posts'], plan['max_logical'], plan['max_posts'], plan['max_seconds']) == (1680, 5040, 2048, 6144, 1800)
    assert plan['independent_window'] and not plan['repeat_existing_slots']
    assert result['model_calls'] == 1200 and result['model_transmission_posts'] == 3600
    cases = result['phases']['dual']['cases']
    assert [(case['round'], case['position'], ARMS.index(case['arm'])) for case in cases] == EXPECTED[:5]
    assert [(slot['round'], slot['position'], slot['arm_index']) for slot in plan['slots']] == EXPECTED
    sends = rows('pre-send-records.jsonl')
    assert len(sends) == 3600 and Counter(row['kind'] for row in sends) == {'gateway': 1200, 'router': 1200, 'model': 1200}
    failed = 'model-supplement/dual/round-2-position-4-lotus-method-semloom/'
    assert load(failed + 'data-preflight-command.json')['returncode'] == 0
    assert load(failed + 'preflight-command.json')['returncode'] == 1
    assert not any(name.startswith(failed + 'execute') for name in raw)
    assert not any(name.startswith('model-supplement/dual/round-2-position-5-') for name in raw)
    assert 'ValueError: private Ray parent path must leave room for short unique session paths' in raw[failed + 'preflight.stderr.log'].decode()
    assert not any(row['slot'].startswith('round-2-position-4-') or row['slot'].startswith('round-2-position-5-') for row in sends)
    failure = load('failure-audit.json')
    assert failure['configured_ray_parent_bytes'] == 38 and failure['allowed_ray_parent_bytes'] == 24 and failure['original_ray_parent_bytes'] == 22
    if original:
        assert len(load('state.json')['ray_parent'].encode()) == failure['configured_ray_parent_bytes']
    assert all(row['phase'] == 'dual' and row['returned_ns'] and row['status'] == 200 and not row['error_type'] for row in sends)
    units = []
    grouped = defaultdict(list)
    protocols_all = []
    traces_all = []
    pairs = []
    endpoint_totals = defaultdict(lambda: {'requests': 0, 'prompt': 0, 'generation': 0})
    for case in cases:
        prefix = f"model-supplement/dual/round-{case['round']}-position-{case['position']}-{case['arm']}/"
        config = load(prefix + 'config.json')
        assert (config['arms'], config['queries'], config['warmups'], config['repeats'], config['max_attempts']) == ([case['arm']], [2], 1, 1, 240)
        assert config['lotus_observation_mode'] == 'performance'
        assert all(load(prefix + action + '-command.json')['returncode'] == 0 for action in ('data-preflight', 'preflight', 'execute'))
        assert case['matrix']['status'] == 'passed' and len(case['matrix']['cells']) == 2
        assert [cell['phase'] for cell in case['matrix']['cells']] == ['warmup', 'measurement']
        for cell in case['matrix']['cells']:
            path = prefix + 'output/' + cell['unit_id'] + '/'
            summary = load(path + 'summary.json')
            protocols = rows(path + 'protocols.jsonl')
            traces = rows(path + 'http-trace.jsonl')
            calls = load(path + 'calls.json')
            decisions = load(path + 'filter-decisions.json')
            assert len(protocols) == len(traces) == len(calls) == len(decisions) == cell['actual_posts'] == 120
            assert len({(c['row_id'], c['stage_id'], c['stage_ordinal'], c['call_id']) for c in calls}) == 120
            assert {c['row_id'] for c in calls} == {d[0] for d in decisions}
            assert summary['execution']['status'] == 'completed' and not summary['errors']
            assert summary['native_http_audit']['readable_json_responses'] == 120
            assert summary['identity']['original_method_preserved'] and summary['identity']['original_query']['query_id'] == 2
            assert summary['identity']['original_query']['commit'] == 'c814e3807e72d4cf876b852b17e77f3cc94575c2'
            assert load(path + 'model-method-check.json')['passed'] and summary['quality'] == quality
            assert all(t['status'] == 'completed' and t['retry_count'] == 0 and not t['errors'] and t['request_body_sha256'] == t['forwarded_body_sha256'] for t in traces)
            if original:
                digest = lambda value: hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
                assert Counter(digest(p['request']) for p in protocols) == Counter(routes['request_weights'])
            else:
                assert Counter(c['request_values_sha256'] for c in calls) == Counter(routes['request_weights']) or (case['arm'] == ARMS[0] and summary['method_http_values_audit']['status'] == 'unavailable')
            pairs.append((decisions, load(path + 'original-query-result.json'), summary['quality']))
            qmetrics = load(prefix + 'query-metrics/' + cell['unit_id'] + '-metrics.json')
            before = {v['endpoint']: v['text'] for v in qmetrics['before']}
            after = {v['endpoint']: v['text'] for v in qmetrics['after']}
            assert set(before) == set(after) and len(before) == 2
            delta = {ep: {key: metric(name, after[ep]) - metric(name, before[ep]) for key, name in
                          (('requests', 'vllm:request_success_total'), ('prompt', 'vllm:prompt_tokens_total'), ('generation', 'vllm:generation_tokens_total'))} for ep in before}
            assert all(v['requests'] == 60 and v['prompt'] > 0 and v['generation'] > 0 for v in delta.values())
            for endpoint, values in delta.items():
                for key, value in values.items():
                    endpoint_totals[endpoint][key] += value
            assert cell['full_query_seconds'] == summary['full_query_seconds'] and cell['ready_query_seconds'] == summary['execution']['ready_query_seconds']
            request_values = [(t['request_terminal_monotonic_ns'] - t['received_monotonic_ns']) / 1e9 for t in traces]
            fact = {'arm': case['arm'], 'round': case['round'], 'position': case['position'], 'phase': cell['phase'], 'path': path,
                    'full_query_seconds': cell['full_query_seconds'], 'ready_query_seconds': cell['ready_query_seconds'],
                    'full_slot_seconds': case['full_slot_seconds'], 'request_values_s': request_values, 'request_stats_s': stats(request_values),
                    'observed_peak_http': summary['observed_peak_http'], 'model_metric_delta': delta, 'errors': 0}
            units.append(fact)
            grouped[(cell['phase'], case['arm'])].append(fact)
            protocols_all.extend(protocols)
            traces_all.extend(traces)
    assert len(units) == 10 and len(traces_all) == 1200 and all(pair == pairs[0] for pair in pairs)
    assert Counter(t['request_body_sha256'] for t in traces_all) == Counter(row['body_sha256'] for row in sends if row['kind'] == 'gateway')
    router = rows('model-supplement/router-http-trace.jsonl')
    assert len(router) == 1200 and all(t['status'] == 'completed' and not t['errors'] and t['retry_count'] == 0 and t['request_body_sha256'] == t['forwarded_body_sha256'] for t in router)
    usage = {'prompt': sum(p['response']['usage']['prompt_tokens'] for p in protocols_all),
             'generation': sum(p['response']['usage']['completion_tokens'] for p in protocols_all)}
    assert usage == {key: sum(v[key] for v in endpoint_totals.values()) for key in ('prompt', 'generation')}
    services = result['services']
    assert len(services) == 2 and all(s.get('api_exit') is None and s['service_history_before'] == 0 for s in services)
    for index, service in enumerate(services):
        assert set(service['actual_api_affinity']) == set(service['engine_affinity']) == set(range(82 + index * 8, 90 + index * 8))
        root = f'model-supplement/dual/service-{index}/'
        before = raw[root + 'ready-metrics.txt'].decode()
        assert root + 'final-metrics.txt' not in raw
        last_case = cases[-1]
        last_prefix = f"model-supplement/dual/round-{last_case['round']}-position-{last_case['position']}-{last_case['arm']}/"
        last_cell = last_case['matrix']['cells'][-1]
        last_observation = load(last_prefix + 'query-metrics/' + last_cell['unit_id'] + '-metrics.json')
        endpoint = service['endpoint'].rsplit('/v1/chat/completions', 1)[0]
        after = next(value['text'] for value in last_observation['after'] if value['endpoint'] == endpoint)
        assert metric('vllm:request_success_total', before) == 0 and metric('vllm:request_success_total', after) == 600
        assert metric('vllm:prompt_tokens_total', after) == endpoint_totals[endpoint]['prompt']
        assert metric('vllm:generation_tokens_total', after) == endpoint_totals[endpoint]['generation']
    aggregates = []
    for (phase, arm), facts in grouped.items():
        queries = [f['full_query_seconds'] for f in facts]
        ready = [f['ready_query_seconds'] for f in facts]
        requests = [value for f in facts for value in f['request_values_s']]
        aggregates.append({'phase': phase, 'arm': arm, 'query_n': len(facts), 'request_n': len(requests),
                           'full_query_values_s': queries, 'ready_query_values_s': ready,
                           'full_query_stats_s': stats(queries), 'ready_query_stats_s': stats(ready),
                           'request_stats_s': stats(requests), 'max_observed_http': max(f['observed_peak_http'] for f in facts),
                           'full_query_throughput_per_s': 120 * len(facts) / sum(queries),
                           'ready_query_throughput_per_s': 120 * len(facts) / sum(ready), 'errors': 0})
    assert quality['original_metric']['f1_score'] == 1.0
    assert quality['classification_audit']['false_negative'] == 7 and quality['classification_audit']['true_positive'] == 7
    gpu_samples = defaultdict(list)
    for sample in rows('model-supplement/gpu-timeseries.jsonl'):
        if sample.get('returncode') == 0:
            for line in sample['rows']:
                columns = [value.strip() for value in line.split(',')]
                gpu_samples[columns[0]].append({'utilization': float(columns[2]), 'memory_mib': float(columns[4])})
    gpu_summary = {index: {'samples': len(values), 'utilization_mean_percent': sum(v['utilization'] for v in values) / len(values),
                           'utilization_max_percent': max(v['utilization'] for v in values), 'memory_max_mib': max(v['memory_mib'] for v in values)} for index, values in gpu_samples.items()}
    return {'source_head': SOURCE_HEAD, 'passed': True, 'run_passed': False, 'independent_window': True, 'completed_slots': 5,
            'unrun_slots': [{'round': r, 'position': p, 'arm': ARMS[a]} for r, p, a in EXPECTED[5:]],
            'warmup_queries': 5, 'measurement_queries': 5, 'logical_calls': 1200, 'transmission_posts': 3600,
            'elapsed_seconds': result['elapsed_seconds'], 'outer_elapsed_seconds': outer_elapsed,
            'startup_seconds': result['startup_seconds_used'],
            'started_epoch': clock['started_epoch'], 'ended_epoch': clock['started_epoch'] + outer_elapsed,
            'supervisor_cleanup_ended_epoch': clock['started_epoch'] + result['elapsed_seconds'],
            'limits': {'seconds': 1800, 'logical': 2048, 'posts': 6144, 'startup_seconds': 900},
            'units': units, 'aggregates': aggregates, 'quality': quality, 'classification_recall': .5,
            'completed_response_usage': usage, 'endpoint_totals': dict(endpoint_totals),
            'gpu_timeseries_summary': gpu_summary,
            'registered_process_identities': len(result['cleanup']['processes']),
            'watchdog_registered_process_identities': len(watch['cleanup']['processes']),
            'independent_checked_union_process_identities': len({(r['pid'], r['born']) for r in result['cleanup']['processes'] + watch['cleanup']['processes']}),
            'fallback_processes_before_final_cleanup': len(result['fallback_processes_before_final_cleanup']),
            'failure': failure, 'api_exit_codes': 'unavailable; registered API and engine identities independently confirmed exited',
            'final_lifetime_metrics': 'unavailable; last successful query snapshot retained',
            'owned_residuals': 0, 'affinity_violations': 0, 'gpu_compute_after': [],
            'watchdog_returncode': 1, 'original_clock_unchanged': True,
            'old_cohort': {'logical_calls': 7680, 'posts': 23040, 'single_slots': 18, 'dual_slots': 11,
                           'failures_preserved': True, 'continuous_n3_claimed': False}}


def main():
    root = Path(__file__).resolve().parent
    proof = read_json(root/'supplement-verification.json')
    members = read_json(root/'supplement-members-sha256.json')['members']
    archive = root / 'supplement-observations.tar.gz'
    assert hashlib.sha256(read_bytes(archive)).hexdigest() == proof['public_archive']['sha256']
    with open_archive(archive) as source:
        raw = {m.name: source.extractfile(m).read() for m in source if m.isfile()}
    assert set(raw) == set(members)
    for name, value in members.items():
        assert hashlib.sha256(raw[name]).hexdigest() == value['public_sha256'], name
    actual = audit(raw)
    for key, value in actual.items():
        assert value == proof[key], key
    print(json.dumps({'passed': True, 'source_head': SOURCE_HEAD, 'public_members': len(raw),
                      'run_passed': False, 'completed_slots': 5, 'unrun_slots': 2, 'logical_calls': 1200, 'posts': 3600,
                      'measurement_queries': 5, 'warmup_queries': 5, 'model_or_network_calls': 0}))


if __name__ == '__main__':
    main()
