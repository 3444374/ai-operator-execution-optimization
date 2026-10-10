"""Verify retained adapter observations without a database, model, or SDK run."""
import gzip
import hashlib
import json
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parents[3] / 'code'))
from src.experiments.postgresql.native_adapter_http import bind_native_http_events
from src.experiments.postgresql.native_adapter_metrics import sample_distribution, summarize_calls, summarize_queries


def stored_json(info):
    encoded = (ROOT / info['path']).read_bytes()
    assert len(encoded) == info['compressed_bytes']
    assert hashlib.sha256(encoded).hexdigest() == info['compressed_sha256']
    decoded = gzip.decompress(encoded)
    assert len(decoded) == info['uncompressed_bytes']
    assert hashlib.sha256(decoded).hexdigest() == info['uncompressed_sha256']
    return json.loads(decoded)


def verify_cost_model(model, *, queries, calls, configs, comparisons):
    assert model['queries'] == len(model['all_queries']) == queries
    assert model['real_model_posts'] == sum(q['posts'] for q in model['all_queries']) == calls
    assert len(model['records']) == configs and model['measurements'] == 2 * configs
    for record in model['records']:
        assert len(record['samples']) == 2 and record['http']['count'] == 1024
        for name in ('full', 'submit_to_eof', 'preparation', 'source', 'first_row', 'tail_consume', 'http', 'request_e2e'):
            observed = record[name]
            recomputed = sample_distribution(observed['samples'], unit='seconds')
            assert all(observed[key] == value for key, value in recomputed.items())
            if observed['samples']:
                assert observed['mean'] == statistics.mean(observed['samples'])
                assert observed['median'] == statistics.median(observed['samples'])
        for observed in record['sema_stages'].values():
            assert all(observed[key] == value for key, value in sample_distribution(observed['samples'], unit='seconds').items())
        assert record['submit_to_eof']['p99_is_sample_maximum']
    assert len(model['request_comparisons']) == comparisons
    assert all(q['complete_request_values_multiset_equal'] and q['complete_request_bytes_multiset_equal'] for q in model['request_comparisons'])


def members(path, expected):
    assert hashlib.sha256(path.read_bytes()).hexdigest() == expected
    result = {}
    with gzip.open(path, 'rt', encoding='utf-8') as stream:
        for line in stream:
            item = json.loads(line)
            data = item['text'].encode()
            assert len(data) == item['bytes']
            assert hashlib.sha256(data).hexdigest() == item['sha256']
            assert item['member'] not in result
            result[item['member']] = item['text']
    return result


def replay_calls(values, prefixes=None):
    count = 0
    for name, text in values.items():
        if not name.endswith('/summary.json'):
            continue
        summary = json.loads(text)
        if summary.get('status') != 'passed' or not summary.get('call_timing', {}).get('calls'):
            continue
        prefix = name.removesuffix('summary.json')
        if prefixes is not None and prefix not in prefixes:
            continue
        calls = json.loads(values[prefix + 'calls.json'])
        events = [json.loads(line) for line in values[prefix + 'method-events.jsonl'].splitlines()]
        for member, body in values.items():
            if member.startswith(prefix + 'worker-events/') and member.endswith('.jsonl'):
                events.extend(json.loads(line) for line in body.splitlines())
        events = bind_native_http_events(events)
        assert summarize_calls(events, calls) == summary['call_timing']
        count += 1
    return count


verification = json.loads((ROOT / 'verification.json').read_text())
for identity in verification['analysis_storage']['files'].values():
    compressed = (ROOT / identity['path']).read_bytes()
    assert len(compressed) == identity['compressed_bytes']
    assert hashlib.sha256(compressed).hexdigest() == identity['compressed_sha256']
    original = gzip.decompress(compressed)
    assert len(original) == identity['original_bytes']
    assert hashlib.sha256(original).hexdigest() == identity['original_sha256']
analysis = json.loads(gzip.decompress((ROOT / 'model-analysis.json.gz').read_bytes()))
fixture = members(ROOT / verification['raw']['archive'], verification['raw']['sha256'])
model = members(ROOT / analysis['archive']['path'], analysis['archive']['sha256'])
assert replay_calls(fixture) == 8
assert replay_calls(model, {row['unit_member'] for row in analysis['queries']}) == 27
for arm, expected in analysis['query_distributions'].items():
    summaries = [json.loads(model[row['unit_member'] + 'summary.json'])
                 for row in analysis['queries'] if row['arm'] == arm]
    assert summarize_queries(summaries) == expected
assert len(analysis['queries']) == 39
assert sum(row['actual_posts'] for row in analysis['queries']) == 384
print(f'{len(fixture)} fixture members and {len(model)} model members verified; '
      '8 fixture and 27 model call timelines, 13 query distributions, 39 queries / 384 POST replayed')

if 'adapter_cost_diagnosis' in verification:
    storage = verification['adapter_cost_diagnosis']['storage']
    compressed = (ROOT / storage['path']).read_bytes()
    assert len(compressed) == storage['compressed_bytes']
    assert hashlib.sha256(compressed).hexdigest() == storage['compressed_sha256']
    raw = gzip.decompress(compressed)
    assert len(raw) == storage['uncompressed_bytes']
    assert hashlib.sha256(raw).hexdigest() == storage['uncompressed_sha256']
    diagnostic = json.loads(raw)
    runs = [diagnostic[name] for name in ('http_repair', 'cost_diagnosis', 'cost_followup')]
    assert all(run['status'] == 'passed' for run in runs)
    assert sum(run['queries'] for run in runs) == 80
    assert sum(run['real_model_posts'] for run in runs) == 34408
    records = diagnostic['cost_diagnosis']['records'] + diagnostic['cost_followup']['records']
    assert len(records) == 12
    for record in records:
        samples = record['samples']
        assert len(samples) == 3 and record['http']['count'] == 1536
        values = sorted(sample['full_seconds'] for sample in samples)
        assert record['full']['median'] == values[1]
        assert record['full']['p99'] == record['full']['p99_99'] == values[-1]
        assert record['http']['p99_99'] == record['http']['maximum']
    comparisons = diagnostic['cost_diagnosis']['body_comparisons'] + diagnostic['cost_followup']['body_comparisons']
    assert len(comparisons) == 27 and all(item['complete_http_body_multiset_equal'] for item in comparisons)
    engineering = diagnostic['engineering_audit']
    assert engineering['status'] == 'passed' and engineering['real_model_posts'] == 0
    assert sum(item['queries'] for item in engineering['records']) == 12
    assert sum(item['fixture_posts'] for item in engineering['records']) == 576
    assert all(item['local_backup_verified'] for item in diagnostic['preservation'].values())
    print('Compact observation hashes and all 36 cost samples verified; '
          '80 real queries / 34408 POST and 12 Arrow fixture queries / 576 POST; private originals retained')

if 'latest_short_comparison' in verification:
    stored = verification['latest_short_comparison']
    info = stored['storage']
    encoded = (ROOT / info['path']).read_bytes()
    assert len(encoded) == info['compressed_bytes']
    assert hashlib.sha256(encoded).hexdigest() == info['compressed_sha256']
    decoded = gzip.decompress(encoded)
    assert len(decoded) == info['uncompressed_bytes']
    assert hashlib.sha256(decoded).hexdigest() == info['uncompressed_sha256']
    current = json.loads(decoded)
    assert current['source_commit'] == stored['source_commit']
    assert len(current['records']) == 9 and len(current['all_queries']) == 63
    assert sum(query['posts'] for query in current['all_queries']) == 27720
    for record in current['records']:
        assert len(record['samples']) == 5 and record['http']['count'] == 2560
        for name in ('full', 'submit_to_eof', 'preparation', 'source', 'first_row', 'tail_consume', 'http', 'request_e2e'):
            observed = record[name]
            recomputed = sample_distribution(observed['samples'], unit='seconds')
            assert all(observed[key] == value for key, value in recomputed.items())
            if observed['samples']:
                assert observed['median'] == statistics.median(observed['samples'])
                assert observed['mean'] == statistics.mean(observed['samples'])
        for name, field in (('full', 'full_seconds'), ('submit_to_eof', 'ready_seconds'),
                            ('preparation', 'preparation_seconds'), ('source', 'source_seconds')):
            assert record[name]['samples'] == [query[field] for query in record['samples']]
        assert record['submit_to_eof']['p99_is_sample_maximum']
        assert record['full']['p99_is_sample_maximum']
        assert record['http']['p99_99_is_sample_maximum']
        assert record['request_e2e']['count'] in (0, 2560)
    pairs = current['request_comparisons']
    assert len(pairs) == 30 and all(pair['complete_request_values_multiset_equal'] for pair in pairs)
    assert all(pair['complete_request_bytes_multiset_equal'] for pair in pairs if pair['supplier'] != 'LOTUS')
    assert all(original['local_backup_verified'] for original in current['preservation'].values())
    print('Latest comparison: 63 queries / 27720 real calls, all 45 query measurements, '
          '23040 HTTP samples and 30 same-method request comparisons verified')

if 'execution_cost_repair' in verification:
    current = stored_json(verification['execution_cost_repair']['storage'])
    model = current['model']
    verify_cost_model(model, queries=68, calls=26248, configs=17, comparisons=30)
    cpu = current['cpu_transport']
    assert cpu['real_model_posts'] == 0 and cpu['fixture_posts'] == 13376 and len(cpu['records']) == 8
    core = current['core_cpu']
    assert not core['cprofile_enabled'] and len(core['attempts']) == 10
    for mode, summary in core['summary'].items():
        attempts = [a for a in core['attempts'] if a['mode'] == mode]
        assert len(attempts) == 5
        for field, aggregate in (('query_seconds', 'query_median'), ('offer_seconds', 'offer_median')):
            assert summary[field] == [a[field] for a in attempts]
            assert statistics.median(summary[field]) == summary[aggregate]
    diagnostic = current['sema_cpu']
    assert len(diagnostic['units']) == 24 and diagnostic['gpu_calls'] == diagnostic['model_calls'] == 0
    for variant, summary in diagnostic['summaries'].items():
        units = [u for u in diagnostic['units'] if u['variant'] == variant]
        assert len(units) == 6 and all(u['rows'] == len(u['spans']) == 64 for u in units)
        for unit in units:
            assert not any(unit['last_observed_core_usage'].values())
            assert unit['peak_observed_core_usage']['held_tasks'] <= 16
            assert unit['peak_observed_core_usage']['active_requests'] <= 8
        for name, field in (('query', None), ('transport_to_terminal', 'transport_to_terminal_ns'),
                            ('terminal_to_write', 'terminal_to_write_ns')):
            values = ([u['query_elapsed_ns'] for u in units] if field is None
                      else [span[field] for u in units for span in u['spans']])
            expected = summary[name]
            assert expected['n'] == len(values)
            assert expected['median_ms'] == statistics.median(values) / 1e6
            assert expected['max_ms'] == max(values) / 1e6
            rank = (len(values) * 99 + 99) // 100
            assert expected['p99_ms'] == sorted(values)[rank - 1] / 1e6
    if 'completion_progress' in current:
        followup = current['completion_progress']
        verify_cost_model(followup['model'], queries=32, calls=12352, configs=8, comparisons=12)
        assert followup['same_input_model_signature']
        assert followup['service_count'] == dict(success_delta=12352, running=0, waiting=0)
        before = followup['core_repro']['repro-before']
        after = followup['core_repro']['repro-after']
        assert before['delivered'] == 8 and before['remaining_backend_events'] == 24
        assert not before['immediate'] and before['progress_generation'] == before['wake_generation_before_wait']
        assert [tick['remaining_backend_events'] for tick in after['ticks']] == [24, 16, 8, 0]
        assert all(tick['immediate'] and tick['delivered'] == 8 for tick in after['ticks'])
        assert not after['empty_next_tick']['immediate']
        print('Completion progress: 32 queries / 12352 real calls, 16 measurements, '
              '8192 HTTP samples and 12 original-adapter request comparisons verified')
    if 'observation_repair' in current:
        followup = current['observation_repair']
        verify_cost_model(followup['model'], queries=8, calls=3088, configs=2, comparisons=2)
        assert followup['same_input_model_signature']
        assert followup['service_count'] == dict(success_delta=3088, running=0, waiting=0)
        assert len(followup['matching_method_comparisons']) == 4
        assert all(pair['complete_request_bytes_multiset_equal'] and pair['complete_request_values_multiset_equal']
                   for pair in followup['matching_method_comparisons'])
        assert followup['cleanup']['status'] == 'passed' and not followup['cleanup']['cleanup_errors']
        assert not followup['cleanup']['remaining_gpu_compute']
        print('Deferred observation repair: 8 queries / 3088 real calls, 4 measurements, '
              '2048 HTTP samples and 4 previous-source request comparisons verified')
    assert current['prelaunch_failure']['model_posts'] == 0
    assert all(info['local_backup_verified'] for info in current['preservation'].values())
    print('Execution costs: 68 queries / 26248 real calls, 34 measurements, '
          '17408 HTTP samples, 30 request comparisons and the zero-call startup failure verified')
