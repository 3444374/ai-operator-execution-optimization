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
