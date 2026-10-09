"""Verify retained adapter observations without a database, model, or SDK run."""
import gzip
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parents[3] / 'code'))
from src.experiments.postgresql.native_adapter_http import bind_native_http_events
from src.experiments.postgresql.native_adapter_metrics import summarize_calls, summarize_queries


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
analysis = json.loads((ROOT / 'model-analysis.json').read_text())
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
