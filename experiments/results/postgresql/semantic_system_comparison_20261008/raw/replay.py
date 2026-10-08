"""Check retained hashes, all classifications, request counts and original values.

Uses only the Python standard library and never opens a model or database.
"""
from collections import Counter
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
read = lambda name: json.loads((root / name).read_text())
lines = lambda name: [json.loads(s) for s in (root / name).read_text().splitlines()]

for name, expected in read('storage-public.json').items():
    data = (root / name).read_bytes()
    assert len(data) == expected['bytes']
    assert hashlib.sha256(data).hexdigest() == expected['sha256'], name

summary, units, ledger = read('summary.json'), read('units.json'), read('ledger.json')
assert summary['real_requests'] == 32 == len(ledger['charged_requests'])
assert ledger['budget']['allocated_requests'] == 32
assert summary['model_counter']['vllm:request_success_total'] == 32
assert summary['model_counter']['vllm:num_requests_running'] == 0
assert summary['model_counter']['vllm:num_requests_waiting'] == 0
identity_sets = []
for unit in units:
    outputs = lines(unit['role'] + '-outputs.jsonl')
    identities = {r['occurrence_sha256'] for r in outputs}
    assert len(outputs) == len(identities) == 16
    identity_sets.append(identities)
    counts = dict(true_positive=0, false_positive=0, true_negative=0, false_negative=0, invalid=0)
    for row in outputs:
        label, prediction = row['reference'], row['prediction']
        assert label in ('POSITIVE', 'NEGATIVE')
        if prediction not in ('POSITIVE', 'NEGATIVE'):
            counts['invalid'] += 1
        elif prediction == 'POSITIVE':
            counts['true_positive' if label == 'POSITIVE' else 'false_positive'] += 1
        else:
            counts['true_negative' if label == 'NEGATIVE' else 'false_negative'] += 1
    assert all(unit['quality'][name] == value for name, value in counts.items())
    execution = unit['execution']
    actual = (execution['t_query_terminal_ns'] - unit['query_preparation_started_ns']) / 1e9
    assert actual == unit['full_query_seconds']
assert identity_sets[0] == identity_sets[1]

trace, protocol = lines('sema-http-trace.jsonl'), lines('sema-protocols.jsonl')
assert len(trace) == len(protocol) == 16
assert all(r['status'] == 'completed' and r['retry_count'] == 0 and
           r['request_body_sha256'] == r['forwarded_body_sha256'] for r in trace)
assert all(r['http_status'] == 200 and r['response']['choices'][0]['finish_reason'] == 'stop' for r in protocol)
parsed = [json.loads(r['response']['choices'][0]['message']['content']) for r in protocol]
assert Counter(parsed) == Counter(r['prediction'] for r in lines('sema-map-outputs.jsonl'))
for field, metric in (('prompt_tokens', 'vllm:prompt_tokens_total'), ('output_tokens', 'vllm:generation_tokens_total')):
    pg = sum(r[field] for r in lines('pg-events.jsonl') if r['event'] == 'core_map_completion')
    sema_field = 'completion_tokens' if field == 'output_tokens' else field
    native = sum(r['response']['usage'][sema_field] for r in protocol)
    assert pg + native == summary['model_counter'][metric]
assert len([r for r in lines('pg-events.jsonl') if r['event'] == 'request']) == 16
assert read('offline-association-audit.json')['sema_associations'] == 16
print(json.dumps(dict(status='passed',real_requests=32,model_requests_from_replay=0,
    pg_legal=16,sema_invalid=16,hashes_checked=len(read('storage-public.json')))))
