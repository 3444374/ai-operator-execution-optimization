"""Replay the complete partial campaign and preserve its failed warmup."""
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
def read(name):
    return json.loads((root/name).read_text())
def lines(name):
    file = root/name
    if file.exists():
        text = file.read_text()
    else:
        text = gzip.decompress((root/(name+'.gz')).read_bytes()).decode()
    return [json.loads(s) for s in text.splitlines()]

for name, expected in read('storage-public.json').items():
    data = (root/name).read_bytes()
    assert len(data) == expected['bytes']
    assert hashlib.sha256(data).hexdigest() == expected['sha256'], name
for p in root.glob('*.identity.json'):
    value = json.loads(p.read_text())
    data = gzip.decompress((root/value['gzip_file']).read_bytes())
    assert len(data) == value['uncompressed_bytes']
    assert hashlib.sha256(data).hexdigest() == value['uncompressed_sha256']

summary, records, ledger = read('summary.json'), read('records.json'), read('ledger.json')
assert summary['status'] == 'failed'
assert summary['actual_posts'] == len(ledger['charged_requests']) == 8009
assert ledger['budget']['allocated_requests'] == 8272
assert summary['model_counter']['vllm:request_success_total'] == 8009
assert summary['model_counter']['vllm:num_requests_running'] == 0
assert summary['model_counter']['vllm:num_requests_waiting'] == 0
assert len(records) == 55
outputs = defaultdict(list)
for row in lines('outputs.jsonl'):
    outputs[row['unit_id']].append(row)
for record in records:
    rows = outputs[record['unit_id']]
    assert len(rows) == len({r['occurrence_sha256'] for r in rows}) == record['expected_rows']
    counts = dict(true_positive=0,false_positive=0,true_negative=0,false_negative=0,invalid=0)
    for r in rows:
        prediction, reference = r['prediction'], r['reference']
        if prediction not in ('POSITIVE','NEGATIVE'):
            counts['invalid'] += 1
        elif prediction == 'POSITIVE':
            counts['true_positive' if reference == 'POSITIVE' else 'false_positive'] += 1
        else:
            counts['true_negative' if reference == 'NEGATIVE' else 'false_negative'] += 1
    assert all(record['quality'][k] == v for k,v in counts.items())
    assert counts['invalid'] == 0

diagnostic = read('protocol-diagnosis.json')
assert diagnostic['actual_posts'] == 16
assert diagnostic['results'][0]['quality']['invalid'] == 8
assert diagnostic['results'][1]['quality']['invalid'] == 0
failed = read('failed-unit.json')
assert failed['summary']['actual_posts'] == 249
assert failed['summary']['error']['type'] == 'OSError'
assert 'Too many open files' in failed['summary']['error']['message']
assert sum(r['actual_posts'] for r in records) + 16 + 249 == 8009
assert read('method-supply-decision.json')['decision']['status'] == 'inconclusive_supply'
assert read('file-capacity-fixture.json')['fixture_posts'] == 512
assert read('file-capacity-fixture.json')['real_model_requests'] == 0
assert read('independent-cleanup.json')['remaining_owned_processes'] == []
assert read('independent-audit.json')['status'] == 'passed'
print(json.dumps(dict(status='passed',real_requests=8009,accepted_queries=55,
    failed_warmup_requests=249,new_model_requests=0)))
