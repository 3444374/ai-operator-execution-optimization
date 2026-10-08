"""Recheck retained complete query evidence without sending a model request."""
from collections import Counter
import gzip,hashlib,itertools,json
from pathlib import Path
root=Path(__file__).resolve().parent
read=lambda name:json.loads((root/name).read_text())
def lines(name):
    with gzip.open(root/name,'rt') as f:
        for line in f:yield json.loads(line)
def groups(name):return itertools.groupby(lines(name),lambda v:v['unit_id'])
for name,expected in read('storage-public.json').items():
    p=root/name;h=hashlib.sha256()
    with p.open('rb') as f:
        for data in iter(lambda:f.read(1048576),b''):h.update(data)
    assert p.stat().st_size==expected['bytes'] and h.hexdigest()==expected['sha256'],name
for p in root.glob('*.identity.json'):
    v=json.loads(p.read_text());h=hashlib.sha256();n=0
    with gzip.open(root/v['gzip_file'],'rb') as f:
        for data in iter(lambda:f.read(1048576),b''):h.update(data);n+=len(data)
    assert n==v['uncompressed_bytes'] and h.hexdigest()==v['uncompressed_sha256']
summary=read('summary.json');records=read('records.json');records={r['unit_id']:r for r in records}
failures={r['unit_id']:r for r in read('failures.json')['units']};failure_charges={}
timings={t['unit_id']:t for t in read('timings.json')}
event_groups=iter(groups('events.jsonl.gz'));charge_groups=iter(groups('ledger-charges.jsonl.gz'))
total=0;verified=[]
for identity,group in groups('outputs.jsonl.gz'):
    rows=list(group);event_id,event_group=next(event_groups);events=list(event_group)
    charge_id,charge_group=next(charge_groups);charges=list(charge_group)
    while charge_id!=identity:
        assert charge_id in failures and charge_id not in records and charge_id not in failure_charges
        failure_charges[charge_id]=charges
        charge_id,charge_group=next(charge_groups);charges=list(charge_group)
    assert identity==event_id==charge_id
    r=records[identity];assert len(rows)==len({v['occurrence_sha256'] for v in rows})==r['expected_rows']==r['actual_posts']
    assert len(charges)==len({c['sequence'] for c in charges})==r['actual_posts']
    counts=Counter()
    for row in rows:
        pred,ref=row['prediction'],row['reference']
        assert pred in ('POSITIVE','NEGATIVE') and ref in ('POSITIVE','NEGATIVE')
        counts[('true_' if pred==ref else 'false_')+('positive' if pred=='POSITIVE' else 'negative')]+=1
    assert all(r['quality'][k]==counts[k] for k in ('true_positive','false_positive','true_negative','false_negative'))
    assert r['quality']['invalid']==r['quality']['missing_rows']==0
    if r['role'].startswith('pg-'):
        hashes=[e['request_bytes_sha256'] for e in events if e.get('event')=='request']
        assert {v['pg_sequence'] for v in rows}==set(range(r['expected_rows']))
    else:
        hashes=[e['request_body_sha256'] for e in events]
        assert all(e['status']=='completed' and e['forwarded'] and e['retry_count']==0 and e['upstream_status']==200 and e['request_body_sha256']==e['forwarded_body_sha256'] for e in events)
    assert len(hashes)==r['actual_posts'] and Counter(hashes)==Counter(c['request_sha256'] for c in charges)
    t=timings[identity];e=t['execution']
    assert (e['t_query_terminal_ns']-t['query_preparation_started_ns'])/1e9==r['full_query_seconds']
    total+=len(charges);verified.append(identity)
try:next(event_groups);raise AssertionError('extra complete event unit')
except StopIteration:pass
for charge_id,charge_group in charge_groups:
    assert charge_id in failures and charge_id not in records and charge_id not in failure_charges
    failure_charges[charge_id]=list(charge_group)
assert set(failure_charges)==set(failures)
partial_rows={identity:list(group) for identity,group in groups('failed-partial-outputs.jsonl.gz')}
for identity,failure in failures.items():
    assert len(failure_charges[identity])==failure['attempted_requests']
    assert len(partial_rows[identity])==failure['received_rows']
    assert all(r['partial_results_are_provisional'] for r in partial_rows[identity])
assert set(verified)==set(records)
assert total==summary['accepted_posts']<=summary['actual_posts']<=823424
assert read('ledger.json')['charged_count']==summary['actual_posts']==total+sum(len(v) for v in failure_charges.values())
assert summary['model_counter']['vllm:request_success_total']==sum(r['model_counter_delta']['vllm:request_success_total'] for r in records.values())+sum(r['model_counter_delta']['vllm:request_success_total'] for r in failures.values())
assert summary['model_counter']['vllm:num_requests_running']==summary['model_counter']['vllm:num_requests_waiting']==0
for name in ('vllm:prompt_tokens_total','vllm:generation_tokens_total'):
    assert sum(r['model_counter_delta'][name] for r in records.values())+sum(r['model_counter_delta'][name] for r in failures.values())==summary['model_counter'][name]
assert summary['cleanup']['acl_restored'] and summary['cleanup']['pg_shutdown']
assert read('independent-cleanup.json')['remaining_owned_processes']==[]
fixture=read('method-scale-fixture.json');assert fixture['status']=='passed' and fixture['fixture_posts']==1536 and fixture['real_model_requests']==0
analysis=read('tail-analysis.json')
for row in analysis['rows']:
    measured=sorted((r for r in records.values() if r['role']==row['role'] and r['stage']=='tail-evaluation'),key=lambda r:r['repeat'])
    values=sorted(r['full_query_seconds'] for r in measured);n=len(values)
    assert n==row['complete_queries'] and n*512==row['measured_rows']
    if not n:continue
    rank99=(99*n+99)//100;rank9999=(9999*n+9999)//10000
    d=row['distributions']['full_query']
    assert d['p99_s']==values[rank99-1] and d['empirical_p9999_s']==values[rank9999-1]
    assert d['population_p9999_s'] is None
if summary['status']=='passed':assert len(records)==1616 and total==823424 and all(r['complete_queries']==200 for r in analysis['rows'])
print(json.dumps(dict(status='passed',campaign_status=summary['status'],complete_queries=len(records),recorded_request_attempts=summary['actual_posts'],service_success=summary['model_counter']['vllm:request_success_total'],failed_queries=len(failures),measured_queries_by_role={r['role']:r['complete_queries'] for r in analysis['rows']},new_model_requests=0)))
