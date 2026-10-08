"""Replay occurrence, quality, actual POST, time and cleanup evidence without a model."""
from collections import Counter,defaultdict
import gzip,hashlib,json
from statistics import median,stdev
from pathlib import Path
root=Path(__file__).resolve().parent
def read(name):return json.loads((root/name).read_text())
def lines(name):
    p=root/name
    data=p.read_bytes() if p.exists() else gzip.decompress((root/(name+'.gz')).read_bytes())
    return [json.loads(s) for s in data.decode().splitlines()]
for name,expected in read('storage-public.json').items():
    data=(root/name).read_bytes()
    assert len(data)==expected['bytes'] and hashlib.sha256(data).hexdigest()==expected['sha256'],name
for p in root.glob('*.identity.json'):
    v=json.loads(p.read_text());data=gzip.decompress((root/v['gzip_file']).read_bytes())
    assert len(data)==v['uncompressed_bytes'] and hashlib.sha256(data).hexdigest()==v['uncompressed_sha256']
summary,records,ledger=read('summary.json'),read('records.json'),read('ledger.json')
assert summary['status']=='passed'
assert len(records)==62
assert summary['actual_posts']==len(ledger['charged_requests'])==sum(r['actual_posts'] for r in records)==14408
assert ledger['budget']['allocated_requests']==14408
assert summary['model_counter']['vllm:request_success_total']==14408
assert summary['model_counter']['vllm:num_requests_running']==summary['model_counter']['vllm:num_requests_waiting']==0
for key in ('vllm:request_success_total','vllm:prompt_tokens_total','vllm:generation_tokens_total'):
    assert sum(r['model_counter_delta'][key] for r in records)==summary['model_counter'][key]
outputs,events,charges=defaultdict(list),defaultdict(list),defaultdict(list)
for r in lines('outputs.jsonl'):outputs[r['unit_id']].append(r)
for r in lines('events.jsonl'):events[r['unit_id']].append(r)
for r in ledger['charged_requests']:charges[r['unit_id']].append(r['request_sha256'])
timings={r['unit_id']:r for r in read('timings.json')}
for record in records:
    identity=record['unit_id'];rows=outputs[identity];observed=events[identity]
    assert len(rows)==len({r['occurrence_sha256'] for r in rows})==record['expected_rows']
    counts=dict(true_positive=0,false_positive=0,true_negative=0,false_negative=0,invalid=0)
    for r in rows:
        value,ref=r['prediction'],r['reference']
        if value not in ('POSITIVE','NEGATIVE'):counts['invalid']+=1
        elif value=='POSITIVE':counts['true_positive' if ref=='POSITIVE' else 'false_positive']+=1
        else:counts['true_negative' if ref=='NEGATIVE' else 'false_negative']+=1
    assert all(record['quality'][k]==v for k,v in counts.items()) and counts['invalid']==0
    assert record['quality']['missing_rows']==0
    if 'q3' in record['stage']:
        assert record['count_result']==sum(r['prediction']=='POSITIVE' for r in rows)
        gold=sum(r['reference']=='POSITIVE' for r in rows)
        metric=record['original_query_quality']['original_metric']
        assert metric['absolute_error']==abs(record['count_result']-gold)
    if record['role'] in ('lotus-map','daft-prompt','duckdb-ai','sema-map'):
        hashes=[e['request_body_sha256'] for e in observed]
        assert len(hashes)==record['actual_posts']
        assert all(e['status']=='completed' and e['forwarded'] and e['retry_count']==0 and e['upstream_status']==200 and e['request_body_sha256']==e['forwarded_body_sha256'] for e in observed)
    else:hashes=[e['request_bytes_sha256'] for e in observed if e.get('event')=='request']
    assert Counter(hashes)==Counter(charges[identity]),identity
    timing=timings[identity];execution=timing['execution']
    full=(execution['t_query_terminal_ns']-timing['query_preparation_started_ns'])/1e9
    assert abs(full-record['full_query_seconds'])<1e-9
analysis=read('analysis.json')
for row in analysis['rows']:
    measured=sorted((r for r in records if r['role']==row['role'] and r['stage'] in ('evaluation','q3-evaluation') and r['repeat']>0),key=lambda r:r['repeat'])
    values=[r['full_query_seconds'] for r in measured]
    assert len(values)==3 and values==row['measured_seconds'] and median(values)==row['median_seconds']
    assert row['sample_cv']==stdev(values)/(sum(values)/3)
    assert row['accuracy_percent']==[100*(r['quality']['true_positive']+r['quality']['true_negative'])/r['expected_rows'] for r in measured]
assert summary['driver_soft_nofile']==8192 and summary['local_model_cost_map'] is True
assert read('map-scale-fixture.json')['fixture_posts']==2560
assert read('q3-fixture.json')['fixture_posts']==32
assert read('map-scale-fixture.json')['real_model_requests']==read('q3-fixture.json')['real_model_requests']==0
assert read('method-supply-decision.json')['decision']['status']=='inconclusive_supply'
assert summary['method_status']=='conditional_stages_not_started:inconclusive_supply'
assert read('independent-audit.json')['status']=='passed'
assert read('independent-cleanup.json')['remaining_owned_processes']==[]
assert summary['cleanup']['acl_restored'] is True and summary['cleanup']['pg_shutdown'] is True
tail=read('tail-analysis.json')
assert tail['new_model_requests']==0 and len(tail['rows'])==8
for name,digest in tail['inputs'].items():
    assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest
row_times=defaultdict(list)
for v in lines('row-timing.jsonl'):row_times[v['unit_id']].append(v)
provenance=read('row-timing-provenance.json')
assert provenance['exported_rows']==sum(map(len,row_times.values()))==7704
assert provenance['source_private_archive_sha256']==read('backup-verification.json')['archive_sha256']
assert len(provenance['units'])==18
for entry in tail['rows']:
    measured=[];whole=[]
    for unit in entry['units']:
        identity=unit['unit_id'];timing=timings[identity]
        values=row_times[identity]
        assert {v['occurrence_sha256'] for v in values}=={v['occurrence_sha256'] for v in outputs[identity]}
        assert provenance['units'][identity]['source_results_sha256']==timing['execution']['results_sha256']
        start=timing['query_preparation_started_ns'];eof=timing['execution']['t_query_terminal_ns']
        assert all(start<=v['received_ns']<=eof for v in values)
        measured.extend((v['received_ns']-start)/1e9 for v in values)
        whole.append((eof-start)/1e9)
    for name,values in (('row_e2e',measured),('full_query',whole)):
        stats=entry[name];ordered=sorted(values)
        assert stats['samples']==len(values) and stats['query_units']==len(entry['units'])
        for percentage,field in ((99,'p99_s'),(99.99,'empirical_p9999_s')):
            import math
            rank=math.ceil(len(values)*percentage/100)
            assert stats['order_statistic_ranks'][str(percentage)]==rank and stats[field]==ordered[rank-1]
        assert stats['population_p9999_s'] is None
        assert stats['empirical_p9999_s']==max(values)
assert len(tail['q3_queries'])==2 and all(v['full_query']['samples']==3 for v in tail['q3_queries'])
print(json.dumps(dict(status='passed',real_requests=14408,accepted_queries=62,new_model_requests=0)))
