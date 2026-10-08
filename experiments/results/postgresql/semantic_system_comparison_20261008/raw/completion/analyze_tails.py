"""Describe measured batch-row, HTTP and whole-query tails; never infer rare-tail qualification."""
from collections import defaultdict
import gzip,hashlib,json,math
from pathlib import Path

root=Path(__file__).resolve().parent
ROLES=('pg-map','lotus-map','duckdb-ai','daft-prompt','sema-map','pg-request','pg-token-wide','pg-work-limited')
def read(name):return json.loads((root/name).read_text())
def lines(name):
    p=root/name
    data=p.read_bytes() if p.exists() else gzip.decompress((root/(name+'.gz')).read_bytes())
    return [json.loads(s) for s in data.decode().splitlines()]
def describe(values,query_units):
    if not values or any(not math.isfinite(v) or v<0 for v in values):raise ValueError('missing or invalid measured intervals')
    ordered=sorted(values);n=len(ordered)
    ranks={str(p):math.ceil(n*p/100) for p in (50,99,99.99)}
    return dict(samples=n,query_units=query_units,p50_s=ordered[ranks['50']-1],p99_s=ordered[ranks['99']-1],
        empirical_p9999_s=ordered[ranks['99.99']-1],maximum_s=ordered[-1],order_statistic_ranks=ranks,
        observations_above_p99_rank=n-ranks['99'],observations_above_p9999_rank=n-ranks['99.99'],
        p99_status='descriptive_batch_sample' if n>=100 else 'insufficient_tail_samples',
        p9999_status='insufficient_tail_samples_and_independent_query_units',population_p9999_s=None)
records=read('records.json');timings={v['unit_id']:v for v in read('timings.json')}
rows,events=defaultdict(list),defaultdict(list)
for v in lines('row-timing.jsonl'):rows[v['unit_id']].append(v)
for v in lines('events.jsonl'):events[v['unit_id']].append(v)
provenance=read('row-timing-provenance.json');result=[]
for role in ROLES:
    chosen=sorted((r for r in records if r['role']==role and ((r['stage']=='evaluation' and r['repeat']>0) or r['stage']=='method-qualification')),key=lambda r:r['repeat'])
    if len(chosen)!=(3 if role.endswith('-map') or role in ('duckdb-ai','daft-prompt') else 1):raise ValueError('unexpected measured query count')
    all_rows=[];all_http=[];full_queries=[];released_queries=[];units=[]
    for r in chosen:
        identity=r['unit_id'];t=timings[identity];execution=t['execution'];values=rows[identity]
        assert len(values)==len({v['occurrence_sha256'] for v in values})==r['expected_rows']
        assert provenance['units'][identity]['source_results_sha256']==execution['results_sha256']
        start=t['query_preparation_started_ns'];eof=execution['t_query_terminal_ns']
        assert all(start<=v['received_ns']<=eof for v in values)
        assert max(v['received_ns'] for v in values)==execution['t_last_row_ns']
        latencies=[(v['received_ns']-start)/1e9 for v in values]
        if role.startswith('pg-'):
            intervals={kind:{} for kind in ('core_http_started','core_http_finished')}
            for e in events[identity]:
                if e.get('event') not in intervals:continue
                key=(e['key']['session_id'],e['key']['sequence'])
                assert key not in intervals[e['event']]
                intervals[e['event']][key]=e['monotonic_ns']
            begins,ends=(intervals[k] for k in intervals)
            assert set(begins)==set(ends) and len(begins)==r['actual_posts']
            http=[(ends[k]-begins[k])/1e9 for k in begins]
            http_scope='Core HTTP begin to finish; client transport observation, excludes earlier preparation and later row delivery'
        else:
            trace=events[identity];assert len(trace)==r['actual_posts'] and all(e['status']=='completed' for e in trace)
            http=[e['response_completed_epoch_s']-e['received_epoch_s'] for e in trace]
            http_scope='gateway receipt to response prepared, including durable callbacks; excludes native client queue before receipt and client consumption after return'
        full=(eof-start)/1e9;released=(eof-execution['t_release_ns'])/1e9
        assert full==r['full_query_seconds']
        all_rows.extend(latencies);all_http.extend(http);full_queries.append(full);released_queries.append(released)
        units.append(dict(unit_id=identity,repeat=r['repeat'],stage=r['stage'],row_e2e=describe(latencies,1),
            http_observed=describe(http,1),full_query_s=full,release_to_eof_s=released))
    result.append(dict(role=role,point=chosen[0]['point'],active_work=chosen[0]['active_work'],
        scope='three 512-row measured bulk queries' if len(chosen)==3 else 'single eight-row engineering query',
        http_scope=http_scope,row_e2e=describe(all_rows,len(chosen)),http_observed=describe(all_http,len(chosen)),
        full_query=describe(full_queries,len(chosen)),release_to_eof_query=describe(released_queries,len(chosen)),units=units))
q3=[]
for role in ('pg-q3','lotus-q3'):
    measured=sorted((r for r in records if r['role']==role and r['stage']=='q3-evaluation' and r['repeat']>0),key=lambda r:r['repeat'])
    assert len(measured)==3
    q3.append(dict(role=role,task='original Movie Q3, 120 decisions and one COUNT result',full_query=describe([r['full_query_seconds'] for r in measured],3)))
value=dict(schema='semloom.semantic_tail_descriptive.v1',q3_queries=q3,new_model_requests=0,rows=result,
    percentile_method='nearest rank: sorted_values[ceil(p/100*n)-1]; no interpolation',
    row_e2e_definition='(received_ns-query_preparation_started_ns)/1e9; common query origin for this fixed full-scan batch',
    full_query_definition='(t_query_terminal_ns-query_preparation_started_ns)/1e9',
    release_to_eof_definition='(t_query_terminal_ns-t_release_ns)/1e9',
    external_per_request_arrival_clock=dict(status='unavailable',reason='original run recorded query origin and row receipts, not independent request arrival times'),
    interpretation='batch-row delivery includes query preparation and submission/consumption waits; HTTP phases have different observation points; rows sharing one query are correlated',
    population_p9999_qualification='unavailable; insufficient rare-tail samples and independent queries',
    inputs={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ('records.json','timings.json','events.jsonl.gz','row-timing.jsonl.gz','row-timing-provenance.json')})
(root/'tail-analysis.json').write_text(json.dumps(value,sort_keys=True,indent=2)+'\n')
for r in result:print(json.dumps({k:r[k] for k in ('role','scope','row_e2e','http_observed','full_query')}))
