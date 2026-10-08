"""Offline replay of prepared-query clocks, HTTP scopes and complete samples."""
from collections import Counter,defaultdict
from pathlib import Path
import argparse,gzip,hashlib,json,math,statistics

parser=argparse.ArgumentParser();parser.add_argument('root',nargs='?',type=Path,default=Path(__file__).resolve().parent)
root=parser.parse_args().root
read=lambda name:json.loads((root/name).read_text())
def lines(name):
    with gzip.open(root/name,'rt') as f:
        for line in f:yield json.loads(line)
def distribution(values):
    values=sorted(values);n=len(values)
    return dict(n=n,min=values[0],median=statistics.median(values),max=values[-1],
        p99=values[math.ceil(.99*n)-1],p9999=values[math.ceil(.9999*n)-1],
        p99_rank=math.ceil(.99*n),p9999_rank=math.ceil(.9999*n)) if n else dict(n=0)

owner=read('owner.json');records=read('records.json');timings=read('timings.json');provenance=read('row-provenance.json');ledger=read('ledger.json')
# Derive identity from role/stage/repeat, not the order in a JSON object.
by_id={f"{r['stage']}.{r['role']}.r{r['repeat']}":r for r in records}
assert set(by_id)==set(timings)==set(provenance)
rows=defaultdict(list);http=defaultdict(list);charges=defaultdict(list)
for v in lines('rows.jsonl.gz'):rows[v['unit_id']].append(v)
for v in lines('http.jsonl.gz'):http[v['unit_id']].append(v)
for v in lines('charges.jsonl.gz'):charges[v['unit_id']].append(v)
groups=defaultdict(lambda:defaultdict(list));unit_results=[];all_prompt=all_output=0
for identity,r in by_id.items():
    t=timings[identity];e=t['execution'];ready=t['ready_timing'];submit=ready['t_submit_ns'];end=e['t_query_terminal_ns'];start=t['query_preparation_started_ns']
    assert e['status']=='completed' and t['preparation_model_posts']==0
    assert start<=ready['t_backend_ready_ns']<=submit<=e['t_first_row_ns']<=e['t_last_row_ns']<=end
    assert e['recorded_rows']==r['rows']==len(rows[identity])==len(http[identity])==len(charges[identity])
    assert len({x['row_id'] for x in rows[identity]})==r['rows']
    assert min(x['received_ns'] for x in rows[identity])==e['t_first_row_ns']
    assert max(x['received_ns'] for x in rows[identity])==e['t_last_row_ns']
    assert all(x['output'] in ('POSITIVE','NEGATIVE') and submit<=x['received_ns']<=end for x in rows[identity])
    assert [x['sequence'] for x in charges[identity]]==list(range(1,r['rows']+1))
    assert Counter(x['request_sha256'] for x in charges[identity])==Counter(x['request_body_sha256'] for x in http[identity])
    full=(end-start)/1e9;query=(end-submit)/1e9;prep=(ready['t_backend_ready_ns']-start)/1e9
    assert full==r['full_query_seconds']==t['full_query_seconds'] and query==r['ready_query_seconds']==ready['ready_query_seconds']
    source_setup_to_submit=(submit-ready['t_backend_ready_ns'])/1e9
    values=dict(full_query=[full],ready_query=[query],preparation=[prep],recorder_before_submit=[source_setup_to_submit],
                first_row_ready=[(e['t_first_row_ns']-submit)/1e9],row_delivery_ready=[(x['received_ns']-submit)/1e9 for x in rows[identity]],
                row_delivery_application=[(x['received_ns']-start)/1e9 for x in rows[identity]])
    for metric in ('proxy_request','upstream_request','proxy_before_forward','proxy_after_forward'):values[metric]=[]
    active=peak=0;points=[]
    for x in http[identity]:
        keys=('received_monotonic_ns','request_body_read_completed_monotonic_ns','before_forward_started_monotonic_ns','before_forward_completed_monotonic_ns',
            'upstream_dispatch_started_monotonic_ns','upstream_response_body_read_completed_monotonic_ns','upstream_attempt_finished_monotonic_ns',
            'after_forward_started_monotonic_ns','after_forward_completed_monotonic_ns','response_ready_monotonic_ns','response_write_started_monotonic_ns','response_write_completed_monotonic_ns')
        moments=[x[k] for k in keys]
        assert x['status']=='completed' and x['response_write_status']=='completed' and x['retry_count']==0
        assert x['upstream_response_status']==200 and x['upstream_status']==200
        assert all(type(v) is int for v in moments) and moments==sorted(moments) and submit<=moments[0]
        assert x['request_body_sha256']==x['forwarded_body_sha256'] and x['forwarded']
        values['proxy_request'].append((x['response_write_completed_monotonic_ns']-x['received_monotonic_ns'])/1e9)
        values['upstream_request'].append((x['upstream_response_body_read_completed_monotonic_ns']-x['upstream_dispatch_started_monotonic_ns'])/1e9)
        values['proxy_before_forward'].append((x['before_forward_completed_monotonic_ns']-x['before_forward_started_monotonic_ns'])/1e9)
        values['proxy_after_forward'].append((x['after_forward_completed_monotonic_ns']-x['after_forward_started_monotonic_ns'])/1e9)
        points.extend(((x['upstream_dispatch_started_monotonic_ns'],1),(x['upstream_response_body_read_completed_monotonic_ns'],-1)))
        all_prompt+=x['actual_prompt_tokens'];all_output+=x['actual_output_tokens']
    for _,change in sorted(points):
        active+=change;assert active>=0;peak=max(peak,active)
    assert active==0 and peak==r['observed_peak_http']
    unit_results.append(dict(unit_id=identity,role=r['role'],stage=r['stage'],repeat=r['repeat'],query_samples=1,row_samples=r['rows'],request_samples=len(http[identity]),
        observed_peak_http=peak,quality=r['quality'],metrics={k:distribution(v) for k,v in values.items()}))
    if r['stage']=='evaluation':
        for name,value in values.items():groups[r['role']][name].extend(value)
        q=r['quality']
        groups[r['role']]['accuracy'].append(100*(q['true_positive']+q['true_negative'])/q['evaluated_rows'])
        groups[r['role']]['http_peak'].append(peak)
assert sum(len(v) for v in charges.values())==ledger['request_attempts']
if owner['status']=='passed':
    assert len(records)==104 and len(ledger['closed_units'])==104 and owner['request_attempts']==ledger['request_attempts']==46208
    assert all(len(groups[role]['ready_query'])==10 for role in groups) and len(groups)==8
    service_before=read('service-before.json');service_after=read('service-after.json')
    assert service_after['vllm:request_success_total']-service_before.get('vllm:request_success_total',0)==46208==owner['model_service_success']
    assert service_after['vllm:prompt_tokens_total']-service_before.get('vllm:prompt_tokens_total',0)==all_prompt
    assert service_after['vllm:generation_tokens_total']-service_before.get('vllm:generation_tokens_total',0)==all_output
result=dict(schema='semloom.ready_query_analysis.v1',campaign_status=owner['status'],new_model_requests=0,
    offline_checks='passed',accepted_units=len(records),retained_model_post_attempts=ledger['request_attempts'],
    all_observed_prompt_tokens=all_prompt,all_observed_output_tokens=all_output,
    roles={role:{name:distribution(values) for name,values in data.items()} for role,data in groups.items()},units=unit_results,
    percentile_method='nearest rank; sorted[ceil(n*p)-1], no interpolation',
    formulas=dict(full_query='(consumer EOF - preparation start)/1e9',ready_query='(consumer EOF - SQL/native API entry)/1e9',
        row_delivery_ready='(consumer row receipt - SQL/native API entry)/1e9',proxy_request='(proxy write_eof return - proxy handler entry)/1e9',
        upstream_request='(complete upstream response.read return - actual upstream session.post entry)/1e9'),
    limitations=['Proxy HTTP interval excludes SDK earlier waiting, client network receipt and parsing. Local EOF write is not client acknowledgement.',
        'Native accounting runs inside the proxy before_forward; PG accounting precedes proxy arrival. Upstream-only interval excludes both.',
        'Batch row delivery is measured from a common query origin; rows within each query are correlated.',
        'Native prompts, token work, parsing and actual concurrency differ; ready data does not isolate scheduling.',
        'Sema includes one ordinary SQL completion marker. DuckDB uses native fetchall; its rows reach the consumer together.',
        'Ten measured queries cannot support population P99.99; sample P99 and P99.99 both select the maximum.',
        'PG SemLoom local HTTP is an internal reference; SemLoom Daft/Ray main execution paths are outside this campaign.'],
    failures=read('failures.json'),inputs={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ('records.json','timings.json','rows.jsonl.gz','http.jsonl.gz','charges.jsonl.gz')})
(root/'analysis.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'offline_checks':'passed','campaign_status':result['campaign_status'],'accepted_units':len(records),'model_post_attempts':ledger['request_attempts'],'evaluation_roles':len(groups)}))
