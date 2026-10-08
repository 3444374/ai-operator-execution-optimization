"""Streaming replay of batch row, HTTP, delivery and complete query intervals."""
from collections import defaultdict
import gzip,hashlib,itertools,json,math,statistics
from pathlib import Path

root=Path(__file__).resolve().parent
ROLES=('pg-map','lotus-map','duckdb-ai','daft-prompt','sema-map','pg-request','pg-token-wide','pg-work-limited')
read=lambda name:json.loads((root/name).read_text())
def lines(name):
    with gzip.open(root/name,'rt') as stream:
        for line in stream:yield json.loads(line)
def describe(values,queries,*,signed=False):
    if not values:return dict(samples=0,query_units=queries,status='unavailable',reason='no complete measured observations')
    if any(not math.isfinite(v) or (v<0 and not signed) for v in values):raise ValueError('negative or non-finite interval')
    ordered=sorted(values);n=len(ordered);ranks={str(p):math.ceil(n*p/100) for p in (50,99,99.99)}
    return dict(samples=n,query_units=queries,mean_s=statistics.mean(ordered),p50_s=ordered[ranks['50']-1],p99_s=ordered[ranks['99']-1],empirical_p9999_s=ordered[ranks['99.99']-1],minimum_s=ordered[0],maximum_s=ordered[-1],negative_observations=sum(v<0 for v in values),signed_observation_offset=signed,order_statistic_ranks=ranks,observations_above_p9999_rank=n-ranks['99.99'],population_p9999_s=None,p9999_status='descriptive_sample; rows share query origins and at most 200 complete queries per path')
records=read('records.json');by_id={r['unit_id']:r for r in records}
timings={t['unit_id']:t for t in read('timings.json')}
provenance=read('row-timing-provenance.json')
samples={role:defaultdict(list) for role in ROLES};units=defaultdict(list)
event_groups=iter(itertools.groupby(lines('events.jsonl.gz'),lambda e:e['unit_id']))
for identity,group in itertools.groupby(lines('outputs.jsonl.gz'),lambda r:r['unit_id']):
    rows=list(group);event_id,event_group=next(event_groups);events=list(event_group)
    if event_id!=identity:raise ValueError('row and event groups differ')
    r=by_id[identity];t=timings[identity];e=t['execution'];start=t['query_preparation_started_ns'];release=e['t_release_ns'];eof=e['t_query_terminal_ns']
    if len(rows)!=r['expected_rows'] or len({v['occurrence_sha256'] for v in rows})!=r['expected_rows']:raise ValueError('row occurrence count differs')
    assert len(rows)==r['expected_rows'] and all(start<=v['received_ns']<=eof for v in rows)
    assert provenance['units'][identity]['source_results_sha256']==e['results_sha256']
    assert max(v['received_ns'] for v in rows)==e['t_last_row_ns']
    if r['stage']!='tail-evaluation':continue
    role=r['role'];values={'row_e2e':[(v['received_ns']-start)/1e9 for v in rows]}
    if role.startswith('pg-'):
        indexed={name:{} for name in ('core_map_task','core_http_started','core_http_finished','core_map_completion')}
        guard_prefix=[];last_http=None
        for event in events:
            kind=event.get('event')
            if kind in indexed:
                seq=event.get('sequence') if 'map_' in kind else event['key']['sequence']
                assert seq not in indexed[kind];indexed[kind][seq]=event['monotonic_ns']
                if kind=='core_http_started':last_http=event
            elif kind=='request':
                if last_http is None:raise ValueError('request has no preceding HTTP observation')
                guard_prefix.append((event['monotonic_ns']-last_http['monotonic_ns'])/1e9);last_http=None
        assert all(set(v)==set(range(len(rows))) for v in indexed.values())
        receives={v['pg_sequence']:v['received_ns'] for v in rows};assert set(receives)==set(range(len(rows)))
        task,begins,ends,completed=(indexed[k] for k in indexed)
        values.update(http_observed=[(ends[k]-begins[k])/1e9 for k in begins],task_observation_to_http_signed_offset=[(begins[k]-task[k])/1e9 for k in begins],http_to_map_completion=[(completed[k]-ends[k])/1e9 for k in ends],map_completion_to_row=[(receives[k]-completed[k])/1e9 for k in completed],http_to_row=[(receives[k]-ends[k])/1e9 for k in ends],http_begin_to_accounted_observation=guard_prefix)
        assert len(guard_prefix)==len(rows)
    else:
        assert len(events)==len(rows) and all(v['status']=='completed' for v in events)
        values.update(http_observed=[v['response_completed_epoch_s']-v['received_epoch_s'] for v in events],native_body_read_and_dispatch_prefix=[v['upstream_start_epoch_s']-v['received_epoch_s'] for v in events],native_forward_and_response=[v['upstream_response_epoch_s']-v['upstream_start_epoch_s'] for v in events],native_post_response_observation=[v['response_completed_epoch_s']-v['upstream_response_epoch_s'] for v in events])
    full=(eof-start)/1e9;assert full==r['full_query_seconds']
    values.update(full_query=[full],preparation=[(release-start)/1e9],release_to_eof_query=[(eof-release)/1e9],first_row=[(e['t_first_row_ns']-start)/1e9])
    for name,v in values.items():
        if any(x<0 for x in v) and name!='task_observation_to_http_signed_offset':raise ValueError('chronology differs: '+identity+' '+name)
        samples[role][name].extend(v)
    units[role].append(dict(unit_id=identity,repeat=r['repeat'],point=r['point'],active_work=r['active_work'],expected_rows=r['expected_rows'],full_query_s=full,preparation_s=(release-start)/1e9,segments={name:describe(v,1,signed=name=='task_observation_to_http_signed_offset') for name,v in values.items()},quality=r['quality'],model_counter_delta=r['model_counter_delta'],observed_peak_http=r['observed_peak_http']))
try:next(event_groups);raise ValueError('extra event unit')
except StopIteration:pass
result=[]
for role in ROLES:
    chosen=sorted(units[role],key=lambda v:v['repeat']);n=len(chosen)
    assert [v['repeat'] for v in chosen]==list(range(1,n+1))
    result.append(dict(role=role,point=chosen[0]['point'] if chosen else None,active_work=chosen[0]['active_work'] if chosen else None,complete_queries=n,measured_rows=sum(v['expected_rows'] for v in chosen),scope='fixed 512-row collection; fresh per-query driver; round-robin path order',distributions={name:describe(v,n,signed=name=='task_observation_to_http_signed_offset') for name,v in samples[role].items()},units=chosen,completed_requested_schedule=n==200))
value=dict(schema='semloom.semantic_tail_descriptive.v2',new_model_requests=0,campaign_status=read('summary.json')['status'],rows=result,percentile_method='nearest rank: sorted_values[ceil(p/100*n)-1]; no interpolation',row_e2e_definition='(consumer received_ns-query_preparation_started_ns)/1e9; common fixed full-scan query origin',full_query_definition='(t_query_terminal_ns-query_preparation_started_ns)/1e9',external_per_request_arrival_clock=dict(status='unavailable',reason='no independent application request arrival clock'),durable_commit_duration=dict(status='unavailable',reason='no isolated commit start/end in these eight-path source snapshots; PG begin-to-request captures a prefix and native forwarding includes commit and HTTP'),native_response_to_row=dict(status='unavailable',reason='native gateway uses wall clocks and row recorder uses monotonic clocks; no retained conversion observations'),ray_future_callback=dict(status='not_applicable',reason='none of these eight paths uses the SemLoom Ray transport; separate main-source probe'),interpretation='HTTP observations include experimental accounting; per-row intervals overlap; segment sums across rows are not full query wall time; batch rows are correlated',failures=read('failures.json'),inputs={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ('records.json','timings.json','outputs.jsonl.gz','events.jsonl.gz','row-timing-provenance.json')})
value['schema']='semloom.semantic_tail_descriptive.v3'
value['task_acceptance_to_http_wait']=dict(status='unavailable',reason='map_task observer runs after Core offer; HTTP thread can timestamp first; signed observer offset retained without clipping or exclusion')
(root/'tail-analysis.json').write_text(json.dumps(value,sort_keys=True,indent=2)+'\n')
print(json.dumps({r['role']:dict(complete_queries=r['complete_queries'],measured_rows=r['measured_rows'],row=r['distributions'].get('row_e2e'),sql=r['distributions'].get('full_query')) for r in result}))
