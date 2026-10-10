"""Replay retained repair-model evidence without issuing requests."""
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import statistics
import sys
import tarfile

root = Path(__file__).resolve().parent
report = json.loads(gzip.decompress((root/'resident-repair-model-analysis.json.gz').read_bytes()))
identity = report['public_archive']
archive = root/identity['path']
assert hashlib.sha256(archive.read_bytes()).hexdigest() == identity['sha256']
raw = {}; originals = {}
for line in __import__('gzip').decompress(archive.read_bytes()).splitlines():
    row=json.loads(line); name=row['path']
    assert not Path(name).is_absolute() and '..' not in Path(name).parts and name not in raw
    payload=row['text'].encode(); assert hashlib.sha256(payload).hexdigest()==row['sha256']
    raw[name]=payload; originals[name]=row['original_sha256']
assert len(raw)==identity['members']==1890
assert report['source_commit']=='aef350b29727b2ddcf82f0734f8905d16edb3a13'
base = 'resident-repair-model01/'
def value(path): return json.loads(raw[base + path])
def lines(path): return [json.loads(line) for line in raw[base + path].splitlines() if line.strip()]
def quantile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered)-1, max(0, __import__('math').ceil(q*len(ordered))-1))]
def integrate(intervals, start, end):
    changes = defaultdict(int)
    changes[start] += 0; changes[end] += 0
    for a, b in intervals:
        assert start <= a <= b <= end
        changes[a] += 1; changes[b] -= 1
    active = peak = area = empty = 0
    previous = start
    for stamp, change in sorted(changes.items()):
        span = stamp-previous
        area += active*span
        if active == 0: empty += span
        active += change
        assert active >= 0
        peak = max(peak, active); previous = stamp
    assert active == 0
    return dict(mean_inflight=area/(end-start), peak=peak, zero_inflight_seconds=empty/1e9)

cfg = value('owner-config.json'); final = value('worker-final.json')
assert final['status'] == 'passed' and final['queries'] == 120 and final['posts'] == 7680
assert len(final['records']) == 120 and len({r['unit_id'] for r in final['records']}) == 120
assert cfg['max_posts'] == 7680 and len(cfg['arms']) == 8
owner = value('owner-exit.json'); ledger = value('ledger-final.json'); service = value('service-count.json')
assert owner['status'] == 'passed' and not owner['cleanup_errors']
assert owner['worker_exit'] == 0 and not owner['remaining_gpu_compute'] and not any(owner['ports_open'].values())
assert ledger['limit'] == ledger['allocated_requests'] == 7680 and len(ledger['units']) == 120
assert all(u['claimed'] == 1 for u in ledger['units'])
assert service['success_delta'] == 7680 and service['running'] == service['waiting'] == 0
responses = set(); request_sets = {}; queries = []; tokens = Counter(); lifetimes = defaultdict(list)
for record in final['records']:
    arm, ordinal, count = record['arm'], record['ordinal'], record['rows']
    prefix = 'arms/' + arm + '/query-' + str(ordinal) + '/'
    summary = value(prefix+'summary.json'); life = value(prefix+'persistent-query.json')
    digest = originals[base+prefix+'summary.json']
    assert digest == record['summary_sha256'] == life['query_summary_sha256']
    assert summary['status'] == 'passed' and summary['rows'] == life['rows'] == count
    assert summary['actual_posts'] == life['actual_posts'] == count and summary['preparation_model_posts'] == 0
    assert summary['execution']['recorded_rows'] == count
    result_bytes = raw[base+prefix+'q0/results.jsonl']
    assert originals[base+prefix+'q0/results.jsonl'] == summary['execution']['results_sha256']
    results = [json.loads(line) for line in result_bytes.splitlines()]
    refs = value('references-'+str(count)+'.json')
    assert len(results) == count and len({str(r['row'][0]) for r in results}) == count
    assert {str(r['row'][0]) for r in results} == set(refs)
    assert all(r['row'][1] in ('POSITIVE','NEGATIVE') for r in results)
    correct = sum(r['row'][1] == refs[str(r['row'][0])] for r in results)
    assert correct == summary['quality']['correct'] and summary['quality']['rows'] == count
    lifetime = life['persistent_lifecycle']; lifetimes[arm].append(lifetime)
    assert not lifetime['result_cache'] and lifetime['core_jobs'] in (None, 0)
    assert lifetime['core_usage'] is None or not any(lifetime['core_usage'].values())
    protocols = lines(prefix+'protocols.jsonl'); http = lines(prefix+'http-trace.jsonl')
    assert len(protocols) == len(http) == count
    request_sets[(arm, ordinal)] = Counter()
    for item in protocols:
        response = item['response']; usage = response['usage']
        assert item['http_status'] == 200 and response['id'] not in responses
        responses.add(response['id'])
        assert item['request_parameters']['model'] == response['model'] == 'Qwen2.5-7B-Instruct'
        assert len(response['choices']) == 1 and response['choices'][0]['finish_reason'] == 'stop'
        assert usage['total_tokens'] == usage['prompt_tokens'] + usage['completion_tokens']
        tokens.update(prompt_tokens=usage['prompt_tokens'], completion_tokens=usage['completion_tokens'])
        request = item['request_values_sha256']
        request_sets[(arm, ordinal)][request] += 1
    for item in http:
        assert item['status'] == 'completed' and item['upstream_headers_send_count'] == 1 and item['retry_count'] == 0
        assert item['request_body_sha256'] == item['forwarded_body_sha256'] and item['query_id'] == record['unit_id']
    assert sum(h['actual_prompt_tokens'] for h in http) == sum(p['response']['usage']['prompt_tokens'] for p in protocols)
    assert sum(h['actual_output_tokens'] for h in http) == sum(p['response']['usage']['completion_tokens'] for p in protocols)
    q0, q1 = life['t_submit_ns'], life['t_eof_ns']
    intervals = [(h['upstream_dispatch_started_monotonic_ns'], h['upstream_response_body_read_completed_monotonic_ns']) for h in http]
    h0, h1 = min(a for a,b in intervals), max(b for a,b in intervals)
    stamps = [life['t_release_ns'], q0, h0, h1, q1]
    assert stamps == sorted(stamps)
    spans = [(b-a)/1e9 for a,b in zip(stamps,stamps[1:])]
    assert abs(sum(spans)-life['release_query_seconds']) < 1e-9
    assert abs(sum(spans[1:])-summary['execution']['ready_query_seconds']) < 1e-9
    events = lines(prefix+'method-events.jsonl')
    returned = [e for e in events if e.get('event') == 'lotus_batch_return']
    if arm.startswith('lotus-'):
        assert len(returned) == 1
    b1 = None if not returned else returned[0]['monotonic_ns']
    if b1 is not None: assert h1 <= b1 <= q1
    timing = summary.get('call_timing', {})
    query = dict(arm=arm, ordinal=ordinal, rows=count, phase=record['phase'], unit_id=record['unit_id'],
        release_seconds=life['release_query_seconds'], submit_seconds=summary['execution']['ready_query_seconds'],
        release_to_submit_seconds=spans[0], submit_to_first_dispatch_seconds=spans[1],
        http_coverage_seconds=spans[2], last_body_to_eof_seconds=spans[3], correct=correct,
        upstream_query=integrate(intervals,q0,q1), upstream_coverage=integrate(intervals,h0,h1),
        request_proxy_seconds=[(h['response_write_completed_monotonic_ns']-h['received_monotonic_ns'])/1e9 for h in http],
        before_forward_seconds=[(h['before_forward_completed_monotonic_ns']-h['before_forward_started_monotonic_ns'])/1e9 for h in http],
        call_timing=timing, batch_return_events=returned,
        last_body_to_batch_return_seconds=None if b1 is None else (b1-h1)/1e9,
        batch_return_to_eof_seconds=None if b1 is None else (q1-b1)/1e9)
    queries.append(query)
assert len(responses) == 7680
pairs = [('lotus-adapted-native','lotus-method-semloom-local-diagnostic','lotus-method-semloom'),
         ('duckdb-adapted-native','duckdb-method-semloom'),
         ('sema-native-direct','sema-native-transparent','sema-method-semloom-request-service')]
for group in pairs:
    for ordinal in range(15):
        assert all(request_sets[(arm,ordinal)] == request_sets[(group[0],ordinal)] for arm in group)
reused = {}
for arm, lives in lifetimes.items():
    assert len(lives) == 15 and len({l['owner_id'] for l in lives}) == 1
    for key in ('execution_id','lm_id','duckdb_connection_id','sema_pid'):
        assert len({l[key] for l in lives if l[key] is not None}) <= 1
    if arm.startswith('duckdb-'): assert lives[0]['duckdb_connection_id'] is not None
    if arm.startswith('sema-'): assert lives[0]['sema_pid'] is not None
    group = value('arms/'+arm+'/group-summary.json')
    assert group['status'] == 'passed' and group['queries'] == 15 and not group['cleanup_errors']
    reused[arm] = {key: lives[0].get(key) for key in ('owner_id','execution_id','lm_id','duckdb_connection_id','sema_pid')}
aggregates = []
for arm in cfg['arms']:
    for shape in (8,128):
        rows = [r for r in queries if r['arm']==arm and r['rows']==shape and r['phase']=='measurement']
        assert len(rows) == 5
        numeric = ['release_seconds','submit_seconds','release_to_submit_seconds','submit_to_first_dispatch_seconds',
                   'http_coverage_seconds','last_body_to_eof_seconds','correct']
        med = {key: statistics.median(r[key] for r in rows) for key in numeric}
        for key in ('last_body_to_batch_return_seconds','batch_return_to_eof_seconds'):
            med[key] = None if rows[0][key] is None else statistics.median(r[key] for r in rows)
        requests = [x for r in rows for x in r['request_proxy_seconds']]
        aggregates.append(dict(arm=arm,rows=shape,count=5,medians=med,
            query_samples=[r['release_seconds'] for r in rows], correct_samples=[r['correct'] for r in rows],
            query_p99=quantile([r['release_seconds'] for r in rows],.99),
            query_p9999=quantile([r['release_seconds'] for r in rows],.9999),
            proxy_request_samples=len(requests),proxy_request_p99=quantile(requests,.99),proxy_request_p9999=quantile(requests,.9999),
            mean_inflight_median=statistics.median(r['upstream_query']['mean_inflight'] for r in rows),
            zero_inflight_seconds_median=statistics.median(r['upstream_coverage']['zero_inflight_seconds'] for r in rows)))
assert dict(tokens)==report['protocol_tokens']
assert aggregates==report['measurement_aggregates']
assert owner['ended_epoch']-value('ownership.json')['started_epoch']==report['owner_seconds']
assert cfg['source'].endswith('/adapter-repair-source02')
for arm in ('lotus-method-semloom','duckdb-method-semloom'):
    pools=[row for row in lines('arms/'+arm+'/owners/'+arm+'/owner-events.jsonl') if row.get('event')=='ray_worker_pool']
    assert len(pools)==1 and pools[0]['identity_available'] and len(set(pools[0]['actor_ids']))==2
records=[]
for q in queries:
 if q['phase']!='measurement':continue
 prefix='arms/'+q['arm']+'/query-'+str(q['ordinal'])+'/'
 name=base+prefix+'sema-service.jsonl'
 if name not in raw:continue
 rows=lines(prefix+'sema-service.jsonl')
 assert len(rows)==q['rows'] and all(r['http_status']==200 and r['error_type'] is None for r in rows)
 assert len({r['request_sequence'] for r in rows})==len(rows)
 elapsed=[];admission=[];return_delay=[]
 for r in rows:
  a,b=r['proxy_arrived_ns'],r['response_written_ns'];assert a<=r['body_read_ns']<=r['forward_started_ns']<=b
  assert r['native_task_ready_ns'] is None and r['native_task_ready_status']=='unavailable'
  elapsed.append((b-a)/1e9)
  if 'core_accepted_ns' in r:
   assert r['body_read_ns']<=r['core_accepted_ns']<=b
   admission.append((r['core_accepted_ns']-r['body_read_ns'])/1e9)
   return_delay.append((b-r['model_returned_ns'])/1e9)
 records.append(dict(arm=q['arm'],ordinal=q['ordinal'],rows=q['rows'],service_request_seconds=elapsed,
  body_read_to_accept_seconds=admission,model_return_to_service_write_seconds=return_delay))
aggregates=[]
for arm in ['sema-native-transparent','sema-method-semloom-request-service']:
 for size in (8,128):
  selected=[r for r in records if r['arm']==arm and r['rows']==size];assert len(selected)==5
  latency=[v for r in selected for v in r['service_request_seconds']]
  admission=[v for r in selected for v in r['body_read_to_accept_seconds']]
  returned=[v for r in selected for v in r['model_return_to_service_write_seconds']]
  aggregates.append(dict(arm=arm,rows=size,samples=len(latency),service_p50=quantile(latency,.5),
   service_p99=quantile(latency,.99),service_p9999=quantile(latency,.9999),service_max=max(latency),
   admission_p99=None if not admission else quantile(admission,.99),
   return_to_service_write_p99=None if not returned else quantile(returned,.99)))
capacity=[]
for arm in reused:
 rows=[r for r in queries if r['arm']==arm and r['phase']=='measurement' and r['rows']==128]
 capacity.append(dict(arm=arm,upstream_peaks=[r['upstream_query']['peak'] for r in rows],
  upstream_mean_over_query_median=statistics.median(r['upstream_query']['mean_inflight'] for r in rows)))
resources=lines('resident-process-tree.jsonl');peaks={};cpu_totals={}
for sample in resources:
 for p in sample['processes']:
  key=str(p['pid'])+':'+str(p['created_epoch'])
  old=peaks.setdefault(key,dict(pid=p['pid'],created_epoch=p['created_epoch'],name=p['name'],rss_bytes=0,pss_bytes=0))
  old['rss_bytes']=max(old['rss_bytes'],p['rss_bytes'])
  if p['pss_bytes'] is not None:old['pss_bytes']=max(old['pss_bytes'],p['pss_bytes'])
  cpu_totals[key]=max(cpu_totals.get(key,0),p['cpu_seconds'])
extra=report['additional_audit']
assert aggregates==extra['service_aggregates']
assert capacity==extra['capacity_observations']
assert len(resources)==extra['process_samples']==386
assert max(sum(p['pss_bytes'] or 0 for p in row['processes']) for row in resources)==extra['whole_tree_pss_peak_bytes']
assert max(sum(p['rss_bytes'] for p in row['processes']) for row in resources)==extra['whole_tree_rss_peak_bytes']
assert sum(cpu_totals.values())==extra['cpu_seconds_sum_of_process_sample_maxima']
print(str(len(raw))+' public members verified;120 queries/7680 unique responses, quality, requests, clocks, all5 measurement repeats, Sema service quantiles, actual in-flight counts and386 process samples; no model requests issued')
