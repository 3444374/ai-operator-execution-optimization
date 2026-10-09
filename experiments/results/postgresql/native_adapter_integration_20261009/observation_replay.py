"""Verify retained LOTUS observation clocks and complete replayed HTTP bodies."""
import base64
from collections import Counter,defaultdict
import gzip
import hashlib
import json
from pathlib import Path

root=Path(__file__).parent
analysis=json.loads((root/'lotus-observation-analysis.json').read_text())
archive=root/analysis['public_archive']['path']
assert hashlib.sha256(archive.read_bytes()).hexdigest()==analysis['public_archive']['sha256']
members={}
with gzip.open(archive,'rt') as stream:
    for line in stream:
        item=json.loads(line);name=item['path']
        assert not Path(name).is_absolute() and '..' not in Path(name).parts and name not in members
        body=base64.b64decode(item['data_base64'],validate=True)
        assert hashlib.sha256(body).hexdigest()==item['sha256']
        members[name]=(body,item['original_sha256'])
assert len(members)==analysis['public_archive']['members']
def value(name):return json.loads(members[name][0])
def lines(name):return [json.loads(line) for line in members[name][0].splitlines()]
def integrate(intervals,start,end):
    changes=defaultdict(int);changes[start]+=0;changes[end]+=0
    for left,right in intervals:
        assert start<=left<=right<=end
        changes[left]+=1;changes[right]-=1
    active=area=zero=peak=0;previous=start
    for stamp,delta in sorted(changes.items()):
        span=stamp-previous;area+=active*span
        if active==0:zero+=span
        active+=delta;peak=max(peak,active);previous=stamp
    assert active==0
    return dict(average_inflight=area/(end-start),zero_inflight_s=zero/1e9,peak=peak)
posts=queries=0;request_sets={}
measurements=[('lotus-observation-diagnostic02/',entry) for entry in analysis['function_and_timeline_measurements']]
measurements += [('lotus-observation-diagnostic04/',entry) for entry in analysis.get('after_function_and_timeline_measurements',[])]
for base,summary in measurements:
    owner=value(base+'owner-result.json');assert owner['status']=='passed' and owner['real_model_posts']==0
    replies=value(base+'replies.json')
    arm,mode=summary['arm'],summary['mode']
    prefix=base+mode+'-'+arm+'/'
    assert value(prefix+'diagnostic-summary.json')==summary
    metrics=lines(prefix+'function-clocks.jsonl');flows=value(prefix+'flow-clocks.json')
    for record in summary['queries']:
        shape=record['rows'];query=prefix+'query-'+str(shape)+'/'
        sidecar=value(query+'persistent-query.json');native=value(query+'summary.json')
        assert native['status']=='passed' and native['actual_posts']==shape
        assert sidecar['query_summary_sha256']==members[query+'summary.json'][1]
        q0=sidecar['t_submit_ns'];q1=sidecar['t_eof_ns']
        trace=lines(query+'http-trace.jsonl');assert len(trace)==shape
        assert all(row['status']=='completed' and row['retry_count']==0 and
            row['request_body_sha256']==row['forwarded_body_sha256'] for row in trace)
        intervals=[(row['upstream_dispatch_started_monotonic_ns'],row['upstream_response_body_read_completed_monotonic_ns']) for row in trace]
        h0=min(a for a,b in intervals);h1=max(b for a,b in intervals)
        events=lines(query+'method-events.jsonl')
        batches=[row for row in events if row['event']=='lotus_batch_return'];assert len(batches)==1
        b1=batches[0]['monotonic_ns'];assert q0<=h0<=h1<=b1<=q1
        assert [record[k] for k in ('q0_ns','h0_ns','h1_ns','b1_ns','q1_ns')]==[q0,h0,h1,b1,q1]
        assert record['over_query']==integrate(intervals,q0,q1)
        assert record['during_http_work']==integrate(intervals,h0,h1)
        protocol=lines(query+'protocols.jsonl');outputs=lines(query+'q0/results.jsonl')
        assert len(protocol)==len(outputs)==shape
        request_sets[(base,arm,mode,shape)]=Counter(p['request_values_sha256'] for p in protocol)
        for response in lines(query+'upstream-response-bodies.jsonl'):
            assert response['response_body_base64']==replies[response['request_values_sha256']]
        posts+=shape;queries+=1
    for function in summary['functions']:
        if function['label'] in ('flow_wait','flow_advance'):
            matching=[row for row in flows if row['phase']=='measurement' and row['label']==function['label']]
            assert len(matching)==1
            row=matching[0]
            assert row['count']==function['count'] and row['stale_waits']==function['stale_waits']
            assert row['wall_ns']/1e9==function['wall_sum_s']
            assert row['thread_cpu_ns']/1e9==function['thread_cpu_sum_s']
        else:
            matching=[row for row in metrics if row['phase']=='measurement' and row['label']==function['label']]
            assert len(matching)==function['count']
            assert sum(row['wall_ns'] for row in matching)/1e9==function['wall_sum_s']
            assert sum(row['thread_cpu_ns'] for row in matching)/1e9==function['thread_cpu_sum_s']
for shape in (8,128):
    samples=[value for (base,arm,mode,rows),value in request_sets.items() if rows==shape]
    assert len(samples)==len(measurements) and all(value==samples[0] for value in samples)
assert posts==analysis['valid_fixture_posts'] and queries==analysis['valid_queries']
failed=value('lotus-observation-diagnostic01/owner-result.json')
assert failed['status']=='failed' and failed['real_model_posts']==0
if analysis.get('after_function_and_timeline_measurements'):
    interrupted=value('lotus-observation-diagnostic03/restart-interruption.json')
    assert interrupted['status']=='interrupted_by_user_server_restart'
    assert interrupted['complete_group_summaries']==0 and interrupted['real_model_posts']==0
print(str(len(members))+' members verified; '+str(queries)+' observation queries / '+str(posts)+' fixture POST, unchanged response bodies, request sets, Q0/H0/H1/B1/Q1 and wall/thread CPU clocks replayed; original harness cap failure and restart retained')
