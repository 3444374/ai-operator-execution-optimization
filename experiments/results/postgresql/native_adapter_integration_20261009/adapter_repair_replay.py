"""Verify integrated resident inputs and the real Ray initialization failure."""
import base64
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path

root=Path(__file__).parent
identity=json.loads((root/'adapter-repair-verification.json').read_text())
archive=root/identity['public_archive']['path']
assert hashlib.sha256(archive.read_bytes()).hexdigest()==identity['public_archive']['sha256']
members={}
with gzip.open(archive,'rt') as stream:
    for line in stream:
        record=json.loads(line);name=record['path']
        assert not Path(name).is_absolute() and '..' not in Path(name).parts and name not in members
        data=base64.b64decode(record['data_base64'],validate=True)
        assert hashlib.sha256(data).hexdigest()==record['sha256']
        members[name]=(data,record['original_sha256'])
assert len(members)==identity['public_archive']['members']
def value(name):return json.loads(members[name][0])
def lines(name):return [json.loads(line) for line in members[name][0].splitlines()]
base='adapter-repair-fixture02/'
owner=value(base+'owner-result.json');assert owner['status']=='passed' and owner['real_model_posts']==0
posts=0
for run in owner['runs']:
    arm=run['arm'];assert run['exit_code']==0 and not run['timed_out']
    prefix=base+arm+'/'
    inputs=value(prefix+'fixture-inputs.json');requests=lines(prefix+'fixture-requests.jsonl')
    assert [len(rows) for rows in inputs]==[8,8,128] and len(requests)==144
    owners=[];offset=0
    for ordinal,rows in enumerate(inputs):
        query=prefix+'supplier-query-'+str(ordinal)+'/'
        summary=value(query+'summary.json');timing=value(query+'persistent-query.json')
        assert summary['status']=='passed' and summary['actual_posts']==len(rows)
        assert timing['query_summary_sha256']==members[query+'summary.json'][1]
        assert timing['t_release_ns']<=timing['t_submit_ns']<timing['t_eof_ns']
        assert Counter(tuple(v['row']) for v in lines(query+'q0/results.jsonl'))==Counter((v['row_id'],'ok') for v in rows)
        traces=lines(query+'http-trace.jsonl');assert len(traces)==len(rows)
        assert all(v['status']=='completed' and v['retry_count']==0 and
            v['request_body_sha256']==v['forwarded_body_sha256'] for v in traces)
        expected=Counter(v['text'] for v in rows);observed=Counter()
        for request in requests[offset:offset+len(rows)]:
            matches=[text for text in expected if text in json.dumps(request)]
            assert len(matches)==1;observed.update(matches)
        assert observed==expected;offset+=len(rows);owners.append(timing['persistent_lifecycle'])
        if arm.startswith('lotus-'):
            events=lines(query+'method-events.jsonl')
            returned=[v for v in events if v['event']=='lotus_batch_return'];assert len(returned)==1
            assert returned[0]['response_count']==len(rows)
            assert timing['t_submit_ns']<=returned[0]['monotonic_ns']<=timing['t_eof_ns']
    for key in ('owner_id','execution_id','lm_id','duckdb_connection_id','sema_pid','ray_session_id'):
        assert len({v[key] for v in owners})==1,(arm,key)
    assert value(prefix+'group/group-summary.json')['status']=='passed'
    posts+=144
endpoint=value(base+'sema-native-direct-endpoint/endpoint-verification.json')
assert endpoint['endpoint_changed'] and endpoint['same_author_pid'] and endpoint['process_closed']
posts+=endpoint['fixture_posts']
assert posts==owner['fixture_posts']==identity['fixture_posts']==1154
failure=value(base+'actual-initialization-failure/verification.json')
assert failure['status']=='passed' and failure['drain_calls']==failure['execution_created']==1
assert len(set(failure['actual_actor_ids']))==2 and failure['fixture_posts']==failure['real_model_posts']==0
assert all(failure[key] is True for key in ('gateway_thread_closed','event_thread_closed','ray_runtime_closed'))
assert not failure['owned_ray_processes_remaining']
assert 'No such file or directory' in failure['primary_error']['message']
print(str(len(members))+' members verified; 8 integrated arms / 26 queries / 1154 fixture POST, current inputs, B1 and owner reuse; actual 2-worker initialization failure preserves its first error and closes all recorded resources with zero POST')
