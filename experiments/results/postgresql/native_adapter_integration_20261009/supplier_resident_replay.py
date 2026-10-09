"""Replay actual resident supplier fixtures without a model or native libraries."""
import base64
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path

root=Path(__file__).parent
identity=json.loads((root/'supplier-resident-verification.json').read_text())
path=root/identity['public_archive']['path']
assert hashlib.sha256(path.read_bytes()).hexdigest()==identity['public_archive']['sha256']
members={}
with gzip.open(path,'rt',encoding='utf-8') as source:
    for line in source:
        record=json.loads(line);name=record['path']
        assert not Path(name).is_absolute() and '..' not in Path(name).parts and name not in members
        value=base64.b64decode(record['data_base64'],validate=True)
        assert hashlib.sha256(value).hexdigest()==record['sha256'],name
        members[name]=(value,record['original_sha256'])
assert len(members)==identity['public_archive']['members']
def value(name):return json.loads(members[name][0])
def lines(name):return [json.loads(line) for line in members[name][0].splitlines()]
base='supplier-resident-fixture04/'
owner=value(base+'owner-result.json')
assert owner['status']=='passed' and owner['real_model_posts']==0 and owner['fixture_posts']==722
assert value(base+'owner-start.json')['cpu_affinity']==list(range(64,72))
preflight=value(base+'preflight.json')
assert preflight['status']=='ok' and preflight['profile_selection']=='explicit'
assert preflight['machine']['gpu_names']==[]
assert value(base+'cpu-fixture-profile.json')['gpu']['required'] is False
arms=('duckdb-adapted-native','duckdb-method-semloom','sema-native-direct',
      'sema-native-transparent','sema-method-semloom-request-service')
posts=0
for arm in arms:
    prefix=base+arm+'/'
    inputs=value(prefix+'fixture-inputs.json')
    assert [len(rows) for rows in inputs]==[8,8,128]
    requests=lines(prefix+'fixture-requests.jsonl');assert len(requests)==144
    lifecycles=[];offset=0;pools=[]
    for ordinal,rows in enumerate(inputs):
        query=prefix+'supplier-query-'+str(ordinal)+'/'
        summary=value(query+'summary.json');sidecar=value(query+'persistent-query.json')
        assert summary['status']=='passed' and summary['rows']==len(rows)
        assert summary['actual_posts']==sidecar['actual_posts']==len(rows)
        assert sidecar['query_summary_sha256']==members[query+'summary.json'][1]
        assert sidecar['t_release_ns']<=sidecar['t_submit_ns']<sidecar['t_eof_ns']
        traces=lines(query+'http-trace.jsonl');assert len(traces)==len(rows)
        assert all(h['status']=='completed' and h['retry_count']==0 and
            h['request_body_sha256']==h['forwarded_body_sha256'] for h in traces)
        actual=lines(query+'q0/results.jsonl')
        assert Counter(tuple(item['row']) for item in actual)==Counter((row['row_id'],'ok') for row in rows)
        expected=Counter(row['text'] for row in rows);observed=Counter()
        for request in requests[offset:offset+len(rows)]:
            matches=[text for text in expected if text in json.dumps(request)]
            assert len(matches)==1
            observed.update(matches)
        assert observed==expected;offset+=len(rows)
        lifecycles.append(sidecar['persistent_lifecycle'])
        if arm=='sema-method-semloom-request-service':
            service=summary['request_service']
            assert service['received_requests']==service['forwarded_posts']==len(rows)
            assert not service['cleanup_errors'] and service['first_error'] is None
            workers=[e for e in service['core_events'] if e['event']=='ray_worker_pool']
            assert len(workers)==1 and len(set(workers[0]['actor_ids']))==2
            pools.extend(workers[0]['actor_ids'])
    for key in ('owner_id','execution_id','duckdb_connection_id','sema_pid','ray_session_id'):
        assert len({entry[key] for entry in lifecycles})==1,(arm,key)
    if arm=='duckdb-method-semloom':
        batches=value(prefix+'native-vectors.json')
        assert [entry['rows'] for entry in batches]==[list(range(count)) for count in (8,8,128)]
        assert len({call for entry in batches for call in entry['native_call_ids']})==144
        assert len({query for entry in batches for query in entry['native_query_ids']})==3
        assert all(not any(entry['core_usage'].values()) and entry['core_jobs']==0 for entry in lifecycles)
    if arm.startswith('sema-'):
        replacements=value(prefix+'source-replacements.json')
        assert [entry['rows'] for entry in replacements]==[8,8,128]
        assert len({entry['pid'] for entry in replacements})==1
        assert len({entry['source_sha256'] for entry in replacements})==3
        if arm!='sema-native-direct':assert len({entry['endpoint'] for entry in replacements})==3
    if arm=='sema-method-semloom-request-service':assert len(set(pools))==6
    assert value(prefix+'group/group-summary.json')['status']=='passed'
    posts+=offset
endpoint=value(base+'sema-native-direct-endpoint/endpoint-verification.json')
assert all(endpoint[key] is True for key in ('same_author_pid','endpoint_changed','input_changed','process_closed'))
posts+=endpoint['fixture_posts'];assert posts==722
for ordinal in (1,2,3):
    failure=value('supplier-resident-fixture0'+str(ordinal)+'/owner-result.json')
    assert failure['status']=='preflight_failed' and failure['real_model_posts']==0
print(str(len(members))+' members verified; 5 resident supplier arms, 17 queries / 722 fixture POST; input and endpoint replacement, native owner reuse and retained zero-POST failures passed')
