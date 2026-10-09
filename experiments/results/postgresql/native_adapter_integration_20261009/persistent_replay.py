"""Verify retained repeated-query fixtures without importing optional libraries."""
import base64
import gzip
import hashlib
import json
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent
    archive=root/'raw/persistent-fixtures.jsonl.gz'
    evidence=json.loads((root/'persistent-verification.json').read_text())
    assert hashlib.sha256(archive.read_bytes()).hexdigest()==evidence['archive_sha256']
    members={}
    originals={}
    for line in gzip.decompress(archive.read_bytes()).splitlines():
        value=json.loads(line)
        payload=base64.b64decode(value['data_base64'],validate=True)
        assert hashlib.sha256(payload).hexdigest()==value['sha256']
        assert value['path'] not in members
        members[value['path']]=payload
        originals[value['path']]=value['original_sha256']
    assert len(members)==evidence['archive_members']
    posts=queries=rows=0
    for arm in evidence['six_arms']:
        identities=[]
        for number in range(3):
            prefix='resident-fixtures02/'+arm+'/'+arm+'-'+str(number)+'/'
            summary=json.loads(members[prefix+'summary.json'])
            timing=json.loads(members[prefix+'persistent-query.json'])
            traces=[json.loads(line) for line in members[prefix+'http-trace.jsonl'].splitlines()]
            assert summary['status']=='passed'
            assert len(traces)==summary['actual_posts']==(4,8,4)[number]
            assert summary['rows']==summary['execution']['recorded_rows']==(4,8,4)[number]
            assert timing['measurement_phase']==('qualification','warmup','measurement')[number]
            assert {trace['query_id'] for trace in traces}=={timing['unit_id']}
            assert timing['t_release_ns']<=timing['t_submit_ns']<timing['t_eof_ns']
            assert (timing['t_eof_ns']-timing['t_release_ns'])/1e9==timing['release_query_seconds']
            assert all(trace['status']=='completed' and trace['request_body_sha256']==trace['forwarded_body_sha256'] for trace in traces)
            assert timing['query_summary_sha256']==originals[prefix+'summary.json']
            identities.append(timing['persistent_lifecycle'])
            posts+=summary['actual_posts'];queries+=1;rows+=summary['rows']
        for key in ('owner_id','execution_id','lm_id','ray_session_id'):
            assert len({value[key] for value in identities})==1,(arm,key)
        assert [value['completed_queries'] for value in identities]==[1,2,3]
    assert (queries,posts,rows)==(18,96,96)
    repaired=json.loads(members['resident-duckdb-old03/runs.json'])
    assert len(repaired)==2 and all(value['exit_code']==0 and value['actual_fixture_posts']==4 for value in repaired)
    failed=[json.loads(value) for name,value in members.items()
            if name.startswith('resident-regression02/') and name.endswith('/query/summary.json')]
    assert len(failed)==1 and failed[0]['status']=='failed' and failed[0]['attempted_posts']==0
    print(f'{len(members)} members verified; 18 repeated queries / 96 fixture POST; 2 old DuckDB entries / 8 fixture POST; original zero-POST loader failure retained')


if __name__=='__main__':
    main()
