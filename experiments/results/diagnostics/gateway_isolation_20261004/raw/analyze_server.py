"""Verify retained finite server events and derive component timings, without execution."""
import argparse
from collections import Counter
import gzip
import hashlib
import io
import json
from pathlib import Path
import statistics
import tarfile


def analyze(archive):
    with tarfile.open(archive,'r:gz') as stream:
        members={m.name:stream.extractfile(m).read() for m in stream if m.isfile()}
    manifest=[json.loads(line) for line in members['storage-manifest.jsonl'].decode().splitlines()]
    for entry in manifest:
        data=members[entry['path']]
        assert len(data)==entry['bytes'] and hashlib.sha256(data).hexdigest()==entry['sha256']
    get=lambda name:json.loads(members[name])
    def events(case):
        return [json.loads(line) for line in gzip.decompress(members[case+'/events.jsonl.gz']).decode().splitlines()]
    def verify(case,rows,counted=False):
        obs=events(case)
        expected={(label,i) for label in ('A','B') for i in range(rows)}
        for kind in ('loopback_http_started','loopback_http_finished','fixture_result_validated'):
            selected=[e for e in obs if e['event']==kind]
            assert len(selected)==2*rows and {(e['label'],e['sequence']) for e in selected}==expected
        joined=[e for e in obs if e['event']=='query_flow_joined']
        assert len(joined)==3
        keys={(e['engine_session_id'],i) for e in joined[:2] for i in range(rows)}
        terminal=[e for e in obs if e['event']=='terminal']
        assert len(terminal)==2*rows and {(e['key']['session_id'],e['key']['sequence']) for e in terminal}==keys
        offered=[e for e in obs if e['event']=='offer']
        assert len(offered)==2*rows+1 and all(e['accepted_prefix_count']==1 for e in offered)
        final=next(e['usage'] for e in reversed(obs) if 'usage' in e)
        assert not any(final.values())
        for entry in obs:
            if 'usage' in entry:
                assert entry['usage']['active_requests']<=4 and entry['usage']['active_work']<=4
        for event in (e for e in obs if e['event']=='fixture_result_validated'):
            expected_hash=hashlib.sha256(f"fixture:{event['label']}:{event['sequence']}".encode()).hexdigest()
            assert event['output_sha256']==expected_hash
        if counted:
            grant=[e for e in obs if e['event']=='fixture_request_counted']
            assert sorted(e['attempt'] for e in grant)==list(range(1,2*rows+1))
            assert Counter(e['request_sha256'] for e in grant)==Counter(e['request_sha256'] for e in obs if e['event']=='loopback_http_started')
        ray=[e for e in obs if e['event']=='ray_http_completed']
        if ray:
            assert len(ray)==2*rows and {(e['key']['session_id'],e['key']['sequence']) for e in ray}==keys
            assert all(e['shared_clock'] and e['rpc_started_ns']<=e['worker_started_ns']<=e['worker_ended_ns']<=e['received_ns'] for e in ray)
            assert any(e['event']=='ray_transport_closed' and e['confirmed'] and e['object_bytes']==0 for e in obs)
        return obs
    gateway=get('gateway-platform-20261004c/suite/summary.json')
    for case in gateway['cases']:
        verify('gateway-platform-20261004c/suite/'+f"{case['phase']}-{case['repeat']}-{case['arm']}",case['rows_per_job'])
    for arm in ('direct_http','ray_http'):
        verify('gateway-platform-20261004b/suite/check-0-'+arm,4)
    mapped=get('mapped-runtime-20261004c/suite/summary.json')
    stages=[]
    for case in mapped['cases']:
        path='mapped-runtime-20261004c/suite/'+f"{case['phase']}-{case['repeat']}-{case['arm']}"
        obs=verify(path,case['rows_per_job'],counted=True)
        assert case['allocated_requests']==2*case['rows_per_job'] and case['budget_handles_closed']
        if case['phase']!='measurement':continue
        guards=[e for e in obs if e['event']=='remote_request_guard']
        rpc=[e for e in obs if e['event']=='ray_http_completed']
        payload=[e for e in obs if e['event']=='ray_work' and e['stage']=='payload_next']
        row={k:case[k] for k in ('arm','repeat','consume_ms','budget_setup_ms','complete_case_ms')}
        row.update(guard_reserve_total_ms=sum(e['reserve_ns'] for e in guards)/1e6,
                   guard_total_ms=sum(e['elapsed_ns'] for e in guards)/1e6,
                   guard_reserve_median_ms=statistics.median(e['reserve_ns'] for e in guards)/1e6,
                   payload_next_work_total_ms=sum(e['work_ns'] for e in payload)/1e6,
                   payload_next_resume_total_ms=sum(e['resume_ns'] for e in payload)/1e6,
                   payload_windows=sum(e['event']=='ray_block_put' for e in obs),
                   payload_rows=[e['rows'] for e in obs if e['event']=='ray_block_put'],
                   rpc_call_total_ms=sum(e['submit_elapsed_ns'] for e in rpc)/1e6,
                   rpc_call_median_ms=statistics.median(e['submit_elapsed_ns'] for e in rpc)/1e6,
                   before_worker_median_ms=statistics.median(e['before_worker_ns'] for e in rpc)/1e6,
                   worker_median_ms=statistics.median(e['worker_elapsed_ns'] for e in rpc)/1e6,
                   after_worker_median_ms=statistics.median(e['after_worker_ns'] for e in rpc)/1e6,
                   startup_ms={e['stage']:e['elapsed_seconds']*1000 for e in obs if e['event']=='ray_startup'},
                   object_accounted_peak=max(e['object_bytes'] for e in obs if 'object_bytes' in e),
                   ray_rows=len(rpc))
        stages.append(row)
    verify('mapped-runtime-20261004a/suite/check-0-durable',4,counted=True)
    sdk=get('mapped-runtime-20261004c/sdk-both/result.json')
    assert sdk['status']=='passed' and sdk['owner_closed_all_callers']
    assert sorted(i for c in sdk['caller_results'] for i in c['attempts'])==list(range(1,38))
    assert all(c['mock_posts'] and c['handles_closed'] for c in sdk['caller_results'])
    cleanup=get('cleanup.json');assert not cleanup['live_owned_services']
    return dict(schema='semloom.server-observation-replay.v1',archive_sha256=hashlib.sha256(Path(archive).read_bytes()).hexdigest(),
        archive_members=len(members),verified_manifest_entries=len(manifest),gateway_http_requests=272,
        mapped_http_requests=1040,sdk_mock_posts=74,model_requests=0,pg_queries=0,
        gateway_metrics=gateway['metrics'],mapped_metrics=mapped['metrics'],mapped_stages=stages,
        sdk_positive_posts={c['mode']:c['mock_posts'] for c in sdk['caller_results']},cleanup=cleanup,
        interpretation='CPU fixture and real vendor/SDK lifecycle only; no native baseline, PG or GPU performance claim')


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('archive',type=Path);parser.add_argument('output',type=Path)
    args=parser.parse_args();result=analyze(args.archive)
    with args.output.open('x') as stream:json.dump(result,stream,ensure_ascii=False,separators=(',',':'))
    print(json.dumps({k:v for k,v in result.items() if k not in ('mapped_stages','gateway_metrics','mapped_metrics')},ensure_ascii=False))
