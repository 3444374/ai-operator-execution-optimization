"""Recheck the two bounded software runs without third-party libraries or HTTP."""
import base64
from collections import Counter
import hashlib
import json
from pathlib import Path
import tarfile


def main():
    root=Path(__file__).parent
    facts=json.loads((root/'imlane_async_followup_verification.json').read_text())
    artifact=root/facts['public_archive']['path']
    assert hashlib.sha256(artifact.read_bytes()).hexdigest()==facts['public_archive']['sha256']
    output={}
    with tarfile.open(artifact) as archive:
        def read(name):return archive.extractfile(name).read()
        calls=[json.loads(line) for line in read('q1/canonical/prepared.jsonl').splitlines()]
        assert len(calls)==len({c['call_id'] for c in calls})==2000
        payloads=[base64.b64decode(c['payload'],validate=True) for c in calls]
        expected=facts['original_q1']['prepared']
        assert sum(map(len,payloads))==expected['payload_total_bytes']==1500488
        assert max(map(len,payloads))==expected['max_payload_bytes']==902
        assert hashlib.sha256(b''.join(payloads)).hexdigest()==expected['payload_sequence_sha256']
        for call in calls:call['endpoint']='http://127.0.0.1:9/v1/chat/completions'
        normalized=b'\n'.join(json.dumps(c,separators=(',',':')).encode() for c in calls)+b'\n'
        assert len(normalized)==2649072
        assert hashlib.sha256(normalized).hexdigest()==expected['original_wire_sequence_sha256']
        originals={c['call_id']:c for c in calls}
        for phase in ('lifecycle','q1'):
            summary=json.loads(read(phase+'/summary.json'))
            supervisor=json.loads(read(phase+'-supervisor-result.json'))
            receipts=[json.loads(line) for line in read(phase+'/fixture-receipts.jsonl').splitlines()]
            counts=Counter(row['method'] for row in receipts)
            assert summary['passed'] and supervisor['passed']
            assert counts['POST']==summary['actual_posts']==supervisor['actual_posts']==facts['phases'][phase]['actual_posts']
            assert counts['GET']==supervisor['actual_gets']==facts['phases'][phase]['actual_gets']
            assert supervisor['elapsed_seconds']<=supervisor['wall_limit_s']
            assert counts['POST']<=summary['post_limit']
            assert not summary['post_guard_violations'] and not summary['fixture_journal_errors']
            assert summary['fixture_peak_active']<=2 and summary['fixture_active_after_cleanup']==0 and summary['fixture_connections_closed']
            assert not supervisor['owned_survivors'] and not supervisor['remaining_shm'] and not supervisor['forced_cleanup']
            assert all(c['passed'] and not c['owned_survivors'] and not c['remaining_shm'] for c in summary['cleanup'])
            assert {m['runtime'] for m in summary['modes']}=={'cpython','ray'}
            assert sum(m['posts'] for m in summary['modes'])==counts['POST']
            traces=[]
            for name in archive.getnames():
                if name.startswith(phase+'/') and '/worker-traces/' in name:
                    for line in read(name).splitlines():
                        traces.append((name.split('/')[1].split('-')[1],json.loads(line)))
            for mode in summary['modes']:
                rows=[row for runtime,row in traces if runtime==mode['runtime']]
                if phase=='lifecycle':rows=[r for r in rows if r['call_id'].startswith(('normal-first:','normal-next:','window129:'))]
                expected_rows=145 if phase=='lifecycle' else 2000
                assert len(rows)==len({r['call_id'] for r in rows})==expected_rows
                loops={}
                for row in rows:
                    assert row['loop_running'] and not row['loop_closed'] and row['loaded_http_code_matches_source']
                    assert row['kernel_sha256']==facts['kernel_sha256'] and row['native_calls_sha256']==facts['native_calls_sha256']
                    loops.setdefault(row['worker_pid'],set()).add((row['native_thread_id'],row['loop_generation']))
                    if phase=='q1':
                        call=originals[row['call_id']]
                        assert row['row_id']==call['row_id']
                        assert row['request_sha256']==hashlib.sha256(base64.b64decode(call['payload'])).hexdigest()
                assert len(loops)==2 and all(len(values)==1 for values in loops.values())
                if mode['runtime']=='ray':assert len({r['ray_actor_id'] for r in rows})==2
                if phase=='q1':
                    assert mode['author_dbend_query_batches']==1 and mode['posts']==mode['distinct_occurrences']==2000
                    assert mode['original_query_result_rows']==5 and mode['complete_response_equality'] and mode['request_multiset_identity']
                    assert mode['public_entry']['configured_max_batch_rows']==mode['public_entry']['effective_max_batch_rows']==2000
                    observed=receipts[:2000] if mode['runtime']=='cpython' else receipts[2000:]
                    assert Counter(r['payload_sha256'] for r in observed)==Counter(hashlib.sha256(p).hexdigest() for p in payloads)
                    close=mode['close'];assert close['clean'] and close['bridge_stats']['submitted']==close['bridge_stats']['delivered']==2000
                    limits=close['snapshot']['configured_limits']
                    assert limits['max_batch_rows']==2000 and limits['max_pending_calls']==128
                    assert close['snapshot']['pending_limit_is_execution_concurrency'] is False
            output[phase]=dict(posts=counts['POST'],gets=counts['GET'],elapsed_seconds=supervisor['elapsed_seconds'],modes_passed=2)
    audit=facts['cleanup'];assert audit['passed'] and not audit['owned_survivors'] and not audit['owned_token_survivors'] and not any(audit['remaining_shm'].values())
    assert facts['models']==facts['gpu']==0
    print(json.dumps(dict(passed=True,candidate_sha=facts['candidate_sha'],phases=output,payload_bytes=1500488,normalized_wire_bytes=2649072,checked_process_identities=audit['checked_process_identity_count'],models=0,gpu=0)))


if __name__=='__main__':main()
