"""Offline verification of the complete-call capacity observations; never sends HTTP."""
from collections import Counter
from pathlib import Path
import hashlib
import json
import tarfile


def verify(root):
    summary=json.loads((root/'imlane_capacity_verification.json').read_text())
    path=root/summary['observations']['path']
    assert hashlib.sha256(path.read_bytes()).hexdigest()==summary['observations']['sha256']
    with tarfile.open(path) as archive:
        observations={member.name:archive.extractfile(member).read() for member in archive if member.isfile()}
    assert len(observations)==summary['observations']['members']
    manifest=json.loads(observations['redaction-manifest.json'])
    for entry in manifest:
        raw=observations[entry['member']]
        assert len(raw)==entry['bytes']
        assert hashlib.sha256(raw).hexdigest()==entry['redacted_sha256']
    total=0
    for number in range(1,5):
        prefix='attempt'+str(number)+'/'
        result=json.loads(observations[prefix+'test-result.json'])
        supervisor=json.loads(observations[prefix+'test-supervisor-result.json'])
        receipts=json.loads(observations[prefix+'completed-receipts.json'])
        total+=len(receipts)
        assert total==result['actual_posts']
        assert not supervisor['owned_survivors']
        assert result['fixture_active_after_cleanup']==0
        assert result['fixture_server_closed']
        assert all(c['passed'] and not c['owned_survivors'] and not c['remaining_shm'] for c in result['cleanup'])
        assert supervisor['elapsed_seconds']<=900
        assert result['passed']==(number in (3,4))
    assert total==summary['server_budget']['actual_posts']==4559
    assert total+summary['local_combination']['local_actual_posts']==summary['total_new_software_posts']==4563
    assert summary['total_new_software_posts']<=summary['server_budget']['post_limit']
    assert summary['bridge']['elapsed_seconds']<=120 and summary['bridge']['exit_code']==0
    assert not summary['bridge']['rebuilt_author_system']
    assert not summary['models'] and not summary['gpu']
    q1={}
    for number,mode in ((1,'cpython'),(3,'ray')):
        traces=[]
        for name,raw in observations.items():
            if name.startswith('attempt'+str(number)+'/'+mode+'-') and '/worker-traces/' in name:
                traces.extend(json.loads(line) for line in raw.decode().splitlines())
        values=[row for row in traces if row['call_id'].startswith('q1-offline:filter:1:')]
        assert len(values)==2000 and len({row['call_id'] for row in values})==2000
        assert {row['row_id'] for row in values}==set(range(2000))
        assert all(row['status']==200 and not row['transport_error_type'] and not row['execution_error_type'] for row in values)
        result=summary['modes'][mode]
        prepared=result['q1']['prepared'];expected=summary['q1_measurement']
        assert prepared['payload_sequence_sha256']==expected['payload_sequence_sha256']
        assert prepared['original_wire_sequence_sha256']==expected['wire_sequence_sha256']
        assert result['q1']['complete_responses']==2000
        assert result['q1']['original_query_result_rows']==5
        stats=result['q1']['close']['bridge_stats']
        assert stats['submitted']==stats['delivered']==2000 and stats['pending_peak']<=128
        assert result['continuous_supply']['third_started_before_first_completed']
        assert result['execution_error']['type']=='ValueError'
        assert not result['cancel']['clean'] and result['cancel']['external_coordinator_requires_cleanup']
        assert result['normal_after_error_cleanup'] and result['normal_after_cancel_cleanup']
        assert result['integration_source_error_cancel_next_passed'] and result['integration_source_posts']==6
        receipts=json.loads(observations['attempt'+str(number)+'/completed-receipts.json'])
        posts=[row for row in receipts if row['path']=='/v1/chat/completions']
        assert len(posts)==2000
        assert Counter(row['payload_sha256'] for row in posts)==Counter(row['request_sha256'] for row in values)
        q1[mode]=dict(rows=len(values),actual_worker_pids=len({row['worker_pid'] for row in values}),
                      ray_actor_ids=len({row.get('ray_actor_id') for row in values if row.get('ray_actor_id')}))
        assert q1[mode]['actual_worker_pids']==2
        if mode=='ray':assert q1[mode]['ray_actor_ids']==2
    assert len(summary['q1_dbend_query_batches'])==2
    assert all(v['author_dbend_batches_for_q1']==1 and v['batch']['rows']==2000 for v in summary['q1_dbend_query_batches'])
    cleanup=summary['cleanup']
    assert not cleanup['active_owned_processes'] and not cleanup['active_run_token_processes']
    assert not cleanup['remaining_owned_shared_memory'] and all(cleanup['author_binaries_unchanged'].values())
    return dict(passed=True,server_posts=total,local_posts=4,total_posts=4563,q1=q1,
                retained_failed_attempts=2,models=0,gpu=0)


if __name__=='__main__':
    print(json.dumps(verify(Path(__file__).resolve().parent),sort_keys=True))
