"""Rebuild counters and paired medians from public server evidence, without calls."""
import argparse
import gzip
import json
from pathlib import Path
import statistics


def analyze(root):
    summary=json.loads((root/'completed/evidence/summary.json').read_text())
    assert summary['status']=='passed' and summary['http_requests']==2112
    assert summary['model_requests']==summary['pg_queries']==0
    assert summary['cleanup']==dict(http_thread_stopped=True,http_active=0,ray_disconnected=True)
    total=0
    for case in summary['cases']:
        result=case['result'];path=root/'completed/evidence'/case['case']
        assert result['status']=='passed' and not result['cleanup_errors'] and result['exactly_once']
        with gzip.open(path/'events.jsonl.gz','rt') as stream:events=[json.loads(line) for line in stream]
        rows=result['rows'];expected=set(range(rows))
        for kind in ('loopback_http_started','loopback_http_finished','fixture_counted'):
            group=[e for e in events if e['event']==kind]
            assert len(group)==rows and {e['sequence'] for e in group}==expected
            assert sorted(e['request_sha256'] for e in group)==result['request_sha256']
        for kind in ('submitted','terminal','released','ray_http_completed'):
            group=[e for e in events if e['event']==kind]
            assert len(group)==rows and {e['key']['sequence'] for e in group}==expected
        for event in events:
            if 'usage' in event:
                assert event['usage']['active_requests']<=4 and event['usage']['active_work']<=4
                assert event['usage']['held_tasks']<=128
            if event['event']=='loopback_http_started':assert event['active']<=4
            if event['event']=='ray_preparation_completed':
                assert not event['failed']
                assert event['preparation']['stages']['ready_held_bytes']<=2**22
                assert event['preparation']['stages']['encoded_held_bytes']<=2**21
                assert event['preparation']['stages']['ready_held_work']<=128
        ledger=json.loads((path/'ledger.projection.json').read_text())
        assert ledger['unit_closed'] and ledger['attempts']==rows
        assert ledger['allocated_requests']==rows
        assert [r['sequence'] for r in ledger['requests']]==list(range(1,rows+1))
        assert sorted(r['request_sha256'] for r in ledger['requests'])==result['request_sha256']
        assert sum(e['event']=='ray_block_put' for e in events)==result['object_batches']
        total+=rows
    assert total==2112
    metrics={}
    for arm in summary['metrics']:
        metrics[arm]={}
        for metric in ('query_seconds','first_result_seconds','case_seconds','object_batches'):
            values=[c['result'][metric] for c in summary['cases'] if c['phase']=='measurement' and c['arm']==arm]
            metrics[arm][metric]=dict(values=values,median=statistics.median(values))
        assert metrics[arm]==summary['metrics'][arm]
    changes={backend:{m:100*(metrics[backend+'-True'][m]['median']/metrics[backend+'-False'][m]['median']-1)
             for m in ('query_seconds','first_result_seconds','case_seconds')} for backend in ('daft','arrow')}
    return dict(status='passed',cases=len(summary['cases']),http_requests=total,model_requests=0,
                metrics=metrics,percent_changes=changes)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    args.output.write_text(json.dumps(analyze(args.evidence),indent=2)+'\n')
