"""Recompute all fixture query metrics and private durable request hashes."""

import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics

from src.experiments import map_observation_probe as fixture
from src.experiments.postgresql.query_evaluation import ray_transport_accounting


def analyze(root, ledger_root=None):
    summary = json.loads((root/'summary.json').read_text())
    assert summary['status'] == 'passed' and summary['simulated_calls'] == 24672
    if ledger_root is None and all((root/c['case']/'ledger.sqlite').exists() for c in summary['cases']):
        ledger_root = root
    results = []
    for case in summary['cases']:
        result, folder = case['result'], root/case['case']
        with gzip.open(folder/'events.jsonl.gz', 'rt') as stream:
            events = [json.loads(line) for line in stream]
        n = result['rows']
        for kind in ('submitted', 'terminal', 'ray_http_completed', 'remote_request_guard', 'fixture_request_observed'):
            population = [e for e in events if e['event'] == kind]
            assert len(population) == n
            assert {e['key']['sequence'] for e in population} == set(range(n))
        recomputed = fixture.analyze(events, n, result['query_started_ns'], result['query_completed_ns'],
                                     result['synthetic_worker_peak'])
        assert all(result[key] == value for key,value in recomputed.items())
        observed = sorted((e['attempt'], e['key']['sequence']) for e in events
                          if e['event']=='fixture_request_observed')
        assert [a for a,_ in observed] == list(range(1,n+1))
        if ledger_root is not None:
            with sqlite3.connect(f'file:{ledger_root/case["case"]}/ledger.sqlite?mode=ro', uri=True) as database:
                durable = database.execute('SELECT sequence,request_sha256 FROM shared_requests ORDER BY sequence').fetchall()
            assert durable == [(a,hashlib.sha256(fixture.payload(i)).hexdigest()) for a,i in observed]
        rpc = {e['key']['sequence']:e for e in events if e['event']=='ray_http_completed'}
        for event in events:
            if event['event']=='remote_request_guard':
                assert event['monotonic_ns'] <= rpc[event['key']['sequence']]['rpc_started_ns']
            if 'usage' in event:
                usage=event['usage']
                assert usage['held_tasks'] <= 128 and usage['active_requests'] <= 64 and usage['active_work'] <= 64
            if event['event']=='ray_preparation_completed':
                stages=event['preparation']['stages']
                assert stages['encoded_held_bytes'] <= 2**21 and stages['ready_held_bytes'] <= 2**22
                assert stages['ready_held_work'] <= 256 and stages['prepare_inflight'] <= 1
        objects=ray_transport_accounting([dict(e,event='core_'+e['event']) for e in events],n)
        assert result['resources_drained'] and result['transport_drained'] and not result['cleanup_errors']
        assert result['actor_disposed'] and result['synthetic_worker_stopped']
        results.append(dict(case=case['case'], arm=case['arm'], phase=case['phase'], repeat=case['repeat'],
            prepare_seconds=case['prepare_seconds'], rows=n, query_seconds=result['query_seconds'],
            first_result_seconds=result['first_result_seconds'], case_seconds=result['case_seconds'],
            prepared_blocks=result['prepared_blocks'], prepared_group_sizes=result['prepared_group_sizes'],
            core_active_mean=result['core_active_mean'], worker_active_mean=result['synthetic_worker_active_mean'],
            object_accounting=objects))
    assert sum(q['rows'] for q in results)==24672
    metrics={}
    for cost in (.0005,.015):
        metrics[str(cost)]={}
        for arm in ('immediate','original','coalesced'):
            queries=[q for q in results if q['phase']=='measurement' and q['prepare_seconds']==cost and q['arm']==arm]
            metrics[str(cost)][arm]={
                key:dict(values=[q[key] for q in queries],median=statistics.median(q[key] for q in queries))
                for key in ('query_seconds','first_result_seconds','case_seconds','prepared_blocks','core_active_mean','worker_active_mean')}
    return dict(status='passed', verified_simulated_calls=24672, http_requests=0, model_requests=0,
                pg_queries=0, private_request_hash_checks='passed' if ledger_root is not None else 'unavailable: private ledgers not supplied',
                queries=results, metrics=metrics)


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--ledger-root',type=Path)
    args=parser.parse_args()
    print(json.dumps(analyze(args.root,args.ledger_root),indent=2))
