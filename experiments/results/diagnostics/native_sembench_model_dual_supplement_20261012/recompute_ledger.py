"""Audit retained localhost ledger samples without starting any server or model."""
import argparse
import gzip
import hashlib
import json
import math
import statistics
from pathlib import Path


def percentile(values, p):
    values=sorted(values);position=(len(values)-1)*p;low=int(position);high=min(low+1,len(values)-1)
    return values[low]+(values[high]-values[low])*(position-low)


def analyse(raw):
    assert raw['passed'] and raw['model_calls']==raw['gpu_calls']==0
    assert raw['elapsed_seconds']<raw['hard_fixture_seconds']==180
    cases=[]
    for case in raw['cases']:
        assert case['new_logical_requests']==96 and case['transmission_posts']==288
        assert case['concurrency']==4 and case['fixed_response_delay_ms']==25
        assert case['all_processes_exited'] and case['indexes']==[]
        calls,transactions,complete=[case[key] for key in ['reserve_samples','transaction_samples','fixture_request_values_ns']]
        assert len(calls)==288 and len(transactions)==576 and len(complete)==96
        assert {kind:sum(call['kind']==kind for call in calls) for kind in ['gateway','router','model']}==dict(gateway=96,router=96,model=96)
        assert sorted(call['sequence'] for call in calls)==list(range(case['prefix_rows']+1,case['prefix_rows']+289))
        reserve_transactions=[row for row in transactions if row.get('reservation',row['scan_wall_ns']>0)]
        assert len(reserve_transactions)==288
        expected={key:value for key,value in case.items() if key not in ['reserve_samples','transaction_samples','fixture_request_values_ns']}
        for key in ['reserve_wall_ns','reserve_cpu_ns']:
            value=statistics.mean(call[key] for call in calls)/1e6
            assert math.isclose(value,expected[key.replace('_ns','_mean_ms')],rel_tol=1e-12)
        for key in ['begin_wait_wall_ns','begin_wait_cpu_ns','scan_wall_ns','scan_cpu_ns','commit_wall_ns','commit_cpu_ns']:
            value=statistics.mean(row[key] for row in reserve_transactions)/1e6
            assert math.isclose(value,expected[key.replace('_ns','_mean_ms')],rel_tol=1e-12)
        assert math.isclose(statistics.mean(complete)/1e6,expected['fixture_request_mean_ms'],rel_tol=1e-12)
        assert math.isclose(percentile(complete,.95)/1e6,expected['fixture_request_p95_ms'],rel_tol=1e-12)
        cases.append(expected)
    expected=[(0,3000),(0,15000),(0,23000),(1,23000),(1,15000),(1,3000)]
    comparison=raw.get('ledger_version')=='comparison'
    if comparison:
        assert len(cases)==12
        for version in ['original','fixed']:
            assert [(c['round'],c['prefix_rows']) for c in cases if c['ledger_version']==version]==expected
    else:
        assert [(c['round'],c['prefix_rows']) for c in cases]==expected
    assert sum(case['new_logical_requests'] for case in cases)==raw['logical_fixture_requests']==96*len(cases)
    assert sum(case['transmission_posts'] for case in cases)==raw['fixture_transmission_posts']==288*len(cases)
    for round_index in [0,1]:
        original=[c for c in cases if c.get('ledger_version','original')=='original']
        small=next(c for c in original if c['round']==round_index and c['prefix_rows']==3000)
        large=next(c for c in original if c['round']==round_index and c['prefix_rows']==23000)
        assert large['scan_cpu_mean_ms']>6*small['scan_cpu_mean_ms']
        assert large['fixture_request_mean_ms']>2*small['fixture_request_mean_ms']
        if comparison:
            fixed=next(c for c in cases if c['ledger_version']=='fixed' and c['round']==round_index and c['prefix_rows']==23000)
            assert fixed['scan_cpu_mean_ms']==fixed['scan_wall_mean_ms']==0
            assert fixed['reserve_cpu_mean_ms']<large['reserve_cpu_mean_ms']/4
            assert fixed['fixture_request_mean_ms']<large['fixture_request_mean_ms']*.6
    return {key:value for key,value in raw.items() if key!='cases'} | {'cases':cases}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--result-dir',type=Path,default=Path(__file__).resolve().parent)
    args=parser.parse_args()
    proof=json.loads((args.result_dir/'ledger-verification.json').read_text())
    archive=args.result_dir/'ledger-observations.json.gz'
    assert hashlib.sha256(archive.read_bytes()).hexdigest()==proof['raw_archive']['sha256']
    actual=analyse(json.loads(gzip.decompress(archive.read_bytes())))
    assert actual==proof['matched_connection_lifecycle_fixture']
    paired=args.result_dir/'ledger-comparison.json.gz'
    assert hashlib.sha256(paired.read_bytes()).hexdigest()==proof['comparison_archive']['sha256']
    comparison=analyse(json.loads(gzip.decompress(paired.read_bytes())))
    assert comparison==proof['paired_comparison']
    repo=args.result_dir.parents[3]
    for item in proof['fixed_source_files']:
        assert hashlib.sha256((repo/item['path']).read_bytes()).hexdigest()==item['sha256']
    server=json.loads((args.result_dir/'server-ledger-readonly.json').read_text())
    assert server['passed'] and server['model_calls']==0
    assert [row['row_count'] for row in server['results']]==[23520,3600,1440]
    for row in server['results']:
        assert row['sends_indexes']==[] and all(any('SCAN sends' in part[-1] for part in plan) for plan in row['query_plans'])
    print(json.dumps(dict(passed=True,cases=18,fixture_queries=1728,fixture_posts=5184,model_calls=0,
                          server_ledgers=3,source_head='3bd5358706679b639e790585347133cd8d60b836')))


if __name__=='__main__':main()
