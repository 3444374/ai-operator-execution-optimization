"""Reconstruct query sharing diagnostics from retained service and capacity traces."""

import argparse
import gzip
import json
from pathlib import Path


def audit(run):
    jobs = {row['job_id']: (label, run['queries'][next(i for i, q in enumerate(run['queries'])
            if q['label'] == label)]) for label, row in run['jobs'].items()}
    submitted, started, completed, active = {}, {}, {}, {}
    peak = 0
    for event in sorted(run['trace'], key=lambda e: (e['t'], e['event'] != 'service_finish')):
        key = (event['key']['session_id'], event['key']['sequence'])
        query = jobs[event['job_id']][1]
        if event['event'] == 'submitted':
            assert key not in submitted and event['t'] >= query['release_s']
            submitted[key] = event
            active[key] = event['estimated_work']
            peak = max(peak, len(active))
            assert len(active) <= run['admission'] <= 4
            assert sum(active.values()) <= 4 * max(q['estimated_work'] for q in run['queries'])
        elif event['event'] == 'service_start':
            assert key not in started and event['t'] >= submitted[key]['t']
            started[key] = event
        else:
            assert key in active and key not in completed
            assert event['t'] - started[key]['t'] == query['service_s']
            assert event['endpoint'] == submitted[key]['endpoint'] == started[key]['endpoint']
            completed[key] = event
            del active[key]
    assert not active and submitted.keys() == started.keys() == completed.keys()
    assert len(completed) == sum(q['rows'] for q in run['queries'])
    assert peak == run['peaks']['active_requests']
    for name in {e['endpoint'] for e in run['trace']}:
        intervals = sorted((started[k]['t'], completed[k]['t']) for k in started
                           if started[k]['endpoint'] == name)
        assert all(b[0] >= a[1] for a, b in zip(intervals, intervals[1:]))
    for sample in run['resources']:
        for usage in sample['jobs'].values():
            assert usage['held_tasks'] <= 16 and usage['input_bytes'] <= 1024 and usage['result_bytes'] <= 128
        assert all(sum(u[name] for u in sample['jobs'].values()) == sample[name]
                   for name in run['final_usage'])
    for label, row in run['jobs'].items():
        query = jobs[row['job_id']][1]
        finish = max(e['t'] for e in completed.values() if e['job_id'] == row['job_id'])
        assert row['rows'] == sum(e['job_id'] == row['job_id'] for e in completed.values()) == query['rows']
        assert row['finish_s'] == finish and row['jct_s'] == finish - query['release_s']
    assert all(v == 0 for v in run['final_usage'].values())


def summarize(evidence):
    trials = evidence['calibration'] + evidence['trials'] + evidence['solo']
    for run in trials:
        audit(run)
    solo = {(r['scenario'], label, r['resource_role']): row['jct_s']
            for r in evidence['solo'] for label, row in r['jobs'].items()}
    static = {(r['scenario'], r['repetition']): r for r in evidence['trials'] if r['arm'] == 'strong_static'}
    rows = []
    for run in evidence['trials']:
        control = static[run['scenario'], run['repetition']]
        rows.append(dict(scenario=run['scenario'], repetition=run['repetition'], arm=run['arm'],
            admission=run['admission'], group_jct_s=run['group_jct_s'],
            rows_per_simulated_s=run['rows_per_simulated_s'],
            jobs={label: dict(**row,
                full_solo_ratio=row['jct_s'] / solo[run['scenario'], label, 'full_solo'],
                reserve_solo_ratio=row['jct_s'] / solo[run['scenario'], label, 'reserve_solo'],
                static_multi_ratio=row['jct_s'] / control['jobs'][label]['jct_s'])
                for label, row in run['jobs'].items()}))
    outcomes = []
    for name in sorted({r['scenario'] for r in evidence['trials']}):
        first = {r['arm']: r for r in evidence['trials'] if r['scenario'] == name and r['repetition'] == 0}
        candidate = first['bounded_short']
        against = {}
        for arm in ('work_conserving', 'strong_static'):
            ref = first[arm]
            short_ratio = max(candidate['jobs'][label]['jct_s'] / row['jct_s']
                              for label, row in ref['jobs'].items() if label != 'long')
            long_ratio = candidate['jobs']['long']['jct_s'] / ref['jobs']['long']['jct_s']
            throughput_ratio = candidate['rows_per_simulated_s'] / ref['rows_per_simulated_s']
            against[arm] = dict(worst_short_jct_ratio=short_ratio, long_jct_ratio=long_ratio,
                                throughput_ratio=throughput_ratio,
                                passes=short_ratio <= .9 and long_ratio <= 1.05 and throughput_ratio >= .98)
        outcomes.append(dict(scenario=name, comparisons=against,
                             passes_all=all(r['passes'] for r in against.values())))
    return dict(evidence=evidence['evidence'], source_base=evidence['source_base'],
                source_sha256=evidence['source_sha256'], audited_runs=len(trials),
                synthetic_submissions=sum(sum(q['rows'] for q in r['queries']) for r in trials),
                model_calls=0, gpu_calls=0, http_calls=0, postgres_queries=0,
                outcomes=outcomes, rows=rows, exploratory_cells=len(evidence['exploratory_grid']),
                exploratory_bulk_limit_violations=[r for r in evidence['exploratory_grid']
                    if r['candidate_long'] > 1.05 * r['static_long']])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    with gzip.open(args.input, 'rt', encoding='utf-8') as stream:
        summary = summarize(json.load(stream))
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2)
    print(json.dumps(dict(audited_runs=summary['audited_runs'], synthetic_submissions=summary['synthetic_submissions'],
                          passes=[r['scenario'] for r in summary['outcomes'] if r['passes_all']])))


if __name__ == '__main__':
    main()
