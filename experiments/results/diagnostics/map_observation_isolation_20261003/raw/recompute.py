"""Verify the retained archive and recompute only its primary local diagnosis."""
import argparse
import ast
import gzip
import hashlib
import inspect
import json
from pathlib import Path
import statistics
import sys
import tarfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[5]
    sys.path.insert(0, str(root / 'code'))
    from src.experiments.map_observation_probe import analyze, _distribution

    raw = Path(__file__).parent
    storage = json.loads((raw / 'storage-manifest.json').read_text())
    archive = raw / 'evidence.tar.gz'
    if hashlib.sha256(archive.read_bytes()).hexdigest() != storage['archive_sha256']:
        raise AssertionError('archive digest differs')
    with tarfile.open(archive, 'r:gz') as tar:
        def data(name):
            with tar.extractfile(name) as stream:
                return stream.read()
        if set(tar.getnames()) != {item['member'] for item in storage['members']}:
            raise AssertionError('archive member history differs')
        for item in storage['members']:
            value = data(item['member'])
            if len(value) != item['bytes'] or hashlib.sha256(value).hexdigest() != item['sha256']:
                raise AssertionError('archive member digest differs')
        source = ast.parse(data('runs/run-03/probe-source.py').decode())
        # Later changes only corrected final failure accounting. Analyze the same code.
        for function in (analyze, _distribution):
            original = next(node for node in source.body if isinstance(node, ast.FunctionDef)
                            and node.name == function.__name__)
            current = ast.parse(inspect.getsource(function)).body[0]
            if ast.dump(original) != ast.dump(current):
                raise AssertionError('analysis implementation differs from measured source')
        summary = json.loads(data('runs/run-03/summary.json'))
        if summary['status'] != 'passed':
            raise AssertionError('primary suite did not pass')
        runs = []
        for run in summary['runs']:
            body = gzip.decompress(data(f"runs/run-03/{run['run_id']}/events.jsonl.gz"))
            events = [json.loads(line) for line in body.splitlines()]
            recomputed = analyze(events, run['rows'], run['query_started_ns'],
                                 run['query_completed_ns'], run['synthetic_worker_peak'])
            if any(value != run[key] for key, value in recomputed.items()):
                raise AssertionError('raw event recomputation differs')
            runs.append({key: run[key] for key in ('run_id', 'phase', 'repeat', 'accounting', 'recording',
                        'rows', 'query_seconds', 'case_seconds', 'teardown_seconds', 'first_result_seconds',
                        'preparation_windows', 'object_batches', 'object_peak_bytes',
                        'core_active_mean', 'synthetic_worker_active_mean',
                        'projection_seconds', 'writer_call_seconds', 'submitted_event_after_rpc_rows')} |
                        dict(reserve_seconds=run['guard_seconds']['reserve_ns'],
                             submitted_to_rpc_median_ms=run['submitted_to_rpc']['median_ms'],
                             synthetic_rpc_submit_seconds=run['rpc_submit']['sum_seconds']))
        arms = {}
        for accounting, recording in (('durable', 'memory'), ('memory', 'memory'),
                                      ('durable', 'buffered'), ('memory', 'buffered')):
            measured = [run for run in runs if run['phase'] == 'measurement'
                        and (run['accounting'], run['recording']) == (accounting, recording)]
            arms[f'{accounting}_{recording}'] = {
                key: statistics.median(run[key] for run in measured)
                for key in ('query_seconds', 'case_seconds', 'reserve_seconds', 'submitted_to_rpc_median_ms',
                            'core_active_mean', 'synthetic_worker_active_mean', 'preparation_windows',
                            'object_batches', 'projection_seconds', 'writer_call_seconds')}
        comparisons = {}
        for recording in ('memory', 'buffered'):
            comparisons[recording] = dict(
                query_reduction_percent=100 * (1 - arms[f'memory_{recording}']['query_seconds'] /
                                                   arms[f'durable_{recording}']['query_seconds']),
                paired_query_reduction_percent=[100 * (1 - next(run['query_seconds'] for run in runs
                    if run['phase'] == 'measurement' and run['repeat'] == repeat
                    and (run['accounting'], run['recording']) == ('memory', recording)) /
                    next(run['query_seconds'] for run in runs if run['phase'] == 'measurement'
                    and run['repeat'] == repeat and (run['accounting'], run['recording']) == ('durable', recording)))
                    for repeat in (1, 2, 3)])
        failed_counts = {}
        for identity in ('preparation-01', 'preparation-02'):
            events = [json.loads(line) for line in gzip.decompress(
                data(f'preparation/{identity}/events.jsonl.gz')).splitlines()]
            failed_counts[identity] = dict(
                initially_reported_calls=json.loads(data(f'preparation/{identity}/result.json'))['synthetic_calls'],
                completed_synthetic_calls=sum(e['event'] == 'ray_http_completed' for e in events),
                observed_guard_calls=sum(e['event'] == 'fixture_request_observed' for e in events))
        result = dict(schema='semloom.map_observation_analysis.v1', primary_run='run-03',
                      raw_recomputed_runs=len(runs), synthetic_calls=summary['synthetic_calls_completed'],
                      http_requests=0, model_requests=0, pg_queries=0,
                      medians=arms, comparisons=comparisons, runs=runs, failure_count_correction=failed_counts)
    with args.output.open('x') as stream:
        stream.write(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(raw_recomputed_runs=len(runs), archive_members=len(storage['members']),
                          comparisons=comparisons), sort_keys=True))


if __name__ == '__main__':
    main()
