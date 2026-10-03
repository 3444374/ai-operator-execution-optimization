"""Read-only validation and aggregation of local component and Core controls."""
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
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    raw = Path(__file__).parent
    sys.path.insert(0, str(Path(__file__).resolve().parents[5] / 'code'))
    from src.experiments.map_observation_probe import analyze, _distribution
    manifest = json.loads((raw / 'storage-manifest.json').read_text())
    archive = raw / 'evidence.tar.gz'
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == manifest['archive_sha256']
    with tarfile.open(archive, 'r:gz') as tar:
        def data(name):
            with tar.extractfile(name) as stream:
                return stream.read()
        assert set(tar.getnames()) == {x['member'] for x in manifest['members']}
        for record in manifest['members']:
            value = data(record['member'])
            assert len(value) == record['bytes'] and hashlib.sha256(value).hexdigest() == record['sha256']
        component = json.loads(data('component/summary.json'))
        core = json.loads(data('core/summary.json'))
        assert component['status'] == core['status'] == 'passed'
        for run in component['runs']:
            events = [json.loads(line) for line in gzip.decompress(
                data(f"component/{run['run_id']}/grants.jsonl.gz")).splitlines()]
            assert sorted(e['sequence'] for e in events) == list(range(1024))
            assert sorted(e['attempt'] for e in events) == list(range(1, 1025))
            assert all(e['request_sha256'] == hashlib.sha256(
                f"fixture-input-{e['sequence'] % 127}".encode()).hexdigest() for e in events)
            assert run['workers_stopped'] and all(x == 0 for x in run['worker_exits'])
        source = ast.parse(data('core/source/map_observation_probe.py').decode())
        for function in (analyze, _distribution):
            original = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == function.__name__)
            assert ast.dump(original) == ast.dump(ast.parse(inspect.getsource(function)).body[0])
        for run in core['runs']:
            events = [json.loads(line) for line in gzip.decompress(
                data(f"core/{run['run_id']}/events.jsonl.gz")).splitlines()]
            values = analyze(events, run['rows'], run['query_started_ns'], run['query_completed_ns'],
                             run['synthetic_worker_peak'])
            assert all(value == run[key] for key, value in values.items())
            assert run['transport_drained'] and run['synthetic_worker_stopped']
            if run['accounting'] == 'mapped':
                assert run['mapped_handles_closed']
        component_medians = {}
        for mode in ('durable', 'mapped'):
            for workers in (1, 4):
                runs = [r for r in component['runs'] if r['mode'] == mode and r['workers'] == workers and not r['warmup']]
                component_medians[f'{mode}_{workers}'] = {key: statistics.median(r[key] for r in runs)
                    for key in ('grant_seconds', 'preparation_seconds', 'component_total_seconds')}
        core_medians = {}
        for mode in ('durable', 'mapped'):
            runs = [r for r in core['runs'] if r['accounting'] == mode and r['phase'] == 'measurement']
            core_medians[mode] = {key: statistics.median(r[key] for r in runs) for key in
                ('query_seconds', 'accounting_setup_seconds', 'case_seconds', 'preparation_windows',
                 'object_batches', 'core_active_mean', 'synthetic_worker_active_mean')}
            core_medians[mode]['with_accounting_setup_seconds'] = statistics.median(
                r['case_seconds'] + r['accounting_setup_seconds'] for r in runs)
        result = dict(schema='semloom.mapped_budget_analysis.v1', component_recomputed_runs=16,
            component_grants=sum(r['received_grants'] for r in component['runs']), core_recomputed_runs=10,
            synthetic_core_calls=sum(r['synthetic_calls'] for r in core['runs']), http_requests=0, model_requests=0,
            component_medians=component_medians, core_medians=core_medians,
            core_query_reduction_percent=100 * (1 - core_medians['mapped']['query_seconds'] / core_medians['durable']['query_seconds']),
            core_with_setup_reduction_percent=100 * (1 - core_medians['mapped']['with_accounting_setup_seconds'] /
                                                    core_medians['durable']['with_accounting_setup_seconds']),
            component_runs=component['runs'], core_runs=[{key: r[key] for key in
                ('run_id', 'phase', 'repeat', 'accounting', 'query_seconds', 'accounting_setup_seconds',
                 'case_seconds', 'preparation_windows', 'object_batches', 'core_active_mean', 'synthetic_worker_active_mean')}
                 for r in core['runs']])
    with args.output.open('x') as stream:
        stream.write(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: result[key] for key in ('component_grants', 'synthetic_core_calls',
        'core_query_reduction_percent', 'core_with_setup_reduction_percent')}))


if __name__ == '__main__':
    main()
