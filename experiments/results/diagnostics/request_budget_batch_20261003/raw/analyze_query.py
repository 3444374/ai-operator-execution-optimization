"""Recheck restored query evidence without starting any service or request."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import statistics
import sys


def analyze(root, repository):
    sys.path.insert(0, str(repository/'code'))
    from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
    from src.execution_provider.semantic_map import SemanticMapPlan
    from src.experiments.postgresql.map_bindings import parse_pg_bindings, verify_bound_map_results
    from src.experiments.query_resources import verify_logical_resources
    from src.experiments.postgresql.query_evaluation import ray_transport_accounting

    def read(name):
        return json.loads((root/name).read_text())

    def lines(name):
        return [json.loads(line) for line in (root/name).read_text().splitlines()]

    manifest = read('source-manifest.json')
    for name, digest in manifest.items():
        if hashlib.sha256((repository/name).read_bytes()).hexdigest() != digest:
            raise ValueError('execution source differs: '+name)
    controller = read('o3/r/controller-summary.json')
    if (controller['status'] != 'passed' or controller['fixture_posts'] != 8224
            or len(controller['completed']) != 10 or read('cleanup-independent.json')['status'] != 'passed'):
        raise ValueError('query completion or independent cleanup differs')
    output = dict(source_base_commit='88173f85', source_files_verified=len(manifest),
                  fixture_requests=8224, model_requests=0, queries=[])
    plan = SemanticMapPlan(MOVIE_MAP_INSTRUCTION, 'fixture-model', 128)
    for query in controller['completed']:
        n, mode, unit = query['rows'], query['mode'], query['unit']
        base = 'o3/r/'+mode+str(n)+'/'+unit
        summary = read(base+'/summary.json')
        config = summary['config']
        if config['remote_budget_mode'] != mode or config['map_payload_backend'] != 'daft':
            raise ValueError('query mode differs')
        events = lines(base+'/events.jsonl')
        inputs = lines('o3/r/inputs-'+str(n)+'/raw.jsonl')
        results = lines(base+'/q0/results.jsonl')
        if hashlib.sha256((root/(base+'/q0/results.jsonl')).read_bytes()).hexdigest() != summary['execution']['results_sha256']:
            raise ValueError('result bytes differ')
        bindings = parse_pg_bindings((root/(base+'/q0-producer.log')).read_text().splitlines())
        association = verify_bound_map_results([(r[1],r[3]) for r in inputs], [r['row'] for r in results],
            bindings, events, lines(base+'/sessions.jsonl'), plan=plan)
        if [b.row_id for b in bindings] != [r['row'][0] for r in results]:
            raise ValueError('result order differs')
        guards = [e for e in events if e['event']=='remote_request_guard']
        if len(guards) != n or {g['key']['sequence'] for g in guards} != set(range(n)) or any(g['status']!='completed' for g in guards):
            raise ValueError('guard association differs')
        batches = [e for e in events if e['event']=='remote_request_batch']
        if mode == 'batched':
            attempts = []
            for batch in batches:
                if not 1 <= batch['rows'] <= 16 or batch['last_attempt']-batch['first_attempt']+1 != batch['rows']:
                    raise ValueError('transaction size differs')
                attempts.extend(range(batch['first_attempt'],batch['last_attempt']+1))
            if sorted(attempts) != sorted(g['attempt'] for g in guards) or len(set(attempts)) != n:
                raise ValueError('transaction and row attempts differ')
            reserve_seconds = sum(b['reserve_ns'] for b in batches)/1e9
            transactions = len(batches)
        else:
            if batches:
                raise ValueError('unexpected batch event in synchronous control')
            reserve_seconds = sum(g['reserve_ns'] for g in guards)/1e9
            transactions = n
        blocks = [e for e in events if e['event']=='core_ray_block_reserved']
        if sum(b['rows'] for b in blocks) != n:
            raise ValueError('Ray block row count differs')
        resources = summary['evaluation']['logical_resources']
        if resources['drained_jobs'] != 1 or any(value > resources['limits'][key] for key,value in resources['logical_peaks'].items()):
            raise ValueError('logical resource report differs')
        if verify_logical_resources(events,resources['limits'],expected_jobs=1) != resources:
            raise ValueError('resource transition reconstruction differs')
        if ray_transport_accounting(events,n) != summary['evaluation']['ray_transport']:
            raise ValueError('Ray object lifetime reconstruction differs')
        work = [e for e in events if e['event']=='core_ray_work']
        stages = {stage:[e for e in work if e['stage']==stage] for stage in ('payload_next','payload_close','object_put')}
        output['queries'].append(dict(mode=mode, unit=unit, rows=n, repeat=query['repeat'],
            matched_rows=association['matched_rows'], jct_seconds=query['jct_seconds'],
            preparation_seconds=query['preparation_seconds'], transactions=transactions,
            reserve_seconds=reserve_seconds, windows=len(stages['payload_close']),
            ray_blocks=len(blocks), batch_sizes=dict(Counter(b['rows'] for b in batches)),
            next_work_seconds=sum(e['work_ns'] for e in stages['payload_next'])/1e9,
            next_resume_seconds=sum(e['resume_ns'] for e in stages['payload_next'])/1e9,
            peak_http=query['peak_http'], logical_resources=resources))
    measured = [q for q in output['queries'] if q['repeat']>0]
    output['medians_seconds'] = {mode:statistics.median(q['jct_seconds'] for q in measured if q['mode']==mode)
                                 for mode in ('synchronous','batched')}
    output['query_reduction_percent'] = 100*(1-output['medians_seconds']['batched']/output['medians_seconds']['synchronous'])
    output['verified_rows'] = sum(q['matched_rows'] for q in output['queries'])
    if output['verified_rows'] != 8224:
        raise ValueError('verified input total differs')
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.evidence, args.repository)
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:result[k] for k in ('verified_rows','medians_seconds','query_reduction_percent')}))
