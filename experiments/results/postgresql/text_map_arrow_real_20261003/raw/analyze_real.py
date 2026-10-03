"""Recompute the real-model comparison from retained evidence and private inputs."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import statistics
import sys


def analyze(root, inputs_root, repository):
    sys.path.insert(0, str(repository/'code'))
    from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
    from src.execution_provider.semantic_map import SemanticMapPlan
    from src.experiments.postgresql.map_bindings import parse_pg_bindings, verify_bound_map_results
    from src.experiments.postgresql.query_workloads import load_manifest, read_prepared
    from src.experiments.postgresql.map_direct import request_body
    from src.baselines.common.private_artifacts import content_digest
    from src.experiments.query_resources import verify_logical_resources
    from src.experiments.postgresql.query_evaluation import ray_transport_accounting
    from src.experiments.postgresql.window_memory import verify_window_memory

    def read(name):
        return json.loads((root/name).read_text())

    def lines(name):
        return [json.loads(s) for s in (root/name).read_text().splitlines()]

    source = read('source.json')
    for name, digest in source.items():
        if hashlib.sha256((repository/name).read_bytes()).hexdigest() != digest:
            raise ValueError('execution source differs: '+name)
    owner, worker = read('real01/controller-summary.json'), read('real01/worker/summary.json')
    if (owner['status'] != 'passed' or worker['status'] != 'passed'
            or any(owner[k] != 8224 for k in ('actual_posts','server_access_posts','server_success_delta'))
            or read('cleanup-independent.json')['status'] != 'passed'):
        raise ValueError('run count or cleanup differs')
    inputs, references = {}, {}
    declared = read('inputs-record.json')['inputs']
    for split in ('qualification','evaluation'):
        manifest = inputs_root/split/'manifest.json'
        if load_manifest(manifest)['sha256'] != declared[split]['manifest_sha256']:
            raise ValueError('prepared input identity differs')
        inputs[split] = list(read_prepared(manifest,'raw.jsonl'))
        references[split] = {r['row_id']:r['reference'] for r in read_prepared(manifest,'references.jsonl')}
    plan = SemanticMapPlan(MOVIE_MAP_INSTRUCTION,'Qwen2.5-7B-Instruct',128)
    result = dict(real_model_requests=8224, verified_rows=0, source_files_verified=len(source),
                  owner_seconds=owner['elapsed_seconds'], worker_seconds=worker['elapsed_seconds'], queries=[])
    predictions = {}
    for item in worker['completed']:
        n, mode, split, unit = item['rows'], item['mode'], item['split'], item['unit']
        base = 'real01/worker/'+mode+str(n)+'/'+unit
        summary = read(base+'/summary.json')
        cfg = summary['config']
        if cfg['remote_budget_mode'] != 'synchronous' or cfg['map_payload_backend'] != mode:
            raise ValueError('query mode differs')
        events, sessions = lines(base+'/events.jsonl'), lines(base+'/sessions.jsonl')
        raw = (root/(base+'/q0/results.jsonl')).read_bytes()
        if hashlib.sha256(raw).hexdigest() != summary['execution']['results_sha256']:
            raise ValueError('result bytes differ')
        output = [json.loads(s)['row'] for s in raw.splitlines()]
        bindings = parse_pg_bindings((root/(base+'/q0-producer.log')).read_text().splitlines())
        association = verify_bound_map_results([(r[1],r[3]) for r in inputs[split]], output,
                                              bindings, events, sessions, plan=plan)
        if [b.row_id for b in bindings] != [r[0] for r in output]:
            raise ValueError('result order differs')
        guards = [e for e in events if e['event']=='remote_request_guard']
        if len(guards) != n or {g['key']['sequence'] for g in guards} != set(range(n)) or any(g['status']!='completed' for g in guards):
            raise ValueError('pre-send guard association differs')
        batches = [e for e in events if e['event']=='remote_request_batch']
        if mode == 'batched':
            numbers = []
            for batch in batches:
                if not 1 <= batch['rows'] <= 16 or batch['last_attempt']-batch['first_attempt']+1 != batch['rows']:
                    raise ValueError('transaction size differs')
                numbers.extend(range(batch['first_attempt'],batch['last_attempt']+1))
            if sorted(numbers) != sorted(g['attempt'] for g in guards) or len(set(numbers)) != n:
                raise ValueError('transaction and guard IDs differ')
            transactions, reserved = len(batches), sum(b['reserve_ns'] for b in batches)/1e9
        else:
            if batches:
                raise ValueError('synchronous arm contains batched accounting')
            transactions, reserved = n, sum(g['reserve_ns'] for g in guards)/1e9
        metrics = item['service_after']
        if (item['model_success_delta'] != n or metrics['vllm:request_success_total']-item['service_before']['vllm:request_success_total'] != n
                or metrics['vllm:num_requests_running'] != 0 or metrics['vllm:num_requests_waiting'] != 0):
            raise ValueError('per-query model counts differ')
        resources = summary['evaluation']['logical_resources']
        if resources['drained_jobs'] != 1 or any(v > resources['limits'][k] for k,v in resources['logical_peaks'].items()):
            raise ValueError('resource peak exceeds declared allowance')
        limits = dict(held_tasks=256,input_bytes=134217728,result_bytes=134217728,active_requests=64,active_work=64)
        if verify_logical_resources(events,limits,expected_jobs=1) != resources:
            raise ValueError('resource transition reconstruction differs')
        if ray_transport_accounting(events,n) != summary['evaluation']['ray_transport']:
            raise ValueError('Ray object lifetime reconstruction differs')
        memory = verify_window_memory((root/(base+'/q0-producer.log')).read_text().splitlines(),
            backend_pid=read(base+'/pg-backend.json')['backend_pid'],retained_limit=268435456,
            staging_limit=4194304,window=256)
        if memory != summary['evaluation']['pg_memory']:
            raise ValueError('PG memory reconstruction differs')
        sent = [content_digest(e['body']) if 'body' in e else e['request_values_sha256']
                for e in events if e['event']=='request']
        if Counter(sent) != Counter(content_digest(request_body(plan,r[3])) for r in inputs[split]):
            raise ValueError('complete request multiset differs from input')
        work = [e for e in events if e['event']=='core_ray_work']
        next_work = [e for e in work if e['stage']=='payload_next']
        blocks = [e for e in events if e['event']=='core_ray_block_reserved']
        if sum(b['rows'] for b in blocks) != n:
            raise ValueError('Ray block row count differs')
        values = dict(output)
        if len(values) != n or any(v not in ('POSITIVE','NEGATIVE') for v in values.values()):
            raise ValueError('invalid or repeated output')
        correct = sum(values[k] == references[split][k] for k in values)
        if correct/n != item['accuracy']:
            raise ValueError('accuracy differs from row recomputation')
        predictions[(mode,split,item['repeat'])] = values
        result['verified_rows'] += association['matched_rows']
        result['queries'].append(dict(mode=mode,unit=unit,split=split,repeat=item['repeat'],rows=n,
            jct_seconds=item['jct_seconds'],preparation_seconds=item['preparation_seconds'],
            accuracy=correct/n,correct_rows=correct,transactions=transactions,reserve_seconds=reserved,
            windows=sum(e['stage']=='payload_close' for e in work),ray_blocks=len(blocks),
            batch_sizes=dict(Counter(b['rows'] for b in batches)),
            next_work_seconds=sum(e['work_ns'] for e in next_work)/1e9,
            next_resume_seconds=sum(e['resume_ns'] for e in next_work)/1e9,logical_resources=resources))
    if len(result['queries']) != 10 or result['verified_rows'] != 8224:
        raise ValueError('query or verified row count differs')
    result['medians_seconds'] = {m:statistics.median(q['jct_seconds'] for q in result['queries'] if q['mode']==m and q['repeat']>0)
                                 for m in ('daft','arrow')}
    result['slowdown_percent'] = 100*(result['medians_seconds']['arrow']/result['medians_seconds']['daft']-1)
    result['paired_output_differences'] = []
    for repeat in (1,2,3):
        left,right = (predictions[(m,'evaluation',repeat)] for m in ('daft','arrow'))
        result['paired_output_differences'].append(sum(left[k]!=right[k] for k in left))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument('--inputs',type=Path,required=True)
    parser.add_argument('--repository',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    result = analyze(args.evidence,args.inputs,args.repository)
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:result[k] for k in ('verified_rows','medians_seconds','slowdown_percent','paired_output_differences')}))
