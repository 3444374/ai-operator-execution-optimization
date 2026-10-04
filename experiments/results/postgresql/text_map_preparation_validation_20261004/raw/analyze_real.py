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
    model_samples=lines('real01/model-rss.jsonl')
    for item in worker['completed']:
        n, mode, split, unit = item['rows'], item['mode'], item['split'], item['unit']
        base = 'real01/worker/'+mode+str(n)+'/'+unit
        summary = read(base+'/summary.json')
        cfg = summary['config']
        if cfg['remote_budget_mode'] != 'synchronous' or cfg['map_payload_backend'] != 'daft':
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
        preparations = [e for e in events if e['event']=='core_ray_preparation_completed']
        if mode == 'prepared':
            if not preparations or any(e['failed'] for e in preparations) or sum(e['rows'] for e in preparations) != n:
                raise ValueError('prepared block outcomes or rows differ')
            for e in preparations:
                stages=e['preparation']['stages']
                if (stages['encoded_held_bytes'] > 2097152 or stages['ready_held_bytes'] > 4194304
                        or stages['ready_held_work'] > 256 or stages['prepare_inflight'] > 1):
                    raise ValueError('preparation stages exceed declared limits')
        elif preparations:
            raise ValueError('immediate arm emitted preparation')
        memory=summary['resources']['processes']
        raw_samples=lines(base+'/query-rss.jsonl')
        if memory['pss_unavailable'] or not raw_samples or any(not s['pss_complete'] for s in raw_samples):
            raise ValueError('physical PSS sampling is incomplete')
        peak=max(s['summed_pss_bytes'] for s in raw_samples)
        if peak!=memory['sampled_total_pss_peak_bytes']:
            raise ValueError('physical PSS peak reconstruction differs')
        completions=[e for e in events if e['event']=='core_ray_http_completed']
        first=min(e['rpc_started_ns'] for e in completions);last=max(e['received_ns'] for e in completions)
        usage=sorted((e for e in events if isinstance(e.get('usage'),dict) and 'active_requests' in e['usage']),key=lambda e:e['monotonic_ns'])
        at,value,area=first,0,0
        for e in usage:
            t=e['monotonic_ns']
            if t<first:
                value=e['usage']['active_requests'];continue
            if t>last:break
            area+=(t-at)*value;at=t;value=e['usage']['active_requests']
        area+=(last-at)*value
        observed_model=[v for v in model_samples if summary['query_preparation_started_ns']<=v['monotonic_ns']<=summary['execution']['t_query_terminal_ns']]
        if not observed_model or any(not v['pss_complete'] for v in observed_model):
            raise ValueError('model PSS sampling unavailable for query')
        timeline=dict(first_rpc_ns=first,last_received_ns=last,
            mean_logical_requests=area/(last-first),
            mean_worker_methods=sum(e['worker_ended_ns']-e['worker_started_ns'] for e in completions)/(last-first),
            model_host_pss_peak_bytes=max(v['summed_pss_bytes'] for v in observed_model),
            model_samples=len(observed_model),
            scope='same-host method interval area, not GPU occupancy; model PSS is host memory after service readiness')
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
            first_row_seconds=item['first_row_seconds'],cleanup_seconds=item['cleanup_seconds'],
            timeline=timeline,physical_memory=memory,prepared_blocks=len(preparations),
            prepared_batch_sizes=dict(Counter(e['rows'] for e in preparations)),
            payload_prepare_seconds=sum(e['payload_ns'] for e in preparations)/1e9 if preparations else None,
            prepared_put_seconds=sum(e['put_ns'] for e in preparations)/1e9 if preparations else None,
            windows=sum(e['stage']=='payload_close' for e in work),ray_blocks=len(blocks),
            batch_sizes=dict(Counter(b['rows'] for b in batches)),
            next_work_seconds=sum(e['work_ns'] for e in next_work)/1e9,
            next_resume_seconds=sum(e['resume_ns'] for e in next_work)/1e9,logical_resources=resources))
    if len(result['queries']) != 10 or result['verified_rows'] != 8224:
        raise ValueError('query or verified row count differs')
    result['medians_seconds'] = {m:statistics.median(q['jct_seconds'] for q in result['queries'] if q['mode']==m and q['repeat']>0)
                                 for m in ('immediate','prepared')}
    result['slowdown_percent'] = 100*(result['medians_seconds']['prepared']/result['medians_seconds']['immediate']-1)
    result['paired_output_differences'] = []
    for repeat in (1,2,3):
        left,right = (predictions[(m,'evaluation',repeat)] for m in ('immediate','prepared'))
        result['paired_output_differences'].append(sum(left[k]!=right[k] for k in left))
    result['source_commit']='12ec50de52d392a016c35faf54247b602cc15c09'
    result['shared_startup']={key:value for key,value in worker.items() if key.endswith('startup_seconds')}
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
