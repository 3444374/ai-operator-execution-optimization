"""Reuse the row/resource audit and add candidate, timing and durable checks."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import statistics


def read(path):
    return json.loads(path.read_text())


def lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def analyze(root, inputs, repository, base_analysis, ledger=None):
    spec = importlib.util.spec_from_file_location('retained_map_audit', base_analysis)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module.analyze(root, inputs, repository)
    result.pop('source_commit')
    result['source_base_commit'] = '4e9734be99e383ca00aab376e0546660de7a6cb2'
    result['source_identity'] = 'declared per-file SHA-256 snapshot plus optional queued-tail change'
    result['base_analysis_sha256'] = hashlib.sha256(base_analysis.read_bytes()).hexdigest()
    from src.baselines.common.private_artifacts import content_digest
    from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
    from src.execution_provider.semantic_map import SemanticMapPlan
    from src.experiments.postgresql.map_direct import request_body
    from src.experiments.postgresql.query_workloads import read_prepared
    input_rows = {split:list(read_prepared(inputs/split/'manifest.json','raw.jsonl'))
                  for split in ('qualification','evaluation')}
    plan = SemanticMapPlan(MOVIE_MAP_INSTRUCTION,'Qwen2.5-7B-Instruct',128)
    owner = read(root/'real01/controller-summary.json')
    if owner['max_seconds'] != 1200 or owner['elapsed_seconds'] > 1200:
        raise ValueError('authorized duration differs')
    worker = read(root/'real01/worker/summary.json')
    if not worker['worker_cpu_slots_released'] or not worker['ray_driver_disconnected']:
        raise ValueError('worker lifecycle did not settle')
    guards_by_attempt, outgoing_by_attempt, grouped_attempts = {}, {}, {}
    for query in result['queries']:
        mode, rows, unit = query['mode'], query['rows'], query['unit']
        group = mode+str(rows)
        directory = root/'real01/worker'/group/unit
        physical = read(directory/'map-transport.json')
        expected_preparation = dict(encoded_bytes=2097152, ready_bytes=4194304,
            ready_work=256, prepare_inflight=1, model_inflight=256)
        if (physical['coalesce_queued_preparation'] is not (mode == 'prepared')
                or physical['preparation'] != (expected_preparation if mode == 'prepared' else None)
                or physical['workers'] != 1 or physical['batch_rows'] != 16
                or physical['window_bytes'] != 2097152 or physical['object_bytes'] != 4194304
                or physical['payload_backend'] != 'daft'):
            raise ValueError('resolved preparation candidate differs')
        summary = read(directory/'summary.json')
        timing = summary['execution']
        recomputed = dict(jct_seconds=(timing['t_query_terminal_ns']-summary['query_preparation_started_ns'])/1e9,
            preparation_seconds=(timing['t_release_ns']-summary['query_preparation_started_ns'])/1e9,
            first_row_seconds=(timing['t_first_row_ns']-timing['t_release_ns'])/1e9,
            cleanup_seconds=(summary['t_execution_cleanup_ns']-timing['t_query_terminal_ns'])/1e9)
        if any(query[name] != value for name, value in recomputed.items()):
            raise ValueError('query timing differs from independent timestamps')
        events = lines(directory/'events.jsonl')
        starts = {(e['key']['session_id'],e['key']['sequence']): e for e in events if e['event']=='core_ray_http_completed'}
        requests = [e for e in events if e['event']=='request']
        guards = [e for e in events if e['event']=='remote_request_guard']
        if len(requests) != rows or len(guards) != rows:
            raise ValueError('request/guard count differs')
        group_attempts = grouped_attempts.setdefault(group, set())
        # This synchronous recorder omits attempt from the guard. The request
        # and its keyed guard execute without an await on one event loop.
        paired, pending = [], None
        for event in events:
            if event['event'] == 'request':
                if pending is not None:
                    raise ValueError('overlapping synchronous request records')
                pending = event
            elif event['event'] == 'remote_request_guard':
                if pending is None or pending['monotonic_ns'] > event['monotonic_ns']:
                    raise ValueError('guard lacks its preceding request observation')
                paired.append((event,pending))
                pending = None
        if pending is not None or len(paired) != rows:
            raise ValueError('unpaired synchronous request')
        for guard, outgoing in paired:
            attempt = outgoing['attempt']
            key = (guard['key']['session_id'], guard['key']['sequence'])
            if attempt in guards_by_attempt or guard['monotonic_ns'] > starts[key]['rpc_started_ns']:
                raise ValueError('duplicate guard or RPC before durable guard returned')
            expected = content_digest(request_body(plan,input_rows[query['split']][key[1]][3]))
            if outgoing['request_values_sha256'] != expected:
                raise ValueError('guarded request body differs from its independently bound row')
            guards_by_attempt[attempt] = guard
            group_attempts.add(attempt)
        for event in requests:
            attempt = event['attempt']
            if attempt in outgoing_by_attempt:
                raise ValueError('duplicate outgoing attempt')
            outgoing_by_attempt[attempt] = event
        preparations = [e for e in events if e['event']=='core_ray_preparation_completed']
        if preparations:
            query['preparation_capacity_peaks'] = {
                name:max(e['preparation']['stages'][name] for e in preparations)
                for name in preparations[0]['preparation']['stages']}
        blocks = [e for e in events if e['event']=='core_ray_block_reserved']
        completions = list(starts.values())
        if any(e.get('shared_clock') is not True for e in completions):
            raise ValueError('RPC and worker observations lack a shared clock')
        query['ray_block_sizes'] = dict(Counter(e['rows'] for e in blocks))
        query['rpc_stage_totals_seconds'] = {
            name:sum(e[name] for e in completions)/1e9 for name in
            ('submit_elapsed_ns','before_worker_ns','worker_elapsed_ns','after_worker_ns','rpc_elapsed_ns')}
        query['rpc_stage_per_request_ms'] = {}
        for name in query['rpc_stage_totals_seconds']:
            values = [e[name]/1e6 for e in completions]
            query['rpc_stage_per_request_ms'][name] = dict(mean=statistics.mean(values),
                p50=statistics.median(values),p95=statistics.quantiles(values,n=100,method='inclusive')[94])
        query['preparation_remainder_seconds'] = sum(e['elapsed_ns']-e['payload_ns']-e['put_ns'] for e in preparations)/1e9 if preparations else None
        query['stream_seconds'] = (timing['t_query_terminal_ns']-timing['t_release_ns'])/1e9
        query['synchronous_guard_return_before_rpc'] = True
        query['guard_request_association'] = 'one preceding synchronous request per keyed guard; body checked against producer input order'
    if set(guards_by_attempt) != set(range(1,8225)) or set(outgoing_by_attempt) != set(guards_by_attempt):
        raise ValueError('global attempt coverage differs')
    if ledger is None:
        result['durable_ledger'] = dict(status='unavailable', reason='private SQLite not provided',
            public_guards=8224, public_outgoing_records=8224)
    else:
        with sqlite3.connect(ledger.as_uri()+'?mode=ro', uri=True) as db:
            if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise ValueError('private SQLite integrity differs')
            budget = db.execute('SELECT budget_id,limit_count,allocated FROM budget').fetchone()
            if budget != ('map-queued-tail-real-20261008',8224,8224):
                raise ValueError('private budget identity differs')
            units = db.execute('SELECT unit_id,first_attempt,requests,claimed FROM units').fetchall()
            closed = {r[0] for r in db.execute('SELECT unit_id FROM closed_shared_units')}
            if closed != set(grouped_attempts) or len(units) != 4:
                raise ValueError('budget groups not closed exactly once')
            durable = {}
            for name, first, count, claimed in units:
                if claimed != 1 or grouped_attempts[name] != set(range(first,first+count)):
                    raise ValueError('private unit association differs')
                records = db.execute('SELECT sequence,request_sha256 FROM shared_requests WHERE unit_id=? ORDER BY sequence',(name,)).fetchall()
                if [r[0] for r in records] != list(range(1,count+1)):
                    raise ValueError('private unit sequence differs')
                for sequence, digest in records:
                    attempt = first+sequence-1
                    if outgoing_by_attempt[attempt]['request_bytes_sha256'] != digest:
                        raise ValueError('private durable payload differs from outgoing request')
                    durable[attempt] = digest
            if len(durable) != 8224:
                raise ValueError('private request count differs')
        result['durable_ledger'] = dict(status='passed', requests=len(durable), closed_groups=len(closed),
            sha256=hashlib.sha256(ledger.read_bytes()).hexdigest())
    result['measurement_summary'] = {}
    for mode in ('immediate','prepared'):
        queries = [q for q in result['queries'] if q['mode']==mode and q['repeat']>0]
        times = [q['jct_seconds'] for q in queries]
        metrics = ('jct_seconds','preparation_seconds','stream_seconds','first_row_seconds',
            'prepared_blocks','ray_blocks','payload_prepare_seconds','prepared_put_seconds','next_work_seconds','reserve_seconds')
        entry = {name:statistics.median(q[name] for q in queries) if all(q[name] is not None for q in queries) else None
                 for name in metrics}
        entry.update(jct_repeats_seconds=times, sample_cv=statistics.stdev(times)/statistics.mean(times),
            mean_worker_methods=statistics.median(q['timeline']['mean_worker_methods'] for q in queries),
            mean_logical_requests=statistics.median(q['timeline']['mean_logical_requests'] for q in queries),
            query_pss_peak_median_bytes=statistics.median(q['physical_memory']['sampled_total_pss_peak_bytes'] for q in queries),
            model_host_pss_peak_median_bytes=statistics.median(q['timeline']['model_host_pss_peak_bytes'] for q in queries))
        entry['rpc_stage_totals_medians_seconds'] = {
            name:statistics.median(q['rpc_stage_totals_seconds'][name] for q in queries)
            for name in queries[0]['rpc_stage_totals_seconds']}
        entry['rpc_stage_per_request_mean_medians_ms'] = {
            name:statistics.median(q['rpc_stage_per_request_ms'][name]['mean'] for q in queries)
            for name in queries[0]['rpc_stage_totals_seconds']}
        result['measurement_summary'][mode] = entry
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument('--inputs',type=Path,required=True)
    parser.add_argument('--repository',type=Path,required=True)
    parser.add_argument('--base-analysis',type=Path)
    parser.add_argument('--ledger',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    base_analysis = args.base_analysis or (Path(__file__).resolve().parents[5]/
        'experiments/results/postgresql/text_map_preparation_validation_20261004/raw/analyze_real.py')
    result = analyze(args.evidence,args.inputs,args.repository,base_analysis,args.ledger)
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:result[k] for k in ('verified_rows','medians_seconds','slowdown_percent','paired_output_differences','durable_ledger')}))
