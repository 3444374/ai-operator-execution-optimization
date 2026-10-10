"""Offline analysis of retained query files or an immutable query archive."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
import tarfile

p = argparse.ArgumentParser()
p.add_argument('evidence', type=Path)
p.add_argument('--code', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--ordinals', default='2,3,4,5,6')
p.add_argument('--reference-variant', help='Compare a named same-supplier variant when this run has no native arm')
args = p.parse_args()
sys.path.insert(0, str(args.code))
from src.baselines.common.private_artifacts import content_digest
from src.experiments.postgresql.native_adapter_metrics import sample_distribution

archive = args.evidence.is_file()
members = {}
selected_ordinals = {int(v) for v in args.ordinals.split(',')}
if archive:
    total = 0
    with tarfile.open(args.evidence, 'r|gz') as stream:
        for item in stream:
            assert item.isfile() and not Path(item.name).is_absolute() and '..' not in Path(item.name).parts
            if '/query-' in item.name and not item.name.endswith('/summary.json'):
                ordinal = int(item.name.split('/query-', 1)[1].split('/', 1)[0])
                if ordinal not in selected_ordinals:
                    continue
            if (item.name in ('owner-exit.json', 'workflow-manifest.json')
                    or item.name.endswith(('/startup.json', '/group-summary.json', '/summary.json',
                        '/http-trace.jsonl', '/protocols.jsonl', '/method-events.jsonl',
                        '/persistent-query.json', '/sema-service.jsonl'))):
                total += item.size
                assert total <= 512 * 2**20, 'short analysis memory allowance exceeded'
                members[item.name] = stream.extractfile(item).read()
def exists(name):
    return name in members if archive else (args.evidence / name).is_file()
def raw(name):
    if archive:
        return members[name]
    return (args.evidence / name).read_bytes()
def read(name):
    return json.loads(raw(name))
def lines(name):
    return [json.loads(line) for line in raw(name).splitlines()]
def distribution(values, *, preserve=False):
    result = sample_distribution(values, unit='seconds')
    if values:
        result.update(mean=statistics.mean(values), median=statistics.median(values))
    if not preserve:
        result.pop('samples', None)
    return result

owner = read('owner-exit.json')
assert owner['status'] == 'passed', 'failed owner must remain separately reported'
manifest = read('workflow-manifest.json')
ordinals = tuple(int(v) for v in args.ordinals.split(','))
all_queries = []
for cell in manifest['cells']:
    for ordinal in range(manifest.get('queries_per_cell', 7)):
        summary = read(cell['name'] + '/query-' + str(ordinal) + '/summary.json')
        assert summary['status'] == 'passed' and not summary['errors']
        assert summary['preparation_model_posts'] == 0
        assert summary['actual_posts'] == summary['rows'] == (8 if ordinal == 0 else 512)
        all_queries.append(dict(cell=cell['name'], query=ordinal,
            phase='qualification' if ordinal == 0 else 'warmup' if ordinal == 1 else 'measurement',
            rows=summary['rows'], posts=summary['actual_posts'], full_seconds=summary['full_query_seconds'],
            ready_seconds=summary['execution']['ready_query_seconds']))
assert len(all_queries) == manifest['queries'] and sum(q['posts'] for q in all_queries) == manifest['max_posts']
records, bodies, actual_values, comparisons = [], {}, {}, []
for cell in manifest['cells']:
    startup = read(cell['name'] + '/startup.json')
    assert startup['ready_ns'] >= startup['started_ns']
    http, call_e2e, rpc_before, worker, rpc_after, service = [], [], [], [], [], []
    method_stages = defaultdict(list)
    sema_stages = defaultdict(list)
    http_stages = defaultdict(list)
    phases = defaultdict(list)
    samples, lifecycles = [], []
    for ordinal in ordinals:
        prefix = cell['name'] + '/query-' + str(ordinal) + '/'
        value = read(prefix + 'summary.json')
        execution = value['execution']
        rows = value['rows']
        assert value['status'] == 'passed' and rows == value['actual_posts'] == manifest['query_rows']
        assert not execution['ready_timing_errors']
        assert math.isclose(execution['ready_query_seconds'],
            (execution['t_query_terminal_ns'] - execution['t_submit_ns']) / 1e9, abs_tol=1e-9)
        traces, protocols = lines(prefix + 'http-trace.jsonl'), lines(prefix + 'protocols.jsonl')
        assert len(traces) == len(protocols) == rows
        query_http = []
        for trace in traces:
            assert trace['status'] == 'completed' and trace['retry_count'] == 0
            assert trace['upstream_headers_send_count'] == 1
            assert trace['request_body_sha256'] == trace['forwarded_body_sha256']
            assert trace['upstream_pool_generation'] == ordinal + 1
            first, last = (trace['upstream_dispatch_started_monotonic_ns'],
                           trace['upstream_response_body_read_completed_monotonic_ns'])
            assert execution['t_submit_ns'] <= first <= last <= execution['t_query_terminal_ns']
            query_http.append((last - first) / 1e9)
            for label, left, right in (
                ('read_body', 'received_monotonic_ns', 'request_body_read_completed_monotonic_ns'),
                ('before_forward', 'before_forward_started_monotonic_ns', 'before_forward_completed_monotonic_ns'),
                ('after_forward', 'after_forward_started_monotonic_ns', 'after_forward_completed_monotonic_ns'),
                ('response_write', 'response_write_started_monotonic_ns', 'response_write_completed_monotonic_ns'),
                ('connection_wait', 'upstream_connection_queued_started_monotonic_ns', 'upstream_connection_queued_completed_monotonic_ns')):
                if trace.get(left) is not None:
                    assert trace.get(right) is not None and trace[right] >= trace[left]
                    http_stages[label].append((trace[right] - trace[left]) / 1e9)
        http.extend(query_http)
        bodies[cell['name'], ordinal] = Counter(t['request_body_sha256'] for t in traces)
        actual_values[cell['name'], ordinal] = Counter(content_digest(t['request']) for t in protocols)
        events = lines(prefix + 'method-events.jsonl') + value.get('request_service', {}).get('core_events', [])
        rpc = [event for event in events if event.get('event') == 'ray_http_completed']
        if 'semloom' in cell['arm'] and not cell['arm'].endswith('local-diagnostic'):
            assert len(rpc) == rows and len({(e['key']['session_id'], e['key']['sequence']) for e in rpc}) == rows
        for event in rpc:
            assert event['shared_clock']
            assert event['rpc_started_ns'] <= event['worker_started_ns'] <= event['worker_ended_ns'] <= event['received_ns']
            assert event['before_worker_ns'] + event['worker_elapsed_ns'] + event['after_worker_ns'] == event['received_ns'] - event['rpc_started_ns']
            rpc_before.append(event['before_worker_ns'] / 1e9)
            worker.append(event['worker_elapsed_ns'] / 1e9)
            rpc_after.append(event['after_worker_ns'] / 1e9)
        per_query_phases = defaultdict(float)
        for event in events:
            if event.get('event') == 'payload_stage':
                assert event['status'] == 'completed' and event['elapsed_ns'] >= 0
                per_query_phases['payload.' + event['stage']] += event['elapsed_ns'] / 1e9
            elif event.get('event') == 'ray_work':
                assert event['status'] == 'completed'
                for field in ('queue_ns', 'work_ns', 'resume_ns', 'elapsed_ns'):
                    if event.get(field) is not None:
                        assert event[field] >= 0
                        per_query_phases['ray.' + event['stage'] + '.' + field.removesuffix('_ns')] += event[field] / 1e9
        for label, duration in per_query_phases.items():
            phases[label].append(duration)
        timing = value.get('call_timing', {})
        current_e2e = timing.get('request_e2e', {}).get('samples', [])
        if current_e2e:
            assert len(current_e2e) == rows
            call_e2e.extend(current_e2e)
            for label, spans in timing.get('stages', {}).items():
                method_stages[label].extend(spans['samples'])
        if exists(prefix + 'sema-service.jsonl'):
            for event in lines(prefix + 'sema-service.jsonl'):
                assert event['error_type'] is None and event['response_written_ns'] >= event['proxy_arrived_ns']
                service.append((event['response_written_ns'] - event['proxy_arrived_ns']) / 1e9)
                for label, left, right in (
                    ('arrival_to_accept', 'proxy_arrived_ns', 'core_accepted_ns'),
                    ('accept_to_rpc', 'core_accepted_ns', 'forward_started_ns'),
                    ('received_to_delivery', 'model_returned_observed_ns', 'core_delivery_ns'),
                    ('delivery_to_resume', 'response_ready_ns', 'forward_resumed_ns'),
                    ('resume_to_write', 'forward_resumed_ns', 'response_written_ns'),
                ):
                    if event.get(left) is not None and event.get(right) is not None:
                        assert event[right] >= event[left]
                        sema_stages[label].append((event[right] - event[left]) / 1e9)
        adapter = value.get('identity', {}).get('adapter_timings')
        if adapter:
            assert adapter['vector_rows'] == rows
            assert adapter['bridge']['phases']['native_consume']['count'] == rows
            for owner_name in ('executor', 'bridge'):
                for label, observation in adapter[owner_name]['phases'].items():
                    phases['duckdb.' + owner_name + '.' + label].append(observation['total_ns'] / 1e9)
        life = read(prefix + 'persistent-query.json')['persistent_lifecycle']
        lifecycles.append(life)
        samples.append(dict(query=ordinal, summary_sha256=hashlib.sha256(raw(prefix + 'summary.json')).hexdigest(),
            full_seconds=value['full_query_seconds'], ready_seconds=execution['ready_query_seconds'],
            preparation_seconds=execution['preparation_seconds'],
            source_seconds=value['stages']['source_seconds'],
            first_row_seconds=(execution['t_first_row_ns']-execution['t_submit_ns'])/1e9,
            tail_consume_seconds=(execution['t_query_terminal_ns']-execution['t_first_row_ns'])/1e9,
            correct=value['quality']['correct'], rows=rows,
            prompt_tokens=sum(t['actual_prompt_tokens'] for t in traces),
            output_tokens=sum(t['actual_output_tokens'] for t in traces),
            mean_http_inflight=sum(query_http)/execution['ready_query_seconds'],
            http_peak=value['observed_peak_http'], measured_phase_totals=dict(per_query_phases)))
    if cell['sema_executor_scope'] == 'group-diagnostic':
        assert len({life['execution_id'] for life in lifecycles}) == 1
        for life in lifecycles:
            assert life['core_jobs'] == 0 and all(amount == 0 for amount in life['core_usage'].values())
            assert life['sema_executor']['object_bytes'] == 0 and not life['sema_executor']['poisoned']
        final = read(cell['name'] + '/group-summary.json')['owners'][cell['arm']]['sema_executor']
        assert not final['control_thread_alive']
    records.append(dict(cell, samples=samples, group_startup_seconds=startup['startup_seconds'],
        group_startup_sha256=hashlib.sha256(raw(cell['name'] + '/startup.json')).hexdigest(),
        full=distribution([s['full_seconds'] for s in samples], preserve=True),
        submit_to_eof=distribution([s['ready_seconds'] for s in samples], preserve=True),
        preparation=distribution([s['preparation_seconds'] for s in samples], preserve=True),
        source=distribution([s['source_seconds'] for s in samples], preserve=True),
        first_row=distribution([s['first_row_seconds'] for s in samples], preserve=True),
        tail_consume=distribution([s['tail_consume_seconds'] for s in samples], preserve=True),
        http=distribution(http, preserve=True), request_e2e=distribution(call_e2e, preserve=True),
        rpc_before_worker=distribution(rpc_before), worker=distribution(worker), rpc_after_worker=distribution(rpc_after),
        sema_service_arrival_to_write=distribution(service),
        sema_stages={k: distribution(v, preserve=True) for k, v in sema_stages.items()},
        method_stages={k: distribution(v) for k, v in method_stages.items()},
        http_stages={k: distribution(v) for k, v in http_stages.items()},
        measured_phase_totals={k: distribution(v, preserve=True) for k, v in phases.items()},
        accuracy=sum(s['correct'] for s in samples)/sum(s['rows'] for s in samples)))
for supplier in sorted({cell['supplier'] for cell in manifest['cells']}):
    family = [cell for cell in manifest['cells'] if cell['supplier'] == supplier]
    reference = next(cell for cell in family if (
        cell.get('variant') == args.reference_variant if args.reference_variant is not None
        else cell['payload_backend'] == 'native'))
    for cell in family:
        if cell == reference:
            continue
        for ordinal in ordinals:
            assert actual_values[reference['name'], ordinal] == actual_values[cell['name'], ordinal]
            byte_equal = bodies[reference['name'], ordinal] == bodies[cell['name'], ordinal]
            if supplier != 'LOTUS':
                assert byte_equal
            pair = dict(supplier=supplier, adapter=cell['name'], query=ordinal,
                complete_request_values_multiset_equal=True, complete_request_bytes_multiset_equal=byte_equal)
            if args.reference_variant is None:
                pair['native'] = reference['name']
            else:
                pair.update(reference=reference['name'], reference_variant=args.reference_variant,
                            reference_role='same-supplier adapter control')
            comparisons.append(pair)
result = dict(schema='semloom.latest_supplier_short_observations.v1', status='passed',
    run_id=manifest['run_id'], source_commit=manifest['source_commit'],
    real_model_posts=manifest['max_posts'], queries=manifest['queries'], measurements=len(ordinals)*len(records),
    records=records, all_queries=all_queries, request_comparisons=comparisons,
    quantile_scope=f'nearest rank; {len(ordinals)} queries make whole-query P99 a sample maximum; related request samples are not independent population tails',
    timing_scope='direct preparation and SQL/API-to-EOF timestamps; HTTP includes transfer and service wait; nested and parallel spans are not added or subtracted',
    unavailable=dict(model_inference='no per-call model execution probes', organization='no separate task-selection start/end probe',
        native_task_ready='native DuckDB and author Sema do not expose upstream legal-task readiness'))
assert not args.output.exists()
args.output.write_text(json.dumps(result, ensure_ascii=False, separators=(',', ':'))+'\n')
print(json.dumps(dict(status='passed', run_id=result['run_id'], queries=result['queries'],
    calls=result['real_model_posts'], comparisons=len(comparisons),
    records=[dict(name=r['name'], supplier=r['supplier'], backend=r['payload_backend'],
        ready_median=r['submit_to_eof']['median'], ready_p99=r['submit_to_eof']['p99'],
        http_p99=r['http']['p99'], request_e2e_p99=r['request_e2e']['p99'], accuracy=r['accuracy']) for r in records])))
