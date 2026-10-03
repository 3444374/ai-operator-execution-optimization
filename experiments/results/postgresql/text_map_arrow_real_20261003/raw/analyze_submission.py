"""Read existing same-clock RPC events without starting workers or model services."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics


def union_ns(intervals):
    total = 0
    end = -1
    for start, stop in sorted(intervals):
        if not 0 <= start <= stop:
            raise ValueError('invalid observation interval')
        total += max(0, stop - max(start, end))
        end = max(end, stop)
    return total


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def analyze(root):
    root = Path(root)
    worker = json.loads((root/'real01/worker/summary.json').read_text())
    if worker['status'] != 'passed':
        raise ValueError('comparison was not completed')
    rows = []
    for query in worker['completed']:
        if query['repeat'] < 1:
            continue
        path = root/'real01/worker'/f"{query['mode']}{query['rows']}"/query['unit']/'events.jsonl'
        raw = path.read_bytes()
        events = [json.loads(line) for line in raw.splitlines()]
        rpc = [event for event in events if event['event'] == 'core_ray_http_completed']
        guards = [event for event in events if event['event'] == 'core_ray_request_guard']
        if len(rpc) != 1024 or len(guards) != 1024 or len({tuple(sorted(e['key'].items())) for e in rpc}) != 1024:
            raise ValueError('per-row observation count or identity differs')
        for event in rpc:
            if not event['shared_clock'] or not (
                event['rpc_started_ns'] <= event['rpc_returned_ns'] <= event['received_ns']
                and event['rpc_started_ns'] <= event['worker_started_ns']
                <= event['worker_ended_ns'] <= event['received_ns']):
                raise ValueError('RPC and worker clocks are not comparable')
        lower = min(event['rpc_started_ns'] for event in rpc)
        upper = max(event['received_ns'] for event in rpc)
        busy = union_ns([(event['worker_started_ns'], event['worker_ended_ns']) for event in rpc])
        key = lambda event: tuple(sorted(event['key'].items()))
        submitted = {key(event): event['monotonic_ns'] for event in events
                     if event['event'] == 'core_submitted'}
        terminal = {key(event): event['monotonic_ns'] for event in events
                    if event['event'] == 'core_terminal'}
        if len(submitted) != 1024 or set(submitted) != set(terminal) or set(submitted) != {key(e) for e in rpc}:
            raise ValueError('logical submission and terminal identities differ')
        usage = [event for event in events if isinstance(event.get('usage'), dict)
                 and 'active_requests' in event['usage']]
        if not usage or usage[0]['usage']['active_requests'] or usage[-1]['usage']['active_requests']:
            raise ValueError('logical request observations do not start and end empty')
        if any(not 0 <= event['usage']['active_requests'] <= 64 for event in usage):
            raise ValueError('logical request capacity differs')
        spans = [b['monotonic_ns']-a['monotonic_ns'] for a, b in zip(usage, usage[1:])]
        if min(spans) < 0:
            raise ValueError('logical resource clock is not monotonic')
        usage_span = usage[-1]['monotonic_ns']-usage[0]['monotonic_ns']
        if usage_span <= 0:
            raise ValueError('logical resource interval is empty')
        area = sum(span*event['usage']['active_requests'] for span, event in zip(spans, usage))
        row = dict(mode=query['mode'], repeat=query['repeat'], rpc_rows=len(rpc),
                   events_sha256=hashlib.sha256(raw).hexdigest(),
                   jct_seconds=query['jct_seconds'],
                   submit_sum_seconds=sum(event['submit_elapsed_ns'] for event in rpc)/1e9,
                   worker_interval_span_seconds=(upper-lower)/1e9,
                   any_worker_active_seconds=busy/1e9,
                   no_worker_active_seconds=(upper-lower-busy)/1e9,
                   mean_active_workers=sum(event['worker_elapsed_ns'] for event in rpc)/(upper-lower),
                   logical_request_span_seconds=usage_span/1e9,
                   mean_logical_active_requests=area/usage_span,
                   guard_elapsed_sum_seconds=sum(event['elapsed_ns'] for event in guards)/1e9)
        for label, field in [('before_worker', 'before_worker_ns'),
                             ('inside_worker', 'worker_elapsed_ns'),
                             ('after_worker', 'after_worker_ns')]:
            values = [event[field] for event in rpc]
            row[label+'_p50_ms'] = statistics.median(values)/1e6
            row[label+'_p95_ms'] = percentile(values, .95)/1e6
        for label, values in [
                ('core_submitted_to_ray', [event['rpc_started_ns']-submitted[key(event)] for event in rpc]),
                ('ray_received_to_core_terminal', [terminal[key(event)]-event['received_ns'] for event in rpc])]:
            if min(values) < 0:
                raise ValueError('logical and RPC observations are not causal')
            row[label+'_p50_ms'] = statistics.median(values)/1e6
            row[label+'_p95_ms'] = percentile(values, .95)/1e6
        rows.append(row)
    if len(rows) != 6:
        raise ValueError('the three measured pairs are required')
    return dict(schema='semloom.submission_analysis.v1', measured_rows=6144,
                added_model_requests=0, percentile='nearest rank; median uses statistics.median',
                scope='Core submitted means accepted by the transport. Worker occupancy includes HTTP, queueing and payload work, not GPU activity. The two occupancy means use separately declared intervals. Per-row durations overlap.',
                queries=rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.evidence)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps({key:result[key] for key in ['measured_rows','added_model_requests']}))
