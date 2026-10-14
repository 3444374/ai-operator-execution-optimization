"""Recompute descriptive timings from compact, complete retained observations.

Usage: python recompute_single_gpu_full.py single-gpu-full-q1-q2-timings.json.gz
The input has timing points and operation durations, no requests/model responses.
"""
import argparse
import gzip
import json
import math
from pathlib import Path


def describe(values, empty_reason='no completed observations'):
    if not values:
        return dict(status='unavailable', count=0, reason=empty_reason)
    ordered = sorted(values)
    if any(not math.isfinite(v) or v < 0 for v in ordered):
        raise ValueError('nonfinite or negative measured duration')
    size = len(ordered)
    ranks = {p: (size * p + 99) // 100 for p in (50, 95, 99)}
    return dict(status='observed', count=size, unit='seconds',
                sum_seconds=math.fsum(ordered), mean_seconds=math.fsum(ordered) / size,
                minimum_seconds=ordered[0], maximum_seconds=ordered[-1],
                p50_seconds=ordered[ranks[50] - 1], p95_seconds=ordered[ranks[95] - 1],
                p99_seconds=ordered[ranks[99] - 1], ranks=ranks,
                quantile_method='nearest rank: sorted[ceil(n*p)-1]',
                population_tail_estimate=False)


def compute(observations):
    rows = []
    for query in observations['queries']:
        events = query['owner_proxy_http_ns']
        expected = {1: 2000, 2: 120}[query['query']]
        if len(events) != expected or any(len(v) != 4 or v != sorted(v) for v in events):
            raise ValueError('complete HTTP observation count/order differs')
        durations = {
            'owner_proxy_request': [(v[3] - v[0]) / 1e9 for v in events],
            'gateway_before_dispatch': [(v[1] - v[0]) / 1e9 for v in events],
            'upstream_roundtrip': [(v[2] - v[1]) / 1e9 for v in events],
            'gateway_after_upstream': [(v[3] - v[2]) / 1e9 for v in events],
        }
        arrival, ready, eof = query['query_arrival_ready_eof_ns']
        if not arrival <= ready <= eof:
            raise ValueError('query clock points are not nested')
        repeated = query['sdk_batch_wait_seconds']
        if len(repeated) != expected or len(set(repeated)) != 1:
            raise ValueError('expected one complete original SDK batch wait, repeated as row labels')
        rows.append(dict(
            arm=query['arm'], query=query['query'], warm_state_family=query['warm_state_family'],
            task_completion_ns=eof - arrival, task_completion_seconds=(eof - arrival) / 1e9,
            ready_completion_seconds=(eof - ready) / 1e9,
            arrival_to_ready_seconds=(ready - arrival) / 1e9,
            public_query_seconds=query['public_query_seconds'],
            post_query_evaluation_seconds=query['post_query_evaluation_seconds'],
            query_p99=dict(status='unavailable', reason='one query execution; no repeated query population'),
            request_statistics={key: describe(value) for key, value in durations.items()},
            sdk_batch_wait=dict(seconds=repeated[0], repeated_row_labels=expected,
                                distinct_timing_spans=1, request_percentiles='unavailable: whole SDK batch wait, not individual request timing'),
            executor_operation_statistics={key: describe(value) for key, value in query['executor_operation_seconds'].items()},
            executor_operation_unavailable=query['executor_operation_unavailable'],
            per_call_stage_unavailable=query['per_call_stage_unavailable'],
            model_inference_seconds=dict(status='unavailable', reason='retained HTTP responses expose no per-call GPU kernel/model execution timing'),
            pure_queue_seconds=dict(status='unavailable', reason='dispatch and upstream intervals include transport, preparation and scheduling'),
            span_aggregation='nested and parallel operation sums are not exclusive query components and cannot be subtracted from JCT'))
    if len(rows) != 8 or len({(r['arm'], r['query']) for r in rows}) != 8:
        raise ValueError('all eight original queries are required')
    return dict(schema='semloom.single_gpu_full_retained_timing_statistics.v1', queries=rows,
                request_definition='owner proxy handler entry through response write EOF; each retained HTTP request is one observation',
                partition_definition=dict(
                    gateway_before_dispatch='received through upstream dispatch: request read and before-forward work',
                    upstream_roundtrip='upstream dispatch through attempt finish: connection/pool/backend queue, serving and response transfer',
                    gateway_after_upstream='attempt finish through consumer response write EOF: audit and downstream transfer'),
                scope='descriptive within-run request/operation quantiles, not stable population or query tail latency')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('observations', type=Path)
    args = parser.parse_args()
    with gzip.open(args.observations, 'rt') as stream:
        value = compute(json.load(stream))
    print(json.dumps(value, ensure_ascii=False, indent=2))
