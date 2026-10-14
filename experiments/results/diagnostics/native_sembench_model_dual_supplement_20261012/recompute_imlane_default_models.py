"""Recompute four complete IMLane query timings, offline with the standard library."""
import argparse
import gzip
import json
import math
from pathlib import Path


def describe(values):
    ordered = sorted(values)
    size = len(ordered)
    if not size or any(not math.isfinite(value) or value < 0 for value in ordered):
        raise ValueError('invalid complete duration series')
    return dict(count=size, unit='seconds', mean=math.fsum(ordered) / size,
                minimum=ordered[0], maximum=ordered[-1],
                **{'p' + str(p): ordered[(size * p + 99) // 100 - 1] for p in (50, 95, 99)},
                method='nearest rank ceil(n*p); descriptive, not population tail')


def compute(observations):
    rows = []
    for query in observations['queries']:
        expected = {1: 2000, 2: 120}[query['query']]
        events = query['owner_proxy_http_ns']
        if len(events) != expected or any(len(v) != 4 or v != sorted(v) for v in events):
            raise ValueError('complete HTTP count or timestamp order differs')
        arrival, ready, eof = query['query_arrival_ready_eof_ns']
        merged = []
        for first, _, _, last in sorted(events):
            if merged and first <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], last)
            else:
                merged.append([first, last])
        first, last = merged[0][0], merged[-1][1]
        if not arrival <= ready <= first <= last <= eof:
            raise ValueError('query and request boundaries differ')
        active = peak = 0
        for _, change in sorted([(v[0], 1) for v in events] + [(v[3], -1) for v in events]):
            active += change
            if active < 0:
                raise ValueError('negative HTTP ownership')
            peak = max(peak, active)
        if active:
            raise ValueError('unclosed HTTP interval')
        batch_wait = query['sdk_batch_wait_seconds']
        if len(batch_wait) != expected or len(set(batch_wait)) != 1:
            raise ValueError('original single SDK batch wait labels differ')
        covered = sum(end - start for start, end in merged)
        gaps = [merged[i][0] - merged[i - 1][1] for i in range(1, len(merged))]
        rows.append(dict(
            arm=query['arm'], query=query['query'], unit_id=query['unit_id'], requests=expected,
            task_completion_seconds=(eof - arrival) / 1e9,
            ready_completion_seconds=(eof - ready) / 1e9,
            public_query_seconds=query['public_query_seconds'],
            source_preparation_seconds=query['source_preparation_seconds'],
            post_query_evaluation_seconds=query['post_query_evaluation_seconds'],
            arrival_to_first_http_seconds=(first - arrival) / 1e9,
            first_to_last_http_seconds=(last - first) / 1e9,
            http_activity_covered_seconds=covered / 1e9,
            http_activity_gap_seconds=sum(gaps) / 1e9,
            last_http_to_consumer_eof_seconds=(eof - last) / 1e9,
            observed_peak_http=peak,
            average_inflight_during_http_span=sum(v[3] - v[0] for v in events) / (last - first),
            request_statistics=dict(
                owner_proxy_request=describe([(v[3] - v[0]) / 1e9 for v in events]),
                gateway_before_dispatch=describe([(v[1] - v[0]) / 1e9 for v in events]),
                upstream_roundtrip=describe([(v[2] - v[1]) / 1e9 for v in events]),
                gateway_after_upstream=describe([(v[3] - v[2]) / 1e9 for v in events])),
            sdk_batch_wait=dict(seconds=batch_wait[0], row_labels=expected, distinct_measured_spans=1,
                request_percentiles='unavailable: one SDK batch wait, repeated as row labels'),
            query_p99=dict(status='unavailable', reason='one Q2 and one Q1 execution in each author mode'),
            model_inference_seconds=dict(status='unavailable', reason='HTTP timestamps do not measure GPU kernel time'),
            pure_queue_seconds=dict(status='unavailable', reason='upstream interval includes serving, transport and scheduling')))
    wanted = {(arm, q) for arm in ('IMLane-CPython', 'IMLane-Ray') for q in (2, 1)}
    if len(rows) != 4 or {(row['arm'], row['query']) for row in rows} != wanted:
        raise ValueError('exact four author-mode queries required')
    return dict(schema='semloom.imlane_default_complete_timing_statistics.v1', queries=rows,
        request_definition='owner proxy handler entry through response write EOF, including audit and transport',
        average_inflight_definition='sum(request entry-to-EOF ns)/(last EOF-first entry ns); not GPU concurrency',
        attribution='initialize, lock, SQL and commit stage costs unavailable in this run',
        scope='within-run descriptive observations; no mixed mean, query tail, ranking or subtraction')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('observations', type=Path)
    args = parser.parse_args()
    with gzip.open(args.observations, 'rt') as stream:
        print(json.dumps(compute(json.load(stream)), ensure_ascii=False, indent=2))
