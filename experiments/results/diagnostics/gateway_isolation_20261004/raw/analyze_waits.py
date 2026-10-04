"""Recompute observed queue residence and capacity checks from this fixture's events."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import statistics

from src.execution_provider.adapters.model_config import MAX_MODEL_RESPONSE_BYTES
from src.execution_provider.wire.framing import MAX_FRAME_BYTES


def analyze(root):
    suite = json.loads((root / 'summary.json').read_text())
    cells, windows, prepare, snapshots = [], [], [], []
    for case in suite['cases']:
        usage = case['core_observed_peak']
        held, capacity = case['held_tasks'], case['capacity']
        if (usage['held_tasks'] > held or usage['input_bytes'] > held * MAX_FRAME_BYTES
                or usage['result_bytes'] > held * MAX_MODEL_RESPONSE_BYTES
                or usage['active_requests'] > capacity or usage['active_work'] > capacity
                or case['worker_active_peak'] > capacity or case['object_observed_peak_bytes'] > 4194304
                or case['fd_count_before'] != case['fd_count_after'] or case['errors'] or case['remaining_threads']
                or any(case['core_final_usage'].values()) or not case['transport_drained']
                or not case['worker_stopped'] or not case['actor_disposed']):
            raise AssertionError('retained capacity or cleanup snapshot differs')
        snapshots.append(case['case_id'])
        if case['phase'] != 'measurement':
            continue
        with gzip.open(root / case['case_id'] / 'events.jsonl.gz', 'rt') as stream:
            events = [json.loads(line) for line in stream]
        times = {}
        for event in events:
            if event['event'] in ('offer', 'submitted', 'fixture_worker_started'):
                key = tuple(event['key'][field] for field in ('session_id', 'sequence'))
                identity = event['event'], key
                if identity in times:
                    raise AssertionError('duplicate observed phase identity')
                times[identity] = event['monotonic_ns']
        rows = []
        for event in events:
            if event['event'] != 'fixture_worker_started' or event['label'] != 'B':
                continue
            key = tuple(event['key'][field] for field in ('session_id', 'sequence'))
            rows.append(dict(key=key,
                accepted_observation_to_submit_ms=(times['submitted', key] - times['offer', key]) / 1e6,
                submit_to_worker_observation_ms=(times['fixture_worker_started', key] - times['submitted', key]) / 1e6))
        if len(rows) != case['rows_per_producing_job']:
            raise AssertionError('observed wait population differs')
        fields = ('accepted_observation_to_submit_ms', 'submit_to_worker_observation_ms')
        cells.append(dict(case_id=case['case_id'], scenario=case['scenario'], paused=case['paused'], rows=rows,
            medians={field: statistics.median(row[field] for row in rows) for field in fields},
            negative_observations={field: sum(row[field] < 0 for row in rows) for field in fields}))
        windows.extend(case['window_rows'])
        if case['scenario'] == 'payload_prepare' and case['paused']:
            begin = next(e['monotonic_ns'] for e in events if e['event'] == 'fixture_interference_started')
            end = next(e['monotonic_ns'] for e in events if e['event'] == 'fixture_interference_ended')
            prepare.append(dict(case_id=case['case_id'],
                logical_active_observed=sorted({e['usage']['active_requests'] for e in events
                    if 'usage' in e and 'key' in e and begin <= e['monotonic_ns'] <= end}),
                worker_calls_before_release=sum(e['event'] == 'fixture_worker_submitted'
                    and e['monotonic_ns'] <= end for e in events)))
    return dict(schema='semloom.gateway-isolation.waits.v1', status='passed',
        analysis_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), cells=cells,
        capacity_and_cleanup_snapshots_checked=snapshots, prepare_observations=prepare,
        measurement_windows=dict(count=len(windows), singletons=windows.count(1), rows=sum(windows)),
        scope='observed intervals include organization, capacity and recording; not exclusive queue or GPU time')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    result = analyze(args.evidence)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
