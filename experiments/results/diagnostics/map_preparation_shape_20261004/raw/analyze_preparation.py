"""Replay the retained fixed-window and real-Core diagnostics without network or model work."""
import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import statistics


def key(event):
    return event['key']['session_id'], event['key']['sequence']


def summary(values):
    return dict(values=values, median=statistics.median(values))


def interval(values):
    return dict(count=len(values), negative=sum(value < 0 for value in values),
                median_ms=statistics.median(values)/1e6)


def analyze(initial, core):
    component = json.loads((initial/'component/summary.json').read_text())
    component_cases = []
    for case in component['cases']:
        path = initial/'component'/f"{case['phase']}-{case['repeat']}-{case['backend']}-{case['window_rows']}"/'result.json'
        raw = json.loads(path.read_text())
        assert raw['status'] == 'passed' and raw['total_ms'] == case['total_ms']
        assert raw['windows'] * raw['window_rows'] == raw['input_rows']
        observations = raw['observations']
        for stage in ('next', 'exhaust', 'close'):
            assert len([row for row in observations if row['stage'] == stage]) == raw['windows']
        assert all(row['elapsed_ns'] >= 0 for row in observations)
        assert sum(row['rows'] for row in observations) == raw['input_rows']
        component_cases.append(dict(phase=case['phase'], repeat=case['repeat'], backend=raw['backend'],
            window_rows=raw['window_rows'], windows=raw['windows'], total_ms=raw['total_ms'],
            stages_ms={stage:sum(row['elapsed_ns'] for row in observations if row['stage'] == stage)/1e6
                       for stage in ('next', 'exhaust', 'close')}))
    measured_components = [case for case in component_cases if case['phase'] == 'measurement']
    for name, metric in component['metrics'].items():
        values = [case['total_ms'] for case in measured_components
                  if f"{case['backend']}-{case['window_rows']}" == name]
        assert metric == summary(values)
    component_hashes = json.loads((initial/'component/measurement-1-arrow-1/result.json').read_text())['payload_sha256']
    suite = json.loads((core/'suite/summary.json').read_text())
    cases = []
    for case in suite['cases']:
        path = core/'suite'/f"{case['phase']}-{case['repeat']}-{case['arm']}"
        result = json.loads((path/'result.json').read_text())
        with gzip.open(path/'events.jsonl.gz', 'rt') as stream:
            events = [json.loads(line) for line in stream]
        assert result['status'] == 'passed' and not result['errors']
        count = result['total_rows']
        http = [event for event in events if event['event'] == 'loopback_http_started']
        finished = [event for event in events if event['event'] == 'loopback_http_finished']
        for group in (http, finished):
            assert len(group) == count
            assert {(event['label'],event['sequence']) for event in group} == {('row',i) for i in range(count)}
            assert all(event['request_sha256'] == component_hashes[event['sequence']] for event in group)
        counted = [event for event in events if event['event'] == 'fixture_request_counted']
        expected_hashes = Counter(component_hashes[:count])
        assert len(counted) == count and Counter(event['request_sha256'] for event in counted) == expected_hashes
        assert sorted(event['attempt'] for event in counted) == list(range(1,count+1))
        ledger = json.loads((path/'ledger.projection.json').read_text())
        assert ledger['allocated_requests'] == count and ledger['closed_shared_units'] == 1
        assert [row['sequence'] for row in ledger['shared_requests']] == list(range(1,count+1))
        assert Counter(row['request_sha256'] for row in ledger['shared_requests']) == expected_hashes
        offers = [event for event in events if event['event'] == 'offer']
        assert len(offers) == count+1 and all(event['status'] == 'ACCEPTED' for event in offers)
        rpc = [event for event in events if event['event'] == 'ray_http_completed']
        submitted = [event for event in events if event['event'] == 'submitted']
        terminals = [event for event in events if event['event'] == 'terminal']
        released = [event for event in events if event['event'] == 'released']
        key_sets = []
        for group in (rpc, submitted, terminals, released):
            assert len(group) == count and len({key(event) for event in group}) == count
            key_sets.append({key(event) for event in group})
        assert all(keys == key_sets[0] for keys in key_sets)
        assert len({key(event) for event in offers}-key_sets[0]) == 1
        assert all(event['shared_clock'] for event in rpc)
        validated = [event for event in events if event['event'] == 'fixture_result_validated']
        assert len(validated) == count and {event['ordinal'] for event in validated} == set(range(count))
        assert all(event['output_sha256'] == hashlib.sha256(f"fixture:row:{event['ordinal']}".encode()).hexdigest()
                   for event in validated)
        assert not any(result['core_final_usage'].values()) and result['transport_drained']
        closed = [event for event in events if event['event'] == 'ray_transport_closed']
        assert len(closed) == 1 and closed[0]['confirmed'] and closed[0]['object_bytes'] == 0
        blocks = [event for event in events if event['event'] == 'ray_block_put']
        assert sum(event['rows'] for event in blocks) == count
        assert all(1 <= event['rows'] <= 4 for event in blocks)
        assert len(blocks) == len([event for event in events if event['event'] == 'ray_block_released'])
        assert len({event['block_id'] for event in blocks}) == len(blocks)
        work = [event for event in events if event['event'] == 'ray_work']
        assert all(event['status'] == 'completed' for event in work)
        assert len([event for event in work if event['stage'] == 'payload_close']) == len(blocks)
        assert len([event for event in work if event['stage'] == 'object_put']) == len(blocks)
        assert len([event for event in work if event['stage'] == 'payload_next']) == 2*len(blocks)
        assert all(event['queue_ns']+event['work_ns']+event['resume_ns'] == event['elapsed_ns'] for event in work)
        assert all(event['rpc_started_ns'] <= event['worker_started_ns'] <= event['worker_ended_ns'] <= event['received_ns']
                   and event['submit_elapsed_ns'] == event['rpc_returned_ns']-event['rpc_started_ns'] for event in rpc)
        peaks = {name:max(event['usage'][name] for event in events if 'usage' in event)
                 for name in result['core_observed_peak']}
        assert peaks == result['core_observed_peak']
        for name, limit in [('held_tasks',384),('input_bytes',12582912),('result_bytes',402653184),
                            ('active_requests',4),('active_work',4)]:
            assert peaks[name] <= limit
        assert all(0 <= event.get('object_bytes',0) <= 4194304 for event in events)
        stages = {stage:{name:sum(event[name+'_ns'] for event in work if event['stage']==stage)/1e6
                         for name in ('queue','work','resume','elapsed')}
                  for stage in ('payload_next','object_put','payload_close')}
        offered = {key(event):event['monotonic_ns'] for event in offers}
        dispatch = {key(event):event['monotonic_ns'] for event in submitted}
        waits = {name:interval([event['rpc_started_ns']-origins[key(event)] for event in rpc])
                 for name, origins in [('offer_to_rpc', offered), ('submitted_to_rpc', dispatch)]}
        guards = [event for event in events if event['event'] == 'remote_request_guard']
        assert len(guards) == count and all(event['status'] == 'completed' for event in guards)
        guard = {name:sum(event[name+'_ns'] for event in guards)/1e6
                 for name in ('reserve','request_observe','elapsed')}
        sizes = Counter(event['rows'] for event in blocks)
        startup = {event['stage']:event['elapsed_seconds']*1000
                   for event in events if event['event'] == 'ray_startup'}
        offered_start = min(event['monotonic_ns'] for event in events
                            if event['event'] == 'fixture_offer_started' and event['label'] != 'C')
        first_result_ms = (min(event['monotonic_ns'] for event in validated)-offered_start)/1e6
        cases.append(dict(phase=case['phase'],repeat=case['repeat'],arm=case['arm'],
            consume_ms=case['consume_ms'],complete_case_ms=case['complete_case_ms'],
            first_result_ms=first_result_ms,
            block_count=len(blocks),block_sizes=dict(sorted(sizes.items())),
            rpc_count=len(rpc),rpc_ms={name:dict(sum=sum(event[name+'_ns'] for event in rpc)/1e6,
                         median=statistics.median(event[name+'_ns'] for event in rpc)/1e6)
                   for name in ('submit_elapsed','before_worker','worker_elapsed','after_worker')},
            stages_ms=stages,guard_ms=guard,waits=waits,startup_ms=startup,
            max_object_buffer_bytes=max(event.get('object_bytes',0) for event in events),
            core_peaks=result['core_observed_peak'],http_peak=result['actual_http_peak']))
    assert len(cases) == 20 and sum(case['rpc_count'] for case in cases) == suite['http_requests'] == 2080
    measured = [case for case in cases if case['phase'] == 'measurement']
    aggregate = {}
    for arm in ('one-daft','one-arrow','two-daft','two-arrow'):
        group = [case for case in measured if case['arm'] == arm]
        assert len(group) == 3
        for name in ('consume_ms','complete_case_ms'):
            assert suite['metrics'][arm][name] == summary([case[name] for case in group])
        aggregate[arm] = dict(consume_ms=summary([case['consume_ms'] for case in group]),
            first_result_ms=summary([case['first_result_ms'] for case in group]),
            complete_case_ms=summary([case['complete_case_ms'] for case in group]),
            block_count=summary([case['block_count'] for case in group]),
            singleton_counts=[case['block_sizes'].get(1,0) for case in group],
            startup_ms={name:summary([case['startup_ms'][name] for case in group]) for name in group[0]['startup_ms']},
            stages_ms={stage:{name:summary([case['stages_ms'][stage][name] for case in group])
                         for name in ('queue','work','resume','elapsed')} for stage in group[0]['stages_ms']},
            guard_ms={name:summary([case['guard_ms'][name] for case in group]) for name in group[0]['guard_ms']},
            rpc_ms={name:{kind:summary([case['rpc_ms'][name][kind] for case in group]) for kind in ('sum','median')}
                    for name in group[0]['rpc_ms']},
            waits={name:dict(median_ms=summary([case['waits'][name]['median_ms'] for case in group]),
                            negative=sum(case['waits'][name]['negative'] for case in group)) for name in group[0]['waits']})
    cleanup = json.loads((core/'cleanup.json').read_text())
    assert cleanup['live_owned_services'] == []
    return dict(status='passed',model_requests=0,pg_queries=0,http_requests=2080,
        component=dict(prepared_rows=component['prepared_rows'],metrics=component['metrics'],cases=component_cases),
        core=dict(metrics=aggregate,cases=cases,ray_startup_ms=suite['ray_startup_ms'],suite_seconds=suite['suite_seconds'],
            max_object_buffer_bytes=max(case['max_object_buffer_bytes'] for case in cases),
            max_http_active=max(case['http_peak'] for case in cases),cleanup=cleanup))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--initial', type=Path, required=True)
    parser.add_argument('--core', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.initial,args.core)
    with args.output.open('x') as stream:
        json.dump(result,stream,separators=(',',':'),ensure_ascii=False)
    print(json.dumps(dict(status=result['status'],http_requests=result['http_requests'],model_requests=0,
                         core_metrics={arm:dict(consume_ms=value['consume_ms'],blocks=value['block_count'],
                                               singletons=value['singleton_counts']) for arm,value in result['core']['metrics'].items()})))


if __name__ == '__main__':
    main()
