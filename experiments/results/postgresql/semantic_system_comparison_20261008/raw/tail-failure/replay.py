"""Recompute the retained transport failure and fixture comparisons offline."""
import collections
import gzip
import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent
def read(name):
    content = (ROOT / name).read_bytes()
    return json.loads(gzip.decompress(content) if name.endswith('.gz') else content)


def main():
    analysis = read('analysis.json')
    execution = read('failed-query-execution.json')
    for name, identity in read('compressed-identity.json').items():
        compressed = (ROOT / name).read_bytes()
        content = gzip.decompress(compressed)
        assert len(compressed) == identity['bytes']
        assert hashlib.sha256(compressed).hexdigest() == identity['sha256']
        assert len(content) == identity['uncompressed_bytes']
        assert hashlib.sha256(content).hexdigest() == identity['uncompressed_sha256']
    with gzip.open(ROOT / 'failed-request-events.jsonl.gz', 'rt') as stream:
        events = [json.loads(line) for line in stream]
    starts, ends, accounted = {}, {}, {}
    errors = []
    pending = None
    for event in events:
        if event['event'] == 'core_http_started':
            assert pending is None
            key = event['key']['sequence']
            assert key not in starts
            starts[key] = event['monotonic_ns']
            pending = key
        elif event['event'] == 'request':
            assert pending is not None and pending not in accounted
            accounted[pending] = event['monotonic_ns']
            pending = None
        elif event['event'] == 'core_http_finished':
            key = event['key']['sequence']
            assert key not in ends
            ends[key] = event['monotonic_ns']
        elif event['event'] == 'core_http_error':
            errors.append(event)
    assert pending is None
    assert set(starts) == set(ends) == set(accounted) == set(range(310))
    assert len(errors) == 1
    error = errors[0]
    sequence = error['key']['sequence']
    assert sequence == 302
    assert error['reason'] == analysis['transport_error']
    assert error['reason']['exception_type'] == 'httpx.ReadError'
    assert error['reason']['cause_types'][-1] == 'builtins.BrokenPipeError'
    assert math.isclose((ends[sequence] - starts[sequence]) / 1e6,
                        analysis['http_duration_ms'], abs_tol=1e-9)
    assert math.isclose((accounted[sequence] - starts[sequence]) / 1e6,
                        analysis['pre_send_prefix_ms'], abs_tol=1e-9)
    assert ends[sequence] <= error['monotonic_ns'] < execution['t_query_terminal_ns']
    assert analysis['http_duration_ms'] < analysis['configured_http_deadline_ms']
    assert execution['t_cancel_triggered_ns'] is None
    assert execution['received_rows'] == analysis['failed_query_received_rows'] == 246
    assert execution['status'] == 'failed' and execution['partial_results_are_provisional']

    measured = sum(analysis['measured_queries_by_role'].values())
    assert measured == 1093
    assert analysis['accepted_queries_including_checks_and_warmup'] == measured + 16
    assert analysis['accepted_successful_model_calls'] == measured * 512 + 8 * 512 + 8 * 16
    assert analysis['recorded_request_attempts'] - analysis['accepted_successful_model_calls'] == 310
    assert analysis['service_success_count'] - analysis['accepted_successful_model_calls'] == 309

    fixture_hash = hashlib.sha256(b'{"fixture":"POSITIVE"}').hexdigest()
    local_requests = 0
    for mode in ['reset', 'plain', 'fresh']:
        probe = read(mode + '.json')
        assert probe['real_model_requests'] == probe['postgres_queries'] == probe['gpu_usage'] == 0
        assert len(probe['trials']) == 5
        assert probe['status'] == ('failed' if mode == 'reset' else 'passed')
        for trial in probe['trials']:
            assert trial['cleanup_passed']
            assert len(trial['served']) == len(trial['results']) == 2
            assert trial['served'] == [{'connection_request': 1},
                                      {'connection_request': 1 if mode == 'fresh' else 2}]
            counts = collections.Counter(event['event'] for event in trial['observed'])
            assert counts['http_started'] == counts['http_finished'] == 2
            assert counts['http_error'] == (1 if mode == 'reset' else 0)
            for index, row in enumerate(trial['results']):
                if mode == 'reset' and index == 1:
                    assert row['status'] == 'failed'
                    assert row['reason']['exception_type'] == 'httpx.ReadError'
                    assert row['reason']['cause_types'][-1] == 'builtins.ConnectionResetError'
                else:
                    assert row == {'status': 'passed', 'sha256': fixture_hash}
            local_requests += len(trial['served'])
    assert local_requests == analysis['diagnostic_local_http_requests'] == 30
    idle = read('idle-summary.json.gz')
    assert idle['status'] == 'passed' and len(idle['cases']) == 15
    assert idle['real_model_requests'] == idle['postgres_queries'] == idle['gpu_usage'] == 0
    idle_hash = hashlib.sha256(b'{}').hexdigest()
    for case in idle['cases']:
        assert case['cleanup_passed'] and case['matches_prediction']
        assert case['request_attempts'] == 2
        assert case['results'][0] == {'status': 'passed', 'sha256': idle_hash}
        second_trace = [row['event'] for row in case['trace'] if row['second_request']]
        if case['mode'] == 'current':
            assert case['results'][1]['reason'] == analysis['transport_error']
            assert 'connection.connect_tcp.started' not in second_trace
            assert 'http11.receive_response_headers.failed' in second_trace
            assert case['injected_pauses'] == 1
        else:
            assert case['results'][1] == {'status': 'passed', 'sha256': idle_hash}
            assert case['injected_pauses'] == 0
            assert ('connection.connect_tcp.started' in second_trace) == (case['mode'] == 'retire')
    control = read('constant-pause-summary.json.gz')
    assert control['status'] == 'passed' and len(control['trials']) == 5
    assert control['real_model_requests'] == 0 and not control['production_code_changed']
    source = (ROOT / 'run_idle_fixture.py').read_text()
    marker = "state['second'] and not state['new_connection'] and event"
    assert source.count(marker) == 1
    mutated = source.replace(marker, "state['second'] and event")
    assert hashlib.sha256(source.encode()).hexdigest() == control['source_sha256']
    assert hashlib.sha256(mutated.encode()).hexdigest() == control['controlled_source_sha256']
    for case in control['trials']:
        assert case['injected_pauses'] == 1 and case['cleanup_passed']
        assert case['client_expiry_seconds'] == 4
        assert case['server_idle_timeout_seconds'] == 5
        assert case['injected_pause_seconds'] == .3
        assert case['server_received_requests'] == 2
        assert all(row == {'status': 'passed', 'sha256': idle_hash} for row in case['results'])
        second_trace = [row['event'] for row in case['trace'] if row['second_request']]
        assert 'connection.connect_tcp.started' in second_trace
    scout = read('idle-exploratory.json')
    assert scout['reason'] == analysis['transport_error']
    assert scout['local_request_attempts'] == 2 and scout['real_model_requests'] == 0
    assert local_requests + idle['local_request_attempts'] + control['local_request_attempts'] + scout['local_request_attempts'] == analysis['diagnostic_local_request_attempts'] == 72
    identity = read('private-backup-identity.json')
    verification = read('backup-verification.json')
    assert identity['archive_sha256'] == verification['archive_sha256']
    assert identity['archive_bytes'] == verification['archive_bytes']
    assert verification['cross_machine_verification_passed'] and verification['verified_members'] == 36
    assert read('idle-backup-verification.json')['verified_members'] == 18
    assert read('constant-pause-backup-verification.json')['verified_members'] == 7
    assert analysis['precise_socket_close_trigger'] is None
    assert analysis['retained_server_resolved_keepalive_seconds'] is None
    assert not analysis['production_code_changed'] and not analysis['original_run_retried']
    result = dict(status='passed', retained_http_attempts=310, original_read_errors=1,
                  measured_queries=measured, local_fixture_request_attempts=72,
                  matched_broken_pipe_trials=5, retirement_with_constant_pause_trials=5,
                  replay_model_requests=0, exact_idle_expiry_cause='unavailable')
    (ROOT / 'replay-result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
