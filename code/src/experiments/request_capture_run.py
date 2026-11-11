"""Replay complete prepared requests through the existing native process owner.

This is a request-layer experiment, not a semantic query or a baseline executor.
The enclosing record-observation workflow owns the lease and process supervisor.
"""
from dataclasses import asdict
import json
from pathlib import Path
import time

from src.infrastructure.private_artifacts import file_identity, write_private_json
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_protocol import AsyncEngineLimits, EngineSpec, GenerationSpec, _prepared


def read_capture(config):
    binding = config['workload']['input']
    path = Path(binding['capture'])
    if file_identity(path)['sha256'] != binding['sha256']:
        raise ValueError('request capture changed')
    capture = json.loads(path.read_text())
    requests = capture['requests']
    if (capture['schema'] != 'semloom.semantic_request_capture.v1'
            or len(requests) != config['workload']['expected_calls']):
        raise ValueError('capture must match the complete declared request count')
    identities, _ = _prepared([request['row'] for request in requests])
    generation = GenerationSpec.from_dict(config['generation'])
    if any(GenerationSpec.from_dict(request['generation']) != generation for request in requests):
        raise ValueError('captured generation must remain unchanged')
    limits = AsyncEngineLimits(**config['ipc_limits'])
    group = config['request_group_rows']
    if type(group) is not int or not 1 <= group <= limits.max_inflight:
        raise ValueError('request group must fit the declared native process capacity')
    if len({identity['query_id'] for identity in identities}) != 1:
        raise ValueError('one capture must contain one query')
    return requests, generation, limits


def run_capture(config, ledger, *, process_type=AsyncEngineProcess):
    requests, generation, limits = read_capture(config)
    root = Path(config['artifact_root'])
    claim = ledger.claim_unit(next(iter(config['units'])))
    claim.bind_execution()
    owner = None
    calls, groups = [], []
    pending = set()
    submitted_requests = received_events = confirmed_events = 0
    report = dict(status='failed', scope='complete saved requests; semantic query not executed')

    def collect_receipts():
        nonlocal received_events, confirmed_events
        failed = []
        for event in owner.poll(max_events=limits.result_slots):
            received_events += 1
            confirmed_events += int(event.engine_terminal_confirmed)
            pending.remove(event.request_id)
            if event.result is not None:
                calls.append(event.result)
            if (not event.engine_terminal_confirmed or event.result is None
                    or event.result['status'] != 'succeeded'):
                failed.append(dict(request_id=event.request_id, identity=event.identity,
                    engine_terminal_confirmed=event.engine_terminal_confirmed,
                    error_type=event.error_type))
        if failed:
            report.setdefault('terminal_failures', []).extend(failed)
        return failed

    try:
        opened = time.monotonic_ns()
        owner = process_type(EngineSpec.from_dict(config['engine']), generation, limits=limits,
                             startup_timeout_s=config['resources']['startup_timeout_s'])
        report.update(startup_ns=time.monotonic_ns()-opened, ready=owner.ready_report)
        started = time.monotonic_ns()
        deadline = time.monotonic()+config['resources']['query_timeout_s']
        for offset in range(0, len(requests), config['request_group_rows']):
            members = requests[offset:offset+config['request_group_rows']]
            begin = time.monotonic_ns()
            submitted = owner.submit_batch([item['row'] for item in members], generation=generation,
                                           deadline_monotonic=deadline)
            if submitted.status != 'queued':
                raise RuntimeError('complete request group was not accepted')
            pending = set(submitted.request_ids)
            submitted_requests += len(submitted.request_ids)
            while pending:
                if time.monotonic() >= deadline:
                    raise TimeoutError('saved request experiment deadline expired')
                if collect_receipts():
                    raise RuntimeError('native request did not finish successfully')
                if pending:
                    owner.wait(timeout=min(0.05, max(0.001, deadline-time.monotonic())))
            groups.append(dict(offset=offset, requests=len(members), wall_ns=time.monotonic_ns()-begin))
        report['request_run_ns'] = time.monotonic_ns()-started
        by_id = {call['call_id']:call for call in calls}
        for request in requests:
            if json.loads(by_id[request['row']['call_id']]['prompt_token_ids_json']) != request['token_ids']:
                raise RuntimeError('native tokenizer differs from the saved request tokens')
        report['status'] = 'completed'
    except Exception as error:
        report['error_type'] = type(error).__name__
    finally:
        if owner is not None:
            try:
                report['close'] = owner.close(timeout=config['limits']['owner_close_s'])
            except Exception as error:
                report['close'] = dict(status='unknown', error_type=type(error).__name__)
            try:
                # Close may deliver terminals for the remaining accepted group.
                # One full result-slot page drains the bounded buffered set;
                # unknown close keeps any missing identities explicit.
                if collect_receipts():
                    report['status'] = 'failed'
            except Exception as error:
                report.update(status='failed', receipt_drain_error_type=type(error).__name__)
            if report['close']['status'] != 'closed':
                report['status'] = 'failed'
        report.update(calls=calls, groups=groups, actual_calls=len(calls),
                      actual_calls_scope='retained result records; not physical model submissions',
                      submitted_requests=submitted_requests, received_terminal_events=received_events,
                      confirmed_terminal_events=confirmed_events, unreceived_request_ids=sorted(pending),
                      ipc_limits=asdict(limits), quality='not_evaluated')
        write_private_json(root/'REQUEST-RESULTS.json', report)
    return report
