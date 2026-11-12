"""Finite colocated/P-D correctness worker; no retries or performance claims.

The caller owns preflight, the host lease, persistent attempt reservations, the
outer process supervisor and independent GPU-exit observation.
"""
import argparse
import ast
from dataclasses import replace
import os
from pathlib import Path
import re
import time

from src.infrastructure.private_artifacts import new_private_directory, read_json, write_private_json
from src.infrastructure.redact import redact_json_values, redact_text
from src.serving.backends.prefill_decode_process import NixlPlacement, PrefillDecodeProcess
from src.serving.backends.staged_generation_process import ChainEngineProcess
from src.serving.backends.stage_process_cleanup import StageEngineCleanup
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec, _prepared


def audit_native_transfers(report, decode_log):
    """Associate fixed v0.25.1 native receipts through returned remote IDs.

    Internal request suffixes are not interpreted. Public native counters may
    omit output-free sync iterations, so this audit uses actual completion logs.
    """
    starts = re.findall(r'start_load_kv for request (\S+)', decode_log)
    received = re.findall(r'Finished recving KV transfer for request (\S+)', decode_log)
    finished, external = {}, {}
    for line in decode_log.splitlines():
        match = re.search(r'request_finished\(([^)]+)\), request_status=(\w+), kv_transfer_params=(\{.*\})', line)
        if match:
            native_id, status, encoded = match.groups()
            params = ast.literal_eval(encoded)
            remote_id = params['remote_request_id']
            if remote_id in finished:
                raise ValueError('ambiguous native transfer finish receipt')
            finished[remote_id] = (native_id, status, params.get('_remote_blocks_processed') is True)
        match = re.search(r'update_state_after_alloc: num_external_tokens=([1-9][0-9]*), kv_transfer_params=(\{.*\})', line)
        if match:
            count, encoded = match.groups()
            remote_id = ast.literal_eval(encoded)['remote_request_id']
            if remote_id in external:
                raise ValueError('ambiguous native external-token receipt')
            external[remote_id] = int(count)
    receipts = []
    for result in report['arms']['split']['results']:
        producer = result['stage_receipts']['prefill'][0]
        remote_id = producer['kv_transfer_params']['remote_request_id']
        native_id, status, processed = finished.get(remote_id, (None, None, False))
        receipts.append(dict(call_id=result['call_id'], producer_native_id=remote_id,
            decoder_native_id=native_id, external_tokens=external.get(remote_id),
            completed=(processed and starts.count(native_id) == received.count(native_id) == 1
                       and status in ('FINISHED_STOPPED', 'FINISHED_LENGTH_CAPPED')
                       and remote_id in external)))
    expected = {row['decoder_native_id'] for row in receipts}
    return dict(complete=bool(receipts) and all(row['completed'] for row in receipts)
                and len(expected) == len(receipts) and set(starts) == set(received) == expected,
                source='vLLM 0.25.1 NIXL load, receive and request-finished logs',
                transfers=receipts)


def run(config, directory):
    if os.environ.get('VLLM_LOGGING_LEVEL') != 'DEBUG':
        raise ValueError('this diagnostic worker requires VLLM_LOGGING_LEVEL=DEBUG for transfer receipts')
    engine = EngineSpec.from_dict(config['engine'])
    generation = GenerationSpec.from_dict(config['generation'])
    records = config['rows']
    identities, _ = _prepared(records)
    if not records or config['reserved_physical_calls'] != 3 * len(records):
        raise ValueError('reserve one reference and two stage calls per input row')
    engine.validate_driver_environment()
    engine = replace(engine, max_model_calls=len(records))
    placements = {name: NixlPlacement(**config[name]) for name in ('prefill', 'decode')}
    directory = Path(directory)
    new_private_directory(directory)
    report = dict(status='failed', logical_requests=len(identities),
                  reserved_physical_calls=config['reserved_physical_calls'], arms={})
    deadline = time.monotonic() + config['worker_timeout_s']
    try:
        for name in ('colocated', 'split'):
            control = directory / name
            control.mkdir(mode=0o700)
            owner = None
            arm = report['arms'][name] = {}
            try:
                if name == 'colocated':
                    owner = ChainEngineProcess(engine, generation, control, deadline,
                        config['startup_timeout_s'], worker_env=placements['prefill'].environment(),
                        worker_log=control / 'native.log')
                    arm['ready'] = owner.ready_report
                else:
                    owner = PrefillDecodeProcess(engine, generation, control, deadline,
                        config['startup_timeout_s'], **placements,
                        kv_buffer_device=config['kv_buffer_device'])
                    arm['ready'] = owner.ready()
                started = time.monotonic()
                # Keep the colocated native batching and split request sequence matched.
                arm['results'] = []
                for row in records:
                    received = owner.run_prepared([row], generation=generation)
                    arm['results'].extend(received)
                    if len(received) != 1 or received[0]['status'] != 'succeeded':
                        raise RuntimeError('native request failed or has inconsistent result count')
                arm['requests_elapsed_s'] = time.monotonic() - started
            finally:
                if owner is not None:
                    arm['close'] = owner.close(timeout=config['close_timeout_s'])
                arm['registered_process_cleanup'] = StageEngineCleanup(control).stop(
                    grace_s=config['close_timeout_s'], started=True)
                write_private_json(directory / (name + '.json'), redact_json_values(arm))
            if (not arm['registered_process_cleanup']['process_exit_confirmed']
                    or arm.get('close', {}).get('status') != 'closed'
                    or any(row['status'] != 'succeeded' for row in arm['results'])):
                raise RuntimeError('native arm did not finish requests and cleanup successfully')
        compared = ('call_id', 'messages_sha256', 'prompt_token_ids_json',
                    'output_token_ids_json', 'text', 'finish_reason')
        reference = report['arms']['colocated']['results']
        split = report['arms']['split']['results']
        report['parity'] = [dict(call_id=a['call_id'],
            equal=all(a[key] == b[key] for key in compared)) for a, b in zip(reference, split, strict=True)]
        if not all(row['equal'] for row in report['parity']):
            raise RuntimeError('native split output differs from colocated reference')
        report['native_transfer_audit'] = audit_native_transfers(
            report, (directory / 'split/decode.log').read_text())
        if not report['native_transfer_audit']['complete']:
            raise RuntimeError('native KV receive completion was not fully observed')
        report['status'] = 'passed'
    except BaseException as error:
        report['error'] = redact_text(type(error).__name__ + ': ' + str(error))
        raise
    finally:
        write_private_json(directory / 'report.json', redact_json_values(report))
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    run(read_json(args.config), args.output)
