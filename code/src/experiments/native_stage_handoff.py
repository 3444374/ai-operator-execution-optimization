"""Finite colocated/P-D correctness worker; no retries or performance claims.

The caller owns preflight, the host lease, persistent attempt reservations, the
outer process supervisor and independent GPU-exit observation.
"""
import argparse
from dataclasses import replace
from pathlib import Path
import time

from src.infrastructure.private_artifacts import new_private_directory, read_json, write_private_json
from src.infrastructure.redact import redact_json_values, redact_text
from src.serving.backends.prefill_decode_process import NixlPlacement, PrefillDecodeProcess
from src.serving.backends.staged_generation_process import ChainEngineProcess
from src.serving.backends.stage_process_cleanup import StageEngineCleanup
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec, _prepared


def run(config, directory):
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
