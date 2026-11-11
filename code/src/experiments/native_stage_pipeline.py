"""Finite resident native-batch versus asynchronous P/D request replay.

The caller supplies preflight, exclusive host ownership, a durable reservation,
outer supervision and independent GPU-exit evidence. This is not a LOTUS query.
"""
import argparse
from dataclasses import replace
from pathlib import Path
import time

from src.baselines.text.frameworks.sync_reference_engine import ReferenceChainEngineProcess
from src.infrastructure.private_artifacts import content_digest, new_private_directory, read_json, write_private_json
from src.infrastructure.redact import redact_json_values, redact_text
from src.serving.backends.pipelined_prefill_decode import PipelinedPrefillDecodeProcess
from src.serving.backends.prefill_decode_process import NixlPlacement
from src.serving.backends.stage_process_cleanup import StageEngineCleanup
from src.serving.backends.vllm_protocol import AsyncEngineLimits, EngineSpec, GenerationSpec, _prepared


PARITY_FIELDS = ('call_id', 'messages_sha256', 'prompt_token_ids_json',
                 'output_token_ids_json', 'text', 'finish_reason')


def result_identity(results, expected):
    if len(results) != len(expected) or any(
            result.get('status') != 'succeeded' or result.get('call_id') != row['call_id']
            or result.get('messages_sha256') != row['messages_sha256']
            for result, row in zip(results, expected, strict=True)):
        raise RuntimeError('request replay has failed, missing or reassociated results')
    return content_digest([{key: row[key] for key in PARITY_FIELDS} for row in results])


def run(config, directory):
    engine = EngineSpec.from_dict(config['engine'])
    generation = GenerationSpec.from_dict(config['generation'])
    rows = config['rows']
    expected, _ = _prepared(rows)
    arm, warmups, rounds = config['arm'], config['warmup_rounds'], config['rounds']
    if (arm not in ('colocated', 'pipeline') or not rows
            or any(type(x) is not int or x < 1 for x in (warmups, rounds))):
        raise ValueError('explicit arm, nonempty rows and positive warmup/measurement rounds required')
    calls = len(rows) * (warmups + rounds)
    if config['reserved_physical_calls'] != calls * (2 if arm == 'pipeline' else 1):
        raise ValueError('physical reservation must cover every stage and warmup')
    engine.validate_driver_environment()
    engine = replace(engine, max_model_calls=calls)
    directory = Path(directory)
    new_private_directory(directory)
    control = directory / 'control'
    control.mkdir(mode=0o700)
    deadline = time.monotonic() + config['worker_timeout_s']
    report = dict(status='failed', arm=arm, logical_rows=len(rows), rounds=[],
                  reserved_physical_calls=config['reserved_physical_calls'])
    owner = None
    first_digest = None
    try:
        started = time.monotonic()
        if arm == 'colocated':
            owner = ReferenceChainEngineProcess(engine, generation, control, deadline,
                config['startup_timeout_s'], worker_log=control/'native.log')
            report['ready'] = owner.ready_report
        else:
            owner = PipelinedPrefillDecodeProcess(engine, generation, control, deadline,
                config['startup_timeout_s'],
                prefill=NixlPlacement(**config['prefill']), decode=NixlPlacement(**config['decode']),
                limits=AsyncEngineLimits(**config['limits']), batch_size=config['batch_size'],
                prefill_window=config['prefill_window'], close_timeout_s=config['close_timeout_s'])
            report['ready'] = owner.ready()
        report['startup_s'] = time.monotonic()-started
        for index in range(warmups+rounds):
            started = time.monotonic()
            results = owner.run_prepared(rows, generation=generation)
            elapsed = time.monotonic()-started
            # Evidence serialization and parity checks are outside the request timer.
            if index == 0:
                write_private_json(directory/'first-results.json', redact_json_values(results))
            try:
                digest = result_identity(results, expected)
                if first_digest is not None and digest != first_digest:
                    raise RuntimeError('resident repeat differs from its first complete output')
            except Exception:
                write_private_json(directory/('failed-results-'+str(index)+'.json'), redact_json_values(results))
                raise
            first_digest = digest
            item = dict(round=index, warmup=index < warmups, elapsed_s=elapsed,
                        result_sha256=digest,
                        prompt_tokens=sum(row['prompt_tokens'] for row in results),
                        output_tokens=sum(row['completion_tokens'] for row in results))
            if arm == 'pipeline':
                item['pipeline'] = owner.last_run_report
            report['rounds'].append(item)
            write_private_json(directory/('round-'+str(index)+'.json'), redact_json_values(item))
        report['result_sha256'] = first_digest
        report['measured_s'] = sum(row['elapsed_s'] for row in report['rounds'] if not row['warmup'])
        report['status'] = 'passed'
    except BaseException as error:
        report['error'] = redact_text(type(error).__name__+': '+str(error))
        raise
    finally:
        try:
            if owner is not None:
                report['close'] = owner.close(timeout=config['close_timeout_s'])
        except Exception as error:
            report['close'] = dict(status='unknown', error_type=type(error).__name__)
        finally:
            report['cleanup'] = StageEngineCleanup(control).stop(
                grace_s=config['close_timeout_s'], started=True)
        if not report['cleanup']['process_exit_confirmed'] or report.get('close', {}).get('status') != 'closed':
            report['status'] = 'failed'
        write_private_json(directory/'report.json', redact_json_values(report))
    if report['status'] != 'passed':
        raise RuntimeError('request replay cleanup was not confirmed')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    run(read_json(args.config), args.output)
