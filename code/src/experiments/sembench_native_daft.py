"""Independent SemBench JSON tasks over Daft's native AI prompt graph.

The JSON task semantics are an engineering comparison shared with other
native systems, not an author method or a LOTUS prompt/parser implementation.
The existing prepared worker owns native model execution and ACK-based close.
"""

import argparse
from copy import deepcopy
import json
import math
from pathlib import Path
import time

from src.baselines.text.sembench_native_text import TASK_SOURCE_FIELDS, task_from_config
from src.baselines.text.sembench_source import verify_checkout
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.native_daft_query import _save_artifact, supervised_run
from src.infrastructure.private_artifacts import (
    file_sha256, json_metric, new_private_directory, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text


SCHEMA = 'semloom.sembench_native_daft.v1'


def configuration(config):
    fields = {'schema', 'sembench_checkout', 'query', 'query_id', 'model',
              'sampling_params', 'engine_kwargs', 'provider_options', 'max_model_calls',
              'query_timeout_s', 'cleanup_s', 'stop_grace_s'}
    if (type(config) is not dict or not fields <= config.keys()
            or config.keys() - fields - {'scenario'} - TASK_SOURCE_FIELDS or config['schema'] != SCHEMA):
        raise ValueError('an explicit independent native Daft JSON configuration is required')
    task = task_from_config(config)
    for name in ('sampling_params', 'engine_kwargs', 'provider_options'):
        if type(config[name]) is not dict:
            raise ValueError(name + ' must be an explicit native object')
    if (type(config['sampling_params'].get('n')) is not int or config['sampling_params']['n'] != 1
            or config['sampling_params'].get('response_format') != task.response_format):
        raise ValueError('native JSON tasks require n=1 and the independent JSON object response format')
    for name in ('query_timeout_s', 'cleanup_s', 'stop_grace_s'):
        value = config[name]
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError(name + ' must be finite and positive')
    if config['stop_grace_s'] >= config['cleanup_s']:
        raise ValueError('forced process stop must fit inside the cleanup allowance')
    if not isinstance(config['model'], str) or not config['model'].strip():
        raise ValueError('the native model must be explicit')
    AttemptBudget(config['query_id'], config['max_model_calls'])
    from src.baselines.text.frameworks.daft_sembench_native import _provider_options
    _provider_options(config['provider_options'])
    verify_checkout(config['sembench_checkout'])
    return task


def check(config):
    task = configuration(config)
    task.prepare_evaluation()
    source = task.read_source()
    if task.model_request_count(source) > config['max_model_calls']:
        raise ValueError('the finite native budget cannot cover the eligible source')
    description = task.describe(source)
    return dict(schema=SCHEMA, status='checked_cpu_only', task=description,
                execution_enabled=False, model_calls=0, target_native_model_qualified=False)


def run_query(config, directory, *, model_python, tokenizer=None, supervise=supervised_run, monitor=None):
    started = time.monotonic_ns()
    task = configuration(config)
    if tokenizer is None:
        from transformers import AutoTokenizer
        engine = config['engine_kwargs']
        revision = engine.get('tokenizer_revision') or engine.get('revision')
        tokenizer = AutoTokenizer.from_pretrained(engine.get('tokenizer', config['model']),
            local_files_only=True, trust_remote_code=False,
            **(dict(revision=revision) if revision is not None else {}))
    directory = Path(directory)
    new_private_directory(directory)
    report = dict(schema='semloom.sembench_native_daft.result.v1', status='failed',
        baseline_identity='daft-native-project-json', comparison_identity='统一语义工程对照',
        semantic_owner='independent text task JSON prompt/parser',
        execution_owner='Daft native vllm-prefix-caching AI expression',
        author_original_semantic_method=False, lotus_sdk_required=False,
        worker_entry_ns=started, source_start_ns=None, sink_ready_ns=None, source_to_sink_seconds=None,
        query_clock_version='native_daft.source_to_sink.v1', model_ready_before_source=False,
        model_startup_seconds=None, physical_gpu_exit_confirmed=None, performance_claim=False,
        warmup=0, whole_query_retries=0,
        timing_scope='fresh CSV read, independent messages, native chat template, prepared packet/digest I/O, '
            'cold native Daft graph and native evidence I/O, IPC/polling, complete strict task JSON relation sink; '
            'preflight, tokenizer loading and budget claim precede source; final native close/process exit, '
            'source identity audit, official evaluation and result persistence follow sink')
    ledger = unit = primary = source = frame = None
    requests = []
    try:
        evaluation = task.prepare_evaluation()
        budget = AttemptBudget(config['query_id'], config['max_model_calls'])
        ledger = CellBudgetLedger.create(directory / 'physical-budget.sqlite', budget,
                                         deadline_utc=time.time() + config['query_timeout_s'])
        ledger.reserve_unit('query', budget.limit)
        unit = ledger.claim_shared_unit('query')
        report['preflight_finished_ns'] = time.monotonic_ns()
        report['source_start_ns'] = time.monotonic_ns()
        deadline = report['source_start_ns'] / 1e9 + config['query_timeout_s']
        source = task.read_source()
        if task.model_request_count(source) > unit.remaining:
            raise ValueError('the finite native budget cannot cover the eligible source')
        requests = task.requests(source)

        def finish(predictions):
            nonlocal frame
            frame = task.finish(source, predictions)
            report['sink_ready_ns'] = time.monotonic_ns()
            report['source_to_sink_seconds'] = (report['sink_ready_ns'] - report['source_start_ns']) / 1e9
            if report['sink_ready_ns'] / 1e9 >= deadline:
                raise TimeoutError('independent Daft complete sink exceeded its query allowance')

        if requests:
            prepared = [dict(call_id=row['call_id'], prompt=tokenizer.apply_chat_template(
                row['messages'], tokenize=False, add_generation_prompt=True)) for row in requests]
            packet, worker_path = directory / 'prepared-prompts.json', directory / 'worker-config.json'
            write_private_json(packet, dict(schema='semloom.native_daft_prompts.v1', requests=prepared))
            worker = {name: deepcopy(config[name]) for name in
                      ('model', 'sampling_params', 'engine_kwargs', 'provider_options', 'cleanup_s', 'stop_grace_s')}
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('independent Daft source preparation exceeded its query allowance')
            worker.update(schema='semloom.native_daft_map.v1', query_timeout_s=remaining,
                requests_path=str(packet), requests_sha256=file_sha256(packet),
                request_budget=dict(path=str(directory / 'physical-budget.sqlite'),
                    budget_id=budget.budget_id, limit=budget.limit, unit_id='query'), wait_for_result_release=True)
            write_private_json(worker_path, worker)

            def consume(raw):
                finish(raw['result']['predictions'])

            report['supervision'] = supervise(worker_path, directory / 'native-worker', model_python=model_python,
                monitor=monitor, on_result_ready=consume)
            terminal = read_json(directory / 'native-worker' / 'worker.json')
            report.update(native_api=terminal.get('native_api'), model_close_receipt=terminal.get('model_close_receipt'),
                          result_release=terminal.get('result_release'))
            if terminal['status'] != 'completed' or frame is None:
                raise RuntimeError('the native worker did not establish a complete sink and cleanup')
        else:
            finish([])
            report.update(model_ready_before_source=None, model_close_receipt=None,
                          no_model_reason='no eligible semantic occurrences; no native worker or model started')
        if unit.attempts != len(requests):
            raise RuntimeError('permanent physical attempts differ from independent task occurrences')
        report['source_audit_started_ns'] = time.monotonic_ns()
        task.verify_source(source)
        report['task'] = task.describe(source)
        report['evaluation_started_ns'] = time.monotonic_ns()
        report['quality'] = json_metric(task.evaluate(source, frame, checkout=config['sembench_checkout'], evaluation=evaluation))
        report['evaluation_finished_ns'] = time.monotonic_ns()
        report['status'] = 'completed'
    except BaseException as error:
        primary = error
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        if hasattr(error, 'report'):
            report['supervision'] = error.report
    finally:
        if ledger is not None:
            try:
                ledger.close_shared_unit('query')
                report['physical_attempts_reserved'] = unit.attempts
                report['budget'] = ledger.snapshot()
            except BaseException as error:
                if primary is None:
                    primary = error
                else:
                    primary.add_note('native budget close failed: ' + redact_text(str(error)))
        if primary is not None:
            report['status'] = 'failed'
            report.pop('quality', None)
        if frame is not None:
            try:
                report['result'] = task.encode_result(frame)
            except BaseException as error:
                report.update(status='failed', result_encoding_error_type=type(error).__name__)
                report.pop('quality', None)
                if primary is None:
                    primary = error
                    report.update(error_type=type(error).__name__, error=redact_text(str(error)))
                else:
                    primary.add_note('native result encoding failed: ' + redact_text(str(error)))
        report['logical_model_requests'] = len(requests)
        try:
            _save_artifact(directory / 'driver.json', report, primary=primary)
        except BaseException as error:
            primary = error
            report.update(status='failed', error_type=type(error).__name__, error=redact_text(str(error)))
            report.pop('quality', None)
        if primary is not None:
            primary.query_report = report
            if frame is not None:
                primary.query_result = frame
            raise primary
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'run'))
    parser.add_argument('--config', required=True)
    parser.add_argument('--output-dir')
    parser.add_argument('--model-python')
    parser.add_argument('--execute-model', action='store_true')
    args = parser.parse_args(argv)
    config = read_json(args.config)
    if args.action == 'check' or not args.execute_model:
        print(json.dumps(check(config), ensure_ascii=False, allow_nan=False))
        return 0
    if args.output_dir is None or args.model_python is None:
        parser.error('executing the native query requires --output-dir and --model-python')
    run_query(config, args.output_dir, model_python=args.model_python)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
