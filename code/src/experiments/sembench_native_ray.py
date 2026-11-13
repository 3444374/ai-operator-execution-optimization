"""Project-owned JSON tasks through Ray's native public LLM processor.

This is independent of LOTUS prompts, parsers and execution. The caller owns
the existing supervised worker, host lease and physical resource checks.
"""

import argparse
import json
from pathlib import Path
import time

from src.baselines.text.frameworks.ray_data_sembench_native import query_scope
from src.baselines.text.frameworks.ray_native_observer import summarize
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.infrastructure.private_artifacts import (
    json_metric, new_private_directory, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text


def _sampling(config):
    sampling = dict(config['sampling_params'])
    if (type(sampling.get('n')) is not int or sampling['n'] != 1
            or sampling.get('structured_outputs') != {'json_object': True}):
        raise ValueError('native JSON tasks require n=1 and native JSON-object generation')
    return sampling


def _image_stage_enabled(options):
    stage = options.get('prepare_image_stage', False)
    if isinstance(stage, bool):
        return stage
    if isinstance(stage, dict):
        from pydantic import TypeAdapter
        # Ray 2.56.1 parses dicts with PrepareImageStageConfig(enabled=True).
        return TypeAdapter(bool).validate_python(stage.get('enabled', True))
    return stage.enabled


def run_query(task, config, directory, *, checkout, scope=query_scope):
    """Keep fresh source, prediction, strict parsing and full sink inside one owner.

    ``task`` is a neutral SemBench text task. ``budget`` identifies a previously
    reserved permanent unit; native AsyncLLM.generate charges each occurrence.
    Sampling and engine settings are explicit and retain native Ray execution.
    """
    entered = time.monotonic_ns()
    sampling = _sampling(config)
    directory = Path(directory).resolve()
    new_private_directory(directory)
    report = dict(schema='semloom.sembench_native_ray.result.v1', status='failed',
        baseline_identity='ray-native-project-json', backend='ray-data-native',
        comparison_identity='统一语义工程对照',
        semantic_owner='project unified JSON task semantics; not author-original or LOTUS semantics',
        author_original_semantic_method=False, lotus_sdk_required=False,
        execution_owner='Ray Data public LLM processor',
        worker_entry_ns=entered, source_started_ns=None, sink_ready_ns=None,
        source_to_sink_seconds=None, query_clock_version='native_ray.source_to_sink.v1',
        model_ready_before_source=False,
        model_ready_before_source_reason='lazy query scope; native model starts in the first nonempty prediction batch',
        physical_gpu_exit_confirmed=None, performance_claim=False, warmup_calls=0,
        result_cache_enabled=False, requested_generate_retries=0,
        timing_scope='fresh task source, necessary image reads/RGB-to-PNG content, project messages, '
            'native startup/prediction, strict task JSON parsing and complete memory sink; '
            'mandatory budget reservation and synchronous native observer writes included; '
            'source verification, evaluation, result persistence and final runtime close excluded')
    source = result = native = ledger = unit = primary = None
    requests = []

    def failed(error, phase):
        nonlocal primary
        report['status'] = 'failed'
        report.pop('quality', None)
        if primary is None:
            primary = error
            report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        else:
            primary.add_note(phase + ' also failed: ' + type(error).__name__)

    try:
        evaluation = task.prepare_evaluation()
        value = config['budget']
        ledger = CellBudgetLedger(Path(value['path']), AttemptBudget(value['id'], value['limit']))
        unit = ledger.claim_shared_unit(value['unit_id'])
        runtime_env = dict(py_executable=config['model_python'], env_vars=dict(
            PYTHONPATH=str(Path(__file__).parents[2]),
            SEMLOOM_RAY_NATIVE_OBSERVER=json.dumps(dict(value, output=str(directory)))))
        with scope(model=config['model'], engine_kwargs=config['engine_kwargs'],
                processor_options=config['processor_options'], runtime_options=config['runtime_options'],
                engine_runtime_env=runtime_env) as owner:
            native = owner.report
            report['source_started_ns'] = time.monotonic_ns()
            source = task.read_source()
            required = task.model_request_count(source)
            if required > unit.remaining:
                raise BudgetExhausted('permanent budget cannot cover every source occurrence')
            requests = task.requests(source)
            report['logical_requests'] = len(requests)
            images = any(isinstance(message['content'], list)
                and any(part.get('type') == 'image_url' for part in message['content'])
                for request in requests for message in request['messages'])
            if images and not _image_stage_enabled(config['processor_options']):
                raise ValueError('native image content requires enabled prepare_image_stage')
            if not requests:
                report.update(model_ready_before_source=None,
                    model_ready_before_source_reason='no eligible occurrences; native runtime and model were not started')
            batch = owner.predict(requests, sampling_params=sampling)
            result = task.finish(source, batch['predictions'])
            report['sink_ready_ns'] = time.monotonic_ns()
            report['source_to_sink_seconds'] = (report['sink_ready_ns'] - report['source_started_ns']) / 1e9
        report['post_sink_audit_started_ns'] = time.monotonic_ns()
        task.verify_source(source)
        report['task'] = task.describe(source)
        observed = report['native_observation'] = summarize(directory)
        if unit.attempts != len(requests) or (requests and (
                not observed['available'] or len(observed['submitted_attempts']) != len(requests)
                or len(set(observed['submitted_attempts'])) != len(requests)
                or sorted(observed['submitted_attempts']) != sorted(observed['terminal_attempts'])
                or observed['unknown_attempts'])):
            raise RuntimeError('native charged submissions and confirmed terminals differ from complete occurrences')
        report['post_sink_audit_finished_ns'] = time.monotonic_ns()
        report['evaluation_started_ns'] = time.monotonic_ns()
        report['quality'] = json_metric(task.evaluate(source, result, checkout=checkout, evaluation=evaluation))
        report['evaluation_finished_ns'] = time.monotonic_ns()
        report['status'] = 'completed'
    except BaseException as error:
        failed(error, 'query')
    finally:
        if unit is not None:
            try:
                ledger.close_shared_unit(config['budget']['unit_id'])
                report['physical_attempts_reserved'] = unit.attempts
                report['permanent_budget'] = ledger.snapshot()
            except BaseException as error:
                failed(error, 'budget finalization')
        try:
            if result is not None:
                report['result'] = task.encode_result(result)
            if native is not None:
                write_private_json(directory / 'native-predictions.json', native)
                report['native'] = {key: value for key, value in native.items() if key != 'batches'}
            write_private_json(directory / 'requests.json', requests)
        except BaseException as error:
            failed(error, 'native evidence persistence')
        try:
            write_private_json(directory / 'query.json', report)
        except BaseException as error:
            failed(error, 'report persistence')
    if primary is not None:
        primary.query_report = report
        raise primary
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'run'))
    parser.add_argument('--config', required=True)
    parser.add_argument('--output-dir')
    parser.add_argument('--execute-model', action='store_true')
    args = parser.parse_args(argv)
    config = read_json(args.config)
    if config.get('schema') != 'semloom.sembench_native_ray.v1':
        raise ValueError('an explicit independent native Ray configuration is required')
    from src.baselines.text.sembench_native_text import task_from_config
    from src.baselines.text.sembench_source import verify_checkout
    verify_checkout(config['sembench_checkout'])
    task = task_from_config(config)
    if args.action == 'check' or not args.execute_model:
        _sampling(config)
        task.prepare_evaluation()
        source = task.read_source()
        if task.model_request_count(source) > config['budget']['limit']:
            raise ValueError('the permanent native budget cannot cover the eligible source')
        description = task.describe(source)
        if description.get('input_modality') == 'image' and not _image_stage_enabled(config['processor_options']):
            raise ValueError('native image content requires enabled prepare_image_stage')
        print(json.dumps(dict(status='checked_cpu_only', execution_enabled=False,
            task=description, model_calls=0), ensure_ascii=False, allow_nan=False))
        return 0
    if args.output_dir is None:
        parser.error('executing the native query requires --output-dir')
    execution = {name: config[name] for name in ('model', 'model_python', 'engine_kwargs',
        'processor_options', 'runtime_options', 'sampling_params', 'budget')}
    run_query(task, execution, args.output_dir, checkout=config['sembench_checkout'])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
