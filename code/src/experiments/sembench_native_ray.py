"""Project-owned JSON tasks through Ray's native public LLM processor.

This is independent of LOTUS prompts, parsers and execution. The caller owns
the existing supervised worker, host lease and physical resource checks.
"""

import argparse
import json
import math
from pathlib import Path
import time

from src.baselines.text.frameworks.ray_data_sembench_native import image_stage_enabled, query_scope
from src.baselines.text.frameworks.ray_native_observer import summarize
from src.baselines.text.sembench_native_text import native_query_batches
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.infrastructure.private_artifacts import (
    json_metric, new_private_directory, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text


SERVE_SCHEMA = 'semloom.sembench_native_ray_serve_session.v2'


def _sampling(config):
    sampling = dict(config['sampling_params'])
    if (type(sampling.get('n')) is not int or sampling['n'] != 1
            or sampling.get('structured_outputs') != {'json_object': True}):
        raise ValueError('native JSON tasks require n=1 and native JSON-object generation')
    return sampling


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
        native_output_artifacts=False,
        result_cache_enabled=False, requested_generate_retries=0,
        timing_scope='fresh task source, necessary image reads/RGB-to-PNG content, project messages, '
            'native startup/dependent predictions, necessary terminal checks, strict task parsing and complete memory sink; '
            'mandatory API allowance included; no per-request success event or output persistence; '
            'source verification, evaluation, result persistence and final runtime close excluded')
    source = result = native = ledger = unit = primary = None
    requests, predictions = [], []

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

            def sink(value):
                nonlocal result
                result = value
                report['sink_ready_ns'] = time.monotonic_ns()
                report['source_to_sink_seconds'] = (report['sink_ready_ns'] - report['source_started_ns']) / 1e9

            _predict_query_batches(task, source, owner, sampling=sampling, unit=unit,
                requests=requests, counts=report.setdefault('request_batch_counts', []),
                predictions=predictions, sink=sink, processor_options=config['processor_options'])
            if not requests:
                report.update(model_ready_before_source=None,
                    model_ready_before_source_reason='no eligible occurrences; native runtime and model were not started')
        report['post_sink_audit_started_ns'] = time.monotonic_ns()
        task.verify_source(source)
        report['task'] = task.describe(source)
        report['native_observation'] = dict(summarize(directory),
            API_invocations_reserved=unit.attempts, complete_native_API_results=len(predictions),
            completion_source='Ray native processor requires finished output or raises; not a separate engine receipt')
        if unit.attempts != len(requests) or len(predictions) != len(requests):
            raise RuntimeError('native charged API invocations and complete results differ from source occurrences')
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
            report['logical_requests'] = len(requests)
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


def _predict_query_batches(task, source, owner, *, sampling, unit,
                           requests, counts, predictions, sink, processor_options=None):
    """Drive the task's ordinary query generator; native Ray owns every batch."""
    program = native_query_batches(task, source)
    try:
        try:
            pending = next(program)
        except StopIteration as complete:
            sink(complete.value)
            return
        while True:
            counts.append(len(pending))
            if len(pending) > unit.remaining:
                if isinstance(pending, list):
                    requests.extend(pending)
                raise BudgetExhausted('permanent budget cannot cover the next complete native batch')
            if not isinstance(pending, list):
                pending = list(pending)
            requests.extend(pending)
            if processor_options is not None:
                images = any(isinstance(message['content'], list)
                    and any(part.get('type') == 'image_url' for part in message['content'])
                    for request in pending for message in request['messages'])
                if images and not image_stage_enabled(processor_options):
                    raise ValueError('native image content requires enabled prepare_image_stage')
            before = unit.attempts
            batch = owner.predict(pending, sampling_params=sampling)
            predictions.extend(batch['predictions'])
            try:
                following = program.send(batch['predictions'])
            except StopIteration as complete:
                sink(complete.value)
                return
            try:
                if unit.attempts-before != len(pending):
                    raise RuntimeError('native charged attempts differ from complete batch occurrences')
            except BaseException:
                # Preserve locally constructed dependent work, without submitting it.
                if isinstance(following, list):
                    requests.extend(following)
                counts.append(len(following))
                raise
            pending = following
    finally:
        program.close()


def _serve_attempt_window(directory, unit, first_attempt, before, predictions=None):
    """Match API allowance to complete native responses, after the query sink."""
    expected = list(range(first_attempt+before, first_attempt+unit.attempts))
    associations = []
    if predictions is not None:
        response_ids = [row['response_id'] for row in predictions]
        if len(predictions) != len(expected) or len(set(response_ids)) != len(response_ids):
            raise RuntimeError('Serve complete API response IDs do not cover the charged invocations')
        associations = [dict(call_id=row['call_id'], response_id=row['response_id']) for row in predictions]
    return dict(attempts=expected, response_associations=associations,
        complete_native_API_results=None if predictions is None else len(predictions),
        completion_source='complete non-streaming native API responses; not an independent engine-output trace'), summarize(directory)


def _owner_snapshot(snapshot):
    if snapshot is None:
        return None, None
    before = time.monotonic_ns()
    value = snapshot()
    after = time.monotonic_ns()
    fields = {'observed_monotonic_ns', 'node_id', 'model_actor_id', 'model_actor_pid',
              'model_actor_create_time', 'model_processes'}
    if not isinstance(value, dict) or not fields <= value.keys():
        return value, None
    children = value['model_processes']
    if (not before <= value['observed_monotonic_ns'] <= after
            or not value['node_id'] or not value['model_actor_id']
            or type(value['model_actor_pid']) is not int or value['model_actor_pid'] <= 0
            or not isinstance(children, list) or not children):
        return value, None
    processes = [(value['model_actor_pid'], value['model_actor_create_time'])]
    for child in children:
        if not isinstance(child, dict) or not {'pid', 'create_time'} <= child.keys():
            return value, None
        processes.append((child['pid'], child['create_time']))
    if any(type(pid) is not int or pid <= 0 or type(birth) not in (int, float)
           or not math.isfinite(birth) or birth <= 0 for pid, birth in processes):
        return value, None
    return value, (value['node_id'], value['model_actor_id'], processes[0], tuple(sorted(processes[1:])))


def _serve_constructor_events(directory):
    events = [json.loads(line) for path in Path(directory).glob('native-events-*.jsonl')
              for line in path.read_text().splitlines()]
    return sorted((row['monotonic_ns'], row.get('pid'), row['event']) for row in events
        if row['event'] in ('native_constructor_started', 'native_constructor_returned'))


def _native_serve_process(config, directory, runtime_env):
    from src.experiments.native_ray_serve_process import NativeServeProcess

    options = {key: config[key] for key in ('model', 'model_source', 'app_name',
        'deployment_name', 'engine_kwargs', 'processor_options', 'runtime_options')}
    return NativeServeProcess(model_python=config['model_python'], directory=directory,
        startup_timeout_s=config['startup_timeout_s'], session_timeout_s=config['session_timeout_s'],
        cleanup_s=config['cleanup_s'], native_options=dict(options, model_runtime_env=runtime_env))


def run_serve_session(config, directory, *, checkout, session_factory=None,
                      identity_snapshot=None, task_factory=None):
    """Two fresh complete queries, one owned Serve deployment and finite unit.

    The optional host snapshot callback must return freshly observed native model
    actor/PID/birth and actual model subprocess PID/birth on this same host.
    Missing identity evidence leaves warm qualification unknown. The caller
    still owns the existing bounded host supervision and physical GPU checks.
    """
    from src.baselines.text.sembench_native_text import task_from_config

    if config.get('schema') != SERVE_SCHEMA:
        raise ValueError('the explicit independent Ray Serve session schema is required')
    sampling = _sampling(config)
    directory = Path(directory).resolve()
    new_private_directory(directory)
    report = dict(schema='semloom.sembench_native_ray_serve_session.result.v2',
        baseline_identity='native-ray-serve-json-isolated-caller', status='failed',
        semantic_owner='project unified JSON task semantics; not LOTUS or author-original',
        author_original_semantic_method=False, lotus_sdk_required=False,
        execution_owner='Ray Data ServeDeploymentProcessor and owned Ray Serve deployment',
        target_native_model_qualified=None, performance_claim=False, queries=[],
        native_output_artifacts=False, native_output_evidence='complete native API response',
        model_owner_reused=None, warm_qualified=None, extra_query_warmup=0,
        warm_qualification_scope='requires fresh native model actor and actual EngineCore subprocess identities plus '
            'unchanged native constructor events; software fixtures do not qualify a target model',
        native_startup_attempts=None, worker_entry_ns=time.monotonic_ns(),
        timing_scope='deployment startup separately measured; each fresh source, native Serve graph, '
            'dependent batches, native complete response checks, strict task parsing and full memory sink measured directly; API allowance '
            'and necessary cross-environment batch IPC included; no per-request success persistence; post-sink audits/scoring/persistence '
            'and final deployment/caller close excluded')
    primary = ledger = unit = owner = current = None

    def failed(error, phase):
        nonlocal primary
        report['status'] = 'failed'
        report['warm_qualified'] = None
        if current is not None:
            current['status'] = 'failed'
            current.pop('quality', None)
            current.setdefault('error_type', type(error).__name__)
        if primary is None:
            primary = error
            report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        elif primary is not error:
            primary.add_note(phase+' also failed: '+type(error).__name__)

    try:
        factory = task_factory or task_from_config
        tasks = [factory(dict(config, query_id=config['query_id']+'-round-'+str(index+1))) for index in range(2)]
        evaluations = [task.prepare_evaluation() for task in tasks]
        value = config['budget']
        ledger = CellBudgetLedger(Path(value['path']), AttemptBudget(value['id'], value['limit']))
        unit = ledger.claim_shared_unit(value['unit_id'])
        first = next(row['first_attempt'] for row in ledger.snapshot()['units'] if row['unit_id'] == value['unit_id'])
        runtime_env = dict(py_executable=config['model_python'], env_vars=dict(
            PYTHONPATH=str(Path(__file__).parents[2]),
            SEMLOOM_RAY_NATIVE_OBSERVER=json.dumps(dict(value, output=str(directory),
                observe_model_actor_identity=True))))
        if session_factory is None:
            owner = _native_serve_process(config, directory/'native-caller', runtime_env)
        else:
            owner = session_factory(model=config['model'], model_source=config['model_source'],
                app_name=config['app_name'], deployment_name=config['deployment_name'],
                engine_kwargs=config['engine_kwargs'], processor_options=config['processor_options'],
                runtime_options=config['runtime_options'], model_runtime_env=runtime_env)
        owner.start()
        if identity_snapshot is None:
            identity_snapshot = getattr(owner, 'identity_snapshot', None)
        report['native'] = owner.report
        report['native_startup_attempts'], initial = _serve_attempt_window(directory, unit, first, 0)
        constructors = initial['model_constructor_returned_ns']
        if not initial['available'] or not constructors:
            raise RuntimeError('Serve startup did not establish its mandatory native observer')
        constructor_events = _serve_constructor_events(directory)
        baseline_raw, baseline_identity = _owner_snapshot(identity_snapshot)
        report['initial_model_owner'] = baseline_raw
        report['deployment_startup_seconds'] = owner.report['deployment_startup_seconds']
        report['native_caller_startup_seconds'] = owner.report.get('native_caller_startup_seconds')
        identity_complete = baseline_identity is not None
        accounted = unit.attempts
        for index, (task, evaluation) in enumerate(zip(tasks, evaluations, strict=True)):
            current = dict(round=index+1, phase='ready-first' if index == 0 else 'same-owner-repeat',
                status='failed', result=None, model_ready_before_source=None,
                query_clock_version='native_ray_serve.source_to_sink.v2')
            report['queries'].append(current)
            phase = directory/('round-'+str(index+1))
            new_private_directory(phase)
            requests, predictions, result = [], [], None
            before = unit.attempts
            try:
                if before != accounted or _serve_constructor_events(directory) != constructor_events:
                    raise RuntimeError('Serve submitted new work or constructed a model between complete queries')
                raw_identity, actual_identity = _owner_snapshot(identity_snapshot)
                current['owner_before_source'] = raw_identity
                identity_complete = identity_complete and actual_identity is not None
                if baseline_identity is not None and actual_identity is not None and actual_identity != baseline_identity:
                    raise RuntimeError('Serve model owner changed before fresh source')
                current['source_started_ns'] = time.monotonic_ns()
                source = task.read_source()
                if task.model_request_count(source) > unit.remaining:
                    raise BudgetExhausted('Serve session budget cannot cover the complete fresh source')
                def sink(value):
                    nonlocal result
                    result = value
                    current['sink_ready_ns'] = time.monotonic_ns()
                    current['source_to_sink_seconds'] = (current['sink_ready_ns']-current['source_started_ns'])/1e9

                _predict_query_batches(task, source, owner, sampling=sampling, unit=unit,
                    requests=requests, counts=current.setdefault('request_batch_counts', []),
                    predictions=predictions, sink=sink)
                current['post_sink_audit_started_ns'] = time.monotonic_ns()
                current['native_observation'], observed = _serve_attempt_window(directory, unit, first, before, predictions)
                accounted = unit.attempts
                if len(predictions) != len(requests):
                    raise RuntimeError('Serve complete predictions differ from task occurrences')
                if _serve_constructor_events(directory) != constructor_events:
                    raise RuntimeError('Serve constructed a new native model during the session')
                raw_identity, actual_identity = _owner_snapshot(identity_snapshot)
                current['model_owner'] = raw_identity
                identity_complete = identity_complete and actual_identity is not None
                if baseline_identity is not None and actual_identity is not None and actual_identity != baseline_identity:
                    raise RuntimeError('Serve model owner changed during the session')
                current['model_ready_before_source'] = (
                    owner.report['events']['deployment_ready_ns'] <= current['source_started_ns']
                    and max(constructors) <= current['source_started_ns'])
                task.verify_source(source)
                current['task'] = task.describe(source)
                current['quality'] = json_metric(task.evaluate(source, result, checkout=checkout, evaluation=evaluation))
                current['status'] = 'completed'
                if index == 1:
                    report['model_owner_reused'] = (True if identity_complete and actual_identity == baseline_identity
                        and requests and report['queries'][0]['logical_requests'] else None)
                    report['warm_qualified'] = report['model_owner_reused']
            except BaseException as error:
                failed(error, 'query')
            finally:
                try:
                    current['logical_requests'] = len(requests)
                    if result is not None:
                        current['result'] = task.encode_result(result)
                    write_private_json(phase/'requests.json', requests)
                except BaseException as error:
                    failed(error, 'query evidence persistence')
            if primary is not None:
                raise primary
        report['status'] = 'completed'
    except BaseException as error:
        failed(error, 'session')
    finally:
        if unit is not None:
            try:
                ledger.close_shared_unit(config['budget']['unit_id'])
                report['physical_attempts_reserved'] = unit.attempts
                report['permanent_budget'] = ledger.snapshot()
            except BaseException as error:
                failed(error, 'budget close')
        if owner is not None:
            report['native'] = owner.report
            try:
                owner.close()
            except BaseException as error:
                failed(error, 'owned Serve close')
        for query in report['queries']:
            try:
                write_private_json(directory/('round-'+str(query['round']))/'query.json', query)
            except BaseException as error:
                failed(error, 'query report persistence')
        try:
            write_private_json(directory/'session.json', report)
        except BaseException as error:
            failed(error, 'session evidence persistence')
    if primary is not None:
        primary.query_report = report
        raise primary
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'run', 'serve-session'))
    parser.add_argument('--config', required=True)
    parser.add_argument('--output-dir')
    parser.add_argument('--execute-model', action='store_true')
    args = parser.parse_args(argv)
    config = read_json(args.config)
    if config.get('schema') == SERVE_SCHEMA:
        from src.baselines.text.sembench_native_text import task_from_config
        from src.baselines.text.sembench_source import verify_checkout
        if args.action == 'run':
            raise ValueError('the Serve profile uses the explicit serve-session action')
        verify_checkout(config['sembench_checkout'])
        _sampling(config)
        if args.action == 'check' or not args.execute_model:
            task = task_from_config(config)
            task.prepare_evaluation()
            source = task.read_source()
            if 2*task.model_request_count(source) > config['budget']['limit']:
                raise ValueError('the session budget cannot cover two current-source queries plus native startup attempts')
            _native_serve_process(config, Path(args.output_dir or '.')/'native-caller', {})
            print(json.dumps(dict(status='checked_cpu_only', profile='native-ray-serve-json-isolated-caller',
                task=task.describe(source), execution_enabled=False, model_calls=0,
                target_native_model_qualified=None), ensure_ascii=False, allow_nan=False))
            return 0
        if args.output_dir is None:
            parser.error('executing the Serve session requires --output-dir')
        run_serve_session(config, args.output_dir, checkout=config['sembench_checkout'])
        return 0
    if args.action == 'serve-session':
        raise ValueError('serve-session requires its explicit Serve profile schema')
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
        if description.get('input_modality') in ('image', 'text+image') and not image_stage_enabled(config['processor_options']):
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
