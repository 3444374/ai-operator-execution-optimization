"""Complete SemBench sink inside the existing native Ray runtime lifetime."""

import json
from pathlib import Path
import time

from src.baselines.text.frameworks.ray_data_sembench_native import execute_prepared
from src.baselines.text.sembench_prepared import prepare, finish, result_value
from src.baselines.text.sembench_text import SemBenchTextQuery
from src.infrastructure.private_artifacts import write_private_json, json_metric
from src.infrastructure.redact import redact_text


def run_query(config, checked_query, engine, resources, directory, *, execute=execute_prepared):
    """Fresh source through original in-memory sink, excluding audit and close.

    Called by sembench_text_query's existing supervised worker. The Ray engine
    actor uses the declared native model interpreter, not the project's fixed
    vLLM bridge. The host owns physical cleanup verification.
    """
    from .attempt_ledger import AttemptBudget
    from .cell_budget import CellBudgetLedger

    started = time.monotonic_ns()
    report = dict(schema='semloom.sembench_text_result.v1', status='failed', backend='ray-data',
        execution_layout='public Ray Data LLM processor; original SemBench parser and full query',
        worker_entry_ns=started, source_started_ns=None, source_to_sink_seconds=None,
        query_clock_version='sembench.source_to_sink.v2', strict_controller_to_sink_seconds=None,
        production_core_used=False, result_cache_enabled=False, warmup_calls=0,
        lifecycle='single cold native instance', requested_generate_retries=0,
        performance_claim=False, physical_gpu_exit_confirmed=None,
        timing_scope='prepared execution and original fresh CSV scan/parser/full sink; native cold startup included; '
                     'source/config preflight, budget claim, audit, evaluation, persistence and ray.shutdown excluded',
        strict_controller_unavailable_reason='worker API does not observe earliest host/controller code',
        engine_accept_ns=None, kernel_seconds=None)
    primary = None
    native = None
    ledger = unit = None
    result = None
    try:
        query = SemBenchTextQuery(checked_query.checkout, checked_query.data_dir,
            checked_query.scenario, checked_query.query, execution_id=checked_query.query_id,
            model_name=checked_query.model_name, generation=checked_query.generation,
            scale_factor=config.get('scale_factor'))
        if query.files != checked_query.files:
            raise ValueError('native Ray fresh source differs from checked input identity')
        report['query'] = query.describe()
        spec = config['ray_native']
        value = config['budget']
        ledger = CellBudgetLedger(Path(value['path']), AttemptBudget(value['id'], value['limit']))
        unit = ledger.claim_shared_unit(value['unit_id'])
        if query.describe()['expected_model_calls'] > unit.remaining:
            raise ValueError('shared physical budget cannot cover every original occurrence')
        observer = dict(value, output=str(directory.resolve()))
        runtime_env = dict(py_executable=engine.model_python,
            env_vars=dict(PYTHONPATH=str(Path(__file__).parents[2]),
                          SEMLOOM_RAY_NATIVE_OBSERVER=json.dumps(observer)))
        engine_kwargs = dict(seed=engine.seed, **engine.optional_kwargs())
        if engine.tokenizer_path is not None:
            engine_kwargs['tokenizer'] = engine.tokenizer_path
        conflicts = {key for key in engine_kwargs if key in spec['engine_kwargs']
                     and engine_kwargs[key] != spec['engine_kwargs'][key]}
        if conflicts or engine.chat_template is not None:
            raise ValueError('conflicting Ray engine settings or unsupported explicit chat template')
        engine_kwargs.update(spec['engine_kwargs'])

        report['preflight_finished_ns'] = time.monotonic_ns()
        report['source_started_ns'] = time.monotonic_ns()
        method, prepared, requests = prepare(query)
        report.update(logical_requests=len(requests), prepare_finished_ns=time.monotonic_ns())

        def accept(raw):
            nonlocal result, native
            native = raw
            report['raw_native_ready_ns'] = raw['events']['predictions_materialized_ns']
            def sink_ready(value):
                nonlocal result
                result = value
                report['sink_ready_ns'] = time.monotonic_ns()
                report['source_to_sink_seconds'] = (report['sink_ready_ns'] - report['source_started_ns']) / 1e9
                report['semantic_audit_started_ns'] = time.monotonic_ns()
            _, semantic = finish(query, method, prepared, raw['predictions'], on_result_ready=sink_ready)
            report['semantic_audit_finished_ns'] = time.monotonic_ns()
            report['semantic'] = semantic

        native = execute(requests, model=engine.model_path,
            sampling_params={} if query.operation is None else query.operation.stage.generation.kwargs(),
            engine_kwargs=engine_kwargs, processor_options=spec['processor_options'],
            runtime_options=config['profile'], engine_runtime_env=runtime_env, on_result_ready=accept)
        report['query_seconds'] = (time.monotonic_ns() - started) / 1e9
        report['query_timing_scope'] = 'legacy worker entry through preflight, semantic audit and runtime close; use versioned source_to_sink_seconds'
        if unit.attempts != len(requests):
            raise RuntimeError('mandatory native generate accounting does not match complete occurrences')
        from src.baselines.text.frameworks.ray_native_observer import summarize
        report['native_event_audit_started_ns'] = time.monotonic_ns()
        observed = report['native_observation'] = summarize(directory)
        if requests and (not observed['available']
                or len(observed['submitted_attempts']) != len(requests)
                or len(set(observed['submitted_attempts'])) != len(requests)
                or sorted(observed['submitted_attempts']) != sorted(observed['terminal_attempts'])
                or observed['unknown_attempts']):
            raise RuntimeError('native event audit lacks unique confirmed terminals for every occurrence')
        report['native_event_audit_finished_ns'] = time.monotonic_ns()
        report['evaluation_started_ns'] = time.monotonic_ns()
        report['quality'] = json_metric(query.evaluate(result))
        report['evaluation_finished_ns'] = time.monotonic_ns()
        report['result_serialization_started_ns'] = time.monotonic_ns()
        report['result'] = result_value(result)
        report['result_serialization_finished_ns'] = time.monotonic_ns()
        report['status'] = 'completed'
    except BaseException as error:
        primary = error
        native = getattr(error, 'native_query_report', native)
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        error.query_report = report
        raise
    finally:
        try:
            if unit is not None:
                ledger.close_shared_unit(config['budget']['unit_id'])
                report['physical_attempts_reserved'] = unit.attempts
                report['permanent_budget'] = ledger.snapshot()
        except BaseException as error:
            report.update(status='failed', budget_close_error_type=type(error).__name__)
            if primary is None:
                primary = error
            else:
                primary.add_note('Ray budget finalization failed: ' + type(error).__name__)
        try:
            if native is not None:
                report['native'] = {key: value for key, value in native.items() if key != 'predictions'}
                report['native_persistence_started_ns'] = time.monotonic_ns()
                write_private_json(directory / 'native-predictions.json', native)
                report['native_persistence_finished_ns'] = time.monotonic_ns()
        except BaseException as error:
            report.update(status='failed', native_persistence_error_type=type(error).__name__)
            if primary is None:
                primary = error
            else:
                primary.add_note('Ray native evidence persistence failed: ' + type(error).__name__)
        report['report_persistence_started_ns'] = time.monotonic_ns()
        try:
            write_private_json(directory / 'query.json', report)
            write_private_json(directory / 'persistence.json', dict(
                query_json_started_ns=report['report_persistence_started_ns'],
                query_json_finished_ns=time.monotonic_ns(), excludes='this receipt publication'))
        except BaseException as error:
            if primary is None:
                raise
            primary.add_note('Ray report persistence failed: ' + type(error).__name__)
        if primary is not None:
            primary.query_report = report
            raise primary
    return report
