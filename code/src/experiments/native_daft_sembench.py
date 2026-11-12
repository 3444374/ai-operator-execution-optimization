"""One cold native Daft SemBench F/M/E or exact Join through the existing supervisor.

Driver and model dependencies stay isolated. Raw native readiness wakes the
original parser/query sink before the supervisor waits for model/process close.
This entry does not assert cross-query model reuse or launch a campaign.
"""

from pathlib import Path
import time

from src.baselines.text.sembench_prepared import prepare, finish, result_value
from src.baselines.text.sembench_text import SemBenchTextQuery
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.native_daft_query import supervised_run, _save_artifact
from src.infrastructure.private_artifacts import (
    new_private_directory, write_private_json, publish_private_json,
    read_json, file_sha256, json_metric,
)
from src.serving.backends.vllm_protocol import GenerationSpec


def run_query(config, directory, *, model_python, tokenizer=None, supervise=supervised_run,
              monitor=None):
    started = time.monotonic_ns()
    fields = {'schema', 'sembench_checkout', 'data_dir', 'scenario', 'query', 'query_id',
              'model_name', 'model', 'sampling_params', 'engine_kwargs', 'provider_options',
              'query_timeout_s', 'cleanup_s', 'stop_grace_s', 'max_model_calls'}
    if (config.get('schema') != 'semloom.native_daft_sembench.v1'
            or set(config) - {'scale_factor'} != fields):
        raise ValueError('explicit native Daft SemBench configuration required')
    sampling = dict(config['sampling_params'])
    if sampling.pop('n') != 1:
        raise ValueError('the original single-output query requires n=1')
    generation = GenerationSpec.from_dict(sampling)
    budget = AttemptBudget(config['query_id'], config['max_model_calls'])
    directory = Path(directory)
    new_private_directory(directory)
    deadline = started / 1e9 + config['query_timeout_s']
    report = dict(schema='semloom.native_daft_sembench.result.v1', status='failed',
        backend='daft-native-ai', worker_entry_ns=started, source_start_ns=None,
        query_clock_version='sembench.source_to_sink.v3', strict_controller_entry_ns=None,
        model_ready_before_source=False,
        strict_controller_entry_reason='host imports, preflight and lease precede this function',
        clock='time.monotonic_ns on one host', warmup=0, whole_query_retries=0,
        result_cache=False, prefix_cache='native provider/config; effective value observed when exposed',
        lifecycle='isolated cold; no warm or cross-query model reuse claim',
        physical_gpu_exit_confirmed=None, performance_claim=False)
    ledger = primary = result = None
    try:
        query = SemBenchTextQuery(config['sembench_checkout'], config['data_dir'],
            config['scenario'], config['query'], execution_id=config['query_id'],
            model_name=config['model_name'], generation=generation,
            scale_factor=config.get('scale_factor'))
        if tokenizer is None:
            from transformers import AutoTokenizer
            tokenizer = AutoTokenizer.from_pretrained(config['model'], local_files_only=True,
                                                       trust_remote_code=False)
        if query.describe()['expected_model_calls'] > budget.limit:
            raise ValueError('complete original query exceeds the declared model-call budget')
        if query.operation is not None and query.operation.records:
            ledger = CellBudgetLedger.create(directory / 'physical-budget.sqlite', budget,
                deadline_utc=time.time() + max(0, deadline - time.monotonic()))
            ledger.reserve_unit('query', budget.limit)
            ledger.claim_shared_unit('query')
        execute_query = query.bind_execution()
        report['preflight_finished_ns'] = time.monotonic_ns()
        report['source_start_ns'] = time.monotonic_ns()
        method, prepared, requests = prepare(query, tokenizer)
        report.update(query=query.describe(), logical_requests=len(requests),
                      prepare_finished_ns=time.monotonic_ns())

        def accept(raw):
            nonlocal result
            if 'sink_ready_ns' in report:
                raise ValueError('duplicate native result callback')
            report['raw_native_ready_ns'] = raw['result_ready_monotonic_ns']
            report['raw_native_observed_ns'] = time.monotonic_ns()
            def sink_ready(value):
                nonlocal result
                result = value
                report['sink_ready_ns'] = time.monotonic_ns()
                report['source_to_sink_seconds'] = (report['sink_ready_ns'] - report['source_start_ns']) / 1e9
                if report['sink_ready_ns'] / 1e9 > deadline:
                    raise TimeoutError('complete original Daft query exceeded its query allowance')
                report['semantic_audit_started_ns'] = time.monotonic_ns()

            _, semantic = finish(query, method, prepared, raw['result']['predictions'],
                                 on_result_ready=sink_ready, execute_query=execute_query)
            report['semantic_audit_finished_ns'] = time.monotonic_ns()
            # The original sink timestamp precedes serialization and all audit.
            report['sink_persistence_started_ns'] = time.monotonic_ns()
            publish_private_json(directory / 'result-ready.json', dict(result=result_value(result),
                semantic=semantic, sink_ready_ns=report['sink_ready_ns']))
            report['sink_persistence_finished_ns'] = time.monotonic_ns()

        if requests:
            packet = directory / 'prepared-prompts.json'
            write_private_json(packet, dict(schema='semloom.native_daft_prompts.v1', requests=requests))
            native_sampling = query.operation.generation.kwargs()
            worker = {key: config[key] for key in ('model', 'engine_kwargs', 'provider_options',
                                                   'cleanup_s', 'stop_grace_s')}
            worker.update(schema='semloom.native_daft_map.v1', sampling_params=native_sampling,
                query_timeout_s=deadline - time.monotonic(), requests_path=str(packet),
                requests_sha256=file_sha256(packet), request_budget=dict(
                    path=str(directory / 'physical-budget.sqlite'), budget_id=budget.budget_id,
                    limit=budget.limit, unit_id='query'))
            worker_path = directory / 'worker-config.json'
            write_private_json(worker_path, worker)
            report['supervision'] = supervise(worker_path, directory / 'native-worker',
                model_python=model_python, monitor=monitor, on_result_ready=accept)
            report['supervision_finished_ns'] = time.monotonic_ns()
            terminal = read_json(directory / 'native-worker' / 'worker.json')
            if terminal['status'] != 'completed' or 'sink_ready_ns' not in report:
                raise RuntimeError('native worker did not establish complete query and cleanup')
            report.update(native_api=terminal['native_api'],
                          model_close_receipt=terminal['model_close_receipt'])
        else:
            accept(dict(result_ready_monotonic_ns=time.monotonic_ns(), result=dict(predictions=[])))
        report['evaluation_started_ns'] = time.monotonic_ns()
        report['quality'] = json_metric(query.evaluate(result))
        report['evaluation_finished_ns'] = time.monotonic_ns()
        report.update(status='completed', result=result_value(result),
            timing_scope='prepared execution, native cold model/query, original fresh CSV scan/parser/full sink; '
                         'source/config/tokenizer preflight and budget setup excluded; '
                         'no wait for worker close on sink path; polling/IPC included',
            close_overlap='worker shutdown may overlap driver parsing; close and supervision are separate events')
    except BaseException as error:
        primary = error
        report['error_type'] = type(error).__name__
        raise
    finally:
        budget_error = None
        if ledger is not None:
            try:
                ledger.close_shared_unit('query')
                report['budget'] = ledger.snapshot()
            except BaseException as error:
                if primary is None:
                    primary = budget_error = error
                    report.update(status='failed', error_type=type(error).__name__)
                else:
                    primary.add_note('native budget close failed: ' + type(error).__name__)
        report['final_persistence_started_ns'] = time.monotonic_ns()
        _save_artifact(directory / 'driver.json', report, primary=primary)
        report['final_persistence_finished_ns'] = time.monotonic_ns()
        _save_artifact(directory / 'persistence.json', dict(
            driver_json_started_ns=report['final_persistence_started_ns'],
            driver_json_finished_ns=report['final_persistence_finished_ns'],
            excludes='this receipt publication'), primary=primary)
        if budget_error is not None:
            raise budget_error
    return report
