"""Independent SemBench JSON tasks over Daft's native AI prompt graph.

The JSON task semantics are an engineering comparison shared with other
native systems, not an author method or a LOTUS prompt/parser implementation.
The existing prepared worker owns native model execution and ACK-based close.
"""

import argparse
from collections import Counter
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
    content_digest, file_sha256, json_metric, new_private_directory, read_json, write_private_json,
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
    required = task.model_request_count(source)
    if required > config['max_model_calls']:
        raise ValueError('the finite native budget cannot cover the eligible source')
    description = task.describe(source)
    if required and description.get('input_modality') == 'image':
        raise ValueError('the local Daft string prompt profile does not support image content')
    return dict(schema=SCHEMA, status='checked_cpu_only', task=description,
                execution_enabled=False, model_calls=0, target_native_model_qualified=False)


def _render(requests, tokenizer):
    if any(isinstance(message['content'], list)
           and any(part.get('type') != 'text' for part in message['content'])
           for request in requests for message in request['messages']):
        raise ValueError('the local Daft string prompt profile cannot preserve non-text message parts')
    return [dict(call_id=row['call_id'], prompt=tokenizer.apply_chat_template(
        row['messages'], tokenize=False, add_generation_prompt=True)) for row in requests]


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
            prepared = _render(requests, tokenizer)
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


ATTACHED_SCHEMA = 'semloom.sembench_native_daft_attached_session.v1'


def attached_configuration(config):
    fields = {'schema', 'sembench_checkout', 'query', 'query_id', 'model', 'sampling_params',
              'engine_kwargs', 'provider_options', 'native_openai', 'max_http_attempts',
              'session_timeout_s', 'query_timeout_s', 'cleanup_s', 'stop_grace_s'}
    if (type(config) is not dict or not fields <= config.keys()
            or config.keys() - fields - {'scenario'} - TASK_SOURCE_FIELDS
            or config['schema'] != ATTACHED_SCHEMA):
        raise ValueError('an explicit independent Daft attached-session configuration is required')
    task = task_from_config(config)
    for name in ('sampling_params', 'engine_kwargs', 'provider_options'):
        if type(config[name]) is not dict:
            raise ValueError(name + ' must be an explicit native object')
    sampling = config['sampling_params']
    if (type(sampling.get('n')) is not int or sampling['n'] != 1
            or sampling.get('response_format') != task.response_format
            or sampling.get('stream', False) is not False):
        raise ValueError('attached JSON tasks require n=1, nonstream JSON object responses')
    for name in ('session_timeout_s', 'query_timeout_s', 'cleanup_s', 'stop_grace_s'):
        value = config[name]
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError(name + ' must be finite and positive')
    if config['stop_grace_s'] >= config['cleanup_s'] or config['cleanup_s'] >= config['session_timeout_s']:
        raise ValueError('client stop and cleanup must fit inside the finite session allowance')
    if not isinstance(config['model'], str) or not config['model'].strip():
        raise ValueError('the native model must be explicit')
    AttemptBudget(config['query_id'], config['max_http_attempts'])
    from src.experiments.native_daft_query import native_openai_options
    native_openai_options(config)
    verify_checkout(config['sembench_checkout'])
    return task


def _attached_layout(requests):
    # Native OpenAI prompt binds one static system and one content-column layout.
    # Check the whole batch before launching a client; PNG decoding stays in Daft.
    from src.baselines.text.frameworks.daft_sembench_native import effective_openai_messages
    layout = None
    for row in requests:
        messages = effective_openai_messages(row['messages'])
        current = (messages[0]['content'], tuple(part['type'] for part in messages[1]['content']))
        if layout is not None and current != layout:
            raise ValueError('native attached prompt requires one static system and content-part layout per batch')
        layout = current


def check_attached_session(config):
    task = attached_configuration(config)
    task.prepare_evaluation()
    source = task.read_source()
    required = task.model_request_count(source)
    if 2 * required > config['max_http_attempts']:
        raise ValueError('the finite HTTP allowance cannot cover two current complete queries, before retries')
    _attached_layout(task.requests(source))
    return dict(schema=ATTACHED_SCHEMA, status='checked_cpu_only', task=task.describe(source),
        execution_enabled=False, model_calls=0, query_rounds=2, warm_qualified=None,
        target_native_model_qualified=False,
        budget_scope='one permanent shared HTTP attempt limit including native SDK retries')


def _attached_owner(probe):
    if probe is None:
        return None, None
    before = time.monotonic_ns()
    value = probe()
    after = time.monotonic_ns()
    fields = {'observed_monotonic_ns', 'host_id', 'service_pid', 'service_create_time', 'model_processes'}
    if not isinstance(value, dict) or not fields <= value.keys():
        return value, None
    children = value['model_processes']
    if (type(value['observed_monotonic_ns']) is not int
            or not before <= value['observed_monotonic_ns'] <= after
            or not isinstance(value['host_id'], str) or not value['host_id']
            or not isinstance(children, list) or not children):
        return value, None
    processes = [(value['service_pid'], value['service_create_time'])]
    for child in children:
        if not isinstance(child, dict) or not {'pid', 'create_time'} <= child.keys():
            return value, None
        processes.append((child['pid'], child['create_time']))
    if (any(type(pid) is not int or pid <= 0 or type(birth) not in (int, float)
            or not math.isfinite(birth) or birth <= 0 for pid, birth in processes)
            or len({pid for pid, _ in processes}) != len(processes)):
        return value, None
    return value, (value['host_id'], processes[0], tuple(sorted(processes[1:])))


def _attached_audit(requests, predictions, native, *, model, first_attempt, attempts):
    """Post-sink HTTP evidence; row association remains the native call_id projection."""
    from src.baselines.text.frameworks.daft_sembench_native import effective_openai_messages
    occurrences = [dict(call_id=row['call_id'], original_messages_sha256=content_digest(row['messages']),
        effective_messages_sha256=content_digest(effective_openai_messages(row['messages']))) for row in requests]
    digests = {row['call_id']: row['effective_messages_sha256'] for row in occurrences}
    expected = Counter((digests[row['call_id']], row['generated_text']) for row in predictions)
    calls = native['requests']
    successful = [call for call in calls if call['finished_ns'] is not None and call['error_type'] is None
                  and call['http_status'] is not None and 200 <= call['http_status'] < 300]
    observed = Counter((call['messages_sha256'], call['response']['choices'][0]['message']['content'])
                       for call in successful)
    response_ids = [call['response']['id'] for call in successful]
    if (attempts != native['reserved_attempts'] or attempts != len(calls)
            or sorted(call['attempt'] for call in calls) != list(range(first_attempt + 1, first_attempt + attempts + 1))
            or native['http_send_entries'] != attempts or native['unresolved']
            or any(call['messages_sha256'] not in digests.values() or call['model'] != model for call in calls)
            or observed != expected or len(predictions) != len(requests)
            or {row['call_id'] for row in predictions} != digests.keys()
            or any(not isinstance(value, str) or not value for value in response_ids)
            or len(set(response_ids)) != len(response_ids)):
        raise ValueError('native HTTP attempts or responses do not establish the complete effective message batch')
    return dict(reserved_attempts=attempts, successful_responses=len(successful), unresolved_attempts=0,
        response_ids=response_ids, message_occurrences=occurrences,
        raw_HTTP_call_id_association_available=False,
        result_row_association='native Daft call_id projection; HTTP association is a message/output multiset')


def run_attached_session(config, directory, *, model_python, supervise=supervised_run,
                         monitor=None, identity_probe=None):
    """Two fresh queries, separate client workers, one externally owned model service."""
    attached_configuration(config)
    directory = Path(directory)
    new_private_directory(directory)
    report = dict(schema=ATTACHED_SCHEMA + '.result', status='failed',
        baseline_identity='daft-native-openai-attached-json', comparison_identity='统一语义工程对照',
        semantic_owner='independent task JSON prompt/parser', author_original_semantic_method=False,
        lotus_sdk_required=False, execution_owner='Daft builtin OpenAI prompt graph',
        message_encoding='daft.openai.system_single_user.parts.v1', target_renderer_equivalence_verified=None,
        HTTP_evidence_source='prepared message packet and worker.json.native_api fingerprints/responses',
        HTTP_JSONL_evidence_used=False,
        HTTP_evidence_limitation='the pinned optional JSONL writer drops HTTP events with an existing monotonic_ns; '
            'this profile uses complete worker.json HTTP receipts and does not claim a JSONL archive',
        model_process_owned=False, remote_service_shutdown_owned=False,
        client_release_boundary='ACK then isolated worker exit; explicit SDK client close is unobserved',
        service_ready_before_session=None, model_startup_seconds=None,
        model_owner_reused=None, warm_qualified=None, target_native_model_qualified=False,
        owner_evidence_scope='fresh caller host probe: service PID/birth and actual model subprocess PID/birth',
        performance_claim=False, whole_query_retries=0, extra_query_warmup=0, queries=[],
        timing_scope='each fresh task source through complete strict in-memory task sink; native messages packet, '
            'native graph/HTTP evidence, IPC and polling included; evaluation, message audit, ACK/client exit and '
            'result persistence follow sink; external service startup is not measured by this driver')
    ledger = unit = primary = None
    frames = []
    baseline_owner = None
    identities_complete = True
    try:
        task = task_from_config(dict(config, query_id=config['query_id'] + '-round-1'))
        evaluation = task.prepare_evaluation()
        budget = AttemptBudget(config['query_id'], config['max_http_attempts'])
        deadline = time.monotonic() + config['session_timeout_s']
        ledger = CellBudgetLedger.create(directory / 'physical-budget.sqlite', budget,
            deadline_utc=time.time() + config['session_timeout_s'])
        ledger.reserve_unit('session', budget.limit)
        unit = ledger.claim_shared_unit('session')
        seen_response_ids = set()
        for index in range(2):
            if unit.remaining <= 0:
                raise ValueError('the permanent HTTP session allowance is exhausted; no next query started')
            current = dict(round=index + 1, status='failed', source_start_ns=None, sink_ready_ns=None,
                source_to_sink_seconds=None, model_ready_before_source=None, model_startup_seconds=None)
            report['queries'].append(current)
            query_dir = directory / ('query-' + str(index + 1))
            new_private_directory(query_dir)
            if index:
                task = task_from_config(dict(config, query_id=config['query_id'] + '-round-2'))
            source = frame = native_result = None
            requests = []
            first_attempt = unit.attempts
            error = None
            try:
                if index:
                    evaluation = task.prepare_evaluation()
                raw, owner = _attached_owner(identity_probe)
                current['owner_before_source'] = raw
                identities_complete = identities_complete and owner is not None
                if index == 0:
                    baseline_owner = owner
                elif baseline_owner is not None and owner is not None and owner != baseline_owner:
                    report['model_owner_reused'] = False
                    raise RuntimeError('the observed external model owner changed; no next query started')
                current['source_start_ns'] = time.monotonic_ns()
                query_deadline = min(current['source_start_ns'] / 1e9 + config['query_timeout_s'],
                                     deadline - config['cleanup_s'])
                if time.monotonic() >= query_deadline:
                    raise TimeoutError('the finite session allowance cannot fit another complete query and client cleanup')
                source = task.read_source()
                if task.model_request_count(source) > unit.remaining:
                    raise ValueError('the remaining HTTP allowance cannot cover the complete fresh source')
                requests = task.requests(source)
                _attached_layout(requests)

                def consume(raw):
                    nonlocal frame, native_result
                    native_result = raw['result']
                    frame = task.finish(source, native_result['predictions'])
                    current['sink_ready_ns'] = time.monotonic_ns()
                    current['source_to_sink_seconds'] = (current['sink_ready_ns'] - current['source_start_ns']) / 1e9
                    if current['sink_ready_ns'] / 1e9 >= query_deadline:
                        raise TimeoutError('the attached Daft complete sink exceeded its query allowance')

                if requests:
                    packet, worker_path = query_dir / 'prepared-messages.json', query_dir / 'worker-config.json'
                    write_private_json(packet, dict(schema='semloom.native_daft_messages.v1', requests=requests))
                    worker = {name: deepcopy(config[name]) for name in ('model', 'sampling_params', 'engine_kwargs',
                        'provider_options', 'native_openai', 'cleanup_s', 'stop_grace_s')}
                    worker.update(schema='semloom.native_daft_map.v1',
                        query_timeout_s=query_deadline - time.monotonic(), requests_path=str(packet),
                        requests_sha256=file_sha256(packet), wait_for_result_release=True,
                        request_budget=dict(path=str(directory / 'physical-budget.sqlite'), budget_id=budget.budget_id,
                            limit=budget.limit, unit_id='session'))
                    write_private_json(worker_path, worker)
                    current['supervision'] = supervise(worker_path, query_dir / 'native-worker',
                        model_python=model_python, monitor=monitor, on_result_ready=consume)
                    current['client_worker_exit_observed_ns'] = time.monotonic_ns()
                    terminal = read_json(query_dir / 'native-worker' / 'worker.json')
                    current.update(native_api=terminal.get('native_api'), result_release=terminal.get('result_release'))
                    if (terminal['status'] != 'completed' or frame is None
                            or not terminal.get('result_release', {}).get('confirmed')):
                        raise RuntimeError('the native client did not establish a complete sink, ACK and exit')
                    current['HTTP_audit'] = _attached_audit(requests, native_result['predictions'],
                        terminal['native_api'], model=config['model'], first_attempt=first_attempt,
                        attempts=unit.attempts - first_attempt)
                    if seen_response_ids.intersection(current['HTTP_audit']['response_ids']):
                        raise ValueError('native HTTP response IDs must be distinct across query rounds')
                    seen_response_ids.update(current['HTTP_audit']['response_ids'])
                else:
                    consume(dict(result=dict(predictions=[])))
                    current['no_model_reason'] = 'no eligible semantic occurrences; no native client started'
                raw, owner = _attached_owner(identity_probe)
                current['owner_after_client_exit'] = raw
                identities_complete = identities_complete and owner is not None
                if baseline_owner is not None and owner is not None and owner != baseline_owner:
                    report['model_owner_reused'] = False
                    raise RuntimeError('the observed external model owner changed during the query')
                current['source_audit_started_ns'] = time.monotonic_ns()
                task.verify_source(source)
                current['task'] = task.describe(source)
                current['evaluation_started_ns'] = time.monotonic_ns()
                current['quality'] = json_metric(task.evaluate(source, frame,
                    checkout=config['sembench_checkout'], evaluation=evaluation))
                current['status'] = 'completed'
            except BaseException as caught:
                error = caught
                current.update(error_type=type(caught).__name__, error=redact_text(str(caught)))
                if hasattr(caught, 'report'):
                    current['supervision'] = caught.report
                raise
            finally:
                current.update(logical_model_requests=len(requests), physical_HTTP_attempts=unit.attempts-first_attempt)
                terminal_path = query_dir / 'native-worker' / 'worker.json'
                if terminal_path.exists():
                    try:
                        terminal = read_json(terminal_path)
                        current.update(native_api=terminal.get('native_api'), result_release=terminal.get('result_release'))
                    except BaseException as caught:
                        if error is None:
                            error = caught
                        else:
                            error.add_note('attached worker report read failed: ' + redact_text(str(caught)))
                if frame is not None:
                    frames.append(frame)
                    try:
                        current['result'] = task.encode_result(frame)
                    except BaseException as caught:
                        current.update(status='failed', result_encoding_error_type=type(caught).__name__)
                        if error is None:
                            error = caught
                        else:
                            error.add_note('attached result encoding failed: ' + redact_text(str(caught)))
                if error is not None:
                    current['status'] = 'failed'
                    current.pop('quality', None)
                try:
                    _save_artifact(query_dir / 'driver.json', current, primary=error)
                except BaseException as caught:
                    error = caught
                    current.update(status='failed', error_type=type(caught).__name__, error=redact_text(str(caught)))
                    current.pop('quality', None)
                if error is not None:
                    raise error
        report['model_owner_reused'] = True if identities_complete and all(
            query['physical_HTTP_attempts'] > 0 for query in report['queries']) else None
        report['warm_qualified'] = report['model_owner_reused']
        report['status'] = 'completed'
    except BaseException as caught:
        primary = caught
        report.update(status='failed', error_type=type(caught).__name__, error=redact_text(str(caught)), warm_qualified=None)
    finally:
        if ledger is not None:
            try:
                ledger.close_shared_unit('session')
                report.update(physical_HTTP_attempts=unit.attempts, budget=ledger.snapshot())
            except BaseException as caught:
                if primary is None:
                    primary = caught
                else:
                    primary.add_note('attached budget close failed: ' + redact_text(str(caught)))
        if primary is not None:
            report.update(status='failed', warm_qualified=None, error_type=type(primary).__name__,
                          error=redact_text(str(primary)))
        try:
            _save_artifact(directory / 'session.json', report, primary=primary)
        except BaseException as caught:
            primary = caught
            report.update(status='failed', warm_qualified=None, error_type=type(caught).__name__, error=redact_text(str(caught)))
        if primary is not None:
            primary.query_report = report
            primary.session_results = frames
            raise primary
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'run', 'attached-session'))
    parser.add_argument('--config', required=True)
    parser.add_argument('--output-dir')
    parser.add_argument('--model-python')
    parser.add_argument('--execute-model', action='store_true')
    args = parser.parse_args(argv)
    config = read_json(args.config)
    attached = args.action == 'attached-session' or isinstance(config, dict) and config.get('schema') == ATTACHED_SCHEMA
    if args.action == 'check' or not args.execute_model:
        print(json.dumps((check_attached_session if attached else check)(config), ensure_ascii=False, allow_nan=False))
        return 0
    if args.output_dir is None or args.model_python is None:
        parser.error('executing the native query requires --output-dir and --model-python')
    if attached:
        if args.action != 'attached-session':
            parser.error('the attached profile requires the explicit attached-session action')
        run_attached_session(config, args.output_dir, model_python=args.model_python)
    else:
        run_query(config, args.output_dir, model_python=args.model_python)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
