"""Native Daft worker and result-ready-based process supervision.

The caller provides the host lease, permanent attempt reservation, environment
preflight and final GPU/SHM observation. This entry never installs or retries.
"""

import argparse
from contextlib import nullcontext
import hashlib
import json
import math
import os
from pathlib import Path
import time

from src.infrastructure.private_artifacts import (
    new_private_directory, publish_private_json, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text


def _save_artifact(path, value, *, primary=None, publish=False):
    try:
        (publish_private_json if publish else write_private_json)(path, value)
    except Exception as error:
        if primary is None:
            raise
        primary.add_note('native artifact ' + path.name + ' write failed: ' + redact_text(str(error)))
        return False
    return True


def configuration(value):
    fields = {'schema', 'model', 'sampling_params', 'engine_kwargs',
              'provider_options', 'query_timeout_s', 'cleanup_s', 'stop_grace_s'}
    if type(value) is not dict:
        raise ValueError('an explicit native Daft configuration is required')
    fields |= {'requests_path', 'requests_sha256', 'request_budget'}
    if value.get('schema') != 'semloom.native_daft_map.v1':
        raise ValueError('native Daft worker requires prepared prompts from the original query task layer')
    if set(value) - {'wait_for_result_release', 'native_openai'} != fields:
        raise ValueError('native Daft configuration fields do not match its query schema')
    if type(value.get('wait_for_result_release', False)) is not bool:
        raise ValueError('wait_for_result_release must be boolean')
    for name in ('query_timeout_s', 'cleanup_s', 'stop_grace_s'):
        number = value[name]
        if type(number) not in (int, float) or not math.isfinite(number) or number <= 0:
            raise ValueError(name + ' must be finite and positive')
    if value['stop_grace_s'] >= value['cleanup_s']:
        raise ValueError('forced process stop must fit inside the cleanup allowance')
    for name in ('sampling_params', 'engine_kwargs', 'provider_options'):
        if type(value[name]) is not dict:
            raise ValueError(name + ' must be an explicit object')
    if (type(value['request_budget']) is not dict
            or set(value['request_budget']) != {'path', 'budget_id', 'limit', 'unit_id'}):
        raise ValueError('prepared native prompts require an explicit claimed request budget')
    if 'native_openai' in value:
        native_openai_options(value)
    return value


def native_openai_options(config):
    """Resolve the attached native provider without writing credentials to packets."""
    reference = config['native_openai']
    if (type(reference) is not dict or set(reference) != {'base_url_env', 'api_key_env'}
            or any(type(value) is not str or not value for value in reference.values())):
        raise ValueError('native_openai requires the endpoint and key environment variable names')
    if config['engine_kwargs'] or {'base_url', 'api_key'} & config['provider_options'].keys():
        raise ValueError('attached OpenAI uses environment credentials and no local vLLM engine options')
    return dict(config['provider_options'], base_url=os.environ[reference['base_url_env']],
                api_key=os.environ[reference['api_key_env']])


def _http_report(events):
    requests = [event for event in events if event['event'] == 'request']
    responses = {event['attempt']: event for event in events if event['event'] == 'http_response'}
    ends = {event['attempt']: event for event in events if event['event'] == 'http_finished'}
    calls = []
    for request in requests:
        attempt = request['attempt']
        response, end = responses.get(attempt), ends.get(attempt)
        calls.append(dict(request, response=None if response is None else response['response'],
            http_status=None if response is None else response['status'],
            finished_ns=None if end is None else end['monotonic_ns'],
            error_type=None if end is None else end['error_type']))
    return dict(scope='native OpenAI HTTP attempts; graph owns row association; no engine or kernel evidence',
        provider='Daft builtin OpenAI', requests=calls, reserved_attempts=len(requests),
        http_send_entries=sum(event['event'] == 'http_started' for event in events),
        unresolved=sum(call['finished_ns'] is None or call['error_type'] is not None for call in calls),
        raw_HTTP_call_id_association_available=False, model_process_owned=False,
        SDK_explicit_client_close_observed=False)


class QueryDeadline:
    """One cleanup allowance, including graceful close and forced process stop."""
    def __init__(self, ready_path, *, query_s, cleanup_s, stop_grace_s, monitor=None, clock=time.monotonic,
                 ready_schema='semloom.native_daft_map.result_ready.v1', on_result_ready=None,
                 wait_for_result_release=False):
        self.path = Path(ready_path)
        self.clock, self.monitor = clock, monitor
        self.started = clock()
        self.query_deadline = self.started + query_s
        self.cleanup_s, self.stop_grace_s = cleanup_s, stop_grace_s
        self.ready = None
        self.ready_schema = ready_schema
        self.on_result_ready = on_result_ready
        self.wait_for_result_release = wait_for_result_release
        self.released = None

    def __call__(self, identities):
        if self.monitor is not None:
            self.monitor(identities)
        if self.ready is None and self.path.exists():
            value = read_json(self.path)
            if value['schema'] != self.ready_schema:
                raise ValueError('unexpected native result-ready artifact')
            self.ready = value['result_ready_monotonic_ns'] / 1e9
            if self.ready > self.query_deadline:
                raise TimeoutError('native Daft result exceeded its complete query allowance')
            primary = None
            try:
                if self.on_result_ready is not None:
                    self.on_result_ready(value)
            except BaseException as error:
                primary = error
                raise
            finally:
                if self.wait_for_result_release:
                    released = self.clock()
                    saved = _save_artifact(self.path.with_name('result-release.json'), dict(
                        schema='semloom.native_daft_map.result_release.v1',
                        result_ready_monotonic_ns=value['result_ready_monotonic_ns'],
                        released_monotonic_ns=int(released * 1e9)), primary=primary, publish=True)
                    if saved:
                        self.released = released
        now = self.clock()
        if self.ready is not None and self.ready > self.query_deadline:
            raise TimeoutError('native Daft result exceeded its complete query allowance')
        if self.ready is None and now >= self.query_deadline:
            raise TimeoutError('native Daft query exceeded its complete query allowance')
        if self.wait_for_result_release and self.released is not None and self.released >= self.query_deadline:
            raise TimeoutError('native Daft complete query exceeded its absolute allowance')
        cleanup_start = self.released if self.wait_for_result_release else self.ready
        if cleanup_start is not None and now >= cleanup_start + self.cleanup_s - self.stop_grace_s:
            raise TimeoutError('native Daft post-result close exceeded its graceful allowance')

    def report(self):
        cleanup_start = self.released if self.wait_for_result_release else self.ready
        report = dict(started_monotonic=self.started, query_deadline_monotonic=self.query_deadline,
            result_ready_monotonic=self.ready, cleanup_s=self.cleanup_s,
            forced_stop_reserved_s=self.stop_grace_s,
            cleanup_deadline_monotonic=None if cleanup_start is None else cleanup_start + self.cleanup_s)
        if self.wait_for_result_release:
            report.update(result_release_required=True, result_release_monotonic=self.released)
        return report


def _wait_for_result_release(directory, timestamp, deadline, parent_pid):
    """Keep the current native owner until the query consumes its result."""
    path = directory / 'result-release.json'
    while not path.exists():
        if os.getppid() != parent_pid:
            raise InterruptedError('native Daft result consumer exited before release')
        if time.monotonic() >= deadline:
            raise TimeoutError('native Daft result release exceeded the absolute query allowance')
        time.sleep(0.01)
    value = read_json(path)
    if (value.get('schema') != 'semloom.native_daft_map.result_release.v1'
            or value.get('result_ready_monotonic_ns') != timestamp):
        raise ValueError('native Daft release does not identify this result')
    if value['released_monotonic_ns'] / 1e9 >= deadline:
        raise TimeoutError('native Daft result release exceeded the absolute query allowance')
    return value


def run_worker(config, directory, *, execute_query=None):
    started = time.monotonic_ns()
    parent_pid = os.getppid()
    config = configuration(config)
    directory = Path(directory)
    if not directory.exists():
        new_private_directory(directory)
    native_sampling = config['sampling_params']
    raw = Path(config['requests_path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != config['requests_sha256']:
        raise ValueError('prepared native Map packet digest changed')
    packet = json.loads(raw)
    query_input = packet['requests']
    attached = 'native_openai' in config
    input_field = 'messages' if attached else 'prompt'
    if (packet['schema'] != ('semloom.native_daft_messages.v1' if attached else 'semloom.native_daft_prompts.v1')
            or not query_input
            or any(type(row) is not dict or set(row) != {'call_id', input_field}
                   or not isinstance(row['call_id'], str) or not row['call_id']
                   or not isinstance(row[input_field], list if attached else str) for row in query_input)
            or len({row['call_id'] for row in query_input}) != len(query_input)):
        raise ValueError('native Map packet requires complete prompts and unique occurrence IDs')
    source = dict(requests_sha256=config['requests_sha256'], rows=len(query_input))
    schema = config['schema'].removesuffix('.v1')
    ready_path = directory / 'result-ready.json'
    if ready_path.exists():
        raise FileExistsError('native query output must not reuse an earlier result')

    http_events = []

    def ready(result):
        if attached:
            result['metadata']['native_api'] = _http_report(http_events)
        timestamp = result['metadata']['result_ready_monotonic_ns']
        publish_private_json(ready_path, dict(schema=schema + '.result_ready.v1',
            result_ready_monotonic_ns=timestamp, worker_started_monotonic_ns=started,
            cold_result_ready_seconds=(timestamp - started) / 1e9,
            source=source, result=result))
        if config.get('wait_for_result_release', False):
            report['result_release'] = dict(required=True, confirmed=False)
            value = _wait_for_result_release(directory, timestamp,
                started / 1e9 + config['query_timeout_s'], parent_pid)
            report['result_release'].update(confirmed=True, **value,
                observed_monotonic_ns=time.monotonic_ns())

    report = dict(schema=schema + '.worker.v1', status='failed',
        source=source, warmup=0, whole_query_retries=0, performance_claim=False,
        model_ready_query_seconds=None, physical_gpu_exit_confirmed=None,
        result_artifact='result-ready.json', worker_started_monotonic_ns=started)
    if attached:
        report.update(provider='Daft builtin OpenAI', model_process_owned=False,
            client_release_boundary='isolated worker process exit after result ACK; explicit SDK client close unobserved',
            remote_service_shutdown_owned=False)
    primary = None
    try:
        if execute_query is None:
            from src.baselines.text.frameworks import daft_sembench_native
            execute_query = (daft_sembench_native.execute_prepared_messages if attached
                             else daft_sembench_native.execute_prepared_map)
        from src.observability.native_model_api_trace import writer
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.shared_request_budget import SharedClaimedUnit
        from src.infrastructure.private_artifacts import content_digest
        budget = config['request_budget']
        unit = SharedClaimedUnit(Path(budget['path']),
            AttemptBudget(budget['budget_id'], budget['limit']), budget['unit_id'])
        if unit.remaining < len(query_input):
            raise ValueError('remaining physical allowance cannot cover the complete native query')
        def reserve(call_id, request_id, prompt):
            unit.reserve(content_digest(dict(call_id=call_id, request_id=request_id, prompt=prompt)))
        arguments = dict(model=config['model'], sampling_params=native_sampling, on_result_ready=ready)
        observation = nullcontext()
        if attached:
            from src.observability.httpx_observer import _observe_native_httpx_client
            options = native_openai_options(config)
            emit = writer(directory)
            def record(event):
                event = dict(event)
                body = event.pop('body', None)
                if body is not None:
                    event['messages_sha256'] = content_digest(body['messages'])
                    event['model'] = body['model']
                http_events.append(event)
                emit(event['event'], **{key: value for key, value in event.items() if key != 'event'})
            observation = _observe_native_httpx_client(unit, record,
                options['base_url'].rstrip('/') + '/chat/completions', timeout_s=config['query_timeout_s'])
            arguments['provider_options'] = options
        else:
            arguments.update(on_native_event=writer(directory), before_generate=reserve,
                engine_kwargs=config['engine_kwargs'], provider_options=config['provider_options'],
                close_timeout_s=config['cleanup_s'] - config['stop_grace_s'])
        with observation:
            result = execute_query(query_input, **arguments)
        if attached:
            result['metadata']['native_api'] = _http_report(http_events)
        report.update(status='completed', model_close_receipt=result['metadata']['model_close_receipt'])
        report['native_api'] = result['metadata']['native_api']
    except BaseException as error:
        primary = error
        partial = getattr(error, 'native_query_report', None)
        report.update(error_type=type(error).__name__, error=redact_text(str(error)),
            model_close_receipt=getattr(error, 'model_close_report', None))
        if attached:
            report['native_api'] = _http_report(http_events)
            if partial is not None:
                partial['metadata']['native_api'] = report['native_api']
        if partial is not None:
            report['native_api'] = partial['metadata'].get('native_api')
            if report['model_close_receipt'] is None:
                report['model_close_receipt'] = partial['metadata']['model_close_receipt']
            if not ready_path.exists():
                # Materialized predictions can survive a later native collection or READY
                # publication failure. They do not establish a ready result.
                saved = _save_artifact(directory / 'failed-result.json', dict(
                    schema=schema + '.failed_result.v1', source=source,
                    result=partial, error_type=type(error).__name__), primary=error, publish=True)
                report['result_artifact'] = 'failed-result.json' if saved else None
        raise
    finally:
        report.update(worker_end_monotonic_ns=time.monotonic_ns(), result_ready_recorded=ready_path.exists())
        _save_artifact(directory / 'worker.json', report, primary=primary)
    return report


def supervised_run(config_path, directory, *, model_python, monitor=None, on_result_ready=None):
    from src.experiments.lotus_records_supervisor import supervise

    config = configuration(read_json(config_path))
    directory = Path(directory)
    new_private_directory(directory)
    deadline = QueryDeadline(directory / 'result-ready.json', query_s=config['query_timeout_s'],
        cleanup_s=config['cleanup_s'], stop_grace_s=config['stop_grace_s'], monitor=monitor,
        ready_schema=config['schema'].removesuffix('.v1') + '.result_ready.v1',
        on_result_ready=on_result_ready,
        wait_for_result_release=config.get('wait_for_result_release', False))
    command = [str(model_python), '-u', '-B', '-m', 'src.experiments.native_daft_query', 'worker',
               '--config', str(config_path), '--output-dir', str(directory)]
    primary = None
    try:
        return supervise(command, directory / 'supervision',
            timeout_s=config['query_timeout_s'] + config['cleanup_s'],
            grace_s=config['stop_grace_s'], natural_exit_grace_s=config['stop_grace_s'] / 2,
            monitor=deadline)
    except BaseException as error:
        primary = error
        raise
    finally:
        _save_artifact(directory / 'deadline.json', deadline.report(), primary=primary)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('worker', 'sembench'))
    parser.add_argument('--config', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--model-python')
    args = parser.parse_args(argv)
    if args.action == 'sembench':
        if args.model_python is None:
            parser.error('sembench requires --model-python for its separate native worker')
        from src.experiments.native_daft_sembench import run_query
        run_query(read_json(args.config), args.output_dir, model_python=args.model_python)
    else:
        run_worker(read_json(args.config), args.output_dir)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
