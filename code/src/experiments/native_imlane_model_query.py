"""One author DuckDB Q3 query with an independently owned vLLM IPC service.

This is a project baseline binding. The author database owns ordinary operators,
prediction batches and CPython execution. No SemLoom execution policy is used.
The caller supplies the host lease, environment preflight and final GPU/SHM
supervision. This entry installs nothing and never retries a whole query.
"""

import argparse
from copy import deepcopy
import math
import os
from pathlib import Path
import signal
import threading
import time

from src.experiments.native_imlane_query import audit_result, materialize_input, publish_input_packet
from src.infrastructure.private_artifacts import (
    content_digest, new_private_directory, publish_private_json, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text

CONFIG_SCHEMA = 'semloom.imlane.duckdb.q3.model_run.v2'
READY_SCHEMA = 'semloom.imlane.duckdb.q3.model_result_ready.v1'
CLEANUP_SCHEMA = 'semloom.imlane.duckdb.q3.cleanup_started.v1'


def configuration(value):
    fields = {'schema', 'provider_mode', 'reviews_csv', 'model_service', 'sampling_params',
              'request_timeout_s'}
    if type(value) is not dict or set(value) != fields or value['schema'] != CONFIG_SCHEMA:
        raise ValueError('an explicit native IMLane model-run configuration is required')
    if value['provider_mode'] not in {'model', 'software_ipc'}:
        raise ValueError('declare model or software_ipc explicitly')
    if type(value['reviews_csv']) is not str or not value['reviews_csv']:
        raise ValueError('declare the complete original Reviews.csv')
    if type(value['model_service']) is not dict:
        raise ValueError('declare the independently owned model service')
    return deepcopy(value)


class QueryWindow:
    """A query deadline and one cleanup allowance starting at native readiness."""
    def __init__(self, query_seconds, cleanup_seconds, stop_grace_seconds, *, clock=time.monotonic,
                 cleanup_path=None):
        for value in (query_seconds, cleanup_seconds, stop_grace_seconds):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError('query and cleanup allowances must be finite and positive')
        if stop_grace_seconds >= cleanup_seconds:
            raise ValueError('forced stop must fit inside the cleanup allowance')
        self.clock = clock
        self.started = clock()
        self.query_deadline = self.started + query_seconds
        self.cleanup_seconds, self.stop_grace_seconds = cleanup_seconds, stop_grace_seconds
        self.ready_ns = self.cleanup_deadline = None
        self.cleanup_path = None if cleanup_path is None else Path(cleanup_path)
        self.cleanup_started_ns = self.cleanup_publication_error = None

    def observe(self, query_path, ready_path):
        if self.ready_ns is not None or not Path(query_path).exists():
            return
        native = read_json(query_path)
        if native['status'] != 'query_returned':
            return
        self.accept_ready(native.get('result_ready_monotonic_ns'), clock=native.get('result_clock'))
        publish_private_json(ready_path, dict(schema=READY_SCHEMA,
            result_ready_monotonic_ns=self.ready_ns, query_artifact='query.json',
            positive_review_cnt=native['positive_review_cnt'], clock='CLOCK_MONOTONIC',
            physical_gpu_exit_confirmed=None))

    def accept_ready(self, ready, *, clock):
        if self.ready_ns is not None:
            return
        if (type(ready) is not int or ready <= 0 or clock != 'CLOCK_MONOTONIC'
                or ready / 1e9 < self.started
                or ready / 1e9 > self.clock()):
            raise ValueError('native result needs a qualified CLOCK_MONOTONIC readiness timestamp; rebuild driver')
        if ready / 1e9 > self.query_deadline:
            raise TimeoutError('native IMLane result exceeded the query allowance')
        self.ready_ns = ready
        until = ready / 1e9 + self.cleanup_seconds
        self.cleanup_deadline = until if self.cleanup_deadline is None else min(self.cleanup_deadline, until)

    def begin_cleanup(self):
        if self.cleanup_deadline is None:
            now = self.clock()
            self.cleanup_started_ns = int(now * 1e9)
            self.cleanup_deadline = min(self.cleanup_started_ns / 1e9, self.query_deadline) + self.cleanup_seconds
            if self.cleanup_path is not None:
                try:
                    publish_private_json(self.cleanup_path, dict(schema=CLEANUP_SCHEMA,
                        cleanup_started_monotonic_ns=self.cleanup_started_ns,
                        cleanup_deadline_monotonic=self.cleanup_deadline, clock='CLOCK_MONOTONIC'))
                except BaseException as error:
                    # Evidence failure must not skip partially started resource
                    # cleanup. The worker preserves it after teardown.
                    self.cleanup_publication_error = error
        return self.cleanup_deadline

    def accept_cleanup(self, value):
        started = value['cleanup_started_monotonic_ns']
        until = value['cleanup_deadline_monotonic']
        if (value['schema'] != CLEANUP_SCHEMA or value['clock'] != 'CLOCK_MONOTONIC'
                or type(started) is not int or not self.started <= started / 1e9 <= self.clock()
                or type(until) not in (int, float) or not math.isfinite(until) or until <= 0
                or until > started / 1e9 + self.cleanup_seconds):
            raise ValueError('invalid native failure cleanup deadline')
        self.cleanup_started_ns = started
        self.cleanup_deadline = min(until, self.query_deadline + self.cleanup_seconds)

    def check(self):
        until = (self.query_deadline if self.cleanup_deadline is None
                 else self.cleanup_deadline - self.stop_grace_seconds)
        if self.clock() >= until:
            raise TimeoutError('native IMLane query or graceful post-result allowance exhausted')

    def report(self):
        return dict(worker_started_monotonic=self.started, query_deadline_monotonic=self.query_deadline,
            result_ready_monotonic_ns=self.ready_ns, cleanup_deadline_monotonic=self.cleanup_deadline,
            cleanup_started_monotonic_ns=self.cleanup_started_ns,
            cleanup_marker_error_type=(None if self.cleanup_publication_error is None
                                       else type(self.cleanup_publication_error).__name__),
            cleanup_seconds=self.cleanup_seconds, forced_stop_reserved_seconds=self.stop_grace_seconds)


class _ResultWindow(QueryWindow):
    """Publish the complete relation's readiness before audit and owner close."""
    def __init__(self, output, *args, **kwargs):
        self.output = Path(output)
        super().__init__(*args, **kwargs)

    def accept_ready(self, ready, *, clock):
        if self.ready_ns is not None:
            return
        super().accept_ready(ready, clock=clock)
        publish_private_json(self.output / 'result-ready.json',
            dict(schema=READY_SCHEMA, result_ready_monotonic_ns=ready, clock=clock))


def audit_model_result(directory, checkout):
    """Join saved native/IPC evidence without another model or graph execution."""
    from integrations.imlane_duckdb.model_wire import SCHEMA

    directory = Path(directory)
    provider = read_json(directory / 'model-provider.json')
    mode = provider['provider_mode']
    audit = audit_result(directory / 'input', directory / 'query.json', directory / 'udf-receipts',
                         checkout, provider_mode=mode)
    calls_dir = directory / 'model-calls'
    starts = {p.name.removesuffix('.started.json') for p in calls_dir.glob('*.started.json')}
    ends = {p.name.removesuffix('.terminal.json') for p in calls_dir.glob('*.terminal.json')}
    if starts != ends:
        raise ValueError('model-provider batch has no terminal receipt; attempts remain unknown')
    calls, batches = [], {}
    for name in sorted(ends):
        path = calls_dir / (name + '.terminal.json')
        receipt = read_json(path, max_bytes=path.stat().st_size)
        if (receipt['schema'] != 'semloom.imlane.duckdb.q3.model_batch.v2'
                or receipt['provider_mode'] != mode or receipt['status'] != 'succeeded'
                or receipt['client_closed'] is not True):
            raise ValueError('native model-provider execution or IPC client close remains incomplete')
        request, response = receipt['request'], receipt['response']
        if (receipt['ipc_attempted'] is not True
                or receipt['engine_generate_invocations'] != len(receipt['calls'])
                or content_digest(request) != receipt['request_sha256']
                or content_digest(response) != receipt['response_sha256']
                or request['schema'] != SCHEMA or response['schema'] != SCHEMA
                or request['batch_id'] != receipt['batch_id']
                or response['batch_id'] != request['batch_id']
                or response['status'] != 'succeeded'
                or request['model'] != provider['model'] or response['model'] != provider['model']
                or len(response['calls']) != len(receipt['calls'])
                or request['messages_batch'] != [c['requested_payload']['messages'] for c in receipt['calls']]
                or request['sampling_params'] != dict(provider['sampling_params'], n=1)):
            raise ValueError('native IPC request/result identity differs from its same batch')
        for position, call in enumerate(receipt['calls']):
            payload = call['requested_payload']
            sampling = {key: value for key, value in payload.items() if key != 'messages'}
            expected_sampling = dict(provider['sampling_params'], model=provider['model'], n=1)
            if (call['position'] != position or call['attempted'] is not True or call['status'] != 'succeeded'
                    or sampling != expected_sampling
                    or content_digest(payload) != call['request_payload_sha256']
                    or content_digest(payload['messages']) != call['messages_sha256']
                    or call['response'] != response['calls'][position]
                    or call['response']['position'] != position
                    or call['response']['attempted'] is not True
                    or call['response']['status'] != 'succeeded'
                    or type(call['response']['generated_text']) is not str
                    or call['response']['finish_reason'] not in ('stop', 'length')
                    or call['response']['requested_max_tokens'] != provider['sampling_params']['max_tokens']
                    or call['response']['resolved_max_tokens'] != provider['sampling_params']['max_tokens']
                    or call['response_id'] != call['response']['request_id']
                    or call['usage'] != call['response']['usage']
                    or call['truncated'] != (call['response']['finish_reason'] == 'length')):
                raise ValueError('native IPC request/result differs from the declared Q3 model binding')
            calls.append(call)
        batches[name] = receipt
    for path in sorted((directory / 'udf-receipts').glob('q3-batch-*.json')):
        udf = read_json(path, max_bytes=path.stat().st_size)
        invocation = udf['provider_calls'][0]
        matches = [name for name, batch in batches.items()
            if batch['worker_pid'] == udf['worker_pid']
            and invocation['started_monotonic_ns'] <= batch['started_monotonic_ns']
                <= batch['finished_monotonic_ns'] <= invocation['finished_monotonic_ns']]
        if len(matches) != 1:
            raise ValueError('UDF call must identify exactly one same-worker IPC batch')
        batch = batches.pop(matches[0])
        for source, prediction, call in zip(udf['sources'], udf['predictions'], batch['calls'], strict=True):
            if (any(source[key] != prediction[key] for key in ('occurrence_id', 'id', 'reviewId', 'messages_sha256'))
                    or source['messages_sha256'] != call['messages_sha256']
                    or prediction['generated_text'] != call['response']['generated_text']):
                raise ValueError('IPC response position differs from its native Q3 occurrence')
    if batches:
        raise ValueError('IPC batches remain unassociated with a native Q3 invocation')
    ids = [call['response_id'] for call in calls]
    if len(ids) != len(set(ids)):
        raise ValueError('IPC model request identities repeat across occurrences')
    audit['ipc'] = dict(batch_rpc_invocations=len(ends), engine_generate_invocations=len(calls),
        logical_rows=len(audit['predictions']),
        physical_model_attempts=None, truncated=sum(call['truncated'] for call in calls),
        request_ids=ids,
        usage=[call['usage'] for call in calls], batches=len(ends),
        sampling_params=provider['sampling_params'], model=provider['model'],
        declared_query_deadline_monotonic=provider.get('deadline_monotonic'),
        transport='independent AF_UNIX IPC to one AsyncLLM; database/executor uses author ArrowLane')
    audit['comparison_qualified'] = False
    audit['comparison_scope'] = 'single query; quality, external GPU/SHM exit and repetition assessed separately'
    return audit


def run_worker(args, *, service_factory=None):
    from integrations.imlane_duckdb.model_provider import configuration as provider_configuration
    from integrations.imlane_duckdb.model_service import ModelService
    from integrations.imlane_duckdb.runtime import DuckDBScope
    from integrations.imlane_records import owner

    for name in ('config', 'output', 'author_root', 'driver', 'sembench_checkout'):
        setattr(args, name, Path(getattr(args, name)).resolve())
    args.method_python = Path(args.method_python).absolute()
    new_private_directory(args.output)
    window = QueryWindow(args.query_seconds, args.cleanup_seconds, args.stop_grace_seconds,
                         cleanup_path=args.output / 'cleanup-start.json')
    report = dict(schema=CONFIG_SCHEMA, status='failed', performance_claim=False, cleanup=[],
        model_requests=None, physical_gpu_exit_confirmed=None, outer_host_lease_required=True,
        kind=args.kind, warmup_semantic_requests=0, whole_query_retries=0,
        source_preparation_in_cold_scope=True, service_close=None)
    report['source_to_sink_timing_scope'] = ('fresh original CSV read/parse, occurrence projection, necessary input.csv '
        'materialization/flush, native driver/SQL/Arrow/UDF/provider and complete COUNT; '
        'outer source/packet identity hashes and statistics, packet JSON, evaluation and owner close excluded; '
        'UDF/provider fingerprints and synchronous request receipts remain included, along with necessary IPC, '
        'attempt reservation and failure control; this is not the shared v3 comparison scope')
    args.scopes = []
    args.stop_budget = owner.StopBudget(threading.Event())
    previous = {number: signal.signal(number, args.stop_budget.signal)
                for number in (signal.SIGTERM, signal.SIGINT)}
    primary = service = None
    pending_input = None

    def publish_source_audit():
        nonlocal pending_input
        snapshot, pending_input = pending_input, None
        if snapshot is None:
            return
        # Transfer once before I/O. A partial/uncertain publication must never
        # be retried by failure cleanup against the same exclusive artifact.
        report['input_audit_started_monotonic_ns'] = time.monotonic_ns()
        try:
            report['source'] = publish_input_packet(args.output / 'input', snapshot)['source']
        finally:
            report['input_audit_ended_monotonic_ns'] = time.monotonic_ns()

    try:
        config = configuration(read_json(args.config))
        mode = config['provider_mode']
        if mode == 'software_ipc' and service_factory is None:
            raise ValueError('software_ipc validation requires an explicit no-model test service factory')
        report.update(provider_mode=mode, model_requests=0 if mode == 'software_ipc' else None)
        for name in ('udf-receipts', 'model-calls', 'model-service'):
            (args.output / name).mkdir(mode=0o700)
        service_environment = dict(os.environ)
        service = (ModelService if service_factory is None else service_factory)(
            config['model_service'], args.output / 'model-service', environment=service_environment,
            deadline=window.query_deadline, cleanup_deadline=window.begin_cleanup,
            stop_budget=args.stop_budget, stop_grace_s=args.stop_grace_seconds)
        # This validation happens before allocating the model service. No
        # GenerationSpec constructor may inject short-output/stop defaults.
        provider = provider_configuration(dict(schema='semloom.imlane.duckdb.q3.model_provider.v2',
            provider_mode=mode, socket_path=str(service.socket_path), model=service.served_model_name,
            sampling_params=config['sampling_params'], request_timeout_s=config['request_timeout_s'],
            deadline_monotonic=window.query_deadline,
            receipt_dir=str(args.output / 'model-calls')))
        report['service'] = service.report
        with service:
            remaining = window.query_deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('model startup exhausted the complete query allowance')
            provider['request_timeout_s'] = min(provider['request_timeout_s'], remaining)
            context = service.ready_info.get('max_model_len')
            if type(context) is int and provider['sampling_params']['max_tokens'] > context:
                raise ValueError('requested output exceeds the actual model context')
            write_private_json(args.output / 'model-provider.json', provider)
            kernel_root = Path(__file__).resolve().parents[2] / 'integrations/imlane_duckdb'
            udf = args.output / 'udf.json'
            write_private_json(udf, dict(provider_path=str(kernel_root / 'model_provider.py'),
                provider_function='infer_batch', provider_mode=mode,
                receipt_dir=str(args.output / 'udf-receipts')))
            args.workload_environment = dict(SEMLOOM_IMLANE_Q3_CONFIG=str(udf),
                SEMLOOM_IMLANE_Q3_MODEL_CONFIG=str(args.output / 'model-provider.json'))
            window.check()
            args.startup_timeout_s = window.query_deadline - time.monotonic()
            checked_batches = set()
            try:
                with DuckDBScope(args, report, kernel=kernel_root / 'q3_udf.py') as scope:
                    try:
                        window.check()
                        scope.check()
                        service.check()
                        report['source_started_ns'] = time.monotonic_ns()
                        pending_input = materialize_input(Path(config['reviews_csv']).resolve(),
                            args.output / 'input', provider_mode=mode)
                        command = [str(args.driver), '--input', str(args.output / 'input/input.csv'),
                            '--module', str(kernel_root / 'q3_udf.py'), '--function', 'infer_q3',
                            '--kind', args.kind, '--output', str(args.output / 'query.json')]
                        if args.threads is not None:
                            command += ['--threads', str(args.threads)]
                        process, writer = owner.spawn(command, scope.env, args.output / 'query.log')
                        scope.children.append((process, writer))
                        scope.track_root(process)
                        while True:
                            # The author IPC may still be awaiting a result
                            # after a Python UDF failed. Preserve that failure
                            # and stop this query rather than waiting for a
                            # valid scalar that the failed batch cannot supply.
                            for path in (args.output / 'model-calls').glob('*.terminal.json'):
                                if path in checked_batches:
                                    continue
                                batch = read_json(path, max_bytes=path.stat().st_size)
                                if batch['status'] != 'succeeded' or batch['client_closed'] is not True:
                                    report['model_batch_failure'] = dict(artifact=path.name,
                                        status=batch['status'], error_type=batch.get('error_type'),
                                        physical_model_attempts=None)
                                    raise RuntimeError('native Q3 model-provider batch failed; inspect ' + path.name)
                                checked_batches.add(path)
                            window.observe(args.output / 'query.json', args.output / 'result-ready.json')
                            if window.ready_ns is not None:
                                if 'result_ready_monotonic_ns' not in report:
                                    native = read_json(args.output / 'query.json')
                                    native_version = native.get('query_clock_version')
                                    report['native_query_clock_version'] = native_version
                                    report['query_clock_version'] = ('imlane.q3.source_to_sink.v2'
                                        if native_version == 'imlane.q3.source_to_sink.v1' else None)
                                    report['result_ready_monotonic_ns'] = window.ready_ns
                                    report['ready_source_to_sink_seconds'] = (window.ready_ns - report['source_started_ns']) / 1e9
                                    report['cold_worker_entry_to_sink_seconds'] = window.ready_ns / 1e9 - window.started
                                service.begin_cleanup(window.cleanup_deadline)
                            window.check()
                            scope.check()
                            service.check()
                            code = process.poll()
                            if code is not None:
                                writer.join(1)
                                if code:
                                    raise RuntimeError('native model Q3 driver failed; retain author/IPC evidence')
                                if window.ready_ns is None:
                                    raise ValueError('native model Q3 exited without a qualified ready result')
                                break
                            time.sleep(.05)
                    finally:
                        service.begin_cleanup(window.begin_cleanup())
                report['author_cleanup'] = owner.resource_state(scope)
            finally:
                service.begin_cleanup(window.begin_cleanup())
        report['service_close'] = service.report
        args.stop_budget.check()
        if time.monotonic() > window.begin_cleanup():
            raise TimeoutError('native model Q3 cleanup exceeded its single allowance')
        report['owned_cleanup_completed_monotonic_ns'] = time.monotonic_ns()
        publish_source_audit()
        report['audit_started_monotonic_ns'] = time.monotonic_ns()
        audit = audit_model_result(args.output, args.sembench_checkout)
        write_private_json(args.output / 'audit.json', audit)
        report['audit_ended_monotonic_ns'] = time.monotonic_ns()
        report.update(status='completed', positive_review_cnt=audit['native_result']['positive_review_cnt'],
            logical_model_requests=len(audit['predictions']), ipc=audit['ipc'],
            quality=audit['quality'], process_exit_scope='model/author owners; outer GPU/SHM verification pending')
    except BaseException as error:
        primary = error
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        raise
    finally:
        window.begin_cleanup()
        if service is not None:
            # __enter__ failures also own partially started resources. The
            # service close is idempotent and shares this absolute deadline.
            with args.stop_budget.cleanup():
                try:
                    service.close(deadline=window.cleanup_deadline)
                except BaseException as error:
                    if primary is None:
                        primary = error
                        report.update(status='failed', error_type=type(error).__name__, error=redact_text(str(error)))
                    else:
                        primary.add_note('model service cleanup also failed: ' + type(error).__name__)
                report['service_close'] = service.report
        try:
            publish_source_audit()
        except BaseException as error:
            if primary is None:
                primary = error
                report.update(status='failed', error_type=type(error).__name__, error=redact_text(str(error)))
            else:
                primary.add_note('input audit publication also failed: ' + type(error).__name__)
        for number, handler in previous.items():
            signal.signal(number, handler)
        report['deadline'] = window.report()
        report['worker_ended_monotonic_ns'] = time.monotonic_ns()
        if window.cleanup_publication_error is not None:
            error = window.cleanup_publication_error
            if primary is None:
                primary = error
                report.update(status='failed', error_type=type(error).__name__, error=redact_text(str(error)))
            else:
                primary.add_note('failure cleanup marker publication also failed: ' + type(error).__name__)
        try:
            write_private_json(args.output / 'worker.json', report)
        except BaseException as error:
            if primary is None:
                raise
            primary.add_note('model worker report also failed: ' + type(error).__name__)
        if primary is not None:
            raise primary
    return report


def supervised_run(command, output, supervision_dir, *, query_seconds=1200,
                   cleanup_seconds=120, stop_grace_seconds=8, monitor=None):
    """Bind the generic process supervisor to the native result's close deadline.

    ``output`` must be fresh and separate from the supervision directory. The
    worker owns its creation. The caller retains host/GPU/SHM qualification and
    must inspect its ResourceMonitor.finish() result exactly once.
    """
    from src.experiments.lotus_records_supervisor import supervise
    output, supervision_dir = Path(output), Path(supervision_dir)
    if (output.exists() or output.resolve() == supervision_dir.resolve()
            or output.resolve() in supervision_dir.resolve().parents):
        raise ValueError('worker output must be fresh and separate from process-supervision artifacts')
    window = QueryWindow(query_seconds, cleanup_seconds, stop_grace_seconds)

    def watch(identities):
        if monitor is not None:
            monitor(identities)
        ready = output / 'result-ready.json'
        if window.ready_ns is None and ready.exists():
            value = read_json(ready)
            if value['schema'] != READY_SCHEMA:
                raise ValueError('unexpected native IMLane result-ready schema')
            window.accept_ready(value['result_ready_monotonic_ns'], clock=value['clock'])
        cleanup = output / 'cleanup-start.json'
        if window.cleanup_deadline is None and cleanup.exists():
            window.accept_cleanup(read_json(cleanup))
        window.check()

    primary = None
    try:
        return supervise(command, supervision_dir, timeout_s=query_seconds + cleanup_seconds,
            grace_s=stop_grace_seconds, natural_exit_grace_s=stop_grace_seconds / 2, monitor=watch)
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            write_private_json(supervision_dir / 'native-deadline.json', window.report())
        except BaseException as error:
            if primary is None:
                raise
            primary.add_note('native supervision deadline report also failed: ' + type(error).__name__)


def _positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError('finite positive seconds required')
    return number


def main(argv=None):
    from src.experiments.native_imlane_query import _positive_int
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'output', 'author-root', 'method-python', 'driver', 'sembench-checkout'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--kind', choices=('thread', 'process'), default='thread')
    parser.add_argument('--executor-count', type=_positive_int)
    parser.add_argument('--threads', type=_positive_int)
    parser.add_argument('--query-seconds', type=_positive, default=1200)
    parser.add_argument('--cleanup-seconds', type=_positive, default=120)
    parser.add_argument('--stop-grace-seconds', type=_positive, default=8)
    run_worker(parser.parse_args(argv))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
