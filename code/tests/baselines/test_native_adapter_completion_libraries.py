"""Five opt-in fresh-process pinned-library cases against a public localhost fixture.

Select one method per process with SEMLOOM_NATIVE_ADAPTER_COMPLETION_TEST=1.
ROOT is a private artifact parent; RAY_TMP is a new short runtime path. BUDGET
names an existing CellBudgetLedger; optional BUDGET_LIMIT defaults to 16 and
must be 1..16, matching that ledger. BUDGET_ID defaults to native-adapter-completion.
All names have the SEMLOOM_NATIVE_ADAPTER_COMPLETION_ prefix. Source imports use
the caller's PYTHONPATH; no server path is embedded.

The campaign expects 14 logical/fixture calls: 14 client-to-proxy POSTs plus
14 proxy-to-fixture POSTs = 28 physical POSTs, within the physical limit 32.
Missing libraries fail active cases. No kernel, UDF, graph, or site package is
changed; EOF injection only wraps the real SDK iterator's close method.
"""
from collections import Counter
from contextlib import contextmanager, ExitStack
from dataclasses import asdict
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.metadata
import json
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import new_private_directory, open_private_text, write_private_json
from src.baselines.common.redact import redact_json_values
from src.baselines.text.frameworks import native_calls as native
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.async_fixed_model import exception_details
from src.execution_provider.adapters.full_response import decode_full_response, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import NativeTaskSession, build_native_execution, prepare_native_task
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.native_adapter_query import _runtime
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from src.scheduling.core.session_contract import SessionTimeouts, State, Usage


_PREFIX = 'SEMLOOM_NATIVE_ADAPTER_COMPLETION_'
_PROCESS_USED = False
_PLAN = dict(expected_logical=14, expected_fixture=14, expected_proxy=14,
             expected_physical=28, max_physical=32, max_logical=16,
             model_requests=0, gpu_requests=0, window_s=300, case_window_s=70, cleanup_reserve_s=45)


def _payload(index):
    return json.dumps(dict(model='public-fixture', call_id='fixture-call:'+str(index), row_id=index,
        messages=[dict(role='user', content='fixed public synthetic duplicate')]),
        separators=(',', ':')).encode()


def _response(payload):
    row = json.loads(payload)
    body = b'public-fixture-response:' + payload + b'\n\x00\xff'
    headers = (('Server', 'public-fixture'), ('Date', 'Thu, 01 Jan 1970 00:00:00 GMT'),
        ('Content-Type', 'application/octet-stream'), ('X-Repeated', 'one'), ('X-Repeated', 'two'),
        ('X-Call-Id', row['call_id']), ('Content-Length', str(len(body))))
    return native.FullModelResponse(429 if row['row_id'] % 2 == 0 else 500, headers, body)


def _call(endpoint, index):
    return native.NativeCall('fixture-call:'+str(index), index, _payload(index), endpoint,
        (('Content-Type', 'application/json'), ('X-Input', 'one'), ('X-Input', 'two')),
        10, max_response_bytes=8192)


@contextmanager
def _fixture(root):
    state = SimpleNamespace(requests=[], active=0, peak=0, proxy_arrivals=0, threads=[], lock=threading.Lock(),
                            cleanup=None)
    with open_private_text(root/'fixture-receipts.jsonl') as receipts:
        def record(row):
            receipts.write(json.dumps(redact_json_values(row), sort_keys=True)+'\n')
            receipts.flush()

        class Server(ThreadingHTTPServer):
            daemon_threads = False
            block_on_close = True

            def get_request(self):
                connection, address = super().get_request()
                connection.settimeout(5)
                return connection, address

            def process_request_thread(self, request, address):
                with state.lock:
                    state.threads.append(threading.current_thread())
                super().process_request_thread(request, address)

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def do_POST(self):
                with state.lock:
                    item = dict(sequence=len(state.requests), path=self.path,
                        input_headers=self.headers.get_all('X-Input'), received_ns=time.monotonic_ns())
                    state.requests.append(item)
                    state.active += 1
                    state.peak = max(state.peak, state.active)
                    record(dict(event='fixture_received', **item))
                written = False
                failure = None
                try:
                    size = int(self.headers.get('Content-Length', '0'))
                    if not 0 < size <= 4096:
                        raise ValueError('public fixture request exceeds its byte allowance')
                    payload = self.rfile.read(size)
                    row = json.loads(payload)
                    if payload != _payload(row['row_id']):
                        raise ValueError('fixture received a body outside the public synthetic input')
                    full = _response(payload)
                    with state.lock:
                        item.update(call_id=row['call_id'], row_id=row['row_id'],
                                    body_base64=base64.b64encode(payload).decode())
                        record(dict(event='fixture_complete_body', **item))
                    self.send_response_only(full.status_code)
                    for name, value in full.headers:
                        self.send_header(name, value)
                    self.end_headers()
                    self.wfile.write(full.body)
                    self.wfile.flush()
                    written = True
                except BaseException as error:
                    failure = type(error).__name__
                    raise
                finally:
                    self.close_connection = True
                    with state.lock:
                        state.active -= 1
                        item.update(response_written=written, error_type=failure)
                        record(dict(event='fixture_response_written', sequence=item['sequence'],
                            written=written, error_type=failure, ended_ns=time.monotonic_ns()))

            def log_message(self, *_args):
                pass

        server = Server(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .05},
                                  name='public-completion-fixture', daemon=False)
        thread.start()
        primary = None
        try:
            yield 'http://127.0.0.1:'+str(server.server_port)+'/complete', state
        except BaseException as error:
            primary = error
            raise
        finally:
            server.shutdown()
            server.server_close()
            thread.join(5)
            state.cleanup = dict(active=state.active, serving_thread_alive=thread.is_alive(),
                live_handler_threads=[item.name for item in state.threads if item.is_alive()],
                handler_threads_daemon=[item.daemon for item in state.threads])
            if state.active or thread.is_alive() or state.cleanup['live_handler_threads']:
                error = RuntimeError('public fixture threads or HTTP activity did not stop')
                if primary is None:
                    raise error
                primary.add_note(str(error))


class CloseFailure(RuntimeError):
    pass


class _CloseOnce:
    def __init__(self, iterator, failure, *, at_eof=False):
        if not callable(getattr(iterator, 'close', None)):
            raise AssertionError('the actual vendor iterator has no close method')
        self.iterator, self.failure = iterator, failure
        self.at_eof = at_eof
        self.real_close_calls = self.injected = 0

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.iterator)
        except StopIteration:
            if self.at_eof:
                self.close()
            raise

    def close(self):
        if self.real_close_calls:
            return
        self.real_close_calls += 1
        self.iterator.close()
        self.injected += 1
        raise self.failure


@unittest.skipUnless(os.environ.get(_PREFIX+'TEST') == '1', 'actual completion-library cases are opt-in')
class NativeAdapterCompletionLibraryTests(unittest.TestCase):
    def setUp(self):
        global _PROCESS_USED
        if _PROCESS_USED:
            raise RuntimeError('select exactly one completion-library case per fresh process')
        _PROCESS_USED = True
        self.budget_limit = int(os.environ.get(_PREFIX+'BUDGET_LIMIT', '16'))
        if not 1 <= self.budget_limit <= 16:
            raise ValueError('BUDGET_LIMIT must be an integer from 1 to 16')
        self.root = Path(os.environ[_PREFIX+'ROOT'])/self._testMethodName
        new_private_directory(self.root)
        self.sequence = 0
        self.details = dict(status='failed', case=self._testMethodName, plan=_PLAN,
                            budget_limit=self.budget_limit, budget_physical_limit=2*self.budget_limit,
                            test_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        self.addCleanup(lambda: self.save('final', self.details))
        self.ledger = CellBudgetLedger(Path(os.environ[_PREFIX+'BUDGET']),
            AttemptBudget(os.environ.get(_PREFIX+'BUDGET_ID', 'native-adapter-completion'), self.budget_limit))
        self.ray_tmp = Path(os.environ[_PREFIX+'RAY_TMP'])
        self.details['versions'] = {name: importlib.metadata.version(name) for name in ('daft', 'ray', 'pyarrow')}
        self.assertEqual(self.details['versions']['daft'], '0.7.21')
        self.assertEqual(self.details['versions']['ray'], '2.56.1')
        self.save('setup', self.details)

    def save(self, stage, value):
        self.sequence += 1
        write_private_json(self.root/(str(self.sequence).zfill(3)+'-'+stage+'.json'), redact_json_values(value))

    @contextmanager
    def observed(self, maximum):
        identity = self._testMethodName
        self.ledger.reserve_unit(identity, maximum)
        unit = self.ledger.claim_shared_unit(identity)
        allowed = {hashlib.sha256(_payload(i)).hexdigest() for i in range(maximum)}
        seen = set()
        gateway = thread = state = None
        primary = None
        with open_private_text(self.root/'pre-send.jsonl') as attempts, \
                open_private_text(self.root/'proxy-arrivals.jsonl') as arrivals:
            def before(_route, payload):
                digest = hashlib.sha256(payload).hexdigest()
                arrivals.write(json.dumps(dict(sequence=state.proxy_arrivals, request_sha256=digest,
                                                arrived_ns=time.monotonic_ns()))+'\n')
                arrivals.flush()
                state.proxy_arrivals += 1
                if digest not in allowed or digest in seen:
                    raise ValueError('fixture forwarding changed or repeated a complete call')
                number = unit.reserve(digest)
                seen.add(digest)
                attempts.write(json.dumps(dict(attempt=number, request_sha256=digest,
                                                pre_send_ns=time.monotonic_ns()))+'\n')
                attempts.flush()
            try:
                with _fixture(self.root) as (upstream, state):
                    with ObservationGateway(routes=(GatewayRoute('public-fixture', 'model', upstream),),
                            trace_path=self.root/'proxy-trace.jsonl', request_timeout_s=10,
                            before_forward=before, preserve_http=True) as gateway:
                        thread = gateway._thread
                        try:
                            yield gateway.endpoint_url('public-fixture', 'model'), state, gateway
                        finally:
                            self.details['proxy_trace'] = gateway.snapshot(5)
            except BaseException as error:
                primary = error
                self.details['primary_error'] = exception_details(error)
                raise
            finally:
                self.ledger.close_shared_unit(identity)
                self.details.update(pre_send_charged=unit.attempts,
                    proxy_arrivals_count=0 if state is None else state.proxy_arrivals,
                    fixture_requests=[] if state is None else state.requests,
                    fixture_cleanup=None if state is None else state.cleanup,
                    fixture_peak=None if state is None else state.peak,
                    gateway_thread_stopped=thread is not None and not thread.is_alive(),
                    ledger=self.ledger.snapshot())
                if 'proxy_trace' in self.details:
                    proxy, fixture = len(self.details['proxy_trace']), len(self.details['fixture_requests'])
                    self.details.update(actual_proxy_posts=proxy, actual_fixture_posts=fixture,
                                        actual_physical_posts=proxy+fixture)
                self.save('http-and-budget', self.details)
        if primary is None:
            self.assertEqual(self.details['pre_send_charged'], maximum)
            self.assertEqual(self.details['actual_proxy_posts'], maximum)
            self.assertEqual(self.details['proxy_arrivals_count'], self.details['actual_proxy_posts'])
            self.assertEqual(self.details['actual_fixture_posts'], maximum)
            self.assertEqual(self.details['actual_physical_posts'], 2*maximum)
            self.assertTrue(all(row['response_written'] for row in state.requests))
            self.assertTrue(self.details['gateway_thread_stopped'])
            self.assertEqual(state.cleanup['active'], 0)
            self.assertFalse(state.cleanup['serving_thread_alive'])
            self.assertFalse(state.cleanup['live_handler_threads'])
            self.assertFalse(any(state.cleanup['handler_threads_daemon']))
            self.assertTrue(all(not row['error_type'] and not row['errors']
                and row['upstream_status'] in (429, 500) and row['client_status'] == row['upstream_status']
                and row['request_terminal_monotonic_ns'] is not None
                and row['upstream_response_body_read_completed_monotonic_ns'] is not None
                and row['response_write_status'] == 'completed'
                and row['response_write_completed_monotonic_ns'] is not None
                and row['request_body_sha256'] == row['forwarded_body_sha256']
                for row in self.details['proxy_trace']))

    @contextmanager
    def runtime(self, arm, options, physical=None):
        self.details.update(options=asdict(options), runtime_arm=arm)
        self.save('runtime-start', self.details)
        report = None
        try:
            with ExitStack() as stack:
                physical, report = _runtime(stack, arm, options, physical, self.ray_tmp, None)
                self.details['runtime'] = report
                if arm != 'fixed-map-native-daft':
                    import ray
                    self.assertEqual(ray.cluster_resources().get('CPU'), options.num_threads)
                    self.assertEqual(ray.cluster_resources().get('GPU', 0), 0)
                self.save('runtime-ready', self.details)
                yield physical
        finally:
            self.save('runtime-exit', dict(runtime=report))

    def check_response(self, index, full):
        expected = _response(_payload(index))
        self.assertEqual(full.status_code, expected.status_code)
        self.assertEqual(full.body, expected.body)
        self.assertEqual(full.http_version, 'HTTP/1.1')
        self.assertEqual(Counter((key.lower(), value) for key, value in full.headers),
                         Counter((key.lower(), value) for key, value in expected.headers))
        self.assertEqual([value for key, value in full.headers if key.lower() == 'x-repeated'], ['one', 'two'])

    def remember_outcomes(self, outcomes):
        self.details.setdefault('complete_outcomes', []).extend(dict(call_id=item.call_id, row_id=item.row_id,
            transport_error=item.transport_error, execution_error=item.execution_error,
            response_wire=None if item.response is None else
                base64.b64encode(encode_full_response(item.response)).decode()) for item in outcomes)

    def batch(self, executor, endpoint, start=0):
        outcomes = []
        try:
            outcomes.extend(executor.iter_batch(tuple(_call(endpoint, i) for i in range(start, start+2)), timeout_s=35))
        finally:
            self.remember_outcomes(outcomes)
            self.details.setdefault('close_reports', []).append(executor.last_close_report)
            self.save('batch-attempt', self.details)
        self.assertEqual({(item.call_id, item.row_id) for item in outcomes},
                         {('fixture-call:'+str(i), i) for i in range(start, start+2)})
        for item in outcomes:
            self.assertFalse(item.transport_error or item.execution_error)
            self.check_response(item.row_id, item.response)
        self.assertTrue(executor.last_close_report['clean'])
        self.assertTrue(executor.last_close_report['graph_consumer_stopped'])
        self.assertEqual(native._DAFT_CONTROLS, {})

    def ray_released(self, owned_ids=()):
        import ray
        deadline = time.monotonic()+10
        while True:
            actors = {key: value.get('State') for key, value in ray._private.state.actors().items()}
            available, cluster = ray.available_resources(), ray.cluster_resources()
            if (available.get('CPU', 0) == cluster.get('CPU')
                    and all(actors.get(key) == 'DEAD' for key in owned_ids)):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError('actual Ray CPU resources remain held')
            time.sleep(.02)
        keys = ('CPU', 'GPU', 'object_store_memory')
        self.details.setdefault('ray_release', []).append(dict(
            available={key: available.get(key, 0) for key in keys},
            cluster={key: cluster.get(key, 0) for key in keys}, actor_states=actors,
            checked_owned_actor_ids=list(owned_ids)))

    def test_core_cpu1_public_runtime(self):
        options = NativeGraphOptions(concurrency=1, num_threads=1, ray_batch_rows=256, source_blocks=2)
        with self.observed(2) as (endpoint, _state, _gateway):
            with self.runtime('supplier-native-ray', options):
                executor = native.NativeCallExecutor('ray', options)
                self.batch(executor, endpoint)
                self.ray_released(executor.last_close_report['owned_ray_actor_ids'])
                self.assertEqual(executor.last_close_report['owned_ray_actors'], 1)
                self.assertEqual(executor.last_close_report['ray_actor_shutdown_requested'], 1)
        self.assertTrue(self.details['runtime']['driver_disconnected'])
        self.details['status'] = 'passed'

    def test_ray_data_cpu2_public_runtime(self):
        rejected = NativeGraphOptions(concurrency=1, num_threads=1, ray_actors=1,
            ray_async_batches_per_actor=1, ray_batch_rows=1, source_blocks=2)
        options = NativeGraphOptions(concurrency=1, num_threads=2, ray_actors=1,
            ray_async_batches_per_actor=1, ray_batch_rows=1, source_blocks=2)
        with self.observed(2) as (endpoint, state, gateway):
            with patch.dict(os.environ, {'RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY': '4'}), \
                    patch('src.experiments.postgresql.native_adapter_query.native_ray_runtime') as runtime, \
                    patch('builtins.__import__') as imports:
                with ExitStack() as stack, self.assertRaisesRegex(ValueError, 'spare CPU slot') as caught:
                    _runtime(stack, 'fixed-map-native-ray', rejected, None, self.ray_tmp, None)
                runtime.assert_not_called()
                imports.assert_not_called()
                self.assertEqual(os.environ['RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY'], '4')
            self.assertEqual((len(state.requests), len(gateway.snapshot(5)), state.proxy_arrivals), (0, 0, 0))
            self.details['cpu1_rejection'] = dict(options=asdict(rejected),
                error=exception_details(caught.exception), runtime_starts=0, import_calls=0,
                import_batch_environment_changed=False, fixture_posts=0, proxy_posts=0, proxy_arrivals=0)
            self.save('cpu1-rejected-before-runtime-and-http', self.details)
            with self.runtime('fixed-map-native-ray', options):
                executor = native.NativeCallExecutor('ray-data', options)
                self.batch(executor, endpoint)
                self.assertEqual(executor.last_close_report['native_graph_parameters']['async_batches_effective'], 1)
                self.ray_released()
        self.assertTrue(self.details['runtime']['driver_disconnected'])
        self.details['status'] = 'passed'

    def test_semloom_cpu1_arrow_native_session(self):
        options = NativeGraphOptions(concurrency=1, num_threads=1, ray_batch_rows=256, source_blocks=2)
        physical = RayMapConfig('resolved-by-owned-runtime', 1, 1, MAX_FRAME_BYTES+24, 4*MAX_FRAME_BYTES,
                                payload_backend='arrow', response_mode='full')
        events = []
        with self.observed(2) as (endpoint, _state, _gateway):
            with self.runtime('fixed-map-semloom', options, physical) as selected:
                execution = build_native_execution(FixedModelConfig(endpoint, 'public-fixture', 10000),
                    physical=selected, max_tasks=1, max_active_requests=1, observer=events.append,
                    timeouts=SessionTimeouts(backend_s=30))
                flow = None
                complete = False
                primary = None
                try:
                    flow = NativeTaskSession(execution, 'public-query', 'complete-map')
                    tasks = tuple(prepare_native_task(_payload(i), i, row_sequence=i,
                        call_id='fixture-call:'+str(i), max_result_bytes=8192) for i in range(2))
                    offered = 0
                    sealed = False
                    received = set()
                    deadline = time.monotonic()+35
                    while not complete:
                        if time.monotonic() >= deadline:
                            raise TimeoutError('actual Arrow/Ray native session deadline exceeded')
                        if offered < len(tasks):
                            accepted = flow.offer(tasks[offered:offered+flow.limits.offer_tasks])
                            self.assertIn(accepted.status, ('ACCEPTED', 'BACKPRESSURE'))
                            offered += accepted.accepted_prefix_count
                        if offered == len(tasks) and not sealed:
                            flow.end_input()
                            sealed = True
                        progress = flow.advance(1)
                        for delivery in progress.deliveries:
                            try:
                                index = delivery.key.sequence
                                self.assertNotIn(index, received)
                                self.assertEqual(delivery.status, 'completed')
                                self.assertEqual((delivery.info.call_id, delivery.info.row_sequence, delivery.info.stage_id),
                                                 ('fixture-call:'+str(index), index, 'model'))
                                self.check_response(index, decode_full_response(delivery.result))
                                self.details.setdefault('complete_outcomes', []).append(dict(call_id=delivery.info.call_id,
                                    row_id=index, response_wire=base64.b64encode(delivery.result).decode()))
                                received.add(index)
                            finally:
                                flow.release((delivery.lease_id,))
                        self.assertNotIn(progress.state, (State.FAILED, State.CANCELLED))
                        complete = progress.state == State.FINISHED
                        if not complete:
                            flow.wait(progress)
                    self.assertEqual(received, {0, 1})
                except BaseException as error:
                    primary = error
                    raise
                finally:
                    cleanup_errors = []
                    if flow is not None:
                        try:
                            if not complete:
                                flow.request_cancel()
                            self.details['flow_close'] = asdict(flow.close(clean=complete))
                        except BaseException as error:
                            cleanup_errors.append(error)
                    until = time.monotonic()+execution.drain_timeout_s
                    while execution.engine.capacity.usage() != Usage() and time.monotonic() < until:
                        execution.engine.advance()
                        time.sleep(.01)
                    closed = None
                    try:
                        closed = execution.close(execution.drain_timeout_s)
                    except BaseException as error:
                        cleanup_errors.append(error)
                    self.details.update(execution_closed=closed, core_usage=asdict(execution.engine.capacity.usage()),
                        core_jobs=len(execution.engine.jobs.jobs), events=events,
                        execution_cleanup_errors=[exception_details(error) for error in cleanup_errors])
                    self.save('semloom-close', self.details)
                    if cleanup_errors:
                        if primary is None:
                            raise cleanup_errors[0]
                        for error in cleanup_errors:
                            primary.add_note('Native session cleanup also failed: '+type(error).__name__)
                    if primary is None:
                        self.assertIs(closed, True)
                        self.assertEqual(execution.engine.capacity.usage(), Usage())
                        self.assertFalse(execution.engine.jobs.jobs)
                actor_ids = [key for event in events if event.get('event') == 'ray_worker_pool'
                             for key in event['actor_ids']]
                self.assertEqual(len(actor_ids), 1)
                self.ray_released(actor_ids)
        self.assertTrue(self.details['runtime']['driver_disconnected'])
        self.details['status'] = 'passed'

    def eof_case(self, backend):
        options = NativeGraphOptions(concurrency=4, num_threads=8, source_blocks=2)
        arm = 'fixed-map-native-daft' if backend == 'daft' else 'fixed-map-native-ray'
        self.details['fault_location'] = ('natural_eof_after_real_iterator_close' if backend == 'ray-data'
                                         else 'outer_finally_after_real_iterator_close')
        with self.observed(4) as (endpoint, state, gateway):
            with self.runtime(arm, options):
                if backend == 'daft':
                    import daft
                    owner = type(daft.from_pydict({'probe': [0]}))
                else:
                    from ray.data.dataset import Dataset
                    owner = Dataset
                original = owner.iter_rows
                failure = CloseFailure('public fixture after actual vendor iterator close')
                wrapped = []
                def iterator(instance, *args, **kwargs):
                    iterable = original(instance, *args, **kwargs)
                    actual_iterator = iter(iterable) if backend == 'ray-data' else iterable
                    self.details.update(actual_iterable_type=type(iterable).__module__+'.'+type(iterable).__name__,
                        actual_iterator_type=type(actual_iterator).__module__+'.'+type(actual_iterator).__name__)
                    value = _CloseOnce(actual_iterator, failure, at_eof=(backend == 'ray-data'))
                    wrapped.append(value)
                    return value
                executor = native.NativeCallExecutor(backend, options)
                received = []
                with patch.object(owner, 'iter_rows', iterator):
                    try:
                        with self.assertRaises(CloseFailure) as caught:
                            for item in executor.iter_batch(tuple(_call(endpoint, i) for i in range(2)), timeout_s=35):
                                received.append(item)
                                self.check_response(item.row_id, item.response)
                    finally:
                        self.details['fault_close_report'] = executor.last_close_report
                        self.remember_outcomes(received)
                        self.save('eof-batch-attempt', self.details)
                    self.assertIs(caught.exception, failure)
                    self.assertEqual({(item.call_id, item.row_id) for item in received},
                                     {('fixture-call:0', 0), ('fixture-call:1', 1)})
                    report = executor.last_close_report
                    self.assertEqual(report['received'], report['prepared'])
                    self.assertTrue(report['graph_consumer_stopped'])
                    self.assertTrue(report['unconfirmed_work'])
                    self.assertFalse(report['clean'] or report['reuse_allowed'])
                    self.assertEqual(report['worker_errors'], [dict(stage='graph_consume',
                        type=CloseFailure.__module__+'.'+CloseFailure.__name__)])
                    self.assertEqual(len(wrapped), 1)
                    self.assertEqual((wrapped[0].real_close_calls, wrapped[0].injected), (1, 1))
                    before = (len(state.requests), len(gateway.snapshot(5)), state.proxy_arrivals, len(wrapped))
                    with self.assertRaisesRegex(RuntimeError, 'unconfirmed work'):
                        list(executor.iter_batch(tuple(_call(endpoint, i) for i in range(2, 4))))
                    self.assertEqual((len(state.requests), len(gateway.snapshot(5)), state.proxy_arrivals, len(wrapped)), before)
                    self.details.update(fault_close_report=report, first_error_preserved=True,
                        rejected_batch_added_fixture_posts=0, rejected_batch_added_proxy_posts=0,
                        rejected_batch_added_proxy_arrivals=0,
                        rejected_batch_added_vendor_iterators=0,
                        actual_iterator_type=type(wrapped[0].iterator).__module__+'.'+type(wrapped[0].iterator).__name__)
                    self.save('eof-injected-and-rejected', self.details)
                self.assertEqual(native._DAFT_CONTROLS, {})
                self.assertEqual(state.active, 0)
                if backend == 'daft':
                    self.assertTrue(report['http_pools_closed'])
                else:
                    self.ray_released()
                self.save('fault-runtime-cleared', self.details)
                self.batch(native.NativeCallExecutor(backend, options), endpoint, start=2)
                if backend == 'ray-data':
                    self.ray_released()
        if backend == 'ray-data':
            self.assertTrue(self.details['runtime']['driver_disconnected'])
        self.details['status'] = 'passed'

    def test_daft_real_iterator_close_failure(self):
        self.eof_case('daft')

    def test_ray_data_real_iterator_close_failure(self):
        self.eof_case('ray-data')


if __name__ == '__main__':
    unittest.main()
