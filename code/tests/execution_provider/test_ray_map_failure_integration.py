"""Opt-in actual Ray/Daft/Arrow failures against a bounded loopback HTTP fixture."""

from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
import uuid

from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport, owned_map_worker_pool
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.scheduling.core.session_contract import State, Usage


_REAL = (os.environ.get('SEMLOOM_RAY_FAILURE_TEST') == '1'
         and all(importlib.util.find_spec(name) for name in ('ray', 'daft', 'pyarrow')))


@unittest.skipUnless(_REAL, 'Requires an explicitly enabled, preflight-checked CPU Ray fixture')
class RayMapFailureIntegrationTests(unittest.TestCase):
    observations = []
    fixture_posts = 0

    @classmethod
    def setUpClass(cls):
        import ray
        cls.ray = ray
        if ray.is_initialized():
            raise RuntimeError('failure fixture must own its Ray cluster')
        cls.calls, cls.slow_entered, cls.finish = [], threading.Event(), threading.Event()
        cls.active = 0
        cls.lock = threading.Lock()
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                with cls.lock:
                    cls.calls.append(body)
                    cls.fixture_posts += 1
                    cls.active += 1
                    allowed = cls.fixture_posts <= 16
                try:
                    if not allowed:
                        raise RuntimeError('fixture POST budget exceeded')
                    mode = json.loads(body)['mode']
                    if mode == 'disconnect':
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                    if mode == 'timeout':
                        time.sleep(0.6)
                    if mode == 'slow':
                        cls.slow_entered.set()
                        if not cls.finish.wait(20): return
                    self.send_response(200)
                    self.send_header('Content-Length', '2')
                    self.end_headers()
                    try:
                        self.wfile.write(b'ok')
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                finally:
                    with cls.lock: cls.active -= 1
        cls.http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.server = threading.Thread(target=cls.http.serve_forever, daemon=True)
        cls.server.start()
        cls.temp = tempfile.TemporaryDirectory(prefix='r', dir=os.environ.get('SEMLOOM_RAY_FAILURE_TMP'))
        try:
            ray.init(num_cpus=2, num_gpus=0, include_dashboard=False, _temp_dir=cls.temp.name,
                     runtime_env={'env_vars': {'PYTHONPATH': str(Path(__file__).resolve().parents[2]),
                                              'CUDA_VISIBLE_DEVICES': ''}})
        except BaseException:
            cls.http.shutdown(); cls.http.server_close(); cls.server.join(3)
            cls.temp.cleanup()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.finish.set()
        cls.ray.shutdown()
        cls.http.shutdown(); cls.http.server_close(); cls.server.join(3)
        cls.temp.cleanup()

    def config(self, timeout=3000):
        return FixedModelConfig(f'http://localhost:{self.http.server_port}/fixture', 'fixture', timeout)

    def physical(self, backend):
        return RayMapConfig(self.ray.get_runtime_context().gcs_address, 1, 1,
                            MAX_FRAME_BYTES + 24, 2 * MAX_FRAME_BYTES, payload_backend=backend)

    def task(self, mode='ok'):
        return prepare_native_task(json.dumps({'model': 'fixture', 'mode': mode}).encode(),
                                   0, row_sequence=0, call_id='fixture-call', max_result_bytes=512)

    def assembly(self, physical, config, *, ray_api=None):
        transports, events = [], []
        def factory(*args):
            transport = RayMapTransport(*args, physical=physical, ray_api=ray_api)
            transports.append(transport)
            return transport
        execution = build_fixed_model_execution(config, max_tasks=2, max_active_requests=2,
            transport_factory=factory, observer=events.append)
        return execution, transports[0], events

    def wait_for(self, predicate, advance=lambda: None):
        deadline = time.monotonic() + 25
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail('actual failure fixture exceeded its progress deadline')
            advance()
            time.sleep(0.005)

    def test_identified_payload_failure_releases_core_and_worker_for_next_query(self):
        import pyarrow as pa
        real_ray = self.ray
        class CorruptPayloadRay:
            corrupt = True
            def __getattr__(self, name): return getattr(real_ray, name)
            def put(self, batch):
                if self.corrupt:
                    values = [sid + 999 for sid in batch['session_id'].to_pylist()]
                    batch = batch.set_column(batch.schema.get_field_index('session_id'),
                                             batch.schema.field('session_id'), pa.array(values, type=pa.int64()))
                return real_ray.put(batch)
        for backend in ('arrow', 'daft'):
            with self.subTest(backend=backend):
                proxy = CorruptPayloadRay()
                config = self.config()
                physical = replace(self.physical(backend), worker_pool='failure-' + uuid.uuid4().hex[:8])
                before = len(self.calls)
                owners = []
                with owned_map_worker_pool(real_ray, config, 2, physical):
                    for corrupt in (True, False):
                        proxy.corrupt = corrupt
                        execution, transport, events = self.assembly(physical, config, ray_api=proxy)
                        owners.append(transport.worker_owner)
                        flow = NativeTaskSession(execution, 'fixture-query', 'map')
                        flow.offer((self.task(),))
                        try:
                            if corrupt:
                                self.wait_for(lambda: flow.session.state == State.FAILED,
                                              lambda: flow.advance(1))
                                self.assertEqual(flow.session.error, 'backend task failed')
                                failure = next(e for e in events if e['event'] == 'ray_execution_error')
                                self.assertEqual((failure['stage'], failure['remote_outcome']), ('payload', 'not_sent'))
                                self.assertEqual(failure['reason']['exception_type'], 'builtins.ValueError')
                                self.assertEqual(len(self.calls), before)
                                self.assertEqual(execution.engine.capacity.usage(), Usage())
                            else:
                                flow.advance(1)
                                self.wait_for(lambda: any(r.phase == 'READY' for r in execution.engine.capacity.records.values()),
                                              execution.engine.advance)
                                delivery = flow.advance(1).deliveries[0]
                                self.assertEqual(delivery.result, b'ok')
                                flow.release((delivery.lease_id,))
                            self.assertFalse(transport.unknown)
                            self.assertEqual(transport.used_bytes, 0)
                        finally:
                            flow.close(clean=True)
                            self.assertTrue(execution.close())
                        self.assertEqual(sum(e['event'] == 'ray_block_released' for e in events), 1)
                        self.assertTrue(any(e['event'] == 'ray_transport_closed' for e in events))
                    self.assertNotEqual(*owners)
                self.assertEqual(len(self.calls) - before, 1)
                self.observations.append(dict(case='payload_failure_and_query_reuse', backend=backend,
                                              payload_failure_posts=0, followup_posts=1, retained_bytes=0))

    def test_http_timeout_and_disconnect_keep_core_and_object_charges(self):
        for backend in ('arrow', 'daft'):
            for mode in ('timeout', 'disconnect'):
                with self.subTest(backend=backend, mode=mode):
                    before = len(self.calls)
                    execution, transport, events = self.assembly(self.physical(backend), self.config(300))
                    flow = NativeTaskSession(execution, 'unknown-query', 'map')
                    flow.offer((self.task(mode),))
                    try:
                        self.wait_for(lambda: flow.session.state == State.FAILED, lambda: flow.advance(1))
                        self.assertEqual(len(self.calls) - before, 1)
                        self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
                        self.assertEqual(len(transport.unknown), 1)
                        self.assertGreater(transport.used_bytes, 0)
                        failure = next(e for e in events if e['event'] == 'ray_execution_error')
                        self.assertEqual((failure['stage'], failure['remote_outcome']), ('http', 'unconfirmed'))
                        retained = transport.used_bytes
                        for _ in range(10): execution.engine.advance()
                        self.assertEqual(transport.used_bytes, retained)
                        self.assertEqual(len(self.calls) - before, 1)
                    finally:
                        report = flow.close()
                        self.assertEqual(report.uncertain_requests, 1)
                        with self.assertRaisesRegex(RuntimeError, 'finalization failed'):
                            execution.close()
                    self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
                    self.observations.append(dict(case=mode, backend=backend, posts=1,
                                                  retained_bytes=retained, active_requests=1))

    def test_actual_cancel_waits_for_late_http_result(self):
        for backend in ('arrow', 'daft'):
            with self.subTest(backend=backend):
                self.slow_entered.clear(); self.finish.clear()
                before = len(self.calls)
                execution, transport, _ = self.assembly(self.physical(backend), self.config(10000))
                flow = NativeTaskSession(execution, 'cancel-query', 'map')
                flow.offer((self.task('slow'),))
                try:
                    self.wait_for(self.slow_entered.is_set, lambda: flow.advance(1))
                    flow.session.set_dispatch_enabled(False)
                    flow.request_cancel()
                    self.assertEqual(flow.advance(1).state, State.CANCELLED)
                    report = flow.close()
                    self.assertEqual(report.uncertain_requests, 1)
                    self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
                    self.assertGreater(transport.used_bytes, 0)
                    self.assertFalse(execution.close(0.01))
                    self.finish.set()
                    self.wait_for(lambda: execution.engine.capacity.usage() == Usage(), execution.engine.advance)
                    self.assertFalse(transport.unknown)
                    self.assertEqual(transport.used_bytes, 0)
                    self.assertTrue(execution.close())
                finally:
                    self.finish.set()
                    flow.close(clean=True)
                self.assertEqual(len(self.calls) - before, 1)
                self.observations.append(dict(case='cancel_and_late_reply', backend=backend, posts=1,
                                              held_before_reply=True, retained_bytes_after_reply=0))
