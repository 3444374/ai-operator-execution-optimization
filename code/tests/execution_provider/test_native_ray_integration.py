"""Opt-in real Daft/Ray and local HTTP fixture, with no model or external endpoint."""

from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
import tempfile
import threading
import time
import unittest

from src.execution_provider.adapters.full_response import decode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import NativeTaskSession, build_native_execution, prepare_native_task
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, owned_map_worker_pool
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.scheduling.core.session_contract import State, Usage


_REAL = (os.environ.get('SEMLOOM_NATIVE_RAY_TEST') == '1'
         and all(importlib.util.find_spec(name) for name in ('ray', 'daft', 'pyarrow')))


@unittest.skipUnless(_REAL, 'Set SEMLOOM_NATIVE_RAY_TEST=1 in a preflight-checked Daft/Ray environment')
class NativeRayIntegrationTests(unittest.TestCase):
    def test_real_payload_workers_complete_protocol_and_owned_service_mode(self):
        import ray
        calls, events = [], []
        release = threading.Event()
        success = b'{"model":"fixture","choices":[{"message":{"content":"yes"},"finish_reason":"stop",' \
                  b'"logprobs":{"content":[{"token":"yes","logprob":-0.2}]}}],' \
                  b'"usage":{"prompt_tokens":9,"completion_tokens":1,"total_tokens":10},"extra":"original"}'
        replies = {0:(200, success), 1:(200, success), 2:(429,b'{"error":{"code":"limit","detail":7}}'),
                   3:(503,b'\xfforiginal error body')}
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                calls.append(body)
                index = json.loads(body)['fixture_index']
                if index == 0:
                    if not release.wait(10): return
                status, response = replies[index]
                self.send_response(status)
                self.send_header('Content-Length', str(len(response)))
                self.send_header('x-request-id', f'fixture-{index}')
                self.send_header('x-duplicate', 'one')
                self.send_header('x-duplicate', 'two')
                self.end_headers()
                self.wfile.write(response)
        http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server_thread = threading.Thread(target=http.serve_forever, daemon=True)
        server_thread.start()
        execution = None
        started_ray = False
        with tempfile.TemporaryDirectory(prefix='native-ray-') as root:
            try:
                # The test owns this CPU-only cluster; it never joins a user cluster.
                self.assertFalse(ray.is_initialized())
                ray.init(num_cpus=2, num_gpus=0, include_dashboard=False, _temp_dir=root)
                started_ray = True
                physical = RayMapConfig(ray.get_runtime_context().gcs_address, 1, 4,
                                        2 * MAX_FRAME_BYTES, 4 * MAX_FRAME_BYTES, response_mode='full')
                config = FixedModelConfig(f'http://localhost:{http.server_port}/fixture', 'fixture', 12000)
                def prepared(index, sequence):
                    payload = json.dumps(dict(model='fixture', messages=[dict(role='user',content='original')],
                        temperature=0, max_tokens=7, logprobs=True, top_logprobs=3, stream=False,
                        fixture_index=index), separators=(',', ':')).encode()
                    return prepare_native_task(payload, sequence, row_sequence=index + 50, call_id=f'call-{index}')
                def poll(flow):
                    deadline = time.monotonic() + 20
                    while time.monotonic() < deadline:
                        progress = flow.advance(1)
                        self.assertNotEqual(progress.state, State.FAILED, progress.error)
                        if progress.deliveries: return progress.deliveries[0]
                        flow.wait(progress)
                    self.fail('real fixture failed to complete before its deadline')
                execution = build_native_execution(config, physical=physical, max_tasks=4, observer=events.append)
                flow = NativeTaskSession(execution, 'native-query', 'map')
                requests = (prepared(0,0), prepared(1,1))
                self.assertEqual(flow.offer(requests).accepted_prefix_count, 2)
                self.assertEqual(calls, [])
                fast = poll(flow)
                self.assertEqual((fast.key.sequence, fast.info.row_sequence), (1, 51))
                raw = decode_full_response(fast.result)
                self.assertEqual(raw.body, success)
                self.assertEqual(raw.status_code, 200)
                self.assertEqual([v for k,v in raw.headers if k == 'x-duplicate'], ['one','two'])
                self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
                flow.release((fast.lease_id,))
                release.set()
                slow = poll(flow)
                self.assertEqual(slow.key.sequence, 0)
                self.assertEqual(decode_full_response(slow.result).body, success)
                flow.release((slow.lease_id,));flow.end_input()
                self.assertEqual(flow.advance(1).state, State.FINISHED)
                flow.close(clean=True)
                self.assertEqual(execution.engine.capacity.usage(), Usage())
                self.assertTrue(execution.close())
                execution = None
                self.assertEqual(sorted(calls), sorted(request.task.payload for request in requests))
                self.assertTrue(any(e['event']=='ray_block_put' for e in events))
                self.assertTrue(any(e['event']=='ray_http_completed' for e in events))
                self.assertTrue(any(e['event']=='ray_transport_closed' and e['confirmed'] for e in events))
                # The service is created and borrowed in the same explicit full-response mode.
                physical = replace(physical, worker_pool='native-fixture-pool')
                with owned_map_worker_pool(ray, config, 4, physical):
                    execution = build_native_execution(config, physical=physical, max_tasks=4)
                    flow = NativeTaskSession(execution, 'error-query', 'map')
                    errors = (prepared(2,0), prepared(3,1))
                    self.assertEqual(flow.offer(errors).accepted_prefix_count, 2)
                    received = {}
                    for _ in errors:
                        delivery = poll(flow)
                        received[delivery.info.row_sequence - 50] = decode_full_response(delivery.result)
                        flow.release((delivery.lease_id,))
                    self.assertEqual({index:(raw.status_code, raw.body) for index,raw in received.items()},
                                     {i:replies[i] for i in (2,3)})
                    flow.end_input();self.assertEqual(flow.advance(1).state, State.FINISHED)
                    flow.close(clean=True)
                    self.assertEqual(execution.engine.capacity.usage(), Usage())
                    self.assertTrue(execution.close());execution = None
                self.assertEqual(len(calls), 4)
                self.assertEqual(sorted(calls[2:]), sorted(request.task.payload for request in errors))
            finally:
                release.set()
                if execution is not None: execution.close()
                if started_ray: ray.shutdown()
                http.shutdown();http.server_close();server_thread.join(3)
