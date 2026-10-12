"""Subprocess/localhost fixtures for the bridge protocol, without author IMLane or a model."""
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from urllib import error, request
from unittest import mock

from src.baselines.text.frameworks.native_calls import NativeCall
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.sema_imlane import (
    AUTHOR_COMMIT, SemaIMLaneExecutor, open_sema_imlane_service,
)
from src.execution_provider.adapters.sema_service import SemaServiceError, SemaServiceLimits


class _HTTP(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        self.server.calls.append(body)
        self.server.started.set()
        self.server.gate.wait(2)
        raw = self.server.body
        self.send_response(self.server.status)
        for key, value in self.server.headers:
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *_args):
        pass


class SemaIMLaneBridgeTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='sema-imlane-fixture-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), _HTTP)
        self.server.calls = []
        self.server.status, self.server.body = 200, b' original \x00 complete response\n'
        self.server.headers = [('Content-Type', 'application/json'), ('X-Repeat', 'one'), ('X-Repeat', 'two')]
        self.server.started, self.server.gate = threading.Event(), threading.Event()
        self.server.gate.set()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_http)
        self.code_root = Path(__file__).resolve().parents[2]
        self.raw = b' {"model":"fixture", "messages":[{"role":"user","content":"unchanged"}]} '

    def stop_http(self):
        self.server.gate.set()
        self.server.shutdown(); self.server.server_close(); self.thread.join(3)

    def call(self, sequence=0, endpoint=None):
        return NativeCall('occurrence-' + str(sequence), sequence, self.raw,
            endpoint or ('http://127.0.0.1:' + str(self.server.server_port) + '/complete'),
            (('Content-Type', 'application/json'),), 2, max_response_bytes=65536)

    def executor(self, *, mutation='', pause=False, metadata=True, late_failure=False):
        # This executable is a protocol fixture, never a substitute for IMLane.
        script = self.root / ('protocol-' + str(time.monotonic_ns()))
        script.write_text('#!' + sys.executable + '\n' +
            'import asyncio,json,sys,time\n' +
            'sys.path.insert(0,' + repr(str(self.code_root)) + ')\n' +
            'from src.baselines.text.frameworks.native_calls import _call_from_wire,_request,_outcome_wire\n' +
            'calls=[_call_from_wire(line) for line in sys.stdin]\n' +
            ('print(\'SEMA_IMLANE_META {"native_executors":2,"native_task_threads":4}\',flush=True)\n'
             if metadata else '') +
            'for call in reversed(calls):\n' +
            ' outcome=asyncio.run(_request(call))\n' +
            ' wire=json.loads(_outcome_wire(outcome))\n' +
            (' ' + mutation + '\n' if mutation else '') +
            ' print("SEMA_IMLANE_RESULT "+json.dumps(wire),flush=True)\n' +
            (' time.sleep(3)\n' if pause else '') +
            ('time.sleep(.15)\nsys.exit(7)\n' if late_failure else ''))
        script.chmod(0o700)
        udf = self.code_root / 'integrations/sema/imlane_http_udf.py'
        config = dict(author_commit=AUTHOR_COMMIT, bridge_binary=str(script),
            bridge_sha256=hashlib.sha256(script.read_bytes()).hexdigest(),
            gateway_port=self.server.server_port, udf_path=str(udf),
            udf_sha256=hashlib.sha256(udf.read_bytes()).hexdigest(), work_root=str(self.root / 'batches'),
            namespace='owned-fixture', native_task_threads=4)
        path = self.root / (script.name + '.json')
        path.write_text(json.dumps(config))
        executor = SemaIMLaneExecutor(path, NativeGraphOptions(concurrency=4))
        self.addCleanup(executor.close)
        return executor

    def test_equal_raw_requests_and_out_of_order_complete_errors_preserve_occurrences(self):
        self.server.status = 429
        self.server.body = gzip.compress(self.server.body)
        self.server.headers.append(('Content-Encoding', 'gzip'))
        executor = self.executor()
        values = list(executor.iter_batch((self.call(0), self.call(1))))
        self.assertEqual([v.row_id for v in values], [1, 0])
        self.assertEqual(self.server.calls, [self.raw, self.raw])
        for value in values:
            self.assertEqual((value.response.status_code, value.response.body), (429, self.server.body))
            self.assertEqual([v for k, v in value.response.headers if k == 'X-Repeat'], ['one', 'two'])
        self.assertTrue(executor.last_close_report['clean'])
        self.assertTrue(executor.last_close_report['bridge_stopped'])
        self.assertEqual(executor.last_close_report['native_settings']['native_task_threads'], 4)
        self.assertEqual(executor.last_close_report['remote_http_stopped'], 'unavailable')

    def test_transport_failure_has_no_synthetic_model_status_or_retry(self):
        executor = self.executor()
        value, = executor.iter_batch((self.call(endpoint='http://127.0.0.1:1/closed'),))
        self.assertIsNone(value.response)
        self.assertIn('ClientConnectorError', value.transport_error)
        self.assertEqual(self.server.calls, [])
        self.assertEqual(executor.last_close_report['received'], 1)

    def test_identity_mismatch_stops_child_and_preserves_failed_evidence(self):
        executor = self.executor(mutation='wire["row_id"]=999')
        with self.assertRaisesRegex(ValueError, 'occurrence identity'):
            list(executor.iter_batch((self.call(),)))
        self.assertFalse(executor.last_close_report['clean'])
        self.assertTrue(executor.last_close_report['bridge_stopped'])
        self.assertEqual(len(list((self.root / 'batches').glob('*/close.json'))), 1)
        self.assertEqual(len(self.server.calls), 1)
        with self.assertRaisesRegex(RuntimeError, 'uncertain previous batch'):
            list(executor.iter_batch((self.call(1),)))
        self.assertEqual(len(self.server.calls), 1)

    def test_missing_native_settings_rejects_unverified_execution(self):
        executor = self.executor(metadata=False)
        with self.assertRaisesRegex(ValueError, 'preceded native settings'):
            list(executor.iter_batch((self.call(),)))
        self.assertFalse(executor.last_close_report['clean'])

    def test_failure_after_final_response_is_not_a_completed_batch(self):
        executor = self.executor(late_failure=True)
        iterator = executor.iter_batch((self.call(),))
        self.assertEqual(next(iterator).response.status_code, 200)
        with self.assertRaisesRegex(RuntimeError, 'did not complete'):
            next(iterator)
        self.assertEqual(executor.last_close_report['returncode'], 7)
        self.assertFalse(executor.last_close_report['clean'])
        self.assertTrue(executor.poisoned)

    def test_cancel_during_sent_http_stops_only_child_and_keeps_unknown_remote_state(self):
        executor = self.executor()
        self.server.gate.clear()
        cancelled = threading.Event()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(lambda: list(executor.iter_batch((self.call(),), cancelled=cancelled.is_set)))
            started = self.server.started.wait(4)
            if not started and future.done():
                future.result()
            self.assertTrue(started)
            cancelled.set()
            with self.assertRaises(InterruptedError):
                future.result(timeout=2)
        self.server.gate.set()
        self.assertEqual(len(self.server.calls), 1)
        self.assertEqual(executor.last_close_report['received'], 0)
        self.assertTrue(executor.last_close_report['bridge_stopped'])
        self.assertEqual(executor.last_close_report['remote_http_stopped'], 'unavailable')

    def test_partial_consumer_close_stops_remaining_work_without_invented_completion(self):
        executor = self.executor(pause=True)
        iterator = executor.iter_batch((self.call(0), self.call(1)))
        self.assertEqual(next(iterator).row_id, 1)
        iterator.close()
        self.assertFalse(executor.last_close_report['clean'])
        self.assertTrue(executor.last_close_report['bridge_stopped'])
        self.assertEqual(executor.last_close_report['received'], 1)
        self.assertEqual(len(self.server.calls), 1)

    def test_deadline_covers_bridge_wait_after_response_and_keeps_first_failure(self):
        executor = self.executor(pause=True)
        with mock.patch('src.execution_provider.adapters.sema_imlane.write_private_json',
                        side_effect=OSError('fixture record failure')):
            with self.assertRaises(TimeoutError) as caught:
                list(executor.iter_batch((self.call(),), timeout_s=.6))
        self.assertIn('OSError', '\n'.join(caught.exception.__notes__))
        self.assertFalse(executor.last_close_report['clean'])
        self.assertTrue(executor.last_close_report['bridge_stopped'])
        self.assertEqual(executor.last_close_report['received'], 1)

    def test_changed_binary_and_undeclared_native_capacity_reject_before_http(self):
        executor = self.executor()
        Path(executor.config['bridge_binary']).write_text('changed executable')
        with self.assertRaisesRegex(ValueError, 'recorded identity'):
            executor.start()
        config = dict(executor.config, native_task_threads=5)
        path = self.root / 'bad-native-capacity.json'; path.write_text(json.dumps(config))
        with self.assertRaisesRegex(ValueError, 'request allowance'):
            SemaIMLaneExecutor(path, NativeGraphOptions(concurrency=4))
        config = dict(executor.config, env={'BASH_ENV': '/untrusted'})
        path.write_text(json.dumps(config))
        with self.assertRaisesRegex(ValueError, 'unsupported entries'):
            SemaIMLaneExecutor(path)
        self.assertEqual(self.server.calls, [])

    def test_service_refuses_direct_or_arithmetic_fallback(self):
        for executor in (None, object(), type('Arithmetic', (), {'backend': 'fixture'})()):
            with self.assertRaisesRegex(ValueError, 'complete-call IMLane executor'):
                open_sema_imlane_service('http://localhost/model', executor=executor)

    def service(self, executor):
        return open_sema_imlane_service('http://127.0.0.1:' + str(self.server.server_port) + '/complete',
            executor=executor, query_id='fixture', limits=SemaServiceLimits(8, 65536, 65536, 5),
            trace_path=self.root / 'requests.jsonl', held_request_capacity=4)

    def post(self, endpoint):
        try:
            with request.urlopen(request.Request(endpoint, data=self.raw,
                    headers={'Content-Type': 'application/json'}), timeout=7) as response:
                return response.status, response.read(), tuple(response.headers.items())
        except error.HTTPError as response:
            return response.status, response.read(), tuple(response.headers.items())

    def test_sema_service_sequential_supply_retains_full_native_responses(self):
        executor = self.executor()
        with self.service(executor) as service:
            for _ in range(2):
                status, body, headers = self.post(service.endpoint_url)
                self.assertEqual((status, body), (200, self.server.body))
                self.assertEqual([v for k, v in headers if k == 'X-Repeat'], ['one', 'two'])
            service.end_input()
        self.assertEqual(self.server.calls, [self.raw, self.raw])
        self.assertEqual(service.summary['native_backend'], 'imlane')
        self.assertEqual(service.summary['snapshot_count'], 2)
        self.assertEqual(service.summary['upstream_concurrency'], 4)
        self.assertTrue(service.summary['native_settings_observed'])
        self.assertEqual(service.summary['uncertain_requests'], 0)
        self.assertEqual(service.summary['held_requests'], 0)
        self.assertTrue(service.summary['io_loop_closed'])

    def test_sema_service_http_error_keeps_original_failure_and_stops_later_intake(self):
        self.server.status = 422
        self.server.body = b' {"error":"unchanged native HTTP bytes"}\n'
        executor = self.executor()
        cancelled = []
        with self.service(executor) as service:
            service.bind_native_cancel(lambda: cancelled.append(True))
            self.assertEqual(self.post(service.endpoint_url)[:2], (422, self.server.body))
            first = service.first_error
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
            self.assertIs(service.first_error, first)
        self.assertEqual((first.status, first.body, first.phase), (422, self.server.body, 'http_response'))
        self.assertTrue(cancelled)
        self.assertEqual(len(self.server.calls), 1)
        self.assertEqual(service.summary['held_requests'], 0)
        self.assertTrue(service.summary['last_close_report']['bridge_stopped'])

    def test_sema_normal_input_end_preserves_failure_after_final_response(self):
        executor = self.executor(late_failure=True)
        with self.assertRaises(SemaServiceError) as caught:
            with self.service(executor) as service:
                self.assertEqual(self.post(service.endpoint_url)[0], 200)
                service.end_input()
        self.assertEqual(service.first_error.phase, 'native_graph')
        self.assertEqual(service.summary['last_close_report']['returncode'], 7)
        self.assertFalse(service.summary['last_close_report']['clean'])
        self.assertTrue(executor.poisoned)
        self.assertIs(caught.exception, service.first_error)


if __name__ == '__main__':
    unittest.main()
