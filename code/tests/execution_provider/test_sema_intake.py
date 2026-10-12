"""Real aiohttp ingress checks with finite synthetic calls and no vendor runtime."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest import mock
from urllib import error, request

from src.execution_provider.adapters.sema_service import SemaRequestService, SemaServiceLimits, _Reply
from src.execution_provider.adapters.sema_native import SemaNativeService
from src.execution_provider.adapters.sema_semloom import SemaSemLoomService
from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.scheduling.core.session_contract import Usage

from test_sema_native_service import _Executor


class _EchoService(SemaRequestService):
    _uses_upstream_client = False

    async def _forward(self, body, headers, row, client):
        if getattr(self, 'delay', 0):
            await asyncio.sleep(self.delay)
        return _Reply(200, body, ())


class SemaIntakeTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='sema-intake-test-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def wait(self, condition, timeout=1):
        deadline = time.monotonic() + timeout
        while not condition() and time.monotonic() < deadline:
            time.sleep(0.002)
        self.assertTrue(condition(), 'HTTP ingress did not reach the expected state')

    def service(self, limits):
        return _EchoService(query_id='intake', upstream_url='http://localhost/model',
                            trace_path=self.root / 'requests.jsonl', limits=limits)

    def connect(self, service, length, initial=b'x'):
        peer = socket.create_connection(('127.0.0.1', service._port), timeout=2)
        peer.sendall(('POST /sema/intake/v1/chat/completions HTTP/1.1\r\n'
                      'Host: localhost\r\nConnection: close\r\nContent-Length: '
                      + str(length) + '\r\n\r\n').encode() + initial)
        return peer

    def post(self, endpoint, body):
        try:
            with request.urlopen(request.Request(endpoint, data=body), timeout=4) as response:
                return response.status, response.read()
        except error.HTTPError as response:
            return response.status, response.read()

    def drained(self, service):
        self.wait(lambda: service.summary['intake_requests'] == 0)
        self.assertEqual(service.summary['intake_bytes'], 0)
        self.assertEqual(service.summary['intake_waiters'], 0)
        self.assertEqual(service.summary['body_readers'], 0)

    def native(self, limits, executor, **options):
        return SemaNativeService(query_id='intake', upstream_url='http://localhost/model',
            limits=limits, trace_path=self.root / 'requests.jsonl', backend='daft',
            held_request_capacity=1, executor=executor, **options)

    def test_body_read_expires_at_the_request_deadline(self):
        with self.service(SemaServiceLimits(8, 4096, 4096, 0.15)) as service:
            peer = self.connect(service, 4000)
            try:
                self.wait(lambda: service.summary['received_requests'] == 1)
                time.sleep(0.35)
                snapshot = service.summary
            finally:
                peer.close()
            self.wait(lambda: service.summary['http_handlers_remaining'] == 0)
        self.assertEqual(snapshot['http_handlers_remaining'], 0,
                         'slow body still owns a handler after the declared request deadline')

    def test_count_limits_bodies_waiting_for_native_admission(self):
        limits = SemaServiceLimits(16, 2048, 4096, 2, intake_requests=3, intake_bytes=8192)
        gate, bodies = threading.Event(), [bytes([65 + n]) * 1500 for n in range(6)]
        executor = _Executor(gate=gate)
        with self.native(limits, executor) as service, ThreadPoolExecutor(max_workers=6) as pool:
            first = pool.submit(self.post, service.endpoint_url, bodies[0])
            self.assertTrue(executor.started.wait(1))
            rest = [pool.submit(self.post, service.endpoint_url, body) for body in bodies[1:]]
            try:
                self.wait(lambda: service.summary['intake_waiters'] == 3
                    and sum(row['body_read_ns'] is not None for row in service._rows) == 3)
                self.assertEqual(service.summary['intake_requests'], 3)
                self.assertEqual(service.summary['intake_bytes'], 4500)
                self.assertEqual(sum(row['body_read_ns'] is not None for row in service._rows), 3)
                self.assertEqual(service.summary['held_requests'], 1)
            finally:
                gate.set()
            self.assertEqual([first.result()] + [future.result() for future in rest],
                             [(200, body) for body in bodies])
            self.drained(service)
            service.end_input()
        self.assertEqual(service.summary['peak_intake_requests'], 3)
        self.assertLessEqual(service.summary['peak_intake_bytes'], limits.intake_bytes)
        self.assertEqual({call.payload for batch in executor.batches for call in batch}, set(bodies))

    def test_total_bytes_limit_concurrent_large_bodies_before_reading(self):
        limits = SemaServiceLimits(16, 2048, 4096, 2, intake_requests=16, intake_bytes=5000)
        gate, executor = threading.Event(), _Executor()
        executor.gate = gate
        bodies = [bytes([65 + n]) * 1500 for n in range(5)]
        with self.native(limits, executor) as service, ThreadPoolExecutor(max_workers=5) as pool:
            first = pool.submit(self.post, service.endpoint_url, bodies[0])
            self.assertTrue(executor.started.wait(1))
            rest = [pool.submit(self.post, service.endpoint_url, body) for body in bodies[1:]]
            try:
                self.wait(lambda: service.summary['intake_waiters'] == 3
                    and sum(row['body_read_ns'] is not None for row in service._rows) == 2)
                self.assertEqual(service.summary['intake_requests'], 2)
                self.assertEqual(service.summary['intake_bytes'], 3000)
                self.assertEqual(sum(row['body_read_ns'] is not None for row in service._rows), 2)
            finally:
                gate.set()
            self.assertEqual([first.result()] + [future.result() for future in rest],
                             [(200, body) for body in bodies])
            self.drained(service)
            service.end_input()
        self.assertLessEqual(service.summary['peak_intake_bytes'], 5000)

    def test_mib_bodies_wait_before_reading_when_total_allowance_is_full(self):
        mib, gate = 1024 * 1024, threading.Event()
        bodies = [bytes([65 + n]) * (2 * mib) for n in range(5)]
        executor = _Executor(gate=gate, response=lambda call: FullModelResponse(200, (), b'ok'))
        limits = SemaServiceLimits(16, 3 * mib, 4096, 3, intake_requests=16, intake_bytes=7 * mib)
        with self.native(limits, executor) as service, ThreadPoolExecutor(max_workers=5) as pool:
            first = pool.submit(self.post, service.endpoint_url, bodies[0])
            self.assertTrue(executor.started.wait(1))
            rest = [pool.submit(self.post, service.endpoint_url, body) for body in bodies[1:]]
            try:
                self.wait(lambda: service.summary['intake_waiters'] == 3
                    and sum(row['body_read_ns'] is not None for row in service._rows) == 2)
                self.assertEqual(service.summary['intake_requests'], 2)
                self.assertEqual(service.summary['intake_bytes'], 4 * mib)
                self.assertEqual(sum(row['body_read_ns'] is not None for row in service._rows), 2)
            finally:
                gate.set()
            self.assertEqual([first.result()] + [future.result() for future in rest], [(200, b'ok')] * 5)
            self.drained(service)
            service.end_input()
        self.assertLessEqual(service.summary['peak_intake_bytes'], 7 * mib)
        self.assertEqual({call.payload for batch in executor.batches for call in batch}, set(bodies))

    def test_unknown_length_reserves_the_maximum_before_first_read(self):
        limits = SemaServiceLimits(8, 1024, 4096, 1, intake_bytes=2049)
        with self.service(limits) as service:
            peers = [socket.create_connection(('127.0.0.1', service._port), timeout=2) for _ in range(2)]
            try:
                header = (b'POST /sema/intake/v1/chat/completions HTTP/1.1\r\nHost: localhost\r\n'
                          b'Connection: close\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n')
                peers[0].sendall(header)
                self.wait(lambda: service.summary['body_readers'] == 1)
                self.assertEqual(service.summary['intake_bytes'], 2049)
                peers[1].sendall(header)
                self.wait(lambda: service.summary['intake_waiters'] == 1)
                self.assertTrue(all(row['body_read_ns'] is None for row in service._rows))
                peers[0].sendall(b'0\r\n\r\n')
                self.assertIn(b'200 OK', peers[0].recv(4096))
                self.wait(lambda: service.summary['body_readers'] == 1 and service._rows[0]['body_read_ns'] is not None)
                peers[1].sendall(b'0\r\n\r\n')
                self.assertIn(b'200 OK', peers[1].recv(4096))
                self.drained(service)
                service.end_input()
            finally:
                for peer in peers:
                    peer.close()
        self.assertEqual(service.summary['peak_intake_bytes'], 2049)

    def test_chunked_payload_preserves_every_byte_at_the_limit(self):
        body = b' { "message" : "' + b'x' * 991 + b'" }\n'
        limits = SemaServiceLimits(4, len(body), 4096, 1, intake_bytes=2 * len(body) + 1)
        with self.service(limits) as service:
            connection = http.client.HTTPConnection('127.0.0.1', service._port, timeout=2)
            try:
                connection.request('POST', '/sema/intake/v1/chat/completions',
                    body=iter((body[:17], body[17:900], body[900:])), encode_chunked=True)
                response = connection.getresponse()
                self.assertEqual((response.status, response.read()), (200, body))
            finally:
                connection.close()
            self.drained(service)
            service.end_input()

    def test_known_oversize_is_rejected_without_reading(self):
        with self.service(SemaServiceLimits(8, 1024, 4096, 1)) as service:
            peer = self.connect(service, 1025, b'')
            try:
                self.assertIn(b'413', peer.recv(4096))
            finally:
                peer.close()
            self.assertEqual(service.summary['peak_intake_bytes'], 0)
            self.assertIsNone(service._rows[0]['body_read_ns'])
            self.assertEqual(service.first_error.phase, 'request_body_limit')
            self.drained(service)

    def test_chunked_oversize_is_rejected_incrementally(self):
        limits = SemaServiceLimits(8, 1024, 4096, 1, intake_bytes=2049)
        with self.service(limits) as service:
            connection = http.client.HTTPConnection('127.0.0.1', service._port, timeout=2)
            try:
                connection.request('POST', '/sema/intake/v1/chat/completions',
                    body=iter((b'x' * 1024, b'y' * 100)), encode_chunked=True)
                response = connection.getresponse()
                self.assertEqual(response.status, 413)
                response.read()
            finally:
                connection.close()
            self.assertIsNone(service._rows[0]['body_read_ns'])
            self.assertEqual(service.summary['peak_intake_bytes'], 2049)
            self.drained(service)

    def test_body_that_cannot_fit_total_reservation_is_rejected(self):
        limits = SemaServiceLimits(8, 1024, 4096, 1, intake_bytes=1024)
        with self.service(limits) as service:
            self.assertEqual(self.post(service.endpoint_url, b'x' * 600)[0], 413)
            self.assertEqual(service.summary['peak_intake_requests'], 0)
            self.drained(service)

    def test_deadline_is_shared_by_body_read_and_forward(self):
        limits = SemaServiceLimits(8, 1024, 4096, 0.22)
        with self.service(limits) as service:
            service.delay = 0.15
            started = time.monotonic()
            peer = self.connect(service, 2)
            try:
                self.wait(lambda: service.summary['body_readers'] == 1)
                time.sleep(0.12)
                peer.sendall(b'y')
                self.assertEqual(peer.recv(4096), b'')
                self.assertLess(time.monotonic() - started, 0.30)
            finally:
                peer.close()
            self.assertEqual(service._rows[0]['error_type'], 'TimeoutError')
            self.assertEqual(service.first_error.status, 502)
            self.drained(service)

    def test_deadline_includes_response_write_and_releases_owners(self):
        from aiohttp import web
        entered, gate = threading.Event(), threading.Event()
        original = web.Response.write_eof

        async def delayed(response, *args, **kwargs):
            if response.status == 200:
                entered.set()
                while not gate.is_set():
                    await asyncio.sleep(0.002)
            return await original(response, *args, **kwargs)

        with mock.patch.object(web.Response, 'write_eof', delayed):
            with self.service(SemaServiceLimits(8, 1024, 4096, 0.15)) as service:
                peer = self.connect(service, 2, b'xy')
                try:
                    self.assertTrue(entered.wait(1))
                    self.assertEqual(service.summary['intake_bytes'], 2)
                    self.wait(lambda: service.summary['http_handlers_remaining'] == 0)
                    self.assertEqual(service.first_error.phase, 'response_write')
                    self.assertIsNone(service._rows[0]['response_written_ns'])
                    self.drained(service)
                finally:
                    gate.set()
                    peer.close()

    def test_cancel_stops_partial_body_and_unread_waiters(self):
        limits = SemaServiceLimits(8, 1024, 4096, 1, intake_requests=1)
        with self.service(limits) as service:
            peers = [self.connect(service, 1000) for _ in range(3)]
            try:
                self.wait(lambda: service.summary['intake_waiters'] == 2)
                self.assertEqual(service.summary['body_readers'], 1)
                service.cancel()
                self.wait(lambda: service.summary['http_handlers_remaining'] == 0)
                self.drained(service)
            finally:
                for peer in peers:
                    peer.close()

    def test_client_disconnect_during_read_releases_reservation(self):
        with self.service(SemaServiceLimits(8, 1024, 4096, 1)) as service:
            peer = self.connect(service, 1000)
            self.wait(lambda: service.summary['body_readers'] == 1)
            peer.close()
            self.wait(lambda: service.summary['http_handlers_remaining'] == 0)
            self.assertTrue(service._halted.is_set())
            self.assertIn(service._rows[0]['error_type'], ('CancelledError', 'ConnectionResetError'))
            self.drained(service)

    def test_native_body_stays_charged_after_reply_until_graph_consumer_stops(self):
        from src.baselines.text.frameworks.native_calls import NativeCallOutcome
        yielded, gate = threading.Event(), threading.Event()

        class Executor:
            backend = 'daft'
            last_close_report = None

            def iter_batch(owner, calls, **options):
                try:
                    for call in calls:
                        yield NativeCallOutcome(call.call_id, call.row_id, FullModelResponse(200, (), b'ok'))
                    yielded.set()
                    self.assertTrue(gate.wait(1))
                finally:
                    owner.last_close_report = dict(graph_consumer_stopped=True, clean=True)

        with self.native(SemaServiceLimits(8, 1024, 4096, 2), Executor()) as service:
            try:
                self.assertEqual(self.post(service.endpoint_url, b'body'), (200, b'ok'))
                self.assertTrue(yielded.wait(1))
                self.assertEqual(service.summary['held_requests'], 0)
                self.assertEqual(service.summary['intake_bytes'], 4)
                self.assertEqual(service.summary['intake_requests'], 1)
            finally:
                gate.set()
            self.drained(service)
            service.end_input()

    def test_unconfirmed_native_consumer_retains_intake_reservation(self):
        class Executor(_Executor):
            def iter_batch(owner, calls, **options):
                yield from super().iter_batch(calls, **options)
                owner.last_close_report = dict(graph_consumer_stopped=False, clean=False)

        with self.native(SemaServiceLimits(8, 1024, 4096, 1), Executor()) as service:
            self.post(service.endpoint_url, b'body')
            self.wait(lambda: service.summary['native_driver_stopped'])
            self.wait(lambda: service.summary['http_handlers_remaining'] == 0)
            self.assertEqual(service.summary['intake_requests'], 1)
            self.assertEqual(service.summary['intake_bytes'], 4)
        self.assertIn('executor_close', [failure['stage'] for failure in service.summary['cleanup_failures']])
        self.assertEqual(service.summary['intake_bytes'], 4)

    def test_all_native_routes_preserve_complete_request_and_response(self):
        for backend in ('daft', 'ray-data', 'imlane'):
            with self.subTest(backend=backend):
                executor = _Executor()
                executor.backend = backend
                with SemaNativeService(query_id='intake', upstream_url='http://localhost/model',
                        limits=SemaServiceLimits(8, 1024, 4096, 1),
                        trace_path=self.root / (backend + '.jsonl'), backend=backend, executor=executor) as service:
                    body = b' { "original" : [1, 2] }\n'
                    self.assertEqual(self.post(service.endpoint_url, body), (200, body))
                    self.drained(service)
                    service.end_input()
                self.assertEqual(executor.batches[0][0].payload, body)
                self.assertEqual(service.cleanup_errors, [])

    def test_semloom_waiting_bodies_keep_intake_charge_through_core_delivery(self):
        gate, started, sent = threading.Event(), threading.Event(), []

        def build(owner):
            async def execute(task, endpoint):
                sent.append(task.task.payload)
                started.set()
                while not gate.is_set():
                    await asyncio.sleep(0.002)
                return encode_full_response(FullModelResponse(200, (), task.task.payload))

            return build_native_execution(owner.model_config, physical=None, execute=execute,
                max_tasks=1, max_active_requests=1, observer=owner._observe)

        limits = SemaServiceLimits(16, 1024, 4096, 2, intake_requests=3, intake_bytes=8192)
        bodies = [json.dumps({'model': 'fixture', 'messages': [], 'occurrence': n}).encode() for n in range(5)]
        with mock.patch.object(SemaSemLoomService, '_build_execution', build):
            with SemaSemLoomService(query_id='intake', model_config=FixedModelConfig('http://localhost/model', 'fixture', 1000),
                    physical=None, local_diagnostic=True, limits=limits, trace_path=self.root / 'requests.jsonl',
                    max_held_tasks=1, max_active_requests=1) as service, ThreadPoolExecutor(max_workers=5) as pool:
                first = pool.submit(self.post, service.endpoint_url, bodies[0])
                self.assertTrue(started.wait(1))
                rest = [pool.submit(self.post, service.endpoint_url, body) for body in bodies[1:]]
                try:
                    self.wait(lambda: service.summary['intake_waiters'] == 2
                        and sum(row['body_read_ns'] is not None for row in service._rows) == 3)
                    self.assertEqual(service.summary['intake_requests'], 3)
                    self.assertEqual(service.summary['intake_bytes'], sum(len(body) for body in bodies[:3]))
                    self.assertEqual(service._execution.engine.capacity.usage().held_tasks, 1)
                    self.assertEqual(sum('core_accepted_ns' in row for row in service._rows), 1)
                finally:
                    gate.set()
                self.assertEqual([first.result()] + [future.result() for future in rest],
                                 [(200, body) for body in bodies])
                self.drained(service)
                service.end_input()
        self.assertEqual(set(sent), set(bodies))
        self.assertEqual(service._execution.engine.capacity.usage(), Usage())
        self.assertEqual(service.cleanup_errors, [])

    def test_optional_configuration_keeps_four_argument_calls_and_validates_new_limits(self):
        limits = SemaServiceLimits(8, 1024, 1024, 1)
        self.assertEqual((limits.intake_requests, limits.intake_bytes), (128, 32 * 1024 * 1024))
        for name in ('intake_requests', 'intake_bytes'):
            for value in (0, -1, True, 1.5):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    SemaServiceLimits(8, 1024, 1024, 1, **{name: value})


if __name__ == '__main__':
    unittest.main()
