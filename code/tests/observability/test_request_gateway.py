from __future__ import annotations

import asyncio
import json
import socket
import struct
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib import error, request
from urllib.parse import urlsplit
from unittest.mock import patch

from src.observability.request_gateway import (
    GatewayRoute,
    ObservationGateway,
)


class _UpstreamHandler(BaseHTTPRequestHandler):
    calls: list[tuple[str, bytes]] = []
    response_status = 200
    response_gate: threading.Event | None = None
    headers_sent: threading.Event | None = None
    two_requests_received: threading.Event | None = None
    response_padding_bytes = 0
    calls_lock = threading.Lock()

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        body = self.rfile.read(int(self.headers["Content-Length"]))
        with type(self).calls_lock:
            type(self).calls.append((self.path, body))
            if len(type(self).calls) >= 2 and type(self).two_requests_received:
                type(self).two_requests_received.set()
        payload = json.dumps(
            {
                "choices": [
                    {
                        "message": {"content": "ok"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 7,
                    "total_tokens": 18,
                },
                "padding": "x" * type(self).response_padding_bytes,
            }
        ).encode("utf-8")
        self.send_response(type(self).response_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if type(self).headers_sent is not None:
            type(self).headers_sent.set()
        if type(self).response_gate is not None:
            type(self).response_gate.wait(timeout=5)
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, _format: str, *_args: object) -> None:
        return


class ObservationGatewayTest(unittest.TestCase):
    def setUp(self) -> None:
        _UpstreamHandler.calls = []
        _UpstreamHandler.response_status = 200
        _UpstreamHandler.response_gate = None
        _UpstreamHandler.headers_sent = None
        _UpstreamHandler.two_requests_received = None
        _UpstreamHandler.response_padding_bytes = 0
        self.upstream = ThreadingHTTPServer(
            ("127.0.0.1", 0), _UpstreamHandler
        )
        self.thread = threading.Thread(
            target=self.upstream.serve_forever, daemon=True
        )
        self.thread.start()

    def tearDown(self) -> None:
        self.upstream.shutdown()
        self.upstream.server_close()
        self.thread.join(timeout=5)

    def _upstream_url(self) -> str:
        return (
            f"http://127.0.0.1:{self.upstream.server_port}"
            "/v1/chat/completions"
        )

    def test_forwards_exact_body_once_and_records_actual_usage(self) -> None:
        with TemporaryDirectory() as directory:
            trace = Path(directory) / "gateway.jsonl"
            body = json.dumps(
                {"model": "m", "messages": [{"role": "user", "content": "p"}]}
            ).encode("utf-8")
            with ObservationGateway(
                routes=(GatewayRoute("job0", "endpoint-0", self._upstream_url()),),
                trace_path=trace,
            ) as gateway:
                endpoint = gateway.endpoint_url("job0", "endpoint-0")
                response = request.urlopen(
                    request.Request(
                        endpoint,
                        data=body,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    ),
                    timeout=5,
                )
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read())["usage"]["total_tokens"], 18)

            self.assertEqual(_UpstreamHandler.calls, [("/v1/chat/completions", body)])
            rows = [json.loads(line) for line in trace.read_text().splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["job_id"], "job0")
            self.assertEqual(rows[0]["endpoint_id"], "endpoint-0")
            self.assertEqual(rows[0]["retry_count"], 0)
            self.assertEqual(rows[0]["request_body_sha256"], rows[0]["forwarded_body_sha256"])
            self.assertEqual(rows[0]["actual_total_tokens"], 18)
            self.assertGreaterEqual(rows[0]["dispatch_delay_s"], 0.0)
            from src.experiments.saor.native_system_observation import (
                JobObservationContract, summarize_gateway_rows,
            )
            summary = summarize_gateway_rows(rows, (
                JobObservationContract("job0", "fixture", 1.0, 60.0),
            ))
            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["gateway_integrity"]["request_count"], 1)

    def test_upstream_failure_is_forwarded_without_retry(self) -> None:
        _UpstreamHandler.response_status = 503
        with TemporaryDirectory() as directory:
            trace = Path(directory) / "gateway.jsonl"
            with ObservationGateway(
                routes=(GatewayRoute("job1", "endpoint-0", self._upstream_url()),),
                trace_path=trace,
            ) as gateway:
                with self.assertRaises(error.HTTPError) as raised:
                    request.urlopen(
                        request.Request(
                            gateway.endpoint_url("job1", "endpoint-0"),
                            data=b"{}",
                            headers={"Content-Type": "application/json"},
                            method="POST",
                        ),
                        timeout=5,
                    )
                self.assertEqual(raised.exception.code, 503)

            self.assertEqual(len(_UpstreamHandler.calls), 1)
            row = json.loads(trace.read_text().strip())
            self.assertEqual(row["upstream_status"], 503)
            self.assertEqual(row["retry_count"], 0)

    def test_idle_upstream_connection_is_not_reused_between_queries(self) -> None:
        class RetiringConnectionHandler(_UpstreamHandler):
            protocol_version = 'HTTP/1.1'
            calls = []
            refused_idle_connections = 0

            def do_POST(self):
                previous = getattr(self, 'last_reply', None)
                if previous is not None and time.monotonic() - previous >= .03:
                    type(self).refused_idle_connections += 1
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                super().do_POST()
                self.last_reply = time.monotonic()

        upstream = ThreadingHTTPServer(('127.0.0.1', 0), RetiringConnectionHandler)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        bodies = [b'{"query":"first"}', b'{"query":"after-preparation"}']
        try:
            with TemporaryDirectory() as directory:
                trace = Path(directory) / 'gateway.jsonl'
                with ObservationGateway(
                    routes=(GatewayRoute('job', 'model',
                        f'http://127.0.0.1:{upstream.server_port}/v1/chat/completions'),),
                    trace_path=trace,
                ) as gateway:
                    statuses = []
                    for ordinal, body in enumerate(bodies):
                        if ordinal:
                            time.sleep(.06)
                            gateway.reset_upstream_connections()
                        try:
                            response = request.urlopen(request.Request(
                                gateway.endpoint_url('job', 'model'), data=body,
                                headers={'Content-Type': 'application/json'}), timeout=5)
                        except error.HTTPError as response:
                            statuses.append(response.code)
                            response.read()
                        else:
                            statuses.append(response.status)
                            response.read()
                rows = [json.loads(line) for line in trace.read_text().splitlines()]
            self.assertEqual(statuses, [200, 200],
                f'upstream connection failures: {[row["error_type"] for row in rows]}')
            self.assertEqual(RetiringConnectionHandler.refused_idle_connections, 0)
            self.assertEqual(RetiringConnectionHandler.calls,
                [('/v1/chat/completions', body) for body in bodies])
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row['retry_count'] == 0 and
                row['upstream_headers_send_count'] == 1 for row in rows))
            self.assertTrue(all(row['upstream_connection_policy'] == 'pooled'
                for row in rows))
            self.assertEqual([row['upstream_pool_generation'] for row in rows], [0, 1])
            self.assertEqual([row['upstream_connection_create_completed_count'] for row in rows], [1, 1])
            self.assertEqual([row['upstream_connection_reused_count'] for row in rows], [0, 0])
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=5)

    def test_query_reuses_connections_unless_fresh_diagnostic_is_requested(self) -> None:
        class KeepAliveHandler(_UpstreamHandler):
            protocol_version = 'HTTP/1.1'
            calls = []

        self.upstream.RequestHandlerClass = KeepAliveHandler
        for fresh in (False, True):
            with self.subTest(fresh=fresh), TemporaryDirectory() as directory:
                trace = Path(directory) / 'gateway.jsonl'
                with ObservationGateway(
                    routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                    trace_path=trace, fresh_upstream_connections=fresh,
                ) as gateway:
                    for _ in range(2):
                        with request.urlopen(request.Request(
                            gateway.endpoint_url('job', 'model'), data=b'{}'), timeout=5) as response:
                            response.read()
                rows = [json.loads(line) for line in trace.read_text().splitlines()]
            self.assertEqual([row['upstream_connection_create_completed_count'] for row in rows],
                [1, 1] if fresh else [1, 0])
            self.assertEqual([row['upstream_connection_reused_count'] for row in rows],
                [0, 0] if fresh else [0, 1])
            self.assertEqual([row['upstream_pool_generation'] for row in rows], [0, 0])
            for row in rows:
                self.assertEqual(row['upstream_connection_policy'], 'fresh_per_request' if fresh else 'pooled')
                self.assertEqual(row['retry_count'], 0)
                self.assertEqual(row['upstream_headers_send_count'], 1)
                self.assertLessEqual(row['upstream_dispatch_started_monotonic_ns'],
                    row['upstream_connection_reused_monotonic_ns'] or row['upstream_connection_create_started_monotonic_ns'])
                self.assertLessEqual(row['upstream_connection_reused_monotonic_ns'] or
                    row['upstream_connection_create_completed_monotonic_ns'], row['upstream_headers_send_started_monotonic_ns'])
        self.assertEqual(len(KeepAliveHandler.calls), 4)

    def test_pool_reset_rejects_an_active_request_without_replay(self) -> None:
        _UpstreamHandler.response_gate = threading.Event()
        _UpstreamHandler.headers_sent = threading.Event()
        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with ObservationGateway(
                routes=(GatewayRoute('job', 'model', self._upstream_url()),), trace_path=trace,
            ) as gateway:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(lambda: request.urlopen(request.Request(
                        gateway.endpoint_url('job', 'model'), data=b'{}'), timeout=5).read())
                    try:
                        self.assertTrue(_UpstreamHandler.headers_sent.wait(timeout=3))
                        with self.assertRaisesRegex(RuntimeError, 'during a request'):
                            gateway.reset_upstream_connections()
                    finally:
                        _UpstreamHandler.response_gate.set()
                    future.result(timeout=5)
                gateway.snapshot()
                gateway.reset_upstream_connections()
            row = json.loads(trace.read_text())
        self.assertEqual(len(_UpstreamHandler.calls), 1)
        self.assertEqual(row['upstream_pool_generation'], 0)
        self.assertEqual(row['retry_count'], 0)
        self.assertEqual(row['status'], 'completed')

    def test_upstream_disconnect_after_receipt_is_not_retried(self) -> None:
        class DisconnectingHandler(_UpstreamHandler):
            protocol_version = 'HTTP/1.1'
            calls = []

            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                type(self).calls.append((self.path, body))
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)

        self.upstream.RequestHandlerClass = DisconnectingHandler
        body = b'{"model":"fixture"}'
        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with ObservationGateway(
                routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                trace_path=trace,
            ) as gateway:
                with self.assertRaises(error.HTTPError) as raised:
                    request.urlopen(request.Request(gateway.endpoint_url('job', 'model'),
                        data=body, headers={'Content-Type': 'application/json'}), timeout=5)
                self.assertEqual(raised.exception.code, 502)
            row = json.loads(trace.read_text())
        self.assertEqual(DisconnectingHandler.calls, [('/v1/chat/completions', body)])
        self.assertEqual(row['error_type'], 'ServerDisconnectedError')
        self.assertEqual(row['retry_count'], 0)
        self.assertEqual(row['upstream_headers_send_count'], 1)
        self.assertIsNone(row['upstream_response_body_read_completed_monotonic_ns'])

    def test_budget_callback_rejects_before_upstream_send(self) -> None:
        def reject(_route, _body):
            raise RuntimeError('request budget exhausted')
        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with ObservationGateway(
                routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                trace_path=trace, before_forward=reject,
            ) as gateway:
                with self.assertRaises(error.HTTPError):
                    request.urlopen(request.Request(gateway.endpoint_url('job', 'model'),
                        data=b'{}', headers={'Content-Type': 'application/json'}), timeout=5)
            self.assertEqual(_UpstreamHandler.calls, [])
            row = json.loads(trace.read_text())
            self.assertFalse(row['forwarded'])
            self.assertIsNone(row['forwarded_body_sha256'])
            self.assertEqual(row['status'], 'failed')
            self.assertEqual(row['error_phase'], 'before_forward')
            self.assertIsNone(row['before_forward_completed_monotonic_ns'])
            self.assertIsNone(row['upstream_dispatch_started_monotonic_ns'])
            self.assertIsNone(row['upstream_response_body_read_completed_monotonic_ns'])
            self.assertIsNotNone(row['response_write_completed_monotonic_ns'])

    def test_callbacks_observe_original_request_and_response_without_replay(self) -> None:
        before, after = [], []
        body = b'{"model":"fixture"}'
        with TemporaryDirectory() as directory:
            with ObservationGateway(
                routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                trace_path=Path(directory) / 'gateway.jsonl',
                before_forward=lambda route, value: before.append((route.job_id, value)),
                after_forward=lambda route, value, response, status:
                    after.append((route.job_id, value, response, status)),
            ) as gateway:
                response = request.urlopen(request.Request(gateway.endpoint_url('job', 'model'),
                    data=body, headers={'Content-Type': 'application/json'}), timeout=5).read()
            self.assertEqual(before, [('job', body)])
            self.assertEqual(after, [('job', body, response, 200)])
            self.assertEqual(len(_UpstreamHandler.calls), 1)

    def test_monotonic_stages_separate_body_callbacks_and_upstream_read(self) -> None:
        before_called = threading.Event()
        callback_clocks = {}
        _UpstreamHandler.response_gate = threading.Event()
        _UpstreamHandler.headers_sent = threading.Event()

        def before(_route, _body):
            callback_clocks['before'] = time.monotonic_ns()
            before_called.set()

        def after(_route, _body, _response, _status):
            callback_clocks['after'] = time.monotonic_ns()

        body = b'{"model":"fixture"}'
        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with ObservationGateway(
                routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                trace_path=trace, before_forward=before, after_forward=after,
            ) as gateway:
                url = urlsplit(gateway.endpoint_url('job', 'model'))
                connection = HTTPConnection(url.hostname, url.port, timeout=5)
                try:
                    connection.putrequest('POST', url.path)
                    connection.putheader('Content-Type', 'application/json')
                    connection.putheader('Content-Length', str(len(body)))
                    connection.endheaders(body[:3])
                    self.assertFalse(before_called.is_set())
                    body_sent = time.monotonic_ns()
                    connection.send(body[3:])
                    self.assertTrue(_UpstreamHandler.headers_sent.wait(timeout=3))
                    self.assertNotIn('after', callback_clocks)
                    upstream_released = time.monotonic_ns()
                    _UpstreamHandler.response_gate.set()
                    response = connection.getresponse()
                    self.assertEqual(response.status, 200)
                    response.read()
                finally:
                    _UpstreamHandler.response_gate.set()
                    connection.close()
            row = json.loads(trace.read_text())
        fields = (
            'received_monotonic_ns', 'request_body_read_completed_monotonic_ns',
            'before_forward_started_monotonic_ns', 'before_forward_completed_monotonic_ns',
            'upstream_dispatch_started_monotonic_ns', 'upstream_headers_send_started_monotonic_ns',
            'upstream_response_body_read_completed_monotonic_ns',
            'after_forward_started_monotonic_ns', 'after_forward_completed_monotonic_ns',
            'response_ready_monotonic_ns', 'response_write_started_monotonic_ns',
            'response_write_completed_monotonic_ns', 'request_terminal_monotonic_ns',
        )
        clocks = [row[field] for field in fields]
        self.assertEqual(clocks, sorted(clocks))
        self.assertGreaterEqual(row['request_body_read_completed_monotonic_ns'], body_sent)
        self.assertLessEqual(row['before_forward_started_monotonic_ns'], callback_clocks['before'])
        self.assertGreaterEqual(row['before_forward_completed_monotonic_ns'], callback_clocks['before'])
        self.assertGreaterEqual(row['upstream_response_body_read_completed_monotonic_ns'], upstream_released)
        self.assertLessEqual(row['after_forward_started_monotonic_ns'], callback_clocks['after'])
        self.assertGreaterEqual(row['after_forward_completed_monotonic_ns'], callback_clocks['after'])
        self.assertEqual(row['timing_schema_version'], 1)
        self.assertEqual(row['timing_clock'], 'time.monotonic_ns')
        self.assertEqual(row['timing_scope'], 'proxy_handler_entry_to_response_write_eof_return')
        self.assertEqual(row['response_write_status'], 'completed')
        self.assertEqual(row['upstream_headers_send_count'], 1)

    def test_response_completion_waits_for_real_write_eof_return(self) -> None:
        from aiohttp.web_response import Response

        original_write_eof = Response.write_eof
        body_written = threading.Event()
        allow_return = threading.Event()
        observed = {}

        async def hold_write_return(response, data=b''):
            await original_write_eof(response, data)
            observed['write_return_before_hold'] = time.monotonic_ns()
            body_written.set()
            await asyncio.to_thread(allow_return.wait, 3)
            observed['write_return_after_hold'] = time.monotonic_ns()

        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with patch.object(Response, 'write_eof', hold_write_return):
                with ObservationGateway(
                    routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                    trace_path=trace,
                ) as gateway:
                    try:
                        response = request.urlopen(request.Request(
                            gateway.endpoint_url('job', 'model'), data=b'{}'), timeout=5)
                        response.read()
                        self.assertTrue(body_written.wait(timeout=3))
                    finally:
                        allow_return.set()
            row = json.loads(trace.read_text())
        self.assertLess(row['response_ready_monotonic_ns'], observed['write_return_before_hold'])
        self.assertGreaterEqual(row['response_write_completed_monotonic_ns'], observed['write_return_after_hold'])
        self.assertEqual(row['request_terminal_monotonic_ns'], row['response_write_completed_monotonic_ns'])

    def test_upstream_timeout_retains_failure_and_absent_body_completion(self) -> None:
        _UpstreamHandler.response_gate = threading.Event()
        _UpstreamHandler.headers_sent = threading.Event()
        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with ObservationGateway(
                routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                trace_path=trace, request_timeout_s=0.15,
            ) as gateway:
                try:
                    with self.assertRaises(error.HTTPError) as raised:
                        request.urlopen(request.Request(
                            gateway.endpoint_url('job', 'model'), data=b'{}'), timeout=5)
                    self.assertEqual(raised.exception.code, 502)
                    self.assertTrue(_UpstreamHandler.headers_sent.is_set())
                finally:
                    _UpstreamHandler.response_gate.set()
            row = json.loads(trace.read_text())
        self.assertEqual(row['status'], 'failed')
        self.assertEqual(row['error_phase'], 'upstream_response_read')
        self.assertIsNone(row['upstream_response_body_read_completed_monotonic_ns'])
        self.assertIsNotNone(row['upstream_dispatch_started_monotonic_ns'])
        self.assertIsNotNone(row['response_write_completed_monotonic_ns'])
        self.assertEqual(row['response_write_status'], 'completed')
        self.assertEqual(row['retry_count'], 0)
        self.assertEqual(len(_UpstreamHandler.calls), 1)

    def test_two_requests_can_reach_upstream_before_either_completes(self) -> None:
        _UpstreamHandler.response_gate = threading.Event()
        _UpstreamHandler.two_requests_received = threading.Event()
        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with ObservationGateway(
                routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                trace_path=trace,
            ) as gateway:
                endpoint = gateway.endpoint_url('job', 'model')

                def send():
                    return request.urlopen(request.Request(endpoint, data=b'{}'), timeout=5).read()

                with ThreadPoolExecutor(max_workers=2) as pool:
                    requests = [pool.submit(send) for _ in range(2)]
                    try:
                        self.assertTrue(_UpstreamHandler.two_requests_received.wait(timeout=3))
                    finally:
                        _UpstreamHandler.response_gate.set()
                    for future in requests:
                        future.result(timeout=5)
            rows = [json.loads(line) for line in trace.read_text().splitlines()]
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0]['gateway_request_id'], rows[1]['gateway_request_id'])
        self.assertLess(max(row['upstream_dispatch_started_monotonic_ns'] for row in rows),
                        min(row['upstream_response_body_read_completed_monotonic_ns'] for row in rows))

    def test_client_disconnect_during_body_read_retains_an_unforwarded_failure(self) -> None:
        from aiohttp.web_request import BaseRequest

        body_read_started = threading.Event()
        original_read = BaseRequest.read

        async def observed_read(value):
            body_read_started.set()
            return await original_read(value)

        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with patch.object(BaseRequest, 'read', observed_read):
                with ObservationGateway(
                    routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                    trace_path=trace,
                ) as gateway:
                    url = urlsplit(gateway.endpoint_url('job', 'model'))
                    with socket.create_connection((url.hostname, url.port), timeout=5) as client:
                        client.sendall((f'POST {url.path} HTTP/1.1\r\nHost: localhost\r\n'
                                        'Content-Length: 100\r\n\r\n{}').encode('ascii'))
                        self.assertTrue(body_read_started.wait(timeout=3))
            row = json.loads(trace.read_text())
        self.assertEqual(row['status'], 'failed')
        self.assertEqual(row['error_phase'], 'request_body_read')
        self.assertIsNone(row['request_body_read_completed_monotonic_ns'])
        self.assertIsNone(row['upstream_dispatch_started_monotonic_ns'])
        self.assertIsNone(row['response_write_completed_monotonic_ns'])
        self.assertIsNotNone(row['request_terminal_monotonic_ns'])
        self.assertFalse(row['forwarded'])
        self.assertEqual(_UpstreamHandler.calls, [])

    def test_client_reset_before_return_does_not_claim_response_write_completion(self) -> None:
        from aiohttp.web_response import Response

        _UpstreamHandler.response_gate = threading.Event()
        _UpstreamHandler.headers_sent = threading.Event()
        _UpstreamHandler.response_padding_bytes = 2 * 1024 * 1024
        writer_finished = threading.Event()
        original_prepare = Response.prepare
        original_write = Response.write_eof

        async def watched_prepare(value, incoming):
            try:
                return await original_prepare(value, incoming)
            except BaseException:
                writer_finished.set()
                raise

        async def watched_write(value, data=b''):
            try:
                await original_write(value, data)
            finally:
                writer_finished.set()

        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with patch.object(Response, 'prepare', watched_prepare), patch.object(Response, 'write_eof', watched_write):
                with ObservationGateway(
                    routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                    trace_path=trace,
                ) as gateway:
                    url = urlsplit(gateway.endpoint_url('job', 'model'))
                    with socket.create_connection((url.hostname, url.port), timeout=5) as client:
                        client.sendall((f'POST {url.path} HTTP/1.1\r\nHost: localhost\r\n'
                                        'Content-Length: 2\r\n\r\n{}').encode('ascii'))
                        self.assertTrue(_UpstreamHandler.headers_sent.wait(timeout=3))
                        client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                    _UpstreamHandler.response_gate.set()
                    self.assertTrue(writer_finished.wait(timeout=3))
            row = json.loads(trace.read_text())
        self.assertEqual(row['status'], 'failed')
        self.assertIn(row['error_phase'], ('response_prepare', 'response_write'))
        self.assertEqual(row['upstream_status'], 200)
        self.assertIsNotNone(row['upstream_response_body_read_completed_monotonic_ns'])
        self.assertIsNone(row['response_write_completed_monotonic_ns'])
        self.assertIsNotNone(row['request_terminal_monotonic_ns'])
        self.assertEqual(row['response_write_status'], 'failed')
        self.assertEqual(len(_UpstreamHandler.calls), 1)

    def test_response_callback_failure_retains_completed_upstream_read(self) -> None:
        def reject(_route, _body, _response, _status):
            raise ValueError('fixture callback failed')

        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with ObservationGateway(
                routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                trace_path=trace, after_forward=reject,
            ) as gateway:
                with self.assertRaises(error.HTTPError) as raised:
                    request.urlopen(request.Request(
                        gateway.endpoint_url('job', 'model'), data=b'{}'), timeout=5)
                self.assertEqual(raised.exception.code, 502)
            row = json.loads(trace.read_text())
        self.assertIsNotNone(row['upstream_response_body_read_completed_monotonic_ns'])
        self.assertIsNone(row['after_forward_completed_monotonic_ns'])
        self.assertEqual(row['error_phase'], 'after_forward')
        self.assertEqual(row['error_type'], 'ValueError')
        self.assertEqual(row['upstream_response_status'], 200)
        self.assertEqual(row['upstream_status'], 200)
        self.assertEqual(row['client_status'], 502)
        self.assertEqual(row['actual_total_tokens'], 18)
        self.assertIsNotNone(row['response_write_completed_monotonic_ns'])
        self.assertEqual(len(_UpstreamHandler.calls), 1)

    def test_monotonic_request_interval_survives_wall_clock_moving_backwards(self) -> None:
        epoch = [10_000.0]

        def decreasing_epoch():
            epoch[0] -= 1.0
            return epoch[0]

        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with patch('src.observability.request_gateway.time.time', decreasing_epoch):
                with ObservationGateway(
                    routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                    trace_path=trace,
                ) as gateway:
                    request.urlopen(request.Request(
                        gateway.endpoint_url('job', 'model'), data=b'{}'), timeout=5).read()
            row = json.loads(trace.read_text())
        self.assertLess(row['response_completed_epoch_s'], row['received_epoch_s'])
        self.assertGreater(row['response_write_completed_monotonic_ns'], row['received_monotonic_ns'])
        self.assertEqual(row['status'], 'completed')


    def test_callback_failure_keeps_real_upstream_status_and_usage(self) -> None:
        observed = []
        def reject(_route, _request, response, status):
            observed.append((response, status))
            raise ValueError('fixture response audit failed')
        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with ObservationGateway(
                routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                trace_path=trace, after_forward=reject,
            ) as gateway:
                with self.assertRaises(error.HTTPError) as failure:
                    request.urlopen(request.Request(gateway.endpoint_url('job', 'model'),
                        data=b'{}', headers={'Content-Type': 'application/json'}), timeout=5)
                self.assertEqual(failure.exception.code, 502)
            row = json.loads(trace.read_text())
            self.assertEqual(row['upstream_status'], 200)
            self.assertEqual(row['client_status'], 502)
            self.assertEqual(row['actual_total_tokens'], 18)
            self.assertEqual(row['error_type'], 'ValueError')
            self.assertEqual(row['callback_error_type'], 'ValueError')
            self.assertEqual(row['status'], 'failed')
            self.assertEqual(observed[0][1], 200)
            self.assertEqual(len(_UpstreamHandler.calls), 1)

    def test_callback_failure_preserves_earlier_send_rejection(self) -> None:
        def before(_route, _body):
            raise RuntimeError('fixture send rejected')
        def after(*_args):
            raise ValueError('fixture response audit failed')
        with TemporaryDirectory() as directory:
            trace = Path(directory) / 'gateway.jsonl'
            with ObservationGateway(
                routes=(GatewayRoute('job', 'model', self._upstream_url()),),
                trace_path=trace, before_forward=before, after_forward=after,
            ) as gateway:
                with self.assertRaises(error.HTTPError):
                    request.urlopen(request.Request(gateway.endpoint_url('job', 'model'),
                        data=b'{}', headers={'Content-Type': 'application/json'}), timeout=5)
            row = json.loads(trace.read_text())
            self.assertEqual(row['error_type'], 'RuntimeError')
            self.assertEqual(row['callback_error_type'], 'ValueError')
            self.assertFalse(row['forwarded'])
            self.assertEqual(_UpstreamHandler.calls, [])


if __name__ == "__main__":
    unittest.main()
