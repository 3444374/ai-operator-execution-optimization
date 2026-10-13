"""Sema HTTP delivery limits and independent requests through real aiohttp."""

from concurrent.futures import ThreadPoolExecutor
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

from src.execution_provider.adapters.sema_service import SemaRequestService, SemaServiceLimits


_BODY = (b' {"choices":[{"message":{"content":"\\\"POSITIVE\\\""},'
         b'"logprobs":{"content":[{"token":"POSITIVE","logprob":-0.25}]}}],'
         b'"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}\n')


class _Upstream(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def do_POST(self):
        self.rfile.read(int(self.headers['Content-Length']))
        self.server.calls.append((self.headers.get_all('X-Repeat'), self.headers.get('Cookie')))
        self.send_response(200)
        for name, value in (('Content-Type', 'application/json'), ('X-Repeat', 'first'),
                            ('X-Repeat', 'second'), ('Set-Cookie', 'previous=leaked; Path=/'),
                            ('Content-Length', str(len(_BODY)))):
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(_BODY)

    def log_message(self, *_args):
        pass


class SemaDeliveryLimitTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='sema-delivery-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), _Upstream)
        self.server.calls = []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def service(self, **limits):
        return SemaRequestService(query_id='delivery',
            upstream_url=f'http://localhost:{self.server.server_port}/model',
            limits=SemaServiceLimits(4, 1024, 4096, 2, **limits),
            trace_path=self.root / 'requests.jsonl')

    @staticmethod
    def post(service, cookie=None):
        connection = http.client.HTTPConnection('127.0.0.1', service._port, timeout=3)
        try:
            connection.putrequest('POST', '/sema/delivery/v1/chat/completions')
            for value in ('left', 'right'):
                connection.putheader('X-Repeat', value)
            if cookie:
                connection.putheader('Cookie', cookie)
            connection.putheader('Content-Length', '2')
            connection.endheaders(b'{}')
            response = connection.getresponse()
            return response.status, response.read(), response.getheaders()
        finally:
            connection.close()

    def test_complete_body_duplicate_headers_and_request_cookie_isolation(self):
        with self.service() as service:
            first = self.post(service, 'current=one')
            second = self.post(service)
            service.end_input()
        for status, body, headers in (first, second):
            self.assertEqual((status, body), (200, _BODY))
            self.assertEqual([value for name, value in headers if name.lower() == 'x-repeat'],
                             ['first', 'second'])
        self.assertEqual(self.server.calls, [(['left', 'right'], 'current=one'),
                                            (['left', 'right'], None)])
        self.assertEqual(service.summary['upstream_headers_send_started'], 2)
        self.assertEqual(service.summary['unconfirmed_request_start_sequences'], [])
        self.assertEqual(service.summary['pending_response_body_bytes'], 0)

    def test_query_cumulative_response_budget_stops_further_supply(self):
        with self.service(cumulative_response_bytes=len(_BODY)) as service:
            self.assertEqual(self.post(service)[0], 200)
            self.assertEqual(self.post(service)[0], 502)
            self.assertEqual(service.first_error.phase, 'response_volume')
            self.assertEqual(self.post(service)[0], 410)
        self.assertEqual(len(self.server.calls), 2)
        self.assertEqual(service.summary['cumulative_response_body_bytes'], len(_BODY))
        self.assertEqual(service.summary['received_response_body_bytes'], 2 * len(_BODY))
        self.assertEqual(service.summary['pending_response_body_bytes'], 0)
        self.assertEqual(service.summary['unconfirmed_request_start_sequences'], [])

    def test_pending_response_budget_includes_body_until_http_write_finishes(self):
        from aiohttp import web
        import asyncio

        entered, release = threading.Event(), threading.Event()
        original = web.Response.write_eof

        async def delayed(response, *args, **kwargs):
            if response.status == 200:
                entered.set()
                while not release.is_set():
                    await asyncio.sleep(0.002)
            return await original(response, *args, **kwargs)

        with mock.patch.object(web.Response, 'write_eof', delayed), self.service(
                pending_response_bytes=len(_BODY)) as service, ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.post, service)
            try:
                self.assertTrue(entered.wait(1))
                self.assertEqual(service.summary['pending_response_body_bytes'], len(_BODY))
                self.assertEqual(pool.submit(self.post, service).result(timeout=2)[0], 502)
                self.assertEqual(service.first_error.phase, 'response_volume')
            finally:
                release.set()
            self.assertEqual(first.result(timeout=2)[0], 200)
        self.assertEqual(service.summary['pending_response_body_bytes'], 0)
        self.assertEqual(service.summary['peak_pending_response_body_bytes'], len(_BODY))

    def test_invalid_response_allowances_fail_before_service_start(self):
        for name in ('pending_response_bytes', 'cumulative_response_bytes'):
            for value in (0, True, -1):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    SemaServiceLimits(4, 1024, 4096, 2, **{name: value})
