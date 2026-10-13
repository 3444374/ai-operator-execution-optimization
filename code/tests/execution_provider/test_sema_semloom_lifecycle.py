"""Local complete-HTTP fixtures; these do not establish Ray or author-Sema parity."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import unittest
from urllib import error, request

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.sema_semloom import SemaSemLoomExecutor, SemaSemLoomService
from src.execution_provider.adapters.sema_service import SemaServiceError, SemaServiceLimits


class _CompleteResponseFixture(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        self.server.calls.append((body, self.headers.get('Cookie'), threading.get_ident()))
        self.server.connections.append(self.client_address)
        self.server.received.set()
        if self.server.gate is not None:
            self.server.gate.wait(2)
        if self.server.drop_response:
            self.close_connection = True
            self.server.finished.set()
            return
        payload = self.server.body
        self.send_response(self.server.status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('X-Repeat', 'first')
        self.send_header('X-Repeat', 'second')
        self.send_header('Set-Cookie', 'first=private; Path=/')
        self.send_header('Set-Cookie', 'second=private; Path=/')
        if self.server.compressed:
            self.send_header('Content-Encoding', 'gzip')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.server.finished.set()

    def log_message(self, *_args):
        pass


class SemaSemLoomLifecycleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='sema-semloom-lifecycle-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), _CompleteResponseFixture)
        self.server.calls = []
        self.server.connections = []
        self.server.status, self.server.compressed = 200, False
        self.server.gate = None
        self.server.drop_response = False
        self.server.received, self.server.finished = threading.Event(), threading.Event()
        self.server.body = (
            b' {"id":"unchanged","choices":[{"message":{"content":"\\\"POSITIVE\\\""},'
            b'"logprobs":{"content":[{"token":"POSITIVE","logprob":-0.12,'
            b'"top_logprobs":[{"token":"NEGATIVE","logprob":-2.1}]}]}}],'
            b'"usage":{"prompt_tokens":17,"completion_tokens":4,"total_tokens":21,'
            b'"prompt_tokens_details":{"cached_tokens":9}},"vendor":{"keep":[1,null]}}\n'
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_fixture)
        self.url = 'http://127.0.0.1:' + str(self.server.server_port) + '/v1/chat/completions'
        self.payload = b' {"model":"fixture-model","messages":[{"role":"user","content":"original"}]} '
        self.limits = SemaServiceLimits(8, 65536, 65536, 2)
        self.model = FixedModelConfig(self.url, 'fixture-model', 1000)

    def stop_fixture(self):
        if self.server.gate is not None:
            self.server.gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def service(self, name='local', *, owner=None, held=2):
        return SemaSemLoomService(query_id=name, model_config=self.model, physical=None,
            limits=self.limits, trace_path=self.root / (name + '.jsonl'),
            max_held_tasks=held, max_active_requests=1, local_diagnostic=True,
            executor_owner=owner)

    def post(self, url, payload=None):
        call = request.Request(url, data=self.payload if payload is None else payload,
                               headers={'Content-Type': 'application/json'})
        try:
            response = request.urlopen(call, timeout=3)
        except error.HTTPError as failure:
            response = failure
        with response:
            return response.status, response.read(), response.headers

    def test_real_local_transport_preserves_status_duplicate_headers_body_usage_and_logprobs(self):
        for status, compressed, body in (
            (200, False, self.server.body),
            (200, True, gzip.compress(self.server.body)),
            (422, False, b' {"error":{"detail":"original"},"usage":{"total_tokens":3}}\n'),
            (200, False, b'not JSON; native parser owns this failure\n'),
        ):
            with self.subTest(status=status, compressed=compressed, body=body):
                self.server.status, self.server.compressed, self.server.body = status, compressed, body
                direct = self.post(self.url)
                name = 'case-' + str(len(self.server.calls))
                with self.service(name) as service:
                    actual = self.post(service.endpoint_url)
                    self.assertEqual(actual[:2], direct[:2])
                    for header in ('Content-Type', 'Content-Encoding', 'X-Repeat', 'Set-Cookie'):
                        self.assertEqual(actual[2].get_all(header), direct[2].get_all(header))
                    if status == 200:
                        service.end_input()
                    else:
                        self.assertEqual(service.first_error.body, body)
                        self.assertEqual(service.first_error.status, status)
                        self.assertEqual([value for key, value in service.first_error.headers
                                          if key.lower() == 'x-repeat'], ['first', 'second'])
                self.assertEqual(service._pending, {})
                self.assertTrue(service.summary['core_cleanup']['settled'])
                self.assertEqual(service.summary['core_cleanup']['unconfirmed_remote_requests'], 0)
                self.assertFalse(service.cleanup_errors)
        self.assertEqual([body for body, _cookie, _thread in self.server.calls], [self.payload] * 8)

    def test_pump_fault_keeps_cause_wakes_consumers_and_is_reported_at_exit(self):
        primary = ValueError('fixture Core advance failed')
        self.server.gate = threading.Event()
        with self.assertRaises(SemaServiceError) as caught:
            with self.service(held=1) as service:
                with ThreadPoolExecutor(max_workers=2) as callers:
                    first = callers.submit(self.post, service.endpoint_url)
                    self.assertTrue(self.server.received.wait(1))
                    second = callers.submit(self.post, service.endpoint_url)

                    async def fail_next_advance():
                        advance = service._session.advance

                        def fail_once(*args):
                            service._session.advance = advance
                            raise primary

                        service._session.advance = fail_once

                    asyncio.run_coroutine_threadsafe(fail_next_advance(), service._loop).result(1)
                    try:
                        self.assertEqual(first.result(timeout=1)[0], 502)
                        self.assertIn(second.result(timeout=1)[0], (410, 502))
                        self.assertEqual(service.first_error.phase, 'execution_pump')
                        self.assertIs(service.first_error.__cause__, primary)
                        self.assertFalse(service._pending)
                    finally:
                        self.server.gate.set()
        self.assertIs(caught.exception.__cause__, primary)
        self.assertEqual(len(self.server.calls), 1)
        self.assertTrue(service.summary['core_cleanup']['settled'])
        self.assertEqual(service.cleanup_errors, [])

    def test_pump_fault_after_delivered_result_poisoned_resident_owner(self):
        primary = LookupError('fixture resident pump failed')
        with SemaSemLoomExecutor(model_config=self.model, physical=None, max_held_tasks=2,
                max_active_requests=1, local_diagnostic=True) as owner:
            with self.assertRaises(SemaServiceError) as caught:
                with self.service('first', owner=owner) as service:
                    self.assertEqual(self.post(service.endpoint_url)[0], 200)

                    async def fail_next_advance():
                        advance = service._session.advance

                        def fail_once(*args):
                            service._session.advance = advance
                            raise primary

                        service._session.advance = fail_once
                        await asyncio.sleep(.03)

                    asyncio.run_coroutine_threadsafe(fail_next_advance(), service._loop).result(1)
                    service.end_input()
            self.assertIs(caught.exception.__cause__, primary)
            self.assertTrue(owner.poisoned)
            with self.assertRaisesRegex(RuntimeError, 'startup failed'):
                with self.service('rejected', owner=owner):
                    self.fail('resident executor accepted work after its pump failed')
        self.assertFalse(owner._thread.is_alive())
        self.assertTrue(owner._loop.is_closed())
        self.assertEqual(len(self.server.calls), 1)

    def test_later_pump_fault_keeps_the_complete_http_error_primary(self):
        primary = LookupError('fixture later pump failed')
        self.server.status = 422
        with self.service() as service:
            self.assertEqual(self.post(service.endpoint_url)[:2], (422, self.server.body))
            first = service.first_error

            async def fail_next_advance():
                advance = service._session.advance

                def fail_once(*args):
                    service._session.advance = advance
                    raise primary

                service._session.advance = fail_once
                await asyncio.sleep(.03)

            asyncio.run_coroutine_threadsafe(fail_next_advance(), service._loop).result(1)
            self.assertIs(service.first_error, first)
            self.assertEqual(first.phase, 'http_response')
            self.assertEqual(first.body, self.server.body)
        self.assertEqual(service.cleanup_errors, ['execution_pump:LookupError'])
        self.assertTrue(service.summary['core_cleanup']['settled'])
        self.assertEqual(len(self.server.calls), 1)

    def test_incomplete_http_retains_unknown_work_and_prevents_resident_reuse(self):
        self.server.drop_response = True
        owner = SemaSemLoomExecutor(model_config=self.model, physical=None, max_held_tasks=2,
            max_active_requests=1, local_diagnostic=True).__enter__()
        try:
            with self.service(owner=owner) as service:
                self.assertEqual(self.post(service.endpoint_url)[0], 502)
            cleanup = service.summary['core_cleanup']
            self.assertFalse(cleanup['settled'])
            self.assertEqual(cleanup['unconfirmed_remote_requests'], 1)
            self.assertEqual(cleanup['remaining_usage']['active_requests'], 1)
            self.assertEqual(cleanup['remaining_jobs'], 1)
            self.assertEqual(cleanup['consumer_close']['uncertain_requests'], 1)
            self.assertTrue(owner.poisoned)
            self.assertEqual(owner.snapshot()['core_usage']['held_tasks'], 1)
            with self.assertRaisesRegex(RuntimeError, 'startup failed'):
                with self.service('rejected', owner=owner):
                    self.fail('unknown remote work permitted another resident query')
            with self.assertRaisesRegex(RuntimeError, 'retains a query or unresolved work'):
                owner.__exit__(None, None, None)
            self.assertTrue(owner._thread.is_alive())
        finally:
            # The in-process fixture has independently finished its one handler.
            # Test teardown stops local threads; it is not production stop evidence
            # and does not erase the retained Core records or report them settled.
            self.assertTrue(self.server.finished.wait(1))
            self.assertTrue(owner.execution.engine.backend.close(1))
            owner._loop.call_soon_threadsafe(owner._loop.stop)
            owner._thread.join(2)
        self.assertFalse(owner._thread.is_alive())
        self.assertTrue(owner._loop.is_closed())
        self.assertEqual(owner.execution.engine.capacity.usage().active_requests, 1)
        self.assertEqual(len(self.server.calls), 1)

    def test_resident_transport_stays_on_its_owner_loop_across_separate_caller_loops(self):
        caller_loops, executor_ids, transport_ids = [], [], []
        with SemaSemLoomExecutor(model_config=self.model, physical=None, max_held_tasks=2,
                max_active_requests=1, local_diagnostic=True) as owner:
            for number in range(2):
                async def consume():
                    caller_loops.append(asyncio.get_running_loop())
                    with self.service('loop-' + str(number), owner=owner) as service:
                        executor_ids.append(service.summary['execution_id'])
                        result = await asyncio.to_thread(self.post, service.endpoint_url)
                        self.assertEqual(result[:2], (200, self.server.body))
                        self.assertEqual(result[2].get_all('Set-Cookie'),
                                         ['first=private; Path=/', 'second=private; Path=/'])
                        service.end_input()
                    transport = owner.execution.engine.backend._execute.__self__
                    transport_ids.append(id(transport._client))
                    self.assertFalse(transport._client.is_closed)
                    self.assertTrue(service.summary['core_cleanup']['settled'])
                    self.assertEqual(owner.snapshot()['core_jobs'], 0)
                asyncio.run(consume())
            self.assertEqual(len(set(executor_ids)), 1)
            self.assertEqual(len(set(transport_ids)), 1)
            transport = owner.execution.engine.backend._execute.__self__
        self.assertIsNot(caller_loops[0], caller_loops[1])
        self.assertTrue(all(loop.is_closed() for loop in caller_loops))
        self.assertTrue(transport._client.is_closed)
        self.assertFalse(owner._thread.is_alive())
        self.assertTrue(owner._loop.is_closed())
        self.assertEqual(len(self.server.calls), 2)
        self.assertEqual([cookie for _body, cookie, _thread in self.server.calls], [None, None])
        self.assertEqual(self.server.connections[0], self.server.connections[1],
                         'request isolation replaced the resident upstream connection pool')


if __name__ == '__main__':
    unittest.main()
