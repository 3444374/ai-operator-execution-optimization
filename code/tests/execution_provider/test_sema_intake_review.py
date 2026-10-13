"""Review regressions using CPU consumers and real aiohttp response code, no sockets."""

import asyncio
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from aiohttp.web_protocol import RequestHandler

from src.baselines.text.frameworks.native_calls import NativeCallOutcome
from src.execution_provider.adapters.full_response import FullModelResponse
from src.execution_provider.adapters.sema_native import SemaNativeService
from src.execution_provider.adapters.sema_service import SemaRequestService, SemaServiceLimits


class _Content:
    def __init__(self, value):
        self.value = value

    async def read(self, count):
        value, self.value = self.value[:count], self.value[count:]
        return value


class _FailedForward(SemaRequestService):
    _uses_upstream_client = False

    async def _forward(self, *args):
        raise ValueError('controlled forwarding failure')


class SemaIntakeReviewTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='sema-intake-review-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    async def wait(self, condition):
        deadline = asyncio.get_running_loop().time() + 1
        while not condition():
            self.assertLess(asyncio.get_running_loop().time(), deadline)
            await asyncio.sleep(0.001)

    async def test_missing_native_stop_proof_keeps_live_consumer_body_charged(self):
        reports = (None, {}, {'graph_consumer_stopped': None, 'clean': True}, {'clean': False},
                   {'graph_consumer_stopped': 1, 'clean': True})
        for report in reports:
            with self.subTest(report=report):
                stopped = threading.Event()

                class Executor:
                    backend = 'imlane'
                    last_close_report = report

                    def iter_batch(owner, calls, **options):
                        owner.calls = calls
                        owner.consumer = threading.Thread(target=lambda: (calls, stopped.wait(2)), daemon=True)
                        owner.consumer.start()
                        for call in calls:
                            yield NativeCallOutcome(call.call_id, call.row_id, FullModelResponse(200, (), b'ok'))

                executor = Executor()
                service = SemaNativeService(query_id='intake', upstream_url='http://localhost/model',
                    limits=SemaServiceLimits(4, 1024, 4096, 1), trace_path=self.root / 'unused.jsonl',
                    backend='imlane', executor=executor)
                service._loop = asyncio.get_running_loop()
                service._intake_changed = asyncio.Event()
                await service._initialize_executor()
                row = dict(request_sequence=0, request_deadline_ns=time.monotonic_ns() + 1_000_000_000)
                request = SimpleNamespace(content_length=4, headers={}, content=_Content(b'body'))
                try:
                    capacity = await service._reserve_body(request, row)
                    body = await service._read_body(request, capacity)
                    service._settle_body_size(row, len(body))
                    self.assertEqual((await service._forward(body, {}, row, None)).body, b'ok')
                    service._release_response(row)
                    service._release_body(0)
                    await self.wait(lambda: not service._active and not service._bridge_thread.is_alive())
                    self.assertTrue(executor.consumer.is_alive())
                    self.assertEqual(executor.calls[0].payload, b'body')
                    self.assertFalse(service._snapshot_body_stopped)
                    self.assertEqual((service.summary['intake_requests'], service.summary['intake_bytes']), (1, 4))
                    self.assertIn('native_graph_close', [f['stage'] for f in service.summary['cleanup_failures']])
                finally:
                    stopped.set()
                    executor.consumer.join(1)
                    executor.calls = ()
                    service.end_input()
                    try:
                        await service._close_executor()
                    except RuntimeError:
                        pass

    async def handler(self, status, writer=None, timeout=0.03):
        captured = {}

        class App:
            def __init__(self, **options):
                self.router = SimpleNamespace(add_post=lambda path, handler: captured.update(handler=handler))

        runner = mock.Mock(setup=mock.AsyncMock())
        site = mock.Mock(start=mock.AsyncMock())
        site._server.sockets = [SimpleNamespace(getsockname=lambda: ('127.0.0.1', 1))]
        service = _FailedForward(query_id='intake', upstream_url='http://localhost/model',
            limits=SemaServiceLimits(4, 1024, 4096, timeout), trace_path=self.root / 'unused.jsonl')
        service._loop = asyncio.get_running_loop()
        with mock.patch.object(web, 'Application', App), mock.patch.object(web, 'AppRunner', return_value=runner), \
                mock.patch.object(web, 'TCPSite', return_value=site):
            await service._initialize_http()
        transport = mock.Mock()
        writer = writer or mock.Mock(write_headers=mock.AsyncMock(), write=mock.AsyncMock(),
                                     write_eof=mock.AsyncMock(), drain=mock.AsyncMock())
        request = make_mocked_request('POST', '/sema/intake/v1/chat/completions',
            headers={'Content-Length': str(1025 if status == 413 else 1)},
            match_info={'query_id': 'intake'}, writer=writer, transport=transport, payload=_Content(b'x'))
        protocol = SimpleNamespace(_parser=None, _handler_waiter=None,
            log_access=mock.Mock(), log_debug=mock.Mock(),
            handle_error=mock.Mock(side_effect=AssertionError('automatic error writer must not run')))
        protocol.finish_response = lambda req, response, start: RequestHandler.finish_response(
            protocol, req, response, start)
        return service, request, captured['handler'], protocol, writer

    async def blocked_error_writer(self, status):
        for stage in ('write_headers', 'write_eof'):
            with self.subTest(status=status, stage=stage):
                entered, finish = asyncio.Event(), asyncio.Event()

                async def blocked(*args, **kwargs):
                    entered.set()
                    await finish.wait()

                writer = mock.Mock(write_headers=mock.AsyncMock(), write=mock.AsyncMock(),
                                   write_eof=mock.AsyncMock(), drain=mock.AsyncMock())
                setattr(writer, stage, mock.AsyncMock(side_effect=blocked))
                service, request, handler, protocol, _ = await self.handler(status, writer)
                task = asyncio.create_task(RequestHandler._handle_request(protocol, request, None, handler))
                try:
                    await asyncio.wait_for(entered.wait(), 0.2)
                    first = service.first_error
                    await asyncio.sleep(0.07)
                    self.assertTrue(task.done(), 'synthetic error writer survived the original deadline')
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                    self.assertIs(service.first_error, first)
                    self.assertEqual(first.status, status)
                    self.assertEqual(service._rows[0]['response_write_error_type'], 'TimeoutError')
                    self.assertIsNone(service._rows[0]['response_written_ns'])
                    request.transport.close.assert_called_once()
                    protocol.handle_error.assert_not_called()
                    self.assertEqual(service.summary['http_handlers_remaining'], 0)
                    self.assertEqual(service.summary['intake_bytes'], 0)
                finally:
                    finish.set()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_generated_413_prepare_and_write_use_original_deadline(self):
        await self.blocked_error_writer(413)

    async def test_generated_502_prepare_and_write_use_original_deadline(self):
        await self.blocked_error_writer(502)

    async def test_expired_error_deadline_closes_without_starting_writer(self):
        for status in (413, 502):
            with self.subTest(status=status):
                service, request, handler, protocol, writer = await self.handler(status, timeout=0.01)
                original = service._fail

                def delay_failure(*args, **kwargs):
                    original(*args, **kwargs)
                    time.sleep(0.02)

                with mock.patch.object(service, '_fail', delay_failure):
                    with self.assertRaises(asyncio.CancelledError):
                        await RequestHandler._handle_request(protocol, request, None, handler)
                self.assertEqual(service.first_error.status, status)
                writer.write_headers.assert_not_called()
                writer.write_eof.assert_not_called()
                request.transport.close.assert_called_once()
                protocol.handle_error.assert_not_called()

    async def test_error_reply_finishes_once_before_framework_finish_response(self):
        for status in (413, 502):
            with self.subTest(status=status):
                service, request, handler, protocol, writer = await self.handler(status, timeout=1)
                response, reset = await RequestHandler._handle_request(protocol, request, None, handler)
                self.assertFalse(reset)
                self.assertEqual(response.status, status)
                self.assertEqual(writer.write_eof.await_count, 1)
                self.assertIsNotNone(service._rows[0]['response_written_ns'])
                self.assertLessEqual(service._rows[0]['response_written_ns'], service._rows[0]['request_deadline_ns'])
                request.transport.close.assert_not_called()

    async def test_rejected_query_writers_share_deadline_without_new_first_error(self):
        for status in (404, 410):
            with self.subTest(status=status):
                entered, finish = asyncio.Event(), asyncio.Event()

                async def blocked(*args):
                    entered.set()
                    await finish.wait()

                writer = mock.Mock(write_headers=mock.AsyncMock(), write=mock.AsyncMock(),
                    write_eof=mock.AsyncMock(side_effect=blocked), drain=mock.AsyncMock())
                service, request, handler, protocol, _ = await self.handler(502, writer)
                if status == 404:
                    request.match_info['query_id'] = 'other-query'
                else:
                    service.end_input()
                task = asyncio.create_task(RequestHandler._handle_request(protocol, request, None, handler))
                try:
                    await asyncio.wait_for(entered.wait(), 0.2)
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(task, 0.2)
                    self.assertIsNone(service.first_error)
                    self.assertEqual(service._halted.is_set(), status == 410)
                    request.transport.close.assert_called_once()
                    protocol.handle_error.assert_not_called()
                finally:
                    finish.set()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_connection_close_failure_keeps_first_error_and_cancels_writer(self):
        service, request, handler, protocol, writer = await self.handler(502, timeout=0.01)
        request.transport.close.side_effect = OSError('controlled connection close failure')
        original = service._fail

        def delayed(*args):
            original(*args)
            time.sleep(0.02)

        with mock.patch.object(service, '_fail', delayed), self.assertRaises(asyncio.CancelledError):
            await RequestHandler._handle_request(protocol, request, None, handler)
        self.assertEqual(service.first_error.status, 502)
        self.assertEqual(service.first_error.phase, 'transport')
        self.assertIn('response_connection_close', [f['stage'] for f in service.summary['cleanup_failures']])
        protocol.handle_error.assert_not_called()
        writer.write_eof.assert_not_called()


if __name__ == '__main__':
    unittest.main()
