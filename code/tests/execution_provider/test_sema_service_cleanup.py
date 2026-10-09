"""Close failures use controlled resources and actual localhost HTTP/Core threads."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import tempfile
import threading
import socket
import unittest
from unittest import mock

from src.baselines.common.private_artifacts import open_private_text
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.adapters.sema_service import (
    SemaRequestService, SemaServiceError, SemaServiceLimits,
)
from src.execution_provider.adapters.sema_semloom import SemaSemLoomService

import test_sema_service as service_tests
from test_sema_semloom import _public_core_diagnostic


class RunnerFailure(RuntimeError): pass
class ExecutorFailure(RuntimeError): pass
class ClientFailure(RuntimeError): pass
class LoopFailure(RuntimeError): pass
class QueryFailure(RuntimeError): pass


class SemaControlledCleanupTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix='sema-cleanup-test-')
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.service = SemaRequestService(query_id='fixture',
            upstream_url='http://localhost:18080/fixture',
            limits=SemaServiceLimits(4, 65536, 65536, 1),
            trace_path=self.root / 'requests.jsonl')
        self.events = []

    def prepare_exit(self, alive=False):
        self.service._thread = mock.Mock()
        self.service._thread.is_alive.return_value = alive
        self.service._trace_context = open_private_text(self.service.trace_path)
        self.service._trace_stream = self.service._trace_context.__enter__()
        self.addCleanup(self.service._trace_context.__exit__, None, None, None)

    @contextmanager
    def resources(self, failures):
        import aiohttp
        from aiohttp import web
        events = self.events

        async def close(stage):
            events.append((stage, threading.get_ident()))
            if stage in failures:
                raise failures[stage]

        class Loop:
            closed = False
            def run_until_complete(self, value): return asyncio.run(value)
            def run_forever(self): pass
            def close(self):
                events.append(('loop', threading.get_ident()))
                if 'loop' in failures: raise failures['loop']
                self.closed = True
            def is_closed(self): return self.closed

        loop = Loop()
        client, runner, site = mock.Mock(), mock.Mock(), mock.Mock()
        client.close = lambda: close('client')
        runner.setup = mock.AsyncMock()
        runner.cleanup = lambda: close('runner')
        runner.sites = set()
        runner.server = None
        site.start = mock.AsyncMock()
        site._server.sockets = [mock.Mock()]
        site._server.sockets[0].getsockname.return_value = ('127.0.0.1', 18080)
        async def executor_close(): await close('executor')
        with ExitStack() as stack:
            stack.enter_context(mock.patch('asyncio.new_event_loop', return_value=loop))
            stack.enter_context(mock.patch('asyncio.set_event_loop'))
            stack.enter_context(mock.patch.object(aiohttp, 'TCPConnector'))
            stack.enter_context(mock.patch.object(aiohttp, 'ClientSession', return_value=client))
            stack.enter_context(mock.patch.object(web, 'Application'))
            stack.enter_context(mock.patch.object(web, 'AppRunner', return_value=runner))
            stack.enter_context(mock.patch.object(web, 'TCPSite', return_value=site))
            stack.enter_context(mock.patch.object(self.service, '_close_executor', executor_close))
            yield loop

    def test_runner_failure_still_closes_executor_client_and_loop(self):
        failure = RunnerFailure('fixture runner')
        with self.resources({'runner': failure}) as loop:
            self.service._run()
        self.assertEqual([e[0] for e in self.events], ['runner', 'executor', 'client', 'loop'])
        self.assertEqual({e[1] for e in self.events}, {threading.get_ident()})
        self.assertTrue(loop.is_closed())
        self.assertIn('runner_cleanup:RunnerFailure', self.service.cleanup_errors)
        self.prepare_exit()
        with self.assertRaises(RunnerFailure) as caught:
            self.service.__exit__(None, None, None)
        self.assertIs(caught.exception, failure)

    def test_multiple_close_failures_keep_all_stages_and_first_exception(self):
        failures = {'runner': RunnerFailure('fixture runner'),
                    'executor': ExecutorFailure('fixture executor'),
                    'client': ClientFailure('fixture client'), 'loop': LoopFailure('fixture loop')}
        with self.resources(failures) as loop:
            self.service._run()
        self.assertEqual([e[0] for e in self.events], ['runner', 'executor', 'client', 'loop'])
        self.assertFalse(loop.is_closed())
        self.assertEqual([r['stage'] for r in self.service.summary['cleanup_failures']],
                         ['runner_cleanup', 'executor_close', 'client_close', 'loop_close'])
        self.prepare_exit()
        with self.assertRaises(RunnerFailure) as caught:
            self.service.__exit__(None, None, None)
        self.assertIs(caught.exception, failures['runner'])
        notes = '\n'.join(caught.exception.__notes__)
        for failure in failures.values(): self.assertIn(type(failure).__name__, notes)
        saved = json.loads(self.service.trace_path.with_suffix('.cleanup.json').read_text())
        self.assertFalse(saved['io_loop_closed'])
        self.assertEqual(len(saved['cleanup_failures']), 4)

    def test_query_first_error_survives_thread_timeout_and_keeps_failure_artifact(self):
        self.prepare_exit(alive=True)
        self.service._loop = mock.Mock()
        self.service._loop.is_closed.return_value = False
        self.service._port = 18080
        first = SemaServiceError(0, 503, b'original HTTP bytes', (('x-fixture', 'original'),))
        self.service.first_error = first
        self.assertIsNone(self.service.__exit__(type(first), first, None))
        self.assertIs(self.service.first_error, first)
        self.assertTrue(self.service.summary['service_thread_alive'])
        self.assertFalse(self.service.summary['io_loop_closed'])
        self.assertEqual(self.service._port, 18080)
        self.assertIn('thread_wait:TimeoutError', self.service.cleanup_errors)
        self.assertIn('TimeoutError', '\n'.join(first.__notes__))
        saved = json.loads(self.service.trace_path.with_suffix('.first-error.json').read_text())
        self.assertEqual(saved['status'], 503)
        self.assertTrue(saved['service_thread_alive'])
        self.service._loop.close.assert_not_called()

    def test_thread_timeout_without_query_error_is_reported(self):
        self.prepare_exit(alive=True)
        self.service._loop = mock.Mock()
        self.service._loop.is_closed.return_value = False
        with self.assertRaises(TimeoutError): self.service.__exit__(None, None, None)
        saved = json.loads(self.service.trace_path.with_suffix('.cleanup.json').read_text())
        self.assertTrue(saved['service_thread_alive'])
        self.assertFalse(saved['io_loop_closed'])
        self.service._loop.close.assert_not_called()

    def test_trace_failures_are_all_recorded_without_replacing_query_error(self):
        self.prepare_exit()
        self.service._rows = [{'query_id': 'fixture'}]
        self.service._trace_stream = mock.Mock()
        self.service._trace_stream.write.side_effect = OSError('fixture trace write')
        original_context = self.service._trace_context
        self.service._trace_context = mock.MagicMock()
        def close(*args):
            original_context.__exit__(*args)
            raise ClientFailure('fixture trace close')
        self.service._trace_context.__exit__.side_effect = close
        primary = QueryFailure('original query')
        self.assertIsNone(self.service.__exit__(type(primary), primary, None))
        self.assertIn('trace_write:OSError', self.service.cleanup_errors)
        self.assertIn('trace_close:ClientFailure', self.service.cleanup_errors)
        self.assertIn('OSError', '\n'.join(primary.__notes__))
        self.assertIn('ClientFailure', '\n'.join(primary.__notes__))

    def test_cancel_failure_keeps_query_error_and_continues_trace_close(self):
        self.prepare_exit()
        primary = QueryFailure('original query')
        with mock.patch.object(self.service, 'cancel', side_effect=ClientFailure('fixture cancel')):
            self.assertIsNone(self.service.__exit__(type(primary), primary, None))
        self.assertTrue(self.service._trace_stream.closed)
        self.assertTrue(self.service._halted.is_set())
        self.assertIn('cancel:ClientFailure', self.service.cleanup_errors)
        self.assertIn('ClientFailure', '\n'.join(primary.__notes__))

    def test_cancel_failure_without_query_error_propagates_after_trace_close(self):
        self.prepare_exit()
        failure = ClientFailure('fixture cancel')
        with mock.patch.object(self.service, 'cancel', side_effect=failure):
            with self.assertRaises(ClientFailure) as caught:
                self.service.__exit__(None, None, None)
        self.assertIs(caught.exception, failure)
        self.assertTrue(self.service._trace_stream.closed)
        saved = json.loads(self.service.trace_path.with_suffix('.cleanup.json').read_text())
        self.assertEqual(saved['cleanup_failures'][0]['stage'], 'cancel')

    def test_http_error_arriving_during_join_remains_primary_with_cleanup_failure(self):
        self.prepare_exit()
        late = SemaServiceError(0, 503, b'late original HTTP bytes', ())
        def join(_timeout):
            self.service._record_cleanup_error('runner_cleanup', RunnerFailure('fixture runner'))
            self.service.first_error = late
        self.service._thread.join.side_effect = join
        with self.assertRaises(SemaServiceError) as caught:
            self.service.__exit__(None, None, None)
        self.assertIs(caught.exception, late)
        self.assertIn('RunnerFailure', '\n'.join(late.__notes__))
        saved = json.loads(self.service.trace_path.with_suffix('.cleanup.json').read_text())
        self.assertEqual(saved['primary_error_type'], 'SemaServiceError')

    def test_http_error_arriving_during_join_without_cleanup_failure_is_reported(self):
        self.prepare_exit()
        late = SemaServiceError(0, 422, b'late original HTTP bytes', ())
        self.service._thread.join.side_effect = lambda _timeout: setattr(self.service, 'first_error', late)
        with self.assertRaises(SemaServiceError) as caught:
            self.service.__exit__(None, None, None)
        self.assertIs(caught.exception, late)
        saved = json.loads(self.service.trace_path.with_suffix('.first-error.json').read_text())
        self.assertEqual(saved['status'], 422)


class SemaThreadedCleanupTests(unittest.TestCase):
    def fixture(self):
        fixture = service_tests.SemaServiceTests(methodName='runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        return fixture

    def run_http_failure(self, query_error=None):
        import aiohttp
        from aiohttp import web
        fixture = self.fixture()
        service = SemaSemLoomService(query_id='query-a',
            model_config=FixedModelConfig(fixture.url, 'fixture-model', 2000),
            physical=RayMapConfig('127.0.0.1:16379', 1, 1, 2**21, 2**23),
            limits=fixture.limits, trace_path=fixture.root / 'requests.jsonl',
            max_held_tasks=2, max_active_requests=1)
        events = []
        original_runner = web.AppRunner.cleanup
        original_client = aiohttp.ClientSession.close
        original_executor = service._close_executor
        failure = RunnerFailure('fixture runner after actual cleanup')
        async def runner_close(runner):
            events.append(('runner', threading.get_ident()))
            await original_runner(runner)
            raise failure
        async def executor_close():
            events.append(('executor', threading.get_ident()))
            await original_executor()
        async def client_close(client):
            if threading.current_thread().name == 'sema-request-service':
                events.append(('client', threading.get_ident()))
            await original_client(client)
        with mock.patch.object(SemaSemLoomService, '_build_execution', _public_core_diagnostic), \
                mock.patch.object(web.AppRunner, 'cleanup', runner_close), \
                mock.patch.object(service, '_close_executor', executor_close), \
                mock.patch.object(aiohttp.ClientSession, 'close', client_close):
            with self.assertRaises(type(query_error) if query_error else RunnerFailure) as caught:
                with service:
                    self.assertEqual(fixture.post(service.endpoint_url)[0], 200)
                    if query_error: raise query_error
                    service.end_input()
        self.assertIs(caught.exception, query_error or failure)
        self.assertEqual([r[0] for r in events], ['runner', 'executor', 'client'])
        self.assertEqual({r[1] for r in events}, {service._thread.ident})
        self.assertFalse(service._thread.is_alive())
        self.assertTrue(service._loop.is_closed())
        self.assertEqual(service._execution.engine.capacity.records, {})
        self.assertEqual(service._execution.engine.jobs.jobs, {})
        self.assertEqual(len(fixture.server.calls), 1)
        saved = json.loads(service.trace_path.with_suffix('.cleanup.json').read_text())
        self.assertFalse(saved['service_thread_alive'])
        self.assertTrue(saved['io_loop_closed'])
        self.assertEqual(saved['cleanup_failures'][0]['stage'], 'runner_cleanup')
        return caught.exception

    def test_actual_http_runner_failure_drains_core_on_io_thread(self):
        self.run_http_failure()

    def test_actual_http_keeps_query_error_when_runner_cleanup_fails(self):
        primary = QueryFailure('original query')
        caught = self.run_http_failure(primary)
        self.assertIn('RunnerFailure', '\n'.join(caught.__notes__))

    def test_startup_failure_survives_executor_close_failure(self):
        fixture = self.fixture()
        service = fixture.service()
        primary = QueryFailure('fixture initialization')
        async def initialize(): raise primary
        async def cleanup(): raise ExecutorFailure('fixture initialization cleanup')
        with mock.patch.object(service, '_initialize_executor', initialize), \
                mock.patch.object(service, '_close_executor', cleanup):
            with self.assertRaisesRegex(RuntimeError, 'startup failed') as caught:
                service.__enter__()
        self.assertIs(caught.exception.__cause__, primary)
        self.assertIn('ExecutorFailure', '\n'.join(primary.__notes__))
        self.assertFalse(service._thread.is_alive())
        self.assertTrue(service._loop.is_closed())
        self.assertEqual(fixture.server.calls, [])

    def test_site_failure_before_close_waits_for_http_writer_before_core_close(self):
        from aiohttp import web
        fixture = self.fixture()
        service = SemaSemLoomService(query_id='query-a',
            model_config=FixedModelConfig(fixture.url, 'fixture-model', 2000),
            physical=RayMapConfig('127.0.0.1:16379', 1, 1, 2**21, 2**23),
            limits=fixture.limits, trace_path=fixture.root / 'requests.jsonl',
            max_held_tasks=2, max_active_requests=1)
        original_stop, original_write = web.TCPSite.stop, web.Response.write_eof
        original_executor = service._close_executor
        writer_started, allow_writer, core_close_started = threading.Event(), threading.Event(), threading.Event()
        observations, sites = [], []
        calls = 0
        async def stop(site):
            nonlocal calls
            calls += 1
            if site not in sites: sites.append(site)
            if calls == 1: raise RunnerFailure('fixture before site stop')
            await original_stop(site)
        async def write(response, *args, **kwargs):
            if threading.current_thread().name == 'sema-request-service' and response.status == 200:
                writer_started.set()
                while not allow_writer.is_set(): await asyncio.sleep(0.002)
            await original_write(response, *args, **kwargs)
        async def close():
            core_close_started.set()
            observations.append(('core_close', allow_writer.is_set()))
            await original_executor()
        def release_writer():
            observations.append(('held_while_writer_waited', service._execution.engine.capacity.usage().held_tasks))
            observations.append(('core_not_closed_early', not core_close_started.is_set()))
            allow_writer.set()
        try:
            with mock.patch.object(SemaSemLoomService, '_build_execution', _public_core_diagnostic), \
                    mock.patch.object(web.TCPSite, 'stop', stop), \
                    mock.patch.object(web.Response, 'write_eof', write), \
                    mock.patch.object(service, '_close_executor', close):
                service.__enter__()
                port = service._port
                with ThreadPoolExecutor(max_workers=1) as callers:
                    request = callers.submit(fixture.post, service.endpoint_url)
                    self.assertTrue(writer_started.wait(2))
                    timer = threading.Timer(0.1, release_writer)
                    timer.start()
                    try:
                        with self.assertRaises(RunnerFailure): service.__exit__(None, None, None)
                        self.assertEqual(request.result(timeout=3)[0], 200)
                    finally:
                        allow_writer.set()
                        timer.join(2)
            self.assertIn(('held_while_writer_waited', 1), observations)
            self.assertIn(('core_not_closed_early', True), observations)
            self.assertIn(('core_close', True), observations)
            self.assertTrue(service.summary['listener_shutdown_confirmed'])
            self.assertEqual(service.summary['http_handlers_remaining'], 0)
            self.assertTrue(service._loop.is_closed())
            self.assertEqual(service._execution.engine.capacity.records, {})
            with socket.socket() as probe: self.assertNotEqual(probe.connect_ex(('127.0.0.1', port)), 0)
        finally:
            allow_writer.set()
            for site in sites:
                if site in site._runner.sites: asyncio.run(original_stop(site))

    def test_persistent_site_stop_failure_retains_unconfirmed_listener(self):
        from aiohttp import web
        fixture = self.fixture()
        service = fixture.service()
        sites = []
        original_stop = web.TCPSite.stop
        async def stop(site):
            if site not in sites: sites.append(site)
            raise RunnerFailure('fixture persistent site stop failure')
        try:
            with mock.patch.object(web.TCPSite, 'stop', stop):
                service.__enter__()
                port = service._port
                self.assertEqual(fixture.post(service.endpoint_url)[0], 200)
                with self.assertRaises(RunnerFailure): service.__exit__(None, None, None)
            self.assertFalse(service.summary['listener_shutdown_confirmed'])
            self.assertEqual(service.summary['listeners_remaining'], 1)
            self.assertEqual(service._port, port)
            self.assertIsNotNone(service._runner)
            self.assertIn('site_stop:RunnerFailure', service.cleanup_errors)
            with socket.socket() as probe: self.assertEqual(probe.connect_ex(('127.0.0.1', port)), 0)
        finally:
            for site in sites:
                if site in site._runner.sites: asyncio.run(original_stop(site))


if __name__ == '__main__': unittest.main()
