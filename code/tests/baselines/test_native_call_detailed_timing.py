"""Native-call diagnostic contracts using vendor API doubles and no models."""
from contextlib import contextmanager
import asyncio
import json
import os
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from src.baselines.text.frameworks import native_calls as native
from src.experiments import execution_timing
from .test_native_dataset_configuration import calls, daft_graph, outcome, ray_graph


class MemoryWriter:
    def __init__(self, _path):
        self.events = []
        self.failure = None

    def record(self, event):
        if self.failure and self.failure(event):
            raise OSError('fixture diagnostic writer failed')
        self.events.append(event)

    def close(self):
        pass


@contextmanager
def observations(enabled, *, fail_operation=None, fail_event='span_ended'):
    with tempfile.TemporaryDirectory(prefix='native-call-timing-') as directory, patch.dict(os.environ):
        if enabled:
            os.environ['SEMLOOM_DETAILED_TIMING_DIR'] = directory
            timing = execution_timing.Timing(directory, writer_factory=MemoryWriter)
            timing.writer.failure = lambda event: (event['operation'] == fail_operation
                                                   and event['event'] == fail_event)
        else:
            os.environ.pop('SEMLOOM_DETAILED_TIMING_DIR', None)
            timing = None
        with patch.object(execution_timing, 'get_timing', return_value=timing):
            if timing is None:
                yield None
            else:
                with timing.bind(query_id='actual-fixture-query', owner_id='actual-fixture-owner'):
                    yield timing


@contextmanager
def tracked_graph(backend, records, *, close_failure=None):
    fixture = daft_graph if backend == 'daft' else ray_graph
    records['closes'] = []

    class Iterator:
        def __init__(self, original):
            self.original = original

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.original)

        def close(self):
            records['closes'].append('iterator.close')
            self.original.close()
            if close_failure is not None:
                raise close_failure

    def track(result):
        original = result.iter_rows

        def opened(*args, **kwargs):
            iterator = Iterator(original(*args, **kwargs))
            records['iterator'] = iterator
            return iterator
        result.iter_rows = opened
        return result

    with fixture(records):
        if backend == 'daft':
            module = sys.modules['daft']
            original = module.from_pydict
            with patch.object(module, 'from_pydict', side_effect=lambda values: track(original(values))):
                yield
        else:
            module = sys.modules['ray.data.llm']
            original = module.build_processor

            def processor(*args, **kwargs):
                inner = original(*args, **kwargs)

                class Processor:
                    stages = inner.stages

                    def __call__(self, dataset):
                        return track(inner(dataset))
                return Processor()

            with patch.object(module, 'build_processor', side_effect=processor):
                yield


def graph_rows(backend, control):
    function = native._daft_rows if backend == 'daft' else native._ray_rows
    return function(calls()[:2], native.NativeGraphOptions(), control)


class NativeCallDetailedTimingTests(unittest.TestCase):
    def test_daft_execution_error_precedes_close_failure_with_timing_on_or_off(self):
        for enabled in (False, True):
            execution, closing = ValueError('fixture execution'), OSError('fixture close')
            with self.subTest(enabled=enabled), observations(enabled), \
                    tracked_graph('daft', records := {}, close_failure=closing), \
                    patch.object(native, '_request', AsyncMock(side_effect=execution)):
                with self.assertRaises(ValueError) as caught:
                    list(graph_rows('daft', native._GraphControl()))
                self.assertIs(caught.exception, execution)
                self.assertIn('OSError', '\n'.join(execution.__notes__))
                self.assertEqual(records['closes'], ['iterator.close'])
                self.assertFalse(native._DAFT_CONTROLS)

    def test_daft_setup_failure_releases_registration_with_timing_on_or_off(self):
        for enabled in (False, True):
            for stage in ('decorator', 'expression', 'iterator_open'):
                setup = ValueError('fixture ' + stage + ' setup')
                with self.subTest(enabled=enabled, stage=stage), observations(enabled), daft_graph({}):
                    module = sys.modules['daft']
                    original = module.from_pydict
                    def failing_open(values):
                        frame = original(values)
                        frame.iter_rows = Mock(side_effect=setup)
                        return frame
                    target, name = (module.func, 'batch') if stage == 'decorator' else (
                        module, 'col' if stage == 'expression' else 'from_pydict')
                    with patch.object(target, name, side_effect=failing_open if stage == 'iterator_open' else setup):
                        try:
                            with self.assertRaises(ValueError) as caught:
                                list(graph_rows('daft', native._GraphControl()))
                            self.assertIs(caught.exception, setup)
                            self.assertFalse(native._DAFT_CONTROLS)
                        finally:
                            native._DAFT_CONTROLS.clear()

    def test_daft_failed_graph_releases_control_and_prevents_executor_reuse(self):
        for enabled in (False, True):
            execution = ValueError('fixture execution')
            with self.subTest(enabled=enabled), observations(enabled), daft_graph({}), \
                    patch.object(native, '_request', AsyncMock(side_effect=execution)):
                executor = native.NativeCallExecutor('daft')
                with self.assertRaises(ValueError) as caught:
                    list(executor.iter_batch(calls()))
                self.assertIs(caught.exception, execution)
                self.assertFalse(native._DAFT_CONTROLS)
                self.assertTrue(executor.last_close_report['graph_consumer_stopped'])
                self.assertFalse(executor.last_close_report['reuse_allowed'])
                with self.assertRaisesRegex(RuntimeError, 'unconfirmed work'):
                    list(executor.iter_batch(calls()))

    def test_daft_partial_close_and_cancellation_keep_cleanup_responsibility(self):
        for enabled in (False, True):
            for category in (None, InterruptedError, asyncio.CancelledError):
                closing = OSError('fixture close')
                primary = category('fixture cancellation') if category else None
                with self.subTest(enabled=enabled, category=category), observations(enabled), \
                        tracked_graph('daft', records := {}, close_failure=closing), \
                        patch.object(native, '_request', AsyncMock(side_effect=lambda call, *_: outcome(call))):
                    iterator = graph_rows('daft', native._GraphControl())
                    next(iterator)
                    with self.assertRaises(category or OSError) as caught:
                        iterator.throw(primary) if primary else iterator.close()
                    self.assertIs(caught.exception, primary or closing)
                    if primary:
                        self.assertIn('OSError', '\n'.join(primary.__notes__))
                    self.assertEqual(records['closes'], ['iterator.close'])
                    self.assertFalse(native._DAFT_CONTROLS)

    def test_on_off_preserve_vendor_graph_results_settings_and_consumer_thread_context(self):
        for backend in ('daft', 'ray-data'):
            samples = []
            for enabled in (False, True):
                records, cookies = {}, []

                async def request(call, *_args, **_kwargs):
                    cookies.append(native._HTTP_CALL_COOKIES.get())
                    return outcome(call)

                with self.subTest(backend=backend, enabled=enabled), observations(enabled) as timing, \
                        tracked_graph(backend, records), patch.object(native, '_request', request):
                    cookie_token = native._HTTP_CALL_COOKIES.set('caller-only-context')
                    try:
                        executor = native.NativeCallExecutor(backend)
                        values = list(executor.iter_batch(calls()[:2]))
                    finally:
                        native._HTTP_CALL_COOKIES.reset(cookie_token)
                    settings = ({name: records[name] for name in ('input', 'udf_options', 'iterator_options')}
                                if backend == 'daft' else
                                {name: records[name] for name in ('input', 'source_options', 'config')})
                    samples.append((values, settings, records['closes'], executor.last_close_report['clean']))
                    self.assertTrue(executor.last_close_report['reuse_allowed'])
                    if backend == 'daft':
                        self.assertEqual(cookies, [None, None])
                    if timing is not None:
                        events = timing.writer.events
                        self.assertTrue(all(e['query_id'] == 'actual-fixture-query' for e in events))
                        self.assertTrue(all(e['owner_id'] == 'actual-fixture-owner' for e in events))
                        opened = [e for e in events if e['operation'] == 'native_call.iterator_open']
                        self.assertTrue(all('actor readiness unavailable' in e['scope'] for e in opened))
                        consumed = [e for e in events if e['operation'] == 'native_call.consume']
                        self.assertTrue(all('consumer yield' in e['scope'] for e in consumed))
                        self.assertFalse(any('actor_init' in e['operation'] or 'queue' in e['operation']
                                             for e in events))
            self.assertEqual(samples[0], samples[1])

    def test_iterator_open_record_failure_still_closes_created_iterator(self):
        for backend in ('daft', 'ray-data'):
            with self.subTest(backend=backend), observations(True,
                    fail_operation='native_call.iterator_open'), tracked_graph(backend, records := {}):
                with self.assertRaisesRegex(OSError, 'diagnostic writer failed'):
                    list(graph_rows(backend, native._GraphControl()))
                self.assertEqual(records['closes'], ['iterator.close'])
                if backend == 'ray-data':
                    records['executor'].shutdown.assert_called_once_with(force=True)
                self.assertFalse(native._DAFT_CONTROLS)

    def test_graph_build_record_failure_releases_known_graph_resources(self):
        for backend in ('daft', 'ray-data'):
            with self.subTest(backend=backend), observations(True,
                    fail_operation='native_call.graph_build'), tracked_graph(backend, records := {}):
                with self.assertRaisesRegex(OSError, 'diagnostic writer failed'):
                    list(graph_rows(backend, native._GraphControl()))
                self.assertEqual(records['closes'], [])
                if backend == 'ray-data':
                    records['executor'].shutdown.assert_called_once_with(force=True)
                self.assertFalse(native._DAFT_CONTROLS)

    def test_cleanup_observation_failure_follows_all_original_cleanup_calls(self):
        for backend in ('daft', 'ray-data'):
            with self.subTest(backend=backend), observations(True,
                    fail_operation='native_call.graph_cleanup', fail_event='span_started'), \
                    tracked_graph(backend, records := {}), patch.object(native, '_request',
                    AsyncMock(side_effect=lambda call, *_args, **_kwargs: outcome(call))):
                with self.assertRaisesRegex(OSError, 'diagnostic writer failed'):
                    list(graph_rows(backend, native._GraphControl()))
                self.assertEqual(records['closes'], ['iterator.close'])
                if backend == 'ray-data':
                    records['executor'].shutdown.assert_called_once_with(force=True)
                self.assertFalse(native._DAFT_CONTROLS)

    def test_on_off_attempt_all_cleanup_and_preserve_native_close_error(self):
        for enabled in (False, True):
            closing = ValueError('original iterator close failed')
            with self.subTest(enabled=enabled), observations(enabled,
                    fail_operation='native_call.graph_cleanup', fail_event='span_started'), \
                    tracked_graph('ray-data', records := {}, close_failure=closing):
                with self.assertRaises(ValueError) as caught:
                    list(graph_rows('ray-data', native._GraphControl()))
                self.assertIs(caught.exception, closing)
                self.assertEqual(records['closes'], ['iterator.close'])
                records['executor'].shutdown.assert_called_once_with(force=True)
                if enabled:
                    self.assertIn('OSError', '\n'.join(caught.exception.__notes__))

    def test_consume_record_error_does_not_replace_original_close_error(self):
        for backend in ('daft', 'ray-data'):
            closing = ValueError('original native iterator close failed')
            with self.subTest(backend=backend), observations(True,
                    fail_operation='native_call.consume'), tracked_graph(backend, records := {},
                    close_failure=closing), patch.object(native, '_request',
                    AsyncMock(side_effect=lambda call, *_args, **_kwargs: outcome(call))):
                with self.assertRaises(ValueError) as caught:
                    list(graph_rows(backend, native._GraphControl()))
                self.assertIs(caught.exception, closing)
                self.assertIn('OSError', '\n'.join(closing.__notes__))
                if backend == 'ray-data':
                    records['executor'].shutdown.assert_called_once_with(force=True)
                self.assertFalse(native._DAFT_CONTROLS)


class NativeProcessorDetailedTimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_session_and_post_on_off_keep_original_full_response(self):
        samples = []
        for enabled in (False, True):
            with self.subTest(enabled=enabled), observations(enabled) as timing:
                client = object()
                call = calls()[0]
                returned = outcome(call)
                ray = SimpleNamespace(is_initialized=lambda: True, get_runtime_context=lambda:
                    SimpleNamespace(get_actor_id=lambda: 'observed-actor'))
                with patch.dict(sys.modules, {'ray': ray}), \
                        patch.object(native, '_new_http_client', return_value=client) as opened, \
                        patch.object(native, '_close_http_client', new_callable=AsyncMock) as closed, \
                        patch.object(native, '_request', AsyncMock(return_value=returned)) as request:
                    async with native._ProcessorSession() as session:
                        response = await session.post(native._PROCESSOR_URL,
                            data=json.dumps({'native_call': native._call_wire(call)}), headers={})
                        samples.append(await response.json())
                    opened.assert_called_once_with()
                    closed.assert_awaited_once_with(client)
                    request.assert_awaited_once_with(call, client=client)
                if timing is not None:
                    events = timing.writer.events
                    posted = [e for e in events if e['operation'] == 'native_call.http_call']
                    self.assertTrue(all(e['call_id'] == call.call_id and e['row_id'] == call.row_id for e in posted))
                    initialized = [e for e in events if e['operation'] == 'native_call.http_session_init']
                    self.assertTrue(all('local HTTP client' in e['scope'] for e in initialized))
                    self.assertTrue(all(e['actor_id'] == 'observed-actor' for e in posted + initialized))
                    self.assertFalse(any('actor_init' in e['operation'] for e in events))
        self.assertEqual(samples[0], samples[1])

    async def test_session_init_end_record_failure_closes_acquired_client(self):
        with observations(True, fail_operation='native_call.http_session_init'):
            client = object()
            with patch.object(native, '_new_http_client', return_value=client), patch.object(
                    native, '_close_http_client', new_callable=AsyncMock) as closed:
                with self.assertRaisesRegex(OSError, 'diagnostic writer failed'):
                    await native._ProcessorSession().__aenter__()
                closed.assert_awaited_once_with(client)

    async def test_session_creation_error_is_not_replaced_by_record_failure(self):
        original = ValueError('original session creation error')
        with observations(True, fail_operation='native_call.http_session_init', fail_event='span_started'):
            with patch.object(native, '_new_http_client', side_effect=original), patch.object(
                    native, '_close_http_client', new_callable=AsyncMock) as closed:
                with self.assertRaises(ValueError) as caught:
                    await native._ProcessorSession().__aenter__()
                self.assertIs(caught.exception, original)
                self.assertIn('OSError', '\n'.join(original.__notes__))
                closed.assert_not_awaited()

    async def test_real_session_close_error_precedes_init_record_error(self):
        original = ValueError('original session close failed')
        with observations(True, fail_operation='native_call.http_session_init'):
            with patch.object(native, '_new_http_client', return_value=object()), patch.object(
                    native, '_close_http_client', AsyncMock(side_effect=original)) as closed:
                with self.assertRaises(ValueError) as caught:
                    await native._ProcessorSession().__aenter__()
                self.assertIs(caught.exception, original)
                self.assertIn('OSError', '\n'.join(original.__notes__))
                closed.assert_awaited_once()

    async def test_real_session_close_error_precedes_post_record_error(self):
        original = ValueError('original session close failed')
        call = calls()[0]
        with observations(True, fail_operation='native_call.http_call'):
            with patch.object(native, '_new_http_client', return_value=object()), patch.object(
                    native, '_close_http_client', AsyncMock(side_effect=original)), patch.object(
                    native, '_request', AsyncMock(return_value=outcome(call))):
                with self.assertRaises(ValueError) as caught:
                    async with native._ProcessorSession() as session:
                        await session.post(native._PROCESSOR_URL,
                            data=json.dumps({'native_call': native._call_wire(call)}), headers={})
                self.assertIs(caught.exception, original)
                self.assertIn('OSError', '\n'.join(original.__notes__))


if __name__ == '__main__':
    unittest.main()
