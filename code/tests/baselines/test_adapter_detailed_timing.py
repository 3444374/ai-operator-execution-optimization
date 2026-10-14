"""Optional adapter timing with protocol fixtures; no vendor runtime or models."""
import base64
from contextlib import contextmanager
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import imlane_calls, prepared_map
from src.baselines.text.frameworks.native_calls import NativeCall, NativeCallOutcome
from src.execution_provider.adapters.full_response import FullModelResponse
from src.experiments import execution_timing


class MemoryWriter:
    def __init__(self, _path):
        self.events = []

    def record(self, event):
        self.events.append(event)

    def close(self):
        pass


class CleanupFailureWriter(MemoryWriter):
    def record(self, event):
        if event['operation'].endswith(('stream_close', 'iterator_close', '.close')) or (
                event['operation'].endswith(('consume', '.batch')) and event['event'] == 'span_ended'):
            raise OSError('fixture timing sink failed')
        super().record(event)


class StreamCloseFailureWriter(MemoryWriter):
    def record(self, event):
        if event['operation'] == 'native.stream_close':
            raise OSError('fixture stream-close timing failed')
        super().record(event)


class ConsumeEndFailureWriter(MemoryWriter):
    def record(self, event):
        if event['operation'] == 'native.consume' and event['event'] == 'span_ended':
            raise OSError('fixture consume-end timing failed')
        super().record(event)


@contextmanager
def observations(enabled, *, writer=MemoryWriter):
    with tempfile.TemporaryDirectory(prefix='adapter-timing-') as directory:
        timing = execution_timing.Timing(directory, writer_factory=writer) if enabled else None
        with patch.dict(os.environ), patch.object(execution_timing, 'get_timing', return_value=timing), \
                patch.object(imlane_calls, 'get_timing', return_value=timing):
            if enabled:
                os.environ['SEMLOOM_DETAILED_TIMING_DIR'] = directory
            else:
                os.environ.pop('SEMLOOM_DETAILED_TIMING_DIR', None)
            if timing is None:
                yield None
            else:
                with timing.bind(query_id='fixture-query', owner_id='fixture-owner'):
                    yield timing


@contextmanager
def native_graph(backend, *, fail=False, close_failure=None):
    """Expose each adapter's actual vendor API sequence without running a kernel."""
    transcript = []
    wire = base64.b64encode(b'complete response').decode()

    class Rows:
        def __init__(self):
            self.position = 0

        def __iter__(self):
            return self

        def __next__(self):
            transcript.append(('next', self.position))
            if fail:
                raise ValueError('fixture native iteration failed')
            if self.position == 2:
                raise StopIteration
            row = dict(call_id='call-' + str(self.position), row_id=self.position, response=wire)
            self.position += 1
            return row

        def close(self):
            transcript.append(('iterator.close',))
            if close_failure is not None:
                raise close_failure

    class Frame:
        def with_column(self, name, _expression):
            transcript.append(('with_column', name))
            return self

        def select(self, *names):
            transcript.append(('select', names))
            return self

        def iter_rows(self):
            transcript.append(('iter_rows',))
            return Rows()

        def stats(self):
            transcript.append(('stats',))
            return 'fixture stats'

    if backend == 'daft':
        daft = types.ModuleType('daft')
        daft.__version__ = '0.7.21'
        daft.DataType = types.SimpleNamespace(string=lambda: 'string')
        daft.col = lambda name: name

        def from_pydict(values):
            transcript.append(('from_pydict', values))
            return Frame()

        def batch(**options):
            transcript.append(('batch', options))
            return lambda _function: lambda *_columns: 'fixture expression'

        daft.from_pydict = from_pydict
        daft.func = types.SimpleNamespace(batch=batch)
        modules = {'daft': daft}
    else:
        ray = types.ModuleType('ray')
        ray.__version__ = '2.56.1'
        ray.is_initialized = lambda: True
        data = types.ModuleType('ray.data')

        def from_items(values, **options):
            transcript.append(('from_items', values, options))
            return 'fixture dataset'

        data.from_items = from_items
        ray.data = data
        llm = types.ModuleType('ray.data.llm')

        def config(**options):
            transcript.append(('config', options))
            return options

        class Processor:
            def __init__(self):
                self.stages = {'http': types.SimpleNamespace(map_batches_kwargs={})}

            def __call__(self, dataset):
                transcript.append(('processor', dataset, dict(self.stages['http'].map_batches_kwargs)))
                return Frame()

        def build_processor(_config, **options):
            transcript.append(('build_processor', options))
            return Processor()

        llm.HttpRequestProcessorConfig, llm.build_processor = config, build_processor
        planner = types.ModuleType('ray.data._internal.planner.plan_udf_map_op')
        planner.DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY = 2
        modules = {'ray': ray, 'ray.data': data, 'ray.data.llm': llm,
                   'ray.data._internal.planner.plan_udf_map_op': planner}
    with patch.dict('sys.modules', modules):
        yield transcript


CALLS = tuple(dict(call_id='call-' + str(index), row_id=index, source_position=index,
                  payload=b'{"model":"fixture"}') for index in range(2))
SESSION = types.SimpleNamespace(endpoint='http://127.0.0.1/fixture')


class AdapterDetailedTimingTests(unittest.TestCase):
    def consume(self, backend, *, enabled, fail=False, partial=False, writer=MemoryWriter):
        with observations(enabled, writer=writer) as timing, native_graph(backend, fail=fail) as transcript:
            options = prepared_map.NativeGraphOptions()
            opener = prepared_map.open_daft_calls if backend == 'daft' else prepared_map.open_ray_calls
            parameters = {} if backend == 'daft' else dict(
                record_stats=lambda value: transcript.append(('record_stats', value)))
            with opener(CALLS, options, SESSION, **parameters) as stream:
                values = [next(stream)] if partial else list(stream)
            return values, transcript, [] if timing is None else timing.writer.events

    def test_native_graphs_on_off_preserve_calls_results_settings_and_close_order(self):
        for backend in ('daft', 'ray-data'):
            with self.subTest(backend=backend):
                off, off_calls, absent = self.consume(backend, enabled=False)
                on, on_calls, events = self.consume(backend, enabled=True)
                self.assertEqual((on, on_calls), (off, off_calls))
                self.assertEqual(absent, [])
                self.assertEqual([value[2] for value in on], [b'complete response'] * 2)
                self.assertIn(('iterator.close',), on_calls)
                opened = [e for e in events if e['operation'] == 'native.iterator_open']
                self.assertTrue(all('actor readiness unavailable' in e['scope'] for e in opened))
                consumed = [e for e in events if e['operation'] == 'native.consume']
                self.assertTrue(all('consumer yield wait' in e['scope'] for e in consumed))
                self.assertTrue(all(e['query_id'] == 'fixture-query' and e['owner_id'] == 'fixture-owner'
                                    for e in events))

    def test_native_partial_consumption_preserves_close_and_cancelled_span(self):
        for backend in ('daft', 'ray-data'):
            with self.subTest(backend=backend):
                off, off_calls, _ = self.consume(backend, enabled=False, partial=True)
                on, on_calls, events = self.consume(backend, enabled=True, partial=True)
                self.assertEqual((on, on_calls), (off, off_calls))
                ended = [e for e in events if e['operation'] == 'native.consume' and e['event'] == 'span_ended']
                self.assertEqual(ended[0]['status'], 'cancelled')
                self.assertEqual(on_calls.count(('iterator.close',)), 1)

    def test_native_iteration_error_survives_failed_timing_and_still_closes(self):
        for backend in ('daft', 'ray-data'):
            with self.subTest(backend=backend), observations(True, writer=CleanupFailureWriter), \
                    native_graph(backend, fail=True) as transcript:
                opener = prepared_map.open_daft_calls if backend == 'daft' else prepared_map.open_ray_calls
                with self.assertRaisesRegex(ValueError, 'fixture native iteration failed') as caught:
                    with opener(CALLS, prepared_map.NativeGraphOptions(), SESSION) as stream:
                        list(stream)
                self.assertEqual(transcript.count(('iterator.close',)), 1)
                self.assertIn('OSError', '\n'.join(caught.exception.__notes__))

    def test_stream_close_observer_failure_does_not_skip_iterator_close_or_stats(self):
        for backend in ('daft', 'ray-data'):
            with self.subTest(backend=backend), observations(True, writer=StreamCloseFailureWriter) as timing, \
                    native_graph(backend) as transcript:
                opener = prepared_map.open_daft_calls if backend == 'daft' else prepared_map.open_ray_calls
                parameters = {} if backend == 'daft' else dict(
                    record_stats=lambda value: transcript.append(('record_stats', value)))
                with self.assertRaisesRegex(OSError, 'stream-close timing failed'):
                    with opener(CALLS, prepared_map.NativeGraphOptions(), SESSION, **parameters) as stream:
                        self.assertEqual(len(list(stream)), 2)
                self.assertEqual(transcript.count(('iterator.close',)), 1)
                if backend == 'ray-data':
                    self.assertEqual(transcript[-3:], [('iterator.close',), ('stats',),
                                                      ('record_stats', 'fixture stats')])
                completed = [e for e in timing.writer.events if e['operation'] == 'native.consume'
                             and e['event'] == 'span_ended']
                self.assertEqual(completed[0]['status'], 'completed')

    def test_consume_observer_error_does_not_hide_original_iterator_close_error(self):
        for backend in ('daft', 'ray-data'):
            closing = ValueError('original native iterator close failed')
            with self.subTest(backend=backend), observations(True, writer=ConsumeEndFailureWriter), \
                    native_graph(backend, close_failure=closing) as transcript:
                opener = prepared_map.open_daft_calls if backend == 'daft' else prepared_map.open_ray_calls
                parameters = {} if backend == 'daft' else dict(
                    record_stats=lambda value: transcript.append(('record_stats', value)))
                with self.assertRaises(ValueError) as caught:
                    with opener(CALLS, prepared_map.NativeGraphOptions(), SESSION, **parameters) as stream:
                        list(stream)
                self.assertIs(caught.exception, closing)
                self.assertIn('OSError', '\n'.join(closing.__notes__))
                if backend == 'ray-data':
                    self.assertIn(('record_stats', 'fixture stats'), transcript)

    def test_imlane_modes_reuse_caller_durations_without_changing_reports(self):
        for mode in ('cpython', 'ray'):
            runs = []
            for enabled in (False, True):
                with self.subTest(mode=mode, enabled=enabled), tempfile.TemporaryDirectory() as root, \
                        observations(enabled) as timing:
                    transcript = []

                    class Delegate:
                        def __init__(self, path, _options, **settings):
                            transcript.append(('construct', settings['reuse_bridge']))
                            self.config = {'namespace': 'fixture'}
                            self.work_root = Path(root)
                            self.last_close_report = None

                        def start(self):
                            transcript.append(('start',))

                        def iter_batch(self, calls, **_settings):
                            transcript.append(('batch', tuple(c.call_id for c in calls)))
                            for call in reversed(calls):
                                yield NativeCallOutcome(call.call_id, call.row_id,
                                    FullModelResponse(429, (('X-Repeat', 'one'), ('X-Repeat', 'two')),
                                                      b'complete\x00body', 'HTTP/1.1'))
                            self.last_close_report = dict(clean=True, graph_consumer_stopped=True,
                                author_submission_possible=True, batch_callbacks_returned=True,
                                timing_ns={'input_preflight_ns': 17, 'consumer_wait_ns': 23})

                        def begin_close(self):
                            transcript.append(('begin_close',))
                            return False, None

                        def close(self):
                            transcript.append(('close',))

                    config = imlane_calls.IMLaneCallConfig(mode, str(Path(root)/'bridge.json'),
                        123, 456, str(Path(root)/'author.json'))
                    calls = tuple(NativeCall('c'+str(i), i, b'{}', 'http://127.0.0.1/fixture', (), 1)
                                  for i in range(2))
                    with patch.object(imlane_calls, 'SemaIMLaneExecutor', Delegate), patch.object(
                            imlane_calls, 'verify_coordinator', return_value={'runtime': mode}):
                        executor = imlane_calls.IMLaneCallExecutor(config)
                        executor.start()
                        values = list(executor.iter_batch(calls))
                        executor.close()
                    runs.append((values, executor.last_close_report, transcript))
                    if timing is not None:
                        retained = [e for e in timing.writer.events if e['operation'] == 'imlane.caller_stage']
                        self.assertEqual({e['source_event']: e['elapsed_ns'] for e in retained},
                                         executor.last_close_report['timing_ns'])
                        self.assertTrue(all(e['event'] == 'mark' and e['kind'] == mode for e in retained))
                        self.assertTrue(all('overlaps' in e['scope'] for e in retained))
                        batch = next(e for e in timing.writer.events
                                     if e['operation'] == 'imlane.batch' and e['event'] == 'span_started')
                        self.assertTrue(all(e['parent_span_id'] == batch['span_id'] for e in retained))
            self.assertEqual(runs[0], runs[1])


if __name__ == '__main__':
    unittest.main()
