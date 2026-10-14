"""Run the repository Ray adapter against deterministic vendor API doubles."""
import asyncio
from contextlib import contextmanager
import itertools
import sys
import threading
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import native_calls as native
from .test_native_call_detailed_timing import observations
from .test_native_dataset_configuration import calls, ray_graph


@contextmanager
def fault_graph(records, *, execution=None, closing=None, shutdown=None,
                iterable=True, opening=None, shutdown_failure_call=None):
    records['actions'] = []
    records['iterations'] = 0
    records['shutdown_started'] = threading.Event()
    with ray_graph(records):
        module = sys.modules['ray.data.llm']
        build = module.build_processor

        def processor(*args, **kwargs):
            original = build(*args, **kwargs)

            class Processor:
                stages = original.stages

                def __call__(self, dataset):
                    result = original(dataset)
                    rows = result.iter_rows

                    def stop(*, force):
                        records['actions'].append('executor.shutdown')
                        if not force:
                            raise AssertionError('shutdown must request force=True')
                        count = records['actions'].count('executor.shutdown')
                        records['shutdown_started'].set()
                        if shutdown is not None and shutdown_failure_call in (None, count):
                            raise shutdown
                    result._current_executor.shutdown.side_effect = stop

                    class Iterator:
                        def __init__(self):
                            self.original, self.index = iter(rows()), 0

                        def __iter__(self):
                            return self

                        def __next__(self):
                            if self.index == 1 and execution is not None:
                                raise execution
                            self.index += 1
                            return next(self.original)

                        def close(self):
                            records['actions'].append('iterator.close')
                            self.original.close()
                            if closing is not None:
                                raise closing

                    class Iterable:
                        def __iter__(self):
                            records['iterations'] += 1
                            if opening == 'iter':
                                raise ValueError('fixture iterable open failed')
                            return Iterator()

                    def open_rows():
                        if opening == 'iter_rows':
                            raise ValueError('fixture iter_rows failed')
                        return Iterable() if iterable else Iterator()
                    result.iter_rows = open_rows
                    return result

            return Processor()

        with patch.object(module, 'build_processor', side_effect=processor):
            yield


def graph_rows(control=None):
    return native._ray_rows(calls()[:2], native.NativeGraphOptions(), control or native._GraphControl())


class NativeRayCleanupParityTests(unittest.TestCase):
    def test_execution_close_shutdown_fault_matrix_on_actual_repository_function(self):
        for iterable, execute, close, shutdown in itertools.product((False, True), repeat=4):
            samples = []
            for enabled in (False, True):
                errors = (ValueError('fixture execution') if execute else None,
                          OSError('fixture iterator close') if close else None,
                          RuntimeError('fixture executor shutdown') if shutdown else None)
                expected = next((error for error in errors if error is not None), None)
                with self.subTest(iterable=iterable, execute=execute, close=close,
                                  shutdown=shutdown, enabled=enabled), observations(enabled), \
                        fault_graph(records := {}, execution=errors[0], closing=errors[1],
                                    shutdown=errors[2], iterable=iterable):
                    caught = None
                    try:
                        values = list(graph_rows())
                    except BaseException as error:
                        caught = error
                    samples.append((type(caught), tuple(getattr(caught, '__notes__', ())), records['actions']))
                    self.assertIs(caught, expected)
                    self.assertEqual(records['actions'], ['iterator.close', 'executor.shutdown'])
                    self.assertEqual(records['iterations'], int(iterable))
                    if expected is None:
                        self.assertEqual(len(values), 2)
                    else:
                        for error in errors:
                            if error is not None and error is not expected:
                                self.assertIn(type(error).__name__, '\n'.join(getattr(expected, '__notes__', ())))
            with self.subTest(iterable=iterable, execute=execute, close=close, shutdown=shutdown, parity=True):
                self.assertEqual(samples[0], samples[1])

    def test_early_close_surfaces_real_cleanup_failures_and_attempts_both_actions(self):
        for enabled, close, shutdown in itertools.product((False, True), repeat=3):
            closing = OSError('fixture close') if close else None
            stopping = RuntimeError('fixture shutdown') if shutdown else None
            with self.subTest(enabled=enabled, close=close, shutdown=shutdown), observations(enabled), \
                    fault_graph(records := {}, closing=closing, shutdown=stopping):
                iterator = graph_rows()
                next(iterator)
                caught = None
                try:
                    iterator.close()
                except BaseException as error:
                    caught = error
                self.assertIs(caught, closing or stopping)
                self.assertEqual(records['actions'], ['iterator.close', 'executor.shutdown'])
                if close and shutdown:
                    self.assertIn('RuntimeError', '\n'.join(getattr(closing, '__notes__', ())))

    def test_thrown_cancellation_remains_primary_with_both_cleanup_failures_noted(self):
        for enabled, category in itertools.product((False, True),
                                                   (asyncio.CancelledError, InterruptedError, KeyboardInterrupt)):
            cancelled = category('fixture cancellation')
            with self.subTest(enabled=enabled, category=category), observations(enabled), \
                    fault_graph(records := {}, closing=OSError('fixture close'),
                                shutdown=RuntimeError('fixture shutdown')):
                iterator = graph_rows()
                next(iterator)
                with self.assertRaises(category) as caught:
                    iterator.throw(cancelled)
                self.assertIs(caught.exception, cancelled)
                self.assertEqual(records['actions'], ['iterator.close', 'executor.shutdown'])
                self.assertIn('OSError', '\n'.join(getattr(cancelled, '__notes__', ())))
                self.assertIn('RuntimeError', '\n'.join(getattr(cancelled, '__notes__', ())))

    def test_stop_during_iteration_and_before_start_keep_cleanup_ownership(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled), observations(enabled), fault_graph(records := {}):
                control = native._GraphControl()
                iterator = graph_rows(control)
                next(iterator)
                control.stopped.set()
                self.assertEqual(list(iterator), [])
                self.assertEqual(records['actions'], ['iterator.close', 'executor.shutdown'])
                self.assertEqual(list(graph_rows(control)), [])
                self.assertEqual(records['actions'], ['iterator.close', 'executor.shutdown'])

    def test_open_failure_still_shuts_down_acquired_executor(self):
        for enabled, opening in itertools.product((False, True), ('iter_rows', 'iter')):
            with self.subTest(enabled=enabled, opening=opening), observations(enabled), \
                    fault_graph(records := {}, opening=opening, shutdown=RuntimeError('fixture shutdown')):
                with self.assertRaises(ValueError) as caught:
                    list(graph_rows())
                self.assertEqual(records['actions'], ['executor.shutdown'])
                self.assertIn('RuntimeError', '\n'.join(getattr(caught.exception, '__notes__', ())))

    def test_writer_failure_never_masks_execution_or_real_cleanup_failure(self):
        operations = ('native_call.consume', 'native_call.graph_cleanup')
        for operation, event, execute in itertools.product(operations, ('span_started', 'span_ended'), (False, True)):
            primary = ValueError('fixture execution') if execute else None
            closing = OSError('fixture close')
            with self.subTest(operation=operation, event=event, execute=execute), \
                    observations(True, fail_operation=operation, fail_event=event), \
                    fault_graph(records := {}, execution=primary, closing=closing,
                                shutdown=RuntimeError('fixture shutdown')):
                with self.assertRaises(type(primary or closing)) as caught:
                    list(graph_rows())
                self.assertIs(caught.exception, primary or closing)
                notes = '\n'.join(getattr(caught.exception, '__notes__', ()))
                self.assertIn('RuntimeError', notes)
                self.assertIn('Detailed timing', notes)
                self.assertEqual(records['actions'], ['iterator.close', 'executor.shutdown'])

    def test_writer_failure_at_graph_acquisition_keeps_real_shutdown_failure(self):
        for operation in ('native_call.graph_build', 'native_call.iterator_open'):
            shutdown = RuntimeError('fixture shutdown')
            with self.subTest(operation=operation), observations(True, fail_operation=operation), \
                    fault_graph(records := {}, shutdown=shutdown):
                with self.assertRaises(RuntimeError) as caught:
                    list(graph_rows())
                self.assertIs(caught.exception, shutdown)
                self.assertIn('Detailed timing', '\n'.join(getattr(shutdown, '__notes__', ())))
                expected = [] if operation == 'native_call.graph_build' else ['iterator.close']
                self.assertEqual(records['actions'], expected + ['executor.shutdown'])

    def test_timing_error_from_close_cannot_mask_real_shutdown_failure(self):
        closing, shutdown = OSError('fixture diagnostic close'), RuntimeError('fixture shutdown')
        closing._semloom_timing_failure = True
        with observations(True), fault_graph(records := {}, closing=closing, shutdown=shutdown):
            with self.assertRaises(RuntimeError) as caught:
                list(graph_rows())
            self.assertIs(caught.exception, shutdown)
            self.assertIn('OSError', '\n'.join(getattr(shutdown, '__notes__', ())))
            self.assertEqual(records['actions'], ['iterator.close', 'executor.shutdown'])

    def test_executor_keeps_all_result_cleanup_failure_unconfirmed_and_refuses_reuse(self):
        for enabled in (False, True):
            closing = OSError('fixture close')
            with self.subTest(enabled=enabled), observations(enabled), fault_graph(records := {}, closing=closing):
                executor = native.NativeCallExecutor('ray-data')
                with self.assertRaises(OSError) as caught:
                    list(executor.iter_batch(calls()[:2]))
                self.assertIs(caught.exception, closing)
                report = executor.last_close_report
                self.assertEqual(report['received'], report['prepared'])
                self.assertFalse(report['clean'])
                self.assertFalse(report['reuse_allowed'])
                self.assertTrue(report['unconfirmed_work'])
                self.assertTrue(report['graph_consumer_stopped'])
                self.assertEqual(report['remote_http_stopped'], 'unavailable')
                self.assertTrue(report['worker_errors'])
                with self.assertRaisesRegex(RuntimeError, 'unconfirmed work'):
                    list(executor.iter_batch(calls()[:2]))

    def test_executor_real_stop_failure_precedes_consume_writer_failure(self):
        shutdown = RuntimeError('fixture control stop')
        with observations(True, fail_operation='native_call.consume'), \
                fault_graph(records := {}, shutdown=shutdown, shutdown_failure_call=2):
            executor = native.NativeCallExecutor('ray-data')
            with self.assertRaises(RuntimeError) as caught:
                list(executor.iter_batch(calls()[:2]))
            self.assertIs(caught.exception, shutdown)
            self.assertIn('Detailed timing', '\n'.join(getattr(shutdown, '__notes__', ())))
            self.assertFalse(executor.last_close_report['clean'])
            self.assertTrue(executor.last_close_report['unconfirmed_work'])

    def test_caller_parser_error_retains_worker_close_and_shutdown_failure_notes(self):
        for enabled in (False, True):
            parser = ValueError('fixture caller parser')
            with self.subTest(enabled=enabled), observations(enabled), \
                    fault_graph(records := {}, closing=OSError('fixture close'),
                                shutdown=RuntimeError('fixture shutdown'), shutdown_failure_call=1):
                executor = native.NativeCallExecutor('ray-data')
                iterator = executor.iter_batch(calls()[:2])
                next(iterator)
                self.assertTrue(records['shutdown_started'].wait(1), 'worker did not reach EOF cleanup')
                with self.assertRaises(ValueError) as caught:
                    iterator.throw(parser)
                self.assertIs(caught.exception, parser)
                self.assertIn('OSError', '\n'.join(getattr(parser, '__notes__', ())))
                self.assertIn('RuntimeError', '\n'.join(getattr(parser, '__notes__', ())))
                self.assertFalse(executor.last_close_report['clean'])
                self.assertTrue(executor.last_close_report['unconfirmed_work'])

    def test_shared_execution_and_close_error_object_does_not_copy_its_own_notes(self):
        class SharedError(ValueError):
            def add_note(self, note):
                # A broken self-copy must fail promptly rather than exhaust memory.
                if len(getattr(self, '__notes__', ())) >= 8:
                    raise AssertionError('exception note self-copy did not terminate')
                super().add_note(note)

        for enabled in (False, True):
            shared = SharedError('fixture shared error')
            with self.subTest(enabled=enabled), observations(enabled), \
                    fault_graph(records := {}, execution=shared, closing=shared,
                                shutdown=RuntimeError('fixture shutdown')):
                with self.assertRaises(SharedError) as caught:
                    list(graph_rows())
                self.assertIs(caught.exception, shared)
                self.assertIn('RuntimeError', '\n'.join(getattr(shared, '__notes__', ())))
                self.assertEqual(records['actions'], ['iterator.close', 'executor.shutdown'])


if __name__ == '__main__':
    unittest.main()
