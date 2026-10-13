"""Actual native consumer lifecycle with a deterministic graph stand-in; no HTTP."""
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import native_calls as native


class GraphCloseError(RuntimeError):
    pass


class ParserError(ValueError):
    pass


class NativeCallCleanupTests(unittest.TestCase):
    def scenario(self, *, early=False, closing=None, primary=None):
        calls = tuple(native.NativeCall(f"cleanup:{i}", i, b"{}",
            "http://localhost/fixture", (), .1) for i in range(2))
        observed = []

        class Graph:
            def __init__(self, calls, options, control):
                self.calls, self.control, self.index = calls, control, 0

            def __iter__(self):
                return self

            def __next__(self):
                if early and self.index == 1:
                    if not self.control.stopped.wait(1):
                        raise AssertionError("consumer did not request stop")
                    raise StopIteration
                if self.index == len(self.calls):
                    raise StopIteration
                call = self.calls[self.index]
                self.index += 1
                return native.NativeCallOutcome(call.call_id, call.row_id,
                    native.FullModelResponse(200, (), b"{}"))

            def close(self):
                observed.append(self.control.stopped.is_set())
                if closing is not None:
                    raise closing

        executor = native.NativeCallExecutor("daft")
        with patch.object(native, "_daft_rows", Graph):
            iterator = executor.iter_batch(calls, timeout_s=2)
            caught = None
            try:
                if early:
                    next(iterator)
                    if primary is None:
                        iterator.close()
                    else:
                        iterator.throw(primary)
                else:
                    list(iterator)
            except BaseException as error:
                caught = error
            finally:
                iterator.close()
        return executor, caught, observed

    def test_complete_success(self):
        executor, error, observed = self.scenario()
        self.assertIsNone(error)
        self.assertEqual(observed, [False])
        self.assertTrue(executor.last_close_report["clean"])
        self.assertEqual(executor.last_cleanup_errors, ())

    def test_complete_close_failure_is_visible_and_recorded(self):
        closing = GraphCloseError("fixture close")
        executor, error, _ = self.scenario(closing=closing)
        self.assertIs(error, closing)
        self.assertEqual(executor.last_cleanup_errors, (("graph_close", closing),))
        self.assertFalse(executor.last_close_report["clean"])

    def test_early_close_failure_is_visible_and_recorded(self):
        closing = GraphCloseError("fixture close")
        executor, error, observed = self.scenario(early=True, closing=closing)
        self.assertIs(error, closing)
        self.assertEqual(observed, [True])
        self.assertEqual(executor.last_cleanup_errors, (("graph_close", closing),))
        report = executor.last_close_report
        self.assertFalse(report["clean"])
        self.assertTrue(report["graph_consumer_stopped"])
        self.assertTrue(report["worker_errors_complete"])
        self.assertEqual(report["worker_errors"][0]["stage"], "graph_close")
        self.assertEqual(report["remote_http_stopped"], "unavailable")

    def test_parser_error_keeps_first_cause_and_close_error(self):
        primary, closing = ParserError("fixture parser"), GraphCloseError("fixture close")
        executor, error, _ = self.scenario(early=True, closing=closing, primary=primary)
        self.assertIs(error, primary)
        self.assertEqual(executor.last_cleanup_errors, (("graph_close", closing),))
        self.assertTrue(any("GraphCloseError" in note for note in error.__notes__))
        self.assertFalse(executor.last_close_report["clean"])

    def test_early_close_without_failure_remains_uncertain(self):
        executor, error, _ = self.scenario(early=True)
        self.assertIsNone(error)
        self.assertFalse(executor.last_close_report["clean"])
        self.assertEqual(executor.last_close_report["remote_http_stopped"], "unavailable")

    def test_missing_result_prevents_reuse_even_after_the_consumer_thread_stops(self):
        executor, error, _ = self.scenario(early=True)
        self.assertIsNone(error)
        self.assertTrue(executor.last_close_report['unconfirmed_work'])
        self.assertFalse(executor.last_close_report['reuse_allowed'])
        calls = (native.NativeCall('later', 0, b'{}', 'http://localhost/fixture', (), 1),)
        with patch.object(native, '_daft_rows') as graph, self.assertRaisesRegex(RuntimeError, 'unconfirmed work'):
            list(executor.iter_batch(calls))
        graph.assert_not_called()

    def test_complete_result_allows_another_independent_batch(self):
        executor, error, _ = self.scenario()
        self.assertIsNone(error)
        self.assertFalse(executor.last_close_report['unconfirmed_work'])
        calls = (native.NativeCall('later', 0, b'{}', 'http://localhost/fixture', (), 1),)
        def graph(values, options, control):
            for call in values:
                yield native.NativeCallOutcome(call.call_id, call.row_id,
                    native.FullModelResponse(200, (), b'complete'))
        with patch.object(native, '_daft_rows', graph):
            self.assertEqual(len(list(executor.iter_batch(calls))), 1)
        self.assertTrue(executor.last_close_report['clean'])

    def test_cancelled_before_graph_start_does_not_prevent_later_use(self):
        executor = native.NativeCallExecutor('daft')
        calls = (native.NativeCall('later', 0, b'{}', 'http://localhost/fixture', (), 1),)
        with patch.object(native, '_daft_rows') as graph, self.assertRaises(InterruptedError):
            list(executor.iter_batch(calls, cancelled=lambda: True))
        graph.assert_not_called()
        self.assertFalse(executor.last_close_report['unconfirmed_work'])

    def test_all_results_with_failed_graph_close_still_prevent_reuse(self):
        executor, error, _ = self.scenario(closing=GraphCloseError('failed closure'))
        self.assertIsInstance(error, GraphCloseError)
        self.assertEqual(executor.last_close_report['received'], executor.last_close_report['prepared'])
        self.assertTrue(executor.last_close_report['unconfirmed_work'])
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed work'):
            list(executor.iter_batch(()))

    def test_overlapping_batch_cannot_replace_an_active_or_unconfirmed_batch(self):
        calls = tuple(native.NativeCall(str(i), i, b'{}', 'http://localhost/fixture', (), 1) for i in range(2))
        def graph(values, options, control):
            for call in values:
                yield native.NativeCallOutcome(call.call_id, call.row_id,
                    native.FullModelResponse(200, (), b'complete'))
        executor = native.NativeCallExecutor('daft')
        with patch.object(native, '_daft_rows', graph):
            first = executor.iter_batch(calls)
            self.assertEqual(next(first).call_id, '0')
            with self.assertRaisesRegex(RuntimeError, 'active batch'):
                list(executor.iter_batch(calls[:1]))
            first.close()
            self.assertTrue(executor.last_close_report['unconfirmed_work'])
            with self.assertRaisesRegex(RuntimeError, 'unconfirmed work'):
                list(executor.iter_batch(calls[:1]))

    def test_complete_transport_errors_keep_remote_work_unconfirmed(self):
        calls = (native.NativeCall('timeout', 0, b'{}', 'http://localhost/fixture', (), 1),)
        def graph(values, options, control):
            for call in values:
                yield native.NativeCallOutcome(call.call_id, call.row_id, None, 'TimeoutError: fixture')
        executor = native.NativeCallExecutor('daft')
        with patch.object(native, '_daft_rows', graph):
            self.assertEqual(len(list(executor.iter_batch(calls))), 1)
        report = executor.last_close_report
        self.assertEqual(report['received'], report['prepared'])
        self.assertTrue(report['graph_consumer_stopped'])
        self.assertFalse(report['clean'])
        self.assertTrue(report['transport_completion_uncertain'])
        self.assertTrue(report['unconfirmed_work'])
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed work'):
            list(executor.iter_batch(calls))

    def test_full_http_error_response_does_not_mark_transport_unknown(self):
        calls = (native.NativeCall('http-error', 0, b'{}', 'http://localhost/fixture', (), 1),)
        def graph(values, options, control):
            for call in values:
                yield native.NativeCallOutcome(call.call_id, call.row_id,
                    native.FullModelResponse(429, (), b'{"error":"known HTTP result"}'))
        executor = native.NativeCallExecutor('daft')
        with patch.object(native, '_daft_rows', graph):
            self.assertEqual(len(list(executor.iter_batch(calls))), 1)
            self.assertEqual(len(list(executor.iter_batch(calls))), 1)
        self.assertTrue(executor.last_close_report['clean'])
        self.assertFalse(executor.last_close_report['transport_completion_uncertain'])


if __name__ == "__main__":
    unittest.main()
