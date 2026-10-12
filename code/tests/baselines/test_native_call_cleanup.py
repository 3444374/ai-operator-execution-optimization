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


if __name__ == "__main__":
    unittest.main()
