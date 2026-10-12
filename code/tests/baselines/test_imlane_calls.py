"""Local subprocess protocol checks; these are not real author IMLane execution."""
import asyncio
from dataclasses import replace
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig, IMLaneCallExecutor
from src.baselines.text.frameworks.native_calls import NativeCall, _call_wire
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
import tests.execution_provider.test_sema_imlane as old_tests


class IMLaneCallProtocolTests(unittest.TestCase):
    def setUp(self):
        self.fixture = old_tests.SemaIMLaneBridgeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        from src.baselines.text.frameworks.imlane_calls import verify_coordinator
        self.original_verify = verify_coordinator
        self.probe = mock.patch("src.baselines.text.frameworks.imlane_calls.verify_coordinator",
            return_value=dict(runtime="cpython", actual_coordinator_executors=2,
                              evidence="synthetic protocol only"))
        self.verify = self.probe.start()
        self.addCleanup(self.probe.stop)

    def executor(self, *, runtime="cpython", max_batch_rows=128, max_batch_bytes=32 * 1024**2, **changes):
        original = self.fixture.executor(**changes)
        config = IMLaneCallConfig(runtime, str(original.config_path), 123, 456,
                                   str(self.fixture.root/"author-build.json"))
        value = IMLaneCallExecutor(config, NativeGraphOptions(concurrency=4),
                                   max_batch_rows=max_batch_rows, max_batch_bytes=max_batch_bytes)
        self.addCleanup(value.close)
        return value

    def test_reuses_full_response_protocol_and_duplicate_source_rows(self):
        executor = self.executor(runtime="ray")
        self.fixture.server.status = 429
        self.fixture.server.headers += [("X-Repeat", "third")]
        calls = [replace(self.fixture.call(i), row_id=5) for i in range(2)]
        values = list(executor.iter_batch(calls))
        self.assertEqual([v.call_id for v in values], ["occurrence-1", "occurrence-0"])
        self.assertEqual([v.row_id for v in values], [5, 5])
        self.assertEqual(self.fixture.server.calls, [self.fixture.raw] * 2)
        self.assertEqual(values[0].response.status_code, 429)
        self.assertEqual(values[0].response.body, self.fixture.server.body)
        self.assertEqual([v for k, v in values[0].response.headers if k == "X-Repeat"],
                         ["one", "two", "third"])
        self.assertTrue(executor.last_close_report["clean"])
        self.assertEqual(executor.last_close_report["runtime"], "ray")
        self.assertFalse(executor.last_close_report["native_settings_are_physical_executor_count"])

    def test_pre_dispatch_cancel_and_empty_do_not_contact_author_or_http(self):
        executor = self.executor()
        with self.assertRaises(InterruptedError):
            list(executor.iter_batch([self.fixture.call()], cancelled=lambda: True))
        self.assertFalse(executor.last_close_report["author_dispatch_started"])
        self.assertEqual(list(executor.iter_batch([])), [])
        self.assertTrue(executor.last_close_report["clean"])
        self.verify.assert_not_called()
        self.assertEqual(self.fixture.server.calls, [])

    def test_effective_snapshot_limits_reject_without_split_or_fallback(self):
        executor = self.executor(max_batch_rows=128)
        self.assertEqual(executor.max_batch_rows, 128)
        with self.assertRaises(ValueError):
            list(executor.iter_batch([self.fixture.call(i) for i in range(129)]))
        executor = self.executor(max_batch_bytes=10)
        with self.assertRaises(ValueError):
            list(executor.iter_batch([self.fixture.call()]))
        self.verify.assert_not_called()
        self.assertEqual(self.fixture.server.calls, [])

    def test_explicit_total_row_capacity_is_shared_with_the_delegate(self):
        executor=self.executor(max_batch_rows=2000)
        self.assertEqual(executor.max_batch_rows,2000)
        self.assertIs(executor.delegate.limits,executor.limits)
        self.assertEqual(executor.limits.max_pending_calls,128)
        with self.assertRaises(ValueError):
            list(executor.iter_batch([self.fixture.call(i) for i in range(2001)]))
        self.verify.assert_not_called()
        self.assertEqual(self.fixture.server.calls,[])

    def test_uncertain_occurrence_persists_across_new_executor_objects(self):
        executor = self.executor(mutation='wire["row_id"]=999')
        with self.assertRaises(ValueError):
            list(executor.iter_batch([self.fixture.call()]))
        self.assertTrue(executor.last_close_report["external_coordinator_requires_cleanup"])
        self.assertTrue(executor._uncertain.is_file())
        another = IMLaneCallExecutor(executor.config, executor.options)
        self.addCleanup(another.close)
        with self.assertRaisesRegex(RuntimeError, "unsettled"):
            list(another.iter_batch([self.fixture.call(1)]))
        self.assertEqual(len(self.fixture.server.calls), 1)

    def test_consumer_close_records_driver_stop_and_unknown_author_cleanup(self):
        executor = self.executor(pause=True)
        iterator = executor.iter_batch([self.fixture.call(0), self.fixture.call(1)])
        self.assertEqual(next(iterator).call_id, "occurrence-1")
        iterator.close()
        report = executor.last_close_report
        self.assertFalse(report["clean"])
        self.assertTrue(report["graph_consumer_stopped"])
        self.assertFalse(report["external_coordinator_stopped"])
        self.assertEqual(report["remote_http_stopped"], "unavailable")

    def test_final_response_does_not_hide_nonzero_driver_exit(self):
        executor = self.executor(late_failure=True)
        iterator = executor.iter_batch([self.fixture.call()])
        self.assertEqual(next(iterator).response.status_code, 200)
        with self.assertRaises(RuntimeError):
            next(iterator)
        self.assertEqual(executor.last_close_report["returncode"], 7)
        self.assertFalse(executor.last_close_report["author_batches_drained"])

    def test_pid_reuse_and_mode_difference_reject_before_dispatch(self):
        import src.baselines.text.frameworks.imlane_calls as adapter
        config = self.executor().config
        with mock.patch.object(adapter, "process_start_ticks", return_value=999):
            with self.assertRaisesRegex(ValueError, "PID was reused"):
                self.original_verify(config, {"gateway_port": 2000, "namespace": "fixture"})
        with mock.patch.object(adapter, "process_start_ticks", return_value=456), mock.patch.object(
                Path, "read_bytes", return_value=b"\0".join([b"imlane", b"--runtime", b"ray", b"--gateway_port", b"2000", b"--n_executors", b"2", b""])):
            with self.assertRaisesRegex(ValueError, "mode"):
                self.original_verify(config, {"gateway_port": 2000, "namespace": "fixture"})


@unittest.skipUnless(importlib.util.find_spec("pyarrow"), "requires real Arrow")
class IMLaneThinKernelTests(unittest.TestCase):
    def test_original_loop_is_restored_after_original_kernel_success_or_http_failure(self):
        import pyarrow as pa
        from integrations.sema import imlane_complete_call as kernel
        call = NativeCall("occurrence", 0, b"{}", "http://localhost/fixture", (), 1)
        table = pa.table({"call_id": [call.call_id], "row_id": [call.row_id],
                          "native_call": [_call_wire(call)]})
        loop = asyncio.new_event_loop()
        try:
            for failure in (False, True):
                asyncio.set_event_loop(loop)
                def work(value):
                    async def invoke():
                        if failure:
                            raise ValueError("response exceeds its byte allowance")
                        return value
                    return asyncio.run(invoke())
                with mock.patch.object(kernel, "_original_complete_http", side_effect=work):
                    result = kernel.complete_http(table)
                self.assertIs(asyncio.get_event_loop(), loop)
                self.assertEqual(result.num_rows, 1)
                self.assertFalse(loop.is_closed())
                if failure:
                    self.assertIn("ValueError", result["native_outcome"][0].as_py())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


class IMLaneFixtureEvidenceTests(unittest.TestCase):
    def test_fixture_failure_log_redacts_before_writing_and_refuses_overwrite(self):
        from integrations.sema.verify_imlane_calls import write_log
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"failure.log"
            secret = "test" + "-value"
            write_log(path, "password=" + secret + "\nfixture failure")
            self.assertEqual(path.read_text(), "password=***\nfixture failure")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                write_log(path, "replacement")


if __name__ == "__main__":
    unittest.main()
