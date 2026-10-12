"""The verifier reports missing libraries and rejects incomplete HTTP evidence."""
from pathlib import Path
import os
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock

from integrations.sema import verify_native as verifier
from src.baselines.text.frameworks.native_calls import NativeCallOutcome
from src.execution_provider.adapters.full_response import FullModelResponse

check_outcomes, main = verifier.check_outcomes, verifier.main

class SemaNativeVerifierTests(unittest.TestCase):
    def response(self, headers=(("X-Repeated", "one"), ("X-Repeated", "two"))):
        return FullModelResponse(429, headers, b"not-json")

    def test_equal_bodies_still_require_every_distinct_occurrence(self):
        first = NativeCallOutcome("one", 0, self.response())
        second = NativeCallOutcome("two", 1, self.response())
        check_outcomes((second, first), {("one", 0), ("two", 1)}, 429, b"not-json")
        with self.assertRaisesRegex(AssertionError, "occurrence"):
            check_outcomes((first, first), {("one", 0), ("two", 1)}, 429, b"not-json")

    def test_transport_error_or_lost_duplicate_header_is_not_a_complete_response(self):
        for outcome in (NativeCallOutcome("one", 0, None, "TimeoutError"),
                        NativeCallOutcome("one", 0, self.response((("X-Repeated", "two"),)))):
            with self.subTest(outcome=outcome), self.assertRaises(AssertionError):
                check_outcomes((outcome,), {("one", 0)}, 429, b"not-json")

    def test_missing_library_is_saved_as_failure_with_zero_tests(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "checks"
            with mock.patch.dict(os.environ, {}), \
                 mock.patch("integrations.sema.verify_native.os.sched_setaffinity", create=True), \
                 mock.patch("integrations.sema.verify_native.library_identity", side_effect=ModuleNotFoundError("daft")), \
                 mock.patch("builtins.print"):
                self.assertEqual(main(["--backend", "daft", "--artifact-root", str(root)]), 1)
            summary = json.loads((root / "summary.json").read_text())
            self.assertFalse(summary["passed"])
            self.assertEqual(summary["tests_run"], 0)
            self.assertEqual(summary["real_model_requests"], 0)
            self.assertIn("ModuleNotFoundError", summary["startup_error"])

    def test_an_existing_ray_runtime_is_rejected_without_shutdown(self):
        ray = SimpleNamespace(is_initialized=lambda: True, shutdown=mock.Mock())
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.dict(os.environ, {}), mock.patch.dict("sys.modules", {"ray": ray}), \
                 mock.patch("integrations.sema.verify_native.os.sched_setaffinity", create=True), \
                 mock.patch("integrations.sema.verify_native.library_identity", return_value={}), \
                 mock.patch("builtins.print"):
                result = main(["--backend", "ray-data", "--artifact-root", str(Path(directory) / "checks"),
                               "--ray-temp", "/tmp/sema-ray-check"])
            self.assertEqual(result, 1)
            ray.shutdown.assert_not_called()

    def test_normal_delivery_requires_clean_graph_eof_after_thread_stop(self):
        checks = verifier.NativeSemaChecks("test_service_sequential_supply_does_not_wait_for_query_end")
        checks.observations = []
        report = {"clean": False, "graph_consumer_stopped": True}
        summary = dict(service_thread_alive=False, io_loop_closed=True,
            native_bridge_thread_alive=False, held_requests=0, http_handlers_remaining=0,
            cleanup_errors=[], native_close_reports=[report], uncertain_requests=0)
        with self.assertRaises(AssertionError):
            checks.service_settled(SimpleNamespace(summary=summary), normal=True)
        report["clean"] = True
        checks.service_settled(SimpleNamespace(summary=summary), normal=True)


if __name__ == "__main__":
    unittest.main()
