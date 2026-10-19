"""Real private CPU worker shutdown budgets and authoritative result retention."""

import importlib.util
import json
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus.records import CallLedger, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.baselines.text.daft_engine_close_fixture import (
    DelayedCloseEngineProcess, FailedCloseEngineProcess, HungCloseEngineProcess,
)


@unittest.skipUnless(importlib.util.find_spec("psutil") is not None, "requires psutil")
class PrivateEngineCloseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="engine-close-cpu-")
        self.addCleanup(self.directory.cleanup)
        self.engine = EngineSpec(model_path=self.directory.name, model_python=sys.executable,
                                 driver_python=sys.executable)
        self.generation = GenerationSpec()
        self.prepared = [dict(identity("close-cpu", "row-0", 0), messages_json=json.dumps([
            dict(role="user", content="Synthetic CPU shutdown fixture")]))]

    def start(self, kernel=DelayedCloseEngineProcess):
        client = kernel(self.engine, self.generation, self.directory.name,
                        time.monotonic() + 45, 5)
        self.addCleanup(client.close)
        return client

    def evidence(self, case, client, report, elapsed):
        value = dict(case=case, elapsed_s=elapsed, status=report["status"],
                     process_exit_confirmed=report["process_exit_confirmed"],
                     observed_processes=report.get("observed_processes"),
                     live_observed_processes=report.get("live_observed_processes"),
                     close_budget_s=report.get("close_budget_s"),
                     engine_api_rpc_budget_s=report.get("engine_api_rpc_budget_s"),
                     process_cleanup_reserved_s=report.get("process_cleanup_reserved_s"),
                     gpu_exit_confirmed=report["gpu_exit_confirmed"],
                     cleanup_errors=report["cleanup_errors"], engine_api=report.get("engine_api"),
                     child_returncode=client.process.poll())
        print("CLOSE_CPU_EVIDENCE " + json.dumps(value, sort_keys=True), flush=True)

    def test_default_close_waits_for_api_longer_than_previous_rpc_budget(self):
        client = self.start()
        results = client.run_prepared(self.prepared)
        started = time.monotonic()
        report = client.close()
        elapsed = time.monotonic() - started
        self.evidence("delayed-default-close", client, report, elapsed)
        self.assertEqual(report["status"], "closed")
        self.assertGreater(report["engine_api"]["close_delay_s"], 2.5)
        self.assertTrue(report["engine_api"]["engine_api_shutdown_confirmed"])
        self.assertTrue(report["process_exit_confirmed"])
        self.assertEqual(report["live_observed_processes"], 0)
        self.assertIsNone(report["gpu_exit_confirmed"])
        self.assertLess(elapsed, 15.5)
        self.assertEqual(results[0]["prompt_tokens"], 7)
        self.assertEqual(client.close(), report)

    def test_explicit_short_close_remains_bounded_and_unknown(self):
        client = self.start()
        results = client.run_prepared(self.prepared)
        started = time.monotonic()
        report = client.close(timeout=0.9)
        elapsed = time.monotonic() - started
        self.evidence("delayed-short-close", client, report, elapsed)
        self.assertEqual(report["status"], "unknown")
        self.assertIsNone(report["engine_api"])
        self.assertTrue(report["process_exit_confirmed"])
        self.assertEqual(report["live_observed_processes"], 0)
        self.assertIn("engine_close: TimeoutError", report["cleanup_errors"])
        self.assertLess(elapsed, 1.5)
        self.assertEqual(results[0]["total_tokens"], 9)
        self.assertEqual(client.close(), report)

    def test_hung_api_is_killed_within_total_close_budget(self):
        client = self.start(HungCloseEngineProcess)
        started = time.monotonic()
        report = client.close(timeout=0.9)
        elapsed = time.monotonic() - started
        self.evidence("hung-short-close", client, report, elapsed)
        self.assertEqual(report["status"], "unknown")
        self.assertIsNone(report["engine_api"])
        self.assertTrue(report["process_exit_confirmed"])
        self.assertEqual(report["live_observed_processes"], 0)
        self.assertEqual(client.process.returncode, -9)
        self.assertLess(elapsed, 1.5)
        self.assertEqual(client.close(), report)

    def test_failed_shutdown_preserves_failed_result_error_and_known_usage(self):
        client = self.start(FailedCloseEngineProcess)
        result = client.run_prepared(self.prepared)
        ledger = CallLedger([identity("close-cpu", "row-0", 0)], max_calls=1)
        ledger.record(result[0])
        started = time.monotonic()
        report = client.close()
        self.evidence("failed-close-known-usage", client, report, time.monotonic() - started)
        self.assertEqual(report["status"], "unknown")
        self.assertEqual(report["engine_api"]["error_type"], "FixtureCloseError")
        self.assertTrue(report["process_exit_confirmed"])
        self.assertEqual(result[0]["error_type"], "FixtureInferenceError")
        self.assertEqual(result[0]["status"], "failed")
        usage = ledger.report()["usage"]
        self.assertEqual((usage["prompt_tokens"], usage["completion_tokens"],
                          usage["total_tokens"]), (7, 2, 9))
        self.assertEqual(usage["known_prompt_tokens"], 7)
        self.assertEqual(usage["unknown_prompt_tokens_calls"], 0)

    def test_close_rejects_unbounded_or_invalid_timeout_before_cleanup(self):
        client = self.start(FailedCloseEngineProcess)
        with patch.object(client, "_close") as close:
            for value in (True, False, 0, -1, float("inf"), float("nan"), "1", None):
                with self.subTest(value=value), self.assertRaisesRegex(ValueError, "timeout"):
                    client.close(timeout=value)
            close.assert_not_called()
        self.assertIsNone(client.process.poll())


if __name__ == "__main__":
    unittest.main()
