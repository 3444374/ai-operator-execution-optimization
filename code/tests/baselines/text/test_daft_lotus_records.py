"""Real Daft process-mode wiring and private CPU engine lifecycle checks."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import daft_lotus_records as native
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.baselines.text.daft_engine_fixture import (
    CrashEngineProcess, FailCloseEngineProcess, FixedEngineProcess, StallEngineProcess,
    StartupStallEngineProcess,
)
from tests.experiments.test_lotus_records_movie import fixture_source


LIBRARIES = all(importlib.util.find_spec(name) is not None for name in ("daft", "lotus", "psutil"))


@unittest.skipUnless(LIBRARIES, "requires isolated pinned Daft and LOTUS CPU libraries")
class DaftRecordsTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory(prefix="daft-records-tests-")
        self.addCleanup(self.root.cleanup)
        self.source = fixture_source(Path(self.root.name))
        self.method = FilterMethod("openai/local-fixture")
        self.engine = EngineSpec(model_path=self.root.name, model_python=sys.executable,
                                 driver_python=sys.executable)
        self.generation = GenerationSpec()
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                                          query_timeout_s=60, startup_timeout_s=20)
        self.query_id = "daft-cpu-q3"

    def rows(self, kernel=FixedEngineProcess, *, cancelled=None, timeout=60):
        deadline = time.monotonic() + timeout
        control = tempfile.mkdtemp(prefix="daft-records-control-")
        self.addCleanup(shutil.rmtree, control, ignore_errors=True)
        expected = self.source.expected(self.query_id)
        frame = native._graph(self.source, self.method, self.engine, self.generation, self.resources,
                              self.query_id, expected, deadline, control,
                              engine_type=kernel, model_gpus=0)
        return native._DaftRows(frame, expected, deadline, cancelled, control)

    def test_process_graph_real_formatter_identity_usage_and_single_owned_engine(self):
        rows = self.rows()
        result = list(rows)
        self.assertEqual(len(result), 3)
        self.assertEqual({row["call_id"] for row in result},
                         {row["call_id"] for row in self.source.expected(self.query_id)})
        ledger = CallLedger(self.source.expected(self.query_id), max_calls=3)
        for row in result:
            ledger.record(row)
            self.assertNotEqual(row["prepare_worker_pid"], os.getpid())
            self.assertNotEqual(row["model_fixture_pid"], row["prepare_worker_pid"])
            self.assertNotIn("scoreSentiment", row)
        self.assertEqual(ledger.report()["usage"]["prompt_tokens"], 36)
        self.assertEqual(len({row["engine_uid"] for row in result}), 1)
        report = rows.last_close_report
        self.assertEqual(report["status"], "closed")
        self.assertTrue(report["engine_reports"][0]["process_exit_confirmed"])
        self.assertEqual(report["engine_reports"][0]["live_observed_processes"], 0)
        self.assertIsNone(report["gpu_exit_confirmed"])

    def test_actual_method_and_original_evaluator(self):
        if not os.environ.get("SEMBENCH_CHECKOUT"):
            self.skipTest("fixed original SemBench checkout required")
        from src.experiments.lotus_records_movie import run_movie_query

        report = run_movie_query(self.source, os.environ["SEMBENCH_CHECKOUT"], self.method,
                                 self.generation, self.rows, query_id=self.query_id)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["result"], {"positive_review_cnt": 1})
        self.assertEqual(report["quality"]["classification_audit"]["true_positive"], 1)
        self.assertEqual(report["quality"]["classification_audit"]["true_negative"], 2)

    def test_validation_precedes_graph_allocation(self):
        with patch.object(native, "_graph") as graph, \
             patch.object(EngineSpec, "validate_driver_environment", return_value={}), \
             patch.object(self.source, "preflight", side_effect=ValueError("bad source")):
            with self.assertRaisesRegex(ValueError, "bad source"):
                native.evaluate(self.source, self.method, self.engine, self.generation,
                                self.resources, query_id=self.query_id)
            graph.assert_not_called()
        with patch.object(native, "_graph") as graph, \
             patch.object(EngineSpec, "validate_driver_environment", return_value={}):
            with self.assertRaisesRegex(ValueError, "max_tokens"):
                native.evaluate(self.source, self.method, self.engine,
                                GenerationSpec(max_tokens=self.engine.max_model_len + 1),
                                self.resources, query_id=self.query_id)
            graph.assert_not_called()

    def test_cancel_before_allocation(self):
        with patch.object(native, "_graph") as graph, \
             patch.object(EngineSpec, "validate_driver_environment", return_value={}):
            with self.assertRaises(InterruptedError):
                native.evaluate(self.source, self.method, self.engine, self.generation,
                                self.resources, query_id=self.query_id, cancelled=lambda: True)
            graph.assert_not_called()

    def test_failed_cleanup_keeps_all_known_usage(self):
        rows = self.rows(FailCloseEngineProcess)
        ledger = CallLedger(self.source.expected(self.query_id), max_calls=3)
        with self.assertRaisesRegex(RuntimeError, "cleanup"):
            for row in rows:
                ledger.record(row)
        self.assertEqual(ledger.report()["counts"]["succeeded"], 3)
        self.assertEqual(ledger.report()["usage"]["prompt_tokens"], 36)
        self.assertEqual(rows.last_close_report["status"], "unknown")
        self.assertTrue(rows.last_close_report["engine_reports"][0]["process_exit_confirmed"])

    def test_native_graph_cancel_stalled_model_preserves_cancel_and_os_exit(self):
        cancelled = threading.Event()
        rows = self.rows(StallEngineProcess, cancelled=cancelled.is_set)
        def cancel_after_allocation():
            until = time.monotonic() + 40
            while time.monotonic() < until and not list(rows.control.glob("engine-*.json")):
                time.sleep(0.02)
            time.sleep(0.2)
            cancelled.set()
        timer = threading.Thread(target=cancel_after_allocation, daemon=True)
        timer.start()
        self.addCleanup(timer.join, 1)
        ledger = CallLedger(self.source.expected(self.query_id), max_calls=3)
        with self.assertRaises(InterruptedError):
            for row in rows:
                ledger.record(row)
        ledger.mark_unconfirmed("InterruptedError")
        self.assertEqual(ledger.report()["counts"]["recorded"], 3)
        self.assertGreater(ledger.report()["counts"]["unknown"], 0)
        self.assertIsNone(ledger.report()["usage"]["prompt_tokens"])
        self.assertTrue(rows.last_close_report["engine_reports"][0]["process_exit_confirmed"])

    def test_native_iterator_early_close_and_repeat_query_use_distinct_engines(self):
        rows = self.rows()
        first = next(rows)
        report = rows.close()
        self.assertTrue(report["engine_reports"][0]["process_exit_confirmed"])
        again = self.rows()
        result = list(again)
        self.assertNotEqual(first["engine_uid"], result[0]["engine_uid"])


class PrivateEngineTests(unittest.TestCase):
    def setUp(self):
        if importlib.util.find_spec("psutil") is None:
            self.skipTest("requires psutil")
        self.root = tempfile.TemporaryDirectory(prefix="daft-engine-tests-")
        self.addCleanup(self.root.cleanup)
        self.engine = EngineSpec(model_path=self.root.name, model_python=sys.executable,
                                 driver_python=sys.executable)
        self.generation = GenerationSpec()
        self.prepared = [dict(identity("q", "row-0", 0), messages_json=json.dumps([
            dict(role="user", content="CPU fixture only")]))]

    def start(self, kernel=FixedEngineProcess, timeout=10, startup=5):
        client = kernel(self.engine, self.generation, self.root.name,
                        time.monotonic() + timeout, startup)
        self.addCleanup(client.close)
        return client

    def test_private_process_reuses_engine_and_closes_idempotently(self):
        client = self.start()
        first = client.run_prepared(self.prepared)
        second = client.run_prepared(self.prepared)
        self.assertEqual(first[0]["engine_uid"], second[0]["engine_uid"])
        report = client.close()
        self.assertEqual(report["status"], "closed")
        self.assertTrue(report["engine_api"]["engine_api_shutdown_confirmed"])
        self.assertEqual(client.close(), report)

    def test_stalled_call_cancel_is_unknown_and_child_exits(self):
        client = self.start(StallEngineProcess)
        timer = threading.Timer(0.2, lambda: (Path(self.root.name) / "stop").touch())
        timer.start()
        self.addCleanup(timer.join)
        started = time.monotonic()
        result = client.run_prepared(self.prepared)
        self.assertLess(time.monotonic() - started, 7)
        self.assertEqual(result[0]["status"], "unknown")
        self.assertIsNone(result[0]["prompt_tokens"])
        self.assertEqual(client.last_close_report["live_observed_processes"], 0)

    def test_idle_engine_reaps_after_stop(self):
        client = self.start()
        (Path(self.root.name) / "stop").touch()
        deadline = time.monotonic() + 7
        while client.last_close_report is None and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue(client.last_close_report["process_exit_confirmed"])

    def test_crashed_engine_returns_unknown_without_retry(self):
        client = self.start(CrashEngineProcess)
        pid = client.process.pid
        result = client.run_prepared(self.prepared)
        self.assertEqual(result[0]["status"], "unknown")
        self.assertFalse(result[0]["submission_confirmed"])
        self.assertEqual(client.process.pid, pid)
        self.assertTrue(client.last_close_report["process_exit_confirmed"])

    def test_startup_deadline_preserves_process_cleanup_report(self):
        with self.assertRaises(TimeoutError):
            self.start(StartupStallEngineProcess, timeout=5, startup=0.2)
        report = json.loads(next(Path(self.root.name).glob("engine-*.json")).read_text())
        self.assertTrue(report["process_exit_confirmed"])
        self.assertEqual(report["live_observed_processes"], 0)


if __name__ == "__main__":
    unittest.main()
