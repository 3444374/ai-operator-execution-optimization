"""Associated receipts survive protocol, audit and cleanup faults; CPU only."""

from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.frameworks.daft_engine_process import EngineProcess, RESULT_PROTOCOL_ERROR
from src.semantic_methods.lotus.records import identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, _prepared
from tests.baselines.text.engine_receipt_fixture import ReplyEngineProcess, terminal


def prepared(query="q", count=3):
    return [dict(identity(query, "row-" + str(i), i), messages_json=json.dumps([
        dict(role="user", content="Self-written CPU receipt " + str(i))])) for i in range(count)]


def fake_process(directory, response):
    """No constructor/Popen: exercise the real public reconciliation entry."""
    client = EngineProcess.__new__(EngineProcess)
    client.control, client.report_path = Path(directory), Path(directory) / "engine-fake.json"
    client.deadline, client.process, client.created = time.monotonic() + 30, None, None
    client.ready_report = dict(engine_uid="cpu-receipt-owned")
    client._broken = client._closed = False
    client._lock, client._reaper_stop = threading.RLock(), threading.Event()
    client.last_result_failure = client.last_close_report = client._last_raw_response_json = None
    client._result_failure_write_errors = []
    client._exchange = Mock(return_value=response)
    client.close = Mock(return_value=dict(status="unknown", process_exit_confirmed=False))
    return client


class ReceiptReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="engine-receipts-no-process-")
        self.addCleanup(self.directory.cleanup)
        self.rows = prepared()
        identities, _ = _prepared(self.rows)
        self.good = [terminal(item) for item in identities]
        self.no_process = patch("subprocess.Popen", side_effect=AssertionError("Popen forbidden in pure receipt fixtures"))
        self.no_process.start()
        self.addCleanup(self.no_process.stop)

    def test_healthy_reply_is_returned_without_copy_marker_or_new_audit(self):
        client = fake_process(self.directory.name, dict(kind="results", rows=self.good))
        before = json.dumps(self.good)
        actual = client.run_prepared(self.rows)
        self.assertIs(actual, self.good)
        self.assertEqual(json.dumps(actual), before)
        self.assertIsNone(client.last_result_failure)
        self.assertFalse(client._broken)
        client.close.assert_not_called()
        self.assertFalse(client.report_path.exists())

    def test_cardinality_order_duplicates_and_foreign_rows_keep_only_unique_actual_receipts(self):
        for fault, usage, indexes in (("short", 3, []), ("extra", 9, [3]), ("reorder", 9, []),
                                      ("duplicate", 6, [0, 3]), ("contradictory", 6, [1])):
            with self.subTest(fault=fault):
                raw = deepcopy(self.good)
                if fault == "short": raw = raw[:1]
                if fault == "extra": raw.append(dict(raw[0], call_id="foreign", engine_request_id="foreign"))
                if fault == "reorder": raw.reverse()
                if fault == "duplicate": raw.append(deepcopy(raw[0]))
                if fault == "contradictory": raw[1]["query_id"] = "different-query"
                response = dict(kind="results", rows=raw)
                client = fake_process(self.directory.name, response)
                actual = client.run_prepared(self.rows)
                self.assertEqual([row["call_id"] for row in actual], [row["call_id"] for row in self.rows])
                self.assertEqual(sum(row["prompt_tokens"] or 0 for row in actual), usage)
                self.assertTrue(all(row["engine_protocol_error"] == RESULT_PROTOCOL_ERROR for row in actual))
                failure = client.last_result_failure
                self.assertEqual(failure["unassociated_result_indexes"], indexes)
                self.assertEqual(json.loads(failure["raw_response_json"]), response)
                self.assertEqual(sum(row["prompt_tokens"] or 0 for row in failure["verified_call_results"]), usage)
                self.assertEqual(client._exchange.call_count, 1)
                client.close.assert_called_once()
                if failure["verified_call_results"]:
                    actual_known = next(row for row in actual if row["status"] == "succeeded")
                    actual_known["prompt_tokens"] = 999
                    self.assertEqual(failure["verified_call_results"][0]["prompt_tokens"], 3)

    def test_incomplete_invalid_usage_tokens_digest_or_engine_are_unassociated(self):
        fields = (("prompt_tokens", True), ("total_tokens", 99), ("prompt_token_ids_json", "[11]"),
                  ("output_token_ids_json", None), ("status", "invalid"),
                  ("messages_sha256", "different"), ("engine_uid", "foreign"),
                  ("finish_reason", "unfinished"), ("submission_confirmed", 1))
        for name, value in fields:
            with self.subTest(field=name):
                raw = deepcopy(self.good)
                raw[1][name] = value
                raw.append(dict(raw[0], call_id="foreign", engine_request_id="foreign"))
                client = fake_process(self.directory.name, dict(kind="results", rows=raw))
                actual = client.run_prepared(self.rows)
                self.assertEqual([row["status"] for row in actual], ["succeeded", "unknown", "succeeded"])
                self.assertIsNone(actual[1]["prompt_tokens"])
                self.assertFalse(actual[1]["submission_confirmed"])
                self.assertEqual(client.last_result_failure["unassociated_result_indexes"], [1, 3])

    def test_real_failed_and_unknown_terminals_keep_status_error_and_partial_usage(self):
        raw = deepcopy(self.good)
        raw[0].update(status="failed", text=None, error_type="OriginalFailure")
        raw[1].update(status="unknown", text=None, submission_confirmed=False, error_type="OriginalUnknown",
                      completion_tokens=None, output_token_ids_json="null", total_tokens=None)
        raw.reverse()
        client = fake_process(self.directory.name, dict(kind="results", rows=raw))
        actual = client.run_prepared(self.rows)
        self.assertEqual([row["status"] for row in actual], ["failed", "unknown", "succeeded"])
        self.assertEqual(actual[0]["error_type"], "OriginalFailure")
        self.assertEqual(actual[1]["error_type"], "OriginalUnknown")
        self.assertEqual(actual[1]["prompt_tokens"], 3)
        self.assertIsNone(actual[1]["completion_tokens"])
        self.assertEqual(len(client.last_result_failure["verified_call_results"]), 3)

    def test_failed_and_unknown_preserve_either_known_usage_component_without_upgrading(self):
        for status in ("failed", "unknown"):
            for missing, ids in (("prompt_tokens", "prompt_token_ids_json"),
                                 ("completion_tokens", "output_token_ids_json")):
                with self.subTest(status=status, missing=missing):
                    raw = deepcopy(self.good)
                    raw[0].update(status=status, text=None, submission_confirmed=False,
                                  error_type="OriginalPartial", total_tokens=None)
                    raw[0][missing], raw[0][ids] = None, "null"
                    original = deepcopy(raw[0])
                    raw.reverse()
                    client = fake_process(self.directory.name, dict(kind="results", rows=raw))
                    actual = client.run_prepared(self.rows)
                    expected = dict(original, engine_protocol_error=RESULT_PROTOCOL_ERROR)
                    self.assertEqual(actual[0], expected)
                    self.assertEqual(client.last_result_failure["verified_call_results"][0], expected)
                    self.assertEqual(client.last_result_failure["unassociated_result_indexes"], [])
                    self.assertEqual(actual[0]["status"], status)
                    self.assertFalse(actual[0]["submission_confirmed"])

    def test_duplicate_native_request_identity_is_not_credited_twice(self):
        raw = deepcopy(self.good)
        raw[1]["engine_request_id"] = raw[0]["engine_request_id"]
        raw.reverse()
        client = fake_process(self.directory.name, dict(kind="results", rows=raw))
        actual = client.run_prepared(self.rows)
        self.assertEqual([row["status"] for row in actual], ["unknown", "unknown", "succeeded"])
        self.assertEqual(client.last_result_failure["unassociated_result_indexes"], [1, 2])

    def test_bad_reply_kind_or_rows_shape_retains_raw_without_attributing_it(self):
        for response in (dict(kind="error", rows=self.good), dict(kind="results", rows={"not": "a list"})):
            with self.subTest(response=response):
                client = fake_process(self.directory.name, response)
                actual = client.run_prepared(self.rows)
                self.assertTrue(all(row["status"] == "unknown" and row["prompt_tokens"] is None for row in actual))
                self.assertEqual(client.last_result_failure["verified_call_results"], [])
                self.assertEqual(json.loads(client.last_result_failure["raw_response_json"]), response)

    def test_decoded_non_object_frame_keeps_raw_and_only_unconfirmed_terminals(self):
        client = fake_process(self.directory.name, None)
        def exchange(*_):
            client._last_raw_response_json = '["decoded non-object reply"]'
            raise ValueError("engine IPC response must be an object")
        client._exchange.side_effect = exchange
        actual = client.run_prepared(self.rows)
        self.assertTrue(all(row["status"] == "unknown" and row["engine_protocol_error"] for row in actual))
        self.assertEqual(client.last_result_failure["raw_response_json"], '["decoded non-object reply"]')
        self.assertEqual(client.last_result_failure["verified_call_results"], [])

    def test_audit_and_cleanup_failures_cannot_replace_received_usage_or_protocol_error(self):
        client = fake_process(self.directory.name, dict(kind="results", rows=self.good[:1]))
        client.close.side_effect = PermissionError("CPU cleanup fixture")
        with patch("src.baselines.text.frameworks.daft_engine_process._publish", side_effect=OSError("CPU audit fixture")):
            actual = client.run_prepared(self.rows)
        self.assertEqual(actual[0]["prompt_tokens"], 3)
        self.assertEqual(client.last_result_failure["error_type"], "ValueError")
        self.assertEqual(client.last_close_report["result_failure"], client.last_result_failure)
        self.assertIn("result_failure_report_write: OSError", client.last_close_report["cleanup_errors"])
        self.assertIn("result_failure_cleanup: PermissionError", client.last_close_report["cleanup_errors"])

    def test_close_observation_failure_retains_the_same_query_receipts(self):
        client = fake_process(self.directory.name, dict(kind="results", rows=self.good[:1]))
        client.run_prepared(self.rows)
        client.owned = set()
        del client.close
        with patch.object(client, "_close", side_effect=PermissionError("CPU observation fixture")):
            report = client.close()
        self.assertEqual(report["result_failure"], client.last_result_failure)
        self.assertEqual(report["result_failure"]["verified_call_results"][0]["query_id"], "q")
        self.assertEqual(json.loads(client.report_path.read_text())["result_failure"], client.last_result_failure)
        with patch.object(client, "_close", side_effect=PermissionError("CPU observation fixture")), \
                patch("src.baselines.text.frameworks.daft_engine_process._publish",
                      side_effect=OSError("CPU close audit fixture")):
            failed_write = client.close()
        self.assertEqual(failed_write["result_failure"], report["result_failure"])
        self.assertIn("cleanup_report_write: OSError", failed_write["cleanup_errors"])

    def test_no_second_exchange_after_failure_and_original_receipts_do_not_change_query(self):
        client = fake_process(self.directory.name, dict(kind="results", rows=self.good[:1]))
        client.run_prepared(self.rows)
        original = deepcopy(client.last_result_failure)
        other = client.run_prepared(prepared("another-query"))
        self.assertTrue(all(row["status"] == "unknown" and row["engine_protocol_error"] for row in other))
        self.assertEqual(client._exchange.call_count, 1)
        self.assertEqual(client.last_result_failure, original)

    def test_imlane_common_marker_ends_model_dispatch_and_enters_existing_audit(self):
        from tests.integrations.test_imlane_filter import FilterTests, FixedEngine, source
        fixture = FilterTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.config["resources"]["model_batch_rows"] = 1
        class Marked(FixedEngine):
            def run_prepared(self, rows):
                return [dict(row, engine_protocol_error=RESULT_PROTOCOL_ERROR) for row in super().run_prepared(rows)]
        worker = fixture.worker(Marked)
        iterator, _ = fixture.iterator(worker, source(), block_rows=3)
        rows = []
        with self.assertRaises(ValueError):
            rows.extend(iterator)
        self.assertEqual(len(worker.kernel.calls), 1)
        self.assertEqual(rows[0]["status"], "succeeded")
        self.assertEqual(rows[0]["prompt_tokens"], 5)
        self.assertTrue(rows[0]["imlane_protocol_error"])
        self.assertEqual([row["status"] for row in rows[1:]], ["failed", "failed"])
        iterator.close()


class PrivateReceiptIPCTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="engine-receipts-real-cpu-")
        self.addCleanup(self.directory.cleanup)
        self.engine = EngineSpec(self.directory.name, sys.executable, sys.executable)

    def start(self, mode):
        cls = type("Reply" + mode, (ReplyEngineProcess,), dict(mode=mode))
        client = cls(self.engine, GenerationSpec(), self.directory.name, time.monotonic() + 30, 5)
        self.addCleanup(client.close)
        return client

    def test_real_pipe_preserves_original_json_known_usage_and_os_exit_without_api_claim(self):
        for mode, usage in (("short", 3), ("duplicate", 6), ("reorder", 9)):
            with self.subTest(mode=mode):
                client = self.start(mode)
                result = client.run_prepared(prepared())
                failure = client.last_result_failure
                self.assertEqual(sum(row["prompt_tokens"] or 0 for row in result), usage)
                self.assertIn('"kind":  "results",  ', failure["raw_response_json"])
                self.assertIn("原始 reply", failure["raw_response_json"])
                self.assertEqual(json.loads(failure["raw_response_json"])["fixture_total_attempts"], 3)
                report = client.close()
                self.assertEqual(report["result_failure"], failure)
                self.assertTrue(report["process_exit_confirmed"])
                self.assertIsNone(report["gpu_exit_confirmed"])
                self.assertEqual(report["status"], "unknown")
                self.assertIn("engine_api_shutdown_unconfirmed", report["cleanup_errors"])
                self.assertEqual(json.loads(client.report_path.read_text())["result_failure"], failure)

    def test_real_healthy_pipe_has_original_api_close_and_no_result_failure(self):
        client = self.start("healthy")
        result = client.run_prepared(prepared())
        self.assertEqual(sum(row["prompt_tokens"] for row in result), 9)
        self.assertTrue(all("engine_protocol_error" not in row for row in result))
        self.assertIsNone(client.last_result_failure)
        report = client.close()
        self.assertEqual(report["status"], "closed")
        self.assertTrue(report["engine_api"]["engine_api_shutdown_confirmed"])
        self.assertNotIn("result_failure", report)


if __name__ == "__main__":
    unittest.main()
