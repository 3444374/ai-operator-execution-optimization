"""Preserve independent execution evidence under query, consumer, and evaluator faults."""

from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
import unittest

from src.experiments.postgresql.map_query_recording import (
    public_execution_summary, record_execution, record_pg_query, verify_map_completions,
)
from src.execution_provider.semantic_map import SemanticMapPlan, canonical_messages
from src.execution_provider.wire.map_codec import semantic_payload_digest


class QueryRecordingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "run"
        self.closed = False

    def source(self, values=(), failure=None):
        @contextmanager
        def open_rows():
            def rows():
                yield from values
                if failure:
                    raise failure
            try:
                yield rows()
            finally:
                self.closed = True
        return open_rows

    def execute(self, source, evaluator=None, **options):
        return record_execution(self.root, source, max_rows=options.get("max_rows", 10),
            max_result_bytes=options.get("max_result_bytes", 4096), evaluator=evaluator)

    def test_evaluator_sees_saved_terminal_and_raw_results(self):
        def evaluate(rows):
            self.assertTrue(self.closed)
            self.assertEqual(json.loads((self.root / "execution.json").read_text())["status"], "completed")
            self.assertEqual(rows, [["id", "  raw\n"]])
            return {"rows": len(rows)}
        self.execute(self.source([("id", "  raw\n")]), evaluate)
        self.assertEqual(public_execution_summary(self.root)["recorded_rows"], 1)

    def test_partial_rows_survive_query_failure(self):
        with self.assertRaisesRegex(RuntimeError, "query_failed"):
            self.execute(self.source([("a", "partial")], RuntimeError("query_failed")))
        execution = json.loads((self.root / "execution.json").read_text())
        self.assertEqual(execution["status"], "failed")
        self.assertTrue(execution["partial_results_are_provisional"])
        self.assertEqual(execution["recorded_rows"], 1)
        self.assertGreaterEqual(execution["ended_ns"], execution["started_ns"])
        self.assertTrue(self.closed)
        self.assertFalse((self.root / "evaluation.json").exists())

    def test_failure_before_first_row_still_has_timing(self):
        @contextmanager
        def source():
            self.assertTrue((self.root / "started.json").exists())
            raise RuntimeError("before_execute")
            yield
        with self.assertRaises(RuntimeError):
            self.execute(source)
        record = json.loads((self.root / "execution.json").read_text())
        self.assertEqual(record["recorded_rows"], 0)
        self.assertEqual(record["status"], "failed")

    def test_evaluator_failure_keeps_completed_execution(self):
        def fail(_rows):
            raise ValueError('password="fixture-only"')
        with self.assertRaises(ValueError):
            self.execute(self.source([("a", "answer")]), fail)
        self.assertEqual(json.loads((self.root / "execution.json").read_text())["status"], "completed")
        self.assertEqual(json.loads((self.root / "evaluation.json").read_text())["status"], "failed")
        self.assertNotIn("fixture-only", (self.root / "evaluation.json").read_text())
        summary = public_execution_summary(self.root)
        self.assertEqual(summary["execution_status"], "completed")
        self.assertEqual(summary["evaluation_status"], "failed")
        self.assertFalse(summary["performance_qualified"])

    def test_empty_result_records_completion_without_quality_claim(self):
        self.execute(self.source())
        summary = public_execution_summary(self.root)
        self.assertEqual(summary["recorded_rows"], 0)
        self.assertEqual(summary["evaluation_status"], "not_run")
        self.assertFalse(summary["performance_qualified"])

    def test_interruption_and_result_limit_close_stream(self):
        for failure, options in ((KeyboardInterrupt(), {}), (None, {"max_rows": 1})):
            with self.subTest(failure=failure):
                self.root = Path(self.tmp.name) / ("interrupt" if failure else "limit")
                with self.assertRaises(KeyboardInterrupt if failure else ValueError):
                    self.execute(self.source([("a", "first"), ("b", "second")], failure), **options)
                self.assertTrue(self.closed)
                self.assertEqual(json.loads((self.root / "execution.json").read_text())["status"], "failed")

    def test_existing_root_prevents_execution(self):
        self.root.mkdir()
        @contextmanager
        def source():
            self.fail("must not start SQL")
            yield
        with self.assertRaises(FileExistsError):
            self.execute(source)

    def test_pg_wrapper_closes_generator_and_cursor(self):
        state = {"generator": False, "cursor": False}
        class Cursor:
            def __enter__(self):
                return self
            def __exit__(self, *_):
                state["cursor"] = True
            def stream(self, query):
                self.query = query
                try:
                    yield ("a", "one")
                    yield ("b", "two")
                finally:
                    state["generator"] = True
        class Connection:
            def cursor(self):
                return Cursor()
        with self.assertRaises(ValueError):
            record_pg_query(Connection(), "explicit query", self.root, max_rows=1, max_result_bytes=4096)
        self.assertEqual(state, {"generator": True, "cursor": True})


class MapAssociationTests(unittest.TestCase):
    def setUp(self):
        self.plan = SemanticMapPlan("Instruction", "model", 64)
        self.inputs = [("a", "same input"), ("b", "same input")]
        self.predictions = [("b", "second"), ("a", "first")]
        digest = semantic_payload_digest(semantic_spec_sha256=self.plan.digest, input_value="same input",
            canonical_messages_utf8=canonical_messages("Instruction", "same input"))
        self.completions = [dict(sequence=sequence, payload_digest=digest, raw_output=output,
            response_model_id="model", finish_reason="stop") for sequence, output in ((1, "second"), (0, "first"))]

    def check(self, completions):
        return verify_map_completions(self.inputs, self.predictions, completions, plan=self.plan, sequence_ids=["a", "b"])

    def test_out_of_order_identical_inputs_keep_distinct_sequences(self):
        self.assertEqual(self.check(self.completions)["matched_rows"], 2)

    def test_duplicates_missing_payload_output_and_length_rejected(self):
        invalid = [self.completions[:1], [self.completions[0]]*2]
        for key, value in (("payload_digest", "0"*64), ("raw_output", "wrong"), ("finish_reason", "length")):
            invalid.append([dict(self.completions[0], **{key: value}), self.completions[1]])
        for records in invalid:
            with self.subTest(records=records), self.assertRaises(ValueError):
                self.check(records)


if __name__ == "__main__":
    unittest.main()
