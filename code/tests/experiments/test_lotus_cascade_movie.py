"""Original SemBench evaluator and evaluation-error evidence for helper cascades."""

import csv
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.experiments.lotus_cascade_movie import run_movie_cascade_query


class _MetadataFailureBackend:
    def __init__(self, execute, *, primary=None):
        self.execute = execute
        self.primary = primary
        self.close_error = OSError("fixture close metadata failure")
        self.core_error = RuntimeError("fixture Core metadata failure")
        self.metadata_reads = []

    def __call__(self, stage):
        rows = self.execute(stage)
        if self.primary is None:
            yield from rows
        else:
            yield rows[0]
            raise self.primary

    @property
    def close_reports(self):
        self.metadata_reads.append("close_reports")
        raise self.close_error

    @property
    def production_core_connected(self):
        self.metadata_reads.append("production_core_connected")
        raise self.core_error


@unittest.skipUnless(os.environ.get("RUN_LOTUS_CASCADE_CPU") == "1" and os.environ.get("SEMBENCH_CHECKOUT"),
                     "pinned LOTUS CPU environment and SemBench checkout required")
class MovieCascadeTests(unittest.TestCase):
    def setUp(self):
        import lotus
        from lotus.cache import InMemoryCache
        from lotus.models import LM
        from lotus.types import CascadeArgs
        from tokenizers import Tokenizer, models as token_models

        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(lambda: self.scope.__exit__(None, None, None))
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        original = root / "reviews.csv"
        with original.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["reviewId", "id", "reviewText", "scoreSentiment"])
            writer.writerows([["same", "taken_3", "yes", "POSITIVE"],
                              ["same", "other", "excluded", "NEGATIVE"],
                              ["same", "taken_3", "no", "NEGATIVE"],
                              ["same", "taken_3", "yes", "POSITIVE"]])
        manifest = prepare_movie_source(original, root / "source",
            expected_csv_sha256=hashlib.sha256(original.read_bytes()).hexdigest(), max_rows=4)
        self.source = MovieRecordSource.from_manifest(manifest)

        def model(name):
            tokenizer = Tokenizer(token_models.WordLevel({"[UNK]": 0, "True": 1, "False": 2}, unk_token="[UNK]"))
            return LM(name, tokenizer=tokenizer, cache=InMemoryCache(max_size=2), max_tokens=8)
        self.main, self.helper = model("openai/movie-fixture-main"), model("openai/movie-fixture-helper")
        self.args = CascadeArgs(filter_pos_cascade_threshold=0, filter_neg_cascade_threshold=0)
        self.stages = []

    def execute(self, stage):
        self.stages.append(stage)
        self.assertEqual(stage.model_role, "helper")
        self.assertTrue(all("scoreSentiment" not in row and "id" not in row for row in stage.records))
        return [dict({key: value for key, value in row.items() if key != "reviewText"},
                     status="succeeded", text="True", finish_reason="stop", submission_confirmed=True,
                     engine_request_id=row["call_id"], engine_uid="declared-software-fixture",
                     prompt_tokens=12, completion_tokens=1, total_tokens=13,
                     output_token_ids_json="[1]", logprobs_json=json.dumps([
                         dict(token="True", logprob=math.log(0.8), top_logprobs=[
                             dict(token="True", logprob=math.log(0.8)),
                             dict(token="False", logprob=math.log(0.2))])])) for row in stage.records]

    def run_query(self, evaluate=None):
        return run_movie_cascade_query(self.source, os.environ["SEMBENCH_CHECKOUT"],
            self.main, self.helper, self.args, self.execute if evaluate is None else evaluate,
            query_id="q", max_calls=6)

    def test_q3_predicate_count_and_original_evaluator(self):
        report = self.run_query()
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["positive_review_cnt"], 3)
        self.assertEqual([row["source_position"] for row in report["decisions"]], [0, 2, 3])
        self.assertEqual(report["quality"]["classification_audit"]["true_positive"], 2)
        self.assertEqual(report["quality"]["classification_audit"]["false_positive"], 1)
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 39)
        self.assertFalse(report["evaluation_in_query_seconds"])
        json.dumps(report, allow_nan=False)

    def test_evaluator_error_preserves_completed_calls_and_usage(self):
        with patch("src.experiments.lotus_cascade_movie.evaluate_original", side_effect=RuntimeError("fixture evaluator")):
            with self.assertRaisesRegex(RuntimeError, "fixture evaluator") as caught:
                self.run_query()
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["positive_review_cnt"], 3)
        self.assertEqual(len(report["calls"]["records"]), 3)
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 39)

    def test_backend_metadata_errors_preserve_primary_partial_calls_and_known_usage(self):
        primary = ValueError("fixture stage failure after a known result")
        backend = _MetadataFailureBackend(self.execute, primary=primary)
        with self.assertRaises(ValueError) as caught:
            self.run_query(backend)
        self.assertIs(caught.exception, primary)
        report = primary.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual([row["status"] for row in report["calls"]["records"]],
                         ["succeeded", "unknown", "unknown"])
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 12)
        self.assertEqual(report["calls"]["usage"]["known_completion_tokens"], 1)
        self.assertIsNone(report["calls"]["usage"]["total_tokens"])
        self.assertEqual(report["backend_close_metadata_error_type"], "OSError")
        self.assertEqual(report["production_core_used_metadata_error_type"], "RuntimeError")
        self.assertEqual(backend.metadata_reads, ["close_reports", "production_core_connected"])
        self.assertEqual(len(primary.__notes__), 2)
        self.assertNotIn("backend_close", report)
        self.assertNotIn("production_core_used", report)

    def test_successful_cascade_metadata_error_carries_completed_calls_and_usage(self):
        backend = _MetadataFailureBackend(self.execute)
        with self.assertRaises(OSError) as caught:
            self.run_query(backend)
        self.assertIs(caught.exception, backend.close_error)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["positive_review_cnt"], 3)
        self.assertEqual(len(report["calls"]["records"]), 3)
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 39)
        self.assertEqual(report["backend_close_metadata_error_type"], "OSError")
        self.assertEqual(report["production_core_used_metadata_error_type"], "RuntimeError")
        self.assertEqual(backend.metadata_reads, ["close_reports", "production_core_connected"])
        self.assertEqual(len(backend.close_error.__notes__), 1)
        self.assertNotIn("backend_close", report)
        self.assertNotIn("production_core_used", report)


if __name__ == "__main__":
    unittest.main()
