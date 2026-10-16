"""Original Movie query/evaluator qualification using explicit fixed responses."""

import csv
import hashlib
import importlib.util
import os
from types import SimpleNamespace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.semantic_methods.lotus.records import FilterMethod, identity


LIBRARIES = all(importlib.util.find_spec(name) is not None for name in ("lotus", "duckdb", "pandas", "cdlib"))
CHECKOUT = os.environ.get("SEMBENCH_CHECKOUT")


def fixture_source(directory):
    original = directory / "Reviews.csv"
    with original.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["reviewId", "id", "reviewText", "scoreSentiment"])
        writer.writerows([
            ["same", "taken_3", "good", "POSITIVE"],
            ["same", "other", "outside query", "NEGATIVE"],
            ["same", "taken_3", "bad", "NEGATIVE"],
            ["last", "taken_3", "", "NEGATIVE"],
        ])
    manifest = prepare_movie_source(original, directory / "prepared",
                                    expected_csv_sha256=hashlib.sha256(original.read_bytes()).hexdigest(), max_rows=4)
    return MovieRecordSource.from_manifest(manifest)


@unittest.skipUnless(LIBRARIES and CHECKOUT, "requires pinned CPU libraries and SEMBENCH_CHECKOUT")
class MovieRecordsTests(unittest.TestCase):
    def setUp(self):
        from src.serving.backends.vllm_engine import GenerationSpec
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.source = fixture_source(Path(self.temporary.name))
        self.method = FilterMethod("openai/local-fixture")
        self.generation = GenerationSpec()

    def test_full_query_matches_original_messages_decisions_stats_and_evaluator(self):
        from src.experiments.lotus_records_movie import fixed_response_qualification
        responses = [dict(row_id="movie-row-" + str(i), text=text,
                          prompt_tokens=10 + i, completion_tokens=2)
                     for i, text in ((0, "Answer: True"), (2, "Answer: False"), (3, "Answer: False"))]
        report = fixed_response_qualification(self.source, CHECKOUT, self.method, self.generation,
                                             responses, query_id="fixed-q3")
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["result"], {"positive_review_cnt": 1})
        self.assertEqual(report["inference_calls"], 0)
        self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
        self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 35)
        self.assertTrue(report["native_reference"]["lm_stats_equal"])
        self.assertEqual(report["quality"]["classification_audit"]["false_positive"], 0)
        self.assertEqual(report["quality"]["classification_audit"]["false_negative"], 0)

    def test_invalid_source_does_not_allocate_backend(self):
        from src.experiments.lotus_records_movie import run_movie_query
        execute = Mock()
        with patch.object(self.source, "preflight", side_effect=ValueError("bad source")):
            with self.assertRaisesRegex(ValueError, "bad source"):
                run_movie_query(self.source, CHECKOUT, self.method, self.generation, execute, query_id="q")
        execute.assert_not_called()

    def test_empty_and_no_eligible_sources_match_native_zero_call_query(self):
        from src.experiments.lotus_records_movie import fixed_response_qualification
        for index, rows in enumerate(([], [["id", "other", "no predicate match", "NEGATIVE"]])):
            directory = Path(self.temporary.name) / ("empty-" + str(index))
            directory.mkdir()
            original = directory / "Reviews.csv"
            with original.open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["reviewId", "id", "reviewText", "scoreSentiment"])
                writer.writerows(rows)
            manifest = prepare_movie_source(original, directory / "prepared",
                expected_csv_sha256=hashlib.sha256(original.read_bytes()).hexdigest(), max_rows=1)
            with self.subTest(rows=len(rows)):
                report = fixed_response_qualification(MovieRecordSource.from_manifest(manifest), CHECKOUT,
                    self.method, self.generation, [], query_id="empty-q3")
                self.assertEqual(report["status"], "completed")
                self.assertEqual(report["result"], {"positive_review_cnt": 0})
                self.assertEqual(report["calls"]["counts"]["expected"], 0)
                self.assertEqual(report["calls"]["usage"]["total_tokens"], 0)
                self.assertTrue(report["native_reference"]["messages_equal"])
                self.assertIsInstance(report["native_reference"]["effective_kwargs"], dict)

    def test_partial_failure_retains_known_usage_and_marks_missing_unconfirmed(self):
        from src.experiments.lotus_records_movie import run_movie_query
        first = dict(identity("q", "movie-row-0", 0), status="succeeded", text="Answer: True",
                     prompt_tokens=11, completion_tokens=2, total_tokens=13,
                     finish_reason="stop", engine_request_id="1")
        def failed():
            yield first
            raise RuntimeError("prepare suffix failed")
        with self.assertRaisesRegex(RuntimeError, "prepare suffix failed") as failure:
            run_movie_query(self.source, CHECKOUT, self.method, self.generation, failed, query_id="q")
        report = failure.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 11)
        self.assertIsNone(report["calls"]["usage"]["prompt_tokens"])
        self.assertEqual(report["calls"]["counts"]["unknown"], 2)
        self.assertNotIn("quality", report)

    def test_native_differential_failure_retains_completed_call_ledger(self):
        from src.experiments.lotus_records_movie import fixed_response_qualification
        import pandas as pd
        responses = [dict(row_id="movie-row-" + str(i), text="Answer: False",
                          prompt_tokens=10, completion_tokens=2) for i in (0, 2, 3)]
        with patch("src.experiments.lotus_records_movie.run_original_lotus_query",
                   return_value=pd.DataFrame({"positive_review_cnt": [0]})):
            with self.assertRaisesRegex(ValueError, "messages") as failure:
                fixed_response_qualification(self.source, CHECKOUT, self.method, self.generation,
                                             responses, query_id="mismatch")
        self.assertEqual(failure.exception.query_report["status"], "failed")
        self.assertEqual(failure.exception.query_report["calls"]["usage"]["prompt_tokens"], 30)
        self.assertEqual(failure.exception.query_report["calls"]["counts"]["succeeded"], 3)


class WorkerCleanupTests(unittest.TestCase):
    def worker(self, *, query_error=None, close_error=None, shutdown_error=None):
        from src.experiments import lotus_records_movie as module
        report = {"status": "completed", "calls": {"known_prompt_tokens": 7}}
        owner = SimpleNamespace(evaluate=Mock(return_value=iter(())),
                                close=Mock(side_effect=close_error, return_value={"cleanup_errors": []}))
        runtime = SimpleNamespace(init=Mock(), is_initialized=Mock(return_value=True),
                                  shutdown=Mock(side_effect=shutdown_error))
        recorded = []
        def query(*args, **kwargs):
            args[4]()  # Allocate the static owner through the real lazy factory.
            if query_error is not None:
                query_error.query_report = report
                raise query_error
            return report
        with patch.dict("sys.modules", {"ray": runtime}), \
             patch("src.execution_provider.adapters.lotus_records_static.SemLoomRecordsStaticReference",
                   return_value=owner), \
             patch.object(module, "run_movie_query", side_effect=query), \
             patch.object(module, "write_private_json", side_effect=lambda path, value: recorded.append(dict(value))):
            with self.assertRaises(BaseException) as failure:
                module._model_worker({"backend": "semloom-local-model-static-reference", "query_id": "q"},
                                     None, None, None, None,
                                     SimpleNamespace(cpu_workers=1, gpu_workers=1), None, Path("unused"))
        owner.close.assert_called_once()
        runtime.shutdown.assert_called_once()
        self.assertEqual(recorded[-1]["status"], "failed")
        self.assertEqual(failure.exception.query_report["calls"]["known_prompt_tokens"], 7)
        return failure.exception

    def test_cleanup_cannot_overwrite_primary_query_failure(self):
        primary = RuntimeError("query failed")
        self.assertIs(self.worker(query_error=primary, close_error=OSError("close failed")), primary)
        self.assertTrue(any("static owner close" in note for note in primary.__notes__))

    def test_cleanup_failure_changes_success_to_failure(self):
        failure = self.worker(close_error=OSError("close failed"))
        self.assertIsInstance(failure, OSError)
        self.assertEqual(failure.query_report["close_error_type"], "OSError")

    def test_ray_shutdown_failure_is_recorded(self):
        failure = self.worker(shutdown_error=OSError("shutdown failed"))
        self.assertEqual(failure.query_report["ray_close_error_type"], "OSError")


if __name__ == "__main__":
    unittest.main()
