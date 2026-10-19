"""Original Movie query/evaluator qualification using explicit fixed responses."""

import csv
from copy import deepcopy
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


def fixed_result(position, query_id="q"):
    return dict(identity(query_id, "movie-row-" + str(position), position), status="succeeded",
                text="Answer: True" if position == 0 else "Answer: False",
                prompt_tokens=11 + position, completion_tokens=2, total_tokens=13 + position,
                finish_reason="stop", logprobs_json="null", engine_uid="software-fixture",
                engine_request_id=str(position), submission_confirmed=True)


class ClosingRows:
    def __init__(self, rows, receipts, *, primary=None, close_error=None):
        self.rows = iter(rows)
        self.receipts = receipts
        self.primary = primary
        self.close_error = close_error
        self.last_close_report = None

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.rows)
        except StopIteration:
            if self.primary is not None:
                raise self.primary
            raise

    def close(self):
        self.last_close_report = dict(unconsumed_call_results=self.receipts)
        if self.close_error is not None:
            raise self.close_error
        return self.last_close_report


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

    def failed_query(self, receipts, *, close_error=None, rows=None):
        from src.experiments.lotus_records_movie import run_movie_query
        primary = RuntimeError("query failed before all results were consumed")
        iterator = ClosingRows([fixed_result(0)] if rows is None else rows, receipts,
                               primary=primary, close_error=close_error)
        with patch("src.semantic_methods.lotus.engine.interpret_results") as interpret:
            with self.assertRaises(RuntimeError) as failure:
                run_movie_query(self.source, CHECKOUT, self.method, self.generation,
                                lambda: iterator, query_id="q")
        self.assertIs(failure.exception, primary)
        interpret.assert_not_called()
        return primary, primary.query_report

    def test_close_receipt_preserves_two_known_calls_and_one_unconfirmed(self):
        _, report = self.failed_query([fixed_result(2)])
        self.assertEqual(report["calls"]["counts"]["succeeded"], 2)
        self.assertEqual(report["calls"]["counts"]["unknown"], 1)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 24)
        self.assertEqual(report["calls"]["usage"]["known_completion_tokens"], 4)
        self.assertIsNone(report["calls"]["usage"]["prompt_tokens"])
        self.assertEqual(report["runner_unconfirmed_call_ids"], [identity("q", "movie-row-3", 3)["call_id"]])
        self.assertEqual(report["status"], "failed")
        self.assertNotIn("quality", report)
        self.assertNotIn("decisions", report)

    def test_identical_close_receipt_is_counted_once(self):
        _, report = self.failed_query([fixed_result(2), fixed_result(2)])
        self.assertEqual(report["calls"]["counts"]["succeeded"], 2)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 24)
        self.assertNotIn("close_receipt_errors", report)

    def test_bad_receipts_preserve_known_prefix_and_valid_receipts_on_both_sides(self):
        receipts = [fixed_result(2), dict(fixed_result(0), prompt_tokens=999), fixed_result(0, "foreign"),
                    dict(fixed_result(3), source_position=9), dict(fixed_result(3), status="running"),
                    dict(fixed_result(3), prompt_tokens=True), fixed_result(3)]
        primary, report = self.failed_query(receipts)
        self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
        self.assertEqual(report["calls"]["counts"]["unknown"], 0)
        self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 38)
        self.assertEqual(report["calls"]["records"][0]["prompt_tokens"], 11)
        self.assertEqual(report["error"], str(primary))
        self.assertTrue(any("conflicts" in note for note in primary.__notes__))
        self.assertTrue(any("undeclared" in note for note in primary.__notes__))
        self.assertEqual(report["status"], "failed")
        self.assertNotIn("result", report)

    def test_close_exception_keeps_primary_and_its_available_receipt(self):
        primary, report = self.failed_query([fixed_result(2)], close_error=OSError("close failed"))
        self.assertEqual(report["calls"]["counts"]["succeeded"], 2)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 24)
        self.assertTrue(any("records result close" in note for note in primary.__notes__))

    def test_bad_close_receipt_changes_completed_query_to_failure_without_overwriting_calls(self):
        from src.experiments.lotus_records_movie import run_movie_query
        iterator = ClosingRows([fixed_result(i) for i in (0, 2, 3)],
                               [dict(fixed_result(0), prompt_tokens=999)])
        with self.assertRaisesRegex(ValueError, "conflicts") as failure:
            run_movie_query(self.source, CHECKOUT, self.method, self.generation,
                            lambda: iterator, query_id="q")
        report = failure.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
        self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 38)

    def test_close_error_after_complete_query_is_reported_with_all_known_usage(self):
        from src.experiments.lotus_records_movie import run_movie_query
        close_error = OSError("close failed after complete consumption")
        iterator = ClosingRows([fixed_result(i) for i in (0, 2, 3)], [], close_error=close_error)
        with self.assertRaises(OSError) as failure:
            run_movie_query(self.source, CHECKOUT, self.method, self.generation,
                            lambda: iterator, query_id="q")
        self.assertIs(failure.exception, close_error)
        report = failure.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
        self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 38)

    def test_only_runner_created_unknown_can_upgrade_to_a_late_terminal(self):
        from src.experiments.lotus_records_movie import merge_unconsumed_call_results
        backend_unknown = dict(fixed_result(2), status="unknown", submission_confirmed=False,
                               prompt_tokens=None, completion_tokens=None, total_tokens=None)
        primary, report = self.failed_query([], rows=[fixed_result(0), backend_unknown])
        self.assertEqual(report["runner_unconfirmed_call_ids"], [identity("q", "movie-row-3", 3)["call_id"]])
        merge_unconsumed_call_results(report, self.source.expected("q"),
            dict(unconsumed_call_results=[fixed_result(2), fixed_result(3)]), primary=primary)
        self.assertEqual(report["calls"]["counts"]["succeeded"], 2)
        self.assertEqual(report["calls"]["counts"]["unknown"], 1)
        self.assertIsNone(report["calls"]["usage"]["prompt_tokens"])
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 25)
        self.assertEqual(report["runner_unconfirmed_call_ids"], [])
        self.assertEqual(report["status"], "failed")

    def test_backend_unknown_receipt_stops_being_an_upgradable_runner_placeholder(self):
        from src.experiments.lotus_records_movie import merge_unconsumed_call_results
        primary, report = self.failed_query([fixed_result(2)])
        actual_unknown = deepcopy(report["calls"]["records"][2])
        merge_unconsumed_call_results(report, self.source.expected("q"),
            dict(unconsumed_call_results=[actual_unknown]), primary=primary)
        self.assertEqual(report["runner_unconfirmed_call_ids"], [])
        merge_unconsumed_call_results(report, self.source.expected("q"),
            dict(unconsumed_call_results=[fixed_result(3)]), primary=primary)
        self.assertEqual(report["calls"]["counts"]["unknown"], 1)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 24)
        self.assertTrue(any("conflicts" in note for note in primary.__notes__))

    def worker_receipts(self, *, close_error=None, expected_error=False):
        from src.experiments import lotus_records_movie as module
        primary = RuntimeError("query failed with receipts still draining")
        iterator = ClosingRows([fixed_result(0)], [fixed_result(2)], primary=primary)
        owner = SimpleNamespace(evaluate=Mock(return_value=iterator), last_query_report=None)
        def close():
            owner.last_query_report = dict(unconsumed_call_results=[fixed_result(2), fixed_result(3)])
            if expected_error:
                self.source.expected = Mock(side_effect=ValueError("source changed after query close"))
            if close_error is not None:
                raise close_error
            return dict(last_query_report=owner.last_query_report, cleanup_errors=[])
        owner.close = Mock(side_effect=close)
        runtime = SimpleNamespace(init=Mock(), is_initialized=Mock(return_value=True), shutdown=Mock())
        recorded = []
        with patch.dict("sys.modules", {"ray": runtime}), \
             patch("src.execution_provider.adapters.lotus_records_static.SemLoomRecordsStaticReference",
                   return_value=owner), \
             patch.object(module, "write_private_json", side_effect=lambda p, v: recorded.append(deepcopy(v))), \
             patch("src.semantic_methods.lotus.engine.interpret_results") as interpret:
            with self.assertRaises(RuntimeError) as failure:
                module._model_worker({"backend": "semloom-local-model-static-reference", "query_id": "q"},
                    self.source, CHECKOUT, self.method, self.generation,
                    SimpleNamespace(cpu_workers=1, gpu_workers=1), None, Path("unused"))
        self.assertIs(failure.exception, primary)
        interpret.assert_not_called()
        runtime.shutdown.assert_called_once()
        self.assertEqual(recorded[-1]["status"], "failed")
        self.assertNotIn("decisions", recorded[-1])
        return primary, recorded[-1]

    def test_owner_close_merges_new_receipt_after_query_unknown_was_recorded(self):
        for close_error in (None, OSError("owner close failed after draining")):
            with self.subTest(close_error=close_error):
                _, report = self.worker_receipts(close_error=close_error)
                self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
                self.assertEqual(report["calls"]["counts"]["unknown"], 0)
                self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 38)
                self.assertEqual(report["calls"]["usage"]["completion_tokens"], 6)
                self.assertEqual(report["runner_unconfirmed_call_ids"], [])

    def test_owner_identity_refresh_failure_is_secondary_to_query_error(self):
        primary, report = self.worker_receipts(expected_error=True)
        self.assertEqual(report["calls"]["counts"]["succeeded"], 2)
        self.assertEqual(report["calls"]["counts"]["unknown"], 1)
        self.assertTrue(any("owner close accounting" in note for note in primary.__notes__))

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
    def test_daft_selector_uses_lazy_native_graph_without_importing_ray(self):
        import builtins
        from src.experiments import lotus_records_movie as module

        native_rows = iter(())
        recorded = []
        original_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name == "ray" or name.startswith("ray."):
                raise AssertionError("Daft selector imported Ray")
            return original_import(name, *args, **kwargs)

        def query(*args, **kwargs):
            self.assertIs(args[4](), native_rows)
            return {"status": "completed", "calls": {"known_prompt_tokens": 7}}

        with patch("src.baselines.text.frameworks.daft_lotus_records.evaluate",
                   return_value=native_rows) as evaluate, \
             patch.object(module, "run_movie_query", side_effect=query), \
             patch.object(module, "write_private_json", side_effect=lambda path, value: recorded.append(dict(value))), \
             patch("builtins.__import__", side_effect=guarded_import):
            module._model_worker({"backend": "daft-native-local-model", "query_id": "q"},
                                 "source", "checkout", "method", "generation", "resources", "engine", Path("unused"))
        evaluate.assert_called_once_with("source", "method", "engine", "generation", "resources", query_id="q")
        self.assertEqual(recorded[-1]["status"], "completed")
        self.assertFalse(recorded[-1]["independent_gpu_process_exit_check"])

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
