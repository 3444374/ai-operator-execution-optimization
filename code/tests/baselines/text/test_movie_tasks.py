"""Task switching exercises the original queries and parser, with explicit CPU replies."""

import csv
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.baselines.text.movie_tasks import MovieTask, RETRIEVAL_INSTRUCTION
from src.semantic_methods.lotus.records import MOVIE_Q3_INSTRUCTION


class MovieTaskTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = self.root / "Reviews.csv"
        self.rows = [
            ["duplicate", "other", "good zero", "POSITIVE"],
            ["same", "taken_3", "bad one", "NEGATIVE"],
            ["same", "taken_3", "good two", "POSITIVE"],
            ["r3", "other", "good three", "POSITIVE"],
            ["same", "taken_3", "good four", "POSITIVE"],
            ["r5", "taken_3", "good five", "POSITIVE"],
            ["r6", "other", "good six", "POSITIVE"],
            ["r7", "taken_3", "good seven", "POSITIVE"],
            ["r8", "taken_3", "good eight", "POSITIVE"],
            ["r9", "other", "bad nine", "NEGATIVE"],
            ["r10", "taken_3", "good ten", "POSITIVE"],
        ]
        with self.raw.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["reviewId", "id", "reviewText", "scoreSentiment"])
            writer.writerows(self.rows)
        self.digest = hashlib.sha256(self.raw.read_bytes()).hexdigest()

    def source(self, query, suffix=""):
        task = MovieTask(query)
        path = prepare_movie_source(self.raw, self.root / (task.name + suffix),
            expected_csv_sha256=self.digest, max_rows=len(self.rows), task=task.name)
        return MovieRecordSource.from_manifest(path)

    def test_task_catalog_preserves_exact_original_prompts_and_predicates(self):
        for query in (1, 2, 3):
            task = MovieTask(query)
            self.assertEqual(task.instruction, MOVIE_Q3_INSTRUCTION if query == 3 else RETRIEVAL_INSTRUCTION)
            self.assertEqual(task.predicate is None, query == 1)
            task.validate_method(task.method("openai/cpu-fixture"))
        for value in (True, 0, 4, "1"):
            with self.assertRaises(ValueError):
                MovieTask(value)

    def test_tasks_select_all_filter_occurrences_before_the_projection(self):
        for query, expected in ((1, 11), (2, 7), (3, 7)):
            source = self.source(query)
            self.assertEqual(len(source.expected("same-outer-query-id")), expected)
            self.assertEqual(source.scan_spec(source.task.method("fixture")).predicate,
                None if query == 1 else ("id", "taken_3"))
            self.assertNotIn("scoreSentiment", source.csv_path.read_text())

    def test_source_order_and_duplicate_business_ids_survive_out_of_order_filter_results(self):
        for query, ids in ((1, ["duplicate", "same", "r3", "same", "r5"]),
                           (2, ["same", "same", "r5", "r7", "r8"])):
            source = self.source(query)
            decisions = [dict(row_id=r["row_id"], decision="good" in r["reviewText"])
                for r in source.rows() if source.task.accepts(r)]
            result = source.task.result_frame(source.rows(), list(reversed(decisions)))
            self.assertEqual(source.task.encode_result(result), {"reviewId": ids})
            self.assertLess(len(set(ids)), len(ids))

    def test_missing_or_duplicate_decision_cannot_hide_behind_head_five(self):
        source = self.source(1)
        decisions = [dict(row_id=r["row_id"], decision=True) for r in source.rows()]
        for values in (decisions[:-1], decisions + decisions[:1]):
            with self.assertRaisesRegex(ValueError, "every unique"):
                source.task.result_frame(source.rows(), values)

    @unittest.skipUnless(all(os.environ.get(k) for k in
        ('SEMBENCH_CHECKOUT', 'SEMBENCH_Q1_MANIFEST', 'SEMBENCH_Q2_MANIFEST')),
        'requires separately pinned full Q1/Q2 inputs and original evaluator')
    def test_fixed_input_ids_and_gold_match_original_csv_types(self):
        import pandas as pd
        from src.baselines.text.sembench_movie import evaluate_original
        for query in (1, 2):
            with self.subTest(query=query):
                source = MovieRecordSource.from_manifest(os.environ[f'SEMBENCH_Q{query}_MANIFEST'])
                manifest = json.loads(source.manifest_path.read_text())
                original = pd.read_csv(manifest['original_csv'])
                self.assertEqual(str(original['reviewId'].dtype), 'int64')
                mask = original['scoreSentiment'].eq('POSITIVE')
                if query == 2:
                    mask &= original['id'].eq('taken_3')
                expected = original.loc[mask, ['reviewId']].head(5).reset_index(drop=True)
                decisions = [dict(row_id=row['row_id'], decision=
                    original.iloc[row['source_position']]['scoreSentiment'] == 'POSITIVE')
                    for row in source.rows() if source.task.accepts(row)]
                actual = source.task.result_frame(source.rows(), decisions)
                # Gold uses the independent original CSV, not the materialized frame.
                actual_quality = evaluate_original(os.environ['SEMBENCH_CHECKOUT'], query, actual, original)
                expected_quality = evaluate_original(os.environ['SEMBENCH_CHECKOUT'], query, expected, original)
                self.assertEqual(actual_quality, expected_quality)
                self.assertGreater(actual_quality['original_metric']['precision'], 0)
                pd.testing.assert_frame_equal(actual, expected)
                pd.testing.assert_frame_equal(source.labeled_frame()[['reviewId']], original[['reviewId']])

    def test_official_profile_never_silently_uses_historical_or_fixture_bytes(self):
        with self.assertRaisesRegex(ValueError, "separately locked"):
            prepare_movie_source(self.raw, self.root / "official-wrong", expected_csv_sha256=self.digest,
                max_rows=11, role="sembench_movie_official", task="movie-q1")
        self.assertFalse((self.root / "official-wrong").exists())

    def test_explicit_empty_selector_is_not_a_default_q3(self):
        with self.assertRaises(ValueError):
            prepare_movie_source(self.raw, self.root / "invalid-task", expected_csv_sha256=self.digest,
                max_rows=11, task="")

    def test_same_prompt_does_not_allow_q1_q2_source_binding_confusion(self):
        from src.experiments.lotus_records_movie import _model_worker
        source = self.source(2)
        with patch("subprocess.Popen", side_effect=AssertionError("no owner may start")), \
                self.assertRaisesRegex(ValueError, "task differs"):
            _model_worker(dict(task="movie-q1", backend="daft-native-local-model"), source,
                None, source.task.method("fixture"), None, None, None, self.root)

    @unittest.skipUnless(os.environ.get("SEMBENCH_CHECKOUT"), "requires pinned CPU libraries")
    def test_same_ray_data_graph_consumes_q1_q2_q3_scan_contracts(self):
        import lotus
        from src.baselines.text.frameworks import ray_data_lotus_records
        from src.semantic_methods.lotus import engine as lotus_engine
        from src.semantic_methods.lotus.records import RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        from tests.baselines.text.test_ray_data_lotus_records import NativeRecordsTests
        real = (lotus_engine.LotusFilterMethod, lotus_engine.bind_filter_method, lotus_engine.validate_filter_source)
        for query in (1, 2, 3, 1):
            source = self.source(query, "-ray-" + str(len(list(self.root.iterdir()))))
            fixture = NativeRecordsTests()
            fixture.rows = source.rows()
            stack, events, _, _ = fixture._runtime()
            with stack, patch.object(lotus_engine, "LotusFilterMethod", real[0]), \
                    patch.object(lotus_engine, "bind_filter_method", real[1]), \
                    patch.object(lotus_engine, "validate_filter_source", real[2]):
                rows = list(ray_data_lotus_records.evaluate(source, source.task.method("openai/fixture"),
                    EngineSpec("/fixture/model", "/fixture/model-python", "/fixture/driver-python"),
                    GenerationSpec(), RecordsResources(), query_id="task-graph"))
            self.assertEqual({r["source_position"] for r in rows},
                {r["source_position"] for r in source.expected("task-graph")})
            self.assertEqual(any(e[0] == "filter" for e in events), query != 1)

    @unittest.skipUnless(os.environ.get("SEMBENCH_CHECKOUT"), "requires pinned CPU libraries")
    def test_same_production_core_keeps_task_binding_outside_its_scheduler(self):
        import lotus
        from src.execution_provider.adapters.lotus_records_core import SemLoomRecordsCore
        from src.experiments.lotus_records_movie import run_movie_query
        from src.semantic_methods.lotus import engine as lotus_engine
        from src.semantic_methods.lotus.records import RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        from tests.execution_provider.test_lotus_records_core import Runtime
        real = (lotus_engine.LotusFilterMethod, lotus_engine.bind_filter_method, lotus_engine.validate_filter_source)
        for query in (1, 2, 3, 1):
            source = self.source(query, "-core-" + str(len(list(self.root.iterdir()))))
            method = source.task.method("openai/fixture")
            with Runtime(source.rows()), patch.object(lotus_engine, "LotusFilterMethod", real[0]), \
                    patch.object(lotus_engine, "bind_filter_method", real[1]), \
                    patch.object(lotus_engine, "validate_filter_source", real[2]):
                owner = SemLoomRecordsCore(method,
                    EngineSpec("/fixture/model", "/fixture/model-python", "/fixture/driver-python"),
                    GenerationSpec(), RecordsResources(prepare_batch_rows=1, model_batch_rows=1))
                try:
                    report = run_movie_query(source, os.environ["SEMBENCH_CHECKOUT"], method, GenerationSpec(),
                        lambda: owner.evaluate(source, query_id="task-core"), query_id="task-core")
                    self.assertTrue(report["production_core_used"])
                    self.assertEqual(report["calls"]["counts"]["succeeded"], len(source.expected("task-core")))
                    source.task.validate_result(report["result"], source.rows(), report["decisions"])
                finally:
                    owner.close()

    @unittest.skipUnless(os.environ.get("SEMBENCH_CHECKOUT"), "requires pinned CPU LOTUS and SemBench checkout")
    def test_q1_q2_q3_and_back_switch_match_original_queries_without_state_leak(self):
        import lotus
        from src.experiments.lotus_records_movie import fixed_response_qualification
        from src.serving.backends.vllm_engine import GenerationSpec, VllmEngine
        before = (lotus.settings.lm, lotus.settings.enable_cache, lotus.settings.serialization_format)
        sources = {query: self.source(query) for query in (1, 2, 3)}
        expected_results = {1: {"reviewId": ["duplicate", "same", "r3", "same", "r5"]},
            2: {"reviewId": ["same", "same", "r5", "r7", "r8"]}, 3: {"positive_review_cnt": 6}}
        seen = {}
        with patch.object(VllmEngine, "__init__", side_effect=AssertionError("no model")), \
                patch("litellm.batch_completion", side_effect=AssertionError("no network inference")):
            for ordinal, query in enumerate((1, 2, 3, 1, 3, 2)):
                source = sources[query]
                rows = {r["row_id"]: r for r in source.rows()}
                responses = [dict(row_id=r["row_id"], text="Answer: True" if "good" in rows[r["row_id"]]["reviewText"] else "Answer: False",
                    prompt_tokens=2, completion_tokens=1) for r in source.expected("switch")]
                report = fixed_response_qualification(source, os.environ["SEMBENCH_CHECKOUT"],
                    source.task.method("openai/task-switch-fixture"), GenerationSpec(), responses,
                    query_id="switch-" + str(ordinal))
                self.assertEqual(report["result"], expected_results[query])
                self.assertTrue(all(report["native_reference"][k] for k in (
                    "messages_equal", "decisions_equal", "result_equal", "lm_stats_equal")))
                self.assertIs(report["native_reference"]["count_equal"], True if query == 3 else None)
                self.assertEqual(report["inference_calls"], 0)
                self.assertEqual((lotus.settings.lm, lotus.settings.enable_cache, lotus.settings.serialization_format), before)
                self.assertTrue(all("_execution_row_id" not in r for r in source.rows()))
                if query in seen:
                    self.assertEqual((report["result"], report["quality"], report["lm"]), seen[query])
                seen[query] = (report["result"], report["quality"], report["lm"])


if __name__ == "__main__":
    unittest.main()
