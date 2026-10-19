"""Pinned LOTUS differentials over deterministic responses; no model or provider."""

from copy import deepcopy
from dataclasses import asdict
import importlib.util
import json
import unittest

from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusFilterMapExtract, LotusRecordMethod, MapMethod,
    RecordOccurrence, StageSpec, records_to_frame, stage_identity, validate_chain_source,
)
from src.semantic_methods.lotus.records import FilterMethod
from src.serving.backends.vllm_engine import GenerationSpec


LIBRARIES = all(importlib.util.find_spec(name) is not None for name in ("lotus", "litellm", "pandas"))
REVIEW = "摄影很美，节奏紧凑，是一部值得推荐的电影。"
SUMMARY = "肯定摄影与节奏，整体积极。"


def stages():
    return (
        StageSpec("positive-filter", FilterMethod("openai/local-fixture"), GenerationSpec()),
        StageSpec("review-summary", MapMethod("openai/local-fixture", "概括 {reviewText}"),
                  GenerationSpec(max_tokens=64, stop=())),
        StageSpec("summary-fields", ExtractMethod("openai/local-fixture", ("summary",),
                  {"aspect": "电影方面", "attitude": "态度"}),
                  GenerationSpec(max_tokens=128, stop=(), response_format={"type": "json_object"})),
    )


def records():
    return [RecordOccurrence(
        "r" + str(label), position, label,
        dict(reviewText=text, reviewId="business-" + str(label), group_key="movie-a"),
        source_refs={"reviews": {"row_id": "r" + str(label), "source_position": position}},
    ) for position, (label, text) in enumerate(((17, REVIEW), (18, "情节拖沓，我没有看完。"), (19, REVIEW)))]


def engine_result(prepared, text):
    return dict(prepared, status="succeeded", text=text, prompt_tokens=11,
                completion_tokens=3, total_tokens=14, finish_reason="stop",
                engine_request_id="engine-" + prepared["call_id"],
                submission_confirmed=True, logprobs_json="null")


class FixtureInfer:
    def __init__(self, responses=None):
        self.responses = responses or {
            "positive-filter": ["True", "False", "True"],
            "review-summary": [SUMMARY, SUMMARY],
            "summary-fields": ['{"aspect":"摄影、节奏","attitude":"积极"}'] * 2,
        }
        self.batches = []

    def __call__(self, prepared, *, generation):
        stage = prepared[0]["algorithm_stage"]
        self.batches.append((deepcopy(prepared), asdict(generation)))
        return [engine_result(row, text) for row, text in zip(prepared, self.responses[stage], strict=True)]


class IdentityTests(unittest.TestCase):
    def test_stage_and_evaluation_are_separate_from_record_membership(self):
        record = records()[0]
        identities = [stage_identity("q", record, stage, call_index=call_index)
                      for stage, call_index in (("left/branch", 0), ("right/branch", 0), ("left/branch", 1))]
        self.assertEqual(len({r["call_id"] for r in identities}), 3)
        self.assertEqual({r["row_id"] for r in identities}, {"r17"})
        self.assertEqual(RecordOccurrence.from_dict(record.describe()), record)

    def test_values_and_named_source_references_are_owned_snapshots(self):
        values, refs = {"reviewText": REVIEW, "correlation_key": ["a", "b"]}, {"left": {"row_id": "a"}, "right": {"row_id": "b"}}
        record = RecordOccurrence("joined-occurrence", 1, 99, values, refs)
        values["correlation_key"].append("c")
        refs["right"]["row_id"] = "changed"
        self.assertEqual(record.values["correlation_key"], ["a", "b"])
        self.assertEqual(record.source_refs["right"]["row_id"], "b")


@unittest.skipUnless(LIBRARIES, "requires the pinned LOTUS CPU environment")
class LotusRecordChainTests(unittest.TestCase):
    def setUp(self):
        import lotus
        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        validate_chain_source()

    def test_three_rows_keep_occurrences_and_consume_new_map_column(self):
        original = records()
        chain = LotusFilterMapExtract(stages(), query_id="q")
        infer = FixtureInfer()
        result = chain.run_batch(original, infer)
        self.assertEqual([r.row_id for r in result["records"]], ["r17", "r19"])
        self.assertEqual([r.source_index for r in result["records"]], [17, 19])
        self.assertEqual([len(batch) for batch, _ in infer.batches], [3, 2, 2])
        self.assertEqual(len(result["calls"]), 7)
        self.assertEqual(len(infer.batches), 3)  # evaluations are not physical batches
        self.assertEqual(len({c["call_id"] for c in result["calls"]}), 7)
        self.assertEqual([c["row_id"] for c in result["calls"]].count("r18"), 1)
        for record in result["records"]:
            self.assertEqual(record.values["reviewText"], REVIEW)
            self.assertEqual(record.values["summary"], SUMMARY)
            self.assertEqual(record.values["attitude"], "积极")
            self.assertEqual(record.values["group_key"], "movie-a")
            self.assertEqual(record.source_refs, original[record.source_position].source_refs)
        for row in infer.batches[1][0]:
            self.assertIn(REVIEW, row["messages_json"])
        for row in infer.batches[2][0]:
            self.assertIn(SUMMARY, row["messages_json"])
            self.assertNotIn(REVIEW, row["messages_json"])
        by_id = {c["call_id"]: c for c in result["calls"]}
        for call in result["calls"]:
            expected_parents = 0 if call["algorithm_stage"] == "positive-filter" else 1
            self.assertEqual(len(call["parent_call_ids"]), expected_parents)
            for parent in call["parent_call_ids"]:
                self.assertEqual(by_id[parent]["row_id"], call["row_id"])
        self.assertNotIn("summary", original[0].values)

    def test_original_accessors_match_messages_parameters_stats_and_final_values(self):
        import lotus
        import pandas as pd
        from src.semantic_methods.lotus.engine import engine_response

        actual_infer = FixtureInfer()
        actual_chain = LotusFilterMapExtract(stages(), query_id="q")
        actual = actual_chain.run_batch(records(), actual_infer)
        native_methods = [LotusRecordMethod(s) for s in stages()]
        native_calls = []
        for method in native_methods:
            def respond(data, options, *_args, method=method):
                name = method.stage.stage_id
                native_calls.append(([m for m, _ in data], deepcopy(options)))
                return [engine_response(engine_result({"call_id": str(i)}, text), method.lm)
                        for i, text in enumerate(actual_infer.responses[name])]
            method.lm._process_uncached_messages = respond
        frame = pd.DataFrame([r.values for r in records()], index=[17, 18, 19])
        with lotus.settings.context(lm=native_methods[0].lm):
            frame = frame.sem_filter(stages()[0].method.instruction)
        with lotus.settings.context(lm=native_methods[1].lm):
            frame = frame.sem_map(stages()[1].method.instruction, suffix="summary")
        with lotus.settings.context(lm=native_methods[2].lm):
            frame = frame.sem_extract(["summary"], stages()[2].method.output_columns)
        pd.testing.assert_frame_equal(records_to_frame(actual["records"]), frame)
        for (prepared, generation), (messages, options), actual_method, native_method in zip(
            actual_infer.batches, native_calls, actual_chain.methods, native_methods, strict=True
        ):
            self.assertEqual([json.loads(p["messages_json"]) for p in prepared], messages)
            self.assertEqual(generation["max_tokens"], options["max_completion_tokens"])
            self.assertEqual(generation.get("response_format"), options.get("response_format"))
            self.assertEqual(asdict(actual_method.lm.stats), asdict(native_method.lm.stats))

    def test_each_call_has_its_own_generation_and_format(self):
        chain = LotusFilterMapExtract(stages(), query_id="q")
        before = chain.describe()
        infer = FixtureInfer()
        chain.run_batch(records(), infer)
        self.assertEqual([options["max_tokens"] for _, options in infer.batches], [8, 64, 128])
        self.assertEqual([options.get("response_format") for _, options in infer.batches],
                         [None, None, {"type": "json_object"}])
        self.assertEqual(chain.describe(), before)
        self.assertEqual(LotusFilterMapExtract.from_dict(before).describe(), before)

    def test_map_preserves_whitespace_and_extract_allows_keys_and_stringifies_values(self):
        infer = FixtureInfer({
            "positive-filter": ["True", "False", "True"],
            "review-summary": ["  Answer: summary\n", "  Answer: summary\n"],
            "summary-fields": ['{"reviewId":42,"row_id":"data-key","flag":true,"missing":null}', '{}'],
        })
        result = LotusFilterMapExtract(stages(), query_id="q").run_batch(records(), infer)
        first, second = result["records"]
        self.assertEqual(first.values["summary"], "  Answer: summary\n")
        self.assertEqual(first.values["reviewId"], "42")
        self.assertEqual(first.values["flag"], "True")
        self.assertEqual(first.values["missing"], "None")
        self.assertEqual(first.values["row_id"], "data-key")
        self.assertEqual(first.row_id, "r17")
        self.assertNotIn("aspect", first.values)
        self.assertEqual(second.values["reviewId"], "business-19")
        self.assertIsNone(records_to_frame(result["records"]).loc[19, "flag"])
        self.assertTrue(all(c["row_id"] != "data-key" for c in result["calls"]))

    def test_invalid_json_keeps_original_parser_fallback(self):
        infer = FixtureInfer()
        infer.responses["summary-fields"] = ["not-json", '```json\n{"aspect":"x"}\n```']
        result = LotusFilterMapExtract(stages(), query_id="q").run_batch(records(), infer)
        self.assertTrue(all("aspect" not in r.values for r in result["records"]))
        self.assertEqual(len(result["calls"]), 7)

    def test_non_object_json_raises_original_error_and_retains_all_call_usage(self):
        for text in ("null", "[]", "1", '"text"'):
            with self.subTest(text=text):
                infer = FixtureInfer()
                infer.responses["summary-fields"] = [text, "{}"]
                with self.assertRaises(AttributeError) as caught:
                    LotusFilterMapExtract(stages(), query_id="q").run_batch(records(), infer)
                self.assertEqual(len(caught.exception.chain_calls), 7)
                self.assertEqual(sum(c["prompt_tokens"] for c in caught.exception.chain_calls), 77)

    def test_all_discarded_rows_produce_no_map_or_extract_work(self):
        infer = FixtureInfer()
        infer.responses["positive-filter"] = ["False"] * 3
        result = LotusFilterMapExtract(stages(), query_id="q").run_batch(records(), infer)
        self.assertEqual(result["records"], [])
        self.assertEqual(len(result["calls"]), 3)
        self.assertEqual(len(infer.batches), 1)
        self.assertEqual(list(records_to_frame([], columns=result["columns"]).columns),
                         ["reviewText", "reviewId", "group_key", "summary"])

    def test_unknown_map_result_stops_extract_and_preserves_prior_calls(self):
        original = FixtureInfer()
        def infer(prepared, *, generation):
            result = original(prepared, generation=generation)
            if prepared[0]["algorithm_stage"] == "review-summary":
                result[1].update(status="unknown", submission_confirmed=False)
            return result
        with self.assertRaisesRegex(RuntimeError, "unknown") as caught:
            LotusFilterMapExtract(stages(), query_id="q").run_batch(records(), infer)
        self.assertEqual(len(caught.exception.chain_calls), 5)
        self.assertEqual(len(original.batches), 2)

    def test_transport_error_keeps_declared_evaluations_unconfirmed(self):
        original = FixtureInfer()
        def infer(prepared, *, generation):
            if prepared[0]["algorithm_stage"] == "review-summary":
                raise TimeoutError("fixture transport")
            return original(prepared, generation=generation)
        with self.assertRaises(TimeoutError) as caught:
            LotusFilterMapExtract(stages(), query_id="q").run_batch(records(), infer)
        self.assertEqual(len(caught.exception.chain_calls), 5)
        self.assertEqual([c["status"] for c in caught.exception.chain_calls],
                         ["succeeded"] * 3 + ["unknown"] * 2)
        self.assertTrue(all(len(c["parent_call_ids"]) == 1 for c in caught.exception.chain_calls[-2:]))

    def test_missing_result_retains_known_usage_and_unknown_missing_identity(self):
        original = FixtureInfer()
        def infer(prepared, *, generation):
            result = original(prepared, generation=generation)
            return result[:1] if prepared[0]["algorithm_stage"] == "review-summary" else result
        with self.assertRaisesRegex(ValueError, "omitted") as caught:
            LotusFilterMapExtract(stages(), query_id="q").run_batch(records(), infer)
        self.assertEqual(len(caught.exception.chain_calls), 5)
        self.assertEqual(caught.exception.chain_calls[-2]["prompt_tokens"], 11)
        self.assertEqual(caught.exception.chain_calls[-1]["status"], "unknown")
        self.assertEqual(len(original.batches), 2)

    def test_duplicate_labels_keep_occurrences_and_original_final_broadcast(self):
        inputs = records()
        inputs[2] = RecordOccurrence("r19", 2, 17, inputs[2].values)
        infer = FixtureInfer()
        infer.responses["summary-fields"] = ['{"reviewId":"first","aspect":"x"}', '{"aspect":"last"}']
        result = LotusFilterMapExtract(stages(), query_id="q").run_batch(inputs, infer)
        self.assertEqual(len(result["calls"]), 7)
        self.assertEqual([r.row_id for r in result["records"]], ["r17", "r19"])
        frame = records_to_frame(list(reversed(result["records"])))
        self.assertEqual(list(frame.index), [17, 17])
        self.assertEqual(list(frame["aspect"]), ["last", "last"])
        self.assertEqual(list(frame["reviewId"]), ["first", "first"])

    def test_extract_dependency_is_checked_before_inference(self):
        selected = list(stages())
        selected[-1] = StageSpec("extract-source", ExtractMethod("openai/local-fixture", ("reviewText",),
                                       {"aspect": None}), selected[-1].generation)
        with self.assertRaisesRegex(ValueError, "new Map output"):
            LotusFilterMapExtract(tuple(selected), query_id="q")

    def test_cache_and_serialization_policy_are_rechecked_before_calls(self):
        import lotus
        chain = LotusFilterMapExtract(stages(), query_id="q")
        infer = FixtureInfer()
        with lotus.settings.context(enable_cache=True), self.assertRaisesRegex(ValueError, "caching disabled"):
            chain.run_batch(records(), infer)
        self.assertEqual(infer.batches, [])


if __name__ == "__main__":
    unittest.main()
