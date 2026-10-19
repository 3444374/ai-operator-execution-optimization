"""Source-index materialization contracts; original LOTUS uses fixed CPU responses."""

from copy import deepcopy
import unittest
from unittest.mock import patch

import pandas as pd

from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusFilterMapExtract, LotusRecordChain, LotusRecordMethod,
    MapMethod, RecordOccurrence, StageSpec, records_to_frame, validate_chain_source,
)
from src.semantic_methods.lotus.records import FilterMethod
from src.serving.backends.vllm_engine import GenerationSpec


MODEL = "openai/local-fixture"
INSTRUCTION = "Keep the review: {reviewText}"
COLUMNS = ("reviewText", "row_id", "source_index", "source_position")


def source_frame(index):
    return pd.DataFrame([
        dict(reviewText="review-" + str(position), row_id="business-row-" + str(position),
             source_index="business-index-" + str(position), source_position="business-position")
        for position in range(len(index))
    ], columns=COLUMNS, index=index, dtype=object)


def occurrences(frame):
    return [RecordOccurrence(
        "occurrence-" + str(position), position, label, frame.iloc[position].to_dict(),
        source_refs={"reviews": {"row_id": "source-" + str(position), "source_position": position}},
    ) for position, label in enumerate(frame.index.tolist())]


def terminal(prepared, decision):
    return dict(prepared, status="succeeded", text="Answer: " + str(decision),
                prompt_tokens=11, completion_tokens=4, total_tokens=15,
                finish_reason="stop", engine_request_id="fixture-" + prepared["call_id"],
                submission_confirmed=True, logprobs_json="null")


def filter_stage():
    return StageSpec("filter", FilterMethod(MODEL, INSTRUCTION), GenerationSpec())


class FrameIndexTests(unittest.TestCase):
    def assert_source_unchanged(self, source, before, records, original_records):
        pd.testing.assert_frame_equal(source, before)
        self.assertEqual(records, original_records)

    def test_empty_integer_metadata_keeps_dtype_name_and_source(self):
        for dtype in ("int32", "int64", "uint64"):
            with self.subTest(dtype=dtype):
                source = source_frame(pd.Index([1, 14, 1], dtype=dtype, name="source-label"))
                before, records = source.copy(deep=True), occurrences(source)
                saved = deepcopy(records)
                actual = records_to_frame([], columns=source.columns, source_index=source.index)
                pd.testing.assert_index_equal(actual.index, source.index[:0], exact=True)
                self.assertEqual(list(actual.columns), list(COLUMNS))
                self.assert_source_unchanged(source, before, records, saved)

    def test_empty_string_metadata_keeps_object_or_extension_dtype(self):
        for dtype in (object, "string"):
            with self.subTest(dtype=dtype):
                index = pd.Index(["left", "right", "left"], dtype=dtype, name="text-label")
                actual = records_to_frame([], columns=COLUMNS, source_index=index)
                pd.testing.assert_index_equal(actual.index, index[:0], exact=True)

    def test_empty_index_preserves_subclass_and_metadata(self):
        for index in (
            pd.RangeIndex(3, 15, 4, name="range-label"),
            pd.CategoricalIndex(["a", "b", "a"], categories=["b", "a", "unused"],
                                ordered=True, name="category-label"),
        ):
            with self.subTest(index_type=type(index).__name__):
                before = index.copy(deep=True)
                actual = records_to_frame([], columns=COLUMNS, source_index=index)
                self.assertIs(type(actual.index), type(index))
                pd.testing.assert_index_equal(actual.index, index[:0], exact=True)
                pd.testing.assert_index_equal(index, before, exact=True)

    def test_empty_source_preserves_explicit_index_metadata(self):
        for index in (
            pd.Index([], dtype="int32", name="empty-integer"),
            pd.Index([], dtype="string", name="empty-text"),
            pd.RangeIndex(0, name="empty-range"),
            pd.CategoricalIndex([], categories=["a", "b"], name="empty-category"),
        ):
            with self.subTest(index_type=type(index).__name__, dtype=str(index.dtype)):
                actual = records_to_frame([], columns=COLUMNS, source_index=index)
                pd.testing.assert_index_equal(actual.index, index[:0], exact=True)

    def test_survivors_keep_integer_dtype_name_and_occurrence_order(self):
        for dtype in ("int32", "int64", "uint64"):
            with self.subTest(dtype=dtype):
                source = source_frame(pd.Index([17, 18, 19], dtype=dtype, name="integer-label"))
                records = occurrences(source)
                selected = [records[2], records[0]]
                before, saved = source.copy(deep=True), deepcopy(selected)
                actual = records_to_frame(selected, columns=COLUMNS, source_index=source.index)
                pd.testing.assert_frame_equal(actual, source.iloc[[0, 2]])
                self.assert_source_unchanged(source, before, selected, saved)

    def test_survivors_keep_string_dtype_and_duplicate_labels(self):
        for dtype in (object, "string"):
            with self.subTest(dtype=dtype):
                source = source_frame(pd.Index(["same", "other", "same"], dtype=dtype,
                                              name="duplicate-label"))
                records = occurrences(source)
                selected = [records[2], records[0]]
                before, saved = source.copy(deep=True), deepcopy(selected)
                actual = records_to_frame(selected, columns=COLUMNS, source_index=source.index)
                pd.testing.assert_frame_equal(actual, source.iloc[[0, 2]])
                self.assert_source_unchanged(source, before, selected, saved)

    def test_default_without_metadata_keeps_legacy_inferred_index(self):
        empty = records_to_frame([], columns=COLUMNS)
        pd.testing.assert_index_equal(empty.index, pd.Index([], dtype=object), exact=True)
        source = source_frame(pd.Index([17, 19], dtype="int32", name="not-in-records"))
        actual = records_to_frame(occurrences(source))
        expected = pd.DataFrame(source.to_dict("records"), columns=COLUMNS, index=[17, 19])
        pd.testing.assert_frame_equal(actual, expected)

    def test_invalid_metadata_and_multiindex_are_rejected(self):
        for value in ([1, 14], pd.MultiIndex.from_tuples([("a", 1)])):
            with self.subTest(index_type=type(value).__name__), self.assertRaisesRegex(
                TypeError, "source_index must be a single pandas Index"
            ):
                records_to_frame([], columns=COLUMNS, source_index=value)

    def test_metadata_must_not_convert_occurrence_label_values(self):
        for labels, metadata in (
            (["1", "14"], pd.Index([1, 14], dtype="int64")),
            ([1, 14], pd.Index(["1", "14"], dtype="string")),
            ([1], pd.Index([True], dtype=bool)),
            ([1], pd.Index([1.0], dtype="float64")),
        ):
            with self.subTest(labels=labels):
                records = occurrences(source_frame(pd.Index(labels)))
                saved = deepcopy(records)
                with self.assertRaisesRegex(ValueError, "changed occurrence labels"):
                    records_to_frame(records, columns=COLUMNS, source_index=metadata)
                self.assertEqual(records, saved)


class EmptyFramePrototypeTests(unittest.TestCase):
    def test_empty_prototype_preserves_complete_schema_without_mutation(self):
        prototype = pd.DataFrame({
            "body": pd.Series([], dtype="string"),
            "amount": pd.Series([], dtype="Int64"),
            "kind": pd.Series(pd.Categorical([], categories=["b", "a"], ordered=True)),
            "derived0": pd.Series([], dtype="float64"),
        }, index=pd.Index([], dtype="int32", name="source-label"))
        prototype.columns.name = "business-fields"
        before = prototype.copy(deep=True)
        actual = records_to_frame([], empty_prototype=prototype)
        pd.testing.assert_frame_equal(actual, before)
        self.assertIsNot(actual, prototype)
        actual["derived0"] = actual["derived0"].astype(object)
        pd.testing.assert_frame_equal(prototype, before)

    def test_explicit_source_index_overrides_only_prototype_index(self):
        prototype = pd.DataFrame({"derived0": pd.Series([], dtype="float64")})
        source_index = pd.Index([17, 18, 17], dtype="uint64", name="input-label")
        actual = records_to_frame([], columns=["derived0"], source_index=source_index,
                                  empty_prototype=prototype)
        expected = prototype.copy(deep=True)
        expected.index = source_index[:0]
        pd.testing.assert_frame_equal(actual, expected)
        pd.testing.assert_index_equal(prototype.index, pd.RangeIndex(0))

    def test_prototype_requires_an_empty_frame_with_unique_string_columns(self):
        for value, error in (
            ({"body": "object"}, TypeError),
            (pd.DataFrame({"body": ["not-empty"]}), ValueError),
            (pd.DataFrame(columns=["body", "body"]), ValueError),
            (pd.DataFrame(columns=[1]), ValueError),
            (pd.DataFrame(index=pd.MultiIndex.from_tuples([], names=["a", "b"])), TypeError),
        ):
            with self.subTest(value_type=type(value).__name__), self.assertRaises(error):
                records_to_frame([], empty_prototype=value)

    def test_explicit_empty_columns_must_match_the_prototype_in_order(self):
        prototype = pd.DataFrame(columns=["body", "derived0"])
        for columns in (["body"], ["derived0", "body"], ["body", "derived0", "other"]):
            with self.subTest(columns=columns), self.assertRaisesRegex(ValueError, "columns"):
                records_to_frame([], columns=columns, empty_prototype=prototype)

    def test_nonempty_records_keep_inference_and_actual_extract_keys(self):
        records = [RecordOccurrence("row-0", 0, 17,
                                    {"body": "text", "derived0": "generated", "": "extra"})]
        prototype = pd.DataFrame({"body": pd.Series([], dtype="string"),
                                  "derived0": pd.Series([], dtype="float64")})
        before = prototype.copy(deep=True)
        actual = records_to_frame(records, empty_prototype=prototype)
        expected = records_to_frame(records)
        pd.testing.assert_frame_equal(actual, expected)
        self.assertEqual(actual.loc[17, "derived0"], "generated")
        self.assertEqual(actual.loc[17, ""], "extra")
        pd.testing.assert_frame_equal(prototype, before)


class OriginalLotusFilterIndexTests(unittest.TestCase):
    def setUp(self):
        import lotus

        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        self.provider = patch("litellm.completion", side_effect=AssertionError("no provider calls"))
        self.provider.start()
        self.addCleanup(self.provider.stop)
        validate_chain_source()

    def original_filter(self, frame, responses):
        import lotus
        from src.semantic_methods.lotus.engine import engine_response

        method = LotusRecordMethod(filter_stage().bind())
        seen = []

        def respond(data, _options, *_args):
            seen.extend(data)
            self.assertEqual(len(data), len(responses))
            return [engine_response(terminal({"call_id": "native-" + str(i)}, value), method.lm)
                    for i, value in enumerate(responses)]

        method.lm._process_uncached_messages = respond
        with lotus.settings.context(lm=method.lm):
            result = frame.sem_filter(INSTRUCTION)
        return result, seen

    def chain_filter(self, source, responses):
        calls = []

        def infer(prepared, *, generation):
            calls.extend(prepared)
            return [terminal(row, value) for row, value in zip(prepared, responses, strict=True)]

        chain = LotusRecordChain((filter_stage(),), query_id="index-query")
        result = chain.run_batch(occurrences(source), infer, columns=list(source.columns))
        return result, calls

    def test_all_false_matches_original_lotus_empty_filter_index(self):
        for index in (
            pd.Index([1, 14, 1], dtype="int64", name="original-integer"),
            pd.Index([1, 14, 1], dtype="int32", name="original-int32"),
            pd.Index(["a", "b", "a"], dtype=object, name="original-text"),
            pd.Index(["a", "b", "a"], dtype="string", name="original-string"),
        ):
            with self.subTest(dtype=str(index.dtype)):
                source = source_frame(index)
                before = source.copy(deep=True)
                expected, native_calls = self.original_filter(source, [False] * len(source))
                result, chain_calls = self.chain_filter(source, [False] * len(source))
                actual = records_to_frame(result["records"], columns=result["columns"],
                                          source_index=source.index)
                pd.testing.assert_frame_equal(actual, expected)
                self.assertEqual(len(native_calls), len(chain_calls))
                self.assertEqual(len(result["calls"]), len(source))
                pd.testing.assert_frame_equal(source, before)

    def test_empty_source_matches_original_filter_with_no_inference(self):
        source = source_frame(pd.Index([], dtype="int32", name="empty-source"))
        expected, native_calls = self.original_filter(source, [])
        result, chain_calls = self.chain_filter(source, [])
        actual = records_to_frame(result["records"], columns=result["columns"],
                                  source_index=source.index)
        pd.testing.assert_frame_equal(actual, expected)
        self.assertEqual(native_calls, [])
        self.assertEqual(chain_calls, [])
        self.assertEqual(result["calls"], [])

    def test_empty_map_prototype_matches_original_new_and_overwritten_columns(self):
        import lotus

        for output_column in ("derived0", "reviewText"):
            with self.subTest(output_column=output_column):
                source = source_frame(pd.Index([], dtype="int32", name="empty-map"))
                before = source.copy(deep=True)
                method = LotusRecordMethod(StageSpec("map", MapMethod(MODEL, "Summarize {reviewText}"),
                    GenerationSpec(max_tokens=64, stop=())).bind())
                seen = []

                def empty(data, *_args, **_kwargs):
                    seen.extend(data)
                    self.assertEqual(data, [])
                    return []

                method.lm._process_uncached_messages = empty
                with lotus.settings.context(lm=method.lm):
                    expected = source.sem_map("Summarize {reviewText}", suffix=output_column)
                actual = records_to_frame([], columns=expected.columns, source_index=source.index,
                                          empty_prototype=expected)
                pd.testing.assert_frame_equal(actual, expected)
                self.assertEqual(str(actual[output_column].dtype), "float64")
                self.assertEqual(seen, [])
                pd.testing.assert_frame_equal(source, before)

    def test_empty_filter_prototype_preserves_source_extension_column_dtype(self):
        source = source_frame(pd.Index([], dtype="int32", name="empty-filter"))
        source["reviewText"] = source["reviewText"].astype("string")
        source["business-count"] = pd.Series([], dtype="Int64", index=source.index)
        expected, seen = self.original_filter(source, [])
        actual = records_to_frame([], columns=expected.columns, source_index=source.index,
                                  empty_prototype=expected)
        pd.testing.assert_frame_equal(actual, expected)
        self.assertEqual(seen, [])

    def test_all_false_fme_never_calls_map_or_extract_and_preserves_inputs(self):
        source = source_frame(pd.Index([1, 14, 1], dtype="int64", name="fme-label"))
        records = occurrences(source)
        before_source, before_records = source.copy(deep=True), deepcopy(records)
        stages = (
            filter_stage(),
            StageSpec("map", MapMethod(MODEL, "Summarize {reviewText}"),
                      GenerationSpec(max_tokens=64, stop=())),
            StageSpec("extract", ExtractMethod(MODEL, ("summary",), {"aspect": "topic"}),
                      GenerationSpec(max_tokens=64, stop=(), response_format={"type": "json_object"})),
        )
        seen = []

        def infer(prepared, *, generation):
            self.assertEqual({row["algorithm_stage"] for row in prepared}, {"filter"})
            seen.append(deepcopy(prepared))
            return [terminal(row, False) for row in prepared]

        result = LotusFilterMapExtract(stages, query_id="empty-fme").run_batch(records, infer)
        actual = records_to_frame(result["records"], columns=result["columns"],
                                  source_index=source.index)
        pd.testing.assert_index_equal(actual.index, source.index[:0], exact=True)
        self.assertEqual(result["records"], [])
        self.assertEqual(len(seen), 1)
        self.assertEqual(len(result["calls"]), len(records))
        self.assertEqual({call["algorithm_stage"] for call in result["calls"]}, {"filter"})
        self.assertEqual(records, before_records)
        pd.testing.assert_frame_equal(source, before_source)


if __name__ == "__main__":
    unittest.main()
