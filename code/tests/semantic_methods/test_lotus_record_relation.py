"""CPU relation-boundary differentials; fixed responses, no model or native runtime."""

from copy import deepcopy
from dataclasses import asdict
import importlib.util
import json
import unittest
from unittest.mock import Mock

from src.baselines.text.frameworks.lotus_chain_results import (
    ChainBatchResults, ChainExecutionError, _source_binding,
)
from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusRecordChain, LotusRecordMethod, MapMethod, RecordOccurrence,
    StageSpec, TextFilterMethod, records_to_frame, validate_chain_source,
)
from src.semantic_methods.lotus.record_relation import complete_extract_relation
from src.serving.backends.vllm_engine import GenerationSpec


LIBRARIES = all(importlib.util.find_spec(name) is not None for name in ("lotus", "litellm", "pandas"))
QUERY = "extract-relation-cpu"
DOCUMENTS = ("source first review", "source separate review", "source later review", "source empty review")
OUTPUTS = (
    '{"shared_value":"drop earlier","retained":"first only","row_id":"data first","first_key":1}',
    '{"shared_value":"drop separate","second_key":"second"}',
    '{"shared_value":"keep broadcast","row_id":"data last","late_key":"late"}',
    '{}',
)


def source_records():
    return [RecordOccurrence(
        "occurrence-" + str(position), position, label,
        dict(reviewText=DOCUMENTS[position], count=position, active=position % 2 == 0,
             optional=None, nested={"members": [position, None]}, group_key="movie-a"),
        source_refs={"reviews": {"business_key": position, "labels": ["a", None]}},
    ) for position, label in enumerate((17, 29, 17, 53))]


def extract_stage():
    return StageSpec("extract-prefix", ExtractMethod("openai/local-fixture", ("reviewText",),
                     {"shared_value": "a textual result", "never_returned": "absent from responses"}),
                     GenerationSpec(max_tokens=96, stop=(), response_format={"type": "json_object"}),
                     model_role="extractor")


def model_result(prepared, text):
    return dict(prepared, status="succeeded", text=text, prompt_tokens=11, completion_tokens=3,
                total_tokens=14, finish_reason="stop", submission_confirmed=True,
                engine_request_id="fixture-" + prepared["call_id"], logprobs_json="null")


def response_for(kind, message):
    """Choose fixtures from the prompt's data values, never occurrence identities."""
    document = json.dumps(message, ensure_ascii=False)
    if kind == "extract":
        return OUTPUTS[next(i for i, text in enumerate(DOCUMENTS) if text in document)]
    if any(text in document for text in DOCUMENTS):
        raise AssertionError("a successor consumed the original review instead of the extracted field")
    if kind == "filter":
        return "True" if "keep broadcast" in document else "False"
    if "keep broadcast" not in document:
        raise AssertionError("Map did not consume the completed broadcast value")
    return "mapped keep broadcast"


def infer_for(chain, observations):
    def infer(prepared, *, generation):
        stage = next(stage for stage in chain.stages if stage.stage_id == prepared[0]["algorithm_stage"])
        messages = [json.loads(row["messages_json"]) for row in prepared]
        observations.append((stage.stage_id, deepcopy(messages), asdict(generation)))
        return [model_result(row, response_for(stage.kind, message))
                for row, message in zip(prepared, messages, strict=True)]
    return infer


def envelope(batch_id, inputs, result, *, error_type=None, raw_results=()):
    return dict(batch_id=batch_id, inputs=[_source_binding(record) for record in inputs],
                records=[record.describe() for record in result["records"]],
                calls=result["calls"], stage_stats=result["stage_stats"], columns=result["columns"],
                raw_results=list(raw_results), raw_stage_id="extract-prefix", error_type=error_type)


@unittest.skipUnless(LIBRARIES, "requires the pinned LOTUS CPU environment")
class ExtractRelationTests(unittest.TestCase):
    def setUp(self):
        import lotus

        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        validate_chain_source()

    def prefix(self, inputs=None):
        inputs = source_records() if inputs is None else inputs
        chain = LotusRecordChain((extract_stage(),), query_id=QUERY)
        observations = []
        infer = infer_for(chain, observations)
        batches = [inputs[:2], inputs[2:]]
        results = [chain.run_batch(batch, infer) for batch in batches]
        accountant = ChainBatchResults(inputs, chain)
        # A label group straddles native batches; delivery order is unrelated to source order.
        accountant.accept(envelope("later", batches[1], results[1]))
        accountant.accept(envelope("earlier", batches[0], results[0]))
        accountant.verify_coverage()
        accountant.finalize_columns()
        collected = list(accountant.pending)
        relation = complete_extract_relation(
            collected, columns=accountant.columns, extract_stage=chain.stages[-1], query_id=QUERY,
            calls=accountant.calls, source_exhausted=True)
        return inputs, chain, observations, accountant, collected, relation

    def original_accessor(self, inputs, stages):
        import lotus
        import pandas as pd
        from src.semantic_methods.lotus.engine import engine_response

        observations, methods = [], [LotusRecordMethod(stage.bind()) for stage in stages]
        frame = pd.DataFrame([record.values for record in inputs], index=[record.source_index for record in inputs])
        for method in methods:
            def respond(data, options, *_args, method=method):
                messages = [message for message, _ in data]
                observations.append((method.stage.stage_id, deepcopy(messages), deepcopy(options)))
                return [engine_response(model_result({"call_id": str(i)}, response_for(method.stage.kind, message)),
                                        method.lm) for i, message in enumerate(messages)]
            method.lm._process_uncached_messages = respond
            with lotus.settings.context(lm=method.lm):
                if method.stage.kind == "extract":
                    frame = frame.sem_extract(list(method.method.input_columns), method.method.output_columns)
                elif method.stage.kind == "filter":
                    frame = frame.sem_filter(method.method.instruction, default=method.method.default)
                else:
                    frame = frame.sem_map(method.method.instruction, suffix=method.method.output_column)
        return frame, observations, methods

    def test_reverse_batch_delivery_matches_original_extract_and_preserves_occurrences(self):
        import pandas as pd

        inputs, chain, observations, accountant, collected, relation = self.prefix()
        expected, original_observations, original_methods = self.original_accessor(inputs, chain.stages)
        pd.testing.assert_frame_equal(records_to_frame(collected, columns=accountant.columns), expected)
        pd.testing.assert_frame_equal(records_to_frame(relation.records, columns=relation.columns), expected)
        self.assertEqual([record.source_position for record in collected], [2, 3, 0, 1])
        self.assertEqual([record.source_position for record in relation.records], [0, 1, 2, 3])
        self.assertEqual([record.source_index for record in relation.records], [17, 29, 17, 53])
        self.assertEqual(relation.columns, tuple(inputs[0].values) + (
            "shared_value", "retained", "row_id", "first_key", "second_key", "late_key"))
        self.assertNotIn("never_returned", relation.columns)
        self.assertEqual(relation.records[0].values["shared_value"], "keep broadcast")
        self.assertEqual(relation.records[2].values["shared_value"], "keep broadcast")
        self.assertEqual(relation.records[2].values["retained"], "first only")
        self.assertEqual(relation.records[2].values["first_key"], "1")
        self.assertEqual(relation.records[0].values["row_id"], "data last")
        self.assertIsNone(relation.records[1].values["late_key"])
        self.assertIsNone(relation.records[3].values["shared_value"])
        for actual, source in zip(relation.records, inputs, strict=True):
            self.assertEqual((actual.row_id, actual.source_position, actual.source_index, actual.source_refs),
                             (source.row_id, source.source_position, source.source_index, source.source_refs))
            self.assertEqual(actual.index_updates, {})
            for name in source.values:
                self.assertIs(type(actual.values[name]), type(source.values[name]))
                self.assertEqual(actual.values[name], source.values[name])
        self.assertEqual(tuple(accountant.calls), relation.call_snapshot)
        self.assertEqual(sum(call["total_tokens"] for call in relation.call_snapshot), 56)
        self.assertEqual([message for _, messages, _ in observations for message in messages],
                         original_observations[0][1])
        self.assertEqual(asdict(chain.methods[0].lm.stats), asdict(original_methods[0].lm.stats))
        self.assertEqual(len(accountant.calls), 4)

    def test_extract_overwrites_existing_source_column_and_missing_key_keeps_original_value(self):
        import pandas as pd

        inputs = source_records()
        for record in inputs:
            record.values["shared_value"] = "original value"
        before = [record.describe() for record in inputs]
        source_columns = tuple(inputs[0].values)
        _, chain, _, accountant, collected, relation = self.prefix(inputs)
        calls = deepcopy(accountant.calls)
        expected, _, _ = self.original_accessor(inputs, chain.stages)
        pd.testing.assert_frame_equal(records_to_frame(collected, columns=accountant.columns), expected)
        pd.testing.assert_frame_equal(records_to_frame(relation.records, columns=relation.columns), expected)
        self.assertEqual([record.values["shared_value"] for record in relation.records],
                         ["keep broadcast", "drop separate", "keep broadcast", "original value"])
        self.assertEqual(relation.columns[:len(source_columns)], source_columns)
        self.assertEqual(relation.columns.count("shared_value"), 1)
        self.assertEqual([record.describe() for record in inputs], before)
        self.assertEqual(accountant.calls, calls)
        self.assertEqual(relation.call_snapshot, tuple(calls))
        self.assertEqual(sum(call["total_tokens"] for call in relation.call_snapshot), 56)

    def test_label_dependencies_include_all_extract_writers_and_snapshots_are_isolated(self):
        inputs, chain, _, accountant, collected, relation = self.prefix()
        by_position = {call["source_position"]: call["call_id"] for call in accountant.calls}
        self.assertEqual(relation.parent_call_ids[(inputs[0].row_id, 0)], (by_position[0], by_position[2]))
        self.assertEqual(relation.parent_call_ids[(inputs[2].row_id, 2)], (by_position[0], by_position[2]))
        self.assertEqual(relation.parent_call_ids[(inputs[1].row_id, 1)], (by_position[1],))
        snapshot = deepcopy(relation.call_snapshot)
        collected[0].values["nested"]["members"].append("changed")
        collected[0].source_refs["reviews"]["labels"].append("changed")
        accountant.calls[0]["total_tokens"] = 999
        accountant.calls[0]["parent_call_ids"].append("changed")
        self.assertEqual(relation.records[2].values["nested"]["members"], [2, None])
        self.assertEqual(relation.records[2].source_refs["reviews"]["labels"], ["a", None])
        self.assertEqual(relation.call_snapshot, snapshot)
        relation.records[0].values["nested"]["members"].append("owned")
        self.assertEqual(inputs[0].values["nested"]["members"], [0, None])
        self.assertEqual(relation.records[2].values["nested"]["members"], [2, None])
        self.assertEqual(chain.query_id, QUERY)

    def test_missing_native_batch_is_rejected_by_shared_coverage_before_assembly(self):
        inputs = source_records()
        chain = LotusRecordChain((extract_stage(),), query_id=QUERY)
        partial = chain.run_batch(inputs[:2], infer_for(chain, []))
        accountant = ChainBatchResults(inputs, chain)
        accountant.accept(envelope("only-first", inputs[:2], partial))
        complete = Mock(wraps=complete_extract_relation)
        with self.assertRaisesRegex(ValueError, "omitted expected"):
            accountant.verify_coverage()
            complete(list(accountant.pending), columns=accountant.columns, extract_stage=chain.stages[-1],
                     query_id=QUERY, calls=accountant.calls, source_exhausted=True)
        complete.assert_not_called()
        self.assertEqual(len(accountant.calls), 2)
        self.assertEqual(sum(call["total_tokens"] for call in accountant.calls), 28)

    def test_matching_rows_without_successful_source_eof_are_rejected_with_call_snapshot(self):
        _, chain, _, accountant, collected, _ = self.prefix()
        before = deepcopy(accountant.calls)
        with self.assertRaisesRegex(ValueError, "source EOF") as caught:
            complete_extract_relation(collected, columns=accountant.columns, extract_stage=chain.stages[-1],
                                      query_id=QUERY, calls=accountant.calls, source_exhausted=False)
        self.assertEqual(caught.exception.chain_calls, before)
        caught.exception.chain_calls[0]["total_tokens"] = 999
        self.assertEqual(accountant.calls, before)

    def test_empty_relation_requires_eof_and_preserves_explicit_source_schema_only(self):
        stage = extract_stage()
        complete = complete_extract_relation([], columns=["reviewText", "group_key"], extract_stage=stage,
                                            query_id=QUERY, calls=[], source_exhausted=True)
        self.assertEqual(complete.records, ())
        self.assertEqual(complete.columns, ("reviewText", "group_key"))
        self.assertEqual((complete.parent_call_ids, complete.call_snapshot), ({}, ()))
        self.assertNotIn("never_returned", complete.columns)
        with self.assertRaisesRegex(ValueError, "source EOF"):
            complete_extract_relation([], columns=complete.columns, extract_stage=stage,
                                      query_id=QUERY, calls=[], source_exhausted=False)

    def test_failed_unknown_and_foreign_call_snapshots_cannot_become_a_completed_relation(self):
        _, chain, _, accountant, collected, _ = self.prefix()
        for problem in ("failed", "unknown", "foreign"):
            with self.subTest(problem=problem):
                calls = deepcopy(accountant.calls)
                calls[0]["query_id" if problem == "foreign" else "status"] = (
                    "another-query" if problem == "foreign" else problem)
                before = deepcopy(calls)
                with self.assertRaisesRegex(ValueError, "failed, unknown, or foreign") as caught:
                    complete_extract_relation(collected, columns=accountant.columns, extract_stage=chain.stages[-1],
                                              query_id=QUERY, calls=calls, source_exhausted=True)
                self.assertEqual(caught.exception.chain_calls, before)
                self.assertEqual(calls, before)

    def test_terminal_occurrence_identity_and_actual_schema_are_required(self):
        _, chain, _, accountant, collected, _ = self.prefix()
        for problem in ("missing", "duplicate", "identity", "schema"):
            with self.subTest(problem=problem):
                calls, columns = deepcopy(accountant.calls), list(accountant.columns)
                if problem == "missing":
                    calls.pop()
                elif problem == "duplicate":
                    calls.append(deepcopy(calls[0]))
                elif problem == "identity":
                    calls[0]["model_role"] = "foreign-role"
                else:
                    columns.remove("late_key")
                with self.assertRaises(ValueError) as caught:
                    complete_extract_relation(collected, columns=columns, extract_stage=chain.stages[-1],
                                              query_id=QUERY, calls=calls, source_exhausted=True)
                self.assertEqual(caught.exception.chain_calls, calls)

    def test_existing_suffix_consumes_broadcast_values_but_does_not_seed_prefix_parents(self):
        import pandas as pd

        inputs, prefix, prefix_observations, accountant, _, relation = self.prefix(source_records()[:3])
        suffix = LotusRecordChain((
            StageSpec("filter-broadcast", TextFilterMethod("openai/local-fixture", "Accept {shared_value}",
                                                         default=False), GenerationSpec()),
            StageSpec("map-broadcast", MapMethod("openai/local-fixture", "Describe {shared_value}",
                                                output_column="derived"), GenerationSpec(max_tokens=64, stop=())),
        ), query_id=QUERY)
        observations = []
        result = suffix.run_batch(list(relation.records), infer_for(suffix, observations))
        self.assertEqual([row.row_id for row in result["records"]], [inputs[0].row_id, inputs[2].row_id])
        self.assertEqual([row.values["derived"] for row in result["records"]], ["mapped keep broadcast"] * 2)
        self.assertEqual(len(result["calls"]), 5)
        expected, original_observations, _ = self.original_accessor(inputs, prefix.stages + suffix.stages)
        pd.testing.assert_frame_equal(records_to_frame(result["records"], columns=result["columns"]), expected)
        self.assertEqual([message for _, messages, _ in prefix_observations for message in messages],
                         original_observations[0][1])
        self.assertEqual([messages for _, messages, _ in observations],
                         [messages for _, messages, _ in original_observations[1:]])
        self.assertTrue(all(relation.parent_call_ids[(row.row_id, row.source_position)] for row in relation.records))
        # The existing stage runner has no parent seed API: this is a pending integration seam.
        self.assertTrue(all(call["parent_call_ids"] == [] for call in result["calls"][:3]))
        self.assertEqual(len(accountant.calls), 3)
        self.assertEqual(sum(call["total_tokens"] for call in relation.call_snapshot), 42)

    def test_sparse_none_still_hits_existing_prepare_guard_without_losing_prefix_usage(self):
        _, _, _, accountant, _, relation = self.prefix()
        suffix = LotusRecordChain((StageSpec("map-sparse", MapMethod("openai/local-fixture", "Read {late_key}"),
                                            GenerationSpec(max_tokens=64, stop=())),), query_id=QUERY)
        infer = Mock()
        snapshot = deepcopy(relation.call_snapshot)
        with self.assertRaisesRegex(ValueError, "present text input columns") as caught:
            suffix.run_batch(list(relation.records), infer)
        infer.assert_not_called()
        self.assertEqual(caught.exception.chain_calls, [])
        self.assertEqual(relation.call_snapshot, snapshot)
        self.assertEqual(sum(call["total_tokens"] for call in accountant.calls), 56)
        method = suffix.methods[0]
        frame = records_to_frame(relation.records, columns=relation.columns)
        docs = method.instructions.df2multimodal_info(frame, ["late_key"])
        messages = [method.instructions.map_formatter(method.lm, doc, method.instruction) for doc in docs]
        self.assertEqual(len(messages), 4)
        self.assertIn("None", json.dumps(messages[1], ensure_ascii=False))
        self.assertIsNone(relation.records[1].values["late_key"])

    def test_parser_failure_and_unknown_extract_stop_at_shared_success_boundary_with_usage(self):
        for problem in ("nonobject", "unknown"):
            with self.subTest(problem=problem):
                inputs = source_records()
                prefix = LotusRecordChain((extract_stage(),), query_id=QUERY)
                responses = infer_for(prefix, [])
                accountant = ChainBatchResults(inputs, prefix)
                first = prefix.run_batch(inputs[:2], responses)
                accountant.accept(envelope("first", inputs[:2], first))

                def broken(prepared, *, generation):
                    results = responses(prepared, generation=generation)
                    if problem == "nonobject":
                        results[0]["text"] = "[]"
                    else:
                        results[1].update(status="unknown", submission_confirmed=False,
                                          prompt_tokens=None, completion_tokens=None, total_tokens=None)
                    return results

                error_type = AttributeError if problem == "nonobject" else RuntimeError
                with self.assertRaises(error_type) as caught:
                    prefix.run_batch(inputs[2:], broken)
                failure = caught.exception
                failed = dict(records=[], calls=failure.chain_calls,
                              stage_stats=failure.chain_stage_stats, columns=[])
                complete = Mock(wraps=complete_extract_relation)
                with self.assertRaises(ChainExecutionError) as shared:
                    accountant.accept(envelope("failed", inputs[2:], failed, error_type=type(failure).__name__,
                                               raw_results=failure.chain_raw_results))
                    complete(list(accountant.pending), columns=accountant.columns, extract_stage=prefix.stages[-1],
                             query_id=QUERY, calls=accountant.calls, source_exhausted=True)
                complete.assert_not_called()
                self.assertEqual(shared.exception.remote_error_type, error_type.__name__)
                self.assertEqual(shared.exception.chain_calls, accountant.calls)
                self.assertEqual(len(accountant.calls), 4)
                self.assertEqual(sum(call["total_tokens"] or 0 for call in accountant.calls),
                                 56 if problem == "nonobject" else 42)
                self.assertEqual([call["status"] for call in accountant.calls],
                                 ["succeeded"] * (4 if problem == "nonobject" else 3)
                                 + ([] if problem == "nonobject" else ["unknown"]))


if __name__ == "__main__":
    unittest.main()
