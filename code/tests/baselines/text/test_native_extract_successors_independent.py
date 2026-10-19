"""Independent Extract-successor contracts; native-library wiring stays pending.

The empty-key successor test deliberately asserts original LOTUS semantics. On
b447fc37 it is a known upstream failure, not a passing rejection contract.
"""

from copy import deepcopy
from dataclasses import asdict
import json
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch
from src.baselines.text.frameworks.lotus_chain_results import ChainBatchResults, _source_binding, record_row
from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.semantic_methods.lotus.engine import engine_response
from src.semantic_methods.lotus.record_chain import LotusRecordMethod, RecordOccurrence, records_to_frame
from src.semantic_methods.lotus.record_relation import complete_extract_relation
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.baselines.text.native_extract_successor_fixture import (
    FixtureInfer, relation_plan, source_records,
)


class ContractCase(unittest.TestCase):
    def setUp(self):
        import lotus

        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)
        no_child = patch("subprocess.Popen", side_effect=AssertionError("pure contract cannot start a process"))
        no_child.start()
        self.addCleanup(no_child.stop)

    def prefix(self, *, fixture=None, pre_filter=False, inputs=None):
        inputs = source_records() if inputs is None else inputs
        fixture = FixtureInfer() if fixture is None else fixture
        plan = relation_plan(pre_filter=pre_filter)
        first, suffix = plan.relation_segments()
        columns = tuple(source_records()[0].values)
        accountant = ChainBatchResults(inputs, first, columns=columns)
        envelopes = []
        for offset in range(0, len(inputs), 2):
            batch = inputs[offset:offset + 2]
            result = first.run_batch(batch, fixture)
            envelopes.append(dict(batch_id="prefix-" + str(offset),
                inputs=[_source_binding(row) for row in batch],
                records=[row.describe() for row in result["records"]],
                calls=result["calls"], columns=result["columns"], stage_stats=result["stage_stats"],
                raw_results=[], error_type=None, cleanup_errors=[]))
        # Delivery order is independent of the original label-assignment order.
        for envelope in reversed(envelopes):
            accountant.accept(json.loads(json.dumps(envelope, allow_nan=False)))
        collected = []
        while accountant.pending:
            collected.append(accountant.pop())
        accountant.verify_success(source_exhausted=True)
        accountant.finalize_columns()
        relation = complete_extract_relation(collected, columns=accountant.columns,
            extract_stage=first.stages[-1], query_id=plan.query_id,
            calls=accountant.calls, source_exhausted=True)
        return SimpleNamespace(inputs=inputs, plan=plan, first=first, suffix=suffix,
            accountant=accountant, collected=collected, relation=relation, fixture=fixture)

    def suffix(self, value):
        relation = value.relation
        accountant = ChainBatchResults(relation.records, value.suffix,
            columns=relation.columns, initial_parents=relation.parent_call_ids)
        result = value.suffix.run_batch(list(relation.records), value.fixture,
            columns=relation.columns, initial_parents=relation.parent_call_ids, allow_nulls=True)
        accountant.accept(dict(batch_id="suffix", inputs=[_source_binding(row) for row in relation.records],
            records=[row.describe() for row in result["records"]], calls=result["calls"],
            columns=result["columns"], stage_stats=result["stage_stats"], raw_results=[],
            error_type=None, cleanup_errors=[]))
        accountant.verify_success(source_exhausted=True)
        accountant.finalize_columns()
        return result, accountant

    def original(self, inputs, plan, calls):
        """Feed returned texts into original LOTUS accessors, never a provider."""
        import lotus
        import pandas as pd

        frame = pd.DataFrame([row.values for row in inputs], index=[row.source_index for row in inputs])
        observations = {}
        for stage in plan.stages:
            work = LotusRecordMethod(stage)
            ordered = sorted([call for call in calls if call["algorithm_stage"] == stage.stage_id],
                             key=lambda call: call["source_position"])
            captured = []

            def respond(messages, _options, *_args):
                self.assertEqual(len(messages), len(ordered))
                captured.extend(message for message, _ in messages)
                return [engine_response(call, work.lm) for call in ordered]

            work.lm._process_uncached_messages = respond
            with lotus.settings.context(lm=work.lm):
                if stage.kind == "extract":
                    frame = frame.sem_extract(list(stage.method.input_columns), stage.method.output_columns)
                elif stage.kind == "map":
                    frame = frame.sem_map(stage.method.instruction, suffix=stage.method.output_column)
                else:
                    frame = frame.sem_filter(stage.method.instruction, default=stage.method.default)
            self.assertEqual(captured, [json.loads(call["messages_json"]) for call in ordered])
            self.assertEqual(work.lm.stats.physical_usage.total_tokens, 14 * len(ordered))
            observations[stage.stage_id] = captured
        return frame, observations


class ExtractSuccessorContractTests(ContractCase):
    def test_reverse_delivery_duplicate_text_and_index_match_original_extract(self):
        import pandas as pd

        value = self.prefix()
        expected, _ = self.original(value.inputs, value.first, value.accountant.calls)
        pd.testing.assert_frame_equal(records_to_frame(value.relation.records, columns=value.relation.columns), expected)
        self.assertEqual([row.source_position for row in value.collected], [2, 3, 0, 1])
        self.assertEqual([row.source_position for row in value.relation.records], [0, 1, 2, 3])
        self.assertEqual(value.inputs[0].values["text"], value.inputs[2].values["text"])
        self.assertEqual(value.relation.records[0].values["field"], "keep broadcast")
        self.assertEqual(value.relation.records[2].values["retained"], "first only")
        self.assertEqual(len({call["call_id"] for call in value.accountant.calls}), 4)
        calls = {call["source_position"]: call["call_id"] for call in value.accountant.calls}
        for position in (0, 2):
            self.assertEqual(value.relation.parent_call_ids[("occ-" + str(position), position)],
                             (calls[0], calls[2]))

    def test_nullable_json_rows_preserve_group_parents_and_generated_input(self):
        import pandas as pd

        value = self.prefix()
        cloned = tuple(RecordOccurrence.from_dict(json.loads(record_row(row)["record_json"]))
                       for row in value.relation.records)
        self.assertEqual([row.describe() for row in cloned], [row.describe() for row in value.relation.records])
        for position in (1, 3):
            self.assertIsNone(cloned[position].values["field"])
            self.assertIsNone(cloned[position].values["late"])
        for row, original in zip(cloned, value.inputs, strict=True):
            self.assertEqual(row.values["row_id"], original.values["row_id"])
            self.assertEqual(row.values["call_id"], original.values["call_id"])
            self.assertEqual(row.source_refs, original.source_refs)
        result, accountant = self.suffix(value)
        calls = [*value.accountant.calls, *accountant.calls]
        expected, _ = self.original(value.inputs, value.plan, calls)
        pd.testing.assert_frame_equal(records_to_frame(result["records"], columns=accountant.columns), expected)
        self.assertEqual([row.row_id for row in result["records"]], ["occ-0", "occ-2"])
        self.assertEqual(len(calls), 12)
        for call in accountant.calls:
            if call["algorithm_stage"] == "map":
                key = (call["row_id"], call["source_position"])
                self.assertEqual(call["parent_call_ids"], list(value.relation.parent_call_ids[key]))

    def test_filtered_late_writer_cannot_update_relation_or_enter_parent_group(self):
        import pandas as pd

        value = self.prefix(pre_filter=True)
        self.assertEqual([row.row_id for row in value.relation.records], ["occ-0", "occ-1", "occ-3"])
        self.assertEqual(value.relation.records[0].values["field"], "drop earlier")
        extracts = [call for call in value.accountant.calls if call["algorithm_stage"] == "extract"]
        self.assertEqual({call["source_position"] for call in extracts}, {0, 1, 3})
        self.assertEqual(value.relation.parent_call_ids[("occ-0", 0)],
                         (next(call["call_id"] for call in extracts if call["source_position"] == 0),))
        expected, _ = self.original(value.inputs, value.first, value.accountant.calls)
        pd.testing.assert_frame_equal(records_to_frame(value.relation.records, columns=value.relation.columns), expected)

    def test_original_parser_extra_empty_key_survives_completed_relation(self):
        import pandas as pd

        value = self.prefix(fixture=FixtureInfer(empty_key=True))
        self.assertIn("", value.relation.columns)
        self.assertTrue(all(row.values[""] == "extra" for row in value.relation.records))
        expected, _ = self.original(value.inputs, value.first, value.accountant.calls)
        self.assertIn("", expected.columns)
        pd.testing.assert_frame_equal(records_to_frame(value.relation.records, columns=value.relation.columns), expected)

    def test_absent_extracted_field_is_not_promoted_to_a_nullable_input(self):
        class EmptyExtract(FixtureInfer):
            def response(self, row):
                return "{}" if row["algorithm_stage"] == "extract" else super().response(row)

        value = self.prefix(fixture=EmptyExtract())
        self.assertNotIn("field", value.relation.columns)
        before = deepcopy(value.relation.call_snapshot)
        forbidden = Mock(side_effect=AssertionError("missing input must fail before inference"))
        with self.assertRaisesRegex(ValueError, "present text input"):
            value.suffix.run_batch(list(value.relation.records), forbidden,
                initial_parents=value.relation.parent_call_ids, columns=value.relation.columns, allow_nulls=True)
        forbidden.assert_not_called()
        self.assertEqual(value.relation.call_snapshot, before)
        self.assertEqual(sum(call["total_tokens"] for call in before), 56)

    def test_empty_relation_keeps_explicit_schema_without_synthetic_evaluations(self):
        value = self.prefix(inputs=[])
        self.assertEqual(value.relation.records, ())
        self.assertEqual(value.relation.parent_call_ids, {})
        result, accountant = self.suffix(value)
        self.assertEqual(result["records"], [])
        self.assertEqual(accountant.columns, [*source_records()[0].values, "mapped"])
        self.assertEqual(value.fixture.prepared, [])
        self.assertEqual(accountant.calls, [])

    def test_core_reference_uses_one_model_fixture_after_actual_source_eof(self):
        fixture, plan, inputs = FixtureInfer(), relation_plan(), source_records()
        models, source_eof = [], []

        class Model:
            def __init__(self, engine, _generation, _control, _deadline, _startup):
                self.limit, self.count, self.close_calls = engine.max_model_calls, 0, 0
                self.uid = "in-memory-reference-" + str(len(models))
                models.append(self)

            def run_prepared(self, rows, *, generation):
                if rows[0]["algorithm_stage"] != "extract":
                    if not source_eof:
                        raise AssertionError("successor entered before actual source EOF")
                if self.count + len(rows) > self.limit:
                    raise AssertionError("model fixture exceeded the query evaluation cap")
                self.count += len(rows)
                return [dict(call, engine_uid=self.uid) for call in fixture(rows, generation=generation)]

            def close(self, **_kwargs):
                self.close_calls += 1
                return dict(status="closed", process_exit_confirmed=True, engine_api_shutdown_confirmed=True,
                            gpu_exit_confirmed=None, cleanup_errors=[], fixture=True,
                            process_scope="in-memory fixture; no child process was started")

        class Batch(FusedChainBatch):
            _engine_type = Model

        def source():
            yield from inputs
            source_eof.append(True)

        with tempfile.TemporaryDirectory(prefix="independent-core-extract-") as temporary, \
             patch.object(EngineSpec, "validate_driver_environment", return_value={"CPU fixture": True}):
            engine = EngineSpec(temporary, sys.executable, sys.executable, max_model_calls=12)
            owner = SemLoomRecordChainCore(engine, RecordsResources(cpu_workers=1, max_active_batches=1,
                prepare_batch_rows=1, model_batch_rows=1, query_timeout_s=20), control_dir=temporary)
            owner._batch_type = Batch
            try:
                stream = owner.evaluate(source(), plan, expected=inputs, query_id=plan.query_id)
                output = list(stream)
                self.assertTrue(stream.completed)
                self.assertTrue(stream.production_core_connected)
                self.assertEqual([row.row_id for row in output], ["occ-0", "occ-2"])
                self.assertEqual(len(models), 1)
                self.assertEqual(models[0].count, 12)
                self.assertEqual(len({call["engine_uid"] for call in stream.calls}), 1)
                self.assertEqual(owner.phase_history[0]["received_occurrences"], 4)
                self.assertTrue(owner.phase_history[0]["source_exhausted"])
                self.assertTrue(all(value == 0 for value in asdict(owner.core.capacity.usage()).values()))
                stages = {stage.stage_id: stage for stage in plan.stages}
                for prepared, generation in fixture.batches:
                    self.assertEqual(generation, asdict(stages[prepared[0]["algorithm_stage"]].generation))
            finally:
                owner.close(timeout=3)
            self.assertEqual(models[0].close_calls, 1)


class OriginalParserParityRegressionTests(ContractCase):
    def test_empty_key_schema_must_not_block_a_successor_reading_a_normal_field(self):
        # Known P2 on b447: leave this ordinary regression RED until its owner
        # fixes schema acceptance. Do not turn the observed ValueError into PASS.
        value = self.prefix(fixture=FixtureInfer(empty_key=True))
        result, accountant = self.suffix(value)
        self.assertIn("", accountant.columns)
        self.assertEqual([row.row_id for row in result["records"]], ["occ-0", "occ-2"])
        self.assertTrue(all(row.values[""] == "extra" for row in result["records"]))


@unittest.skip("pending fixed Ray/Daft Extract-successor wiring commit; no native runtime was executed")
class PendingNativeExtractLibraryTests(unittest.TestCase):
    def test_ray_data_extract_successor_original_lotus_parity(self):
        self.fail("implement actual Ray assertions after consuming the fixed wiring commit")

    def test_daft_native_extract_successor_original_lotus_parity(self):
        self.fail("implement actual Daft assertions after consuming the fixed wiring commit")


if __name__ == "__main__":
    unittest.main()
