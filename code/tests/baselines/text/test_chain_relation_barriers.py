"""Relation-phase contracts with original LOTUS and a CPU-only model substitute."""

from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import socket
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch
from src.baselines.text.frameworks.lotus_chain_results import ChainBatchResults, _source_binding, record_row
from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusRecordChain, LotusRecordMethod, MapMethod, RecordOccurrence, StageSpec,
    records_to_frame,
)
from src.semantic_methods.lotus.record_relation import complete_extract_relation
from src.semantic_methods.lotus.records import FilterMethod
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.semantic_methods.test_lotus_record_chain import engine_result


def stages():
    return (
        StageSpec("extract", ExtractMethod("fixture", ("text",), {"field": None}),
                  GenerationSpec(max_tokens=64, stop=(), response_format={"type": "json_object"})),
        StageSpec("map", MapMethod("fixture", "Summarize {field}", "summary"), GenerationSpec(max_tokens=32)),
        StageSpec("filter", FilterMethod("fixture", "Accept {summary}"), GenerationSpec()),
    )


def records():
    return [RecordOccurrence("r" + str(i), i, 5 if i < 2 else 6,
            {"text": "original " + str(i), "row_id": "business-row", "call_id": "business-call"})
            for i in range(3)]


class CpuEngine:
    instances = []

    def __init__(self, *_):
        self.calls, self.close_calls = [], 0
        type(self).instances.append(self)

    def run_prepared(self, rows, *, generation):
        self.calls.append((deepcopy(rows), asdict(generation)))
        results = []
        for row in rows:
            stage, position = row["algorithm_stage"], row["source_position"]
            text = ({"extract": ('{"field":"first"}' if position == 0 else
                                 '{"field":"last"}' if position == 1 else '{}'),
                     "map": "mapped " + str(position),
                     "filter": "False" if position == 1 else "True"})[stage]
            results.append(dict(engine_result(row, text), engine_uid="one-cpu-engine"))
        return results

    def close(self, **_):
        self.close_calls += 1
        return dict(status="closed", engine_api_shutdown_confirmed=True,
                    process_exit_confirmed=None, gpu_exit_confirmed=None)


class RelationBarrierTests(unittest.TestCase):
    def setUp(self):
        import lotus
        import pandas as pd

        self.pd = pd
        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        self.no_process = patch("subprocess.Popen", side_effect=AssertionError("Popen forbidden in CPU fixture"))
        self.no_process.start()
        self.addCleanup(self.no_process.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="relation-phase-cpu-")
        self.addCleanup(self.temp.cleanup)
        self.control = Path(self.temp.name)
        (self.control / "owner").write_text("cpu-owner")
        self.inputs = records()
        self.plan = LotusRecordChain(stages(), query_id="q", relation_barriers=True)
        self.first, self.second = self.plan.relation_segments()
        CpuEngine.instances = []

    def worker(self, chain=None, inputs=None, **options):
        class Worker(FusedChainBatch):
            _engine_type = CpuEngine
        return Worker((self.first if chain is None else chain).describe(),
            asdict(EngineSpec("/tmp/cpu-model", "/tmp/model-python", "/tmp/driver-python")),
            [_source_binding(row) for row in (self.inputs if inputs is None else inputs)],
            self.control, time.monotonic() + 30, 5, socket.gethostname(), "cpu-owner", **options)

    def infer(self, prepared, *, generation):
        if not CpuEngine.instances:
            CpuEngine()
        return CpuEngine.instances[0].run_prepared(prepared, generation=generation)

    def test_default_guard_wire_and_segments_preserve_global_stage_ids(self):
        with self.assertRaises(ValueError):
            LotusRecordChain(stages(), query_id="q")
        for value in (None, 1, "true"):
            with self.subTest(value=value), self.assertRaises(TypeError):
                LotusRecordChain(stages(), query_id="q", relation_barriers=value)
        self.assertTrue(self.plan.describe()["relation_barriers"])
        self.assertEqual(LotusRecordChain.from_dict(self.plan.describe()).describe(), self.plan.describe())
        self.assertEqual([[s.stage_id for s in chain.stages] for chain in self.plan.relation_segments()],
                         [["extract"], ["map", "filter"]])
        self.assertTrue(all("relation_barriers" not in segment.describe() for segment in (self.first, self.second)))
        self.assertEqual(self.first.relation_segments(), (self.first,))
        with self.assertRaises(ValueError):
            self.plan.run_batch(self.inputs, Mock(side_effect=AssertionError("no inference")))
        with self.assertRaises(ValueError):
            self.worker(self.plan)

    def test_seed_contract_is_exact_finite_and_copied_before_inference(self):
        seeds = {(r.row_id, r.source_position): ("prior-" + r.row_id,) for r in self.inputs}
        invalid = [[], {}, {**seeds, ("extra", 8): ()},
                   {**seeds, ("r0", 0): ["prior"]}, {**seeds, ("r0", 0): ("",)},
                   {**seeds, ("r0", 0): ("prior", "prior")}, {**seeds, ("r0", 0): ({},)}]
        for value in invalid:
            infer = Mock(side_effect=AssertionError("invalid seed reached inference"))
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                self.first.run_batch(self.inputs, infer, initial_parents=value)
            infer.assert_not_called()
        def fail(prepared, *, generation):
            seeds[("r0", 0)] = ("mutated-after-start",)
            raise LookupError("self-written failed transport")
        with self.assertRaises(LookupError) as caught:
            self.first.run_batch(self.inputs, fail, initial_parents=seeds)
        self.assertEqual([c["parent_call_ids"] for c in caught.exception.chain_calls],
                         [["prior-r0"], ["prior-r1"], ["prior-r2"]])
        self.assertTrue(all(c["status"] == "unknown" for c in caught.exception.chain_calls))
        raw = [engine_result(row, "{}") for row in self.first.methods[0].prepare_rows(self.inputs, query_id="q")]
        legacy = ChainBatchResults(self.inputs, self.first)
        legacy._record_calls(raw)
        self.assertEqual(len(legacy.calls), 3)  # unannotated audit prefix is not a complete relation
        with self.assertRaises(ValueError): legacy.verify_success(source_exhausted=True)
        seeded = ChainBatchResults(self.inputs, self.first, initial_parents=seeds)
        with self.assertRaises(ValueError): seeded.recover(dict(calls=raw))

    def test_nullable_preparation_matches_original_formatter_without_cell_conversion(self):
        for stage in self.second.stages + (self.first.stages[0],):
            method = LotusRecordMethod(stage)
            column = method.columns[0]
            rows = [RecordOccurrence("r", 0, 5, {column: None})]
            with self.assertRaises(ValueError):
                method.prepare_rows(rows, query_id="q")
            captured = []
            original = method.instructions.df2multimodal_info
            def observe(frame, columns):
                captured.append(frame.copy(deep=True))
                return original(frame, columns)
            with patch.object(method.instructions, "df2multimodal_info", side_effect=observe):
                actual = method.prepare_rows(rows, query_id="q", allow_nulls=True)
            self.assertIs(captured[0].iloc[0][column], None)
            docs = original(self.pd.DataFrame({column: [None]}, index=[5]), [column])
            if stage.kind == "filter":
                expected = method.instructions.filter_formatter(method.lm, docs[0], method.instruction,
                    system_prompt=method.method.system_prompt, output_tokens=method.method.output_tokens)
                with self.assertRaises(TypeError):
                    method._filter_formatter.messages([{column: None}])
            elif stage.kind == "map":
                expected = method.instructions.map_formatter(method.lm, docs[0], method.instruction,
                                                               system_prompt=method.method.system_prompt)
            else:
                expected = method.instructions.extract_formatter(method.lm, docs[0], method.method.output_columns,
                                                                   method.method.extract_quotes)
            self.assertEqual(json.loads(actual[0]["messages_json"]), expected)
            for values in ({}, {column: 1}, {column: False}, {column: []}):
                with self.subTest(kind=stage.kind, values=values), self.assertRaises(ValueError):
                    method.prepare_rows([RecordOccurrence("r", 0, 5, values)], query_id="q", allow_nulls=True)
            with self.assertRaises(TypeError):
                method.prepare_rows([], query_id="q", allow_nulls=1)

    def test_two_phases_use_one_engine_and_group_parents_after_original_extract_assembly(self):
        worker = self.worker(close_on_source_complete=False)
        ledger = ChainBatchResults(self.inputs, self.first)
        for chunk in (self.inputs[:2], self.inputs[2:]):
            ledger.accept(worker.run_rows([record_row(r) for r in chunk]))
        ledger.verify_success(source_exhausted=True)
        ledger.finalize_columns()
        prefix = list(ledger.pending)
        relation = complete_extract_relation(prefix, columns=ledger.columns, extract_stage=self.first.stages[-1],
            query_id="q", calls=ledger.calls, source_exhausted=True)
        self.assertEqual([r.values["field"] for r in relation.records], ["last", "last", None])
        self.assertEqual([len(v) for v in relation.parent_call_ids.values()], [2, 2, 1])
        self.assertEqual(CpuEngine.instances[0].close_calls, 0)
        worker.begin_relation_phase(self.second.describe(), [_source_binding(r) for r in relation.records],
            initial_parents=relation.parent_call_ids, columns=relation.columns, allow_nulls=True, source_exhausted=True)
        final = ChainBatchResults(relation.records, self.second,
                                  initial_parents=relation.parent_call_ids, columns=relation.columns)
        for chunk in (relation.records[:2], relation.records[2:]):
            envelope = worker.run_rows([record_row(r) for r in chunk])
            self.assertEqual(envelope["phase_stage_ids"], ["map", "filter"])
            self.assertNotIn("engine_close", envelope)
            final.accept(envelope)
        final.verify_success(source_exhausted=True)
        final.finalize_columns()
        self.assertEqual([r.row_id for r in final.pending], ["r0", "r2"])
        self.assertEqual(len(final.calls), 6)
        self.assertEqual(sum(c["total_tokens"] for c in final.calls), 84)
        self.assertTrue(any(c.get("decision") is False for c in final.calls))
        maps = [c for c in final.calls if c["algorithm_stage"] == "map"]
        self.assertEqual([c["parent_call_ids"] for c in maps], list(map(list, relation.parent_call_ids.values())))
        self.assertTrue(all(r.values["row_id"] == "business-row" and r.values["call_id"] == "business-call"
                            for r in final.pending))
        self.assertEqual(len(CpuEngine.instances), 1)
        self.assertEqual({c["engine_uid"] for c in ledger.calls + final.calls}, {"one-cpu-engine"})
        self.assertEqual(worker.close()["status"], "closed")
        self.assertEqual(CpuEngine.instances[0].close_calls, 1)

    def test_empty_following_phase_preserves_ordered_schema_without_model_allocation(self):
        worker = self.worker(inputs=[], close_on_source_complete=False, columns=["text", "field"])
        empty = worker.run_rows([])
        self.assertEqual(empty["columns"], ["text", "field"])
        worker.begin_relation_phase(self.second.describe(), [], initial_parents={}, columns=["text", "field"],
                                    allow_nulls=True, source_exhausted=True)
        envelope = worker.run_rows([])
        ledger = ChainBatchResults([], self.second, initial_parents={}, columns=["text", "field"])
        ledger.accept(envelope)
        ledger.verify_success(source_exhausted=True)
        ledger.finalize_columns()
        self.assertEqual(ledger.columns, ["text", "field", "summary"])
        self.assertEqual(list(records_to_frame([], columns=ledger.columns).columns), ledger.columns)
        self.assertEqual(CpuEngine.instances, [])

    def test_unknown_audit_and_exception_fallback_use_seed_before_model_start(self):
        seeds = {(r.row_id, r.source_position): ("external-" + r.row_id,) for r in self.inputs}
        worker = self.worker(initial_parents=seeds, close_on_source_complete=False)
        def fail(*_): raise RuntimeError("CPU constructor fixture")
        worker._engine_type = fail
        envelope = worker.run_rows([record_row(r) for r in self.inputs])
        self.assertEqual(envelope["error_type"], "RuntimeError")
        self.assertEqual([c["parent_call_ids"] for c in envelope["calls"]], list(map(list, seeds.values())))
        audit = json.loads(next(self.control.glob("chain-audit-*.json")).read_text())
        self.assertEqual(audit["phase_stage_ids"], ["extract"])
        self.assertEqual([c["parent_call_ids"] for c in audit["calls"]], list(map(list, seeds.values())))
        with self.assertRaises(ValueError):
            worker.begin_relation_phase(self.second.describe(), [], initial_parents={}, columns=[],
                                        allow_nulls=True, source_exhausted=True)

    def test_phase_transition_requires_successful_complete_open_source(self):
        for state in ("incomplete", "eof_false", "eof_int", "cleanup", "closed", "default_close"):
            with self.subTest(state=state):
                worker = self.worker(inputs=[] if state != "incomplete" else self.inputs,
                                     close_on_source_complete=state == "default_close")
                if state == "cleanup": worker._phase_cleanup_errors.append("audit-write-failed")
                if state == "closed": worker.close()
                source = False if state == "eof_false" else 1 if state == "eof_int" else True
                with self.assertRaises(ValueError):
                    worker.begin_relation_phase(self.second.describe(), [], initial_parents={}, columns=[],
                                                allow_nulls=True, source_exhausted=source)
        for state in ("stage_id", "query", "deployment", "parents", "columns"):
            with self.subTest(state=state):
                worker = self.worker(inputs=[], close_on_source_complete=False)
                wire = deepcopy(self.second.describe())
                if state == "stage_id": wire = self.first.describe()
                if state == "query": wire["query_id"] = "another-query"
                if state == "deployment":
                    for stage in wire["stages"]: stage["method"]["model_name"] = "another-deployment"
                with self.assertRaises(ValueError):
                    worker.begin_relation_phase(wire, [], initial_parents=None if state == "parents" else {},
                        columns=None if state == "columns" else [], allow_nulls=True, source_exhausted=True)

    def test_success_gate_has_sticky_accept_recover_cleanup_protocol_and_unknown_failures(self):
        worker = self.worker()
        good = worker.run_rows([record_row(r) for r in self.inputs])
        for fault in ("duplicate_accept", "recover_error", "recover_unknown", "raw", "cleanup", "marker", "unknown", "bad_seed"):
            with self.subTest(fault=fault):
                ledger = ChainBatchResults(self.inputs, self.first)
                envelope = deepcopy(good)
                if fault == "cleanup": envelope["cleanup_errors"] = ["audit failure"]
                if fault == "marker": envelope["calls"][0]["engine_protocol_error"] = None
                if fault == "unknown": envelope["calls"][0]["status"] = "unknown"
                if fault == "bad_seed": envelope["calls"][0]["parent_call_ids"] = ["foreign"]
                if fault in ("unknown", "bad_seed"):
                    with self.assertRaises(ValueError): ledger._accept(envelope)
                else:
                    ledger._accept(envelope)
                if fault == "duplicate_accept":
                    with self.assertRaises(ValueError): ledger._accept(deepcopy(good))
                if fault == "recover_error":
                    with self.assertRaises(ValueError):
                        ledger.recover(dict(calls=[dict(good["calls"][0], row_id="foreign")]))
                if fault == "recover_unknown":
                    ledger.recover(dict(calls=[dict(good["calls"][0], status="unknown",
                                                   error_type="NativeTaskUnconfirmed")]))
                    self.assertEqual(ledger.calls[0]["status"], "succeeded")
                if fault == "raw":
                    ledger.recover(dict(calls=[], batch_id=good["batch_id"], raw_stage_id="extract",
                                        raw_results=[dict(good["calls"][0], call_id="foreign")]))
                with self.assertRaises(ValueError): ledger.verify_success(source_exhausted=True)
        ledger = ChainBatchResults(self.inputs, self.first)
        ledger.accept(good)
        for value in (False, 1, None):
            with self.subTest(eof=value), self.assertRaises(ValueError):
                ledger.verify_success(source_exhausted=value)
        ledger.verify_success(source_exhausted=True)
        ledger.recover(dict(calls=[], error_type="OriginalSemanticFailure"))
        with self.assertRaises(ValueError): ledger.verify_success(source_exhausted=True)
        ledger = ChainBatchResults(self.inputs, self.first)
        with self.assertRaises(RuntimeError): ledger._accept(dict(good, error_type="OriginalSemanticFailure"))
        self.assertEqual(len(ledger.calls), 3)
        with self.assertRaises(ValueError): ledger.verify_success(source_exhausted=True)


if __name__ == "__main__":
    unittest.main()
