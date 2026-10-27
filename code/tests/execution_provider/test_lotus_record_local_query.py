"""Whole-query Core/actor/child IPC; only Ray RPC and inference are substitutes."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile
import time
import threading
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore, CoreChainCapacity
from src.execution_provider.adapters.lotus_records_core import CoreRecordsCapacity
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from src.semantic_methods.lotus import record_chain as chains
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_engine import EngineSpec
from src.scheduling.core.session_contract import TaskKey
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.semantic_methods import test_lotus_chain_composition as fixture
from tests.semantic_methods import test_lotus_chain_relation_handoff as relation


class Process(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable, str(Path(__file__).with_name("section_child_fixture.py")), *map(str, descriptors)]


class QueryOwnerTests(unittest.TestCase):
    def setUp(self):
        import lotus
        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__(); self.addCleanup(scope.__exit__, None, None, None)
        temporary = tempfile.TemporaryDirectory(prefix="record-local-query-")
        self.addCleanup(temporary.cleanup)
        self.config = Path(temporary.name) / "responses.json"

    def config_for(self, plan, rows, **options):
        responses = {}
        for stage in plan.stages:
            for row in rows:
                text = (str(row.source_position != 1) if stage.kind == "filter" else
                        "value/" + stage.stage_id + "/" + row.row_id if stage.kind == "map" else
                        json.dumps({next(iter(stage.method.output_columns)): "field-" + str(row.source_position)}))
                responses[stage.stage_id + "|" + row.row_id] = dict(text=text)
        config = dict(responses=responses, **options)
        self.config.write_text(json.dumps(config))
        return config

    def owner(self, plan, *, C=2):
        model = EngineSpec(str(self.config), sys.executable, sys.executable, max_model_len=4096)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=C, query_timeout_s=12, startup_timeout_s=8)
        actors = AsyncRecordsActorOwner.for_prepared(model, plan.stages[0].generation, resources)
        return SemLoomRecordChainCore(model, resources, capacity=CoreRecordsCapacity(
            item_input_bytes=65536, item_result_bytes=65536), execution="record_local_sections", actor_owner=actors)

    def events(self):
        log = Path(str(self.config) + ".events")
        return [] if not log.exists() else [json.loads(line) for line in log.read_text().splitlines()]

    def test_Map_first_mixed_plan_matches_reference_on_one_real_CPU_child(self):
        plan, rows = relation.relation_chain("MFEMF"), fixture.source_rows(size=3)
        config = self.config_for(plan, rows)
        class Replies(fixture.ResponseFixture):
            def one(self, row, generation):
                value = super().one(row, generation)
                value["text"] = config["responses"][row["algorithm_stage"] + "|" + row["row_id"]]["text"]
                return value
        expected = relation.reference(plan, rows, Replies(plan))
        with ThreadedRuntime([]) as runtime, patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                result = list(stream)
            finally:
                report = owner.close(timeout=3)
            actual = dict(records=result, calls=owner.calls, columns=stream.columns)
            self.assertEqual(fixture.normalized(actual), fixture.normalized(expected))
            by_id = {call["call_id"]: call for call in owner.calls}
            replay = Replies(plan)
            relation.reference(plan, rows, replay)
            for call_id, (messages, generation) in replay.requests.items():
                self.assertEqual(by_id[call_id]["fixture_messages"], messages)
                self.assertEqual(by_id[call_id]["fixture_generation"]["max_tokens"], generation["max_tokens"])
            self.assertEqual(len({call["engine_uid"] for call in owner.calls}), 1)
            self.assertEqual(len(runtime.async_actors), 1)
            self.assertEqual(owner._actors.prepare_actors, [])
            self.assertEqual(report["status"], "closed")
            self.assertTrue(report["query_completed"])
            self.assertEqual(report["core_usage"]["held_tasks"], 0)
            self.assertEqual(stream.pool.used, (0, 0))
            self.assertEqual([v["transition"] for v in report["phase_history"]],
                             ["record_local_adoption", "complete_extract_relation", "record_local_adoption"])
        self.assertEqual(sum(e["event"] == "construct" for e in self.events()), 1)
        self.assertEqual(sum(e["event"] == "close" for e in self.events()), 1)

    def test_empty_query_does_not_allocate_model_but_still_completes_Extract(self):
        plan = relation.relation_chain("FMEM")
        self.config_for(plan, [])
        with ThreadedRuntime([]) as runtime:
            owner = self.owner(plan)
            stream = owner.evaluate(iter([]), plan, expected=[], query_id=plan.query_id)
            self.assertEqual(list(stream), [])
            self.assertEqual(runtime.async_actors, [])
            self.assertEqual(owner.calls, [])
            self.assertTrue(owner.last_close_report["query_completed"])
            self.assertIn("derived_3", stream.columns)

    def test_failed_source_coverage_cannot_enter_later_Extract(self):
        plan, rows = relation.relation_chain("FEM"), fixture.source_rows(size=2)
        self.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan)
            stream = owner.evaluate(iter(rows[:1]), plan, expected=rows, query_id=plan.query_id)
            try:
                with self.assertRaisesRegex(ValueError, "omitted expected"):
                    list(stream)
                self.assertFalse(any(c["algorithm_stage"] != "step_0" for c in owner.calls))
                self.assertFalse(owner.last_close_report["query_completed"])
            finally:
                owner.close(timeout=3)

    def test_E_first_does_not_submit_unseen_or_missing_source_rows(self):
        plan, rows = relation.relation_chain("EM"), fixture.source_rows(size=2)
        self.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan)
            stream = owner.evaluate(iter(rows[:1]), plan, expected=rows, query_id=plan.query_id)
            try:
                with self.assertRaisesRegex(ValueError, "omitted expected"):
                    list(stream)
                self.assertEqual(owner.calls, [])
                self.assertEqual(sum(e["event"] == "start" for e in self.events()), 0)
                self.assertEqual(owner.last_close_report["core_usage"]["held_tasks"], 0)
            finally:
                owner.close(timeout=3)

    def test_pure_FM_plan_uses_same_plan_without_changing_default_fused(self):
        plan, rows = fixture.chain_for("MMF"), fixture.source_rows(size=2)
        self.config_for(plan, rows)
        self.assertFalse(plan.relation_barriers)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan)
            result = list(owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id))
            self.assertEqual(len(result), 1)
            self.assertFalse(plan.relation_barriers)
        fused = SemLoomRecordChainCore(EngineSpec(str(self.config), sys.executable, sys.executable),
            RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=1))
        self.assertEqual(fused.execution, "fused")
        self.assertIs(type(fused.capacity), CoreChainCapacity)
        self.assertIsNone(fused._actors)

    def test_legal_Map_submits_before_unrelated_Filter_ends(self):
        plan, rows = fixture.chain_for("FM"), fixture.source_rows(size=2)
        self.config_for(plan, rows, hold=["step_0|occ-1"], release_on=["step_1|occ-0"])
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan)
            result = list(owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id))
            self.assertEqual([r.row_id for r in result], ["occ-0"])
            self.assertEqual(len(owner.calls), 3)
        events = self.events()
        started = next(e["at"] for e in events if e["event"] == "start" and e["token"] == "step_1|occ-0")
        upstream_end = next(e["at"] for e in events if e["event"] == "end" and e["token"] == "step_0|occ-1")
        self.assertLess(started, upstream_end)

    def test_repeated_stages_duplicate_user_index_and_declared_field_overwrite(self):
        for pattern in ("MFMF", "MEEFM", "EMEM", "FE"):
            with self.subTest(pattern=pattern):
                plan, rows = relation.relation_chain(pattern), fixture.source_rows(size=3)
                config = self.config_for(plan, rows)
                class Replies(fixture.ResponseFixture):
                    def one(self, row, generation):
                        value = super().one(row, generation)
                        value["text"] = config["responses"][row["algorithm_stage"] + "|" + row["row_id"]]["text"]
                        return value
                expected = relation.reference(plan, rows, Replies(plan))
                with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
                    owner = self.owner(plan)
                    stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                    actual = dict(records=list(stream), calls=owner.calls, columns=stream.columns)
                    self.assertEqual(fixture.normalized(actual), fixture.normalized(expected))
                    self.assertEqual(owner.last_close_report["status"], "closed")
                    self.assertTrue(all(not r.index_updates for r in actual["records"]))

    def test_all_Filter_false_has_no_downstream_calls_and_preserves_empty_columns(self):
        plan, rows = relation.relation_chain("FMEM"), fixture.source_rows(size=3)
        config = self.config_for(plan, rows)
        for row in rows:
            config["responses"]["step_0|" + row.row_id]["text"] = "False"
        self.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            self.assertEqual(list(stream), [])
            self.assertEqual(len(owner.calls), len(rows))
            self.assertEqual(sum(c["total_tokens"] for c in owner.calls), 42)
            self.assertIn("derived_3", stream.columns)
            self.assertEqual(owner.last_close_report["status"], "closed")

    def test_later_Extract_unknown_preserves_prefix_but_not_unsubmitted_rows(self):
        plan, rows = relation.relation_chain("MEM"), fixture.source_rows(size=2)
        config = self.config_for(plan, rows)
        config["responses"]["step_1|occ-0"].update(status="unknown")
        self.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan, C=1)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                with self.assertRaises(RuntimeError) as caught:
                    list(stream)
                report = owner.close(timeout=3)
                self.assertEqual(len(owner.calls), 3)
                self.assertEqual(sum(c["status"] == "unknown" for c in owner.calls), 1)
                self.assertEqual(sum(c["total_tokens"] for c in owner.calls), 42)
                self.assertFalse(any(c["algorithm_stage"] == "step_2" for c in owner.calls))
                self.assertFalse(any(c["algorithm_stage"] == "step_1" and c["row_id"] == "occ-1" for c in owner.calls))
                self.assertEqual(caught.exception.chain_calls, owner.calls)
                self.assertEqual(report["status"], "closed")
                self.assertFalse(report["query_completed"])
                self.assertEqual(report["core_usage"]["held_tasks"], 0)
                self.assertTrue(report["producer_retirement"]["confirmed"])
                self.assertEqual(stream.pool.used, (0, 0))
            finally:
                owner.close(timeout=3)

    def test_cancel_stops_progress_and_retires_only_after_same_producer_closes(self):
        plan, rows = relation.relation_chain("FEM"), fixture.source_rows(size=2)
        config = self.config_for(plan, rows, hold=["step_0|occ-0"])
        config["responses"]["step_0|occ-0"].update(status="unknown")
        self.config.write_text(json.dumps(config))
        cancelled = [False]
        with ThreadedRuntime([]) as runtime, patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan, C=1)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id,
                                    cancelled=lambda: cancelled[0])
            deadline = time.monotonic() + 5
            while not any(e["event"] == "start" for e in self.events()):
                stream._tick()
                if time.monotonic() > deadline:
                    self.fail("CPU child never entered the declared held call")
            cancelled[0] = True
            with self.assertRaises(InterruptedError):
                next(stream)
            report = owner.close(timeout=3)
            self.assertEqual(len(owner.calls), 1)
            self.assertEqual(owner.calls[0]["status"], "unknown")
            self.assertEqual(owner.calls[0]["total_tokens"], 14)
            self.assertEqual(report["core_usage"]["held_tasks"], 0)
            self.assertEqual(report["status"], "closed")
            self.assertFalse(report["query_completed"])
            self.assertEqual(len(runtime.async_actors), 1)
            self.assertEqual(stream.pool.used, (0, 0))

    def test_close_collects_late_known_receipt_without_parser_or_successor_replay(self):
        plan, rows = fixture.chain_for("MF"), fixture.source_rows(size=1)
        self.config_for(plan, rows)
        ready, release = threading.Event(), threading.Event()
        original = AsyncCoreModelActor._run_core
        def hold_delivery(actor, *args):
            value = original(actor, *args)
            ready.set()
            if not release.wait(5):
                raise TimeoutError("CPU test did not release the known result")
            return value
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process), \
                patch.object(AsyncCoreModelActor, "_run_core", hold_delivery):
            owner = self.owner(plan, C=1)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                deadline = time.monotonic() + 4
                while not ready.is_set():
                    stream._tick()
                    if time.monotonic() >= deadline:
                        self.fail("known CPU receipt was not ready")
                first = deepcopy(owner.close(timeout=0.02))
                self.assertEqual(first["status"], "unknown")
                self.assertEqual(first["core_usage"]["held_tasks"], 1)
                release.set()
                report = owner.close(timeout=3)
                self.assertEqual(report["status"], "closed")
                self.assertEqual(len(owner.calls), 1)
                self.assertEqual(owner.calls[0]["status"], "succeeded")
                self.assertEqual(owner.calls[0]["total_tokens"], 14)
                self.assertFalse(report["query_completed"])
                self.assertEqual(stream.run.position, 0)
                self.assertEqual(stream.run.stats, {})
                self.assertEqual(report["core_usage"]["held_tasks"], 0)
                self.assertEqual(first["core_usage"]["held_tasks"], 1)
            finally:
                release.set()
                owner.close(timeout=3)

    def test_foreign_close_receipt_cannot_be_assigned_by_reused_call_id(self):
        plan, rows = fixture.chain_for("M"), fixture.source_rows(size=1)
        self.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan)
            list(owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id))
            key = next(iter(owner._contexts))
            before = owner.calls
            with self.assertRaises(KeyError):
                owner._record_local_receipt(TaskKey(key.session_id + 1, key.sequence), owner.calls[0])
            self.assertEqual(owner.calls, before)

    def test_Map_and_Extract_can_overwrite_the_same_column_with_grouped_parents(self):
        from src.serving.backends.vllm_engine import GenerationSpec
        stages = (chains.StageSpec("rewrite-a", chains.MapMethod("openai/cpu-fixture", "Rewrite {payload}", "payload"),
                                  GenerationSpec(max_tokens=32, stop=())),
            chains.StageSpec("fields-b", chains.ExtractMethod("openai/cpu-fixture", ("payload",), {"payload": "Text"}),
                             GenerationSpec(max_tokens=48, stop=(), response_format={"type": "json_object"})),
            chains.StageSpec("rewrite-c", chains.MapMethod("openai/cpu-fixture", "Rewrite {payload}", "payload"),
                             GenerationSpec(max_tokens=17, stop=("END",), seed=7)))
        plan = chains.LotusRecordChain(stages, query_id="another-plan-name", relation_barriers=True)
        rows = fixture.source_rows(column="payload", size=3)
        config = self.config_for(plan, rows)
        class Replies:
            def __call__(_self, prepared, *, generation):
                from tests.semantic_methods.test_lotus_record_chain import engine_result
                return [engine_result(row, config["responses"][row["algorithm_stage"] + "|" + row["row_id"]]["text"])
                        for row in prepared]
        expected = relation.reference(plan, rows, Replies())
        parsed = []
        original = chains.LotusRecordMethod.interpret_results
        def capture(method, results):
            parsed.append((method.stage.stage_id, len(results)))
            return original(method, results)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process), \
                patch.object(chains.LotusRecordMethod, "interpret_results", capture):
            owner = self.owner(plan)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            actual = dict(records=list(stream), calls=owner.calls, columns=stream.columns)
            self.assertEqual(fixture.normalized(actual), fixture.normalized(expected))
            self.assertEqual(sum(count for _, count in parsed), len(owner.calls))
            self.assertEqual(sum(stage == "fields-b" for stage, _ in parsed), 1)
            self.assertEqual(len(next(c for c in owner.calls if c["algorithm_stage"] == "rewrite-c"
                                      and c["row_id"] == "occ-0")["parent_call_ids"]), 2)
            bound = {stage.stage_id: stage for stage in plan.stages}
            for call in owner.calls:
                self.assertEqual(call["fixture_generation"], json.loads(json.dumps(bound[call["algorithm_stage"]].generation.kwargs())))

    def test_identical_payloads_and_duplicate_business_index_remain_distinct(self):
        plan = fixture.chain_for("MM")
        rows = fixture.source_rows(size=3)
        rows = [replace(row, values=deepcopy(rows[0].values), source_index=7) for row in rows]
        self.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan)
            result = list(owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id))
            self.assertEqual([r.row_id for r in result], [r.row_id for r in rows])
            self.assertEqual([r.source_index for r in result], [7, 7, 7])
            self.assertEqual(len(owner.calls), 6)
            self.assertTrue(all(r.values["row_id"] == "business-value" for r in result))

    def test_invalid_actual_occurrences_stop_without_entering_Extract(self):
        for defect in ("duplicate", "changed", "out-of-order"):
            with self.subTest(defect=defect):
                plan, rows = relation.relation_chain("FEM"), fixture.source_rows(size=2)
                self.config_for(plan, rows)
                actual = ([rows[0], rows[0]] if defect == "duplicate" else list(reversed(rows))
                          if defect == "out-of-order" else [replace(rows[0], values={"passage": "wrong"}), rows[1]])
                with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
                    owner = self.owner(plan)
                    stream = owner.evaluate(iter(actual), plan, expected=rows, query_id=plan.query_id)
                    try:
                        with self.assertRaises(ValueError):
                            list(stream)
                        self.assertTrue(all(c["algorithm_stage"] == "step_0" for c in owner.calls))
                        self.assertFalse(owner.last_close_report["query_completed"])
                    finally:
                        owner.close(timeout=3)

    def test_source_close_failure_never_publishes_Extract_or_hides_cleanup_error(self):
        plan, rows = relation.relation_chain("EM"), fixture.source_rows(size=1)
        self.config_for(plan, rows)
        class Source:
            def __init__(self):
                self.rows = iter(rows)
            def __iter__(self):
                return self
            def __next__(self):
                return next(self.rows)
            def close(self):
                raise OSError("CPU fixture source close failed")
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan)
            stream = owner.evaluate(Source(), plan, expected=rows, query_id=plan.query_id)
            try:
                with self.assertRaisesRegex(OSError, "source close failed"):
                    list(stream)
                self.assertEqual(owner.calls, [])
                self.assertEqual(owner.last_close_report["status"], "unknown")
                self.assertIn("source_iterator_close: OSError", owner.last_close_report["cleanup_errors"])
                self.assertEqual(owner.last_close_report["core_usage"]["held_tasks"], 0)
            finally:
                owner.close(timeout=3)

    def test_roles_split_local_sections_but_do_not_switch_the_model(self):
        base = fixture.chain_for("MM")
        plan = chains.LotusRecordChain((base.stages[0], replace(base.stages[1], model_role="helper")),
                                      query_id=base.query_id)
        rows = fixture.source_rows(size=1)
        self.config_for(plan, rows)
        with ThreadedRuntime([]) as runtime, patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan)
            list(owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id))
            self.assertEqual(len(owner.last_close_report["phase_history"]), 2)
            self.assertEqual({c["model_role"] for c in owner.calls}, {"main", "helper"})
            self.assertEqual(len(runtime.async_actors), 1)
            self.assertEqual(len({c["engine_uid"] for c in owner.calls}), 1)

    def test_incompatible_plan_and_capacity_fail_before_model_start(self):
        base, rows = fixture.chain_for("MM"), fixture.source_rows(size=1)
        other = chains.LotusRecordChain((base.stages[0], replace(base.stages[1],
            method=replace(base.stages[1].method, model_name="openai/another"))), query_id=base.query_id)
        self.config_for(base, rows)
        with ThreadedRuntime([]) as runtime:
            owner = self.owner(base)
            with self.assertRaisesRegex(ValueError, "one model"):
                owner.evaluate(iter(rows), other, expected=rows, query_id=other.query_id)
            self.assertEqual(runtime.async_actors, [])
            with self.assertRaisesRegex(ValueError, "in-flight"):
                self.owner(base, C=9)
            owner.close()

    def test_explicit_failure_scope_does_not_invent_unsubmitted_Extract_calls(self):
        plan, rows = relation.relation_chain("EM"), fixture.source_rows(size=2)
        run = plan.start_relation(rows)
        prepared = run.prepare()
        before = deepcopy(run.snapshot())
        with self.assertRaises(ValueError):
            run.failure(InterruptedError(), submitted_call_ids={"not-declared"})
        self.assertEqual(run.snapshot(), before)
        run.failure(InterruptedError(), submitted_call_ids={prepared[0]["call_id"]})
        self.assertEqual(len(run.calls), 1)
        self.assertEqual(run.calls[0]["error_type"], "NativeTaskUnconfirmed")
        default = plan.start_relation(rows)
        default.prepare()
        default.failure(InterruptedError())
        self.assertEqual(len(default.calls), 2)

    def fused_result(self, cls, plan, rows, config):
        from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch
        from tests.semantic_methods.test_lotus_record_chain import engine_result
        class Engine:
            def __init__(self, *_args):
                pass
            def run_prepared(self, prepared, *, generation):
                return [engine_result(row, config["responses"][row["algorithm_stage"] + "|" + row["row_id"]]["text"])
                        for row in reversed(prepared)]
            def close(self, timeout=None):
                # This is an explicit software engine, not an OS/GPU assertion.
                return dict(status="closed", process_exit_confirmed=True,
                            engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None, cleanup_errors=[])
        class Batch(FusedChainBatch):
            _engine_type = Engine
        Owner = type("FusedTestOwner", (cls,), {"_batch_type": Batch})
        owner = Owner(EngineSpec(str(self.config), sys.executable, sys.executable),
            RecordsResources(prepare_batch_rows=1, model_batch_rows=2, max_active_batches=1),
            control_dir=str(self.config.parent))
        with patch.object(EngineSpec, "validate_driver_environment", return_value={}):
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                result = list(stream)
            finally:
                report = owner.close(timeout=3)
        self.assertEqual(report["status"], "closed")
        return fixture.normalized(dict(records=result, calls=owner.calls, columns=stream.columns))

    def test_default_fused_owner_matches_shared_kernel_reference(self):
        from tests.semantic_methods.test_lotus_record_chain import engine_result
        plan, rows = relation.relation_chain("MFEFM"), fixture.source_rows(size=3)
        config = self.config_for(plan, rows)
        class Replies:
            def __call__(_self, prepared, *, generation):
                return [engine_result(row, config["responses"][row["algorithm_stage"] + "|" + row["row_id"]]["text"])
                        for row in prepared]
        expected = fixture.normalized(relation.reference(plan, rows, Replies()))
        self.assertEqual(self.fused_result(SemLoomRecordChainCore, plan, rows, config), expected)

    def test_source_row_held_by_backpressure_keeps_its_read_snapshot(self):
        plan, rows = fixture.chain_for("MM"), fixture.source_rows(size=2)
        config = self.config_for(plan, rows)
        config["responses"]["step_0|occ-0"]["delay_s"] = 0.08
        self.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan, C=1)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                deadline = time.monotonic() + 4
                while stream.pending is None or stream.pending.row_id != "occ-1":
                    stream._tick()
                    if time.monotonic() >= deadline:
                        self.fail("second source row was not held behind the active row")
                rows[1].values["passage"] = "CHANGED_AFTER_PULL"
                result = list(stream)
                self.assertEqual([r.row_id for r in result], ["occ-0", "occ-1"])
                self.assertEqual(result[1].values["passage"], "text text ")
                call = next(c for c in owner.calls if c["row_id"] == "occ-1" and c["algorithm_stage"] == "step_0")
                self.assertNotIn("CHANGED_AFTER_PULL", call["fixture_messages"])
            finally:
                owner.close(timeout=3)


if __name__ == "__main__":
    unittest.main()
