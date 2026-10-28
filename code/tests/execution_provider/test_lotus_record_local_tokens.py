"""Whole-query token wiring with real LOTUS/Core/tokenizer and CPU child IPC."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_chain_token_budget import (
    ChainTokenBudget, OutputTokenEstimate, fingerprint,
)
from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_core import CoreRecordsCapacity
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from src.modalities.text.tokenization import chat_token_count
from src.semantic_methods.lotus.record_chain import LotusRecordChain
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.execution_provider.chain_tokenizer_fixture import tokenizer, TEMPLATE
from tests.execution_provider.test_lotus_record_local_query import Process
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.semantic_methods import test_lotus_chain_composition as fixture
from tests.semantic_methods import test_lotus_chain_relation_handoff as relation


class QueryTokenTests(unittest.TestCase):
    def setUp(self):
        import lotus
        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__(); self.addCleanup(scope.__exit__, None, None, None)
        directory = tempfile.TemporaryDirectory(prefix="query-token-")
        self.addCleanup(directory.cleanup)
        self.config = Path(directory.name) / "responses.json"
        self.model = EngineSpec(str(self.config), sys.executable, sys.executable, max_model_len=4096)
        self.tok = tokenizer()

    def responses(self, plan, rows, **options):
        responses = {stage.stage_id + "|" + row.row_id: dict(text=(
            str((row.source_position + i) % 3 != 1) if stage.kind == "filter" else
            "derived/" + stage.stage_id + "/" + row.row_id))
            for i, stage in enumerate(plan.stages) for row in rows}
        value = dict(responses=responses, **options)
        self.config.write_text(json.dumps(value))
        return value

    def budget(self, plan, W=2048):
        return ChainTokenBudget(self.tok, self.model, active_tokens=W,
            tokenizer_signature=fingerprint(self.tok.backend_tokenizer.to_str()),
            template_signature=fingerprint(TEMPLATE),
            expected_output={stage.stage_id: OutputTokenEstimate(2, "declared CPU fixture")
                             for stage in plan.stages})

    def owner(self, plan, *, budget=None, C=2, explicit=True):
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=C, query_timeout_s=12, startup_timeout_s=8)
        actors = AsyncRecordsActorOwner.for_prepared(self.model, plan.stages[0].generation, resources)
        options = dict(token_budget=budget) if explicit else {}
        owner = SemLoomRecordChainCore(self.model, resources, execution="record_local_sections",
            actor_owner=actors, capacity=CoreRecordsCapacity(item_input_bytes=65536,
                item_result_bytes=65536), **options)
        self.addCleanup(owner.close, timeout=3)
        return owner

    def events(self):
        path = Path(str(self.config) + ".events")
        return [] if not path.exists() else [json.loads(line) for line in path.read_text().splitlines()]

    def until(self, stream, predicate):
        deadline = time.monotonic() + 5
        while not predicate():
            stream._tick()
            if time.monotonic() >= deadline:
                self.fail("declared CPU query state was not reached")

    def test_default_count_and_explicit_none_keep_the_same_configuration(self):
        plan = fixture.chain_for("FM")
        with ThreadedRuntime([]):
            default, explicit = self.owner(plan, explicit=False), self.owner(plan)
            self.assertIsNone(default.token_budget)
            self.assertIsNone(explicit.token_budget)
            self.assertEqual(default.resources, explicit.resources)
            self.assertEqual(default.capacity, explicit.capacity)

    def test_token_constructor_rejects_fused_wrong_type_and_other_engine(self):
        plan = fixture.chain_for("M")
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=1)
        with self.assertRaisesRegex(ValueError, "record_local_sections"):
            SemLoomRecordChainCore(self.model, resources, token_budget=self.budget(plan))
        with self.assertRaisesRegex(TypeError, "ChainTokenBudget"):
            SemLoomRecordChainCore(self.model, resources, execution="record_local_sections", token_budget={})
        with self.assertRaisesRegex(ValueError, "model"):
            SemLoomRecordChainCore(replace(self.model, model_path="/tmp/other-cpu-fixture"), resources,
                execution="record_local_sections", token_budget=self.budget(plan))

    def test_unsupported_plan_rejected_before_source_or_model_entry(self):
        base = fixture.chain_for("MM")
        role_change = LotusRecordChain((base.stages[0], replace(base.stages[1], model_role="helper")),
                                      query_id=base.query_id)
        for plan, expected_error in [(relation.relation_chain("MEM"), "Filter/Map"),
                                     (role_change, "model role")]:
            with self.subTest(stages=[stage.kind for stage in plan.stages]), ThreadedRuntime([]) as runtime:
                owner = self.owner(plan, budget=self.budget(plan))
                with self.assertRaisesRegex(ValueError, expected_error):
                    owner.evaluate(None, plan, expected=None, query_id=plan.query_id)
                self.assertEqual(runtime.async_actors, [])
                self.assertIsNone(owner._root_run)

    def test_stage_configuration_checked_before_source_entry(self):
        plan, other = fixture.chain_for("FM"), fixture.chain_for("F")
        with ThreadedRuntime([]) as runtime:
            owner = self.owner(plan, budget=self.budget(other))
            with self.assertRaisesRegex(ValueError, "stages"):
                owner.evaluate(None, plan, expected=None, query_id=plan.query_id)
            self.assertEqual(runtime.async_actors, [])

    def test_owner_snapshots_declared_token_configuration(self):
        plan = fixture.chain_for("M")
        declared = self.budget(plan)
        with ThreadedRuntime([]):
            owner = self.owner(plan, budget=declared)
            declared.active_tokens = 1
            declared.expected_output = {}
            self.assertEqual(owner.token_budget.active_tokens, 2048)
            self.assertEqual(set(owner.token_budget.expected_output), {"step_0"})

    def test_Filter_false_and_empty_source_do_not_create_successors(self):
        plan = fixture.chain_for("FM")
        for size in (0, 2):
            with self.subTest(size=size), ThreadedRuntime([]) as runtime, \
                    patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
                rows = fixture.source_rows(size=size)
                config = self.responses(plan, rows)
                for row in rows:
                    config["responses"]["step_0|" + row.row_id]["text"] = "False"
                self.config.write_text(json.dumps(config))
                owner = self.owner(plan, budget=self.budget(plan))
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                self.assertEqual(list(stream), [])
                self.assertEqual(len(owner.calls), size)
                self.assertTrue(all(c["algorithm_stage"] == "step_0" for c in owner.calls))
                self.assertEqual(owner.last_close_report["core_usage"]["active_work"], 0)
                self.assertTrue(owner.last_close_report["query_completed"])
                if size == 0:
                    self.assertEqual(runtime.async_actors, [])

    def test_actual_successor_messages_generation_parent_and_result_match_count(self):
        plan, rows = fixture.chain_for("MMF"), fixture.source_rows(size=3)
        self.responses(plan, rows)
        expected = relation.reference(plan, rows, fixture.ResponseFixture(plan))
        outputs = []
        for token in (False, True):
            with self.subTest(token=token), ThreadedRuntime([]), \
                    patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
                owner = self.owner(plan, budget=self.budget(plan) if token else None)
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                outputs.append(fixture.normalized(dict(records=list(stream), calls=owner.calls,
                                                       columns=stream.columns)))
                self.assertEqual(outputs[-1], fixture.normalized(expected))
                self.assertEqual(owner.last_close_report["work_unit"], "tokens" if token else "model_calls")
                self.assertEqual(owner.last_close_report["core_usage"]["active_work"], 0)
                self.assertEqual(stream.pool.used, (0, 0))
                for call in owner.calls:
                    if token:
                        cost = call["token_estimate"]
                        self.assertEqual(cost["input_tokens"], chat_token_count(self.tok,
                            json.loads(call["fixture_messages"])))
                        self.assertEqual(cost["reserved_tokens"], cost["input_tokens"] +
                                         call["fixture_generation"]["max_tokens"])
                    else:
                        self.assertNotIn("token_estimate", call)
                    if call["algorithm_stage"] == "step_1":
                        self.assertIn("derived/step_0/" + call["row_id"], call["fixture_messages"])
                        self.assertEqual(len(call["parent_call_ids"]), 1)
        self.assertEqual(outputs[0], outputs[1])

    def test_W_blocks_below_C_then_successful_terminal_restores_supply(self):
        plan, rows = fixture.chain_for("M"), fixture.source_rows(size=2)
        self.responses(plan, rows)
        budget = self.budget(plan)
        run = plan.start_batch(rows); self.assertTrue(run.prepare())
        costs = [budget.estimate(row, plan.stages[0].generation)["reserved_tokens"] for row in run.prepared]
        W = max(costs)
        ready, release = threading.Event(), threading.Event()
        original = AsyncCoreModelActor._run_core
        def hold_first(actor, *args):
            result = original(actor, *args)
            if args[0][0]["source_position"] == 0:
                ready.set()
                if not release.wait(5):
                    raise TimeoutError("CPU test did not release the first receipt")
            return result
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process), \
                patch.object(AsyncCoreModelActor, "_run_core", hold_first):
            owner = self.owner(plan, budget=self.budget(plan, W))
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.until(stream, ready.is_set)
                for _ in range(6):
                    stream._tick()
                usage = owner.core.capacity.usage()
                self.assertEqual(usage.active_requests, 1)
                self.assertEqual(usage.active_work, costs[0])
                self.assertLess(usage.active_requests, owner.core.capacity.limits.active_requests)
                self.assertEqual(owner.core.capacity.limits.active_work, W)
                self.assertEqual(stream.session.spec.work_unit, "tokens")
                self.assertEqual(sum(e["event"] == "start" for e in self.events()), 1)
                release.set()
                self.assertEqual(len(list(stream)), 2)
                self.assertEqual(len(owner.calls), 2)
                self.assertEqual(owner.last_close_report["core_usage"]["active_work"], 0)
                self.assertLessEqual(owner.last_close_report["peak_core_usage"]["active_work"], W)
                self.assertEqual(stream.pool.used, (0, 0))
            finally:
                release.set()
                owner.close(timeout=3)

    def test_oversize_successor_stops_before_its_submission(self):
        plan, rows = fixture.chain_for("MM"), fixture.source_rows(size=1)
        config = self.responses(plan, rows)
        config["responses"]["step_0|occ-0"]["text"] = "long output " * 10
        self.config.write_text(json.dumps(config))
        budget = self.budget(plan)
        run = plan.start_batch(rows); self.assertTrue(run.prepare())
        W = budget.estimate(run.prepared[0], plan.stages[0].generation)["reserved_tokens"]
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan, budget=self.budget(plan, W))
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            with self.assertRaisesRegex(ValueError, "exceeds W"):
                list(stream)
            self.assertEqual(sum(e["event"] == "start" for e in self.events()), 1)
            self.assertFalse(owner.last_close_report["query_completed"])
            self.assertEqual(owner.last_close_report["core_usage"]["active_work"], 0)
            self.assertEqual(stream.pool.used, (0, 0))

    def test_cancel_unknown_preserves_receipt_and_stops_successors(self):
        plan, rows = fixture.chain_for("MM"), fixture.source_rows(size=2)
        config = self.responses(plan, rows, hold=["step_0|occ-0"])
        config["responses"]["step_0|occ-0"]["status"] = "unknown"
        self.config.write_text(json.dumps(config))
        cancelled = [False]
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan, budget=self.budget(plan), C=1)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id,
                                    cancelled=lambda: cancelled[0])
            self.until(stream, lambda: any(e["event"] == "start" for e in self.events()))
            self.assertGreater(owner.core.capacity.usage().active_work, 0)
            cancelled[0] = True
            with self.assertRaises(InterruptedError):
                next(stream)
            report = owner.close(timeout=3)
            self.assertEqual(len(owner.calls), 1)
            self.assertEqual(owner.calls[0]["status"], "unknown")
            self.assertEqual(owner.calls[0]["total_tokens"], 14)
            self.assertIn("token_estimate", owner.calls[0])
            self.assertTrue(report["producer_retirement"]["confirmed"])
            self.assertEqual(report["core_usage"]["active_work"], 0)
            self.assertFalse(report["query_completed"])
            self.assertEqual(stream.pool.used, (0, 0))

    def test_failed_terminal_preserves_failure_without_successor(self):
        plan, rows = fixture.chain_for("MM"), fixture.source_rows(size=1)
        config = self.responses(plan, rows)
        config["responses"]["step_0|occ-0"]["status"] = "failed"
        self.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner(plan, budget=self.budget(plan))
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            with self.assertRaises(RuntimeError):
                list(stream)
            self.assertEqual(len(owner.calls), 1)
            self.assertEqual(owner.calls[0]["status"], "failed")
            self.assertEqual(owner.calls[0]["total_tokens"], 14)
            self.assertIn("token_estimate", owner.calls[0])
            self.assertEqual(owner.last_close_report["core_usage"]["active_work"], 0)
            self.assertFalse(owner.last_close_report["query_completed"])
            self.assertEqual(stream.pool.used, (0, 0))

    def test_unconfirmed_close_keeps_W_until_late_known_terminal_without_replay(self):
        plan, rows = fixture.chain_for("MM"), fixture.source_rows(size=1)
        self.responses(plan, rows)
        ready, release = threading.Event(), threading.Event()
        original = AsyncCoreModelActor._run_core
        def hold_delivery(actor, *args):
            value = original(actor, *args)
            ready.set()
            if not release.wait(5):
                raise TimeoutError("CPU test did not release the late receipt")
            return value
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=Process), \
                patch.object(AsyncCoreModelActor, "_run_core", hold_delivery):
            owner = self.owner(plan, budget=self.budget(plan), C=1)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.until(stream, ready.is_set)
                before = deepcopy(owner.close(timeout=0.02))
                self.assertEqual(before["status"], "unknown")
                self.assertGreater(before["core_usage"]["active_work"], 0)
                release.set()
                # Fixture release precedes delivery through the async transport.
                for pending in tuple(owner.transport.backend._slots.values()):
                    pending.future.result(timeout=3)
                after = owner.close(timeout=3)
                self.assertEqual(after["status"], "closed")
                self.assertEqual(after["core_usage"]["active_work"], 0)
                self.assertEqual(len(owner.calls), 1)
                self.assertEqual(owner.calls[0]["status"], "succeeded")
                self.assertEqual(stream.run.position, 0)
                self.assertFalse(after["query_completed"])
                self.assertEqual(stream.pool.used, (0, 0))
            finally:
                release.set()
                owner.close(timeout=3)


if __name__ == "__main__":
    unittest.main()
