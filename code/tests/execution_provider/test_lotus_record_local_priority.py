"""Historical ready selection through the maintained full-query CPU IPC path."""
from copy import deepcopy
from dataclasses import asdict, replace
import inspect
import json
import sys
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_record_chain_fifo import LotusChainFIFO
from src.execution_provider.adapters.lotus_records_core import CoreRecordsCapacity
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from src.semantic_methods.driver import MethodDriver, SuccessorFirst
from src.semantic_methods.lotus.record_chain import LotusRecordChain
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.execution_provider import test_lotus_record_local_query as query_fixture
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.semantic_methods import test_lotus_chain_composition as fixture
from tests.semantic_methods import test_lotus_chain_relation_handoff as relation


class QueryPriorityTests(unittest.TestCase):
    setUp = query_fixture.QueryOwnerTests.setUp
    config_for = query_fixture.QueryOwnerTests.config_for
    events = query_fixture.QueryOwnerTests.events

    def owner(self, plan, *, C=2, **options):
        model = EngineSpec(str(self.config), sys.executable, sys.executable, max_model_len=4096)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=C, query_timeout_s=12, startup_timeout_s=8)
        actors = AsyncRecordsActorOwner.for_prepared(model, plan.stages[0].generation, resources)
        owner = SemLoomRecordChainCore(model, resources, execution="record_local_sections",
            actor_owner=actors, capacity=CoreRecordsCapacity(item_input_bytes=65536,
                item_result_bytes=65536), **options)
        self.addCleanup(owner.close, timeout=3)
        return owner

    def priority(self, **options):
        return dict(ready_order="successor_first", successor_first=SuccessorFirst(2, 1.0), **options)

    def until(self, stream, predicate):
        deadline = time.monotonic() + 5
        while not predicate():
            stream._tick()
            if time.monotonic() >= deadline:
                self.fail("declared CPU query state was not reached")

    def test_generic_round_robin_wrapper_FIFO_and_query_FIFO_defaults_stay_distinct(self):
        self.assertEqual(inspect.signature(MethodDriver).parameters["ready_order"].default, "round_robin")
        self.assertEqual(inspect.signature(LotusChainFIFO).parameters["ready_order"].default, "fifo")
        plan, rows = fixture.chain_for("FM"), fixture.source_rows(size=2)
        self.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=query_fixture.Process):
            owner = self.owner(plan)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            stream._tick()
            self.assertEqual(stream.driver._ready_order, "fifo")
            self.assertEqual(owner._ready_options, {})
            self.assertEqual(len(list(stream)), 1)
            self.assertNotIn("ready_selection", owner.last_close_report)

    def test_full_query_policies_preserve_actual_messages_generation_parents_and_results(self):
        plan, rows = fixture.chain_for("MFMF"), fixture.source_rows(size=3)
        config = self.config_for(plan, rows)
        class Replies(fixture.ResponseFixture):
            def one(self, row, generation):
                value = super().one(row, generation)
                value["text"] = config["responses"][row["algorithm_stage"] + "|" + row["row_id"]]["text"]
                return value
        replay = Replies(plan)
        expected = fixture.normalized(relation.reference(plan, rows, replay))
        by_stage = {stage.stage_id: stage for stage in plan.stages}
        for order in ("round_robin", "fifo", "successor_first"):
            with self.subTest(order=order), ThreadedRuntime([]) as runtime, \
                    patch.object(AsyncCoreModelActor, "_process_type", return_value=query_fixture.Process):
                events = []
                settings = SuccessorFirst(2, 1.0) if order == "successor_first" else None
                owner = self.owner(plan, ready_order=order, successor_first=settings,
                                   on_ready_event=events.append)
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                actual = fixture.normalized(dict(records=list(stream), calls=owner.calls, columns=stream.columns))
                self.assertEqual(actual, expected)
                offered = [event for event in events if event.action == "Core_offer_accepted"]
                self.assertEqual(len(offered), len(owner.calls))
                self.assertEqual(len({event.key for event in offered}), len(offered))
                self.assertTrue(all(event.at >= event.ready_at for event in offered))
                if settings is not None:
                    self.assertTrue(any(event.stage_index > 0 and event.reason in
                        ("only_successor", "successor_first", "aged") for event in offered))
                for call in owner.calls:
                    self.assertEqual(call["fixture_messages"], replay.requests[call["call_id"]][0])
                    self.assertEqual(call["fixture_generation"], json.loads(json.dumps(
                        by_stage[call["algorithm_stage"]].generation.kwargs())))
                    if call["algorithm_stage"] == "step_1":
                        self.assertIn("value/step_0/" + call["row_id"], call["fixture_messages"])
                        self.assertEqual(len(call["parent_call_ids"]), 1)
                self.assertEqual(owner.last_close_report["ready_selection"]["order"], order)
                self.assertEqual(owner.last_close_report["ready_selection"]["successor_first"],
                                 None if settings is None else asdict(settings))
                self.assertEqual(owner.last_close_report["core_usage"]["held_tasks"], 0)
                self.assertEqual(stream.pool.used, (0, 0))
                self.assertEqual(len(runtime.async_actors), 1)

    def test_invalid_configuration_and_fused_options_fail_at_construction(self):
        model = EngineSpec(str(self.config), sys.executable, sys.executable)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=1)
        for options in [dict(ready_order="missing"), dict(ready_order="successor_first"),
                        dict(successor_first=SuccessorFirst(2, 1.0)),
                        dict(ready_order="successor_first", successor_first={})]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                SemLoomRecordChainCore(model, resources, execution="record_local_sections", **options)
        for options in [dict(ready_clock=1), dict(on_ready_event=1)]:
            with self.subTest(options=options), self.assertRaises(TypeError):
                SemLoomRecordChainCore(model, resources, execution="record_local_sections", **options)
        with self.assertRaisesRegex(ValueError, "record_local_sections"):
            SemLoomRecordChainCore(model, resources, **self.priority())

    def test_Extract_and_role_change_rejected_before_source_or_model_entry(self):
        base = fixture.chain_for("MM")
        role_change = LotusRecordChain((base.stages[0], replace(base.stages[1], model_role="helper")),
                                      query_id=base.query_id)
        for plan, message in [(relation.relation_chain("MEM"), "Filter/Map"),
                              (role_change, "model role")]:
            with self.subTest(message=message), ThreadedRuntime([]) as runtime:
                owner = self.owner(plan, **self.priority())
                with self.assertRaisesRegex(ValueError, message):
                    owner.evaluate(None, plan, expected=None, query_id=plan.query_id)
                self.assertIsNone(owner._root_run)
                self.assertEqual(runtime.async_actors, [])

    def test_required_ready_clock_failure_releases_local_method_without_model_call(self):
        plan, rows = fixture.chain_for("FM"), fixture.source_rows(size=1)
        self.config_for(plan, rows)
        def unavailable():
            raise OSError("CPU fixture ready clock unavailable")
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=query_fixture.Process):
            owner = self.owner(plan, **self.priority(ready_clock=unavailable))
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            with self.assertRaisesRegex(OSError, "ready clock"):
                list(stream)
            self.assertEqual(sum(event["event"] == "start" for event in self.events()), 0)
            self.assertEqual(owner.last_close_report["core_usage"]["held_tasks"], 0)
            self.assertEqual(stream.pool.used, (0, 0))
            self.assertFalse(owner.last_close_report["query_completed"])

    def test_optional_FIFO_event_clock_failure_disables_observation_and_completes(self):
        plan, rows = fixture.chain_for("MM"), fixture.source_rows(size=1)
        self.config_for(plan, rows)
        def unavailable():
            raise OSError("CPU fixture observation clock unavailable")
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=query_fixture.Process):
            events = []
            owner = self.owner(plan, ready_clock=unavailable, on_ready_event=events.append)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            stream._tick(); stream._tick()
            self.assertEqual(stream.driver.ready_observer_error, "OSError")
            self.assertEqual(len(list(stream)), 1)
            self.assertEqual(events, [])
            self.assertEqual(len(owner.calls), 2)
            self.assertTrue(owner.last_close_report["query_completed"])

    def test_cancel_unknown_does_not_create_successor_or_rewrite_usage(self):
        plan, rows = fixture.chain_for("MM"), fixture.source_rows(size=2)
        config = self.config_for(plan, rows, hold=["step_0|occ-0"])
        config["responses"]["step_0|occ-0"]["status"] = "unknown"
        self.config.write_text(json.dumps(config))
        cancelled = [False]
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=query_fixture.Process):
            owner = self.owner(plan, C=1, **self.priority())
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id,
                                    cancelled=lambda: cancelled[0])
            self.until(stream, lambda: any(event["event"] == "start" for event in self.events()))
            cancelled[0] = True
            with self.assertRaises(InterruptedError):
                next(stream)
            report = owner.close(timeout=3)
            self.assertEqual(len(owner.calls), 1)
            self.assertEqual(owner.calls[0]["status"], "unknown")
            self.assertEqual(owner.calls[0]["total_tokens"], 14)
            self.assertFalse(report["query_completed"])
            self.assertTrue(report["producer_retirement"]["confirmed"])
            self.assertEqual(report["core_usage"]["held_tasks"], 0)
            self.assertEqual(stream.pool.used, (0, 0))

    def test_unconfirmed_close_retains_remote_work_until_late_receipt_without_replay(self):
        plan, rows = fixture.chain_for("MM"), fixture.source_rows(size=1)
        self.config_for(plan, rows)
        ready, release = threading.Event(), threading.Event()
        original = AsyncCoreModelActor._run_core
        def hold_delivery(actor, *args):
            value = original(actor, *args)
            ready.set()
            if not release.wait(5):
                raise TimeoutError("CPU test did not release the late receipt")
            return value
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=query_fixture.Process), \
                patch.object(AsyncCoreModelActor, "_run_core", hold_delivery):
            owner = self.owner(plan, C=1, **self.priority())
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.until(stream, ready.is_set)
                first = deepcopy(owner.close(timeout=0.02))
                self.assertEqual(first["status"], "unknown")
                self.assertEqual(first["core_usage"]["held_tasks"], 1)
                release.set()
                after = owner.close(timeout=3)
                self.assertEqual(after["status"], "closed")
                self.assertEqual(after["core_usage"]["held_tasks"], 0)
                self.assertEqual(len(owner.calls), 1)
                self.assertEqual(owner.calls[0]["status"], "succeeded")
                self.assertEqual(stream.run.position, 0)
                self.assertFalse(after["query_completed"])
            finally:
                release.set()
                owner.close(timeout=3)


if __name__ == "__main__":
    unittest.main()
