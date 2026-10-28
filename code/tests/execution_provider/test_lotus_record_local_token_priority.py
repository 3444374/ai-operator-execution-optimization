"""Joint token admission and historical ready selection through full-query CPU IPC."""
from dataclasses import replace
import inspect
import json
import threading
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_record_chain_fifo import LotusChainFIFO
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from src.execution_provider.adapters.lotus_records_core import CoreRecordsCapacity
from src.scheduling.core.session_contract import SessionSpec, TaskProfile
from src.semantic_methods.budget import MethodBudgetPool, MethodCapacity, row_reservation
from src.semantic_methods.continuation import MethodLimits
from src.semantic_methods.driver import SuccessorFirst
from src.semantic_methods.lotus.record_chain import LotusRecordChain
from src.semantic_methods.lotus.records import RecordsResources
from tests.execution_provider import test_lotus_record_local_tokens as token_tests
from tests.execution_provider import test_lotus_record_local_query as query_fixture
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.semantic_methods import test_lotus_chain_composition as fixture
from tests.semantic_methods import test_lotus_chain_relation_handoff as relation
from tests.scheduling.test_incremental_session import setup


class JointTokenPriorityTests(token_tests.QueryTokenTests):
    """Replay the token lifecycle contracts with successor selection enabled too."""

    def make_owner(self, plan, *, C=2, **options):
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=C, query_timeout_s=12, startup_timeout_s=8)
        actors = AsyncRecordsActorOwner.for_prepared(self.model, plan.stages[0].generation, resources)
        owner = SemLoomRecordChainCore(self.model, resources, execution="record_local_sections",
            actor_owner=actors, capacity=CoreRecordsCapacity(item_input_bytes=65536,
                item_result_bytes=65536), **options)
        self.addCleanup(owner.close, timeout=3)
        return owner

    def owner(self, plan, *, budget=None, C=2, explicit=True, **options):
        selected = dict(ready_order="successor_first", successor_first=SuccessorFirst(2, 1.0))
        selected.update(options)
        if explicit:
            selected["token_budget"] = budget
        return self.make_owner(plan, C=C, **selected)

    def test_four_configurations_preserve_messages_generation_identity_and_results(self):
        plan, rows = fixture.chain_for("MFMF"), fixture.source_rows(size=3)
        self.responses(plan, rows)
        replay = fixture.ResponseFixture(plan)
        expected = fixture.normalized(relation.reference(plan, rows, replay))
        outputs = []
        self.assertEqual(inspect.signature(SemLoomRecordChainCore).parameters["ready_order"].default, "fifo")
        self.assertIsNone(inspect.signature(SemLoomRecordChainCore).parameters["token_budget"].default)
        for token, priority in ((False, False), (True, False), (False, True), (True, True)):
            with self.subTest(token=token, priority=priority), ThreadedRuntime([]), \
                    patch.object(AsyncCoreModelActor, "_process_type", return_value=query_fixture.Process):
                events, options = [], {}
                if token:
                    options["token_budget"] = self.budget(plan)
                if priority:
                    options.update(ready_order="successor_first", successor_first=SuccessorFirst(2, 1.0),
                                   on_ready_event=events.append)
                owner = self.make_owner(plan, **options)
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                outputs.append(fixture.normalized(dict(records=list(stream), calls=owner.calls,
                                                       columns=stream.columns)))
                self.assertEqual(outputs[-1], expected)
                for call in owner.calls:
                    self.assertEqual(call["fixture_messages"], replay.requests[call["call_id"]][0])
                    stage = next(stage for stage in plan.stages if stage.stage_id == call["algorithm_stage"])
                    self.assertEqual(call["fixture_generation"], json.loads(json.dumps(stage.generation.kwargs())))
                    self.assertEqual("token_estimate" in call, token)
                report = owner.last_close_report
                self.assertEqual(report["work_unit"], "tokens" if token else "model_calls")
                self.assertEqual("ready_selection" in report, priority)
                if priority:
                    accepted = [event for event in events if event.action == "Core_offer_accepted"]
                    self.assertEqual(len(accepted), len(owner.calls))
                    self.assertEqual(len({event.key for event in accepted}), len(accepted))
                    self.assertEqual(report["ready_selection"]["order"], "successor_first")
                else:
                    self.assertEqual(owner._ready_options, {})
                self.assertTrue(report["query_completed"])
                self.assertEqual(report["core_usage"]["active_work"], 0)
                self.assertEqual(stream.pool.used, (0, 0))
        self.assertTrue(all(output == outputs[0] for output in outputs))

    def test_joint_unsupported_plans_fail_before_source_or_model(self):
        base = fixture.chain_for("MM")
        role_change = LotusRecordChain((base.stages[0], replace(base.stages[1], model_role="helper")),
                                      query_id=base.query_id)
        for plan, budget_plan, message in [(relation.relation_chain("MEM"), None, "Filter/Map"),
                                         (role_change, None, "model role"),
                                         (base, fixture.chain_for("M"), "stages")]:
            with self.subTest(message=message), ThreadedRuntime([]) as runtime:
                owner = self.owner(plan, budget=self.budget(plan if budget_plan is None else budget_plan))
                with self.assertRaisesRegex(ValueError, message):
                    owner.evaluate(None, plan, expected=None, query_id=plan.query_id)
                self.assertIsNone(owner._root_run)
                self.assertEqual(runtime.async_actors, [])
        with self.assertRaisesRegex(ValueError, "record_local_sections"):
            SemLoomRecordChainCore(self.model, RecordsResources(), token_budget=self.budget(base),
                ready_order="successor_first", successor_first=SuccessorFirst(2, 1.0))

    def test_joint_W_limits_native_supply_below_C_without_reordering_accepted_calls(self):
        base, rows = fixture.chain_for("MM"), fixture.source_rows(size=2)
        # Large declared output caps isolate W pressure from the request-count cap.
        plan = LotusRecordChain(tuple(replace(stage, generation=replace(stage.generation, max_tokens=512))
                                      for stage in base.stages), query_id=base.query_id)
        self.responses(plan, rows)
        declared = self.budget(plan)
        run = plan.start_batch(rows)
        self.assertTrue(run.prepare())
        costs = [declared.estimate(row, plan.stages[0].generation)["reserved_tokens"] for row in run.prepared]
        W = max(costs) + 64
        self.assertLess(W, sum(costs))
        clock, events = [0.0], []
        ready, release = threading.Event(), threading.Event()
        original = AsyncCoreModelActor._run_core

        def hold_first(actor, *args):
            result = original(actor, *args)
            if args[0][0]["algorithm_stage"] == "step_0" and args[0][0]["source_position"] == 0:
                ready.set()
                if not release.wait(5):
                    raise TimeoutError("CPU test did not release the first receipt")
            return result

        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=query_fixture.Process), \
                patch.object(AsyncCoreModelActor, "_run_core", hold_first):
            owner = self.owner(plan, budget=self.budget(plan, W), C=2,
                successor_first=SuccessorFirst(2, 0.5), ready_clock=lambda: clock[0],
                on_ready_event=events.append)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.until(stream, ready.is_set)
                for _ in range(6):
                    stream._tick()
                usage = owner.core.capacity.usage()
                self.assertEqual(usage.active_requests, 1)
                self.assertEqual(usage.active_work, costs[0])
                self.assertLess(usage.active_requests, owner.core.capacity.limits.active_requests)
                # Core may queue both calls before W permits the second native start.
                accepted = [event for event in events if event.action == "Core_offer_accepted"]
                self.assertEqual([(event.row.sequence, event.stage_index) for event in accepted], [(0, 0), (1, 0)])
                self.assertTrue(all(event.ready_at == 0.0 for event in accepted))
                self.assertEqual(sum(event["event"] == "start" for event in self.events()), 1)
                self.assertEqual(stream.driver._successor_burst, 0)
                clock[0] = 0.6
                release.set()
                self.assertEqual(len(list(stream)), 2)
                accepted = [event for event in events if event.action == "Core_offer_accepted"]
                self.assertEqual([(event.row.sequence, event.stage_index) for event in accepted[:2]], [(0, 0), (1, 0)])
                self.assertEqual(len(accepted), 4)
                self.assertLessEqual(owner.last_close_report["peak_core_usage"]["active_work"], W)
                self.assertEqual(owner.last_close_report["core_usage"]["active_work"], 0)
                self.assertEqual(stream.pool.used, (0, 0))
            finally:
                release.set()
                owner.close(timeout=3)

    def test_joint_ready_age_and_burst_fairness_with_actual_offer_backpressure(self):
        plan, rows = fixture.chain_for("MMMM"), fixture.source_rows(size=2)
        for aging in (0.5, 100.0):
            with self.subTest(aging=aging):
                core, old, backend, clock = setup(metadata_bytes=4096, held_tasks=1,
                    active_requests=1, active_work=2048, input_bytes=65536,
                    item_input_bytes=65536, result_bytes=65536, item_result_bytes=65536)
                old.close()
                session = core.open(SessionSpec("joint-query", "flow", "fixture", work_unit="tokens",
                    task_profiles=tuple(TaskProfile(stage.stage_id, "fixture") for stage in plan.stages)))
                limits = MethodLimits(65536, 131072, 131072, len(plan.stages))
                pool = MethodBudgetPool(MethodCapacity(2, 2 * row_reservation(limits)))
                events = []
                driver = LotusChainFIFO(session, plan, limits, pool.allocate(pool.capacity),
                    token_budget=self.budget(plan), ready_order="successor_first",
                    successor_first=SuccessorFirst(2, aging), clock=clock, on_ready_event=events.append)
                self.addCleanup(driver.close)
                for row in rows:
                    self.assertTrue(driver.offer_record(row))
                driver.end_input()
                driver.advance(); driver.advance()
                for _ in range(4):
                    clock.now += 0.1
                    driver.advance()
                blocked = [event for event in events if event.action == "Core_backpressure"]
                self.assertTrue(blocked)
                self.assertTrue(all(event.row.sequence == 1 and event.ready_at == 0 for event in blocked))
                self.assertEqual(driver._successor_burst, 0)
                clock.now = 0.6
                replay, outputs = fixture.ResponseFixture(plan), []
                for _ in range(80):
                    for key in tuple(backend.pending):
                        fixture.complete_one(backend, key, replay)
                    driver.advance()
                    for result in driver.results(2):
                        outputs.append(json.loads(result.value))
                        driver.release_result(result.row)
                    if driver.finished:
                        break
                self.assertTrue(driver.finished)
                accepted = [event for event in events if event.action == "Core_offer_accepted"]
                pairs = [(event.row.sequence, event.stage_index) for event in accepted]
                if aging == 0.5:
                    self.assertEqual(pairs[:2], [(0, 0), (1, 0)])
                    self.assertEqual(accepted[1].reason, "aged")
                    self.assertEqual(accepted[1].ready_at, 0)
                else:
                    self.assertEqual(pairs[:4], [(0, 0), (0, 1), (0, 2), (1, 0)])
                    self.assertEqual(accepted[3].reason, "source_turn")
                self.assertEqual(len(pairs), 8)
                self.assertEqual(len(set(pairs)), 8)
                self.assertEqual(len(outputs), 2)
                self.assertEqual(core.capacity.usage().active_work, 0)
                self.assertEqual(pool.used, (0, 0))
                driver.close()
                self.assertEqual(pool.allocated, (0, 0))

    def test_joint_required_clock_failure_releases_local_work_before_native_entry(self):
        plan, rows = fixture.chain_for("FM"), fixture.source_rows(size=1)
        self.responses(plan, rows)

        def unavailable():
            raise OSError("CPU fixture ready clock unavailable")

        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, "_process_type", return_value=query_fixture.Process):
            owner = self.owner(plan, budget=self.budget(plan), ready_clock=unavailable)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            with self.assertRaisesRegex(OSError, "ready clock"):
                list(stream)
            self.assertEqual(sum(event["event"] == "start" for event in self.events()), 0)
            self.assertEqual(owner.last_close_report["core_usage"]["active_work"], 0)
            self.assertEqual(stream.pool.used, (0, 0))
            self.assertFalse(owner.last_close_report["query_completed"])
