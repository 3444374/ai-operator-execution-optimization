"""New profile wiring over existing real Core, threaded actor and CPU IPC tests."""

import json
import sys
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters import lotus_records_async_actor as async_actor
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.execution_provider import test_lotus_records_async_core as async_fixture
from tests.execution_provider import test_lotus_records_core as sync_fixture


class ProfiledAsyncCoreTests(async_fixture.AsyncCoreCompositionTests):
    """Inherited gates test peer admission, late cancel usage and cross-session ack."""

    def owner(self):
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=2, query_timeout_s=8, startup_timeout_s=5)
        return core.create_records_core(
            FilterMethod("fixture", instruction="Keep {reviewText}"),
            EngineSpec("/tmp/fixture-model", "/tmp/model-python", "/tmp/driver-python", max_num_seqs=4),
            GenerationSpec(), resources,
            profile=dict(mode="async", max_inflight="auto", stage_timing=True),
        )

    def test_outer_requests_above_native_iteration_capacity_keep_IPC_limits_and_terminal_accounting(self):
        rows = self.rows(["hold"] * 6)
        with async_fixture.ThreadedRuntime(rows) as runtime:
            owner = core.create_records_core(
                FilterMethod("fixture", instruction="Keep {reviewText}"),
                EngineSpec("/tmp/fixture-model", "/tmp/model-python", "/tmp/driver-python", max_num_seqs=1),
                GenerationSpec(), RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                    max_active_batches=8, query_timeout_s=8, startup_timeout_s=5),
                profile=dict(mode="async", max_inflight=8))
            try:
                stream = owner.evaluate(async_fixture.Source(rows), query_id="outer-above-native", max_inflight=4)
                stream._fill()
                until = time.monotonic() + 3
                engine = runtime.async_actors[0].instance.engine
                while engine.snapshot()["accepted_requests"] < 4:
                    stream.session.advance(1)
                    self.assertLess(time.monotonic(), until)
                    owner.core.wake.wait(owner.core.wake.generation, .005)
                self.assertEqual(owner.engine.max_num_seqs, 1)
                self.assertEqual(owner._actors.limits.max_inflight, 8)
                self.assertEqual(owner._actors.limits.result_slots, 8)
                self.assertEqual(stream.session.limits.active_requests, 4)
                self.assertEqual(engine.snapshot()["active_requests"], 4)
                self.assertEqual(owner.core.capacity.usage().held_tasks, 4)
                self.assertLessEqual(engine.snapshot()["input_bytes"], owner._actors.limits.input_bytes)
                stream.close()
                while owner.core.capacity.usage().held_tasks:
                    owner.core.reap(owner.capacity.step_actions)
                    self.assertLess(time.monotonic(), until)
                    owner.core.wake.wait(owner.core.wake.generation, .005)
                stream.collect_receipts()
                self.assertEqual(len(stream.unconsumed_call_results), 4)
                self.assertEqual(sum(r["prompt_tokens"] for r in stream.unconsumed_call_results), 44)
                self.assertEqual(engine._sequence, 4)
            finally:
                self.assertEqual(owner.close(timeout=3)["status"], "closed")

    def test_consecutive_queries_keep_one_engine_fresh_timing_and_exact_identities(self):
        rows = self.rows(["fast", "fast"])
        with async_fixture.ThreadedRuntime(rows) as runtime:
            owner = self.owner()
            try:
                first = owner.evaluate(async_fixture.Source(rows), query_id="first")
                first_rows = list(first)
                first_timing = first.last_close_report["stage_timings"]
                second = owner.evaluate(async_fixture.Source(rows), query_id="second")
                second_rows = list(second)
                second_timing = second.last_close_report["stage_timings"]
                self.assertEqual(len(runtime.async_actors), 1)
                self.assertEqual(runtime.async_actors[0].instance.engine._sequence, 4)
                self.assertEqual({row["source_position"] for row in first_rows}, {0, 1})
                self.assertEqual({row["source_position"] for row in second_rows}, {0, 1})
                self.assertFalse({row["call_id"] for row in first_rows} & {row["call_id"] for row in second_rows})
                for timing in (first_timing, second_timing):
                    for stage in ("prepare_submit", "model_submit", "dependency_and_model_wait",
                                  "transport_result_processing", "delivery_processing", "lease_release"):
                        self.assertEqual(timing["stages"][stage]["count"], 2)
                        self.assertGreaterEqual(timing["stages"][stage]["total_ns"], 0)
                    self.assertIsNone(timing["prepare_worker_compute_ns"])
                    self.assertIsNone(timing["pure_GPU_compute_ns"])
                    self.assertEqual(timing["timing_errors"], 0)
                    json.dumps(timing, allow_nan=False)
                self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                self.assertEqual(owner.transport.retained_refs(), 0)
                self.assertEqual(owner.execution_metadata()["execution_profile"]["max_inflight"], 2)
            finally:
                self.assertEqual(owner.close(timeout=3)["status"], "closed")


class OptionalStageTimingTests(unittest.TestCase):
    def run_sync(self, *, timing, broken_clock=False):
        fixture = sync_fixture.CoreRecordsTests()
        fixture.setUp()
        with sync_fixture.Runtime(fixture.rows):
            resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                max_active_batches=2, query_timeout_s=3)
            owner = core.create_records_core(FilterMethod("fixture", instruction="Keep {reviewText}"), fixture.spec, GenerationSpec(), resources,
                                             profile=dict(mode="sync", stage_timing=timing))
            try:
                clock_patch = (patch.object(core.time, "monotonic_ns", side_effect=RuntimeError("fixture timer"))
                    if broken_clock else patch.object(core.time, "monotonic_ns", wraps=core.time.monotonic_ns))
                with clock_patch as clock:
                    stream = owner.evaluate(sync_fixture.Source(fixture.rows), query_id="timing")
                    rows = list(stream)
                    report = stream.last_close_report
                    calls = clock.call_count
                self.assertEqual(len(rows), 5)
                self.assertEqual(sum(row["prompt_tokens"] for row in rows), 55)
            finally:
                self.assertEqual(owner.close()["status"], "closed")
        return report, calls

    def test_disabled_timings_do_not_read_the_extra_clock(self):
        report, calls = self.run_sync(timing=False, broken_clock=True)
        self.assertEqual(calls, 0)
        self.assertNotIn("stage_timings", report)

    def test_timer_failure_preserves_query_results_and_reports_missing_measurement(self):
        report, calls = self.run_sync(timing=True, broken_clock=True)
        self.assertGreater(calls, 0)
        self.assertEqual(report["status"], "closed")
        self.assertGreater(report["stage_timings"]["timing_errors"], 0)


class RecordChainCapacityTests(unittest.TestCase):
    def test_record_local_query_shares_the_request_boundary_without_rewriting_native_capacity(self):
        from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
        from tests.execution_provider import test_lotus_record_local_query as chain_fixture

        case = chain_fixture.QueryOwnerTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        plan, rows = chain_fixture.fixture.chain_for("MMF"), chain_fixture.fixture.source_rows(size=3)
        case.config_for(plan, rows)
        model = EngineSpec(str(case.config), sys.executable, sys.executable,
                           max_model_len=4096, max_num_seqs=1)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                                     max_active_batches=3, query_timeout_s=12, startup_timeout_s=8)
        with async_fixture.ThreadedRuntime([]) as runtime, patch.object(
                async_actor.AsyncCoreModelActor, "_process_type", return_value=chain_fixture.Process):
            actors = async_actor.AsyncRecordsActorOwner.for_prepared(model, plan.stages[0].generation, resources)
            owner = SemLoomRecordChainCore(model, resources, execution="record_local_sections",
                actor_owner=actors, capacity=core.CoreRecordsCapacity(item_input_bytes=65536,
                                                                     item_result_bytes=65536))
            try:
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                self.assertEqual(len(list(stream)), 2)
                self.assertEqual(len(owner.calls), 9)
                self.assertEqual(len({call["call_id"] for call in owner.calls}), 9)
                self.assertEqual(len({call["engine_uid"] for call in owner.calls}), 1)
                self.assertEqual(model.max_num_seqs, 1)
                self.assertEqual(actors.limits.max_inflight, 3)
                self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                self.assertEqual(len(runtime.async_actors), 1)
            finally:
                self.assertEqual(owner.close(timeout=3)["status"], "closed")


if __name__ == "__main__":
    unittest.main()
