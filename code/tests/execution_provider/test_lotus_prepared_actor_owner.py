"""Prepared-only owner lifecycle; explicit software actors and CPU IPC only."""

from dataclasses import asdict, replace
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_actor_owner as actors
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from src.semantic_methods.lotus import engine as lotus_engine
from src.semantic_methods.lotus.record_chain import LotusRecordMethod, MapMethod, RecordOccurrence, StageSpec, stage_identity
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.scheduling.core.session_contract import TaskKey
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.execution_provider.test_lotus_records_actor_owner import FakeRuntime
from tests.execution_provider.test_lotus_records_async_actor import CPUProcess


class SoftwareModel(actors._ModelActor):
    """Same trusted model hook, with FakeRuntime's non-model engine."""
    def __init__(self, engine, generation, batch_rows, limits):
        super().__init__(engine, generation, batch_rows)
        self.limits = limits


class SoftwareOwner(AsyncRecordsActorOwner):
    def _model_actor_type(self):
        return SoftwareModel


class PreparedOwnerTests(unittest.TestCase):
    def setUp(self):
        self.spec = EngineSpec("/tmp/prepared-owner-cpu-fixture", sys.executable, sys.executable)
        self.generation = GenerationSpec(max_tokens=32, stop=())
        self.resources = RecordsResources(cpu_workers=2, prepare_batch_rows=1, model_batch_rows=1,
                                          max_active_batches=2)

    def make(self, cls=SoftwareOwner, **options):
        return cls.for_prepared(self.spec, self.generation, self.resources, **options)

    def test_factory_is_explicit_and_default_None_is_still_invalid(self):
        with patch.object(FilterMethod, "from_dict", side_effect=AssertionError("no dummy Filter")):
            owner = self.make(AsyncRecordsActorOwner)
            self.assertIs(type(owner), AsyncRecordsActorOwner)
            self.assertIsNone(owner.method)
            self.assertEqual(owner.generation, self.generation)
            self.assertIsNone(owner.model_actor)
            self.assertEqual(owner.prepare_actors, [])
            self.assertEqual(owner.execution_metadata()["preparation_scope"], "caller_prepared_messages")
            self.assertEqual(owner.close()["status"], "closed")
        with self.assertRaises(TypeError):
            AsyncRecordsActorOwner(None, self.spec, self.generation, self.resources)

    def test_start_once_same_UID_no_Filter_binding_or_prepare_allocation(self):
        with FakeRuntime() as runtime:
            owner = self.make()
            with patch.object(lotus_engine, "bind_filter_method", side_effect=AssertionError("no record binding")), \
                 patch.object(lotus_engine, "validate_generation", side_effect=AssertionError("no Filter generation check")):
                ready = owner.start(time.monotonic() + 3)
                self.assertIs(owner.start(time.monotonic() + 3), ready)
            self.assertEqual(len(runtime.engines), 1)
            self.assertEqual(len(owner.prepare_actors), 0)
            self.assertEqual(runtime.formatters, [])
            allocations = [x for x in runtime.events if x[0] == "actor"]
            self.assertEqual(len(allocations), 1)
            self.assertEqual(allocations[0][1], "SoftwareModel")
            settings = allocations[0][2]
            self.assertEqual((settings["num_cpus"], settings["num_gpus"]), (1, 1))
            self.assertEqual(settings["max_concurrency"], self.resources.max_active_batches + 2)
            self.assertEqual((settings["max_restarts"], settings["max_task_retries"]), (0, 0))
            model = owner.model_actor
            first = owner.close()
            second = owner.close()
            self.assertEqual((first["status"], second["status"]), ("closed", "closed"))
            self.assertIs(owner.model_actor, model)
            self.assertEqual(runtime.engines[0].closes, 1)
            self.assertEqual(sum(x[0] == "kill" for x in runtime.events), 1)
            self.assertIsNone(second["gpu_exit_confirmed"])

    def test_default_Filter_still_allocates_configured_preparation_and_same_metadata(self):
        with FakeRuntime() as runtime:
            method = FilterMethod("openai/fixture", "Keep {reviewText}")
            owner = SoftwareOwner(method, self.spec, GenerationSpec(), self.resources)
            self.assertEqual(set(owner.execution_metadata()),
                             {"model_batch_mode", "native_engine_abort_supported", "async_limits"})
            owner.start(time.monotonic() + 3)
            self.assertEqual(len(owner.prepare_actors), self.resources.cpu_workers)
            self.assertEqual(len(runtime.formatters), self.resources.cpu_workers)
            self.assertEqual(len(runtime.engines), 1)
            owner.close()

    def test_preflight_and_capacity_failures_do_not_allocate_model(self):
        for failure in ("driver", "ray-version", "ray-uninitialized"):
            with self.subTest(failure=failure), FakeRuntime() as runtime:
                owner = self.make()
                if failure == "driver":
                    runtime.stack.enter_context(patch.object(EngineSpec, "validate_driver_environment", side_effect=ValueError("driver")))
                elif failure == "ray-version":
                    runtime.ray.__version__ = "unsupported"
                else:
                    runtime.ray.is_initialized = lambda: False
                with self.assertRaises((ValueError, RuntimeError)):
                    owner.start(time.monotonic() + 3)
                self.assertEqual(runtime.events, [])
        with self.assertRaisesRegex(ValueError, "capacity"):
            self.make(limits=AsyncEngineLimits(max_inflight=1))
        with self.assertRaisesRegex(ValueError, "model_batch_rows"):
            AsyncRecordsActorOwner.for_prepared(self.spec, self.generation,
                replace(self.resources, model_batch_rows=2))

    def test_cancelled_or_invalid_start_allocates_nothing(self):
        with FakeRuntime() as runtime:
            for deadline in (True, 0, -1, float("inf"), float("nan")):
                with self.subTest(deadline=deadline), self.assertRaises(ValueError):
                    self.make().start(deadline)
            with self.assertRaises(InterruptedError):
                self.make().start(time.monotonic() + 3, lambda: True)
            with self.assertRaises(TimeoutError):
                self.make().start(time.monotonic() - 1)
            self.assertEqual(runtime.events, [])

    def test_ready_failure_retains_same_actor_and_does_not_restart(self):
        with FakeRuntime(ready_error=RuntimeError("fixture startup failed")) as runtime:
            owner = self.make()
            with self.assertRaisesRegex(RuntimeError, "fixture startup failed"):
                owner.start(time.monotonic() + 3)
            model = owner.model_actor
            self.assertIsNotNone(model)
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                owner.start(time.monotonic() + 3)
            self.assertEqual(len(runtime.engines), 1)
            report = owner.close()
            self.assertIs(owner.model_actor, model)
            self.assertTrue(report["model_actor_handle_retained"])
            self.assertIsNone(report["gpu_exit_confirmed"])
            self.assertEqual(runtime.engines[0].closes, 1)

    def test_unconfirmed_model_construction_keeps_responsibility_without_kill(self):
        with FakeRuntime(constructor_error=RuntimeError("fixture allocated then failed")) as runtime:
            owner = self.make()
            with self.assertRaisesRegex(RuntimeError, "allocated then failed"):
                owner.start(time.monotonic() + 3)
            model = owner.model_actor
            for _ in range(2):
                report = owner.close()
                self.assertEqual(report["status"], "unknown")
                self.assertIs(owner.model_actor, model)
                self.assertFalse(report["actor_kill_requests_confirmed"])
                self.assertIsNone(report["gpu_exit_confirmed"])
            self.assertFalse(any(x[0] == "kill" and x[1] is model for x in runtime.events))
            self.assertEqual(len(runtime.engines), 1)
            with self.assertRaisesRegex(RuntimeError, "unconfirmed|closing"):
                owner.start(time.monotonic() + 3)

    def test_unknown_close_uses_same_model_and_only_retries_cleanup(self):
        reports = [dict(status="unknown", engine_api_shutdown_confirmed=False, gpu_exit_confirmed=None),
                   dict(status="closed", engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None)]
        with FakeRuntime(close_reports=reports) as runtime:
            owner = self.make()
            owner.start(time.monotonic() + 3)
            model = owner.model_actor
            self.assertEqual(owner.close()["status"], "unknown")
            self.assertIs(owner.model_actor, model)
            self.assertFalse(any(x[0] == "kill" for x in runtime.events))
            with self.assertRaisesRegex(RuntimeError, "closing"):
                owner.start(time.monotonic() + 3)
            self.assertEqual(owner.close()["status"], "closed")
            self.assertEqual(len(runtime.engines), 1)
            self.assertEqual(runtime.engines[0].closes, 2)

    def test_close_timeout_retains_the_original_RPC_until_it_finishes(self):
        with FakeRuntime() as runtime:
            owner = self.make()
            owner.start(time.monotonic() + 3)
            owner.close_deadline = time.monotonic() - 1
            self.assertEqual(owner.close()["status"], "unknown")
            pending = owner._model_close_ref
            self.assertIsNotNone(pending)
            self.assertFalse(pending.resolved)
            owner.close_deadline = time.monotonic() + 3
            self.assertEqual(owner.close()["status"], "closed")
            self.assertTrue(pending.resolved)
            self.assertEqual(sum(x[0] == "submit" and x[1].endswith(".close") for x in runtime.events), 1)
            self.assertEqual(runtime.engines[0].closes, 1)

    def test_prepared_factory_rejects_record_protocol_instead_of_falling_back(self):
        owner = self.make(AsyncRecordsActorOwner)
        with self.assertRaisesRegex(ValueError, "submit_prepared_core"):
            owner.submit_core([], time.monotonic() + 3, 65536, TaskKey(1, 0))
        self.assertIsNone(owner.model_actor)
        owner.close()

    def test_real_CPU_IPC_Map_requests_keep_one_model_UID_across_stage_options(self):
        import lotus
        with lotus.settings.context(enable_cache=False):
            stages = [StageSpec("compose-" + str(i), MapMethod("openai/cpu-fixture", "Rewrite {body}", "derived"),
                GenerationSpec(max_tokens=17+i, stop=())).bind() for i in range(2)]
            rows = [LotusRecordMethod(stage).prepare_rows([
                RecordOccurrence("occ-"+str(i), i, 7, {"body": "plain source text"})], query_id="prepared-query")[0]
                for i, stage in enumerate(stages)]
        with FakeRuntime() as runtime, patch.object(AsyncCoreModelActor, "_process_type", return_value=CPUProcess):
            owner = AsyncRecordsActorOwner.for_prepared(self.spec, stages[0].generation, self.resources)
            try:
                ready = owner.start(time.monotonic() + 8)
                model = owner.model_actor
                self.assertIs(type(model.value), AsyncCoreModelActor)
                process = model.value.engine.process
                for i, (row, stage) in enumerate(zip(rows, stages, strict=True)):
                    key = TaskKey(7, i)
                    ref = owner.submit_prepared_core([row], time.monotonic()+5, 65536, key, asdict(stage.generation))
                    result = json.loads(runtime.ray.get(ref))
                    self.assertEqual(result["status"], "succeeded")
                    self.assertEqual(result["engine_uid"], ready["engine_uid"])
                    self.assertEqual(result["call_id"], row["call_id"])
                    self.assertTrue(runtime.ray.get(owner.acknowledge_core(key)))
                self.assertIs(owner.start(time.monotonic()+3), ready)
                self.assertIs(owner.model_actor, model)
                self.assertEqual(model.value.engine._sequence, 2)
                self.assertEqual(owner.prepare_actors, [])
                self.assertEqual(len([x for x in runtime.events if x[0] == "actor"]), 1)
                self.assertEqual(owner.close()["status"], "closed")
                self.assertIsNotNone(process.poll())
            finally:
                owner.close_deadline = time.monotonic()+3
                owner.close()

    def test_real_CPU_unknown_receipt_survives_ack_refusal_and_owner_close(self):
        record = RecordOccurrence("unknown-occ", 0, 7, {"body": "unused fixture value"})
        row = dict(stage_identity("prepared-unknown", record, "compose"),
                   messages_json=json.dumps([dict(role="user", content="unknown")]))
        with FakeRuntime() as runtime, patch.object(AsyncCoreModelActor, "_process_type", return_value=CPUProcess):
            owner = self.make(AsyncRecordsActorOwner)
            try:
                owner.start(time.monotonic()+8)
                model = owner.model_actor
                process = model.value.engine.process
                key = TaskKey(8, 0)
                result = json.loads(runtime.ray.get(owner.submit_prepared_core(
                    [row], time.monotonic()+5, 65536, key, asdict(self.generation))))
                self.assertEqual(result["status"], "unknown")
                self.assertEqual(result["total_tokens"], 12)
                self.assertFalse(runtime.ray.get(owner.acknowledge_core(key)))
                report = owner.close()
                receipts = report["unconsumed_core_results"]
                match = next(x for x in receipts if x["request_key"] == [8, 0])
                self.assertEqual(match["result"]["status"], "unknown")
                self.assertEqual(match["result"]["total_tokens"], 12)
                self.assertIs(owner.model_actor, model)
                self.assertIsNone(report["gpu_exit_confirmed"])
                self.assertIsNotNone(process.poll())
            finally:
                owner.close_deadline = time.monotonic()+3
                owner.close()


if __name__ == "__main__":
    unittest.main()
