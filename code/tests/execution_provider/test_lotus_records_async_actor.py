"""Actual CPU child/socket IPC and threaded actor object; no Ray/model runtime."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_async_actor import (
    AsyncCoreModelActor, AsyncRecordsActorOwner,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources, identity
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class CPUProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        fixture = Path(__file__).parents[1] / "serving/async_engine_fixture.py"
        return [sys.executable, str(fixture), *map(str, descriptors)]


class CPUActor(AsyncCoreModelActor):
    def _process_type(self):
        return CPUProcess


def prepared(index=0, text="fast", query="same-query"):
    return dict(identity(query, "row-" + str(index), index),
                messages_json=json.dumps([dict(role="user", content=text)]))


class AsyncActorTests(unittest.TestCase):
    def setUp(self):
        self.patches = patch.object(EngineSpec, "validate_driver_environment", return_value={})
        self.patches.start()
        self.addCleanup(self.patches.stop)
        self.actors = []
        self.addCleanup(self.close_all)
        self.spec = EngineSpec("/tmp/cpu-model", sys.executable, "/tmp/driver-python")
        self.limits = AsyncEngineLimits(max_inflight=2, result_slots=2, command_slots=2,
            input_bytes=8192, item_input_bytes=4096, result_bytes=8192, item_result_bytes=4096)

    def close_all(self):
        for actor in self.actors:
            actor.close(2)

    def actor(self):
        actor = CPUActor(asdict(self.spec), asdict(GenerationSpec()), 1, asdict(self.limits))
        self.actors.append(actor)
        actor.ready(time.monotonic() + 5)
        return actor

    def until(self, actor, predicate):
        deadline = time.monotonic() + 3
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("CPU actor did not progress: " + str(actor.snapshot()))
            time.sleep(0.005)

    def submit(self, pool, actor, index=0, text="fast", *, session=0, query="same-query"):
        return pool.submit(actor.run_core, [prepared(index, text, query)],
                           time.monotonic() + 5, 4096, (session, index))

    def test_late_second_input_finishes_while_first_still_active(self):
        actor = self.actor()
        with ThreadPoolExecutor(max_workers=4) as pool:
            first = self.submit(pool, actor, 0, "hold")
            try:
                self.until(actor, lambda: actor.engine.snapshot()["accepted_requests"] == 1)
                second = self.submit(pool, actor, 1)
                result = json.loads(second.result(3))
                self.assertEqual(result["row_id"], "row-1")
                self.assertEqual(result["prompt_tokens"], 11)
                self.assertFalse(first.done())
                self.assertTrue(actor.acknowledge_core((0, 1)))
            finally:
                actor.abort_core((0, 0))
            late = json.loads(first.result(3))
            self.assertEqual(late["row_id"], "row-0")
            self.assertTrue(late["engine_terminal_confirmed"])
            self.assertTrue(actor.acknowledge_core((0, 0)))
            self.assertEqual(actor.snapshot()["held_requests"], 0)

    def test_slow_result_consumer_holds_actor_capacity_until_transport_ack(self):
        actor = self.actor()
        with ThreadPoolExecutor(max_workers=4) as pool:
            one = self.submit(pool, actor, 0)
            two = self.submit(pool, actor, 1)
            one.result(3)
            two.result(3)
            self.assertEqual(actor.snapshot()["held_requests"], 2)
            waiting = self.submit(pool, actor, 2)
            try:
                time.sleep(0.03)
                self.assertFalse(waiting.done())
                self.assertEqual(actor.engine.snapshot()["held_requests"], 0)
                self.assertTrue(actor.acknowledge_core((0, 0)))
                value = json.loads(waiting.result(3))
                self.assertEqual(value["row_id"], "row-2")
                self.assertTrue(actor.acknowledge_core((0, 1)))
                self.assertTrue(actor.acknowledge_core((0, 2)))
            finally:
                actor.close(2)

    def test_cancel_before_prepared_call_arrives_is_presend_and_bounded(self):
        actor = self.actor()
        self.assertTrue(actor.abort_core((0, 0))["requested"])
        self.assertTrue(actor.abort_core((0, 1))["requested"])
        self.assertFalse(actor.abort_core((0, 2))["requested"])
        result = json.loads(actor.run_core([prepared()], time.monotonic() + 3, 4096, (0, 0)))
        self.assertEqual(result["error_type"], "CancelledBeforeSubmission")
        self.assertFalse(result["submission_confirmed"])
        self.assertTrue(result["engine_terminal_confirmed"])
        self.assertEqual(actor.engine._sequence, 0)
        self.assertTrue(actor.acknowledge_core((0, 0)))

    def test_old_abort_does_not_cancel_reused_query_and_occurrence_in_next_session(self):
        actor = self.actor()
        with ThreadPoolExecutor(max_workers=4) as pool:
            old = json.loads(self.submit(pool, actor, 0).result(3))
            actor.acknowledge_core((0, 0))
            current = self.submit(pool, actor, 0, "hold", session=1)
            try:
                self.until(actor, lambda: actor.engine.snapshot()["accepted_requests"] == 1)
                self.assertFalse(actor.abort_core((0, 0))["requested"])
                time.sleep(0.02)
                self.assertFalse(current.done())
            finally:
                actor.abort_core((1, 0))
            result = json.loads(current.result(3))
            self.assertEqual(result["call_id"], old["call_id"])
            self.assertNotEqual(result["engine_request_id"], old["engine_request_id"])
            self.assertTrue(actor.acknowledge_core((1, 0)))

    def test_unknown_keeps_both_ipc_and_actor_reservations_after_ack(self):
        actor = self.actor()
        with ThreadPoolExecutor(max_workers=4) as pool:
            future = self.submit(pool, actor, 0, "hold-unknown")
            self.until(actor, lambda: actor.engine.snapshot()["accepted_requests"] == 1)
            self.assertFalse(actor.abort_core((0, 0))["engine_terminal_confirmed"])
            result = json.loads(future.result(3))
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["prompt_tokens"], 11)
        self.assertFalse(actor.acknowledge_core((0, 0)))
        snapshot = actor.snapshot()
        self.assertEqual(snapshot["held_requests"], 1)
        self.assertEqual(snapshot["ipc"]["held_requests"], 1)
        report = actor.close(2)
        self.assertEqual(report["status"], "unknown")
        self.assertFalse(report["engine_api_shutdown_confirmed"])
        self.assertEqual(report["unconsumed_call_results"][0]["prompt_tokens"], 11)

    def test_close_control_with_full_results_preserves_usage_and_direct_child_exit(self):
        actor = self.actor()
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.submit(pool, actor, 0).result(3)
            self.submit(pool, actor, 1).result(3)
        report = actor.close(2)
        self.assertEqual(report["status"], "closed")
        self.assertTrue(report["ipc_close"]["process_exit_confirmed"])
        self.assertEqual(sum(row["prompt_tokens"] for row in report["unconsumed_call_results"]), 22)
        self.assertIsNone(report["gpu_exit_confirmed"])
        self.assertIsNone(report["descendants_exit_confirmed"])

    def test_child_death_preserves_known_prefix_and_unknown_pending_call(self):
        actor = self.actor()
        with ThreadPoolExecutor(max_workers=4) as pool:
            known = json.loads(self.submit(pool, actor, 0).result(3))
            lost = json.loads(self.submit(pool, actor, 1, "die").result(3))
        self.assertEqual(known["prompt_tokens"], 11)
        self.assertEqual(lost["status"], "unknown")
        self.assertIsNone(lost["prompt_tokens"])
        report = actor.close(2)
        self.assertEqual(len(report["unconsumed_call_results"]), 2)
        self.assertEqual(sum(row["prompt_tokens"] or 0 for row in report["unconsumed_call_results"]), 11)

    def test_startup_exception_retains_same_cleanup_handle(self):
        actor = CPUActor(asdict(self.spec), asdict(GenerationSpec()), 1, asdict(self.limits))
        self.actors.append(actor)
        made = []

        def failed(*args, **kwargs):
            engine = CPUProcess(*args, **kwargs)
            made.append(engine)
            error = RuntimeError("fixture startup reported failure")
            error.async_engine_process = engine
            raise error

        with patch.object(actor, "_process_type", return_value=failed):
            with self.assertRaisesRegex(RuntimeError, "fixture startup"):
                actor.ready(time.monotonic() + 5)
        self.assertIs(actor.engine, made[0])
        self.assertTrue(actor.close(2)["ipc_close"]["process_exit_confirmed"])

    def test_owner_hooks_preserve_gpu_reservation_driver_isolation_and_control_capacity(self):
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=2)
        owner = AsyncRecordsActorOwner(FilterMethod("fixture"), self.spec, GenerationSpec(),
                                        resources, limits=self.limits)
        options = owner._model_actor_options()
        self.assertEqual(options["num_gpus"], 1)
        self.assertGreaterEqual(options["max_concurrency"], self.limits.max_inflight + 2)
        self.assertIn(self.spec.driver_python, options["runtime_env"]["py_executable"])
        self.assertEqual(options["max_task_retries"], 0)
        self.assertEqual(len(owner._model_actor_arguments()), 4)
        self.assertTrue(owner.execution_metadata()["native_engine_abort_supported"])


if __name__ == "__main__":
    unittest.main()
