"""Production Core + actual CPU IPC with explicit Ray/Daft/formatter fixtures."""

from concurrent.futures import ThreadPoolExecutor
import json
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.lotus_records_async_actor import (
    AsyncCoreModelActor, AsyncRecordsActorOwner,
)
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import TaskKey
from src.semantic_methods.lotus import engine as lotus_engine
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS
from tests.execution_provider.test_lotus_records_core import Runtime, Source
from tests.execution_provider.test_lotus_records_async_actor import CPUProcess


class ThreadedRuntime(Runtime):
    def __init__(self, rows):
        super().__init__(rows)
        self.async_actors = []
        original = self.ray.remote
        runtime = self

        def remote(cls):
            actor_class = original(cls)
            if cls is not AsyncCoreModelActor:
                return actor_class
            options = {}

            def configure(**kwargs):
                options.update(kwargs)
                actor_class.options(**kwargs)
                return SimpleNamespace(remote=allocate)

            def allocate(*args):
                actor = actor_class.remote(*args)
                actor.pool = ThreadPoolExecutor(max_workers=options["max_concurrency"],
                                                thread_name_prefix="cpu-async-actor")
                runtime.pools.append(actor.pool)
                runtime.async_actors.append(actor)
                return actor

            return SimpleNamespace(options=configure)

        self.ray.remote = remote

        class Formatter:
            def __init__(self, method):
                pass

            def prepare_rows(self, rows):
                return [dict({name: row[name] for name in IDENTITY_FIELDS},
                    messages_json=json.dumps([dict(role="user", content=row["reviewText"])]))
                    for row in rows]

        self.stack.enter_context(patch.object(lotus_engine, "LotusFilterMethod", Formatter))
        self.stack.enter_context(patch.object(AsyncCoreModelActor, "_process_type", return_value=CPUProcess))

    def __exit__(self, *args):
        for actor in self.async_actors:
            actor.instance.close(2)
        return super().__exit__(*args)


class AsyncCoreCompositionTests(unittest.TestCase):
    def rows(self, texts):
        return [dict(row_id="row-" + str(index), source_position=index,
                     reviewId="repeat-review", id="taken_3", reviewText=text)
                for index, text in enumerate(texts)]

    def owner(self):
        method, generation = FilterMethod("fixture", instruction="Keep {reviewText}"), GenerationSpec()
        spec = EngineSpec("/tmp/fixture-model", "/tmp/model-python", "/tmp/driver-python")
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=2, query_timeout_s=8, startup_timeout_s=5)
        actors = AsyncRecordsActorOwner(method, spec, generation, resources)
        return core.SemLoomRecordsCore(method, spec, generation, resources, actor_owner=actors)

    def test_core_supplies_third_call_while_first_still_borrows_and_uses_original_leases(self):
        rows = self.rows(["hold", "fast", "fast"])
        with ThreadedRuntime(rows) as runtime:
            owner = self.owner()
            try:
                stream = owner.evaluate(Source(rows), query_id="q")
                first = next(stream)
                second = next(stream)
                self.assertEqual({first["source_position"], second["source_position"]}, {1, 2})
                actor = runtime.async_actors[0].instance
                self.assertEqual(actor.engine._sequence, 3)
                self.assertEqual(actor.engine.snapshot()["active_requests"], 1)
                self.assertIsInstance(owner.core, SessionEngine)
                self.assertEqual(owner.core.capacity.usage().held_tasks, 1)
                ref = owner._actors.cancel_core(TaskKey(stream.session.session_id, 0))
                self.assertFalse(ref.future().result(2)["engine_terminal_confirmed"])
                last = list(stream)
                self.assertEqual([row["source_position"] for row in last], [0])
                self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                self.assertEqual(owner.transport.retained_refs(), 0)
                self.assertLessEqual(stream.peak_usage["held_tasks"], 2)
            finally:
                closed = owner.close(timeout=3)
            self.assertEqual(closed["model_batch_mode"], "independent_final_results")
            self.assertTrue(closed["native_engine_abort_supported"])
            self.assertIsNone(closed["gpu_exit_confirmed"])
            self.assertEqual([event[3] for event in runtime.events
                if event[0] == "submit" and event[1] == "AsyncCoreModelActor.close"], [(15.0,)])
            self.assertFalse(any(event[0] == "driver_get" and event[1].endswith(".prepare")
                                 for event in runtime.events))

    def test_core_query_cancel_retains_work_until_late_real_receipts_and_archives_usage(self):
        rows = self.rows(["hold", "hold"])
        with ThreadedRuntime(rows) as runtime:
            owner = self.owner()
            try:
                stream = owner.evaluate(Source(rows), query_id="cancel")
                stream._fill()
                deadline = time.monotonic() + 3
                while runtime.async_actors[0].instance.engine.snapshot()["accepted_requests"] < 2:
                    stream.session.advance(1)
                    if time.monotonic() >= deadline:
                        self.fail("Core did not submit both held CPU calls")
                    owner.core.wake.wait(owner.core.wake.generation, 0.005)
                self.assertEqual(owner.core.capacity.usage().active_requests, 2)
                stream.close()
                while owner.core.capacity.usage().held_tasks:
                    owner.core.reap(owner.capacity.step_actions)
                    if time.monotonic() >= deadline:
                        self.fail("Core did not settle the actual late cancellation receipts")
                    owner.core.wake.wait(owner.core.wake.generation, 0.005)
                stream.collect_receipts()
                self.assertEqual(len(stream.unconsumed_call_results), 2)
                self.assertEqual(sum(row["prompt_tokens"] for row in stream.unconsumed_call_results), 22)
                self.assertEqual(owner.transport.retained_refs(), 0)
            finally:
                closed = owner.close(timeout=3)
            self.assertEqual(closed["status"], "closed")
            self.assertTrue(closed["transport_closed"])

    def test_next_core_session_waits_for_prior_async_ack_without_resubmitting(self):
        rows = self.rows(["fast"])
        ack_started, release_ack, next_started = (threading.Event() for _ in range(3))
        original_ack = AsyncCoreModelActor.acknowledge_core
        original_run = AsyncCoreModelActor.run_core

        def delayed_ack(actor, key):
            if key == (0, 0):
                ack_started.set()
                if not release_ack.wait(3):
                    raise TimeoutError("test did not release the owned acknowledgement")
            return original_ack(actor, key)

        def observe_next(actor, prepared, deadline, maximum, key):
            if key[0] == 1:
                next_started.set()
            return original_run(actor, prepared, deadline, maximum, key)

        with ThreadedRuntime(rows) as runtime, \
                patch.object(AsyncCoreModelActor, "acknowledge_core", delayed_ack), \
                patch.object(AsyncCoreModelActor, "run_core", observe_next), \
                ThreadPoolExecutor(max_workers=1) as pool:
            owner = self.owner()
            try:
                first = list(owner.evaluate(Source(rows), query_id="repeat-query"))
                self.assertTrue(ack_started.wait(3))
                actor = runtime.async_actors[0].instance
                self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                self.assertEqual(actor.snapshot()["held_requests"], 1)

                def release_after_next_arrives():
                    try:
                        self.assertTrue(next_started.wait(3))
                        time.sleep(0.03)
                        self.assertEqual(actor.engine._sequence, 1)
                        self.assertEqual(actor.snapshot()["held_requests"], 1)
                    finally:
                        release_ack.set()

                releasing = pool.submit(release_after_next_arrives)
                second = list(owner.evaluate(Source(rows), query_id="repeat-query"))
                releasing.result(3)
                self.assertEqual(first[0]["call_id"], second[0]["call_id"])
                self.assertEqual(second[0]["status"], "succeeded")
                self.assertEqual(actor.engine._sequence, 2)
                self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
            finally:
                release_ack.set()
                closed = owner.close(timeout=3)
            self.assertEqual(closed["status"], "closed")


if __name__ == "__main__":
    unittest.main()
