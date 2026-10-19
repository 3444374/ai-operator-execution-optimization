"""Production Core + actual CPU IPC with explicit Ray/Daft/formatter fixtures."""

from concurrent.futures import ThreadPoolExecutor
import json
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
        method, generation = FilterMethod("fixture"), GenerationSpec()
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


if __name__ == "__main__":
    unittest.main()
