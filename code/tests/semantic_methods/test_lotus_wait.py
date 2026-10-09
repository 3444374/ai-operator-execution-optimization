"""The real LOTUS batch/Core wait must remain valid while a model result is pending."""

from importlib.util import find_spec
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.semantic_methods.lotus.batch import LotusBatchExecutor, lotus_executor
from src.semantic_methods.lotus.sdk import lotus_response
from tests.execution_provider.test_native_tasks import fixture
from tests.semantic_methods.test_lotus_adapter import completion, config, encode, model


@unittest.skipUnless(find_spec("lotus"), "pinned LOTUS is unavailable")
class LotusWaitTests(unittest.TestCase):
    def test_idle_batch_does_not_invalidate_its_own_wait(self):
        import lotus
        from src.execution_provider.adapters.native_tasks import NativeTaskSession
        execution, backend, _clock = fixture(held_tasks=1, offer_tasks=1,
            input_bytes=8192, result_bytes=8192, item_input_bytes=8192,
            item_result_bytes=8192, active_requests=1, active_work=1)
        lm = model("http://127.0.0.1:1/v1")
        selected = LotusBatchExecutor(execution, config("http://127.0.0.1:1/v1"),
                                      query_id="wait-regression", operator_id="map")
        waits = []

        def finish_after_wait(flow, progress):
            if progress.deliveries:
                # Returning actual leases should wake the owner to refill its storage.
                return
            self.assertFalse(progress.has_immediate_work)
            self.assertEqual(progress.generation, execution.engine.wake.generation,
                             "an idle LOTUS poll changed its own wake generation")
            self.assertEqual(progress.blocked_reason, "WAIT_BACKEND")
            waits.append(progress)
            for key, (_handle, task) in tuple(backend.pending.items()):
                import json
                body = json.loads(task.task.payload)
                backend.complete(key, encode_full_response(FullModelResponse(200, (), encode(completion(body)))))

        try:
            with lotus.settings.context(enable_cache=False), lotus_executor(lm, selected), \
                 patch.object(NativeTaskSession, "wait", finish_after_wait):
                value = lm([[{"role": "user", "content": "one pending call"}]], show_progress_bar=False)
            self.assertEqual(len(value.outputs), 1)
            self.assertEqual(len(waits), 1)
            self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)
            self.assertEqual(execution.engine.jobs.jobs, {})
        finally:
            for key in tuple(backend.pending):
                backend.complete(key)
            execution.engine.advance()

    def test_completed_response_stays_charged_through_observation_and_sdk_conversion(self):
        import lotus
        from src.execution_provider.adapters.native_tasks import NativeTaskSession
        execution, backend, _clock = fixture(held_tasks=1, offer_tasks=1,
            input_bytes=8192, result_bytes=8192, item_input_bytes=8192,
            item_result_bytes=8192, active_requests=1, active_work=1)
        stages = []

        def inspect_owner(name):
            usage = execution.engine.capacity.usage()
            self.assertEqual(usage.held_tasks, 1)
            self.assertEqual(usage.result_bytes, 8192)
            self.assertEqual(usage.active_requests, 0)
            self.assertEqual(next(iter(execution.engine.capacity.records.values())).phase, "LEASED")
            stages.append(name)

        def observe(_index, _full):
            inspect_owner("observation")

        def convert(full):
            inspect_owner("conversion")
            return lotus_response(full)

        def complete(flow, progress):
            if progress.deliveries:
                return
            self.assertEqual(progress.generation, execution.engine.wake.generation)
            for key, (_handle, task) in tuple(backend.pending.items()):
                import json
                body = json.loads(task.task.payload)
                backend.complete(key, encode_full_response(FullModelResponse(200, (), encode(completion(body)))))

        lm = model("http://127.0.0.1:1/v1")
        selected = LotusBatchExecutor(execution, config("http://127.0.0.1:1/v1"),
                                      query_id="response-owner", operator_id="map", on_response=observe)
        try:
            with lotus.settings.context(enable_cache=False), lotus_executor(lm, selected), \
                 patch.object(NativeTaskSession, "wait", complete), \
                 patch("src.semantic_methods.lotus.batch.lotus_response", convert):
                value = lm([[{"role": "user", "content": "bounded result"}]], show_progress_bar=False)
            self.assertEqual(len(value.outputs), 1)
            self.assertEqual(stages, ["observation", "conversion"])
            self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)
            self.assertEqual(execution.engine.jobs.jobs, {})
        finally:
            for key in tuple(backend.pending):
                backend.complete(key)
            execution.engine.advance()


if __name__ == "__main__":
    unittest.main()
