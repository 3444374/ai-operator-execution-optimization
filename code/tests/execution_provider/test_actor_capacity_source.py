"""Direct async owners must use the same declared buffer sizes as the factory."""
from dataclasses import asdict, replace
import json
import sys
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.execution_provider.adapters.records_capacity import (
    CoreRecordsCapacity, RecordsCapacityBudget, resolve_records_async_limits,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from tests.execution_provider.test_lotus_records_async_actor import CPUActor, core_result, prepared


class ActorCapacitySourceTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(EngineSpec, 'validate_driver_environment', return_value={})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.spec = EngineSpec('/tmp/cpu-model', sys.executable, sys.executable)
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=3)

    def owner(self, capacity=None, limits=None, resources=None):
        return AsyncRecordsActorOwner(FilterMethod('openai/cpu-fixture', 'Accept {reviewText}?'),
            self.spec, GenerationSpec(), self.resources if resources is None else resources,
            capacity=capacity, limits=limits)

    def test_direct_owner_uses_custom_items_from_the_shared_resolver(self):
        for inputs, results in ((4096, 8192), (3 * 1024**2, 5 * 1024**2)):
            with self.subTest(input_bytes=inputs, result_bytes=results):
                capacity = CoreRecordsCapacity(item_input_bytes=inputs, item_result_bytes=results)
                owner = self.owner(capacity)
                expected = resolve_records_async_limits(self.resources, capacity)
                self.assertEqual(asdict(owner.limits), asdict(expected))

    def _run_cpu(self, capacity, text):
        owner = self.owner(capacity)
        actor = CPUActor(asdict(self.spec), asdict(GenerationSpec()), 1, asdict(owner.limits))
        try:
            ready = actor.ready(time.monotonic() + 5)
            result = json.loads(core_result(actor, [prepared(text=text)], time.monotonic() + 5,
                                               capacity.item_result_bytes, (0, 0)))
            return ready, result, actor.engine._sequence
        finally:
            self.assertEqual(actor.close(2)['status'], 'closed')

    def test_actual_CPU_IPC_rejects_a_prompt_above_the_declared_small_item(self):
        capacity = CoreRecordsCapacity(item_input_bytes=4096, item_result_bytes=8192)
        ready, result, requests = self._run_cpu(capacity, 'fast' + 'x' * 6000)
        self.assertEqual(result['status'], 'failed')
        self.assertFalse(result['submission_confirmed'])
        self.assertEqual(requests, 0)
        self.assertEqual(ready['limits']['item_input_bytes'], capacity.item_input_bytes)

    def test_actual_CPU_IPC_accepts_a_prompt_above_the_old_two_MiB_default(self):
        capacity = CoreRecordsCapacity(item_input_bytes=3 * 1024**2, item_result_bytes=8192)
        ready, result, requests = self._run_cpu(capacity, 'fast' + 'x' * (2 * 1024**2 + 4096))
        self.assertEqual(result['status'], 'succeeded')
        self.assertTrue(result['submission_confirmed'])
        self.assertEqual(requests, 1)
        self.assertEqual(ready['limits']['item_input_bytes'], capacity.item_input_bytes)

    def test_legacy_omission_keeps_the_original_IPC_values(self):
        for requests in (1, 2, 8, 17):
            with self.subTest(requests=requests):
                resources = replace(self.resources, max_active_batches=requests)
                expected = AsyncEngineLimits(max_inflight=requests, result_slots=requests,
                    command_slots=requests, input_bytes=requests * 2 * 1024**2,
                    result_bytes=requests * 2 * 1024**2, control_slots=max(16, requests * 2))
                self.assertEqual(asdict(self.owner(resources=resources).limits), asdict(expected))

    def test_explicit_IPC_values_keep_the_same_object_and_values(self):
        capacity = CoreRecordsCapacity(item_input_bytes=4096, item_result_bytes=8192)
        limits = AsyncEngineLimits(max_inflight=3, result_slots=4, command_slots=5,
            input_bytes=10000, item_input_bytes=5000, result_bytes=40000, item_result_bytes=10000,
            control_slots=7, control_frame_bytes=8192, feedback_interval_s=.2)
        self.assertIs(self.owner(capacity, limits).limits, limits)

    def test_complete_independent_budget_still_requires_explicit_IPC(self):
        capacity = CoreRecordsCapacity(item_input_bytes=4096, item_result_bytes=8192,
            budget=RecordsCapacityBudget(5, 20000, 50000, 3, 4, 500000))
        with self.assertRaisesRegex(ValueError, 'explicit finite IPC'):
            self.owner(capacity)
        limits = AsyncEngineLimits(max_inflight=2, result_slots=4, command_slots=2,
            input_bytes=10000, item_input_bytes=5000, result_bytes=40000, item_result_bytes=10000)
        self.assertIs(self.owner(capacity, limits).limits, limits)
