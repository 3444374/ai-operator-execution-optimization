"""Implicit totals fit declared items; explicit smaller IPC values stay intact."""
from dataclasses import asdict, replace
import sys
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity
from src.execution_provider.adapters.records_profile import resolve_records_runtime
from src.experiments.movie_records_config import records_runtime
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec

MIB = 1024**2


class ImplicitIPCTotalTests(unittest.TestCase):
    def setUp(self):
        self.engine = EngineSpec('/tmp/implicit-total-model', sys.executable, sys.executable)
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1)

    def resolve(self, capacity=None, overrides=None, *, rolling=False, requests=2):
        return resolve_records_runtime(self.engine,
            replace(self.resources, max_active_batches=requests), dict(mode='async'),
            capacity=capacity, async_limits=overrides,
            prepared_batching=object() if rolling else None, rolling_refill=rolling)

    def test_omitted_totals_fit_large_Core_and_IPC_items_for_both_paths(self):
        cases = (
            (CoreRecordsCapacity(item_input_bytes=32*MIB), None, 'input'),
            (CoreRecordsCapacity(item_result_bytes=32*MIB), None, 'result'),
            (CoreRecordsCapacity(item_input_bytes=32*MIB), {}, 'input'),
            (CoreRecordsCapacity(item_result_bytes=32*MIB), dict(feedback_interval_s=.1), 'result'),
            (CoreRecordsCapacity(), dict(item_input_bytes=32*MIB), 'input'),
            (CoreRecordsCapacity(), dict(item_result_bytes=32*MIB), 'result'),
        )
        for capacity, overrides, field in cases:
            original = None if overrides is None else dict(overrides)
            for rolling in (False, True):
                for requests in (2, 11):
                    with self.subTest(field=field, capacity=capacity, overrides=overrides,
                                      rolling=rolling, requests=requests):
                        config = self.resolve(capacity, overrides, rolling=rolling, requests=requests)
                        self.assertEqual(getattr(config.async_limits, field+'_bytes'), 32*MIB)
                        self.assertEqual(config.async_limits.max_inflight, requests)
                        self.assertEqual(config.async_limits.result_slots, AsyncEngineLimits().result_slots)
                        self.assertEqual(overrides, original)

    def test_explicit_totals_and_typed_IPC_are_never_enlarged(self):
        for field in ('input', 'result'):
            with self.subTest(field=field):
                capacity = CoreRecordsCapacity(**{'item_'+field+'_bytes':32*MIB})
                explicit = {field+'_bytes':16*MIB, 'item_'+field+'_bytes':2*MIB}
                actual = self.resolve(capacity, explicit).async_limits
                self.assertEqual(getattr(actual, field+'_bytes'), 16*MIB)
                self.assertEqual(getattr(actual, 'item_'+field+'_bytes'), 2*MIB)
                with self.assertRaises(ValueError):
                    self.resolve(capacity, {field+'_bytes':16*MIB})
        capacity = CoreRecordsCapacity(item_input_bytes=32*MIB, item_result_bytes=32*MIB)
        explicit = dict(input_bytes=4096, item_input_bytes=4096,
                        result_bytes=4096, item_result_bytes=4096)
        self.assertTrue(all(getattr(self.resolve(capacity, explicit).async_limits,k)==v
                            for k,v in explicit.items()))
        typed = AsyncEngineLimits(**explicit)
        self.assertIs(self.resolve(capacity, typed).async_limits, typed)

    def test_invalid_item_types_still_fail_before_actor_allocation(self):
        for field in ('input', 'result'):
            for value in (True, False, 0, -1, '33554432', 1.5):
                with self.subTest(field=field, value=value), \
                        patch('src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as allocate:
                    with self.assertRaises(ValueError):
                        self.resolve(overrides={'item_'+field+'_bytes':value})
                    allocate.assert_not_called()

    def test_ordinary_and_session_helpers_use_the_same_implicit_totals(self):
        for field in ('input', 'result'):
            for key, backend in (('core_profile','semloom-local-model-core-async'),
                                 ('profile','semloom-core')):
                with self.subTest(field=field, backend=backend):
                    config = dict(backend=backend, engine=asdict(self.engine),
                        resources=asdict(self.resources),
                        core_capacity={'item_'+field+'_bytes':32*MIB},
                        **{key:dict(mode='async')})
                    self.assertEqual(getattr(records_runtime(config).async_limits,field+'_bytes'),32*MIB)


if __name__ == '__main__':
    unittest.main()
