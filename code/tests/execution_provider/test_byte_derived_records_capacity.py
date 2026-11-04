"""Counts follow already-assigned payload bytes; this does not discover RAM."""
from copy import deepcopy
from dataclasses import asdict, replace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity
from src.semantic_methods.budget import MethodBudgetPool, row_reservation
from src.semantic_methods.continuation import MethodLimits
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime, Source


class ByteDerivedCapacityTests(unittest.TestCase):
    def setUp(self):
        self.base = CoreRecordsCapacity(item_input_bytes=65536, item_result_bytes=65536)
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=None, query_timeout_s=12, startup_timeout_s=8)
        self.engine = EngineSpec('/tmp/cpu-model', '/tmp/model-python', '/tmp/driver-python')

    def derive(self, results=19):
        return self.base.with_storage_bytes(input_bytes=2 * 65536,
            result_bytes=results * 65536 + 17,
            method_bytes=2 * row_reservation(self.base.method_limits(1)) + 31)

    def owner(self, capacity, **options):
        settings = dict(profile=dict(mode='async', max_inflight='auto'))
        settings.update(options)
        return core.create_records_core(FilterMethod('cpu-fixture', 'Keep {reviewText}'),
            self.engine, GenerationSpec(), self.resources, capacity=capacity,
            **settings)

    def test_assigned_bytes_are_unchanged_and_counters_follow_result_storage(self):
        before = asdict(self.base)
        for results in (1, 3, 19, 137):
            with self.subTest(results=results):
                value = self.derive(results)
                owner = self.owner(value)
                self.assertEqual(value.budget.input_bytes, 2 * 65536)
                self.assertEqual(value.budget.result_bytes, results * 65536 + 17)
                self.assertEqual(value.budget.active_requests, results)
                self.assertEqual(value.budget.held_tasks, results + 1)
                self.assertEqual(owner.resources.max_active_batches, results)
                self.assertEqual(owner._actors.limits.max_inflight, results)
                self.assertEqual(owner._actors.limits.result_slots, results + 1)
                self.assertEqual(owner._actors.limits.input_bytes, value.budget.input_bytes)
                self.assertEqual(owner._actors.limits.result_bytes, value.budget.result_bytes)
                self.assertIsNone(owner.execution_profile['declared_legacy_request_budget'])
                self.assertIsNone(owner._actors.model_actor)
                self.assertEqual(owner.engine, self.engine)
        self.assertEqual(asdict(self.base), before)
        self.assertIsNone(self.resources.max_active_batches)

    def test_actual_Method_pool_bytes_bind_before_the_derived_run_counter(self):
        value = self.derive()
        for limits in (MethodLimits(1, 1, 1, 1), MethodLimits(200, 400, 100, 1),
                       self.base.method_limits(1), self.base.method_limits(3)):
            with self.subTest(limits=limits):
                expected = value.budget.method_bytes // row_reservation(limits)
                capacity = value.method_capacity(self.resources, limits)
                pool = MethodBudgetPool(capacity)
                grant = pool.allocate(capacity)
                grant.claim()
                for _ in range(expected):
                    self.assertTrue(grant.reserve(row_reservation(limits)))
                self.assertFalse(grant.reserve(row_reservation(limits)))
                self.assertLess(grant.runs, grant.capacity.runs)
                self.assertLessEqual(grant.bytes, value.budget.method_bytes)
                for _ in range(expected):
                    grant.release(row_reservation(limits))
                grant.close()
                self.assertEqual(pool.allocated, (0, 0))

    def test_missing_noninteger_or_too_small_storage_rejects_before_owner_creation(self):
        valid = dict(input_bytes=65536, result_bytes=65536,
                     method_bytes=row_reservation(self.base.method_limits(1)))
        with patch('src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as actor:
            for name in valid:
                for bad in (None, True, 0, -1, 1.5, float('inf')):
                    with self.subTest(name=name, bad=bad), self.assertRaises(ValueError):
                        self.base.with_storage_bytes(**dict(valid, **{name: bad}))
            for name in valid:
                with self.subTest(short=name), self.assertRaises(ValueError):
                    self.base.with_storage_bytes(**dict(valid, **{name: valid[name] - 1}))
            missing = dict(valid)
            missing.pop('method_bytes')
            with self.assertRaises(TypeError):
                self.base.with_storage_bytes(**missing)
            actor.assert_not_called()

    def test_existing_explicit_budget_is_never_replaced_by_byte_derivation(self):
        value = self.derive()
        before = asdict(value)
        with self.assertRaisesRegex(ValueError, 'existing.*budget'):
            value.with_storage_bytes(input_bytes=1000000, result_bytes=1000000, method_bytes=1000000)
        self.assertEqual(asdict(value), before)
        self.assertEqual(CoreRecordsCapacity(**deepcopy(before)), value)

    def test_core_storage_and_method_counts_remain_independent_of_request_count(self):
        value = self.derive()
        limits = value.limits(self.resources)
        self.assertEqual(limits.active_requests, 19)
        self.assertEqual(limits.input_bytes, 2 * 65536)
        self.assertEqual(limits.result_bytes, 19 * 65536 + 17)
        self.assertGreater(value.budget.method_runs, limits.active_requests)
        narrower = replace(self.resources, max_active_batches=1)
        self.assertEqual(value.limits(narrower), limits)

    def test_modern_feedback_starts_at_one_with_byte_derived_owner_permission(self):
        owner = self.owner(self.derive(), profile=dict(mode='async', admission=dict(kind='resource_feedback')))
        self.assertEqual(owner.execution_profile['max_inflight'], 1)
        self.assertEqual(owner.execution_profile['owner_max_inflight'], 19)
        self.assertEqual(owner._actors.limits.max_inflight, 19)
        self.assertIsNone(owner.execution_profile['declared_legacy_request_budget'])
        self.assertIsNone(owner._actors.model_actor)

    def test_rolling_token_factory_receives_the_same_finite_counts_and_bytes(self):
        from src.semantic_methods.lotus.prepared_batching import PreparedBatching
        value = self.derive()
        owner = self.owner(value, prepared_batching=PreparedBatching('token_budget', None, 10000),
                           rolling_refill=True)
        chain = owner._new_owner(None)
        self.assertEqual(chain.resources.max_active_batches, 19)
        self.assertEqual(chain.request_window, 19)
        self.assertEqual(chain.capacity, value)
        self.assertEqual(chain._actors.limits.result_slots, 20)
        self.assertIsNone(chain._actors.model_actor)

    def test_actual_CPU_IPC_reuses_one_owner_with_more_than_eight_resident_rows(self):
        rows = [dict(row_id='row-' + str(i), source_position=i, reviewId='repeat',
                     id='taken_3', reviewText='fast') for i in range(14)]
        with ThreadedRuntime(rows) as runtime:
            owner = self.owner(self.derive())
            try:
                engine_uids = set()
                for index in range(2):
                    stream = owner.evaluate(Source(rows), query_id='bytes-' + str(index))
                    values = list(stream)
                    self.assertEqual(len(values), len(rows))
                    self.assertEqual(len({row['call_id'] for row in values}), len(rows))
                    self.assertGreater(stream.peak_usage['held_tasks'], 8)
                    self.assertLessEqual(stream.peak_usage['active_requests'], 19)
                    self.assertLessEqual(stream.peak_usage['input_bytes'], owner.capacity.budget.input_bytes)
                    self.assertLessEqual(stream.peak_usage['result_bytes'], owner.capacity.budget.result_bytes)
                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                    engine_uids.update(row['engine_uid'] for row in values)
                self.assertEqual(len(engine_uids), 1)
                self.assertEqual(len(runtime.async_actors), 1)
                self.assertEqual(runtime.async_actors[0].instance.engine._sequence, 28)
            finally:
                self.assertEqual(owner.close(timeout=3)['status'], 'closed')
