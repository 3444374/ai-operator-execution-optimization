"""Rolling service grants charge complete delivery independently of requests."""
from dataclasses import replace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity
from src.execution_provider.adapters.records_service_budget import RecordsPayloadPool
from src.execution_provider.adapters.records_service_delivery import declare_ordinary_delivery, declare_ordinary_session
from src.semantic_methods.budget import row_reservation
from src.semantic_methods.lotus.records import identity


class RollingServiceBudgetTests(unittest.TestCase):
    def setUp(self):
        self.shape = CoreRecordsCapacity(item_input_bytes=8192, item_result_bytes=8192)
        self.pool = RecordsPayloadPool(32*1024**2)

    def delivery(self, count, *, query='rolling-budget'):
        return declare_ordinary_delivery(query, [identity(query,'row-'+str(n),n) for n in range(count)],
            item_result_bytes=self.shape.item_result_bytes)

    def test_complete_delivery_is_independent_of_request_and_Method_capacity(self):
        declaration = self.delivery(80)
        plan = self.pool.preview(self.shape, delivery=declaration)
        parts = dict(plan.components)
        n = plan.capacity.budget.active_requests
        self.assertEqual(plan.capacity.budget.result_bytes, n*self.shape.item_result_bytes)
        self.assertEqual(plan.capacity.budget.method_bytes, n*row_reservation(self.shape.method_limits(1)))
        self.assertEqual(parts['delivery_raw_receipts'], declaration.array_bytes)
        self.assertEqual(parts['delivery_consumer_snapshot'], declaration.array_bytes)
        self.assertEqual(parts['delivery_error_calls'], declaration.array_bytes)
        self.assertEqual(parts['delivery_adoption_calls'], declaration.array_bytes)
        self.assertGreater(sum(v for k,v in parts.items() if k.startswith('delivery_')), declaration.array_bytes)
        self.assertIs(plan.delivery, declaration)
        self.assertEqual(self.pool.snapshot()['allocations'],0)

    def test_larger_undelivered_query_reduces_available_requests_without_changing_core_item_sizes(self):
        short = self.pool.preview(self.shape, delivery=self.delivery(1))
        full = self.pool.preview(self.shape, delivery=self.delivery(300))
        self.assertLess(full.capacity.budget.active_requests, short.capacity.budget.active_requests)
        self.assertEqual(full.capacity.item_input_bytes, short.capacity.item_input_bytes)
        self.assertEqual(full.capacity.item_result_bytes, short.capacity.item_result_bytes)
        self.assertLessEqual(full.bytes,self.pool.total_bytes)

    def test_complete_delivery_must_fit_before_any_allocation(self):
        with self.assertRaisesRegex(ValueError,'payload'):
            self.pool.allocate(self.shape, delivery=self.delivery(10000))
        self.assertEqual(self.pool.snapshot()['allocated_bytes'],0)

    def test_two_owners_never_share_full_delivery_or_instance_bytes(self):
        declaration = self.delivery(3)
        first = self.pool.allocate(self.shape, delivery=declaration,max_bytes=self.pool.total_bytes//2)
        second = self.pool.allocate(self.shape, delivery=declaration,max_bytes=self.pool.total_bytes//2)
        try:
            self.assertNotEqual(first.allocation_id,second.allocation_id)
            self.assertEqual(self.pool.snapshot()['allocated_bytes'],first.plan.bytes+second.plan.bytes)
            self.assertEqual(first.plan.describe()['delivery'],declaration.describe())
        finally:
            first.cancel()
            second.cancel()

    def test_delivery_shape_identity_and_each_actual_query_participate_in_plan(self):
        a = self.delivery(2,query='short')
        b = self.delivery(1,query='long-'+'界'*50)
        session = declare_ordinary_session([(a.query_id,[identity(a.query_id,'row-'+str(n),n) for n in range(2)]),
            (b.query_id,[identity(b.query_id,'row-0',0)])], item_result_bytes=self.shape.item_result_bytes)
        plan = self.pool.preview(self.shape,delivery=session)
        self.assertEqual(dict(plan.components)['delivery_raw_receipts'],max(v.array_bytes for v in session.deliveries))
        self.assertNotEqual(plan.signature,self.pool.preview(self.shape,delivery=a).signature)
        with self.assertRaises(ValueError):
            self.pool.preview(replace(self.shape,item_result_bytes=16384),delivery=a)

    def test_preview_and_factory_share_finite_rolling_options_without_starting_an_owner(self):
        from src.execution_provider.adapters.records_service_config import preview_service_records_runtime
        from src.execution_provider.adapters.lotus_records_core import create_records_core
        from src.semantic_methods.lotus.prepared_batching import PreparedBatching
        from src.semantic_methods.lotus.records import FilterMethod
        from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
        engine=EngineSpec('/tmp/rolling-shape-model','/tmp/model-python','/tmp/driver-python')
        policy=PreparedBatching(policy='token_budget',token_budget=128)
        declaration=self.delivery(20)
        options=dict(capacity=self.shape,profile=dict(mode='async'),rolling_refill=True,
            prepared_batching=policy,service_delivery=declaration)
        with patch('src.execution_provider.adapters.lotus_records_batches.PreparedRecordsCore') as construct:
            preview=preview_service_records_runtime(engine,None,self.pool.total_bytes,**options)
            construct.assert_not_called()
        self.assertEqual(preview.profile['service_payload_plan'],self.pool.preview(
            self.shape,delivery=declaration).describe())
        self.assertFalse(preview.profile['service_allocation_acquired'])
        no_tokens = dict(options, prepared_batching=None, interleaved_preparation=True,
            event_driven_preparation=True, profile=dict(mode='async', admission=dict(kind='resource_feedback')))
        for bypass in (False, True):
            selected = dict(no_tokens, experimental_skip_token_packing=bypass)
            preview = preview_service_records_runtime(engine, None, self.pool.total_bytes, **selected)
            owner = create_records_core(FilterMethod('fixture', instruction='Keep {reviewText}'),
                engine, GenerationSpec(), service_budget=self.pool, **selected)
            try:
                self.assertEqual(owner.execution_profile['service_payload_plan'], preview.profile['service_payload_plan'])
                self.assertIsNone(owner.execution_profile['record_batching']['B'])
                self.assertTrue(owner.event_driven_preparation)
                self.assertIsNone(owner.owner)
                self.assertEqual(owner.execution_profile['record_batching'].get('experimental_skip_token_packing', False), bypass)
            finally:
                owner.close()
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)
        bad=[dict(options,service_delivery=None),
             dict(options,rolling_refill=False),dict(options,async_limits={}),
             dict(options,service_delivery={})]
        for value in bad:
            with self.subTest(value=value),patch(
                    'src.execution_provider.adapters.lotus_records_batches.PreparedRecordsCore') as construct:
                with self.assertRaises((ValueError,TypeError)):
                    create_records_core(FilterMethod('fixture',instruction='Keep {reviewText}'),
                        engine,GenerationSpec(),service_budget=self.pool,**value)
                construct.assert_not_called()
                self.assertEqual(self.pool.snapshot()['allocations'],0)


if __name__=='__main__':
    unittest.main()
