"""Finite ordinary defaults from applied IPC declarations, independent of R."""
from dataclasses import asdict, replace
import sys
import unittest

from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity, RecordsCapacityBudget
from src.execution_provider.adapters.records_profile import resolve_core_profile, resolve_records_runtime
from src.execution_provider.adapters.lotus_records_core import create_records_core
from src.experiments.movie_records_config import records_runtime
from src.semantic_methods.budget import row_reservation
from src.semantic_methods.lotus.records import RecordsResources, FilterMethod
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class DefaultResourceTests(unittest.TestCase):
    def setUp(self):
        self.engine = EngineSpec('/tmp/default-resource-model', sys.executable, sys.executable)
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=11)

    def test_changing_request_R_leaves_Core_Method_and_result_allocations_identical(self):
        compared=[]
        for requests in (1, 2, 21):
            resources=replace(self.resources, max_active_batches=requests)
            for rolling in (False, True):
                configured=resolve_records_runtime(self.engine, resources, dict(mode='async'),
                    prepared_batching=None if not rolling else object(), rolling_refill=rolling)
                budget=configured.capacity.budget
                self.assertEqual(budget.active_requests, requests)
                self.assertEqual(configured.async_limits.max_inflight, requests)
                compared.append(tuple(getattr(budget,k) for k in
                    ('held_tasks','input_bytes','result_bytes','method_runs','method_bytes'))+
                    tuple(getattr(configured.async_limits,k) for k in ('result_slots','input_bytes','result_bytes')))
                rows=budget.result_bytes//configured.capacity.item_result_bytes
                self.assertGreater(budget.held_tasks, rows)
                method_rows=min(rows,budget.input_bytes//configured.capacity.item_input_bytes)
                self.assertGreater(budget.method_runs, method_rows)
                self.assertEqual(budget.method_bytes, method_rows*row_reservation(configured.capacity.method_limits(1)))
                self.assertIn('existing IPC', configured.profile['resource_budget_source'])
        self.assertEqual(len(set(compared)),1)
        self.assertIsNone(self.engine.max_model_len)
        self.assertIsNone(self.engine.max_num_seqs)
        self.assertIsNone(self.engine.max_num_batched_tokens)

    def test_static_soft_C_does_not_shrink_the_request_owner_or_storage(self):
        configured=resolve_records_runtime(self.engine,self.resources,dict(mode='async',max_inflight=1))
        self.assertEqual((configured.profile['max_inflight'],configured.profile['owner_max_inflight']),(1,11))
        self.assertEqual(configured.async_limits.max_inflight,11)
        owner=create_records_core(FilterMethod('fixture','Keep {reviewText}'),self.engine,GenerationSpec(),
            self.resources,profile=dict(mode='async',max_inflight=1))
        try:
            self.assertEqual(owner.resources.max_active_batches,11)
            self.assertEqual(owner.capacity,configured.capacity)
            self.assertEqual(owner._actors.limits,configured.async_limits)
        finally:owner.close(timeout=1)

    def test_explicit_result_slots_and_bytes_stay_independent(self):
        capacity=CoreRecordsCapacity(item_input_bytes=65536,item_result_bytes=65536)
        ipc=AsyncEngineLimits(max_inflight=3,result_slots=1,input_bytes=4*65536,result_bytes=6*65536,
            item_input_bytes=65536,item_result_bytes=65536,command_slots=2)
        configured=resolve_records_runtime(self.engine,self.resources,dict(mode='async',max_inflight=2),
            capacity=capacity,async_limits=ipc)
        self.assertIs(configured.async_limits,ipc)
        self.assertEqual(configured.capacity.budget.held_tasks,7)
        self.assertEqual(configured.capacity.budget.active_requests,11)
        self.assertEqual(configured.capacity.budget.input_bytes,ipc.input_bytes)
        self.assertEqual(configured.capacity.budget.result_bytes,ipc.result_bytes)
        self.assertNotEqual(configured.capacity.budget.held_tasks,ipc.result_slots)
        self.assertNotEqual(configured.capacity.budget.active_requests,ipc.max_inflight)

    def test_complete_explicit_budgets_and_IPC_are_unchanged(self):
        raw=CoreRecordsCapacity(item_input_bytes=65536,item_result_bytes=65536)
        budget=RecordsCapacityBudget(9,12*65536,13*65536,4,6,6*row_reservation(raw.method_limits(1)))
        capacity=replace(raw,budget=budget)
        ipc=AsyncEngineLimits(max_inflight=3,result_slots=1,input_bytes=4*65536,result_bytes=6*65536,
            item_input_bytes=65536,item_result_bytes=65536,command_slots=2)
        configured=resolve_records_runtime(self.engine,self.resources,dict(mode='async',max_inflight=2),
            capacity=capacity,async_limits=ipc)
        self.assertIs(configured.capacity,capacity)
        self.assertIs(configured.async_limits,ipc)
        self.assertEqual(configured.profile['resource_budget'],asdict(budget))
        self.assertEqual(configured.profile['resource_budget_source'],
                         'explicit deployment declaration; logical bounds, not measured RSS')

    def test_ordinary_and_session_helpers_use_the_same_complete_resolution(self):
        for key,backend in (('core_profile','semloom-local-model-core-async'),('profile','semloom-core')):
            config=dict(backend=backend,engine=asdict(self.engine),resources=asdict(self.resources),
                        async_limits=dict(result_slots=1),**{key:dict(mode='async',max_inflight=2)})
            actual=records_runtime(config)
            expected=resolve_records_runtime(self.engine,self.resources,config[key],async_limits=config['async_limits'])
            self.assertEqual(actual,expected)
            self.assertEqual(actual.async_limits.result_slots,1)
            self.assertGreater(actual.capacity.budget.held_tasks,1)

    def test_sync_and_nonrolling_batch_declarations_remain_unchanged(self):
        profile=dict(mode='sync',max_inflight=1)
        expected=resolve_core_profile(self.engine,self.resources,profile)
        configured=resolve_records_runtime(self.engine,self.resources,profile)
        self.assertIsNone(configured.capacity.budget)
        self.assertEqual(configured.profile,expected)
        configured=resolve_records_runtime(self.engine,self.resources,dict(mode='async'),prepared_batching=object())
        self.assertIsNone(configured.capacity.budget)
        for retired in ('computed-work-local-pairs','delivered-work-scan',
                        dict(mode='async',admission=dict(kind='legacy_resource_feedback'))):
            with self.subTest(retired=retired), self.assertRaises(ValueError):
                resolve_records_runtime(self.engine,self.resources,retired)


if __name__=='__main__':
    unittest.main()
