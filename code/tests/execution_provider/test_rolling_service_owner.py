"""Actual rolling wrapper/chain/CPU IPC with a process-local payload pool."""
from dataclasses import replace
import gc
import unittest
from unittest.mock import patch
import weakref

from src.baselines.text.movie_records import MovieRecordSource
from src.baselines.text.movie_tasks import MovieTask
from src.execution_provider.adapters.lotus_records_core import create_records_core
from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity
from src.execution_provider.adapters.records_service_budget import RecordsPayloadPool, _plan
from src.execution_provider.adapters.records_service_delivery import declare_ordinary_delivery, declare_ordinary_session
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture
from tests.experiments.test_records_service_entry import runtime_identity


class RollingServiceOwnerTests(unittest.TestCase):
    def prepare(self, queries=('rolling-service',), *, resident=False, rows=None, one_request=False):
        fixture=MovieTokenResidentFixture(self)
        runtime_identity(fixture.runtime)
        plan=fixture.plan('RF')
        manifest=(fixture.manifests['movie-q1'] if rows is None else
            fixture.source_manifest('many-service-rows',rows,task='movie-q1'))
        source=MovieRecordSource.from_manifest(manifest)
        fixture.runtime.rows=source.rows()
        fixture.replies={'filter|'+row['row_id']:dict(text=str(i%2==0))
            for i,row in enumerate(source.rows())}
        fixture.write_replies()
        shape=CoreRecordsCapacity(item_input_bytes=8192,item_result_bytes=8192)
        values=[(q,source.expected(q)) for q in queries]
        declaration=(declare_ordinary_delivery(*values[0],item_result_bytes=shape.item_result_bytes)
            if len(values)==1 else declare_ordinary_session(values,item_result_bytes=shape.item_result_bytes))
        pool=RecordsPayloadPool(32*1024**2)
        maximum=_plan(shape,1,declaration).bytes if one_request else None
        engine=replace(EngineSpec.from_dict(plan['engine']),max_model_calls=len(source.expected(queries[0]))*len(queries))
        owner=create_records_core(MovieTask(1).method(plan['model_name']),engine,
            GenerationSpec.from_dict(plan['generation']),profile=dict(mode='async'),
            prepared_batching=PreparedBatching(**plan['prepared_batching']),rolling_refill=True,
            resident=resident,capacity=shape,service_budget=pool,service_delivery=declaration,
            service_allocation_bytes=maximum)
        fixture.runtime.ray.init()
        self.addCleanup(owner.close,timeout=3)
        return fixture,source,pool,owner,declaration

    def test_actual_producer_uses_declared_delivery_and_no_manual_R(self):
        fixture,source,pool,owner,declaration=self.prepare()
        self.assertIsNone(owner.owner)
        values=list(owner.evaluate(source,query_id=declaration.query_id))
        self.assertEqual([v['decision'] for v in values],[True,False])
        self.assertEqual(sum(v['total_tokens'] for v in values),28)
        self.assertIs(owner._service_allocation._producer,owner.owner)
        self.assertGreater(owner.resources.max_active_batches,8)
        self.assertEqual(pool.snapshot()['allocations'],1)
        self.assertEqual(owner.close(timeout=3)['service_allocation']['status'],'released')
        self.assertEqual(pool.snapshot()['allocations'],0)
        self.assertEqual(len(fixture.runtime.async_actors),1)

    def test_complete_unconsumed_results_outnumber_the_actual_request_grant(self):
        rows=[(str(n),'taken_3','review '+str(n),'POSITIVE') for n in range(12)]
        fixture,source,pool,owner,declaration=self.prepare(rows=rows,one_request=True)
        stream=owner.evaluate(source,query_id=declaration.query_id)
        first=next(stream)
        self.assertEqual(owner.capacity.budget.active_requests,1)
        self.assertEqual(len(owner.last_query_report['calls']),12)
        report=owner.close(timeout=3)
        self.assertEqual(len(report['unconsumed_call_results']),11)
        self.assertEqual(pool.snapshot()['allocations'],1)
        self.assertNotIn(first['call_id'],[r['call_id'] for r in report['unconsumed_call_results']])
        taken=owner.take_unconsumed_results(expected=report['unconsumed_call_results'])
        self.assertEqual(len(taken),11)
        self.assertEqual(len(report['unconsumed_call_results']),11)
        self.assertEqual(owner.take_unconsumed_results(),[])
        self.assertEqual(pool.snapshot()['allocations'],0)
        self.assertEqual(len(fixture.runtime.async_actors),1)

    def test_resident_query_finish_keeps_the_same_instance_grant(self):
        fixture,source,pool,owner,declaration=self.prepare(('service-first','service-second'),resident=True)
        ready=owner.start()
        for query in ('service-first','service-second'):
            values=list(owner.evaluate(source,query_id=query))
            self.assertEqual([v['decision'] for v in values],[True,False])
            self.assertTrue(owner.last_query_report['model_retained'])
            self.assertEqual(pool.snapshot()['allocations'],1)
            self.assertEqual(owner.model_ready['engine_uid'],ready['engine_uid'])
        self.assertEqual(len(fixture.runtime.async_actors),1)
        self.assertEqual(owner.close(timeout=3)['status'],'closed')
        self.assertEqual(pool.snapshot()['allocations'],0)

    def test_wrong_actual_source_and_query_fail_before_model_allocation(self):
        fixture,source,pool,owner,declaration=self.prepare()
        with self.assertRaises(ValueError):
            owner.evaluate(source,query_id='undeclared-query')
        self.assertEqual(len(fixture.runtime.async_actors),0)
        self.assertIsNone(owner.owner)
        self.assertEqual(owner.close(timeout=3)['status'],'closed')
        self.assertEqual(pool.snapshot()['allocations'],0)

    def test_terminal_chain_payloads_are_retired_before_the_grant_can_be_spent_again(self):
        _,source,pool,owner,declaration=self.prepare()
        values=list(owner.evaluate(source,query_id=declaration.query_id))
        grant=owner._service_allocation
        child=owner.owner
        active=owner._active
        self.assertEqual(len(child.calls),len(values))
        report=owner.close(timeout=3)
        self.assertEqual(report['calls'],values)
        self.assertIsNone(owner.owner)
        self.assertIsNone(owner._active)
        self.assertIsNone(grant._producer)
        self.assertEqual(owner.last_close_report['calls_count'],len(values))
        self.assertTrue(owner.last_close_report['payload_transferred'])
        self.assertNotIn('calls',owner.last_close_report)
        self.assertEqual(pool.snapshot()['allocated_bytes'],0)
        # External snapshots and the caller's already-returned iterator retain
        # their values; the wrapper and pool no longer own either payload array.
        self.assertEqual(len(active._report['calls']),len(values))
        self.assertEqual(len(report['calls']),len(values))
        replacement=pool.allocate(CoreRecordsCapacity(
            item_input_bytes=owner.capacity.item_input_bytes,
            item_result_bytes=owner.capacity.item_result_bytes),delivery=declaration)
        replacement.cancel()

    def test_terminal_report_without_live_cleanup_proof_keeps_all_delivery_bytes(self):
        _,source,pool,owner,declaration=self.prepare()
        list(owner.evaluate(source,query_id=declaration.query_id))
        child=owner.owner
        allocation=owner._service_allocation.allocation_id
        before=pool.snapshot()['allocated_bytes']
        with patch.object(child,'_service_cleanup_confirmed',return_value=False):
            report=owner.close(timeout=3)
            self.assertEqual(report['status'],'closed')
            self.assertEqual(pool.snapshot()['allocated_bytes'],before)
            self.assertIs(owner.owner,child)
            with self.assertRaisesRegex(ValueError,'payload'):
                pool.allocate(CoreRecordsCapacity(item_input_bytes=8192,item_result_bytes=8192),
                    delivery=declaration)
        final=pool.retry_cleanup(allocation,timeout=3)
        self.assertEqual(final['service_allocation']['status'],'released')
        self.assertEqual(pool.snapshot()['allocated_bytes'],0)

    def test_a_foreign_retirement_marker_does_not_return_bytes(self):
        _,source,pool,owner,declaration=self.prepare()
        list(owner.evaluate(source,query_id=declaration.query_id))
        grant=owner._service_allocation
        with patch.object(grant,'settle',return_value=False):
            report=owner.close(timeout=3)
        self.assertEqual(report['service_allocation']['status'],'bound')
        self.assertIsNone(owner.owner)
        marker=owner._service_retirement
        for replacement in (object(), {}, None):
            owner._service_retirement=replacement
            with self.subTest(replacement_type=type(replacement).__name__):
                self.assertFalse(grant.settle(owner))
                self.assertEqual(pool.snapshot()['allocations'],1)
        owner._service_retirement=marker
        self.assertTrue(grant.settle(owner))
        self.assertEqual(pool.snapshot()['allocations'],0)

    def test_interrupted_retirement_retries_only_the_same_original_chain(self):
        _,source,pool,owner,declaration=self.prepare()
        list(owner.evaluate(source,query_id=declaration.query_id))
        grant=owner._service_allocation
        allocation=grant.allocation_id
        child=owner.owner
        active=owner._active
        clear=active._clear_iteration
        calls=[]
        def interrupt_after_proof():
            calls.append(True)
            if len(calls)==1:
                raise KeyboardInterrupt('CPU interruption before wrapper payload transfer')
            clear()
        with patch.object(active,'_clear_iteration',side_effect=interrupt_after_proof):
            with self.assertRaises(KeyboardInterrupt):
                owner.close(timeout=3)
        self.assertIs(owner.owner,child)
        self.assertIsNone(grant._producer)
        self.assertIs(grant._retiring_producer_ref(),child)
        self.assertEqual(pool.snapshot()['allocations'],1)
        owner.owner=object()
        try:
            with self.assertRaisesRegex(RuntimeError,'replaced'):
                pool.retry_cleanup(allocation,timeout=3)
            self.assertEqual(pool.snapshot()['allocations'],1)
        finally:
            owner.owner=child
        report=pool.retry_cleanup(allocation,timeout=3)
        self.assertEqual(report['service_allocation']['status'],'released')
        self.assertIsNone(owner.owner)
        self.assertIsNone(grant._retiring_producer_ref)
        self.assertEqual(pool.snapshot()['allocations'],0)

    def test_private_result_cycles_release_arrays_without_running_garbage_collection(self):
        _,source,pool,owner,declaration=self.prepare()
        values=list(owner.evaluate(source,query_id=declaration.query_id))
        producer_ref=weakref.ref(owner.owner)
        stream_ref=weakref.ref(owner.owner._active)
        enabled=gc.isenabled()
        gc.disable()
        try:
            report=owner.close(timeout=3)
            self.assertEqual(report['calls'],values)
            self.assertEqual(pool.snapshot()['allocated_bytes'],0)
            producer=producer_ref()
            stream=stream_ref()
            if producer is not None:
                self.assertTrue(producer._service_payloads_retired_confirmed())
                self.assertEqual(producer._raw_receipts,{})
                self.assertIsNone(producer._root_run)
                self.assertIsNone(producer._active)
                self.assertNotIn('calls',producer.last_close_report)
            if stream is not None:
                self.assertIsNone(stream.run)
                self.assertIsNone(stream.section)
                self.assertIsNone(stream.error)
                self.assertNotIn('calls',stream.last_close_report)
        finally:
            if enabled:
                gc.enable()


if __name__=='__main__':
    unittest.main()
