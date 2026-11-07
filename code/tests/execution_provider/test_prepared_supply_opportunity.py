"""CPU-only handoff opportunities; acceptance and permits remain authoritative."""
import asyncio
from concurrent.futures import Future
from dataclasses import replace
import json
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.scheduling.core.session_contract import Acceptance, BackendTask, BatchKey, BatchMember, BatchSubmission
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.execution_provider import test_prepared_request_organization as packing


def owner(workers=1):
    return AsyncRecordsActorOwner.for_prepared(
        EngineSpec('/tmp/CPU-only',sys.executable,sys.executable),GenerationSpec(),
        RecordsResources(prepare_batch_rows=1,model_batch_rows=1,max_active_batches=8,
                         model_rpc_workers=workers))


def member(sequence=0):
    candidate = packing.candidates([2])[0]
    return BackendTask(replace(candidate.key,sequence=sequence),candidate.spec,
        replace(candidate.task,sequence=sequence,profile_name='stage'),
        BatchMember(BatchKey(0,sequence),0,1))


class SupplySnapshotTests(unittest.IsolatedAsyncioTestCase):
    def transport(self, actors):
        notifications=[]
        value=LotusChainActorBackend(actors,8,lambda:notifications.append(True),
            deadline_monotonic=time.monotonic()+5,on_result=lambda *args:None,
            bulk=True,handoff_progress=True)
        self.addCleanup(lambda:self.assertTrue(value.backend.close(3)))
        return value,notifications

    async def test_host_registration_precedes_coroutine_and_does_not_serialize_other_permits(self):
        for workers,available in ((1,False),(2,True)):
            actors=owner(workers);value,notices=self.transport(actors)
            def pending(tasks,endpoint):
                self.assertEqual(value._handoff_pending,1)
                self.assertEqual(value.prepared_supply_opportunity()[0],available)
                self.assertEqual(actors._data_rpc_progress().pending,0)
                return BatchSubmission(Acceptance.NOT_ACCEPTED)
            with patch.object(value.backend,'try_submit_batch',pending):
                self.assertEqual(value.try_submit_batch((member(),),'only').acceptance,Acceptance.NOT_ACCEPTED)
            self.assertEqual(value._handoff_pending,0)
            self.assertTrue(value.prepared_supply_opportunity()[0])
            self.assertTrue(notices)
            self.assertEqual(value.batch_handoff_report()['submit_operations_started'],0)

    async def test_ack_before_host_return_settles_exact_context_and_bad_ack_does_not_grant(self):
        actors=owner();value,notices=self.transport(actors)
        def immediate(tasks,endpoint):
            context=value._bulk[tasks[0].member.batch_key]
            value._batch_submits_started+=1
            value._count_batch_ack('accepted',context)
            return BatchSubmission(Acceptance.ACCEPTED,('local-only',))
        with patch.object(value.backend,'try_submit_batch',immediate):
            value.try_submit_batch((member(),),'only')
        self.assertEqual(value._handoff_pending,0)
        self.assertTrue(value.prepared_supply_opportunity()[0])
        self.assertTrue(notices)
        for status in ('rejected','unknown'):
            context=dict(handoff_pending=True)
            with value._lock:value._handoff_pending+=1
            value._count_batch_ack(status,context)
            self.assertFalse(value.prepared_supply_opportunity()[0])
            self.assertEqual(value._handoff_pending,0)
            self.assertFalse(context['handoff_pending'] if 'handoff_pending' in context else False)

    async def test_real_poll_permit_and_unregistered_handoff_share_actual_width(self):
        actors=owner(2);value,_=self.transport(actors)
        native=Future()
        actors.model_actor=SimpleNamespace(poll_prepared_batch=SimpleNamespace(
            remote=lambda key:SimpleNamespace(future=lambda:native)))
        poll=asyncio.create_task(actors._data_rpc('poll_prepared_batch',(0,0)))
        await asyncio.sleep(0)
        self.assertEqual(actors._data_rpc_progress().inflight,1)
        with patch.object(value.backend,'try_submit_batch',return_value=BatchSubmission(Acceptance.ACCEPTED,('local',))):
            value.try_submit_batch((member(),),'only')
        self.assertFalse(value.prepared_supply_opportunity()[0])
        native.set_result(b'{}')
        await poll
        self.assertTrue(value.prepared_supply_opportunity()[0])
        context=value._bulk[BatchKey(0,0)]
        value._count_batch_ack('accepted',context)

    async def test_cancelled_batch_waiter_keeps_RPC_and_submit_counts_until_remote_terminal(self):
        actors=owner(2)
        notices=[]
        def notify():
            self.assertFalse(actors._rpc_state_lock._is_owned())
            notices.append(actors._data_rpc_progress())
        subscription=actors._bind_rpc_progress(notify)
        native=Future()
        actors.model_actor=SimpleNamespace(try_submit_prepared_batch=SimpleNamespace(
            remote=lambda payload:SimpleNamespace(future=lambda:native)))
        task=asyncio.create_task(actors._data_rpc('try_submit_prepared_batch',b'fixture'))
        await asyncio.sleep(0)
        self.assertEqual((actors._data_rpc_progress().pending,actors._data_rpc_progress().batch_submits_pending),(1,1))
        self.assertEqual(notices,[], 'enqueue/acquire only reduce availability; they need no Core wake')
        with self.assertRaises(RuntimeError):actors._bind_rpc_progress(lambda:None)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(actors._data_rpc_progress().inflight,1)
        self.assertEqual(actors._data_rpc_progress().batch_submits_pending,1)
        native.set_result(dict(status='accepted'))
        await asyncio.sleep(0)
        self.assertEqual((actors._data_rpc_progress().pending,actors._data_rpc_progress().batch_submits_pending),(0,0))
        self.assertEqual([n.generation for n in notices],sorted(n.generation for n in notices))
        self.assertTrue(actors._unbind_rpc_progress(subscription))

    async def test_old_unsubscribe_cannot_disable_new_query_and_notifications_cannot_fail_RPC(self):
        actors=owner()
        old=[]
        previous=actors._bind_rpc_progress(lambda:old.append(True))
        stale=actors._rpc_observer
        current=[]
        token=actors._bind_rpc_progress(lambda:current.append(True))
        self.assertFalse(actors._unbind_rpc_progress(previous))
        stale()
        self.assertEqual(old,[])
        actors._rpc_observer()
        self.assertEqual(current,[True])
        self.assertTrue(actors._unbind_rpc_progress(token))
        def broken():raise RuntimeError('CPU observer failure')
        subscription=actors._bind_rpc_progress(broken)
        native=Future();native.set_result(dict(status='accepted'))
        actors.model_actor=SimpleNamespace(try_submit_prepared_batch=SimpleNamespace(
            remote=lambda payload:SimpleNamespace(future=lambda:native)))
        self.assertEqual(await actors._data_rpc('try_submit_prepared_batch',b'fixture'),dict(status='accepted'))
        self.assertEqual(actors._data_rpc_progress().pending,0)
        self.assertEqual(actors._data_rpc_progress().batch_submits_pending,0)
        self.assertEqual(actors._rpc_observer_error,'RuntimeError')
        self.assertTrue(actors._unbind_rpc_progress(subscription))

    async def test_constructor_rebind_failure_closes_new_backend_and_preserves_subscription(self):
        actors=owner()
        first=actors._bind_rpc_progress(lambda:None)
        native=Future()
        actors.model_actor=SimpleNamespace(poll_prepared_batch=SimpleNamespace(
            remote=lambda key:SimpleNamespace(future=lambda:native)))
        task=asyncio.create_task(actors._data_rpc('poll_prepared_batch',(0,0)))
        await asyncio.sleep(0)
        from src.scheduling.runtime.async_backend import BoundedAsyncBackend
        close=BoundedAsyncBackend.close
        observed=[]
        def closed(backend,*args,**kwargs):
            value=close(backend,*args,**kwargs);observed.append(value);return value
        with patch.object(BoundedAsyncBackend,'close',closed),self.assertRaises(RuntimeError):
            self.transport(actors)
        self.assertEqual(observed,[True])
        self.assertIs(actors._rpc_subscription,first)
        native.set_result(b'{}');await task
        self.assertTrue(actors._unbind_rpc_progress(first))

    async def test_cancelled_local_Future_preserves_unknown_permit_and_batch_count(self):
        actors=owner();value,_=self.transport(actors)
        native=Future();ref=SimpleNamespace(future=lambda:native)
        actors.model_actor=SimpleNamespace(try_submit_prepared_batch=SimpleNamespace(remote=lambda payload:ref))
        actors.ray=SimpleNamespace(wait=lambda refs,**kw:([],refs))
        task=asyncio.create_task(actors._data_rpc('try_submit_prepared_batch',b'fixture'))
        await asyncio.sleep(0)
        native.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        progress=actors._data_rpc_progress()
        self.assertEqual((progress.pending,progress.batch_submits_pending,progress.unconfirmed),(1,1,1))
        self.assertFalse(value.prepared_supply_opportunity()[0])
        actors.ray.wait=lambda refs,**kw:(refs,[])
        report=actors._rpc_cleanup_status(dict(status='closed',cleanup_errors=[]),deadline=time.monotonic())
        self.assertEqual(report['data_rpc_pending'],0)
        self.assertEqual(actors._data_rpc_progress().batch_submits_pending,0)

    async def test_cancelled_handoff_can_settle_unknown_before_issued_RPC_is_terminal(self):
        actors=owner();value,_=self.transport(actors)
        native=Future();entered=asyncio.Event()
        def remote(payload):
            entered.set();return SimpleNamespace(future=lambda:native)
        actors.model_actor=SimpleNamespace(try_submit_prepared_batch=SimpleNamespace(remote=remote))
        with patch.object(value.backend,'try_submit_batch',return_value=BatchSubmission(Acceptance.ACCEPTED,('local',))):
            value.try_submit_batch((member(),),'only')
        group=BatchKey(0,0);context=value._bulk[group]
        task=asyncio.create_task(value._drive_batch(group,context))
        await asyncio.wait_for(entered.wait(),1)
        task.cancel();task.cancel();await task
        self.assertTrue(context['driver_done'])
        self.assertEqual(value._handoff_pending,0)
        self.assertEqual(value.batch_handoff_report()['actor_ack_counts']['unknown'],1)
        self.assertEqual((actors._rpc_pending,actors._rpc_batch_submits_pending),(1,1))
        self.assertFalse(value.prepared_supply_opportunity()[0])
        self.assertTrue(value.backend.close())
        self.assertIsNotNone(actors._rpc_unsubscribe)
        native.set_result(dict(status='accepted'))
        await asyncio.sleep(0)
        self.assertEqual((actors._rpc_pending,actors._rpc_batch_submits_pending),(0,0))
        self.assertIsNone(actors._rpc_observer)
        self.assertIsInstance(context['futures'][member().key].exception(),asyncio.CancelledError)

    async def test_handoff_notification_failure_keeps_accepted_result_and_cleanup(self):
        actors=owner();value,_=self.transport(actors)
        native=Future();native.set_result(dict(status='accepted'))
        actors.model_actor=SimpleNamespace(try_submit_prepared_batch=SimpleNamespace(
            remote=lambda payload:SimpleNamespace(future=lambda:native)))
        async def poll(key):
            return json.dumps(dict(results=[dict(request_key=[0,0],result=dict(status='succeeded'))],closed=False)).encode()
        actors.wait_prepared_batch=poll
        def broken():raise RuntimeError('CPU notification failure')
        value._handoff_notify=broken
        with patch.object(value.backend,'try_submit_batch',return_value=BatchSubmission(Acceptance.ACCEPTED,('local',))):
            value.try_submit_batch((member(),),'only')
        context=value._bulk[BatchKey(0,0)]
        await value._drive_batch(BatchKey(0,0),context)
        self.assertTrue(context['driver_done'])
        self.assertEqual(json.loads(context['futures'][member().key].result())['status'],'succeeded')
        self.assertEqual(value._handoff_observer_error,'RuntimeError')
        self.assertEqual(value.batch_handoff_report()['actor_ack_counts'],dict(accepted=1,rejected=0,unknown=0))
