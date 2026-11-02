"""RPC CPU permits are independent of owned model requests; no Ray/model runtime."""

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from src.scheduling.core.session_contract import TaskKey
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.async_engine_process import BatchSubmitResult, SubmitResult, TerminalEvent
from src.execution_provider.adapters.lotus_records_core import _encode
from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS, result_record
from tests.execution_provider.test_lotus_records_async_actor import prepared


class ControlledEngine:
    def __init__(self, count):
        self.expected, self.sequence = count, 0
        self.live, self.events = {}, deque()
        self.all_submitted = threading.Event()
        self.ready_report = dict(engine_uid='cpu-rpc-fixture')
        self.owner_uid = 'b' * 32
        self.backpressure = self.closed = False

    def submit(self, row, **options):
        if self.backpressure:
            return SubmitResult('backpressure')
        ticket = self.owner_uid + ':' + str(self.sequence)
        self.sequence += 1
        self.live[ticket] = row
        if self.sequence == self.expected:
            self.all_submitted.set()
        return SubmitResult('queued', ticket)

    def finish(self, ticket, status='succeeded'):
        row = self.live.pop(ticket)
        identity = {n: row[n] for n in IDENTITY_FIELDS}
        value = result_record(identity, engine_uid='cpu-rpc-fixture', start=1., end=2.,
                              status=status, submitted=True)
        value.update(text='True', prompt_tokens=1, completion_tokens=1, total_tokens=2,
                     prompt_token_ids_json='[1]', output_token_ids_json='[2]',
                     finish_reason='stop', engine_request_id=ticket,
                     engine_terminal_confirmed=status != 'unknown')
        self.events.append(TerminalEvent(ticket, identity, value, status != 'unknown', None))

    def submit_batch(self, rows, **options):
        if self.backpressure:
            return BatchSubmitResult('backpressure')
        return BatchSubmitResult('queued', tuple(self.submit(row, **options).request_id for row in rows))

    def poll(self, maximum):
        return tuple(self.events.popleft() for _ in range(min(maximum, len(self.events))))

    def abort(self, ticket):
        if ticket in self.live:
            self.finish(ticket, 'failed')
        return True

    def snapshot(self):
        return dict(owner_uid=self.owner_uid, issued_submissions=self.sequence, broken=None,
                    active_requests=len(self.live), closing=self.closed, child_alive=not self.closed)

    def close(self, **options):
        for ticket in tuple(self.live):
            self.abort(ticket)
        self.closed = True
        return dict(status='closed', producer_quiescence=dict(
            schema='semloom.async_producer_quiescence.v1', confirmed=True, owner_uid=self.owner_uid,
            engine_uid='cpu-rpc-fixture', issued_submissions=self.sequence))


class Handle:
    """Threaded RPC fixture with exactly the configured executor budget."""
    def __init__(self, actor, workers):
        self.actor = actor
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='bounded-data-rpc')
        self.calls = []

    def __getattr__(self, name):
        def remote(*args, **kwargs):
            future = self.pool.submit(getattr(self.actor, name), *args, **kwargs)
            self.calls.append((name, future))
            return SimpleNamespace(future=lambda: future)
        return SimpleNamespace(remote=remote)


def fixture(count=2, *, workers=1):
    resources = RecordsResources(cpu_workers=2, model_rpc_workers=workers,
        prepare_batch_rows=1, model_batch_rows=1, max_active_batches=count)
    spec = EngineSpec('/tmp/cpu-rpc-model', '/tmp/cpu-model-python', '/tmp/cpu-driver-python')
    owner = AsyncRecordsActorOwner(FilterMethod('fixture', instruction='Keep {reviewText}'),
                                  spec, GenerationSpec(), resources)
    actor = AsyncCoreModelActor(*owner._model_actor_arguments())
    actor.engine = ControlledEngine(count)
    owner.model_actor = Handle(actor, owner._model_actor_options()['max_concurrency'])
    return owner, actor


class RPCConfigurationTests(unittest.TestCase):
    def test_existing_records_factory_preserves_explicit_RPC_deployment_setting(self):
        from src.execution_provider.adapters.lotus_records_core import create_records_core
        resources = RecordsResources(**dict(cpu_workers=3, model_rpc_workers=2,
            prepare_batch_rows=1, model_batch_rows=1, max_active_batches=2000))
        owner = create_records_core(FilterMethod('fixture', instruction='Keep {reviewText}'),
            EngineSpec('/tmp/cpu-model', '/tmp/model-python', '/tmp/driver-python'), GenerationSpec(),
            resources, profile=dict(mode='async', max_inflight=2000))
        self.assertEqual(owner._actors._model_actor_options()['max_concurrency'], 4)
        self.assertEqual(owner._actors.limits.max_inflight, 2000)
        self.assertIsNone(owner._actors.model_actor)

    def test_request_capacity_2000_does_not_select_2002_threads(self):
        for capacity in (1, 2, 2000):
            for cpu_workers, explicit in ((1, None), (3, None), (1, 4)):
                with self.subTest(capacity=capacity, cpu=cpu_workers, explicit=explicit):
                    resources = RecordsResources(cpu_workers=cpu_workers, model_rpc_workers=explicit,
                        prepare_batch_rows=1, model_batch_rows=1, max_active_batches=capacity)
                    spec = EngineSpec('/tmp/cpu-model', '/tmp/model-python', '/tmp/driver-python')
                    owner = AsyncRecordsActorOwner.for_prepared(spec, GenerationSpec(), resources)
                    resolved = cpu_workers if explicit is None else explicit
                    self.assertEqual(owner._model_actor_options()['max_concurrency'], resolved + 2)
                    self.assertEqual(owner.limits.max_inflight, capacity)
                    self.assertEqual(owner.limits.result_slots, capacity)
                    self.assertEqual(owner.execution_metadata()['model_rpc_workers'], resolved)

    def test_invalid_thread_budget_and_existing_request_limits_are_independent(self):
        for value in (0, -1, True, 1.5, '2'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RecordsResources(model_rpc_workers=value)
        owner, actor = fixture()
        try:
            with self.assertRaisesRegex(ValueError, 'input capacity'):
                AsyncRecordsActorOwner.for_prepared(owner.engine, owner.generation, owner.resources,
                    limits=AsyncEngineLimits(max_inflight=1))
        finally:
            actor.close(1)
            owner.model_actor.pool.shutdown()

    def test_backpressure_is_not_owned_and_result_capacity_waits_for_ACK(self):
        owner, actor = fixture(1)
        try:
            actor.engine.backpressure = True
            args = ([prepared()], time.monotonic()+5, 4096, (0, 0))
            self.assertEqual(actor.try_submit_core(*args), dict(status='backpressure'))
            self.assertEqual(actor._held, {})
            self.assertEqual(actor.engine.sequence, 0)
            actor.engine.backpressure = False
            self.assertEqual(actor.try_submit_core(*args), dict(status='accepted'))
            key = (0, 1)
            self.assertEqual(actor.try_submit_core([prepared(1)], args[1], 4096, key)['status'], 'backpressure')
            actor.engine.finish(next(iter(actor.engine.live)))
            raw = actor.poll_prepared_batch((0, 0))
            self.assertEqual(json.loads(raw)['results'][0]['result']['status'], 'succeeded')
            self.assertEqual(actor.poll_prepared_batch((0, 0)), raw)
            self.assertEqual(actor.try_submit_core([prepared(1)], args[1], 4096, key)['status'], 'backpressure')
            self.assertTrue(actor.acknowledge_core((0, 0)))
            self.assertEqual(actor.try_submit_core([prepared(1)], args[1], 4096, key)['status'], 'accepted')
            self.assertEqual(actor.engine.sequence, 2)
        finally:
            actor.close(1)
            owner.model_actor.pool.shutdown()

    def test_bulk_backpressure_returns_without_ownership_and_retains_each_member_until_ACK(self):
        owner, actor = fixture(2)
        try:
            rows = [dict(stage_identity('q', RecordOccurrence('row-'+str(i), i, 0, {}), 'map'),
                         messages_json=json.dumps([dict(role='user', content='whole prompt')])) for i in range(2)]
            calls = [dict(row=row, generation=asdict(owner.generation), maximum=4096, request_key=[0, i])
                     for i, row in enumerate(rows)]
            packet = _encode(dict(calls=calls, batch_key=[0, 0], deadline=time.monotonic()+5),
                             owner.limits.input_bytes)
            actor.engine.backpressure = True
            self.assertEqual(actor.try_submit_prepared_batch(packet), dict(status='backpressure'))
            self.assertEqual(actor._held, {})
            self.assertEqual(actor.engine.sequence, 0)
            actor.engine.backpressure = False
            self.assertEqual(actor.try_submit_prepared_batch(packet), dict(status='accepted'))
            self.assertEqual(actor.engine.sequence, 2)
            self.assertEqual(actor.try_submit_prepared_batch(packet), dict(status='backpressure'))
            with actor._condition:
                actor.engine.finish(next(iter(actor.engine.live)))
            receipts = json.loads(actor.poll_prepared_batch((0, 0)))['results']
            self.assertEqual([r['request_key'] for r in receipts], [[0, 0]])
            self.assertTrue(actor.acknowledge_core((0, 0)))
            self.assertEqual(actor.snapshot()['held_requests'], 1)
            self.assertEqual(actor.engine.sequence, 2)
        finally:
            actor.close(1)
            owner.model_actor.pool.shutdown()


class RPCProgressTests(unittest.IsolatedAsyncioTestCase):
    async def test_2000_owned_requests_progress_on_small_RPC_pool_with_controls(self):
        owner, actor = fixture(2000, workers=2)
        handle, tasks = owner.model_actor, []
        try:
            tasks = [asyncio.create_task(owner.submit_core([prepared(i)], time.monotonic()+15,
                4096, TaskKey(0, i))) for i in range(2000)]
            self.assertTrue(await asyncio.to_thread(actor.engine.all_submitted.wait, 10))
            snapshot = await asyncio.wrap_future(handle.snapshot.remote().future())
            self.assertEqual(snapshot['held_requests'], 2000)
            self.assertTrue(all(not task.done() for task in tasks))
            self.assertLessEqual(len(handle.pool._threads), 4)
            cancel = await asyncio.wrap_future(owner.cancel_core(TaskKey(0, 0)).future())
            self.assertTrue(cancel['requested'])
            with actor._condition:
                for ticket in tuple(actor.engine.live):
                    actor.engine.finish(ticket)
            results = [json.loads(raw) for raw in await asyncio.wait_for(asyncio.gather(*tasks), 15)]
            self.assertEqual(len({value['call_id'] for value in results}), 2000)
            self.assertEqual(sum(value['status'] == 'failed' for value in results), 1)
            self.assertEqual(actor.engine.sequence, 2000, 'accepted requests must not be resubmitted')
            for i in range(2000):
                self.assertTrue(await asyncio.wrap_future(owner.acknowledge_core(TaskKey(0, i)).future()))
            self.assertEqual(actor.snapshot()['held_requests'], 0)
            self.assertEqual(owner._rpc_pending, 0)
            report = await asyncio.wrap_future(handle.close.remote(1).future())
            self.assertEqual(report['status'], 'closed')
            self.assertTrue(report['producer_quiescence']['confirmed'])
        finally:
            actor.close(1)
            await asyncio.gather(*tasks, return_exceptions=True)
            handle.pool.shutdown()

    async def test_snapshot_and_close_can_run_while_all_data_permits_are_waiting(self):
        owner, actor = fixture(2, workers=1)
        entered, release = threading.Event(), threading.Event()
        original = actor.try_submit_core
        def blocked(*args):
            entered.set()
            if not release.wait(5):
                raise TimeoutError('test did not release the data handoff')
            return original(*args)
        task = None
        try:
            with patch.object(actor, 'try_submit_core', blocked):
                task = asyncio.create_task(owner.submit_core([prepared()], time.monotonic()+10,
                    4096, TaskKey(0, 0)))
                self.assertTrue(await asyncio.to_thread(entered.wait, 3))
                snapshot = await asyncio.wait_for(asyncio.wrap_future(owner.model_actor.snapshot.remote().future()), 3)
                self.assertEqual(snapshot['held_requests'], 0)
                report = await asyncio.wait_for(asyncio.wrap_future(owner.model_actor.close.remote(1).future()), 3)
                self.assertEqual(report['status'], 'closed')
                release.set()
                result = json.loads(await task)
                self.assertEqual(result['status'], 'failed')
                self.assertFalse(result['submission_confirmed'])
        finally:
            release.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            actor.close(1)
            owner.model_actor.pool.shutdown()

    async def test_unknown_is_not_ACKed_until_producer_quiescence_and_is_not_replayed(self):
        owner, actor = fixture(1)
        task = asyncio.create_task(owner.submit_core([prepared()], time.monotonic()+10, 4096, TaskKey(0, 0)))
        try:
            self.assertTrue(await asyncio.to_thread(actor.engine.all_submitted.wait, 3))
            with actor._condition:
                actor.engine.finish(next(iter(actor.engine.live)), 'unknown')
            result = json.loads(await task)
            self.assertEqual(result['status'], 'unknown')
            self.assertFalse(actor.acknowledge_core((0, 0)))
            self.assertEqual(actor.snapshot()['held_requests'], 1)
            report = actor.close(1)
            self.assertEqual(report['unconsumed_core_results'][0]['result']['status'], 'unknown')
            self.assertTrue(report['producer_quiescence']['confirmed'])
            self.assertEqual(actor.engine.sequence, 1)
        finally:
            actor.close(1)
            await asyncio.gather(task, return_exceptions=True)
            owner.model_actor.pool.shutdown()

    async def test_cancelling_a_waiter_keeps_its_incomplete_RPC_permit_but_not_control_threads(self):
        owner, actor = fixture(1)
        entered, release = threading.Event(), threading.Event()
        original = actor.try_submit_core
        def blocked(*args):
            entered.set()
            if not release.wait(5):
                raise TimeoutError('test did not release the data RPC')
            return original(*args)
        task = None
        try:
            with patch.object(actor, 'try_submit_core', blocked):
                task = asyncio.create_task(owner.submit_core([prepared()], time.monotonic()+10, 4096, TaskKey(0, 0)))
                self.assertTrue(await asyncio.to_thread(entered.wait, 3))
                task.cancel()
                # A control RPC is an ordering barrier after cancellation, not a
                # wall-clock guess that the data call has already returned.
                report = await asyncio.wrap_future(owner.model_actor.close.remote(1).future())
                self.assertEqual(report['status'], 'closed')
                self.assertEqual(owner._rpc_pending, 1)
                self.assertEqual(owner._rpc_inflight, 1)
                release.set()
                remote = next(future for name, future in owner.model_actor.calls if name == 'try_submit_core')
                await asyncio.wrap_future(remote)
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual(owner._rpc_pending, 0)
                self.assertEqual(owner._rpc_inflight, 0)
                self.assertEqual(actor.engine.sequence, 0)
        finally:
            release.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            actor.close(1)
            owner.model_actor.pool.shutdown()


class RPCLoopReuseTests(unittest.TestCase):
    def test_resident_owner_rebinds_only_after_previous_transport_RPC_is_terminal(self):
        owner, actor = fixture(1)
        try:
            for session in (0, 1):
                actor.engine.backpressure = True
                actor.engine.expected = session + 1
                actor.engine.all_submitted.clear()
                async def query():
                    key = TaskKey(session, 0)
                    task = asyncio.create_task(owner.submit_core([prepared(query='q'+str(session))],
                        time.monotonic()+5, 4096, key))
                    actor.engine.backpressure = False
                    self.assertTrue(await asyncio.to_thread(actor.engine.all_submitted.wait, 3))
                    with actor._condition:
                        actor.engine.finish(next(iter(actor.engine.live)))
                    result = json.loads(await task)
                    self.assertEqual(result['status'], 'succeeded')
                    self.assertTrue(await asyncio.wrap_future(owner.acknowledge_core(key).future()))
                asyncio.run(query())
                self.assertEqual(owner._rpc_pending, 0)
            self.assertEqual(actor.engine.sequence, 2)
            self.assertEqual(actor.snapshot()['held_requests'], 0)
        finally:
            actor.close(1)
            owner.model_actor.pool.shutdown()
