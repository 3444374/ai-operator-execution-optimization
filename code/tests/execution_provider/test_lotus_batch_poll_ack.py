"""ACKs travel with the next bounded poll after successful receipt transfer."""
from copy import deepcopy
from dataclasses import asdict, replace
import asyncio
import json
import threading
import time
from types import SimpleNamespace
import unittest

from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.execution_provider.adapters.records_model_adapter import _encode
from src.execution_provider.adapters.records_transport import close_prepared_core
from src.scheduling.core.session_contract import Acceptance, BatchKey, BatchMember, TaskKey, Terminal, Uncertain
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS
from tests.execution_provider.test_lotus_batch_poll_encoding import receipt, snapshot
from tests.scheduling.test_async_backend import task as backend_task


def ack_payload(keys, group=(1, 0)):
    return _encode(dict(batch_key=list(group), acknowledgements=[list(k) for k in keys]), 65536)


class BatchPollAckTests(unittest.TestCase):
    def test_ack_and_next_page_match_separate_ack_bytes_with_partial_ack_and_replay(self):
        for closed, stopped in ((False, False), (True, False), (True, True)):
            with self.subTest(closed=closed, stopped=stopped):
                values = [receipt(i, status='unknown' if i == 2 else 'succeeded') for i in range(5)]
                actual = snapshot(values, closed=closed, stopped=stopped)
                reference = snapshot(values, closed=closed, stopped=stopped)
                original = deepcopy(actual.last_close_report)
                first = actual.poll_prepared_batch((1, 0))
                self.assertEqual(first, reference.poll_prepared_batch((1, 0)))
                self.assertEqual(actual.poll_prepared_batch((1, 0)), first)
                for i in range(5):
                    key = (1, i)
                    reference.acknowledge_core(key)
                    expected = reference.poll_prepared_batch((1, 0))
                    payload = ack_payload([key])
                    result = actual.poll_prepared_batch((1, 0), payload)
                    self.assertEqual(result, expected)
                    self.assertEqual(actual.poll_prepared_batch((1, 0), payload), expected,
                                     'a lost poll response may repeat its committed ACK')
                    self.assertLessEqual(len(result), actual.limits.item_result_bytes + actual.limits.control_frame_bytes)
                self.assertEqual(actual.last_close_report, original)
                self.assertEqual(set(actual._held), set(reference._held))
                if not stopped:
                    self.assertEqual(set(actual._held), {(1, 2)}, 'delivered unknown still borrows its request')

    def test_malformed_or_unexposed_ack_cannot_partially_release_a_page(self):
        for bad in ('unexposed', 'other_batch', 'duplicate', 'oversize', 'other_session'):
            with self.subTest(bad=bad):
                value = snapshot([receipt(i) for i in range(5)])
                first = value.poll_prepared_batch((1, 0))
                keys = [(1, 0), (1, 4)]
                if bad == 'other_batch':
                    value._held[(1, 4)].batch_key = (1, 9)
                if bad == 'duplicate':
                    keys = [(1, 0), (1, 0)]
                if bad == 'other_session':
                    keys = [(1, 0), (2, 0)]
                payload = ack_payload(keys)
                if bad == 'oversize':
                    value.limits = replace(value.limits, input_bytes=128, item_input_bytes=128)
                    payload += b' ' * 129
                with self.assertRaises(ValueError):
                    value.poll_prepared_batch((1, 0), payload)
                self.assertIn((1, 0), value._held)
                self.assertFalse(value._held[(1, 0)].delivered)
                self.assertEqual(value.poll_prepared_batch((1, 0)), first)

    def test_closed_ack_must_belong_to_the_selected_batch_even_after_retirement(self):
        value = snapshot([receipt(i) for i in range(3)], closed=True, stopped=True)
        value.poll_prepared_batch((1, 0))
        value.last_close_report['unconsumed_core_results'][0]['batch_key'] = [1, 9]
        with self.assertRaises(ValueError):
            value.poll_prepared_batch((1, 0), ack_payload([(1, 0)]))
        self.assertIs(value._closed_pending[(1, 0)], True)

    def test_ack_before_encoding_failure_is_still_idempotent_and_next_result_is_unexposed(self):
        from unittest.mock import patch
        from src.execution_provider.adapters import records_async_model as actors
        value = snapshot([receipt(i) for i in range(3)])
        first = json.loads(value.poll_prepared_batch((1, 0)))
        keys = [tuple(v['request_key']) for v in first['results']]
        with patch.object(actors, '_encode', side_effect=ValueError('reply failed')):
            with self.assertRaises(ValueError):
                value.poll_prepared_batch((1, 0), ack_payload(keys))
        self.assertTrue(all(k not in value._held for k in keys))
        self.assertFalse(value._held[(1, 2)].poll_exposed)
        packet = json.loads(value.poll_prepared_batch((1, 0), ack_payload(keys)))
        self.assertEqual([v['request_key'] for v in packet['results']], [[1, 2]])


class PolledOwner(AsyncRecordsActorOwner):
    """The real owner codec/poll entry with an in-process CPU actor boundary."""
    def __init__(self, actor, saved):
        self.actor, self.limits, self.saved = actor, actor.limits, saved
        self.pages, self.standalone, self.submissions = [], [], 0
        self.lose_reply = False
        self.reject_acks = False

    async def submit_prepared_batch(self, calls, batch_key, deadline):
        self.submissions += 1
        members = set(self.actor._held) | set(self.actor._closed_pending or ())
        if {tuple(c['request_key']) for c in calls} != members:
            raise AssertionError('fixture batch did not preserve original call membership')
        return dict(status='accepted')

    async def _data_rpc(self, method, *args, **kwargs):
        if method != 'poll_prepared_batch':
            raise AssertionError('only the existing poll RPC is needed')
        keys = [] if len(args) == 1 else json.loads(args[1])['acknowledgements']
        if any(TaskKey(*key) not in self.saved for key in keys):
            raise AssertionError('ACK preceded owner sink success')
        self.pages.append(keys)
        if self.reject_acks and keys:
            raise OSError('ACK RPC unavailable')
        result = self.actor.poll_prepared_batch(*args)
        if self.lose_reply and keys:
            self.lose_reply = False
            raise OSError('committed ACK response lost')
        return result

    def cancel_core(self, key):
        pass

    def acknowledge_core(self, key):
        self.standalone.append(key)
        return self.actor.acknowledge_core((key.session_id, key.sequence))


class BatchPollAckTransportTests(unittest.TestCase):
    def transport(self, values, *, closed=False, stopped=False, large=False, refuse=None):
        actor = snapshot(values, closed=closed, stopped=stopped, large=large)
        saved = {}
        owner = PolledOwner(actor, saved)
        def sink(key, value):
            if key == refuse:
                raise OSError('owner receipt sink refused')
            saved[key] = value
        transport = LotusChainActorBackend(owner, len(values), lambda: None,
            deadline_monotonic=time.monotonic()+5, on_result=sink, bulk=True)
        transport.poll_interval_s = .001
        self.addCleanup(transport.backend.close, 3, wait_pending=True)
        group = BatchKey(1, 0)
        tasks = []
        for i, result in enumerate(values):
            row = {k: result[k] for k in IDENTITY_FIELDS}
            row['messages_json'] = '[{"role":"user","content":"original complete prompt"}]'
            task = backend_task(i)
            tasks.append(replace(task, key=TaskKey(1, i), member=BatchMember(group, i, len(values)),
                task=replace(task.task, max_result_bytes=4096, profile_name='stage',
                    payload=json.dumps(dict(row=row, generation={}, parent_call_ids=[])).encode())))
        outcome = transport.try_submit_batch(tuple(tasks), 'endpoint')
        self.assertEqual(outcome.acceptance, Acceptance.ACCEPTED)
        return actor, owner, transport, tasks, outcome, saved

    def settled(self, transport, tasks, outcome):
        for task in tasks:
            try:
                transport.backend._slots[task.key].future.result(3)
            except (RuntimeError, OSError):
                pass
        self.assertTrue(transport.backend.close(3, wait_pending=True))
        return transport.poll(tuple((t.key, h) for t, h in zip(tasks, outcome.handles)), len(tasks))

    def test_many_sink_transfers_use_bounded_poll_ack_without_individual_rpcs(self):
        values = [receipt(i, text='original result') for i in range(129)]
        actor, owner, transport, tasks, outcome, saved = self.transport(values, large=True)
        events = self.settled(transport, tasks, outcome)
        self.assertEqual(sum(type(e) is Terminal for e in events), len(values))
        self.assertEqual(set(saved), {t.key for t in tasks})
        for task in tasks:
            self.assertEqual(saved[task.key]['transport_batch'], asdict(task.member))
            self.assertEqual(saved[task.key]['core_task_key'], asdict(task.key))
            self.assertEqual(saved[task.key]['parent_call_ids'], [])
            self.assertEqual(saved[task.key]['text'], 'original result')
        self.assertEqual(owner.standalone, [])
        self.assertEqual(owner.submissions, 1)
        self.assertLess(len(owner.pages), len(values))
        self.assertEqual(sorted(k for page in owner.pages for k in page), [[1, i] for i in range(129)])
        self.assertFalse(actor._held)
        self.assertFalse(actor._tickets)
        self.assertFalse(transport._bulk, 'final member transfer must flush its ACK')

    def test_closed_pages_and_unknown_keep_usage_and_borrowing(self):
        for stopped in (False, True):
            with self.subTest(stopped=stopped):
                values = [receipt(i, status='unknown' if i == 1 else 'succeeded') for i in range(3)]
                actor, owner, transport, tasks, outcome, saved = self.transport(values, closed=True, stopped=stopped)
                audit = deepcopy(actor.last_close_report)
                events = self.settled(transport, tasks, outcome)
                self.assertEqual(sum(type(e) is Terminal for e in events), 2)
                self.assertEqual(sum(type(e) is Uncertain for e in events), 1)
                self.assertEqual(saved[TaskKey(1, 1)]['total_tokens'], 2)
                self.assertEqual(saved[TaskKey(1, 1)]['status'], 'unknown')
                self.assertEqual(transport.retained_refs(), 1)
                self.assertEqual(set(actor._held), set() if stopped else {(1, 1)})
                self.assertEqual(actor.last_close_report, audit)
                self.assertFalse(actor._closed_pending)
                self.assertFalse(owner.standalone)

    def test_refused_sink_keeps_actor_receipt_while_other_members_drain(self):
        actor, owner, transport, tasks, outcome, saved = self.transport(
            [receipt(i) for i in range(3)], refuse=TaskKey(1, 0))
        events = self.settled(transport, tasks, outcome)
        self.assertEqual(sum(type(e) is Terminal for e in events), 2)
        self.assertEqual(sum(type(e) is Uncertain for e in events), 1)
        self.assertNotIn(TaskKey(1, 0), saved)
        self.assertEqual(set(actor._held), {(1, 0)})
        self.assertFalse(actor._held[(1, 0)].delivered)
        self.assertEqual(actor._held[(1, 0)].value['total_tokens'], 2)
        self.assertNotIn([1, 0], [k for page in owner.pages for k in page])

    def test_lost_final_ack_reply_is_retried_by_existing_close_without_resubmission(self):
        actor, owner, transport, tasks, outcome, saved = self.transport([receipt(0)])
        owner.lose_reply = True
        events = self.settled(transport, tasks, outcome)
        self.assertEqual(sum(type(e) is Terminal for e in events), 1)
        self.assertEqual(owner.submissions, 1)
        self.assertEqual([p for p in owner.pages if p], [[[1, 0]], [[1, 0]]])
        self.assertFalse(actor._held)
        self.assertFalse(transport._bulk)

    def test_persistent_ack_rpc_failure_keeps_close_error_and_actor_responsibility(self):
        actor, owner, transport, tasks, outcome, saved = self.transport([receipt(0)])
        owner.reject_acks = True
        transport.backend._slots[tasks[0].key].future.result(3)
        with self.assertRaisesRegex(RuntimeError, 'transport finalization failed'):
            transport.backend.close(3, wait_pending=True)
        self.assertEqual(set(saved), {TaskKey(1, 0)})
        self.assertEqual(set(actor._held), {(1, 0)})
        self.assertFalse(actor._held[(1, 0)].delivered)
        self.assertEqual(set(transport._bulk[BatchKey(1, 0)]['acks']), {TaskKey(1, 0)})
        self.assertEqual(owner.submissions, 1)


class BatchPollAckRpcTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_poll_keeps_real_rpc_permit_and_ack_intent_until_remote_terminal(self):
        from unittest.mock import patch
        from tests.execution_provider.test_lotus_rpc_capacity import fixture
        owner, actor = fixture(1)
        prepared = snapshot([receipt(0)])
        actor._held, actor._tickets = prepared._held, prepared._tickets
        actor.poll_prepared_batch((1, 0))
        entered, release = threading.Event(), threading.Event()
        original = actor.poll_prepared_batch
        task = None
        def blocked(*args):
            entered.set()
            if not release.wait(3):
                raise TimeoutError('test did not release its RPC')
            return original(*args)
        try:
            with patch.object(actor, 'poll_prepared_batch', blocked):
                task = asyncio.create_task(owner.wait_prepared_batch((1, 0), (TaskKey(1, 0),)))
                self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual((owner._rpc_inflight, owner._rpc_pending), (1, 1))
                self.assertIn((1, 0), actor._held)
                report = owner._rpc_cleanup_status(dict(status='closed', cleanup_errors=[]),
                    deadline=time.monotonic())
                self.assertEqual(report['status'], 'unknown')
                self.assertIn('data_rpc_terminal_unconfirmed', report['cleanup_errors'])
                # Snapshot remains independent of the occupied data RPC.
                await asyncio.wrap_future(owner.model_actor.snapshot.remote().future())
                release.set()
                remote = next(f for name, f in owner.model_actor.calls if name == 'poll_prepared_batch')
                await asyncio.wrap_future(remote)
                self.assertEqual((owner._rpc_inflight, owner._rpc_pending), (0, 0))
                self.assertFalse(actor._held)
        finally:
            release.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            actor.close(1)
            owner.model_actor.pool.shutdown()


class BatchPollAckCloseTests(unittest.TestCase):
    def owner_and_core(self, compute=False):
        calls = []
        def close_actor():
            calls.append('actor_close')
            return dict(cleanup_errors=[], unconsumed_core_results=[])
        actors = SimpleNamespace(close=close_actor)
        core = SimpleNamespace(capacity=SimpleNamespace(records={TaskKey(1, 0):SimpleNamespace(compute=compute)}),
                               reap=lambda n:None)
        return actors, core, calls

    def test_final_known_result_ack_drains_before_actor_kill_with_remaining_cleanup_deadline(self):
        actors, core, calls = self.owner_and_core()
        deadline = time.monotonic()+2
        def close_transport(*, timeout, wait_pending):
            self.assertTrue(wait_pending)
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, 2)
            calls.append('transport_drain')
            return True
        transport = SimpleNamespace(_batch_poll_ack=True, backend=SimpleNamespace(close=close_transport),
            recover_actor_receipts=lambda values:[])
        result = close_prepared_core(actors, transport, core, SimpleNamespace(step_actions=1), deadline=deadline)
        self.assertEqual(calls, ['transport_drain','actor_close','transport_drain'])
        self.assertTrue(result['transport_closed'])
        self.assertFalse(result['cleanup_errors'])

    def test_inflight_or_unknown_compute_still_stops_producer_before_waiting_for_delivery(self):
        actors, core, calls = self.owner_and_core(compute=True)
        transport = SimpleNamespace(_batch_poll_ack=True, backend=SimpleNamespace(
            close=lambda **kwargs:calls.append('transport_drain') or True), recover_actor_receipts=lambda values:[])
        close_prepared_core(actors, transport, core, SimpleNamespace(step_actions=1), deadline=time.monotonic()+2)
        self.assertEqual(calls, ['actor_close','transport_drain'])

    def test_interrupted_final_ack_drain_always_runs_original_actor_cleanup(self):
        actors, core, calls = self.owner_and_core()
        interruption = KeyboardInterrupt('CPU-close-interruption')
        def interrupted(**kwargs):
            raise interruption
        transport = SimpleNamespace(_batch_poll_ack=True, backend=SimpleNamespace(close=interrupted))
        with self.assertRaises(KeyboardInterrupt)as raised:
            close_prepared_core(actors, transport, core, SimpleNamespace(step_actions=1), deadline=time.monotonic()+2)
        self.assertIs(raised.exception, interruption)
        self.assertEqual(calls, ['actor_close'])
