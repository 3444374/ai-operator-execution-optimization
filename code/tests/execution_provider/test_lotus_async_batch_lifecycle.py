"""Deterministic bulk handoff/receipt boundaries, without Ray or model execution."""
import json
from concurrent.futures import Future
from copy import deepcopy
from dataclasses import asdict, replace
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_async_actor as actors
from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
from src.execution_provider.adapters.lotus_records_core import _encode
from src.scheduling.core.session_contract import Acceptance, BatchKey, BatchMember
from src.scheduling.runtime import async_backend
from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
from src.serving.backends.async_engine_process import BatchSubmitResult, TerminalEvent
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import GenerationSpec
from tests.scheduling.test_async_backend import task as backend_task


def actor():
    value = object.__new__(actors.AsyncCoreModelActor)
    value.limits = AsyncEngineLimits(max_inflight=2, result_slots=2,
        item_result_bytes=4096, result_bytes=8192, control_frame_bytes=4096)
    value._condition = threading.Condition()
    value._close_lock = threading.Lock()
    value._held, value._tickets, value._early_cancels = {}, {}, set()
    value._session = value._cancel_session = -1
    value._readers = 0
    value._closing = value._unhealthy = False
    value.engine = None
    value.last_close_report = None
    value._closed_pending = None
    return value


class BatchLifecycleTests(unittest.TestCase):
    def member_result_actor(self, *, unknown=False, producer_stopped=False):
        value, events, uid = actor(), [], 'f'*32
        calls = []
        for i, maximum in enumerate((2048, 4096)):
            row = stage_identity('q', RecordOccurrence('r'+str(i), i, 0, {}), 'stage')
            row['messages_json'] = json.dumps([dict(role='user', content='complete prompt')])
            calls.append(dict(row=row, generation=asdict(GenerationSpec()), maximum=maximum,
                              request_key=[0, i]))
        def submit(rows, **options):
            tickets = []
            for i, row in enumerate(rows):
                ticket = uid+':'+str(i)
                identity = {name: row[name] for name in actors.IDENTITY_FIELDS}
                result = actors.result_record(identity, engine_uid='CPU-member-limit', start=1., end=2.,
                    status='unknown' if unknown else 'succeeded', submitted=True)
                result.update(text='x'*2700, prompt_token_ids_json='[11]', output_token_ids_json='[12]',
                    prompt_tokens=1, completion_tokens=1, total_tokens=2, finish_reason='stop',
                    engine_request_id=ticket, engine_terminal_confirmed=not unknown)
                self.assertGreater(len(_encode(result, 100000)), 2048)
                self.assertLessEqual(len(_encode(result, 100000)), 4096)
                events.append(TerminalEvent(ticket, identity, result, not unknown, None))
                tickets.append(ticket)
            return BatchSubmitResult('queued', tuple(tickets))
        proof = dict(schema='semloom.async_producer_quiescence.v1', confirmed=True, owner_uid=uid,
                     engine_uid='CPU-member-limit', issued_submissions=2)
        value.engine = SimpleNamespace(submit_batch=submit,
            poll=lambda maximum: tuple(events.pop(0) for _ in range(min(maximum, len(events)))),
            close=lambda **options: dict(status='closed',
                **(dict(producer_quiescence=proof) if producer_stopped else {})),
            ready_report=dict(engine_uid='CPU-member-limit'),
            snapshot=lambda: dict(owner_uid=uid, issued_submissions=2))
        payload = _encode(dict(calls=calls, batch_key=[0, 0], deadline=time.monotonic()+3),
                          value.limits.input_bytes)
        self.assertEqual(value.submit_prepared_batch(payload), dict(status='accepted'))
        return value

    def assert_member_results(self, receipts, *, unknown=False):
        by_key = {tuple(item['request_key']): item['result'] for item in receipts}
        small, large = by_key[(0, 0)], by_key[(0, 1)]
        self.assertLessEqual(len(_encode(small, 100000)), 2048)
        self.assertLessEqual(len(_encode(large, 100000)), 4096)
        self.assertEqual(small['status'], 'unknown' if unknown else 'failed')
        self.assertEqual(small['error_type'], 'ResultCapacityExceeded')
        self.assertEqual(large['status'], 'unknown' if unknown else 'succeeded')
        self.assertEqual(large['text'], 'x'*2700)
        for result in (small, large):
            self.assertEqual((result['prompt_tokens'], result['completion_tokens'], result['total_tokens']),
                             (1, 1, 2))
            self.assertTrue(result['submission_confirmed'])

    def test_bulk_members_keep_independent_result_reservations(self):
        for unknown in (False, True):
            with self.subTest(unknown=unknown):
                value = self.member_result_actor(unknown=unknown)
                raw = value.poll_prepared_batch((0, 0))
                self.assertEqual(value.poll_prepared_batch((0, 0)), raw, 'no ACK means no delivery')
                self.assert_member_results(json.loads(raw)['results'], unknown=unknown)
                self.assertTrue(all(call.confirmed is not unknown for call in value._held.values()))
                for key in ((0, 0), (0, 1)):
                    self.assertIs(value.acknowledge_core(key), not unknown)
                self.assertEqual(len(value._held), 2 if unknown else 0)

    def test_closed_member_bounds_survive_producer_retirement_and_repeated_close(self):
        for stopped in (False, True):
            for unknown in (False, True):
                with self.subTest(producer_stopped=stopped, unknown=unknown):
                    value = self.member_result_actor(unknown=unknown, producer_stopped=stopped)
                    report = value.close(1)
                    audit = deepcopy(report)
                    self.assert_member_results(report['unconsumed_core_results'], unknown=unknown)
                    if stopped:
                        self.assertEqual(value._held, {})
                    packet = json.loads(value.poll_prepared_batch((0, 0)))
                    self.assertTrue(packet['closed'])
                    self.assert_member_results(packet['results'], unknown=unknown)
                    for key in ((0, 0), (0, 1)):
                        self.assertIs(value.acknowledge_core(key), stopped or not unknown)
                    self.assertEqual(json.loads(value.poll_prepared_batch((0, 0)))['results'], [])
                    self.assertEqual(value.close(1)['unconsumed_core_results'],
                                     audit['unconsumed_core_results'])

    def test_rejected_unsubmitted_members_obey_the_checked_result_reservation(self):
        for started in (False, True):
            with self.subTest(engine_started=started):
                value = actor()
                if started:
                    value.engine = SimpleNamespace(ready_report={},
                        submit_batch=lambda *args, **options: SimpleNamespace(status='rejected'))
                row = stage_identity('q', RecordOccurrence('r0', 0, 0, {}), 'stage')
                row['messages_json'] = json.dumps([dict(role='user', content='complete prompt')])
                call = dict(row=row, generation=asdict(GenerationSpec()), maximum=512, request_key=[0, 0])
                payload = _encode(dict(calls=[call], batch_key=[0, 0], deadline=time.monotonic()+3),
                                  value.limits.input_bytes)
                response = value.submit_prepared_batch(payload)
                self.assertEqual(response['status'], 'rejected')
                result = response['results'][0]['result']
                self.assertLessEqual(len(_encode(result, 100000)), 512)
                self.assertEqual(result['status'], 'failed')
                self.assertEqual(result['error_type'], 'ResultCapacityExceeded')
                self.assertFalse(result['submission_confirmed'])
                self.assertEqual(value._held, {})

    def test_close_collects_terminals_while_abort_handoff_was_temporarily_unavailable(self):
        value, polls, aborts, closes = actor(), [0], {}, []
        events = []
        def poll(maximum):
            polls[0] += 1
            if polls[0] < 3:
                return ()
            result = tuple(events); events.clear()
            return result
        def abort(ticket):
            aborts[ticket] = aborts.get(ticket, 0)+1
            if aborts[ticket] > 1:
                raise RuntimeError('terminal must be collected instead of retrying its stale abort')
            return False
        value.engine = SimpleNamespace(poll=poll, abort=abort, ready_report=dict(engine_uid='CPU-lifecycle'),
            close=lambda **options: closes.append(options) or dict(status='closed'))
        for i, status in enumerate(('unknown', 'failed')):
            identity = stage_identity('q', RecordOccurrence('r'+str(i), i, 0, {}), 'stage', model_role='main')
            key, ticket = (1, i), 'ticket-'+str(i)
            value._held[key] = actors._Call(identity, ticket=ticket, chain_call=True, batch_key=(1, 0))
            value._tickets[ticket] = key
            receipt = value._receipt(identity, status, 'CPUFixtureOutcome')
            events.append(TerminalEvent(ticket, identity, receipt, status != 'unknown', None))
        report = value.close(1)
        self.assertEqual(report['status'], 'closed', report)
        self.assertEqual(len(closes), 1)
        self.assertTrue(all(n == 1 for n in aborts.values()))
        self.assertEqual([r['status'] for r in report['unconsumed_call_results']], ['unknown', 'failed'])
        self.assertEqual(len(value._held), 2, 'no producer proof means borrowing remains held')
        self.assertFalse(value._held[(1, 0)].confirmed)

    def test_completed_batch_retains_a_late_member_until_its_local_transfer(self):
        calls, transferred = [], []
        def ready(value):
            future = Future(); future.set_result(value)
            return SimpleNamespace(future=lambda: future)
        def submit(values, key, deadline):
            calls.extend(values)
            return ready(dict(status='accepted'))
        def poll(key):
            return ready(json.dumps(dict(results=[dict(request_key=c['request_key'],
                result=dict(c['row'], status='succeeded')) for c in calls], closed=False)).encode())
        owner = SimpleNamespace(cancel_core=lambda key: None, acknowledge_core=lambda key: None,
            submit_prepared_batch=submit, poll_prepared_batch=poll)
        transport = LotusChainActorBackend(owner, 2, lambda: None,
            deadline_monotonic=time.monotonic()+5, on_result=lambda key, value: transferred.append(key), bulk=True)
        transport._audit_result = lambda expected, value: None
        group, tasks = BatchKey(0, 0), []
        for i in range(2):
            row = stage_identity('q', RecordOccurrence('r'+str(i), i, 0, {}), 'stage', model_role='main')
            row['messages_json'] = json.dumps([dict(role='user', content='prompt')])
            base = backend_task(i)
            tasks.append(replace(base, member=BatchMember(group, i, 2), task=replace(base.task,
                payload=json.dumps(dict(row=row, generation=asdict(GenerationSpec()), parent_call_ids=[])).encode(),
                max_result_bytes=8192, profile_name='stage')))
        original = async_backend.asyncio.run_coroutine_threadsafe
        scheduled = [0]
        def schedule(coroutine, loop):
            scheduled[0] += 1
            if scheduled[0] == 2:
                deadline = time.monotonic()+3
                while not (transport._bulk[group]['driver_done'] and tasks[0].key in transferred):
                    if time.monotonic() > deadline:
                        raise TimeoutError('first member did not finish the shared driver')
                    time.sleep(.001)
                self.assertEqual(transferred, [tasks[0].key])
                context = transport._bulk[group]
                self.assertEqual(set(context['futures']), {tasks[1].key})
                self.assertFalse(context['tasks']); self.assertFalse(context['values'])
            return original(coroutine, loop)
        try:
            with patch.object(async_backend.asyncio, 'run_coroutine_threadsafe', schedule):
                result = transport.try_submit_batch(tuple(tasks), 'endpoint')
            self.assertEqual(result.acceptance, Acceptance.ACCEPTED)
            for task in tasks:
                self.assertEqual(json.loads(transport.backend._slots[task.key].future.result(3))['row_id'],
                                 'r'+str(task.key.sequence))
            self.assertEqual(transferred, [t.key for t in tasks])
            self.assertEqual(transport._bulk, {})
        finally:
            self.assertTrue(transport.backend.close(3, wait_pending=True))

    def test_cancel_during_zero_acceptance_retry_preserves_exact_ticket_ownership(self):
        value, entered, available = actor(), threading.Event(), threading.Event()
        submitted, aborted, response, errors = [], [], [], []
        def submit(rows, **options):
            entered.set()
            if not available.is_set():
                return BatchSubmitResult('backpressure')
            submitted.extend(r['row_id'] for r in rows)
            return BatchSubmitResult('queued', tuple('ticket-'+r['row_id'] for r in rows))
        value.engine = SimpleNamespace(submit_batch=submit, ready_report=dict(engine_uid='CPU-lifecycle'),
            abort=lambda key: aborted.append(key) or True)
        calls = []
        for i in range(2):
            row = stage_identity('q', RecordOccurrence('r'+str(i), i, 0, {}), 'stage', model_role='main')
            row['messages_json'] = json.dumps([dict(role='user', content='complete prompt')])
            calls.append(dict(row=row, generation=asdict(GenerationSpec()), maximum=8192, request_key=[1, i]))
        payload = _encode(dict(calls=calls, batch_key=[1, 0], deadline=time.monotonic()+3), value.limits.input_bytes)
        def run():
            try: response.append(value.submit_prepared_batch(payload))
            except BaseException as error: errors.append(error)
        thread = threading.Thread(target=run)
        thread.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assertTrue(value.abort_core((1, 0))['requested'])
            available.set()
            with value._condition: value._condition.notify_all()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(response, [dict(status='accepted')])
            self.assertEqual(submitted, ['r1'])
            self.assertIsNone(value._held[(1, 0)].ticket)
            self.assertFalse(value._held[(1, 0)].value['submission_confirmed'])
            self.assertEqual(value._held[(1, 1)].ticket, 'ticket-r1')
            self.assertEqual(value._tickets, {'ticket-r1': (1, 1)})
            value.abort_core((1, 1))
            self.assertEqual(aborted, ['ticket-r1'])
        finally:
            available.set()
            with value._condition: value._condition.notify_all()
            thread.join(3)

    def boundary_actor(self):
        value = actor()
        maximum = value.limits.item_result_bytes + value.limits.control_frame_bytes
        for size in range(3600, 4090):
            candidates = [dict(request_key=[1, i], result=dict(text='x'*size)) for i in range(2)]
            old_size = len(_encode(dict(results=candidates), 100000))
            new_size = len(_encode(dict(results=candidates, closed=False), 100000))
            if old_size <= maximum < new_size:
                break
        else:
            self.fail('could not construct the exact response envelope boundary')
        for i, candidate in enumerate(candidates):
            self.assertLessEqual(len(_encode(candidate['result'], 100000)), value.limits.item_result_bytes)
            value._held[(1, i)] = actors._Call({}, value=candidate['result'], confirmed=True, batch_key=(1, 0))
        return value, maximum

    def test_full_poll_envelope_is_bounded_and_reply_remains_readable_until_ack(self):
        value, maximum = self.boundary_actor()
        first = value.poll_prepared_batch((1, 0))
        self.assertLessEqual(len(first), maximum)
        packet = json.loads(first)
        self.assertEqual(len(packet['results']), 1)
        self.assertFalse(any(c.delivered for c in value._held.values()))
        self.assertEqual(value.poll_prepared_batch((1, 0)), first)  # Lost reply has no ACK.
        key = tuple(packet['results'][0]['request_key'])
        self.assertTrue(value.acknowledge_core(key))
        second = json.loads(value.poll_prepared_batch((1, 0)))
        other = tuple(second['results'][0]['request_key'])
        self.assertNotEqual(other, key)
        self.assertTrue(value.acknowledge_core(other))
        self.assertEqual(value._held, {})

    def test_failed_final_envelope_cannot_commit_any_delivery_flags(self):
        value, _ = self.boundary_actor()
        def encode(packet, maximum):
            if 'closed' in packet:
                raise ValueError('injected full-envelope encoding failure')
            return _encode(packet, maximum)
        with patch.object(actors, '_encode', encode), self.assertRaises(ValueError):
            value.poll_prepared_batch((1, 0))
        self.assertFalse(any(c.delivered or getattr(c, 'poll_exposed', False) for c in value._held.values()))
