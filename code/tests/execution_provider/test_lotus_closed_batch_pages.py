"""Closed bounded result pages remain ACK-aware and preserve outcome evidence."""
from concurrent.futures import Future
from copy import deepcopy
from dataclasses import asdict, replace
import json
import time
from types import SimpleNamespace
import unittest

from src.execution_provider.adapters import lotus_records_async_actor as actors
from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
from src.execution_provider.adapters.lotus_chain_token_budget import audit_chain_call
from src.execution_provider.adapters.lotus_records_core import _encode
from src.scheduling.core.session_contract import Acceptance, BatchKey, BatchMember, Terminal, Uncertain
from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
from src.serving.backends.vllm_engine import GenerationSpec
from tests.execution_provider.test_lotus_async_batch_lifecycle import actor
from tests.scheduling.test_async_backend import task as backend_task


class ClosedBatchPagesTests(unittest.TestCase):
    def calls(self):
        result = []
        for i in range(3):
            row = stage_identity('q', RecordOccurrence('r'+str(i), i, 0, {}), 'stage')
            row['messages_json'] = json.dumps([dict(role='user', content='complete prompt')])
            result.append(dict(row=row, generation=asdict(GenerationSpec()), maximum=8192, request_key=[0,i]))
        return result

    def completed(self, identity, status='succeeded'):
        result = actors.result_record(identity, engine_uid='CPU-pages', start=1., end=2., status=status, submitted=True)
        result.update(text='x'*2700, prompt_token_ids_json='[11]', output_token_ids_json='[12]',
            prompt_tokens=1, completion_tokens=1, total_tokens=2, finish_reason='stop',
            engine_request_id='CPU-native/'+identity['row_id'], engine_terminal_confirmed=status!='unknown')
        audit_chain_call(identity, result)
        return result

    def close_actor(self, calls, *, producer_stopped=False, unknown=False, single_pages=False):
        value, uid = actor(), 'f'*32
        value.limits = replace(value.limits, max_inflight=3, result_slots=3, result_bytes=3*4096)
        for i, call in enumerate(calls):
            identity = {n:call['row'][n] for n in actors.IDENTITY_FIELDS}
            result = self.completed(identity, 'unknown' if unknown else 'succeeded')
            if single_pages:
                result['text'] += 'x'*(4090-len(_encode(result,100000)))
            self.assertLessEqual(len(_encode(result, 100000)), value.limits.item_result_bytes)
            ticket = uid+':'+str(i)
            key = tuple(call['request_key'])
            value._held[key] = actors._Call(identity, ticket=ticket, value=result,
                confirmed=not unknown, batch_key=(0,0), chain_call=True)
            value._tickets[ticket] = key
        proof = dict(schema='semloom.async_producer_quiescence.v1', confirmed=True, owner_uid=uid,
                     engine_uid='CPU-pages', issued_submissions=3)
        report = dict(status='closed', **(dict(producer_quiescence=proof) if producer_stopped else {}))
        value.engine = SimpleNamespace(poll=lambda maximum: (), close=lambda **options: report,
            ready_report=dict(engine_uid='CPU-pages'),
            snapshot=lambda:dict(owner_uid=uid, issued_submissions=3))
        value.close(1)
        return value

    def test_closed_pages_advance_after_ack_with_or_without_borrowing_ledger(self):
        for stopped in (False, True):
            with self.subTest(producer_stopped=stopped):
                value = self.close_actor(self.calls(), producer_stopped=stopped)
                audit = deepcopy(value.last_close_report)
                if stopped:
                    self.assertEqual(value._held, {})
                seen = set()
                for page in range(2):
                    raw = value.poll_prepared_batch((0,0))
                    self.assertEqual(value.poll_prepared_batch((0,0)), raw, 'lost reply is readable until ACK')
                    self.assertLessEqual(len(raw), value.limits.item_result_bytes+value.limits.control_frame_bytes)
                    packet = json.loads(raw)
                    self.assertTrue(packet['closed'])
                    self.assertEqual(packet['more_results'], page==0)
                    for receipt in packet['results']:
                        key = tuple(receipt['request_key'])
                        self.assertNotIn(key, seen); seen.add(key)
                        self.assertTrue(value.acknowledge_core(key))
                self.assertEqual(seen, {(0,0),(0,1),(0,2)})
                self.assertEqual(json.loads(value.poll_prepared_batch((0,0))),
                                 dict(results=[], closed=True, more_results=False))
                self.assertEqual(value.last_close_report, audit)
                repeated = value.close(1)
                self.assertEqual(repeated['unconsumed_core_results'], audit['unconsumed_core_results'],
                                 'a close retry must preserve receipts already acknowledged for delivery')

    def run_transport(self, missing=False, drop_first_ack=False):
        value = [None]
        ack_attempts = {}
        def ready(packet):
            future = Future(); future.set_result(packet)
            return SimpleNamespace(future=lambda: future)
        def submit(calls, key, deadline):
            value[0] = self.close_actor(calls[:2] if missing else calls, single_pages=drop_first_ack)
            return ready(dict(status='accepted'))
        def acknowledge(key):
            self.assertIn(key, transferred, 'ACK must follow local validation and result transfer')
            pair = key.session_id,key.sequence
            ack_attempts[pair] = ack_attempts.get(pair,0)+1
            if drop_first_ack and pair==(0,0) and ack_attempts[pair]==1:
                return False  # The first ACK never reaches the actor; the result did.
            return value[0].acknowledge_core(pair)
        owner = SimpleNamespace(cancel_core=lambda key: None,
            acknowledge_core=acknowledge,
            submit_prepared_batch=submit, poll_prepared_batch=lambda key: ready(value[0].poll_prepared_batch(key)))
        transferred = []
        transport = LotusChainActorBackend(owner, 3, lambda: None, deadline_monotonic=time.monotonic()+5,
            on_result=lambda key, value: transferred.append(key), bulk=True)
        transport.poll_interval_s = .001
        tasks, group = [], BatchKey(0,0)
        for i, call in enumerate(self.calls()):
            base = backend_task(i)
            tasks.append(replace(base, member=BatchMember(group,i,3), task=replace(base.task,
                payload=json.dumps(dict(row=call['row'], generation=call['generation'], parent_call_ids=[])).encode(),
                max_result_bytes=8192, profile_name='stage')))
        try:
            submitted = transport.try_submit_batch(tuple(tasks), 'endpoint')
            self.assertEqual(submitted.acceptance, Acceptance.ACCEPTED)
            until = time.monotonic()+3
            while not all(transport.backend._slots[t.key].future.done() for t in tasks):
                self.assertLess(time.monotonic(), until)
                time.sleep(.001)
            events = transport.poll(tuple((t.key,h) for t,h in zip(tasks,submitted.handles)), 3)
            self.assertEqual(sum(type(e) is Terminal for e in events), 2 if missing else 3)
            self.assertEqual(sum(type(e) is Uncertain for e in events), int(missing))
            self.assertEqual(len(transferred), 2 if missing else 3)
            self.assertEqual(transport.retained_refs(), int(missing))
            self.assertEqual(len(value[0].last_close_report['unconsumed_core_results']), 2 if missing else 3)
            if drop_first_ack:
                self.assertGreaterEqual(ack_attempts[(0,0)], 2)
        finally:
            if value[0] is not None:
                for key in tuple(value[0]._closed_pending):
                    value[0].acknowledge_core(key)
            self.assertTrue(transport.backend.close(3, wait_pending=True))

    def test_closed_transport_drains_successful_later_pages(self):
        self.run_transport()

    def test_closed_transport_retries_a_lost_ack_only_after_validated_transfer(self):
        self.run_transport(drop_first_ack=True)

    def test_closed_transport_retains_uncertainty_for_a_genuinely_missing_member(self):
        self.run_transport(missing=True)

    def test_closed_unknown_ack_advances_delivery_without_refunding_borrowing(self):
        value = self.close_actor(self.calls()[:1], unknown=True)
        audit = deepcopy(value.last_close_report)
        packet = json.loads(value.poll_prepared_batch((0,0)))
        self.assertEqual(packet['results'][0]['result']['status'], 'unknown')
        self.assertFalse(value.acknowledge_core((0,0)))
        self.assertEqual(len(value._held), 1)
        self.assertFalse(value._held[(0,0)].confirmed)
        self.assertEqual(json.loads(value.poll_prepared_batch((0,0)))['results'], [])
        self.assertEqual(value.last_close_report, audit)
        value.close(1)
        self.assertEqual(json.loads(value.poll_prepared_batch((0,0)))['results'], [])
