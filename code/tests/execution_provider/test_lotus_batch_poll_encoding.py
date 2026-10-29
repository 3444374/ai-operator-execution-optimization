"""Byte-exact pagination and bounded encoding work against the frozen 41ce oracle."""
from copy import deepcopy
from dataclasses import replace
import gc
import hashlib
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import weakref

from src.execution_provider.adapters import lotus_records_async_actor as actors
from src.execution_provider.adapters.lotus_records_core import _encode
from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
from src.serving.backends.async_engine_process import TerminalEvent
from tests.execution_provider.test_lotus_async_batch_lifecycle import actor

_request_key = actors._request_key

# Frozen 41ce poll; a test oracle only, never imported by production.
def legacy_poll(self, batch_key):
    batch_key = _request_key(batch_key)
    with self._condition:
        if self.engine is not None and not self._closing:
            self._collect()
        if self._closing and self.last_close_report is not None:
            available = [(tuple(v['request_key']), v['result']) for v in
                self.last_close_report.get('unconsumed_core_results', [])
                if tuple(v.get('batch_key', ())) == batch_key and tuple(v['request_key']) in self._closed_pending]
        else:
            available = [(key, call.value) for key, call in self._held.items()
                         if call.batch_key == batch_key and call.value is not None and not call.delivered]
        results, exposed = [], []
        maximum = self.limits.item_result_bytes + self.limits.control_frame_bytes
        closed = self.last_close_report is not None
        for key, value in available:
            call = self._held.get(key)
            value = self._bounded_call_result(value, None if call is None else call.max_result_bytes)
            candidate = dict(request_key=list(key), result=value)
            try:
                _encode(dict(results=results+[candidate], closed=closed, more_results=False), maximum)
            except ValueError:
                if not results:
                    raise
                break
            results.append(candidate)
            exposed.append(key)
        payload = _encode(dict(results=results, closed=closed, more_results=len(results)<len(available)), maximum)
        for key in exposed:
            if key in self._held:
                self._held[key].poll_exposed = True
            if self._closed_pending is not None and key in self._closed_pending:
                self._closed_pending[key] = True
        return payload


def receipt(index, text='x'*2700, status='succeeded'):
    identity = stage_identity('q', RecordOccurrence('r'+str(index), index, index, {}), 'stage')
    value = actors.result_record(identity, engine_uid='CPU-page', start=1., end=2.,
        status=status, submitted=True)
    value.update(text=text, prompt_token_ids_json='[11]', output_token_ids_json='[12]',
        prompt_tokens=1, completion_tokens=1, total_tokens=2, finish_reason='stop',
        engine_request_id='CPU-native/'+str(index), engine_terminal_confirmed=status!='unknown')
    return value


def snapshot(values, *, closed=False, stopped=False, maximum=None, large=False):
    value = actor()
    count = max(1,len(values))
    item = 2*1024**2 if large else 4096
    value.limits = replace(value.limits, max_inflight=count, result_slots=count,
        result_bytes=count*item, item_result_bytes=item)
    uid = 'f'*32
    for i,result in enumerate(values):
        identity = {k:result[k] for k in actors.IDENTITY_FIELDS}
        ticket = uid+':'+str(i)
        value._held[(1,i)] = actors._Call(identity, ticket=ticket, value=deepcopy(result),
            confirmed=result['status']!='unknown', batch_key=(1,0), chain_call=True,
            max_result_bytes=maximum if maximum is not None else item)
        value._tickets[ticket] = (1,i)
    if closed:
        proof = dict(schema='semloom.async_producer_quiescence.v1', confirmed=True,
            owner_uid=uid, engine_uid='CPU-page', issued_submissions=count)
        value.engine = SimpleNamespace(poll=lambda maximum:(),ready_report=dict(engine_uid='CPU-page'),
            snapshot=lambda:dict(owner_uid=uid,issued_submissions=count),
            close=lambda **options:dict(status='closed',
                **(dict(producer_quiescence=proof) if stopped else {})))
        value.close(1)
    return value


class PollEncodingTests(unittest.TestCase):
    def test_busy_129_member_page_has_linear_encoding_work(self):
        value=snapshot([receipt(i,text='full 雪\n"\\'*130) for i in range(129)],large=True)
        frames=[];sizes=[]
        def counted(packet,maximum):
            raw=_encode(packet,maximum);sizes.append(len(raw))
            if 'results' in packet and packet['results']:frames.append(len(packet['results']))
            return raw
        with patch.object(actors,'_encode',counted):raw=value.poll_prepared_batch((1,0))
        self.assertEqual(len(json.loads(raw)['results']),129)
        self.assertLessEqual(sum(sizes),3*len(raw),
            f'{len(sizes)} encodes emitted {sum(sizes)} bytes for {len(raw)} wire bytes')
        self.assertEqual(frames,[129],'only the final nonempty full-frame encode is necessary')

    def test_open_closed_pages_partial_ACK_and_small_member_limits_match_41ce_bytes(self):
        for closed,stopped in ((False,False),(True,False),(True,True)):
            for maximum in (2048,4096):
                with self.subTest(closed=closed,stopped=stopped,member_maximum=maximum):
                    values=[receipt(i,text=('a雪\n"\\'*600 if i%2 else 'x'*2700),
                        status='unknown' if i==2 else 'succeeded') for i in range(5)]
                    left,right=(snapshot(values,closed=closed,stopped=stopped,maximum=maximum) for _ in range(2))
                    audit=deepcopy(right.last_close_report)
                    seen=[]
                    for page in range(7):
                        expected=legacy_poll(left,(1,0));actual=right.poll_prepared_batch((1,0))
                        self.assertEqual(actual,expected)
                        self.assertEqual(hashlib.sha256(actual).digest(),hashlib.sha256(expected).digest())
                        self.assertLessEqual(len(actual),right.limits.item_result_bytes+right.limits.control_frame_bytes)
                        self.assertEqual(right.poll_prepared_batch((1,0)),actual,'lost reply has no ACK')
                        rows=json.loads(actual)['results']
                        if not rows:break
                        for row in rows:self.assertLessEqual(len(_encode(row['result'],100000)),maximum)
                        key=tuple(rows[0]['request_key']);seen.append(key)
                        self.assertEqual(left.acknowledge_core(key),right.acknowledge_core(key))
                        if closed:
                            self.assertEqual(right.close(1)['unconsumed_core_results'],audit['unconsumed_core_results'])
                    else:self.fail('bounded ACK sequence did not drain')
                    self.assertEqual(seen,[(1,i) for i in range(5)])
                    self.assertEqual(set(left._held),set(right._held))
                    if not stopped:
                        self.assertEqual(set(right._held),{(1,2)})
                        self.assertFalse(right._held[(1,2)].confirmed)
                    else:self.assertEqual(right._held,{})

    def test_true_flag_one_byte_cannot_expand_the_legacy_page(self):
        value=snapshot([receipt(i,text='') for i in range(3)])
        maximum=value.limits.item_result_bytes+value.limits.control_frame_bytes
        candidates=[dict(request_key=[1,i],result=value._held[(1,i)].value) for i in range(2)]
        extra=maximum+1-len(_encode(dict(results=candidates,closed=False,more_results=False),100000))
        for i in range(2):value._held[(1,i)].value['text']='x'*(extra//2+(extra%2 if i else 0))
        self.assertTrue(all(len(_encode(c['result'],100000))<=4096 for c in candidates))
        self.assertEqual(len(_encode(dict(results=candidates,closed=False,more_results=False),100000)),maximum+1)
        self.assertEqual(len(_encode(dict(results=candidates,closed=False,more_results=True),maximum)),maximum)
        raw=value.poll_prepared_batch((1,0));packet=json.loads(raw)
        self.assertEqual(len(packet['results']),1)
        self.assertTrue(packet['more_results'])
        other=snapshot([receipt(i,text=value._held[(1,i)].value['text']) for i in range(3)])
        self.assertEqual(raw,legacy_poll(other,(1,0)))

    def test_nonempty_frame_encoding_failure_never_exposes_results(self):
        value=snapshot([receipt(i,text='ok') for i in range(2)])
        def fail(packet,maximum):
            if packet.get('results'):raise ValueError('injected complete frame encoding failure')
            return _encode(packet,maximum)
        with patch.object(actors,'_encode',fail),self.assertRaises(ValueError):value.poll_prepared_batch((1,0))
        self.assertFalse(any(c.poll_exposed or c.delivered for c in value._held.values()))
        self.assertEqual(value.poll_prepared_batch((1,0)),legacy_poll(snapshot([receipt(i,text='ok') for i in range(2)]),(1,0)))

    def test_later_member_encoding_error_never_commits_a_partial_exposure(self):
        value=snapshot([receipt(i,text='ok') for i in range(2)])
        value._held[(1,1)].value['text']=object()
        with self.assertRaises(TypeError):value.poll_prepared_batch((1,0))
        self.assertFalse(any(c.poll_exposed or c.delivered for c in value._held.values()))

    def test_ACK_releases_original_receipt_objects_without_a_page_cache(self):
        class Payload(dict):pass
        value=snapshot([]);refs=[]
        for i in range(3):
            payload=Payload(receipt(i,text='retained'))
            refs.append(weakref.ref(payload))
            value._held[(1,i)]=actors._Call({},value=payload,confirmed=True,batch_key=(1,0))
        del payload
        fields=set(vars(value))
        raw=value.poll_prepared_batch((1,0))
        self.assertEqual(len(json.loads(raw)['results']),3)
        self.assertTrue(all(ref() is not None for ref in refs))
        for i in range(3):self.assertTrue(value.acknowledge_core((1,i)))
        gc.collect()
        self.assertTrue(all(ref() is None for ref in refs))
        self.assertEqual(set(vars(value)),fields)
        self.assertEqual(value._held,{})

    def test_cancellation_late_confirmed_or_unknown_result_keeps_its_responsibility(self):
        for status in ('succeeded','unknown'):
            with self.subTest(status=status):
                pair=[snapshot([receipt(0,text='early'),receipt(1,text='late',status=status)]) for _ in range(2)]
                for value in pair:
                    value._held[(1,1)].value=None;value._held[(1,1)].confirmed=False
                    events=[];value._test_events=events
                    value.engine=SimpleNamespace(poll=lambda maximum,q=events:tuple(q.pop(0) for _ in range(min(len(q),maximum))),
                        abort=lambda ticket:True)
                    self.assertTrue(value.abort_core((1,1))['requested'])
                self.assertEqual(legacy_poll(pair[0],(1,0)),pair[1].poll_prepared_batch((1,0)))
                for value in pair:
                    self.assertTrue(value.acknowledge_core((1,0)))
                    call=value._held[(1,1)]
                    value._test_events.append(TerminalEvent(call.ticket,call.identity,receipt(1,text='late',status=status),status!='unknown',None))
                self.assertEqual(legacy_poll(pair[0],(1,0)),pair[1].poll_prepared_batch((1,0)))
                for value in pair:
                    self.assertEqual(value._held[(1,1)].confirmed,status!='unknown')
                    self.assertEqual(value.acknowledge_core((1,1)),status!='unknown')
                    self.assertEqual(len(value._held),int(status=='unknown'))
                self.assertEqual(legacy_poll(pair[0],(1,0)),pair[1].poll_prepared_batch((1,0)))
