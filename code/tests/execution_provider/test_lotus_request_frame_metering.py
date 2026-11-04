"""Frozen 72fe organizer replay and exact request-byte metering; no model calls."""
from dataclasses import asdict, replace
import json
import random
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_chain_token_batches as batching
from src.scheduling.core.session_contract import TaskKey
from src.scheduling.organization.batching import PendingBatchBuilder, RowArrival
from src.serving.backends.async_engine_process import _frame
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS, _prepared


# Frozen 72fe2ed9 selection oracle; production never imports this implementation.
class LegacyOrganizer(batching.PreparedTokenOrganizer):
    def __call__(self, candidates):
        owner = self.owner
        active_batches, active_requests = self.usage()
        if not owner.rolling_refill and active_batches >= self.concurrency:
            return ()
        limits = owner._actors.limits
        available = min(owner.resources.max_active_batches-active_requests, limits.max_inflight,
                        limits.result_slots, limits.result_bytes//limits.item_result_bytes)
        if owner.rolling_refill:
            available = min(available, self.request_window-active_requests)
        if available <= 0:
            return ()
        live = owner.core.capacity.records
        self._estimates = {k: v for k, v in self._estimates.items() if k in live}
        builder = PendingBatchBuilder(None, owner.prepared_batching.token_budget)
        chosen, compatibility, wire_bytes, reason = [], None, 0, 'ready_frontier_exhausted'
        for candidate in candidates:
            if candidate.key not in self._estimates:
                value = json.loads(candidate.task.payload)
                row, generation = value['row'], value['generation']
                _, conversations = _prepared([row])
                prompt = batching.chat_token_count(self.tokenizer, conversations[0])
                snapshot = {name: row[name] for name in (*IDENTITY_FIELDS, 'messages_json')}
                item = dict(request_id='f'*32+':'+str((1 << 63)-1), row=snapshot)
                _frame(dict(op='submit', **item, generation=generation, deadline=owner.deadline), limits.item_input_bytes)
                size = len(_frame(item, limits.input_bytes))-4
                header = len(_frame(dict(op='submit_batch', requests=[], generation=generation,
                    deadline=owner.deadline), limits.input_bytes))-4
                rpc_item = len(_frame(dict(row=row, generation=generation,
                    maximum=candidate.task.max_result_bytes,
                    request_key=[candidate.key.session_id, candidate.key.sequence]), limits.input_bytes))-4
                rpc_header = len(_frame(dict(calls=[], batch_key=[candidate.key.session_id, (1 << 64)-1],
                    deadline=owner.deadline), limits.input_bytes))-4
                key = tuple(row[n] for n in ('query_id', 'algorithm_stage', 'model_role')) + (json.dumps(generation, sort_keys=True),)
                self._estimates[candidate.key] = (row['call_id'], prompt, generation['max_tokens'], key,
                    size, header, rpc_item, rpc_header)
            call_id, prompt, output, key, size, header, rpc_item, rpc_header = self._estimates[candidate.key]
            arrival = RowArrival(call_id, 0., prompt, output, '', candidate.key)
            if chosen and key != compatibility:
                reason = 'compatibility'; break
            if builder.would_exceed_token_budget(arrival):
                reason = 'token_budget'; break
            proposed = header+size if not chosen else wire_bytes+1+size
            rpc_proposed = rpc_header+rpc_item if not chosen else rpc_bytes+1+rpc_item
            if max(proposed, rpc_proposed) > min(limits.input_bytes, (1 << 32)-1):
                if not chosen:
                    raise BufferError('complete prepared request exceeds the batch frame allowance')
                reason = 'frame_bytes'; break
            compatibility, wire_bytes, rpc_bytes = key, proposed, rpc_proposed
            chosen.append(candidate.key)
            full = builder.add(arrival)
            if full:
                reason = 'token_budget'; break
            if len(chosen) == available:
                reason = 'request_capacity'; break
        if not chosen:
            return ()
        batch = builder.close()
        self.selected += 1
        self.history = (self.history + [dict(members=len(chosen), tokens=batch.estimated_total_tokens,
            reason=reason, ipc_frame_bytes_upper_bound=wire_bytes, data_rpc_bytes_upper_bound=rpc_bytes,
            **(dict(C_request=self.request_window, requests_before=active_requests)
               if owner.request_feedback is not None else {}))])[-64:]
        return tuple(chosen)


class CountingTokenizer:
    """Deterministic complete-conversation CPU fixture, not a model tokenizer."""
    def __init__(self):
        self.calls = 0

    def apply_chat_template(self, messages, **options):
        self.calls += 1
        return [1] * (7 + sum(len(message['content'].split()) for message in messages))


def candidate(index, *, session=1, text='full 雪🙂\n"\\\t prompt', stage='stage',
              generation=None, extra=None):
    row = dict(query_id='q雪', model_role='primary', algorithm_stage=stage,
        row_id='r'+str(index), source_position=index, call_index=index,
        call_id='call/'+str(index), messages_json=json.dumps([
            dict(role='system', content='保持完整提示'), dict(role='user', content=text)], ensure_ascii=False))
    if extra is not None:
        row['metadata'] = extra
    payload = dict(row=row, generation=asdict(GenerationSpec()) if generation is None else generation)
    return SimpleNamespace(key=TaskKey(session, index),
        task=SimpleNamespace(payload=json.dumps(payload, ensure_ascii=False).encode(), max_result_bytes=8192))


def organizer(candidates, *, legacy=False, B=1000000, R=None, limits=None, rolling=False,
              deadline=123.456, context_length=4096):
    R = max(1, len(candidates)) if R is None else R
    limits = limits or AsyncEngineLimits(max_inflight=R, result_slots=R, result_bytes=R*8192,
        item_result_bytes=8192, input_bytes=64*1024**2, item_input_bytes=2*1024**2)
    # Frame tests declare a finite synthetic model context; no model is started
    # and no native ready receipt is fabricated for the pre-start checks.
    engine = EngineSpec('/tmp/frame-metering-model', sys.executable, sys.executable,
        max_model_len=context_length)
    owner = SimpleNamespace(resources=SimpleNamespace(max_active_batches=R), engine=engine,
        _actors=SimpleNamespace(limits=limits, model_ready=None),
        core=SimpleNamespace(capacity=SimpleNamespace(records={c.key: SimpleNamespace(compute=False, member=None)
            for c in candidates})), prepared_batching=SimpleNamespace(token_budget=B),
        rolling_refill=rolling, request_feedback=None, deadline=deadline)
    return (LegacyOrganizer if legacy else batching.PreparedTokenOrganizer)(owner, CountingTokenizer(), R)


def replay(candidates, **options):
    value = organizer(candidates, **options)
    remaining, trace = list(candidates), []
    while remaining:
        try:
            selected = value(tuple(remaining))
        except (ValueError, BufferError, UnicodeError) as error:
            return trace, (type(error).__name__, str(error), value.tokenizer.calls)
        if not selected:
            return trace, ('blocked',)
        trace.append((selected, value.history[-1]))
        for key in selected:
            value.owner.core.capacity.records.pop(key)
        remaining = remaining[len(selected):]
    return trace, None


class RequestFrameMeteringTests(unittest.TestCase):
    def test_complete_request_still_rejects_explicit_context_overflow(self):
        values = [candidate(0, text='word '*5000)]
        value = organizer(values)
        with self.assertRaisesRegex(ValueError, 'context'):
            value(values)
        self.assertEqual(value.selected, 0)
        self.assertEqual(value._estimates, {})

    def test_omitted_context_without_ready_is_not_treated_as_unlimited(self):
        values = [candidate(0)]
        value = organizer(values, context_length=None)
        with self.assertRaisesRegex(RuntimeError, 'effective model context'):
            value(values)

    def assert_replay(self, candidates, **options):
        expected = replay(candidates, legacy=True, **options)
        self.assertEqual(replay(candidates, **options), expected)
        return expected

    def test_members_order_all_flush_reasons_and_oversize_B_match_frozen_oracle(self):
        values = [candidate(i) for i in range(12)]
        reasons = set()
        for options in (dict(), dict(B=31), dict(B=1), dict(R=3)):
            trace, error = self.assert_replay(values, **options)
            self.assertIsNone(error)
            reasons.update(row['reason'] for _, row in trace)
        mixed = values[:3]+[candidate(i, stage='other') for i in range(3, 6)]
        trace, error = self.assert_replay(mixed)
        reasons.update(row['reason'] for _, row in trace)
        self.assertIsNone(error)
        cap = max(trace[0][1]['ipc_frame_bytes_upper_bound'], trace[0][1]['data_rpc_bytes_upper_bound'])-1
        limits = replace(organizer(values).owner._actors.limits, input_bytes=cap, item_input_bytes=cap)
        trace, error = self.assert_replay(values, limits=limits)
        self.assertIsNone(error)
        reasons.update(row['reason'] for _, row in trace)
        self.assertEqual(reasons, {'ready_frontier_exhausted', 'token_budget', 'request_capacity',
                                  'compatibility', 'frame_bytes'})

    def test_UTF8_escaping_nested_metadata_and_frame_boundaries_are_byte_exact(self):
        values = [candidate(i, text='汉字🙂\x00\n"\\'*17, extra={'键': ['雪', '"\\', i]}) for i in range(4)]
        trace, _ = self.assert_replay(values)
        boundary = max(trace[0][1]['ipc_frame_bytes_upper_bound'], trace[0][1]['data_rpc_bytes_upper_bound'])
        for cap in (boundary-1, boundary, boundary+1):
            with self.subTest(cap=cap):
                limits = replace(organizer(values).owner._actors.limits, input_bytes=cap, item_input_bytes=cap)
                self.assert_replay(values, limits=limits)
        value = organizer(values)
        self.assertEqual(value(values), tuple(c.key for c in values))
        rows = [json.loads(c.task.payload)['row'] for c in values]
        generation = json.loads(values[0].task.payload)['generation']
        ipc = dict(op='submit_batch', requests=[dict(request_id='f'*32+':'+str((1<<63)-1),
            row={name: row[name] for name in (*IDENTITY_FIELDS, 'messages_json')}) for row in rows],
            generation=generation, deadline=value.owner.deadline)
        rpc = dict(calls=[dict(row=row, generation=generation, maximum=c.task.max_result_bytes,
            request_key=[c.key.session_id, c.key.sequence]) for c, row in zip(values, rows)],
            batch_key=[1, (1<<64)-1], deadline=value.owner.deadline)
        self.assertEqual(len(_frame(ipc, boundary))-4, value.history[-1]['ipc_frame_bytes_upper_bound'])
        self.assertEqual(len(_frame(rpc, boundary))-4, value.history[-1]['data_rpc_bytes_upper_bound'])

    def test_single_item_and_RPC_overflow_reject_at_the_same_candidate(self):
        values = [candidate(0), candidate(1, text='雪'*500), candidate(2)]
        row = json.loads(values[1].task.payload)
        snapshot = {name: row['row'][name] for name in (*IDENTITY_FIELDS, 'messages_json')}
        single = len(_frame(dict(op='submit', request_id='f'*32+':'+str((1<<63)-1), row=snapshot,
            generation=row['generation'], deadline=123.456), 100000))-4
        for cap in (single-1, single, single+1):
            with self.subTest(item_cap=cap):
                limits = replace(organizer(values).owner._actors.limits, item_input_bytes=cap)
                trace, error = self.assert_replay(values, limits=limits)
                self.assertEqual(error is None, cap >= single)
        too_large_RPC = [candidate(0), candidate(1, extra='雪'*10000)]
        limits = replace(organizer(values).owner._actors.limits, input_bytes=4096, item_input_bytes=4096)
        self.assertIsNotNone(self.assert_replay(too_large_RPC, limits=limits)[1])

    def test_randomized_ready_prefixes_generations_and_boundaries_match(self):
        rng = random.Random(72)
        for run in range(60):
            values = [candidate(i, session=(1<<63)+run, stage='stage'+str(i//7),
                text=('雪🙂 "\\ '+('word '*rng.randrange(1, 15)))*rng.randrange(1, 8),
                generation=asdict(GenerationSpec(max_tokens=(i//5+1)*3, stop=('雪\n', '"\\'))),
                extra={'source': [i, '汉字']}) for i in range(rng.randrange(1, 25))]
            cap = rng.randrange(2200, 9000)
            limits = replace(organizer(values).owner._actors.limits, input_bytes=cap, item_input_bytes=cap)
            with self.subTest(run=run):
                self.assert_replay(values, B=rng.choice((1, 64, 256, 1000000)),
                    R=rng.randrange(1, 9), limits=limits, rolling=bool(run%2))

    def test_backpressure_retries_reuse_member_metrics_and_live_cache_is_bounded(self):
        values = [candidate(i, text='汉字🙂 "\\ '*100) for i in range(128)]
        value = organizer(values)
        calls, encoded = [], []
        frame = _frame
        def counted(packet, maximum):
            raw = frame(packet, maximum)
            calls.append(packet)
            encoded.append(len(raw))
            return raw
        with patch.object(batching, '_frame', counted):
            selected = value(values)
            first_encodes = len(calls)
            self.assertEqual(value(values), selected, 'rejected handoff retries the same immutable group')
        self.assertEqual(first_encodes, 2*len(values)+3)
        self.assertEqual(len(calls), first_encodes, 'retry must not re-encode members or headers')
        self.assertEqual(value.tokenizer.calls, len(values))
        legacy_calls = []
        def legacy_counted(packet, maximum):
            raw = frame(packet, maximum)
            legacy_calls.append(len(raw))
            return raw
        with patch(__name__+'._frame', legacy_counted):
            self.assertEqual(organizer(values, legacy=True)(values), selected)
        self.assertEqual(len(legacy_calls), 5*len(values))
        self.assertLess(sum(encoded), sum(legacy_calls))
        value.owner.core.capacity.records.clear()
        self.assertEqual(value(()), ())
        self.assertEqual(value._estimates, {})
        self.assertIsNone(value._header_estimate)
        self.assertIsNone(value._header_context)

    def test_deadline_session_and_generation_headers_refresh_without_retokenizing_members(self):
        values = [candidate(0), candidate(1)]
        value = organizer(values)
        value(values)
        for deadline in (1e-6, 123456789012345.67):
            value.owner.deadline = deadline
            expected = organizer(values, legacy=True, deadline=deadline)
            self.assertEqual(value(values), expected(values))
            self.assertEqual(value.history[-1], expected.history[-1])
        self.assertEqual(value.tokenizer.calls, len(values))
        for session, generation in ((1<<63, asdict(GenerationSpec(stop=('雪"\\',)))),
                                    (2, asdict(GenerationSpec(max_tokens=17)))):
            values = [candidate(i, session=session, generation=generation) for i in range(3)]
            value.owner.core.capacity.records = {c.key: SimpleNamespace(compute=False, member=None) for c in values}
            value.owner.resources.max_active_batches = 3
            value.owner._actors.limits = replace(value.owner._actors.limits, max_inflight=3,
                result_slots=3, result_bytes=3*8192)
            expected = organizer(values, legacy=True, deadline=value.owner.deadline)
            self.assertEqual(value(values), expected(values))
            self.assertEqual(value.history[-1], expected.history[-1])
            self.assertEqual(value._header_context, (session, value.owner.deadline,
                json.dumps(generation, sort_keys=True), value.owner._actors.limits.input_bytes))


if __name__ == '__main__':
    unittest.main()
