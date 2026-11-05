"""5ff54 batch-handoff oracle, call-local decode reuse and unchanged owners; CPU only."""
import asyncio
from concurrent.futures import Future
from copy import deepcopy
from dataclasses import asdict, replace
import json
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_chain_actor_backend as backend
from src.execution_provider.adapters import records_transport as records
from src.execution_provider.adapters.lotus_chain_actor_backend import _SectionTransport
from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.scheduling.core.session_contract import (
    Acceptance, BackendTask, BatchKey, BatchMember, BatchSubmission, OfferedTask, SessionSpec, TaskKey,
)
from src.semantic_methods.lotus.record_types import RecordOccurrence, stage_identity
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_protocol import GenerationSpec, IDENTITY_FIELDS


# Frozen 5ff54bfe methods; test-only, no production dependency on the oracle.
def legacy_base_submit(self, tasks, endpoint):
    if not self.supports_batch_submit or not tasks or any(t.member is None for t in tasks):
        raise ValueError('bulk handoff requires an organized prepared batch')
    group = tasks[0].member.batch_key
    if (group in self._bulk or any(t.member.batch_key != group for t in tasks)
            or tuple(t.member.index for t in tasks) != tuple(range(len(tasks)))):
        raise ValueError('bulk handoff changed ordered membership')
    values = [self._request(t) for t in tasks]
    context = dict(tasks={t.key: t for t in tasks}, values={t.key: v for t, v in zip(tasks, values)},
                   members=frozenset(t.key for t in tasks), futures={t.key: Future() for t in tasks},
                   started=False, driver_done=False)
    self._bulk[group] = context
    with self._lock:
        for task, value in zip(tasks, values):
            self._requests[task.key] = {n: value['row'][n] for n in IDENTITY_FIELDS}, task.task.max_result_bytes
            self._refs[task.key] = (value['row'],)
    outcome = self.backend.try_submit_batch(tasks, endpoint)
    if outcome.acceptance == Acceptance.NOT_ACCEPTED:
        del self._bulk[group]
        with self._lock:
            for task in tasks:
                self._requests.pop(task.key, None); self._refs.pop(task.key, None)
    return outcome


def legacy_section_submit(self, tasks, endpoint):
    for task in tasks:
        self.owner._register_local_task(task, self._request(task))
    outcome = legacy_base_submit(self, tasks, endpoint)
    if outcome.acceptance == Acceptance.NOT_ACCEPTED:
        with self.owner._receipt_lock:
            for task in tasks:
                self.owner._contexts.pop(task.key, None)
    return outcome


class FrozenSection(_SectionTransport):
    try_submit_batch = legacy_section_submit


class PacketOwner:
    """Keep the actual owner-side final encoder; substitute only the data RPC."""
    submit_prepared_batch = AsyncRecordsActorOwner.submit_prepared_batch

    def __init__(self, maximum):
        self.limits = AsyncEngineLimits(input_bytes=maximum, item_input_bytes=maximum)
        self.packets, self.cancelled = [], []

    def cancel_core(self, key):
        self.cancelled.append(key)

    async def _data_rpc(self, method, payload):
        self.packets.append((method, payload))
        packet = json.loads(payload)
        return dict(status='rejected', results=[dict(request_key=call['request_key'],
            result=dict(call['row'], status='failed', submitted=False)) for call in packet['calls']])


class Admission:
    """A deterministic backend handoff, without a thread or execution queue."""
    def __init__(self, *args, **options):
        self.acceptance, self.error = Acceptance.ACCEPTED, None
        self.trace, self.cancelled = [], []

    def try_submit_batch(self, tasks, endpoint):
        self.trace.append((tuple(t.key for t in tasks), endpoint))
        if self.error is not None:
            raise self.error
        return BatchSubmission(self.acceptance, tuple(str(t.key.sequence) for t in tasks))

    def request_cancel(self, key, handle):
        self.cancelled.append((key, handle))


def tasks(count=4, *, text='完整 prompt 雪🙂\n"\\', generation=None):
    generation = GenerationSpec() if generation is None else generation
    group = BatchKey(2, 7)
    values = []
    for index in range(count):
        record = RecordOccurrence('r'+str(index), index, index, {})
        row = stage_identity('query', record, 'stage', model_role='main')
        row['messages_json'] = json.dumps([dict(role='system', content='保持原文'),
            dict(role='user', content=text+str(index))], ensure_ascii=False)
        payload = json.dumps(dict(row=row, generation=asdict(generation), parent_call_ids=['parent/'+str(index)]),
                             ensure_ascii=False, allow_nan=False).encode()
        values.append(BackendTask(TaskKey(2, index), SessionSpec('job', 'flow', 'prepared'),
            OfferedTask(index, payload, 1, 8192, profile_name='stage'), BatchMember(group, index, count)))
    return tuple(values)


def transport(values, *, legacy=False, frame_bytes=64*1024**2, generation=None):
    owner = object.__new__(SemLoomRecordChainCore)
    owner._actors = PacketOwner(frame_bytes)
    owner.core = SimpleNamespace(wake=SimpleNamespace(notify=lambda: None))
    owner.deadline, owner.token_budget, owner._async_token_batches = 1e12, None, True
    owner.capacity = SimpleNamespace(poll_interval_s=.001)
    owner._receipt_lock, owner._contexts = threading.Lock(), {}
    owner._record_local_receipt = lambda *args: None
    owner.query_chain = SimpleNamespace(query_id='query')
    owner._stage_bindings = {'stage':SimpleNamespace(stage_id='stage', model_role='main',
        generation=GenerationSpec() if generation is None else generation)}
    owner._expected_bindings = {'r'+str(i):RecordOccurrence('r'+str(i), i, i, {}) for i in range(len(values))}
    with patch.object(records, 'BoundedAsyncBackend', Admission):
        result = (FrozenSection if legacy else _SectionTransport)(owner, max(1, len(values)))
    return result


def state(value):
    return dict(contexts=deepcopy(value.owner._contexts), requests=deepcopy(value._requests), refs=deepcopy(value._refs),
        bulk={group:dict(tasks=tuple(c['tasks']), values=deepcopy(c['values']), members=c['members'],
            futures={key:(future.done(), future.cancelled(),
                (type(future.exception()).__name__, str(future.exception())) if future.done() and future.exception() else None)
                for key, future in c['futures'].items()}, started=c['started'], driver_done=c['driver_done'])
            for group, c in value._bulk.items()}, trace=list(value.backend.trace),
        packets=list(value.actors.packets), ACK=value.batch_handoff_report())


def submit(value, values):
    try:
        return value.try_submit_batch(values, 'same-endpoint'), None
    except Exception as error:
        return None, (type(error).__name__, str(error))


class RequestReuseTests(unittest.TestCase):
    def assert_pair(self, values, **options):
        left, right = transport(values, legacy=True, **options), transport(values, **options)
        self.assertEqual(submit(left, values), submit(right, values))
        self.assertEqual(state(left), state(right))
        return left, right

    def test_success_membership_frames_and_generation_match_frozen_oracle(self):
        generation = GenerationSpec(max_tokens=31, temperature=.3, stop=('雪\n', '"\\'))
        values = tasks(7, generation=generation)
        original = tuple(t.task.payload for t in values)
        left, right = self.assert_pair(values, generation=generation)
        for value in (left, right):
            group = values[0].member.batch_key
            asyncio.run(value._drive_batch(group, value._bulk[group]))
        self.assertEqual(state(left), state(right))
        self.assertEqual(len(right.actors.packets), 1)
        packet = json.loads(right.actors.packets[0][1])
        self.assertEqual([c['request_key'] for c in packet['calls']], [[2,i] for i in range(7)])
        self.assertTrue(all(c['generation'] == json.loads(json.dumps(asdict(generation))) for c in packet['calls']))
        self.assertEqual(tuple(t.task.payload for t in values), original)

    def test_only_initial_handoff_parse_is_removed_and_no_new_state_fields_appear(self):
        values = tasks(32)
        counts = []
        for legacy in (True, False):
            value = transport(values, legacy=legacy)
            fields = set(vars(value))
            original = value._request
            with patch.object(value, '_request', wraps=original) as request:
                self.assertEqual(value.try_submit_batch(values, 'endpoint').acceptance, Acceptance.ACCEPTED)
            counts.append(request.call_count)
            self.assertEqual(set(vars(value)), fields)
        self.assertEqual(counts, [64,32])

    def test_owner_registration_does_not_mutate_parsed_request_or_generation(self):
        generation = GenerationSpec(response_format={'type':'json_object'}, stop=('雪',))
        values = tasks(1, generation=generation)
        value = transport(values, generation=generation)
        request = value._request(values[0])
        original = deepcopy(request)
        value.owner._register_local_task(values[0],request)
        self.assertEqual(request,original)
        self.assertEqual(request['generation'], json.loads(json.dumps(asdict(generation))))

    def test_payload_owner_binding_and_membership_errors_keep_the_same_first_error(self):
        original = tasks(4)
        variants = [(), tuple(replace(t, member=None) for t in original), original[::-1],
            tuple(replace(t, member=replace(t.member, batch_key=BatchKey(2,8))) if i==2 else t
                  for i,t in enumerate(original))]
        for index in (0,2,3):
            broken = list(original)
            broken[index] = replace(broken[index], task=replace(broken[index].task, payload=b'{'))
            variants.append(tuple(broken))
        for field, change in (
            ('generation', dict(max_tokens=0)), ('generation', asdict(GenerationSpec(max_tokens=9))),
            ('token_cost', {}), ('row', dict(messages_json='not JSON')),
        ):
            broken = list(original)
            payload = json.loads(broken[2].task.payload)
            if field == 'row':
                payload['row'].update(change)
            else:
                payload[field] = change
            broken[2] = replace(broken[2], task=replace(broken[2].task, payload=json.dumps(payload).encode()))
            variants.append(tuple(broken))
        # Section validation precedes malformed membership even when both fail.
        variants.append((replace(original[0], member=None, task=replace(original[0].task, payload=b'{')),))
        for index, values in enumerate(variants):
            with self.subTest(variant=index):
                left, right = self.assert_pair(values)
                self.assertIsNotNone(submit(right, values)[1])

    def test_rejection_rolls_back_and_a_retry_reads_fresh_immutable_task_bytes(self):
        initial = tasks(3)
        updated = tasks(3, text='new prompt 新内容')
        pair = [transport(initial, legacy=legacy) for legacy in (True,False)]
        for value in pair:
            value.backend.acceptance = Acceptance.NOT_ACCEPTED
            self.assertEqual(value.try_submit_batch(initial, 'endpoint').acceptance, Acceptance.NOT_ACCEPTED)
            self.assertEqual((value._bulk, value._refs, value._requests, value.owner._contexts), ({},{},{},{}))
            value.backend.acceptance = Acceptance.ACCEPTED
            value.try_submit_batch(updated, 'endpoint')
            group = updated[0].member.batch_key
            asyncio.run(value._drive_batch(group, value._bulk[group]))
        self.assertEqual(state(pair[0]), state(pair[1]))
        self.assertIn('new prompt', json.loads(pair[1].actors.packets[0][1])['calls'][0]['row']['messages_json'])

    def test_unknown_and_backend_error_keep_owned_contexts_and_cancel_routing(self):
        values = tasks(3)
        for fail in (False,True):
            pair = [transport(values, legacy=legacy) for legacy in (True,False)]
            for value in pair:
                value.backend.acceptance = Acceptance.UNKNOWN
                if fail:
                    value.backend.error = RuntimeError('submit outcome unavailable')
            self.assertEqual(submit(pair[0],values), submit(pair[1],values))
            self.assertEqual(state(pair[0]), state(pair[1]))
            for value in pair:
                self.assertEqual(set(value.owner._contexts), {t.key for t in values})
                self.assertEqual(set(value._requests), {t.key for t in values})
                value.request_cancel(values[0].key,'handle')
                self.assertEqual(value.backend.cancelled, [(values[0].key,'handle')])
                self.assertEqual(set(value._bulk), {values[0].member.batch_key})

    def test_final_owner_frame_limit_still_fails_before_the_RPC(self):
        values = tasks(2, text='x'*5000)
        pair = self.assert_pair(values, frame_bytes=4096)
        for value in pair:
            group = values[0].member.batch_key
            asyncio.run(value._drive_batch(group, value._bulk[group]))
            self.assertEqual(value.actors.packets, [])
            self.assertTrue(all(isinstance(f.exception(),ValueError) for f in value._bulk[group]['futures'].values()))
            self.assertEqual(set(value.owner._contexts), {t.key for t in values})
        self.assertEqual(state(pair[0]), state(pair[1]))

    def test_async_member_registration_retains_its_independent_binding_validation(self):
        values = tasks(2)
        pair = self.assert_pair(values)
        for value in pair:
            value.owner._stage_bindings['stage'] = SimpleNamespace(stage_id='stage', model_role='main',
                                                                  generation=GenerationSpec(max_tokens=9))
            with self.assertRaisesRegex(ValueError,'immutable query stage'):
                value._submit_task(values[0], None, None)
            self.assertEqual(value.actors.packets, [])

    def test_bare_backend_preserves_its_preparse_membership_checks(self):
        values = (replace(tasks(1)[0], member=None, task=replace(tasks(1)[0].task,payload=b'{')),)
        for legacy in (True,False):
            value = transport(values)
            call = legacy_base_submit if legacy else backend.LotusChainActorBackend.try_submit_batch
            with self.assertRaisesRegex(ValueError, 'organized prepared batch'):
                call(value,values,'endpoint')


if __name__ == '__main__':
    unittest.main()
