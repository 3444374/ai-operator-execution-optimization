"""Fixed-C packing counterfactuals with real organizer/Core and CPU stubs only."""
from dataclasses import asdict, replace
import hashlib
import json
import sys
import time
from types import SimpleNamespace
import unittest

from src.execution_provider.adapters.lotus_chain_token_batches import PreparedTokenOrganizer
from src.scheduling.core.session_contract import Acceptance, BatchSubmission, OfferedTask, SessionSpec, TaskKey
from src.scheduling.core.session_policy import TaskCandidate
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
from src.serving.backends.async_engine_wire import _frame
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.scheduling.test_incremental_session import setup


class WordCountTokenizer:
    """Declared word-count fixture, no pretrained assets or model rendering."""
    def __init__(self):
        self.calls = 0

    def apply_chat_template(self, messages, **options):
        self.calls += 1
        return [1] * len(messages[0]['content'].split())


def candidates(prompts, stages=None):
    generation = asdict(GenerationSpec(max_tokens=32))
    spec = SessionSpec('q', 'flow', 'fixture')
    result = []
    for i, words in enumerate(prompts):
        stage = 'stage' if stages is None else stages[i]
        row = stage_identity('q', RecordOccurrence('r'+str(i), i, 0, {}), stage)
        row['messages_json'] = json.dumps([dict(role='user', content='x '*words)])
        payload = json.dumps(dict(row=row, generation=generation)).encode()
        result.append(TaskCandidate(TaskKey(0, i), OfferedTask(i, payload, 1, 8192), spec))
    return tuple(result)


def organizer(rows, *, skip=False, C=16, active=0, R=128, B=4096, **limits):
    records = {r.key: SimpleNamespace(compute=False, member=None) for r in rows}
    records.update({TaskKey(1, i): SimpleNamespace(compute=True, member=None) for i in range(active)})
    actor_limits = dict(max_inflight=R, result_slots=R, result_bytes=R*8192,
                        item_result_bytes=8192, input_bytes=2**20, item_input_bytes=2**18)
    actor_limits.update(limits)
    engine = EngineSpec('/tmp/token-packing-model', sys.executable, sys.executable,
        max_model_len=4096)  # Explicit synthetic context, no native model readiness.
    owner = SimpleNamespace(resources=SimpleNamespace(max_active_batches=R), engine=engine,
        core=SimpleNamespace(capacity=SimpleNamespace(records=records)),
        _actors=SimpleNamespace(limits=SimpleNamespace(**actor_limits), model_ready=None),
        prepared_batching=None if B is None else PreparedBatching('token_budget', None, B), rolling_refill=True,
        request_feedback=None, deadline=1e9, experimental_skip_token_packing=skip)
    tokens = WordCountTokenizer()
    value = PreparedTokenOrganizer(owner, tokens, None)
    value.request_window = C
    return value, tokens


def cpu_replay(rows, *, skip, C=16, active=0, B=4096, **limits):
    """Immediate CPU stub terminals; same finite ready set and fixed free C."""
    R = limits.pop('R', 128)
    value, tokens = organizer(rows[:R-active], skip=skip, C=C, active=active, R=R, B=B, **limits)
    pending, by_key = rows, {r.key: r for r in rows}
    started = time.perf_counter_ns()
    select_cpu = encode_cpu = stub_cpu = 0
    first_handoff = None
    groups, delivered, payloads = [], [], []
    while pending:
        frontier = pending[:R-active]
        for row in frontier:
            value.owner.core.capacity.records.setdefault(row.key, SimpleNamespace(compute=False, member=None))
        assert len(value.owner.core.capacity.records) <= R
        before = time.thread_time_ns()
        selected = value(frontier)
        select_cpu += time.thread_time_ns()-before
        if not selected:
            raise AssertionError('fixture made no progress')
        calls = [dict(row=json.loads(by_key[k].task.payload)['row'],
                      generation=json.loads(by_key[k].task.payload)['generation'],
                      maximum=by_key[k].task.max_result_bytes, request_key=[k.session_id, k.sequence])
                 for k in selected]
        before = time.thread_time_ns()
        rpc = _frame(dict(calls=calls, batch_key=[0, len(groups)], deadline=value.owner.deadline),
                     value.owner._actors.limits.input_bytes)
        requests = [dict(request_id='a'*32+':'+str(k.sequence), row=c['row'])
                    for k, c in zip(selected, calls)]
        ipc = _frame(dict(op='submit_batch', requests=requests, generation=calls[0]['generation'],
                          deadline=value.owner.deadline), value.owner._actors.limits.input_bytes)
        encode_cpu += time.thread_time_ns()-before
        # The timed entry below is a local stub, not an actor/vLLM acceptance.
        before = time.thread_time_ns()
        if first_handoff is None:
            first_handoff = time.perf_counter_ns()-started
        delivered.extend(c['row']['call_id'] for c in calls)
        payloads.extend(by_key[k].task.payload for k in selected)
        groups.append([k.sequence for k in selected])
        stub_cpu += time.thread_time_ns()-before
        assert len(rpc)-4 <= value.owner._actors.limits.input_bytes
        assert len(ipc)-4 <= value.owner._actors.limits.input_bytes
        for key in selected:
            value.owner.core.capacity.records.pop(key)
        chosen = set(selected)
        pending = tuple(r for r in pending if r.key not in chosen)
    return dict(groups=groups, packages=len(groups), delivered=delivered,
        input_sha256=hashlib.sha256(b''.join(payloads)).hexdigest(),
        tokenizer_calls=tokens.calls, selected_cpu_ns=select_cpu, encode_cpu_ns=encode_cpu,
        submit_stub_cpu_ns=stub_cpu, first_stub_handoff_wall_ns=first_handoff,
        reasons=[r['reason'] for r in value.history])


class TokenPackingBypassTests(unittest.TestCase):
    def pair(self, rows, **options):
        left, _ = organizer(rows, skip=False, **options)
        right, _ = organizer(rows, skip=True, **options)
        a, b = left(rows), right(rows)
        self.assertEqual(b[:len(a)], a)
        return left, right, a, b

    def test_eight_free_slots_exact_B_is_not_marginal_token_opportunity(self):
        rows = candidates([480]*9)
        a, b, ka, kb = self.pair(rows, C=8)
        self.assertEqual(len(ka), 8)
        self.assertEqual(ka, kb)
        self.assertEqual(a.history[0]['tokens'], 4096)
        self.assertEqual(a.history[0]['reason'], 'token_budget')
        self.assertEqual(b.history[0]['reason'], 'request_capacity')

    def test_omitted_B_groups_complete_requests_using_C_and_IPC_without_token_splitting(self):
        rows = candidates([480]*9)
        value, tokens = organizer(rows, B=None, C=9)
        payloads = tuple(row.task.payload for row in rows)
        self.assertEqual(value(rows), tuple(row.key for row in rows))
        self.assertEqual(value.history[0]['reason'], 'request_capacity')
        self.assertIsNone(value.report()['policy'])
        self.assertNotIn('experimental_skip_token_packing', value.report())
        self.assertEqual(tokens.calls, len(rows))
        self.assertEqual(payloads, tuple(row.task.payload for row in rows))
        value, _ = organizer(rows, B=None, C=9, result_slots=2)
        self.assertEqual(value(rows), tuple(row.key for row in rows[:2]))
        oversize = candidates([4090])
        value, _ = organizer(oversize, B=None)
        with self.assertRaisesRegex(ValueError, 'model context'):
            value(oversize)

    def test_B_removes_members_only_when_another_legal_member_fits_C(self):
        for costs, B in (([480]*24, 4096), ([10, 1800, 15, 1900, 1000, 10], 4096), ([2000, 10, 10], 1024)):
            with self.subTest(costs=costs):
                rows = candidates(costs)
                _, _, a, b = self.pair(rows, B=B)
                self.assertGreater(len(b), len(a))
                x, y = (cpu_replay(rows, skip=s, B=B) for s in (False, True))
                self.assertEqual(x['delivered'], y['delivered'])
                self.assertEqual(x['input_sha256'], y['input_sha256'])
                self.assertEqual(x['tokenizer_calls'], len(rows))
                self.assertEqual(y['tokenizer_calls'], len(rows))
                self.assertLess(y['packages'], x['packages'])

    def test_C_active_ready_and_member_result_capacities_are_kept(self):
        for options, expected in (
                (dict(C=16, active=10), 6), (dict(C=1), 1),
                (dict(C=16, active=16), 0), (dict(C=1, active=3), 0),
                (dict(C=16, R=3), 3), (dict(result_slots=2), 2),
                (dict(result_bytes=8192), 1), (dict(max_inflight=2), 2)):
            with self.subTest(options=options):
                _, _, a, b = self.pair(candidates([2]*min(12, options.get('R', 128))), **options)
                self.assertEqual(len(a), expected)
                self.assertEqual(a, b)
        a, b, ka, kb = self.pair(candidates([2]*3), C=16, active=10)
        self.assertEqual(ka, kb)
        self.assertEqual(a.history[0]['reason'], 'ready_frontier_exhausted')
        self.assertEqual(b.history[0]['reason'], 'ready_frontier_exhausted')

    def test_compatibility_preserves_contiguous_prefix_and_generation(self):
        for changed in ('stage', 'generation'):
            rows = list(candidates([20]*4, ['a', 'a', 'b', 'a'] if changed == 'stage' else None))
            if changed == 'generation':
                body = json.loads(rows[2].task.payload)
                body['generation']['max_tokens'] += 1
                rows[2] = replace(rows[2], task=replace(rows[2].task, payload=json.dumps(body).encode()))
            a, b, ka, kb = self.pair(tuple(rows))
            self.assertEqual(len(ka), 2)
            self.assertEqual(ka, kb)
            self.assertEqual(a.history[0]['reason'], 'compatibility')
            self.assertEqual(b.history[0]['reason'], 'compatibility')

    def test_frame_byte_limit_and_single_request_validation_are_kept(self):
        rows = candidates([20]*3)
        probe, _ = organizer(rows, B=100000)
        probe(rows[:1])
        cap = max(probe.history[0]['ipc_frame_bytes_upper_bound'],
                  probe.history[0]['data_rpc_bytes_upper_bound'])+32
        a, b, ka, kb = self.pair(rows, input_bytes=cap, B=100000)
        self.assertEqual(len(ka), 1)
        self.assertEqual(ka, kb)
        self.assertEqual(a.history[0]['reason'], 'frame_bytes')
        self.assertEqual(b.history[0]['reason'], 'frame_bytes')
        for skip in (False, True):
            with self.subTest(skip=skip):
                value, _ = organizer(rows, skip=skip, item_input_bytes=40)
                with self.assertRaises(ValueError):
                    value(rows)

    def test_report_is_explicit_only_for_opt_in_and_input_is_unchanged(self):
        rows = candidates([20]*3)
        before = tuple(r.task.payload for r in rows)
        a, b, _, _ = self.pair(rows)
        self.assertNotIn('experimental_skip_token_packing', a.report())
        self.assertIs(b.report()['experimental_skip_token_packing'], True)
        self.assertEqual(a.report()['policy'], b.report()['policy'])
        self.assertEqual(before, tuple(r.task.payload for r in rows))

    def test_full_frontier_counts_do_not_confuse_token_reason_with_extra_members(self):
        rows = candidates([480]*24)
        no_margin = marginal_frontiers(rows, 8)
        self.assertEqual((no_margin['frontiers'], no_margin['extra_member_events'],
                          no_margin['extra_members_opportunities']), (3, 0, 0))
        margin = marginal_frontiers(rows, 16)
        self.assertEqual((margin['frontiers'], margin['extra_member_events'],
                          margin['extra_members_opportunities']), (3, 2, 16))

    def engine(self, skip):
        engine, session, backend, clock = setup(held_tasks=8, active_requests=8, active_work=8,
            input_bytes=2**20, result_bytes=8*8192, item_result_bytes=8192,
            item_input_bytes=2**18, metadata_bytes=4096, step_actions=32, offer_tasks=8)
        rows = candidates([480]*8)
        value, _ = organizer(rows, skip=skip, C=8, R=8, B=1024)
        value.owner.core = engine
        engine.policies = replace(engine.policies, organize=value,
            topology=replace(engine.policies.topology, endpoints=engine.policies.topology.endpoints[:1]))
        backend.supports_batch_submit = True
        backend.attempts = []
        def submit(tasks, endpoint):
            backend.attempts.append(tuple(tasks))
            if backend.acceptance != Acceptance.ACCEPTED:
                return BatchSubmission(backend.acceptance)
            return BatchSubmission(Acceptance.ACCEPTED,
                tuple(backend.try_submit(t, endpoint).handle for t in tasks))
        backend.try_submit_batch = submit
        self.assertEqual(session.offer(tuple(r.task for r in rows)).accepted_prefix_count, 8)
        return engine, session, backend, clock, value

    def test_rejection_retries_same_group_after_shrink_and_releases_per_member(self):
        for skip in (False, True):
            with self.subTest(skip=skip):
                engine, session, backend, clock, value = self.engine(skip)
                backend.acceptance = Acceptance.NOT_ACCEPTED
                session.advance(8)
                first = backend.attempts[-1]
                self.assertEqual(engine.capacity.usage().active_requests, 0)
                value.request_window = 1
                backend.acceptance = Acceptance.ACCEPTED
                clock.now += 1
                session.advance(8)
                self.assertEqual(first, backend.attempts[-1])
                self.assertEqual(value.selected, 1)
                self.assertEqual(len(backend.pending), len(first))
                backend.complete(first[0].key, b'done')
                deliveries = session.advance(8).deliveries
                self.assertEqual(engine.capacity.usage().active_requests, len(first)-1)
                session.release(tuple(d.lease_id for d in deliveries))
                session.close_consumer(clean=False)
                for key in tuple(backend.pending):
                    backend.complete(key)
                engine.advance()
                self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_unknown_retains_responsibility_and_is_not_replayed(self):
        for skip in (False, True):
            with self.subTest(skip=skip):
                engine, session, backend, _, value = self.engine(skip)
                backend.acceptance = Acceptance.UNKNOWN
                session.advance(8)
                self.assertEqual(len(backend.attempts), 1)
                count = len(backend.attempts[0])
                self.assertEqual(engine.capacity.usage().active_requests, count)
                close = session.close()
                self.assertEqual(close.uncertain_requests, count)
                self.assertEqual(len(backend.attempts), 1)
                self.assertEqual(value.selected, 1)


def marginal_frontiers(rows, C):
    """Compare both selectors at each identical fixed-B ready frontier.

    Extra members count selection opportunities, not unique model calls. The
    shadow bypass selections are never dispatched or used for C feedback.
    """
    a, _ = organizer(rows[:C], C=C, R=C)
    b, _ = organizer(rows[:C], C=C, R=C, skip=True)
    pending = rows
    frontiers = events = extra = 0
    while pending:
        frontier = pending[:C]
        for value in (a, b):
            for row in frontier:
                value.owner.core.capacity.records.setdefault(row.key, SimpleNamespace(compute=False, member=None))
            assert len(value.owner.core.capacity.records) <= C
        left, right = a(frontier), b(frontier)
        assert right[:len(left)] == left
        frontiers += 1
        events += len(right) > len(left)
        extra += len(right)-len(left)
        chosen = set(left)
        pending = tuple(row for row in pending if row.key not in chosen)
        for key in chosen:
            a.owner.core.capacity.records.pop(key)
            b.owner.core.capacity.records.pop(key)
    return dict(frontiers=frontiers, extra_member_events=events,
                extra_members_opportunities=extra, grouping_trace='fixed-B frontiers, fixed C=R, immediate stub terminals')


def microbenchmark(repeats=20):
    """CPU fixture raw pairs, no counterfactual vLLM or query timing claim."""
    cases = [('C8_exact4096_no_margin', [480]*24, 8),
             ('C16_B_splits', [480]*24, 16),
             ('C16_short_prompts', [12]*24, 16),
             ('C16_mixed_prompts', [10, 1800, 15, 1900, 1000, 10]*4, 16)]
    result = []
    for name, costs, C in cases:
        rows = candidates(costs)
        a, b = (organizer(rows[:C], C=C, R=C, skip=s)[0] for s in (False, True))
        ka, kb = a(rows[:C]), b(rows[:C])
        pairs = []
        for repeat in range(repeats):
            values = {}
            for skip in ((False, True) if repeat % 2 == 0 else (True, False)):
                values['bypass' if skip else 'fixed_B'] = cpu_replay(rows, skip=skip, C=C, R=C)
            pairs.append(values)
        result.append(dict(case=name, C=C, R=C, B=4096, ready_frontier=min(C,len(rows)), total_requests=len(rows),
            frontier_marginal_event=int(len(kb)>len(ka)), frontier_extra_members=len(kb)-len(ka),
            same_frontier_selected_counts=[len(ka), len(kb)],
            marginal_frontier_counts=marginal_frontiers(rows, C), raw_pairs=pairs))
    return dict(scope='CPU organizer/encoding/immediate-submit-stub replay; no Ray/model/GPU',
        repeats=repeats, first_handoff='wall time from replay start to first CPU stub entry after encoding',
        tokenization='synthetic word-count tokenizer, computed in both modes',
        resource_assumption='each stub group completes immediately; fixed C=R, ready residency<=R; no dynamic controller',
        cases=result)


if __name__ == '__main__':
    if sys.argv[1:] == ['--microbenchmark']:
        print(json.dumps(microbenchmark(), sort_keys=True))
    else:
        unittest.main()
