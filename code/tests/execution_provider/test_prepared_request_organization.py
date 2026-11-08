"""Complete prepared-request organization over existing Core and CPU fixtures."""
from dataclasses import asdict, replace
import json
import sys
from types import SimpleNamespace
import unittest
from src.execution_provider.adapters.lotus_chain_token_batches import PreparedTokenOrganizer
from src.scheduling.core.session_contract import Acceptance, BatchSubmission, OfferedTask, SessionSpec, TaskKey
from src.scheduling.core.session_policy import TaskCandidate
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
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


def organizer(rows, *, C=16, active=0, R=128, B=4096, **limits):
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
        request_feedback=None, deadline=1e9)
    tokens = WordCountTokenizer()
    value = PreparedTokenOrganizer(owner, tokens, None)
    value.request_window = C
    return value, tokens


class PreparedRequestOrganizationTests(unittest.TestCase):
    def pair(self, rows, **options):
        left, _ = organizer(rows, **options)
        right, _ = organizer(rows, **dict(options, B=None))
        a, b = left(rows), right(rows)
        self.assertEqual(b[:len(a)], a)
        return left, right, a, b

    def test_exact_B_at_request_capacity_keeps_the_same_members(self):
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
        for B in (4096, None):
            with self.subTest(B=B):
                value, _ = organizer(rows, B=B, item_input_bytes=40)
                with self.assertRaises(ValueError):
                    value(rows)

    def engine(self, B=1024):
        engine, session, backend, clock = setup(held_tasks=8, active_requests=8, active_work=8,
            input_bytes=2**20, result_bytes=8*8192, item_result_bytes=8192,
            item_input_bytes=2**18, metadata_bytes=4096, step_actions=32, offer_tasks=8)
        rows = candidates([480]*8)
        value, _ = organizer(rows, C=8, R=8, B=B)
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
        for B in (1024, None):
            with self.subTest(B=B):
                engine, session, backend, clock, value = self.engine(B)
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
        for B in (1024, None):
            with self.subTest(B=B):
                engine, session, backend, _, value = self.engine(B)
                backend.acceptance = Acceptance.UNKNOWN
                session.advance(8)
                self.assertEqual(len(backend.attempts), 1)
                count = len(backend.attempts[0])
                self.assertEqual(engine.capacity.usage().active_requests, count)
                close = session.close()
                self.assertEqual(close.uncertain_requests, count)
                self.assertEqual(len(backend.attempts), 1)
                self.assertEqual(value.selected, 1)


if __name__ == '__main__':
    unittest.main()
