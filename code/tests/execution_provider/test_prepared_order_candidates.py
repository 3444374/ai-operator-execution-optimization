from dataclasses import replace
import json
import unittest
from unittest.mock import patch

from src.scheduling.core.session_contract import Acceptance, TaskKey
from src.scheduling.organization.prepared_order import (
    PreparedRequestOrder, RequestOrganizationProfile, order_prepared_window,
)
from src.execution_provider.adapters.lotus_chain_token_batches import PreparedTokenOrganizer
from tests.execution_provider.test_prepared_request_organization import candidates, organizer
from tests.execution_provider import test_prepared_request_organization as organization_fixture


class WordIdentityTokenizer:
    def __init__(self): self.calls = 0
    def apply_chat_template(self, messages, **options):
        self.calls += 1
        return [ord(word[0]) for word in messages[0]['content'].split()]


class PreparedOrderTests(unittest.TestCase):
    def test_finite_entry_options_keep_FIFO_and_candidate_transport_identical(self):
        from src.experiments.prepared_request_options import owner_options
        tokenizer = object()
        base = dict(token_budget=1024, batch_concurrency=2, order=None)
        fifo = owner_options(base, tokenizer=tokenizer, max_active_requests=4)
        ordered = owner_options(dict(base, order=dict(mode='prefix_length', candidate_limit=16,
            prefix_token_limit=128, block_tokens=16)), tokenizer=tokenizer, max_active_requests=4)
        self.assertEqual({k: v for k, v in fifo.items() if k != 'prepared_order'},
                         {k: v for k, v in ordered.items() if k != 'prepared_order'})
        self.assertIs(fifo['batching_tokenizer'], tokenizer)
        self.assertIsNone(base['order'])
        for invalid in (dict(base, batch_concurrency=5), dict(base, token_budget=True),
                        dict(base, order='prefix'), dict(base, extra=1)):
            with self.assertRaises((TypeError, ValueError)):
                owner_options(invalid, max_active_requests=4)

    def test_anchor_compatibility_prefix_blocks_and_length_are_separate(self):
        def profile(i, tokens, length, group='a'):
            return RequestOrganizationProfile(TaskKey(0, i), (group,), length, 10, tuple(tokens))
        rows = (profile(0, [1, 2, 3, 4], 4), profile(1, [8, 9, 0, 0], 5),
                profile(2, [1, 2, 3, 9], 50), profile(3, [1, 2, 3, 4], 8),
                profile(4, [1, 2, 3, 4], 4, 'other'))
        expected = {'length': [0, 1, 3, 2, 4], 'prefix': [0, 3, 2, 1, 4],
                    'prefix_length': [0, 3, 2, 1, 4]}
        for mode, order in expected.items():
            result = order_prepared_window(rows, PreparedRequestOrder(mode, 5, 4, 2))
            self.assertEqual([key.sequence for key in result], order)
        self.assertEqual(rows[0].token_prefix, (1, 2, 3, 4))

    def test_short_prefix_keeps_stable_ties(self):
        rows = tuple(RequestOrganizationProfile(TaskKey(0, i), ('same',), 4, 1, (1, 2, i, 9)) for i in range(3))
        self.assertEqual(order_prepared_window(rows, PreparedRequestOrder('prefix', 3, 4, 8)),
                         tuple(row.key for row in rows))

    def ordered_engine(self):
        helper = organization_fixture.PreparedRequestOrganizationTests()
        engine, session, backend, clock, value = helper.engine(1024)
        record = engine.capacity.records[TaskKey(session.session_id, 1)]
        payload = json.loads(record.task.payload)
        payload['row']['messages_json'] = json.dumps([dict(role='user', content='different ' * 480)])
        record.task = replace(record.task, payload=json.dumps(payload).encode())
        value.tokenizer = WordIdentityTokenizer()
        value.prepared_order = PreparedRequestOrder('prefix_length', 8, 16, 4)
        return engine, session, backend, clock, value

    def test_reordered_rejected_group_is_retried_exactly_after_window_shrink(self):
        engine, session, backend, clock, value = self.ordered_engine()
        payloads = {key: record.task.payload for key, record in engine.capacity.records.items()}
        backend.acceptance = Acceptance.NOT_ACCEPTED
        session.advance(8)
        first = backend.attempts[-1]
        self.assertEqual([task.key.sequence for task in first], [0, 2])
        value.request_window = 1; backend.acceptance = Acceptance.ACCEPTED; clock.now += 1
        session.advance(8)
        self.assertEqual(backend.attempts[-1], first)
        self.assertEqual(value.selected, 1)
        self.assertTrue(all(task.task.payload == payloads[task.key] for task in first))
        session.close_consumer(clean=False)
        for key in tuple(backend.pending): backend.complete(key)
        engine.advance()
        self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_unknown_reordered_handoff_is_not_replayed_or_refunded(self):
        engine, session, backend, _, value = self.ordered_engine()
        backend.acceptance = Acceptance.UNKNOWN
        session.advance(8)
        self.assertEqual([task.key.sequence for task in backend.attempts[0]], [0, 2])
        self.assertEqual(session.close().uncertain_requests, 2)
        self.assertEqual(len(backend.attempts), 1)
        self.assertEqual(engine.capacity.usage().active_requests, 2)

    def test_candidate_limit_bounds_tokenization_and_cleanup_releases_prefix_metadata(self):
        rows = candidates([4] * 8)
        old, _ = organizer(rows, C=4, B=None)
        order = PreparedRequestOrder('prefix_length', 3, 2, 2)
        tokens = WordIdentityTokenizer()
        value = PreparedTokenOrganizer(old.owner, tokens, None, prepared_order=order); value.request_window = 4
        result = value(rows)
        self.assertEqual(len(result), 3)
        self.assertEqual(tokens.calls, 3)
        self.assertEqual(sum(map(len, value._token_prefixes.values())), 6)
        value.release_query_payloads(); self.assertEqual(value._token_prefixes, {})

    def test_event_driven_append_recomputes_the_candidate_group_without_retokenizing_old_rows(self):
        engine, session, backend, _, value = self.ordered_engine()
        from src.scheduling.core.session_policy import TaskCandidate
        rows = tuple(TaskCandidate(record.key, record.task, record.spec) for record in engine.capacity.records.values())
        value.owner._event_driven_preparation = True
        value.refresh_prefix(rows[:2], 0)
        value.append_ready(rows[2:4])
        self.assertEqual([key.sequence for key in value._prefix.chosen], [0, 2])
        self.assertEqual(value.tokenizer.calls, 4)
        self.assertEqual(value(rows[:4]), tuple(rows[i].key for i in (0, 2)))
        self.assertEqual(value.tokenizer.calls, 4)
        session.close_consumer(clean=False); engine.advance()


class PublicOwnerOrderTests(unittest.TestCase):
    def test_actual_CPU_Core_owner_preserves_results_and_messages_for_all_candidate_orders(self):
        from tests.execution_provider.test_lotus_async_token_batches import AsyncTokenBatchTests, ThreadedRuntime, query_fixture
        from tests.semantic_methods import test_lotus_chain_composition as fixture
        from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
        helper = AsyncTokenBatchTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        plan, rows = fixture.chain_for('MM'), fixture.source_rows(size=4)
        for row, size in zip(rows, (4, 50, 5, 8)):
            row.values['passage'] = 'text ' * size
        helper.helper.config_for(plan, rows)
        reference = None
        for mode, event_driven in ((None, False), ('length', False), ('prefix', False),
                                   ('prefix_length', False), ('prefix_length', True)):
            with self.subTest(mode=mode, event_driven=event_driven), ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
                options = {} if mode is None else dict(prepared_order=PreparedRequestOrder(mode, 4, 64, 4))
                owner = helper.owner(plan, **options)
                owner._interleaved_preparation = owner._event_driven_preparation = event_driven
                try:
                    stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                    actual = fixture.normalized(dict(records=list(stream), calls=owner.calls, columns=stream.columns))
                    messages = {call['call_id']: (call['fixture_messages'], call['fixture_generation']) for call in owner.calls}
                    if reference is None: reference = actual, messages
                    self.assertEqual((actual, messages), reference)
                    self.assertEqual(owner.last_close_report['status'], 'closed')
                    self.assertFalse(any(owner.last_close_report['core_usage'].values()))
                finally:
                    owner.close(timeout=3)


if __name__ == '__main__':
    unittest.main()
