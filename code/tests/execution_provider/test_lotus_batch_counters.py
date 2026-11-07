"""Query packing counters over real selectors and CPU-only transport fixtures."""
import asyncio
from concurrent.futures import Future
from copy import deepcopy
import json
import sys
from types import SimpleNamespace
import time
import unittest
from unittest.mock import AsyncMock, patch

from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.scheduling.core.session_contract import Acceptance, BatchKey
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.execution_provider import test_prepared_request_organization as packing
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture, query_close


class SealCounterTests(unittest.TestCase):
    def test_whole_query_reasons_survive_history_eviction_and_report_is_a_snapshot(self):
        pending = packing.candidates([480]*80)
        value, _ = packing.organizer(pending, C=len(pending), R=len(pending), B=512)
        self.assertEqual(sum(value.report()['selected_seal_reason_counts'].values()), 0)
        while pending:
            selected = value(pending)
            self.assertEqual(len(selected), 1)
            pending = tuple(row for row in pending if row.key not in selected)
            for key in selected:
                value.owner.core.capacity.records.pop(key)
        report = value.report()
        self.assertEqual(report['selected_batches'], 80)
        self.assertEqual(len(report['recent_batches']), 64)
        self.assertEqual(report['selected_seal_reason_counts'], dict(token_budget=80,
            request_capacity=0, frame_bytes=0, compatibility=0, ready_frontier_exhausted=0))
        report['selected_seal_reason_counts']['token_budget'] = -1
        self.assertEqual(value.report()['selected_seal_reason_counts']['token_budget'], 80)
        self.assertEqual(value(()), ())
        self.assertEqual(value.selected, 80)
        fresh, _ = packing.organizer((), R=1, C=1)
        self.assertEqual(sum(fresh.report()['selected_seal_reason_counts'].values()), 0)

    def test_exact_B_at_C_does_not_imply_extra_members_or_an_accepted_packet(self):
        rows = packing.candidates([480]*9)
        selected = []
        for B, reason in ((4096, 'token_budget'), (None, 'request_capacity')):
            value, _ = packing.organizer(rows, C=8, B=B)
            selected.append(value(rows))
            report = value.report()
            self.assertEqual(report['selected_batches'], 1)
            self.assertEqual(report['selected_seal_reason_counts'][reason], 1)
            self.assertEqual(sum(report['selected_seal_reason_counts'].values()), 1)
            self.assertNotIn('accepted', report['selected_seal_reason_counts'])
        self.assertEqual(selected[0], selected[1])

    def test_Core_rejection_retries_do_not_select_again_and_unknown_keeps_responsibility(self):
        helper = packing.PreparedRequestOrganizationTests()
        for final in (Acceptance.ACCEPTED, Acceptance.UNKNOWN):
            with self.subTest(final=final):
                engine, session, backend, clock, value = helper.engine(B=None)
                backend.acceptance = Acceptance.NOT_ACCEPTED
                session.advance(8)
                first = backend.attempts[-1]
                self.assertEqual(engine.capacity.usage().active_requests, 0)
                snapshot = value.report()['selected_seal_reason_counts']
                for _ in range(2):
                    clock.now += 1
                    session.advance(8)
                self.assertEqual(value.selected, 1)
                self.assertEqual(value.report()['selected_seal_reason_counts'], snapshot)
                backend.acceptance = final
                clock.now += 1
                session.advance(8)
                self.assertEqual(first, backend.attempts[-1])
                self.assertEqual(value.selected, 1)
                if final == Acceptance.UNKNOWN:
                    self.assertEqual(session.close().uncertain_requests, len(first))
                    self.assertEqual(engine.capacity.usage().active_requests, len(first))
                else:
                    for key in tuple(backend.pending):
                        backend.complete(key)
                    deliveries = session.advance(8).deliveries
                    session.release(tuple(d.lease_id for d in deliveries))
                    session.close_consumer(clean=False)
                    engine.advance()
                    self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_cancelled_selection_is_still_selected_without_an_actor_ack(self):
        helper = packing.PreparedRequestOrganizationTests()
        engine, session, backend, _, value = helper.engine(B=None)
        backend.acceptance = Acceptance.NOT_ACCEPTED
        session.advance(8)
        session.close()
        self.assertEqual(value.selected, 1)
        self.assertEqual(sum(value.report()['selected_seal_reason_counts'].values()), 1)
        self.assertEqual(engine.capacity.usage().active_requests, 0)
        self.assertEqual(backend.pending, {})


class BatchAckCounterTests(unittest.IsolatedAsyncioTestCase):
    def transport(self, submit, poll=None, members=1):
        actors = SimpleNamespace(cancel_core=lambda key: None, acknowledge_core=lambda key: None,
            submit_prepared_batch=submit, wait_prepared_batch=poll)
        value = LotusChainActorBackend(actors, members, lambda: None,
            deadline_monotonic=time.monotonic()+5, on_result=lambda key, value: None, bulk=True)
        value.poll_interval_s = 0  # Cooperative CPU fixture polling; no timing assertion.
        self.addCleanup(lambda: self.assertTrue(value.backend.close(3)))
        rows = packing.candidates([2]*members)
        group = BatchKey(0, 0)
        context = dict(tasks={r.key: r for r in rows},
            values={r.key: json.loads(r.task.payload) for r in rows},
            members=frozenset(r.key for r in rows), futures={r.key: Future() for r in rows},
            started=True, driver_done=False, accepted=False, acks={})
        value._bulk[group] = context
        return value, group, context

    async def test_pending_then_accepted_ack_is_counted_once_despite_multiple_result_pages(self):
        entered, proceed = asyncio.Event(), asyncio.Event()
        async def submit(*args):
            entered.set()
            await proceed.wait()
            return dict(status='accepted')
        pages = []
        async def poll(key):
            return json.dumps(pages.pop(0)).encode()
        value, group, context = self.transport(submit, poll, members=2)
        for i, status in enumerate(('succeeded', 'unknown')):
            pages.append(dict(results=[dict(request_key=[0, i], result=dict(status=status))], closed=False))
        task = asyncio.create_task(value._drive_batch(group, context))
        await asyncio.wait_for(entered.wait(), 1)
        self.assertEqual(value.batch_handoff_report(), dict(
            submit_operations_started=1,
            actor_ack_counts=dict(accepted=0, rejected=0, unknown=0), pending_actor_acks=1))
        proceed.set()
        await asyncio.wait_for(task, 1)
        self.assertEqual(value.data_batch_rpcs, 1)
        self.assertEqual(value.batch_handoff_report(), dict(
            submit_operations_started=1,
            actor_ack_counts=dict(accepted=1, rejected=0, unknown=0), pending_actor_acks=0))
        self.assertEqual([json.loads(f.result())['status'] for f in context['futures'].values()],
                         ['succeeded', 'unknown'])
        old = value.batch_handoff_report()
        old['actor_ack_counts']['accepted'] = -1
        self.assertEqual(value.batch_handoff_report()['actor_ack_counts']['accepted'], 1)

    async def test_rejected_ack_keeps_returned_failed_members_and_is_not_accepted(self):
        async def submit(*args):
            return dict(status='rejected', results=[dict(request_key=[0, 0], result=dict(status='failed'))])
        value, group, context = self.transport(submit)
        await value._drive_batch(group, context)
        self.assertEqual(value.batch_handoff_report()['actor_ack_counts'],
                         dict(accepted=0, rejected=1, unknown=0))
        self.assertEqual(json.loads(next(iter(context['futures'].values())).result())['status'], 'failed')

    async def test_owner_backpressure_retries_are_one_logical_submit_and_one_acceptance(self):
        owner = AsyncRecordsActorOwner.for_prepared(
            EngineSpec('/tmp/cpu-fixture-model', sys.executable, sys.executable), GenerationSpec(),
            RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=1))
        async def poll(key):
            return json.dumps(dict(results=[dict(request_key=[0, 0],
                result=dict(status='succeeded'))], closed=False)).encode()
        rpc = AsyncMock(side_effect=[dict(status='backpressure'), dict(status='backpressure'),
                                    dict(status='accepted')])
        value, group, context = self.transport(owner.submit_prepared_batch, poll)
        with patch.object(owner, '_data_rpc', rpc):
            await value._drive_batch(group, context)
        self.assertEqual(rpc.await_count, 3)
        self.assertEqual(value.data_batch_rpcs, 1)
        self.assertEqual(value.batch_handoff_report(), dict(submit_operations_started=1,
            actor_ack_counts=dict(accepted=1, rejected=0, unknown=0), pending_actor_acks=0))

    async def test_repeated_cancel_before_ack_counts_one_unknown_and_preserves_failure(self):
        entered = asyncio.Event()
        async def submit(*args):
            entered.set()
            await asyncio.Event().wait()
        value, group, context = self.transport(submit)
        task = asyncio.create_task(value._drive_batch(group, context))
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        task.cancel()
        await task
        self.assertEqual(value.batch_handoff_report(), dict(
            submit_operations_started=1,
            actor_ack_counts=dict(accepted=0, rejected=0, unknown=1), pending_actor_acks=0))
        self.assertIsInstance(next(iter(context['futures'].values())).exception(), asyncio.CancelledError)

    async def test_exception_before_ack_and_failure_after_ack_have_different_counts(self):
        async def broken(*args):
            raise RuntimeError('CPU fixture')
        for accepted in (False, True):
            with self.subTest(accepted=accepted):
                async def submit(*args):
                    return dict(status='accepted')
                value, group, context = self.transport(submit if accepted else broken, broken)
                await value._drive_batch(group, context)
                self.assertEqual(value.batch_handoff_report()['actor_ack_counts'],
                    dict(accepted=int(accepted), rejected=0, unknown=int(not accepted)))
                self.assertIsInstance(next(iter(context['futures'].values())).exception(), RuntimeError)

    async def test_synchronous_submit_exception_is_unknown_without_changing_old_RPC_count(self):
        def submit(*args):
            raise RuntimeError('CPU fixture')
        value, group, context = self.transport(submit)
        await value._drive_batch(group, context)
        self.assertEqual(value.data_batch_rpcs, 0)
        self.assertEqual(value.batch_handoff_report(), dict(submit_operations_started=1,
            actor_ack_counts=dict(accepted=0, rejected=0, unknown=1), pending_actor_acks=0))
        self.assertIsInstance(next(iter(context['futures'].values())).exception(), RuntimeError)

    async def test_invalid_ack_is_bounded_unknown_without_changing_the_original_exception(self):
        async def submit(*args):
            return {}
        value, group, context = self.transport(submit)
        await value._drive_batch(group, context)
        self.assertEqual(value.batch_handoff_report()['actor_ack_counts'],
                         dict(accepted=0, rejected=0, unknown=1))
        self.assertIsInstance(next(iter(context['futures'].values())).exception(), KeyError)


class PublicBatchCounterTests(unittest.TestCase):
    def test_two_public_resident_queries_reset_counters_in_both_B_modes(self):
        for no_B in (False, True):
            with self.subTest(no_B=no_B):
                fixture = MovieTokenResidentFixture(self)
                plan = fixture.plan('RF')
                if no_B:
                    plan.pop('prepared_batching')
                report = fixture.run(plan)
                self.assertEqual(report['status'], 'completed')
                stats = [query_close(q)['async_token_batches'] for q in report['queries']]
                first = deepcopy(stats[0])
                for row in stats:
                    self.assertEqual(row['selected_batches'], 1)
                    self.assertEqual(sum(row['selected_seal_reason_counts'].values()), 1)
                    self.assertEqual(row['batch_handoff'], dict(
                        submit_operations_started=1,
                        actor_ack_counts=dict(accepted=1, rejected=0, unknown=0), pending_actor_acks=0))
                self.assertEqual(stats[0], first)
                self.assertEqual(fixture.owners[-1].close()['async_token_batches'], stats[-1])


if __name__ == '__main__':
    unittest.main()
