"""Trusted membership, Job opportunities and isolation on the existing engine/ledger."""

from dataclasses import replace
import unittest

from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import (
    SessionSpec,
    SessionTimeouts,
    TaskKey,
    State,
    Uncertain,
)
from src.scheduling.core.session_jobs import JobBudget, JobHandle
from tests.scheduling.test_incremental_session import Backend, Clock, limits, setup, task


def engine(requests=1):
    template, old, _, _ = setup()
    old.close()
    backend, clock = Backend(), Clock()
    core = SessionEngine(
        limits(
            held_tasks=8,
            input_bytes=32,
            result_bytes=32,
            active_requests=requests,
            active_work=requests,
            timeouts=SessionTimeouts(),
        ),
        backend,
        template.policies,
        clock=clock,
        max_jobs=2,
    )
    return core, backend, clock


def budget(sessions=2, requests=1):
    return JobBudget(4, 16, 16, requests, requests, sessions)


def flow(core, handle, name):
    return core.open(SessionSpec("untrusted-label", name, "fixture"), job=handle)


class MultiSessionTests(unittest.TestCase):
    def test_storage_refusal_reports_condition_without_self_waking(self):
        e, _, _ = engine()
        a = e.register_job("A", replace(budget(), result_bytes=4))
        s = flow(e, a, "a")
        s.offer((task(0),))
        generation = e.wake.generation
        refused = s.offer((task(1),))
        self.assertEqual(refused.reason, "job_result_bytes")
        self.assertEqual(refused.status, "BACKPRESSURE")
        self.assertEqual((refused.generation, e.wake.generation), (generation, generation))

    def test_single_item_larger_than_job_storage_is_permanently_rejected(self):
        for constrained in (replace(budget(), input_bytes=3), replace(budget(), result_bytes=3)):
            with self.subTest(constrained=constrained):
                e, backend, _ = engine()
                a = e.register_job("A", constrained)
                s = flow(e, a, "a")
                rejected = s.offer((task(0),))
                self.assertEqual(rejected.status, "REJECTED")
                self.assertEqual(e.capacity.usage().held_tasks, 0)
                self.assertEqual(backend.pending, {})

    def test_global_budget_exhaustion_reports_dispatch_without_a_new_wake(self):
        e, backend, _ = engine(requests=2)
        a = e.register_job("A", budget(requests=2))
        s = flow(e, a, "a")
        s.offer((task(0), task(1)))
        generation = e.wake.generation
        first = e.advance(1)
        self.assertTrue(first.has_immediate_work)
        self.assertEqual(e.wake.generation, generation)
        self.assertEqual(len(backend.pending), 1)
        second = e.advance(1)
        self.assertFalse(second.has_immediate_work)
        self.assertEqual(len(backend.pending), 2)

    def test_queued_but_capacity_blocked_waits_until_completion(self):
        e, backend, clock = engine()
        a = e.register_job("A", budget())
        s = flow(e, a, "a")
        s.offer((task(0), task(1)))
        progress = e.advance(1)
        self.assertFalse(progress.has_immediate_work)
        self.assertEqual(progress.blocked_reason, "WAIT_BACKEND")
        self.assertGreater(progress.next_deadline, clock.now)
        backend.complete(TaskKey(s.session_id, 0))
        self.assertTrue(e.advance(1).has_immediate_work)
        self.assertFalse(e.advance(1).has_immediate_work)
        self.assertIn(TaskKey(s.session_id, 1), backend.pending)

    def test_backend_rejection_has_a_retry_deadline_without_busy_loop(self):
        from src.scheduling.core.session_contract import Acceptance

        e, backend, clock = engine()
        a = e.register_job("A", budget())
        s = flow(e, a, "a")
        s.offer((task(0),))
        backend.acceptance = Acceptance.NOT_ACCEPTED
        progress = e.advance(1)
        self.assertFalse(progress.has_immediate_work)
        self.assertEqual(progress.next_deadline, clock.now + s.limits.poll_interval_s)
        e.advance(1)
        self.assertEqual(len(backend.routes), 1)
        clock.now = progress.next_deadline
        backend.acceptance = Acceptance.ACCEPTED
        e.advance(1)
        self.assertEqual(len(backend.pending), 1)

    def test_continued_global_ticks_preserve_other_job_and_cancel_opportunities(self):
        e, backend, _ = engine(requests=2)
        a = e.register_job("A", budget(requests=2))
        b = e.register_job("B", budget(requests=2))
        sa, sb = flow(e, a, "a"), flow(e, b, "b")
        sa.offer((task(0), task(1)))
        self.assertTrue(e.advance(1).has_immediate_work)
        sb.offer((task(0),))
        e.advance(1)
        self.assertIn(TaskKey(sb.session_id, 0), backend.pending)
        sa.request_cancel()
        progress = e.advance(1)
        self.assertTrue(progress.has_immediate_work)
        e.advance(1)
        self.assertIn(TaskKey(sa.session_id, 0), backend.cancelled)
        self.assertEqual(sb.state, State.OPEN)

    def test_ready_results_do_not_claim_work_the_global_tick_cannot_do(self):
        e, backend, _ = engine()
        a = e.register_job("A", budget())
        s = flow(e, a, "a")
        s.offer((task(0),))
        e.advance()
        backend.complete(TaskKey(s.session_id, 0))
        progress = e.advance()
        self.assertFalse(progress.has_immediate_work)
        self.assertEqual(progress.blocked_reason, "WAIT_CONSUMER")
        self.assertIsNone(progress.next_deadline)

    def test_consumer_close_reclaims_leases_but_keeps_remote_work(self):
        e, backend, _ = engine(requests=2)
        a = e.register_job("A", budget(requests=2))
        b = e.register_job("B", budget())
        sa, sb = flow(e, a, "a"), flow(e, b, "b")
        sa.offer((task(0), task(1)))
        e.advance()
        backend.complete(TaskKey(sa.session_id, 0))
        e.advance()
        delivery = sa.advance(1).deliveries[0]
        self.assertEqual(sa.close().status, "WAITING_FOR_RELEASE")
        self.assertEqual(e.capacity.usage(sa.session_id).held_tasks, 2)
        e.close_job(a)
        generation = e.wake.generation
        closed = sa.close_consumer()
        self.assertGreater(e.wake.generation, generation)
        self.assertEqual(closed.status, "CLOSED")
        self.assertEqual(closed.uncertain_requests, 1)
        self.assertEqual(e.capacity.usage(sa.session_id).held_tasks, 1)
        self.assertIs(sa.close_consumer(), closed)
        sb.offer((task(0),))
        e.advance()
        backend.complete(TaskKey(sa.session_id, 1))
        backend.complete(TaskKey(sb.session_id, 0))
        e.advance()
        self.assertEqual(e.capacity.usage(sa.session_id).held_tasks, 0)
        self.assertNotIn(a, e.jobs.jobs)
        self.assertEqual(len(sb.advance(1).deliveries), 1)
        sb.close_consumer(clean=True)
        e.close_job(b)
        self.assertEqual(e.capacity.usage().held_tasks, 0)
        self.assertEqual(delivery.key.session_id, sa.session_id)

    def test_public_failure_is_local_to_the_flow(self):
        e, backend, _ = engine()
        a = e.register_job("A", budget())
        b = e.register_job("B", budget())
        sa, sb = flow(e, a, "a"), flow(e, b, "b")
        sa.offer((task(0),))
        sb.offer((task(0),))
        sa.fail("connection command rejected")
        e.advance()
        self.assertIsNone(e.error)
        self.assertEqual(sa.state, State.FAILED)
        self.assertEqual(tuple(backend.pending), (TaskKey(sb.session_id, 0),))
        sa.close_consumer()
        e.close_job(a)

    def test_job_selector_can_be_replaced_without_changing_accounting(self):
        from src.scheduling.core.session_jobs import FlowChoice

        e, backend, _ = engine()
        a = e.register_job("A", budget())
        b = e.register_job("B", budget())
        sa, sb = flow(e, a, "a"), flow(e, b, "b")
        seen = []

        def prefer_last(candidates, history):
            seen.append((candidates, history))
            return FlowChoice(candidates[-1].job_id, candidates[-1].session_ids[-1])

        e.choose_flow = prefer_last
        sa.offer((task(0),))
        sb.offer((task(0),))
        e.advance()
        self.assertEqual(tuple(backend.pending), (TaskKey(sb.session_id, 0),))
        self.assertEqual(e.capacity.usage().active_requests, 1)
        self.assertEqual(seen[0][1].last_job, None)
        self.assertEqual(len(seen[0][0]), 2)
        backend.complete(TaskKey(sb.session_id, 0))
        e.advance()
        self.assertIn(TaskKey(sa.session_id, 0), backend.pending)

    def test_invalid_job_selection_cannot_dispatch_foreign_work(self):
        from src.scheduling.core.session_jobs import FlowChoice

        e, backend, _ = engine()
        a = e.register_job("A", budget())
        sa = flow(e, a, "a")
        sa.offer((task(0),))
        e.choose_flow = lambda candidates, history: FlowChoice(a.job_id, sa.session_id + 1)
        e.advance()
        self.assertEqual(e.error, "Job selection failed")
        self.assertEqual(backend.pending, {})
        self.assertEqual(e.capacity.usage().active_requests, 0)

    def test_membership_is_capability_not_a_label(self):
        e, _, _ = engine()
        a = e.register_job("same-label", budget())
        b = e.register_job("same-label", budget())
        self.assertNotEqual(a.job_id, b.job_id)
        for forged in (JobHandle(a.job_id, a.label), "same-label"):
            with self.assertRaises(ValueError):
                flow(e, forged, "forged")
        other, _, _ = engine()
        with self.assertRaises(ValueError):
            flow(other, a, "foreign")
        a1, a2 = flow(e, a, "a1"), flow(e, a, "a2")
        self.assertEqual(a1.spec.job_id, a2.spec.job_id)
        with self.assertRaises(ValueError):
            flow(e, a, "excess-flow")
        with self.assertRaises(RuntimeError):
            e.open(SessionSpec(a.job_id, "spoof", "fixture"))

    def test_two_flows_do_not_double_job_opportunities(self):
        e, b, _ = engine()
        a, j = e.register_job("A", budget()), e.register_job("B", budget())
        a1, a2, b1 = flow(e, a, "a1"), flow(e, a, "a2"), flow(e, j, "b1")
        for s, n in ((a1, 2), (a2, 2), (b1, 4)):
            self.assertEqual(s.offer([task(i) for i in range(n)]).accepted_prefix_count, n)
        # Polling one session cannot submit anything without the owner's global tick.
        for _ in range(5):
            b1.advance(1)
        self.assertFalse(b.pending)
        order = []
        sessions = {s.session_id: s for s in (a1, a2, b1)}
        for _ in range(8):
            e.advance()
            self.assertEqual(len(b.pending), 1)
            key = next(iter(b.pending))
            order.append(sessions[key.session_id].spec.job_id)
            b.complete(key)
            e.reap(1)
            result = sessions[key.session_id].advance(1)
            sessions[key.session_id].release([result.deliveries[0].lease_id])
        self.assertEqual(order, [a.job_id, j.job_id] * 4)
        self.assertEqual(e.capacity.usage().held_tasks, 0)

    def test_job_intake_cannot_take_other_jobs_reserved_storage(self):
        e, b, _ = engine()
        a, j = e.register_job("A", budget()), e.register_job("B", budget())
        a1, a2, b1 = flow(e, a, "a1"), flow(e, a, "a2"), flow(e, j, "b1")
        self.assertEqual(a1.offer([task(i) for i in range(4)]).accepted_prefix_count, 4)
        self.assertEqual(a2.offer([task(0)]).accepted_prefix_count, 0)
        self.assertEqual(b1.offer([task(0)]).accepted_prefix_count, 1)
        # A retains its completed result without releasing it; B still finishes.
        e.advance()
        key = next(iter(b.pending))
        b.complete(key)
        e.reap(1)
        delivery = a1.advance(1).deliveries[0]
        e.advance()
        key = next(iter(b.pending))
        self.assertEqual(key.session_id, b1.session_id)
        b.complete(key)
        e.reap(1)
        result = b1.advance(1)
        b1.release([result.deliveries[0].lease_id])
        self.assertEqual(e.capacity.usage(job_id=j.job_id).held_tasks, 0)
        self.assertGreater(e.capacity.usage(job_id=a.job_id).held_tasks, 0)
        a1.release([delivery.lease_id])

    def test_cancelling_one_job_keeps_late_work_and_other_job_live(self):
        e, b, _ = engine(requests=2)
        a, j = e.register_job("A", budget()), e.register_job("B", budget())
        a1, a2, b1 = flow(e, a, "a1"), flow(e, a, "a2"), flow(e, j, "b1")
        a1.offer([task(0)])
        a2.offer([task(0)])
        b1.offer([task(0)])
        e.advance()
        e.close_job(a)
        e.advance()
        self.assertEqual(a1.state, State.CANCELLED)
        self.assertEqual(a2.state, State.CANCELLED)
        self.assertEqual(b1.state, State.OPEN)
        self.assertEqual(e.capacity.usage(job_id=a.job_id).active_requests, 1)
        with self.assertRaises(ValueError):
            flow(e, a, "late-join")
        for key in tuple(b.pending):
            b.complete(key)
        e.reap(2)
        self.assertFalse(a1.advance(1).deliveries)
        result = b1.advance(1)
        b1.release([result.deliveries[0].lease_id])
        a1.close()
        a2.close()
        e.reap(1)
        self.assertNotIn(a, e.jobs.jobs)
        self.assertIn(j, e.jobs.jobs)

    def test_identified_uncertainty_does_not_poison_other_job(self):
        e, b, _ = engine(requests=2)
        a, j = e.register_job("A", budget()), e.register_job("B", budget())
        a1, b1 = flow(e, a, "a1"), flow(e, j, "b1")
        a1.offer([task(0)])
        b1.offer([task(0)])
        e.advance()
        key = TaskKey(a1.session_id, 0)
        b.events.append(Uncertain(key, b.pending[key][0]))
        e.reap(1)
        self.assertEqual(a1.state, State.FAILED)
        self.assertIsNone(e.error)
        self.assertEqual(e.capacity.usage(job_id=a.job_id).active_requests, 1)
        b.complete(TaskKey(b1.session_id, 0))
        e.reap(1)
        result = b1.advance(1)
        b1.release([result.deliveries[0].lease_id])
        self.assertEqual(b1.state, State.OPEN)

    def test_late_invalid_result_has_job_scope_after_session_close(self):
        e, b, _ = engine(requests=2)
        a, j = e.register_job("A", budget()), e.register_job("B", budget())
        a1, b1 = flow(e, a, "a1"), flow(e, j, "b1")
        a1.offer([task(0)])
        b1.offer([task(0)])
        e.advance()
        e.close_job(a)
        a1.close()
        b.complete(TaskKey(a1.session_id, 0), result=b"oversized")
        e.reap(1)
        self.assertIsNone(e.error)
        self.assertEqual(b1.state, State.OPEN)
        self.assertNotIn(a, e.jobs.jobs)

    def test_queued_terminal_cannot_erase_an_expired_backend_deadline(self):
        e, b, clock = engine()
        a = e.register_job("A", budget())
        a1 = e.open(
            SessionSpec("A", "a1", "fixture"),
            replace(e.capacity.limits, timeouts=SessionTimeouts(backend_s=3)),
            job=a,
        )
        a1.offer([task(0)])
        e.advance()
        clock.now = 3
        b.complete(TaskKey(a1.session_id, 0))
        e.advance()
        self.assertEqual(a1.state, State.FAILED)
        self.assertFalse(a1.advance(1).deliveries)
        self.assertEqual(e.capacity.usage().active_requests, 0)

    def test_shared_identity_conflict_stops_every_session(self):
        e, b, _ = engine(requests=2)
        a, j = e.register_job("A", budget()), e.register_job("B", budget())
        a1, b1 = flow(e, a, "a1"), flow(e, j, "b1")
        b.events.append(Uncertain(TaskKey(999, 0), "wrong"))
        e.reap(1)
        self.assertIsNotNone(e.error)
        self.assertEqual((a1.state, b1.state), (State.FAILED, State.FAILED))
