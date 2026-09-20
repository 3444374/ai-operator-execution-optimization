"""Drive real owner loops with heterogeneous FIFO work and a deterministic clock."""
from dataclasses import replace
import unittest
from unittest.mock import patch

from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import Acceptance, SessionSpec, TaskKey
from src.scheduling.core.session_jobs import JobBudget
from tests.scheduling.test_incremental_session import setup, task


class CapacityWaitTests(unittest.TestCase):
    def test_blocked_owner_tick_does_not_rescan_usage_for_each_queued_row(self):
        template, _, backend, clock = setup(
            held_tasks=128, input_bytes=512, result_bytes=512,
            active_requests=4, active_work=4, offer_tasks=128)
        engine = SessionEngine(template.capacity.limits, backend, template.policies, clock=clock)
        job = engine.register_job('job', JobBudget(128, 512, 512, 4, 4))
        session = engine.open(SessionSpec(job.job_id, 'flow', 'fixture'), job=job)
        session.offer([task(i) for i in range(128)])
        engine.advance()
        self.assertEqual(len(backend.pending), 4)
        with patch.object(engine.capacity, 'usage', wraps=engine.capacity.usage) as usage:
            progress = engine.advance()
            delivery = session.advance(1)
        self.assertFalse(progress.has_immediate_work)
        self.assertFalse(delivery.has_immediate_work)
        self.assertEqual(progress.blocked_reason, 'WAIT_BACKEND')
        # At most a fixed set of global/Job/session summaries per read-only
        # eligibility/progress scan, independent of the 124 queued candidates.
        self.assertLessEqual(usage.call_count, 12)
        backend.complete(TaskKey(session.session_id, 0))
        engine.advance()
        self.assertIn(TaskKey(session.session_id, 4), backend.pending)

    def make_case(self, registered, organized=False):
        original, _, backend, clock = setup(
            held_tasks=4, input_bytes=16, result_bytes=16,
            active_requests=2, active_work=10, poll_interval_s=.01)
        events = []
        policy = original.policies
        if organized:
            policy = replace(policy, organize=lambda candidates: tuple(c.key for c in candidates))
        engine = SessionEngine(original.capacity.limits, backend, policy, clock=clock,
                               sink=lambda kind, key: events.append((kind, key)),
                               observe_capacity_blocks=True)
        if registered:
            job = engine.register_job('job', JobBudget(4, 16, 16, 2, 10))
            session = engine.open(SessionSpec(job.job_id, 'flow', 'fixture'), job=job)
        else:
            session = engine.open(SessionSpec('job', 'flow', 'fixture'))

        def advance():
            progress = engine.advance() if registered else None
            result = session.advance(4)
            session.release([d.lease_id for d in result.deliveries])
            return progress or result

        return engine, session, backend, clock, events, advance

    def test_capacity_release_dispatches_fifo_before_old_retry_deadline(self):
        for registered in (False, True):
            for organized in (False, True):
                with self.subTest(registered=registered, organized=organized):
                    engine, session, backend, clock, events, advance = self.make_case(registered, organized)
                    session.offer([task(i, estimated_work=w) for i, w in enumerate((6, 5, 4))])
                    advance()
                    self.assertEqual(list(backend.pending), [TaskKey(0, 0)])
                    clock.now = .001
                    backend.complete(TaskKey(0, 0))
                    advance()
                    self.assertEqual(list(backend.pending), [TaskKey(0, 1), TaskKey(0, 2)])
                    self.assertEqual(engine.capacity.usage().active_work, 9)

    def test_unchanged_capacity_does_not_retry_or_report_immediate_work(self):
        for registered in (False, True):
            with self.subTest(registered=registered):
                engine, session, backend, clock, events, advance = self.make_case(registered)
                session.offer([task(i, estimated_work=w) for i, w in enumerate((6, 5, 4))])
                advance()
                blocks = [e for e in events if e[0] == 'dispatch_capacity_blocked']
                self.assertEqual(len(blocks), 1)
                for now in (.001, .02, .04):
                    clock.now = now
                    engine.wake.notify()
                    progress = advance()
                    self.assertFalse(progress.has_immediate_work)
                self.assertEqual([e for e in events if e[0] == 'dispatch_capacity_blocked'], blocks)
                self.assertEqual(list(backend.pending), [TaskKey(0, 0)])

    def test_terminal_and_wake_preserve_backend_refusal_backoff(self):
        for registered in (False, True):
            with self.subTest(registered=registered):
                engine, session, backend, clock, events, advance = self.make_case(registered)
                session.offer([task(0, estimated_work=6)])
                advance()
                backend.acceptance = Acceptance.NOT_ACCEPTED
                session.offer([task(1, estimated_work=4)])
                advance()
                attempts = len(backend.routes)
                backend.acceptance = Acceptance.ACCEPTED
                backend.complete(TaskKey(0, 0))
                clock.now = .001
                engine.wake.notify()
                advance()
                self.assertEqual(len(backend.routes), attempts)
                clock.now = .01
                advance()
                self.assertEqual(list(backend.pending), [TaskKey(0, 1)])

    def test_partial_release_keeps_selected_member_waiting(self):
        for registered in (False, True):
            with self.subTest(registered=registered):
                engine, session, backend, clock, events, advance = self.make_case(registered)
                session.offer([task(i, estimated_work=w) for i, w in enumerate((6, 1, 5, 3))])
                advance()
                # Request capacity initially blocks the flow; freeing one request
                # exposes the larger FIFO member while the smaller member fits.
                backend.complete(TaskKey(0, 1))
                clock.now = .001
                advance()
                blocks = [e for e in events if e[0] == 'dispatch_capacity_blocked']
                self.assertEqual(list(backend.pending), [TaskKey(0, 0)])
                self.assertTrue(blocks)
                clock.now = .02
                self.assertFalse(advance().has_immediate_work)
                self.assertEqual([e for e in events if e[0] == 'dispatch_capacity_blocked'], blocks)
                backend.complete(TaskKey(0, 0))
                advance()
                self.assertEqual(list(backend.pending), [TaskKey(0, 2), TaskKey(0, 3)])

    def test_selection_deferral_after_capacity_release_gets_its_own_retry(self):
        for registered in (False, True):
            with self.subTest(registered=registered):
                engine, session, backend, clock, events, advance = self.make_case(registered)
                session.offer([task(i, estimated_work=w) for i, w in enumerate((6, 5, 4))])
                advance()
                calls = []
                def defer(candidates):
                    calls.append(candidates)
                    return None
                engine.policies = replace(engine.policies, choose_task=defer)
                backend.complete(TaskKey(0, 0))
                clock.now = .001
                advance()
                self.assertEqual(len(calls), 1)
                clock.now = .002
                engine.wake.notify()
                self.assertFalse(advance().has_immediate_work)
                self.assertEqual(len(calls), 1)

    def test_other_job_release_makes_waiting_flow_immediately_eligible(self):
        original, _, backend, clock = setup(held_tasks=8, input_bytes=32, result_bytes=32,
                                             active_requests=2, active_work=10, poll_interval_s=.01)
        engine = SessionEngine(original.capacity.limits, backend, original.policies, clock=clock, max_jobs=2)
        first = engine.register_job('first', JobBudget(2, 8, 8, 2, 10))
        second = engine.register_job('second', JobBudget(2, 8, 8, 2, 10))
        a = engine.open(SessionSpec(first.job_id, 'flow', 'fixture'), job=first)
        b = engine.open(SessionSpec(second.job_id, 'flow', 'fixture'), job=second)
        a.offer([task(0, estimated_work=6)])
        engine.advance()
        b.offer([task(0, estimated_work=5), task(1, estimated_work=4)])
        self.assertFalse(engine.advance().has_immediate_work)
        backend.complete(TaskKey(a.session_id, 0))
        clock.now = .001
        # Spending this tick's one action on polling must still advertise ready work.
        self.assertTrue(engine.advance(1).has_immediate_work)
        engine.advance()
        self.assertEqual(list(backend.pending), [TaskKey(b.session_id, 0), TaskKey(b.session_id, 1)])
