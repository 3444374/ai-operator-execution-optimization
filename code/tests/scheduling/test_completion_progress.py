"""Bounded backend drains over the actual Core; no model or HTTP work."""

from dataclasses import replace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.native_tasks import NativeTaskSession
from src.scheduling.core.session_contract import State, Uncertain, Usage
from src.scheduling.submission_control.admission import StaticAdmissionController
from tests.execution_provider.test_native_tasks import fixture, task


class CompletionProgressTests(unittest.TestCase):
    def setUp(self):
        self.execution, self.backend, self.clock = fixture(
            held_tasks=32, input_bytes=32768, result_bytes=16384,
            active_requests=32, active_work=32, offer_tasks=32,
            step_actions=8, poll_interval_s=.01)
        self.engine = self.execution.engine
        self.engine.policies = replace(self.engine.policies, admission=StaticAdmissionController(32))
        self.flow = NativeTaskSession(self.execution, 'query', 'map')
        self.assertEqual(self.flow.offer(tuple(task(i) for i in range(32))).accepted_prefix_count, 32)
        self.flow.end_input()
        for _ in range(4):
            progress = self.flow.advance(32)
        self.assertFalse(progress.has_immediate_work)
        self.assertEqual(len(self.backend.pending), 32)
        self.assertFalse(any(r.phase == 'QUEUED' for r in self.engine.capacity.records.values()))

    def tearDown(self):
        self.flow.close()
        for key in tuple(self.backend.pending):
            self.backend.complete(key)
        for _ in range(12):
            self.engine.advance()
        self.assertEqual(self.engine.capacity.usage(), Usage())
        self.assertFalse(self.engine.jobs.jobs)

    def complete(self, count):
        for key in tuple(self.backend.pending)[:count]:
            self.backend.complete(key)
            self.engine.wake.notify()

    def release(self, progress):
        self.flow.release(tuple(d.lease_id for d in progress.deliveries))

    def test_full_poll_continues_draining_completed_tail_without_waiting(self):
        self.complete(32)
        with patch.object(self.engine.wake, 'wait') as wait:
            for remaining in (24, 16, 8, 0):
                progress = self.flow.advance(32)
                self.assertEqual(len(progress.deliveries), 8)
                self.assertEqual(len(self.backend.events), remaining)
                self.assertEqual(progress.generation, self.engine.wake.generation)
                self.assertTrue(progress.has_immediate_work)
                self.flow.wait(progress)
                wait.assert_not_called()
                self.release(progress)
            self.assertFalse(self.flow.advance(32).has_immediate_work)

    def test_exact_full_poll_prompts_only_one_extra_check_then_empty_poll_can_wait(self):
        self.complete(8)
        progress = self.flow.advance(32)
        self.assertTrue(progress.has_immediate_work)
        self.release(progress)
        progress = self.flow.advance(32)
        self.assertFalse(progress.has_immediate_work)
        self.assertEqual(self.engine.capacity.usage().active_requests, 24)
        self.assertEqual(progress.next_deadline, self.clock.now + .01)
        with patch.object(self.engine.wake, 'wait') as wait:
            self.flow.wait(progress)
        wait.assert_called_once_with(progress.generation, .01)

    def test_partial_poll_can_wait_with_unfinished_remote_work(self):
        self.complete(7)
        progress = self.flow.advance(32)
        self.assertEqual(len(progress.deliveries), 7)
        self.assertFalse(progress.has_immediate_work)
        self.release(progress)
        self.assertEqual(self.engine.capacity.usage().active_requests, 25)

    def test_smaller_requested_action_count_uses_its_own_poll_limit(self):
        self.complete(32)
        with patch.object(self.backend, 'poll', wraps=self.backend.poll) as poll:
            progress = self.engine.advance(3)
        self.assertEqual(poll.call_args.args[1], 3)
        self.assertEqual(progress.events, 3)
        self.assertTrue(progress.has_immediate_work)
        self.assertEqual(len(self.backend.events), 29)
        deliveries = self.flow.session.advance(32)
        self.assertEqual(len(deliveries.deliveries), 3)
        self.release(deliveries)

    def test_uncertain_notices_and_bounded_cancel_finish_then_empty_poll_can_wait(self):
        for key, (handle, _task) in self.backend.pending.items():
            self.backend.events.append(Uncertain(key, handle))
            self.engine.wake.notify()
        for _ in range(4):
            progress = self.flow.advance(32)
            self.assertEqual(progress.state, State.FAILED)
            self.assertTrue(progress.has_immediate_work)
        for _ in range(4):
            before = len(self.backend.cancelled)
            progress = self.flow.advance(32)
            self.assertEqual(len(self.backend.cancelled) - before, 8)
        self.assertFalse(progress.has_immediate_work)
        self.assertFalse(self.flow.advance(32).has_immediate_work)
        self.assertEqual(self.engine.capacity.usage().active_requests, 32)
        self.assertEqual(len(set(self.backend.cancelled)), 32)

    def test_cancelled_completed_tail_still_drains_with_original_action_limit(self):
        self.complete(32)
        self.flow.request_cancel()
        for remaining in (24, 16, 8, 0):
            progress = self.flow.advance(32)
            self.assertEqual(progress.state, State.CANCELLED)
            self.assertFalse(progress.deliveries)
            self.assertEqual(len(self.backend.events), remaining)
            self.assertTrue(progress.has_immediate_work)
        self.assertFalse(self.flow.advance(32).has_immediate_work)
        self.assertEqual(self.engine.capacity.usage(), Usage())


if __name__ == '__main__':
    unittest.main()
