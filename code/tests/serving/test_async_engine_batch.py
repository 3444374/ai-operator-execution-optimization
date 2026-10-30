"""Atomic bulk data frames over the original private CPU child sockets."""
import time
import unittest
from src.serving.backends.vllm_engine import GenerationSpec
from tests.serving import test_async_engine_process as fixture


class EngineBatchTests(unittest.TestCase):
    def setUp(self):
        self.helper = fixture.AsyncProcessTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)

    def test_bulk_has_one_frame_and_fast_terminal_does_not_wait_for_slow_member(self):
        owner = self.helper.owner()
        outcome = owner.submit_batch([fixture.row(0, 'hold'), fixture.row(1)],
            generation=GenerationSpec(), deadline_monotonic=time.monotonic()+5)
        self.assertEqual(outcome.status, 'queued')
        self.assertEqual(owner.snapshot()['submitted_batch_frames'], 1)
        self.helper.until(owner, lambda: owner.snapshot()['ready_results'] > 0)
        fast = owner.poll()[0]
        self.assertEqual(fast.request_id, outcome.request_ids[1])
        self.assertEqual(owner.snapshot()['active_requests'], 1)
        owner.abort(outcome.request_ids[0])
        self.helper.until(owner, lambda: owner.snapshot()['ready_results'] > 0)
        owner.poll()
        self.assertEqual(owner.snapshot()['input_bytes'], 0)
        self.assertEqual(owner.snapshot()['held_requests'], 0)

    def test_oversized_batch_has_zero_member_acceptance_and_no_replay(self):
        owner = self.helper.owner()
        outcome = owner.submit_batch([fixture.row(i) for i in range(3)], generation=GenerationSpec(),
            deadline_monotonic=time.monotonic()+5)
        self.assertEqual(outcome.status, 'backpressure')
        self.assertEqual(owner.snapshot()['issued_submissions'], 0)
        self.assertEqual(owner.snapshot()['held_requests'], 0)
        self.assertEqual(outcome.request_ids, ())

    def test_unknown_member_keeps_its_reservation_after_known_peer_releases(self):
        owner = self.helper.owner()
        outcome = owner.submit_batch([fixture.row(0, 'unknown'), fixture.row(1)],
            generation=GenerationSpec(), deadline_monotonic=time.monotonic()+5)
        self.helper.until(owner, lambda: owner.snapshot()['ready_results'] == 2)
        events = owner.poll(2)
        self.assertEqual(sum(not e.engine_terminal_confirmed for e in events), 1)
        self.assertEqual(owner.snapshot()['held_requests'], 1)
        self.assertGreater(owner.snapshot()['input_bytes'], 0)
        report = owner.close(timeout=3)
        self.assertEqual(report['status'], 'closed')
        self.assertEqual(report['retired_unknown_requests'], 1)
