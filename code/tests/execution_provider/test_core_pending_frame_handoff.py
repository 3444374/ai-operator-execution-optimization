"""Keep an original source frame across a pending legacy RPC; CPU only."""
from concurrent.futures import Future
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core


class PendingFrameTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.object(core, 'time', SimpleNamespace(monotonic=lambda: self.now)))
        self.make_feedback()

    def make_feedback(self):
        self.now = 10.
        self.refs, self.cancels = refs, cancels = [], []
        def remote():
            future = Future()
            ref = SimpleNamespace(future=lambda: future)
            refs.append((ref, future))
            return ref
        self.owner = SimpleNamespace(resources=SimpleNamespace(max_active_batches=4),
            _actors=SimpleNamespace(model_ready=dict(engine_uid='CPU-pending-model'),
                model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
                ray=SimpleNamespace(cancel=lambda ref, **kw: cancels.append(ref))))
        self.feedback = core._CoreFeedback(self.owner,
            dict(kind='legacy_resource_feedback', poll_interval_s=.01))
        with patch('src.scheduling.runtime.observations.threading.Thread'):
            self.feedback.begin(128, 2)
        self.feedback.stream = SimpleNamespace(session=SimpleNamespace(session_id=7))
        self.provider = self.feedback.provider
        self.provider.clock = lambda: self.now
        self.provider._thread.is_alive.return_value = False
        self.addCleanup(self.feedback.stop)

    def frame(self, *, sequence=1, stamp=None, session=7, uid='CPU-pending-model'):
        return dict(session_id=session, ipc=dict(native_feedback=dict(engine_uid=uid,
            sequence=sequence, observed_at_s=self.now if stamp is None else stamp,
            running=2, waiting=0, kv_usage=.1, preemptions_total=0, generation_tokens_total=16)))

    def tick(self):
        self.provider._sample_once(self.provider.sampler)

    def publish(self, frame=None):
        self.tick()
        self.assertEqual(len(self.refs), 1)
        self.refs[0][1].set_result(self.frame() if frame is None else frame)
        self.now = 10.01
        self.tick()

    def test_pending_regular_poll_keeps_the_unconsumed_frame_and_original_RPC(self):
        self.publish()
        original = self.provider._snapshot
        generation = self.provider._sample_generation
        self.now = 10.02
        self.tick()
        ref, future = self.refs[-1]
        observation, source = self.provider.latest_with_source(2)
        self.assertTrue(observation.fresh)
        self.assertIs(source, original)
        self.assertAlmostEqual(observation.sample_age_s, .02)
        self.assertEqual(self.provider._sample_generation, generation)
        self.assertIs(self.feedback.ref, ref)
        self.assertIs(self.feedback.future, future)
        self.assertFalse(future.done())
        self.assertEqual(self.feedback.snapshot_requests, 2)

    def test_pending_polls_never_refresh_age_rate_or_consume_once_generation(self):
        self.publish()
        first = self.provider.latest(2)
        self.assertTrue(first.fresh)
        sampled = self.provider._sampled_at_s
        generation = self.provider._sample_generation
        rate = self.provider._service_rate_tokens_s_per_endpoint
        for stamp in (10.02, 10.03, 10.04):
            self.now = stamp
            self.tick()
            self.assertFalse(self.provider.latest(2).fresh)
            self.assertEqual(self.provider._sampled_at_s, sampled)
            self.assertEqual(self.provider._sample_generation, generation)
            self.assertEqual(self.provider._service_rate_tokens_s_per_endpoint, rate)
        self.now = 10.6
        self.assertFalse(self.provider.latest(2).fresh)
        self.assertEqual(self.feedback.samples, 1)
        self.assertEqual(len(self.refs), 2)

    def test_retained_age_still_allows_the_controller_to_reject_an_old_frame(self):
        self.publish()
        self.now = 10.02
        self.tick()
        observation = self.provider.latest(2)
        self.assertTrue(observation.fresh)
        self.assertAlmostEqual(observation.sample_age_s, .02)
        options = replace(self.feedback.options, search_mode='geometric',
            pressure_response='sample', window_s=.01, allow_exploration=True)
        controller = core.ResourceFeedbackController(4, 2, options, clock=lambda: self.now)
        controller.reset(2, 128)
        decision = controller.update(observation)
        self.assertEqual(decision.reason, 'missing_or_stale_feedback')
        self.assertEqual(decision.window, 2)
        self.assertEqual(controller.report()['changes'], 0)

    def test_a_frame_without_sequence_is_not_republished_as_another_sample(self):
        self.publish(self.frame(sequence=None))
        self.assertTrue(self.provider.latest(2).fresh)
        self.now = 10.02
        self.tick()
        self.assertIsNone(self.provider._snapshot)
        self.assertFalse(self.provider.latest(2).fresh)

    def test_public_sample_retains_its_pending_None_result(self):
        self.publish()
        for _ in range(3):
            self.assertIsNone(self.feedback.sample())
        self.tick()
        self.assertTrue(self.provider.latest(2).fresh)

    def test_expired_pending_RPC_clears_observation_but_keeps_its_original_ref(self):
        self.publish()
        self.now = 10.02
        self.tick()
        ref, future = self.refs[-1]
        self.feedback.ref_deadline = self.now
        self.tick(); self.tick()
        self.assertFalse(self.provider.latest(2).fresh)
        self.assertIsNone(self.provider._snapshot)
        self.assertEqual(self.cancels, [ref])
        self.assertIs(self.feedback.ref, ref)
        self.assertIs(self.feedback.future, future)
        self.assertFalse(future.done())
        self.assertEqual(len(self.refs), 2)

    def test_invalid_return_or_exception_does_not_restore_an_older_frame(self):
        for kind in ('missing', 'session', 'model', 'exception'):
            with self.subTest(kind=kind):
                self.make_feedback()
                self.publish()
                self.now = 10.02
                self.tick()
                future = self.refs[-1][1]
                if kind == 'exception':
                    future.set_exception(RuntimeError('CPU snapshot read failed'))
                else:
                    frame = ({} if kind == 'missing' else self.frame(session=99)
                        if kind == 'session' else self.frame(uid='another-model'))
                    future.set_result(frame)
                self.tick()
                self.assertIsNone(self.provider._snapshot)
                self.tick()
                self.assertFalse(self.provider.latest(2).fresh)
                self.assertIsNone(self.provider._snapshot)

    def test_model_change_and_new_query_cannot_republish_previous_source(self):
        self.publish()
        self.owner._actors.model_ready['engine_uid'] = 'new-CPU-model'
        self.now = 10.02
        self.tick()
        self.assertIsNone(self.provider._snapshot)
        self.feedback.pause()
        self.feedback.begin(128, 2)
        self.feedback.stream = SimpleNamespace(session=SimpleNamespace(session_id=8))
        old = self.refs[-1][1]
        old.set_result(self.frame())
        self.tick(); self.tick()
        self.assertIsNone(self.provider._snapshot)
        self.refs[-1][1].set_result(self.frame(session=8, uid='new-CPU-model'))
        self.tick()
        self.tick()
        observation, source = self.provider.latest_with_source(2)
        self.assertTrue(observation.fresh)
        self.assertEqual(source.source_session_id, 8)
        self.assertEqual(source.source_uid, 'new-CPU-model')

    def test_rejected_future_timestamp_cannot_become_fresh_during_a_later_pending_read(self):
        self.publish(self.frame(stamp=10.03))
        self.assertIsNone(self.provider._snapshot)
        self.now = 10.02
        self.tick()
        self.now = 10.04
        self.tick()
        self.assertFalse(self.provider.latest(2).fresh)
        self.assertIsNone(self.provider._snapshot)

    def test_optional_receipt_clock_error_does_not_remove_a_valid_pending_frame(self):
        self.tick()
        self.refs[0][1].set_result(self.frame())
        with patch.object(core.time, 'monotonic', side_effect=RuntimeError('CPU audit clock unavailable')):
            self.tick()
        self.now = 10.02
        self.tick()
        self.assertTrue(self.provider.latest(2).fresh)
        self.assertIsNone(self.provider._snapshot.received_at_s)
        self.assertEqual(self.feedback.sample_errors, 0)


if __name__ == '__main__':
    unittest.main()
