"""Keep original native identity and age across a pending RPC; CPU only."""
from concurrent.futures import Future
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import records_native_sampler as sampling
from src.execution_provider.adapters import records_request_feedback as wiring
from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider
from tests.execution_provider.test_core_feedback_ray_future import (
    bind_feedback, eligible_samples, make_feedback, native_frame,
)


class PendingFrameTests(unittest.TestCase):
    def setUp(self):
        clock = SimpleNamespace(monotonic=lambda: self.now)
        self.enterContext(patch.object(sampling, 'time', clock))
        self.enterContext(patch.object(wiring, 'time', clock))
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
        with patch('src.scheduling.runtime.observations.threading.Thread'):
            self.feedback = make_feedback(self.owner,session=7,poll_interval_s=.01,
                max_sample_age_s=.5,provider_type=NonBlockingMetricsObservationProvider)
        self.provider = self.feedback.provider
        self.provider.clock = lambda: self.now
        self.provider._thread.is_alive.return_value = False
        self.addCleanup(self.feedback.stop)

    def frame(self, *, sequence=1, stamp=None, session=7, uid='CPU-pending-model'):
        return native_frame(session=session,uid=uid,sequence=sequence,
            source_at_s=self.now if stamp is None else stamp,captured_at_s=self.now)

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
        self.assertEqual(self.feedback.requests, 2)

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
        self.assertEqual(eligible_samples(self.feedback), 1)
        self.assertEqual(len(self.refs), 2)

    def test_a_frame_without_sequence_is_not_republished_as_another_sample(self):
        self.publish(self.frame(sequence=None))
        self.assertFalse(self.provider.latest(2).fresh)
        self.now = 10.02
        self.tick()
        self.assertIsNone(self.provider._snapshot)
        self.assertFalse(self.provider.latest(2).fresh)

    def test_public_sample_reuses_the_original_native_identity_while_pending(self):
        self.publish()
        original = self.feedback.last_native
        for _ in range(3):
            self.assertIs(self.feedback.sample(), original)
        self.tick()
        self.assertTrue(self.provider.latest(2).fresh)

    def test_expired_pending_RPC_keeps_its_ref_without_refreshing_prior_source_age(self):
        self.publish()
        original = self.provider._snapshot
        self.now = 10.02
        self.tick()
        ref, future = self.refs[-1]
        self.feedback.expiry = self.now
        self.tick(); self.tick()
        self.assertIs(self.provider._snapshot, original)
        self.assertAlmostEqual(self.provider.latest(2).sample_age_s, .02)
        self.assertEqual(self.cancels, [ref])
        self.assertIs(self.feedback.ref, ref)
        self.assertIs(self.feedback.future, future)
        self.assertFalse(future.done())
        self.assertEqual(len(self.refs), 2)
        self.now = 10.6
        self.assertFalse(self.provider.latest(2).fresh)

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

    def test_new_query_cannot_republish_the_previous_source(self):
        self.publish()
        self.now = 10.02
        self.tick()
        self.feedback.pause()
        bind_feedback(self.feedback, 8)
        old = self.refs[-1][1]
        old.set_result(self.frame())
        self.tick(); self.tick()
        self.assertIsNone(self.provider._snapshot)
        self.refs[-1][1].set_result(self.frame(session=8))
        self.tick()
        self.tick()
        observation, source = self.provider.latest_with_source(2)
        self.assertTrue(observation.fresh)
        self.assertEqual(source.source_session_id, 8)
        self.assertEqual(source.source_uid, 'CPU-pending-model')

    def test_rejected_future_timestamp_cannot_become_fresh_during_a_later_pending_read(self):
        self.publish(self.frame(stamp=10.03))
        self.assertIsNone(self.provider._snapshot)
        self.now = 10.02
        self.tick()
        self.now = 10.04
        self.tick()
        self.assertFalse(self.provider.latest(2).fresh)
        self.assertIsNone(self.provider._snapshot)

    def test_receipt_clock_error_collects_known_RPC_without_qualifying_unknown_age(self):
        self.tick()
        self.refs[0][1].set_result(self.frame())
        with patch.object(sampling.time, 'monotonic', side_effect=RuntimeError('CPU audit clock unavailable')):
            self.tick()
        self.assertIsNone(self.feedback.ref)
        receipt, = self.feedback.snapshot_rpc_receipts
        self.assertTrue(receipt['future_done_confirmed'])
        self.assertEqual(receipt['rpc_terminal_status'], 'returned')
        self.assertEqual(receipt['frame_disposition'], 'invalid_frame')
        self.now = 10.02
        self.tick()
        self.assertFalse(self.provider.latest(2).fresh)
        self.assertIsNone(self.feedback.last_native)


if __name__ == '__main__':
    unittest.main()
