"""RPC-ready collection must not consume the next scheduled dispatch turn."""
from concurrent.futures import Future
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import records_native_sampler as sampling
from src.execution_provider.adapters import records_request_feedback as wiring
from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider
from src.scheduling.runtime import observations
from tests.execution_provider.test_core_feedback_ray_future import make_feedback, native_frame
from tests.scheduling.test_incremental_session import setup, task


class FeedbackCadenceTests(unittest.TestCase):
    def setUp(self):
        self.now = 10.
        clock = SimpleNamespace(monotonic=lambda: self.now)
        self.enterContext(patch.object(sampling, 'time', clock))
        self.enterContext(patch.object(wiring, 'time', clock))
        self.refs = []
        def remote(**kwargs):
            future = Future()
            ref = SimpleNamespace(future=lambda: future)
            self.refs.append((ref, future, self.now))
            return ref
        self.owner = SimpleNamespace(resources=SimpleNamespace(max_active_batches=4),
            _actors=SimpleNamespace(model_ready=dict(engine_uid='CPU-cadence'),
                model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
                ray=SimpleNamespace(cancel=lambda ref, **kw: None)))
        with patch('src.scheduling.runtime.observations.threading.Thread'):
            self.feedback = make_feedback(self.owner, poll_interval_s=.25,
                max_sample_age_s=.75, provider_type=NonBlockingMetricsObservationProvider)
        self.provider = self.feedback.provider
        self.provider.clock = lambda: self.now
        self.provider._thread.is_alive.return_value = False
        self.addCleanup(self.feedback.stop)

    def finish_rpc(self, sequence, *, session=7):
        self.refs[-1][1].set_result(native_frame(session=session, uid='CPU-cadence',
            sequence=sequence, source_at_s=self.now, captured_at_s=self.now))
        self.provider._sample_once(self.feedback.collect_ready, ready=True)

    def tick(self):
        self.provider._sample_once(self.feedback.sample)

    def test_provider_loop_keeps_dispatch_deadlines_when_ready_notifications_interrupt_wait(self):
        remote = self.owner._actors.model_actor.snapshot.remote
        def counted_remote(**kwargs):
            self.assertFalse(any(not future.done() for _, future, _ in self.refs),
                'a second RPC cannot replace unfinished remote ownership')
            ref = remote(**kwargs)
            if len(self.refs) == 4:
                self.provider._stop.set()
            return ref
        self.owner._actors.model_actor.snapshot.remote = counted_remote
        def wait(remaining):
            future = self.refs[-1][1]
            if not future.done():
                self.now += .01
                future.set_result(native_frame(session=7, uid='CPU-cadence', sequence=len(self.refs),
                    source_at_s=self.now, captured_at_s=self.now))
                return True
            self.now += remaining
            return False
        with patch.object(observations, 'time', SimpleNamespace(monotonic=lambda: self.now)), \
                patch.object(self.provider._ready_event, 'wait', side_effect=wait):
            self.provider._sample_loop()
        self.assertEqual([stamp for _, _, stamp in self.refs], [10., 10.25, 10.5, 10.75])
        self.assertEqual([row['request_index'] for row in self.feedback.snapshot_rpc_receipts], [1, 2, 3])

    def test_invalid_or_duplicate_sources_do_not_wake_control_or_make_old_work_fresh(self):
        self.tick()
        self.now = 10.01
        self.finish_rpc(1)
        self.assertTrue(self.provider.latest(1).fresh)
        generation = self.owner.core.wake.generation
        for value in ('duplicate', 'wrong_session', 'missing_native', 'raised'):
            with self.subTest(value=value):
                self.now += .25
                self.tick()
                future = self.refs[-1][1]
                if value == 'raised':
                    future.set_exception(RuntimeError('fixture snapshot failure'))
                else:
                    frame = ({} if value == 'missing_native' else native_frame(
                        session=8 if value == 'wrong_session' else 7, uid='CPU-cadence', sequence=1,
                        source_at_s=10.01, captured_at_s=10.01))
                    future.set_result(frame)
                self.provider._sample_once(self.feedback.collect_ready, ready=True)
                self.assertEqual(self.owner.core.wake.generation, generation)
                self.assertFalse(self.provider.latest(1).fresh)

    def test_pending_or_locally_cancelled_future_never_allows_replacement_RPC(self):
        self.tick()
        ref, future, _ = self.refs[0]
        for cancelled in (False, True):
            if cancelled:
                future.cancel()
                self.provider._sample_once(self.feedback.collect_ready, ready=True)
            for _ in range(3):
                self.now += .25
                self.tick()
                self.assertEqual(len(self.refs), 1)
                self.assertIs(self.feedback.ref, ref)
                self.assertFalse(self.provider.latest(1).fresh)
        self.feedback.stop()
        report = self.feedback.snapshot_rpc_report()
        self.assertEqual(report['snapshot_rpc_cleanup_status'], 'unknown')
        self.assertEqual(report['snapshot_rpc_receipts'], [])

    def test_close_and_late_result_never_wake_control_or_rewrite_the_original_unknown(self):
        from copy import deepcopy
        self.tick()
        self.feedback.stop()
        first = deepcopy(self.feedback.snapshot_rpc_report())
        generation = self.owner.core.wake.generation
        self.now = 10.01
        self.finish_rpc(1)
        final = self.feedback.snapshot_rpc_report()
        self.assertEqual(self.owner.core.wake.generation, generation)
        self.assertEqual(first['snapshot_rpc_cleanup_status'], 'unknown')
        self.assertEqual(first['snapshot_rpc_receipts'], [])
        row, = final['snapshot_rpc_receipts']
        self.assertEqual((row['rpc_terminal_status'], row['frame_disposition']), ('returned', 'closed'))
        self.assertEqual(len(self.refs), 1)

    def test_early_collection_dispatches_on_each_configured_tick_without_redelivery(self):
        self.tick()
        for sequence in range(1, 4):
            self.now = 10. + (sequence-1)*.25 + .01
            self.finish_rpc(sequence)
            self.assertEqual(len(self.refs), sequence, 'readiness must not issue an extra RPC')
            observation, source = self.provider.latest_with_source(1)
            self.assertTrue(observation.fresh)
            self.assertEqual(source.control_request_index, sequence)
            self.assertFalse(self.provider.latest(1).fresh)
            self.now = 10. + sequence*.25
            self.tick()
            self.assertEqual(len(self.refs), sequence+1,
                'early collection must leave the scheduled turn free to dispatch')
            self.assertFalse(self.provider.latest(1).fresh, 'old source is not a new observation')
        self.assertEqual([stamp for _, _, stamp in self.refs], [10., 10.25, 10.5, 10.75])
        self.assertEqual([row['request_index'] for row in self.feedback.snapshot_rpc_receipts], [1, 2, 3])

    def test_fresh_frame_can_trigger_a_trial_while_the_first_model_request_is_pending(self):
        self.feedback.stop()
        core, session, backend, clock = setup(held_tasks=4, input_bytes=4096, result_bytes=4096,
            item_input_bytes=1024, item_result_bytes=1024, active_requests=4, active_work=4)
        clock.now = self.now
        self.owner.core = core
        controller_type = wiring.ResourceRequestController
        with patch('src.scheduling.runtime.observations.threading.Thread'), \
                patch.object(wiring, 'ResourceRequestController',
                    lambda maximum, options: controller_type(maximum, options, clock=lambda: self.now)):
            self.feedback = wiring.CoreResourceRequestFeedback(self.owner,
                dict(kind='resource_feedback', poll_interval_s=.25, max_sample_age_s=.75))
            self.feedback.begin(4, 1)
            work = dict.fromkeys(('completed_items_total', 'completed_input_work_total',
                'completed_output_work_total', 'completed_computed_input_work_total',
                'completed_cached_input_work_total'), 0)
            stream = SimpleNamespace(session=session, completed_model_work=lambda: dict(work),
                expected=range(4), offered=range(4), source_eof=True, pending=None)
            self.feedback.stream = stream
        self.addCleanup(self.feedback.stop)
        self.provider = self.feedback.provider
        self.provider.clock = lambda: self.now
        self.provider._thread.is_alive.return_value = False
        core.policies = replace(core.policies, admission=self.feedback.gate)
        self.assertEqual(session.offer(tuple(task(i) for i in range(4))).accepted_prefix_count, 4)
        session.seal()
        session.advance(4)
        self.assertEqual(len(backend.pending), 1)
        self.feedback.observe(stream)
        self.assertEqual(self.feedback.next_poll, 10.25)
        self.tick()
        generation = core.wake.generation
        self.now = 10.01
        self.finish_rpc(1, session=session.session_id)
        self.feedback.observe(stream)
        self.assertEqual(self.feedback.gate.limit, 2, 'freshness must not wait for the control poll deadline')
        self.assertGreater(core.wake.generation, generation, 'published feedback must wake the existing Core wait')
        self.assertEqual(work['completed_items_total'], 0)
        self.assertEqual(len(backend.pending), 1, 'feedback itself never submits or completes a model request')
        clock.now = max(self.now, session._retry_at)
        session.advance(4)
        self.assertEqual(len(backend.pending), 2)
        self.assertEqual(self.feedback.controller.report()['accepted_steps'], 0,
            'a provisional trial is not a confirmed performance gain')
