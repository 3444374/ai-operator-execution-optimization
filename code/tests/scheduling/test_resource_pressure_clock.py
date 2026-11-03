"""CPU-only checks for pressure response independent of throughput windows."""
from dataclasses import replace
from types import SimpleNamespace
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.adaptive import (
    ResourceFeedbackConfig, ResourceFeedbackController,
)
from src.scheduling.submission_control.stage_work import SupplyState
from src.scheduling.submission_control.admission import DynamicAdmissionGate
from tests.scheduling.test_ascending_request_scan import ScanTrace
from tests.scheduling.test_incremental_session import setup, task


class ResourcePressureClockTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.
        self.controller = ResourceFeedbackController(8, 8,
            ResourceFeedbackConfig(window_s=10, poll_interval_s=.1),
            clock=lambda: self.now)
        self.controller.reset(8, 100)

    def observe(self, at_s, **changes):
        self.now = at_s
        observation = AdmissionObservation(at_s, True, 8, 8, 0, .1,
            sample_age_s=0., preemptions_total=0, pending_items=100)
        return self.controller.update(replace(observation, **changes))

    def test_memory_pressure_reduces_before_the_first_performance_window(self):
        self.observe(.05)
        decision = self.observe(.1, memory_pressure=True)
        self.assertEqual((decision.window, decision.reason), (4, 'resource_pressure'))
        self.assertEqual(self.controller.report()['measurements'], [])

    def test_preemption_increment_reduces_before_the_performance_window(self):
        self.observe(.05, preemptions_total=20)
        decision = self.observe(.1, preemptions_total=21)
        self.assertEqual((decision.window, decision.reason), (4, 'resource_pressure'))

    def test_repeated_sample_cannot_reduce_again_when_the_window_closes(self):
        self.observe(.05)
        self.observe(.1, memory_pressure=True)
        decision = self.observe(.2, sample_age_s=.1, memory_pressure=True)
        self.assertEqual(decision.window, 4)
        self.assertEqual(self.controller.report()['changes'], 1)

    def test_sample_sequence_is_stable_when_the_consumer_clock_changes(self):
        self.observe(.1, sample_sequence=1, memory_pressure=True)
        decision = self.observe(10.2, sample_sequence=1, memory_pressure=True)
        self.assertEqual(decision.window, 4)
        self.assertEqual(self.controller.report()['changes'], 1)
        decision = self.observe(10.3, sample_sequence=2, memory_pressure=True)
        self.assertEqual(decision.window, 2)

    def test_same_preemption_counter_cannot_repeat_the_increment(self):
        self.observe(.01, preemptions_total=20, sample_sequence=1)
        self.observe(.02, preemptions_total=21, sample_sequence=2)
        self.assertEqual(self.observe(.03, preemptions_total=21, sample_sequence=3).window, 4)
        self.assertEqual(self.observe(.04, preemptions_total=22, sample_sequence=4).window, 2)

    def test_counter_restart_is_not_a_new_preemption(self):
        self.observe(.01, preemptions_total=20)
        self.assertEqual(self.observe(.02, preemptions_total=0).window, 8)
        self.assertEqual(self.observe(.03, preemptions_total=1).window, 4)

    def test_stale_sequence_does_not_consume_a_later_fresh_sample(self):
        self.observe(.1, sample_sequence=1, memory_pressure=True, fresh=False)
        self.assertEqual(self.observe(.2, sample_sequence=1, memory_pressure=True).window, 4)

    def test_out_of_order_sample_cannot_replace_the_preemption_counter(self):
        self.observe(.01, preemptions_total=20, sample_sequence=4)
        self.observe(.02, preemptions_total=0, sample_sequence=3)
        self.assertEqual(self.observe(.03, preemptions_total=20, sample_sequence=5).window, 8)

    def test_pressure_at_minimum_discards_the_old_measurement_anchor(self):
        self.controller.reset(1, 100)
        self.controller._anchor = (0., 0, 0, 0, None, None)
        self.controller._windows = [(1., 1., 1., None, 1, 1.)]
        self.assertEqual(self.observe(.1, memory_pressure=True).reason, 'at_minimum')
        self.assertIsNone(self.controller._anchor)
        self.assertEqual(self.controller._windows, [])

    def test_pressure_wins_over_backpressure_and_an_expired_probe_budget(self):
        self.controller.options = replace(self.controller.options, exploration_s=.01)
        decision = self.observe(.1, memory_pressure=True, consumer_backpressure=True)
        self.assertEqual((decision.window, decision.reason), (4, 'resource_pressure'))

    def test_explicit_historical_window_response_still_waits(self):
        self.controller.options = replace(self.controller.options, pressure_response='window')
        self.assertEqual(self.observe(.1, memory_pressure=True).window, 8)
        self.assertEqual(self.observe(10., memory_pressure=True).window, 4)

    def test_invalid_pressure_mode_and_sample_sequence_are_rejected(self):
        with self.assertRaises(ValueError):
            ResourceFeedbackConfig(pressure_response='timer')
        for value in (-1, True, .5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.observe(.1, sample_sequence=value)

    def test_old_or_unknown_samples_do_not_establish_pressure(self):
        for changes in (dict(fresh=False), dict(sample_age_s=11),
                        dict(running=None, waiting=None, kv_usage=None)):
            with self.subTest(changes=changes):
                self.setUp()
                decision = self.observe(12, memory_pressure=True, **changes)
                self.assertEqual(decision.window, 8)
                self.assertEqual(self.controller.report()['changes'], 0)


class AscendingPressureClockTests(unittest.TestCase):
    def test_early_pressure_clears_reference_even_with_no_ready_work_and_replays_once(self):
        trace = ScanTrace()
        trace.run_until(lambda report, _: report['window'] >= 4)
        controller = trace.controller
        before, measured = controller.current_window, len(controller.measurements)
        trace.now += .01
        observation = AdmissionObservation(trace.now, True, before, before, 0, .1,
            sample_age_s=0, sample_sequence=1000, memory_pressure=True,
            completed_items_total=trace.completed, pending_items=10000,
            preemptions_total=0, transition_pending=before)
        supply = SupplyState(trace.now, 0, trace.completed, trace.completed, 1,
            True, False, capacity_blocked='result_bytes', active_requests=before)
        decision = controller.update_supply(observation, supply)
        self.assertEqual((decision.window, decision.reason), (before//2, 'resource_pressure'))
        self.assertIsNone(controller._reference)
        self.assertEqual(len(controller.measurements), measured)
        repeated = controller.update_supply(replace(observation, observed_at_s=trace.now+.01),
            replace(supply, observed_at_s=trace.now+.01))
        self.assertEqual((repeated.window, repeated.reason), (before//2, 'repeated_feedback_sample'))

    def test_missing_native_feedback_does_not_clear_a_prior_pressure_sample(self):
        trace = ScanTrace()
        trace.run_until(lambda report, _: report['window'] >= 4)
        controller = trace.controller
        before = controller.current_window
        self.assertEqual(trace.tick(0, memory_pressure=True).window, before//2)
        self.assertEqual(trace.tick(0, fresh=False, memory_pressure=True).window, before//2)


class SessionPressureAccountingTests(unittest.TestCase):
    def test_pressure_shrink_keeps_old_core_tasks_charged_until_their_replies(self):
        engine, session, backend, clock = setup(held_tasks=5, input_bytes=20,
            result_bytes=20, active_requests=4, active_work=4, offer_tasks=5, step_actions=64)
        controller = ResourceFeedbackController(4, 4,
            ResourceFeedbackConfig(window_s=10, poll_interval_s=.1), clock=clock)
        controller.reset(4, 5)
        observation = AdmissionObservation(0., True, 0, 4, 0, .1,
            sample_age_s=0, sample_sequence=1, preemptions_total=0)
        provider = SimpleNamespace(latest=lambda inflight, **_: replace(observation, inflight=inflight))
        gate = DynamicAdmissionGate(controller, provider, min_update_interval_s=.1, clock=clock)
        engine.policies = replace(engine.policies, admission=gate)
        session.offer(tuple(task(index) for index in range(5)))
        session.seal()
        session.advance(5)
        self.assertEqual(len(backend.pending), 4)
        before = engine.capacity.usage()
        clock.now = .1
        observation = replace(observation, observed_at_s=.1, sample_sequence=2, preemptions_total=1)
        self.assertFalse(gate.decide(before.active_requests).allowed)
        session.advance(5)
        self.assertEqual(controller.current_window, 2)
        self.assertEqual(engine.capacity.usage(), before)
        self.assertEqual(backend.cancelled, [])
        initial = tuple(backend.pending)
        for key in initial[:2]:
            backend.complete(key)
        deliveries = list(session.advance(5).deliveries)
        self.assertEqual(len(backend.pending), 2)
        session.release(tuple(delivery.lease_id for delivery in deliveries))
        backend.complete(initial[2])
        clock.now = .2
        deliveries.extend(session.advance(5).deliveries)
        self.assertEqual(len(backend.pending), 2)
        self.assertEqual({key.sequence for key in backend.pending}, {3, 4})
        for key in tuple(backend.pending):
            backend.complete(key)
        final = session.advance(5).deliveries
        session.release(tuple(delivery.lease_id for delivery in deliveries[2:]+list(final)))
        deliveries.extend(final)
        self.assertEqual({delivery.key.sequence for delivery in deliveries}, set(range(5)))
        self.assertEqual(len(deliveries), 5)
        self.assertEqual(controller.report()['changes'], 1)
        self.assertEqual(engine.capacity.usage().held_tasks, 0)
        self.assertEqual(backend.cancelled, [])
        session.close()
