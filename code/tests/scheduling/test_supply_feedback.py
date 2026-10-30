"""Deterministic demand/native-window replays; no hardware benefit inference."""
from dataclasses import replace
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig
from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig, SupplyFeedbackController, SupplyState


class SupplyFeedbackTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.
        self.options = ResourceFeedbackConfig(poll_interval_s=1, window_s=1,
            min_samples=2, min_remaining=0, allow_exploration=True)
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4, self.options), clock=lambda: self.now)
        self.c.reset(1, 10000)

    def tick(self, completed=0, **changes):
        self.now += 1
        o = AdmissionObservation(self.now, True, self.c.current_window,
            self.c.current_window, 0, .1, sample_age_s=0,
            completed_items_total=completed, pending_items=100, preemptions_total=0,
            completed_input_work_total=completed*100, completed_output_work_total=completed*4)
        s = SupplyState(self.now, 100, completed+100, completed+5,
                        self.c.current_window, True, False)
        obs = {k: v for k, v in changes.items() if k in o.__dataclass_fields__}
        state = {k: v for k, v in changes.items() if k in s.__dataclass_fields__ and k not in obs}
        return self.c.update_supply(replace(o, **obs), replace(s, **state))

    def test_missing_stale_and_zero_ready_are_not_idle_resource_proofs(self):
        for changes in [dict(running=None), dict(fresh=False), dict(sample_age_s=2)]:
            for n in range(6):
                self.assertEqual(self.tick(n*4, **changes).reason, 'insufficient_information')
            self.assertEqual(self.c.current_window, 1)
        for n in range(6):
            self.assertEqual(self.tick(n*4, ready_items=0, running=0, kv_usage=0).reason,
                             'upstream_supply_limited')
        self.assertEqual(self.c.report()['changes'], 0)

    def test_work_exhaustion_needs_no_fabricated_engine_metrics(self):
        result = self.tick(ready_items=0, inflight=0, active_batches=0,
                           exhausted=True, upstream_pending=False, running=None)
        self.assertEqual(result.reason, 'work_exhausted')

    def test_backend_queue_is_distinct_from_supply_and_resource_pressure(self):
        self.assertEqual(self.tick(waiting=2).reason, 'backend_queued')
        self.c.current_window = 4
        self.tick(kv_usage=.99)
        self.assertEqual(self.tick(kv_usage=.99).reason, 'resource_pressure')
        self.assertEqual(self.c.current_window, 2)

    def test_declared_capacity_is_not_a_measured_platform(self):
        result = self.tick(capacity_blocked='result_bytes')
        self.assertEqual(result.reason, 'configuration_boundary')
        self.assertFalse(self.c.report()['capacity_is_performance_platform'])

    def test_probe_needs_progress_and_supply_and_stops_at_measured_no_gain(self):
        decisions = [self.tick(n*20) for n in range(30)]
        self.assertTrue(any(d.reason == 'performance_probe' for d in decisions))
        self.assertIn('local_plateau', [d.reason for d in decisions])
        self.assertEqual(self.c.current_window, 1)
        self.assertTrue(any(r['state'] == 'measured_no_gain' for r in self.c.report()['demand_windows']))
        self.assertLessEqual(self.c.report()['changes'], 2)

    def test_one_batch_operational_ceiling_is_reported_as_configuration(self):
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(1, self.options), clock=lambda: self.now)
        self.c.reset(1, 10000)
        for n in range(6):self.tick(n*20)
        self.assertEqual(self.c.report()['stop_reason'], 'capacity_limit')
        self.assertEqual(self.c.report()['demand_windows'][-1]['state'], 'configuration_boundary')

    def test_default_computed_work_requires_real_receipt_counters(self):
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4), clock=lambda: self.now)
        self.c.reset(1, 10000)
        for n in range(30):self.tick(n*20)
        self.assertEqual(self.c.current_window, 1)
        self.assertGreater(self.c.report()['reasons'].get('missing_work_feedback', 0), 0)

    def test_rates_and_stage_units_are_not_hardware_utilization(self):
        self.tick(0)
        self.tick(4)
        r = self.c.report()
        self.assertEqual(r['demand_windows'][-1]['supply_rates']['prepared_items_s'], 4.)
        self.assertEqual(r['demand_windows'][-1]['supply_rates']['submitted_items_s'], 4.)
        self.assertEqual(r['demand_windows'][-1]['supply_rates']['completed_items_s'], 4.)
        self.assertEqual(r['control_unit'], 'application_batches')
        self.assertFalse(r['B_adjusted'])

    def test_invalid_configuration_rejected(self):
        for maximum in [0, -1, True]:
            with self.assertRaises(ValueError):SupplyFeedbackConfig(maximum)
        with self.assertRaises(ValueError):
            SupplyFeedbackConfig(4, replace(self.options, poll_interval_s=2))

    def test_gap_cannot_confirm_a_trial_and_retains_its_online_cost(self):
        for n in range(4):self.tick(n*20)
        self.assertEqual(self.c.current_window, 2)
        self.tick(80, fresh=False)
        self.now += 3
        result = self.tick(100)
        self.assertEqual((result.window, result.reason), (1, 'feedback_gap'))
        report = self.c.report()
        self.assertTrue(report['exploration_censored'])
        self.assertGreater(report['exploration_wall_s'], 0)
        self.assertFalse(report['learned_global_optimum'])

    def test_pressure_confirmation_does_not_bridge_a_clear_supply_limited_window(self):
        self.c.current_window = 4
        self.tick(kv_usage=.99)
        self.tick(ready_items=0)
        self.assertEqual(self.tick(kv_usage=.99).window, 4)
        self.assertEqual(self.tick(kv_usage=.99).window, 2)

    def test_default_computed_policy_can_probe_with_qualified_counter_replay(self):
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4), clock=lambda: self.now)
        self.c.reset(1, 10000)
        decisions = [self.tick(n*20, completed_computed_input_work_total=n*2000,
                    completed_cached_input_work_total=0) for n in range(30)]
        self.assertTrue(any(d.reason == 'performance_probe' for d in decisions))
        self.assertLessEqual(self.c.current_window, 4)
        self.assertFalse(self.c.report()['learned_global_optimum'])


if __name__ == '__main__':unittest.main()
