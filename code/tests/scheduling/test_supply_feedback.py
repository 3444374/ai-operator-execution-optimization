"""Deterministic demand/native-window replays; no hardware benefit inference."""
from dataclasses import asdict, replace
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

    def test_JSON_options_inherit_supply_defaults_and_round_trip(self):
        default = SupplyFeedbackConfig.from_dict(dict(max_batches=3))
        self.assertEqual(default, SupplyFeedbackConfig(3))
        self.assertEqual(default.options.work_basis, 'computed')
        self.assertTrue(default.options.allow_exploration)
        self.assertIsNone(default.options.max_candidate_attempts)
        partial = SupplyFeedbackConfig.from_dict(dict(max_batches=3, options=dict(poll_interval_s=.1)))
        self.assertEqual(partial.options, replace(default.options, poll_interval_s=.1))
        self.assertEqual(SupplyFeedbackConfig.from_dict(asdict(partial)), partial)

    def test_JSON_unknown_fields_and_invalid_shapes_are_rejected(self):
        for value in [None, [], {}, dict(max_batches=True), dict(max_batches=0),
                dict(max_batches=3, rows=64), dict(max_batches=3, options=None),
                dict(max_batches=3, options=dict(unsupported=True)),
                dict(max_batches=3, options=dict(poll_interval_s=3))]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                SupplyFeedbackConfig.from_dict(value)

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
        self.assertIsNone(report['stop_reason'])
        self.assertEqual(report['feedback_gaps'], 1)
        self.assertFalse(report['unconfirmed_trial'])
        self.assertEqual(report['evaluations'][-1]['kind'], 'feedback_gap')
        cost, changes, started = report['exploration_wall_s'], report['changes'], self.c.started
        decisions = [self.tick(120+n*20) for n in range(16)]
        self.assertIn('performance_probe', [d.reason for d in decisions])
        self.assertGreaterEqual(self.c.report()['exploration_wall_s'], cost)
        self.assertGreater(self.c.report()['changes'], changes)
        self.assertEqual(self.c.started, started)

    def test_reviewer_gap_before_reference_recovers_from_fresh_points(self):
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4,
            replace(self.options, poll_interval_s=.25)), clock=lambda: self.now)
        self.c.reset(1, 100000)
        decisions = []
        times = [1., 2.01]+[2.01+i*.25 for i in range(1, 41)]
        for i, at in enumerate(times):
            self.now = at-1
            decisions.append(self.tick(i*20))
        report = self.c.report()
        self.assertNotEqual(report['stop_reason'], 'feedback_gap')
        self.assertIn('performance_probe', [d.reason for d in decisions])
        self.assertEqual(report['feedback_gaps'], 1)
        self.assertFalse(report['learned_global_optimum'])

    def test_gap_does_not_reset_trial_attempts_or_elapsed_budget(self):
        options = replace(self.options, search_mode='geometric', max_candidate_attempts=1)
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4, options), clock=lambda: self.now)
        self.c.reset(1, 10000)
        for n in range(5):self.tick(n*20)
        self.assertEqual(self.c.current_window, 2)
        self.now += 3
        self.tick(100)
        row = next(r for r in self.c.report()['search_state']['points'] if r['window'] == 2)
        self.assertEqual((row['attempts'], row['outcome']), (1, 'incomparable'))
        self.assertEqual(self.c.report()['changes'], 2)
        self.assertEqual(self.c.started, 0)
        decisions = [self.tick(120+n*20) for n in range(24)]
        self.assertNotIn(2, [d.window for d in decisions if d.reason == 'performance_probe'])

    def test_gap_does_not_reopen_a_real_stop_or_grow_from_missing_feedback(self):
        for n in range(30):self.tick(n*20)
        self.assertEqual(self.c.report()['stop_reason'], 'local_plateau')
        changes = self.c.report()['changes']
        self.now += 3
        self.tick(620)
        for n in range(8):self.tick(640+n*20, fresh=False)
        for n in range(8):self.tick(800+n*20)
        self.assertEqual(self.c.report()['stop_reason'], 'local_plateau')
        self.assertEqual(self.c.report()['changes'], changes)

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

    def test_actual_default_active_trial_gap_resumes_and_keeps_spent_cost(self):
        # Independent review's unmodified default/data/clock shape. The old
        # probe's final attempts==1 described the defect, not a recovery rule.
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4), clock=lambda: self.now)
        self.c.reset(1, 100000)
        decisions, interrupted = [], False
        for n in range(70):
            decision = self.tick(n*20, pending_items=1000,
                completed_computed_input_work_total=n*2000, completed_cached_input_work_total=0)
            decisions.append(decision)
            if decision.reason == 'feedback_gap':
                point = self.c._search.points[2]
                self.assertEqual((decision.window, point['attempts'], point['outcome']), (1, 1, 'incomparable'))
                self.assertNotIn(2, self.c._search.here)
                self.assertTrue(self.c.evaluations[-1]['retry_eligible'])
                self.assertEqual(self.c.report()['exploration_wall_s'], 4)
            if not interrupted and decision.reason == 'performance_probe':
                self.now += 3
                interrupted = True
        report = self.c.report()
        self.assertEqual(report['feedback_gaps'], 1)
        self.assertGreater(sum(d.reason == 'performance_probe' for d in decisions), 1)
        self.assertGreaterEqual(self.c._search.points[2]['attempts'], 2)
        self.assertIsNone(report['search_state']['attempt_limit'])
        self.assertNotEqual(report['stop_reason'], 'adjacent_trial_unavailable')
        self.assertGreaterEqual(report['exploration_wall_s'], 4)
        self.assertEqual(self.c.started, 0)
        self.assertFalse(report['learned_global_optimum'])

    def test_default_repeated_interruptions_have_no_hidden_two_attempt_stop(self):
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4), clock=lambda: self.now)
        self.c.reset(1, 100000)
        gaps, decisions, interruptions = 0, [], []
        for n in range(100):
            decision = self.tick(n*20, pending_items=1000,
                completed_computed_input_work_total=n*2000, completed_cached_input_work_total=0)
            decisions.append(decision)
            if decision.reason == 'feedback_gap':
                interruptions.append(self.c._search.points[2]['attempts'])
            if decision.reason == 'performance_probe' and gaps < 3:
                self.now += 3
                gaps += 1
        report = self.c.report()
        self.assertEqual(interruptions, [1, 2, 3])
        self.assertGreaterEqual(self.c._search.points[2]['attempts'], 4)
        self.assertGreaterEqual(sum(d.reason == 'performance_probe' for d in decisions), 4)
        self.assertGreaterEqual(report['exploration_wall_s'], 12)
        self.assertEqual(report['exploration_limits'], dict(elapsed_s=None, changes=None,
            attempts_per_candidate=None, min_remaining=0))
        self.assertNotIn(report['stop_reason'], ('exploration_budget', 'search_exhausted', 'adjacent_trial_unavailable'))

    def test_default_gap_during_reference_recheck_restores_the_candidate(self):
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4), clock=lambda: self.now)
        self.c.reset(1, 100000)
        interrupted, probes, gap_seen = False, 0, False
        for n in range(100):
            decision = self.tick(n*20, pending_items=1000,
                completed_computed_input_work_total=n*2000, completed_cached_input_work_total=0)
            probes += decision.reason == 'performance_probe'
            if decision.reason == 'feedback_gap':
                gap_seen = True
                self.assertEqual(decision.window, 1)
                self.assertNotIn(2, self.c._search.here)
                self.assertEqual(self.c._search.points[2]['attempts'], 1)
                self.assertIsNone(self.c._probe_candidate)
            if not interrupted and decision.reason == 'reference_recheck':
                self.now += 3
                interrupted = True
        self.assertTrue(gap_seen)
        self.assertGreater(probes, 1)
        self.assertGreaterEqual(self.c._search.points[2]['attempts'], 2)
        self.assertGreater(self.c.report()['exploration_wall_s'], 4)

    def test_pressure_guard_does_not_hide_the_gap_from_later_comparisons(self):
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4), clock=lambda: self.now)
        self.c.reset(1, 100000)
        for n in range(5):
            self.tick(n*20, completed_computed_input_work_total=n*2000, completed_cached_input_work_total=0)
        self.assertEqual(self.c.current_window, 2)
        self.now += 3
        decision = self.tick(100, kv_usage=.9, completed_computed_input_work_total=10000,
                             completed_cached_input_work_total=0)
        self.assertEqual(decision.reason, 'confirming_pressure')
        pending = self.c.report().get('feedback_gap_pending')
        decision = self.tick(120, completed_computed_input_work_total=12000,
                             completed_cached_input_work_total=0)
        self.assertEqual((decision.reason, decision.window), ('feedback_gap', 1))
        self.assertTrue(pending)
        self.assertNotIn(2, self.c._search.here)
        self.assertEqual(self.c._search.points[2]['attempts'], 1)
        self.assertFalse(self.c.report()['feedback_gap_pending'])
        self.assertEqual(self.c.report()['exploration_wall_s'], 5)

    def test_explicit_finite_attempt_budget_allows_recovery_then_preserves_its_count(self):
        options = replace(SupplyFeedbackConfig(4).options, max_candidate_attempts=2)
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4, options), clock=lambda: self.now)
        self.c.reset(1, 100000)
        decisions, interrupted = [], False
        for n in range(70):
            decision = self.tick(n*20, pending_items=1000,
                completed_computed_input_work_total=n*2000, completed_cached_input_work_total=0)
            decisions.append(decision)
            if not interrupted and decision.reason == 'performance_probe':
                self.now += 3
                interrupted = True
        self.assertEqual(sum(d.reason == 'performance_probe' for d in decisions), 2)
        self.assertEqual(self.c._search.points[2]['attempts'], 2)
        self.assertEqual(self.c._search.limit, 2)
        self.assertGreaterEqual(self.c.report()['exploration_wall_s'], 4)

    def test_explicit_exhausted_attempt_budget_is_censored_without_reset(self):
        options = replace(SupplyFeedbackConfig(4).options, max_candidate_attempts=1)
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4, options), clock=lambda: self.now)
        self.c.reset(1, 100000)
        interrupted = False
        for n in range(40):
            decision = self.tick(n*20, pending_items=1000,
                completed_computed_input_work_total=n*2000, completed_cached_input_work_total=0)
            if not interrupted and decision.reason == 'performance_probe':
                self.now += 3
                interrupted = True
        report = self.c.report()
        self.assertEqual(self.c._search.points[2]['attempts'], 1)
        self.assertTrue(report['exploration_censored'])
        self.assertEqual(report['incomplete_reason'], 'candidate_attempt_budget')
        self.assertFalse(report['can_continue'])
        self.assertEqual(report['exploration_wall_s'], 4)

    def test_default_gain_scan_has_no_ten_second_eight_change_or_sixty_four_cutoff(self):
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(128), clock=lambda: self.now)
        self.c.reset(1, 10000000)
        completed = 0
        for _ in range(200):
            completed += 20*self.c.current_window
            self.tick(completed, pending_items=100000,
                completed_computed_input_work_total=completed*100, completed_cached_input_work_total=0)
        report = self.c.report()
        self.assertEqual(report['recommended_window'], 128)
        self.assertGreater(report['changes'], 8)
        self.assertGreater(self.now, 10)
        self.assertGreater(report['accepted_steps'], 2)
        self.assertNotIn(report['stop_reason'], ('exploration_budget', 'search_exhausted', 'adjacent_trial_unavailable'))
        self.assertFalse(report['capacity_is_performance_platform'])

    def test_default_can_start_after_ten_seconds_with_less_than_sixty_four_pending(self):
        self.c = SupplyFeedbackController(SupplyFeedbackConfig(4), clock=lambda: self.now)
        self.c.reset(1, 100000)
        self.now = 10
        decisions = [self.tick(n*20, ready_items=16, pending_items=32,
            completed_computed_input_work_total=n*2000, completed_cached_input_work_total=0) for n in range(30)]
        self.assertIn('performance_probe', [d.reason for d in decisions])
        self.assertNotEqual(self.c.report()['stop_reason'], 'short_tail_or_fixed')


if __name__ == '__main__':unittest.main()
