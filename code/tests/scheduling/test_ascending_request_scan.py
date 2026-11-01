"""Deterministic controller regressions; these are not GPU speed measurements."""
from copy import deepcopy
from dataclasses import replace
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.request_scan import AscendingRequestController
from src.scheduling.submission_control.resource_step import ResourceStep
from src.scheduling.submission_control.stage_work import SupplyState, default_request_feedback_options


class ScanTrace:
    """Original useful work advances while the tested permit window changes."""

    def __init__(self, maximum=8, *, hint=None):
        self.now = 0.
        self.completed = self.input = self.output = self.computed = self.cached = 0
        self.controller = AscendingRequestController(maximum, clock=lambda: self.now)
        self.controller.reset(1 if hint is None else hint['selected_window'], 100000,
                              search_hint=hint)

    def tick(self, count, *, work=(100, 4, 80, 20), resource_step=None, **changes):
        self.now += 1.
        self.completed += count
        for name, amount in zip(('input', 'output', 'computed', 'cached'), work):
            setattr(self, name, getattr(self, name)+count*amount)
        c = self.controller
        o = AdmissionObservation(self.now, True, c.current_window,
            c.current_window, 0, .1, sample_age_s=0,
            completed_items_total=self.completed, pending_items=10000,
            preemptions_total=0, completed_input_work_total=self.input,
            completed_output_work_total=self.output,
            completed_computed_input_work_total=self.computed,
            completed_cached_input_work_total=self.cached)
        supply = SupplyState(self.now, 100, self.completed+100, self.completed+10,
                            1, True, False, active_requests=c.current_window)
        observation_changes = {k: v for k, v in changes.items() if k in o.__dataclass_fields__}
        supply_changes = {k: v for k, v in changes.items()
                          if k in supply.__dataclass_fields__ and k not in observation_changes}
        return c.update_supply(replace(o, **observation_changes), replace(supply, **supply_changes),
                               resource_step=resource_step)

    def run_until(self, predicate, *, rates=None, work=None, limit=150):
        rates = (lambda window: 20*window) if rates is None else rates
        for _ in range(limit):
            window = self.controller.current_window
            descriptor = (100, 4, 80, 20) if work is None else work(window)
            decision = self.tick(rates(window), work=descriptor)
            if predicate(self.controller.report(), decision):
                return self.controller.report()
        raise AssertionError('fixture evidence did not reach its condition: '+str(self.controller.report()))


class AscendingRequestScanTests(unittest.TestCase):
    def test_comparable_gain_continues_upward_without_the_old_forced_one_two_one_visit(self):
        trace = ScanTrace()
        report = trace.run_until(lambda r, _: r['recommended_window'] >= 2 and r['window'] >= 4)
        self.assertEqual(report['strategy'], 'ascending_request_work_v1')
        self.assertGreaterEqual(report['accepted_steps'], 1)
        self.assertEqual(report['reference_rechecks'], 0)
        changes = report['history']
        self.assertEqual([(r['before'], r['window']) for r in changes[:2]], [(1, 2), (2, 4)])
        self.assertFalse(any(r['window'] == 1 for r in changes))
        self.assertEqual(report['exploration_limits'], dict(elapsed_s=None, changes=None,
            attempts_per_candidate=None, min_remaining=0))

    def test_gain_regression_does_not_recreate_the_recorded_15281ms_forced_reference_return(self):
        # The old real run had comparable gain at 15.281s but returned C2->C1,
        # confirming C2 only at 27.792s. This isolates that choreography with
        # uniform synthetic work; it cannot predict the new real query time.
        trace = ScanTrace(maximum=4)
        report = trace.run_until(lambda r, _: r['accepted_steps'] >= 1)
        self.assertEqual(report['recommended_window'], 2)
        self.assertNotIn('reference_recheck', [r['reason'] for r in report['history']])
        self.assertNotIn(1, [r['window'] for r in report['history']])

    def test_equal_performance_confirms_locally_and_selects_the_smaller_measured_quota(self):
        trace = ScanTrace()
        report = trace.run_until(lambda r, _: r['stop_reason'] == 'local_plateau',
                                 rates=lambda _: 40)
        self.assertEqual(report['recommended_window'], 1)
        self.assertEqual(report['window'], 1)
        self.assertIn('local_confirmation', [r['reason'] for r in report['history']])
        self.assertGreaterEqual(report['reference_rechecks'], 1)
        self.assertFalse(report['learned_global_optimum'])

    def test_comparable_regression_returns_to_the_qualified_lower_quota(self):
        trace = ScanTrace()
        report = trace.run_until(lambda r, _: r['stop_reason'] == 'performance_regression',
                                 rates=lambda window: 40 if window == 1 else 20)
        self.assertEqual((report['window'], report['recommended_window']), (1, 1))
        self.assertIn('local_confirmation', [r['reason'] for r in report['history']])
        self.assertFalse(report['learned_global_optimum'])

    def test_mixed_work_without_directional_pairs_does_not_certify_a_platform(self):
        trace = ScanTrace()
        report = trace.run_until(
            lambda r, _: any(h['reason'] == 'local_confirmation' for h in r['history']),
            rates=lambda _: 40,
            work=lambda window: (100, 4, 80, 20) if window == 1 else (200, 8, 40, 160))
        self.assertEqual(report['accepted_steps'], 0)
        self.assertNotIn(report['stop_reason'], ('local_plateau', 'performance_regression'))
        self.assertTrue(report['exploration_censored'])

    def test_missing_native_supply_tail_and_transport_limits_never_imply_idle_hardware(self):
        cases = [
            ({'fresh': False}, 'insufficient_information'),
            ({'sample_age_s': 2}, 'insufficient_information'),
            ({'waiting': 2}, 'backend_queued'),
            ({'ready_items': 0}, 'upstream_supply_limited'),
            ({'ready_items': 0, 'source_exhausted': True}, 'work_tail_or_dependency_wait'),
            ({'capacity_blocked': 'result_bytes'}, 'configuration_boundary'),
            ({'capacity_blocked': 'pending_transport_admission'}, 'submission_backpressure'),
        ]
        for changes, reason in cases:
            with self.subTest(changes=changes):
                trace = ScanTrace()
                for _ in range(8):
                    self.assertEqual(trace.tick(20, **changes).reason, reason)
                self.assertEqual(trace.controller.current_window, 1)
                self.assertEqual(trace.controller.report()['accepted_steps'], 0)

    def test_ready_work_during_an_instantaneous_empty_handoff_remains_measurable(self):
        trace = ScanTrace()
        for _ in range(5):
            decision = trace.tick(20, inflight=0, active_batches=0, active_requests=0)
        self.assertEqual(decision.window, 2)
        self.assertNotIn('upstream_supply_limited', trace.controller.report()['reasons'])

    def test_real_pressure_shrinks_permits_without_claiming_a_performance_plateau(self):
        trace = ScanTrace()
        trace.run_until(lambda r, _: r['window'] >= 4)
        before = trace.controller.current_window
        decision = trace.tick(20, memory_pressure=True)
        self.assertEqual(decision.reason, 'resource_pressure')
        self.assertLess(decision.window, before)
        self.assertNotEqual(trace.controller.report()['stop_reason'], 'local_plateau')

    def test_unconfirmed_trial_is_not_exported_as_the_next_query_start(self):
        trace = ScanTrace()
        self.assertIsNone(trace.controller.export_hint())
        trace.run_until(lambda r, _: r['window'] >= 4 and r['recommended_window'] == 2)
        hint = trace.controller.export_hint()
        self.assertEqual(hint['schema'], 'semloom.request_scan.v1')
        self.assertEqual(hint['selected_window'], 2)
        # Fresh work, receipt counters and rates must be measured in the new query.
        def keys(value):
            if isinstance(value, dict):
                return set(value).union(*(keys(v) for v in value.values()))
            if isinstance(value, (tuple, list)):
                return set().union(*(keys(v) for v in value))
            return set()
        self.assertFalse({'rate', 'rate_min', 'rate_max', 'completed_items_total'} & keys(hint))

    def test_an_unfinished_compatible_scan_starts_at_the_selection_and_continues_after_fresh_work(self):
        first = ScanTrace()
        first.run_until(lambda r, _: r['window'] >= 4 and r['recommended_window'] == 2)
        second = ScanTrace(hint=first.controller.export_hint())
        self.assertEqual(second.controller.current_window, 2)
        self.assertEqual(second.controller.report()['changes'], 0)
        report = second.run_until(lambda r, _: r['window'] >= 4)
        self.assertTrue(report['state_reused'])
        self.assertTrue(all(row['window'] != 1 for row in report['history']))

    def test_a_finished_compatible_scan_does_not_restart_at_one_or_repeat_the_search(self):
        first = ScanTrace(maximum=2)
        first.run_until(lambda r, _: r['stop_reason'] == 'capacity_limit'
                       and r['recommended_window'] == 2)
        second = ScanTrace(maximum=2, hint=first.controller.export_hint())
        for _ in range(15):
            second.tick(40)
        report = second.controller.report()
        self.assertTrue(report['state_reused'])
        self.assertEqual((report['window'], report['recommended_window'], report['changes']), (2, 2, 0))
        self.assertFalse(report['learned_global_optimum'])

    def test_changed_work_revalidates_locally_instead_of_mechanically_resetting_to_one(self):
        first = ScanTrace(maximum=4)
        first.run_until(lambda r, _: r['stop_reason'] == 'capacity_limit'
                       and r['recommended_window'] == 4)
        second = ScanTrace(maximum=4, hint=first.controller.export_hint())
        report = second.run_until(lambda r, _: bool(r['history']),
                                 work=lambda _: (200, 8, 160, 40))
        self.assertEqual(report['history'][0]['before'], 4)
        self.assertGreater(report['history'][0]['window'], 1)
        self.assertGreater(report['work_epoch'], 0)

    def test_incompatible_old_hint_and_unqualified_non_one_start_are_rejected(self):
        trace = ScanTrace(maximum=2)
        trace.run_until(lambda r, _: r['stop_reason'] == 'capacity_limit'
                        and r['recommended_window'] == 2)
        hint = trace.controller.export_hint()
        for bad in [dict(hint, schema='semloom.admission_search.v1'),
                    dict(hint, selected_window=999), (1, 2)]:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    trace.controller.reset(1, 1000, search_hint=deepcopy(bad))
        with self.assertRaises(ValueError):
            trace.controller.reset(2, 1000)

    def test_costed_resource_jump_does_not_go_backward_to_skipped_geometric_points_after_gain(self):
        trace = ScanTrace(maximum=16)
        step = ResourceStep(6, 'costed_native_KV_prefix', {'source': 'CPU fixture; not real resource evidence'})
        for _ in range(40):
            trace.tick(20*trace.controller.current_window, resource_step=step)
            if trace.controller.report()['recommended_window'] == 6:
                break
        report = trace.controller.report()
        self.assertEqual([(row['before'], row['window']) for row in report['history'][:2]],
                         [(1, 6), (6, 8)])
        self.assertEqual(report['step_decisions'][0]['source'], 'costed_resource_headroom')
        self.assertNotIn(2, [row['window'] for row in report['history']])
        self.assertNotIn(4, [row['window'] for row in report['history']])

    def test_hint_with_other_capacity_or_attempt_policy_cannot_reuse_qualified_selection(self):
        first = ScanTrace(maximum=2)
        first.run_until(lambda r, _: r['stop_reason'] == 'capacity_limit'
                       and r['recommended_window'] == 2)
        hint = first.controller.export_hint()
        for maximum, options in [(4, None), (2, replace(default_request_feedback_options(),
                                                       max_candidate_attempts=2))]:
            with self.subTest(maximum=maximum, options=options):
                changed = AscendingRequestController(maximum, options, clock=lambda: 0.)
                with self.assertRaises(ValueError):
                    changed.reset(2, 1000, search_hint=hint)

    def test_feedback_gap_interrupts_unconfirmed_growth_and_requires_fresh_evidence(self):
        trace = ScanTrace()
        trace.run_until(lambda r, _: r['window'] >= 4 and r['recommended_window'] == 2)
        trace.tick(40, fresh=False, sample_age_s=2)
        decision = trace.tick(40)
        self.assertEqual((decision.reason, decision.window), ('feedback_gap', 2))
        self.assertFalse(trace.controller.report()['unconfirmed_trial'])
        self.assertIsNone(trace.controller.export_hint())
        trace.run_until(lambda r, _: r['window'] >= 4)
        self.assertGreaterEqual(trace.controller.report()['feedback_gaps'], 1)

    def test_feedback_gap_after_a_finished_selection_does_not_leave_measurement_permanently_stopped(self):
        trace = ScanTrace(maximum=2)
        trace.run_until(lambda r, _: r['stop_reason'] == 'capacity_limit'
                       and r['recommended_window'] == 2)
        trace.tick(40, fresh=False, sample_age_s=2)
        self.assertEqual(trace.tick(40).reason, 'feedback_gap')
        self.assertIsNone(trace.controller.export_hint())
        for _ in range(15):
            trace.tick(40)
        self.assertIsNotNone(trace.controller.export_hint())
        self.assertEqual(trace.controller.report()['recommended_window'], 2)

    def test_steady_selection_stays_held_and_fresh_performance_change_reopens_locally(self):
        trace = ScanTrace(maximum=4)
        trace.run_until(lambda r, _: r['stop_reason'] == 'capacity_limit'
                       and r['recommended_window'] == 4)
        before = trace.controller.report()['changes']
        for _ in range(12):
            trace.tick(80)
        self.assertEqual(trace.controller.report()['changes'], before)
        report = trace.run_until(lambda r, _: any(h['reason'] == 'condition_changed_probe'
                                                  for h in r['history']), rates=lambda _: 40)
        change, = [row for row in report['history'] if row['reason'] == 'condition_changed_probe']
        self.assertEqual((change['before'], change['window']), (4, 2))
        self.assertGreater(report['work_epoch'], 0)


if __name__ == '__main__':
    unittest.main()
