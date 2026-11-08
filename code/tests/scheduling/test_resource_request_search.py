"""CPU original-work sequences for measured search and prefix changes.

Synthetic rate functions describe fixtures, never measured GPU performance.
"""
from dataclasses import replace
import math
import unittest

from src.scheduling.submission_control.feedback_options import ResourceRequestConfig
from src.scheduling.submission_control.resource_step import (
    NativeKVFrame, RequestKVWork, propose_kv_window,
)
from tests.scheduling.test_resource_request_controller import ResourceTrace


def work_sequence(trace, ticks, *, rate=lambda c: 16*c, physical=(100., 10.), **kwargs):
    for _ in range(ticks):
        trace.tick(int(rate(trace.controller.current_window)), elapsed=1., physical=physical, **kwargs)
    return trace


def costed_prefix(trace, growth):
    def step(observation, supply, permit):
        current = trace.controller.current_window
        count = min(growth, trace.maximum-current)
        return propose_kv_window(NativeKVFrame(permit.engine_uid, permit.session_id,
            permit.native_sequence, observation.observed_at_s, observation.observed_at_s,
            max(1, 2*(current+count)), 16, 0., cost_model='single_pool_token_blocks'),
            now_s=observation.observed_at_s, max_age_s=1.,
            expected_engine_uid=permit.engine_uid, expected_session_id=permit.session_id,
            current_window=current, hard_limit=trace.maximum,
            committed=tuple(RequestKVWork(('active', i), 16, 16) for i in range(current)),
            ready=tuple(RequestKVWork(('ready', i), 16, 16) for i in range(count)))
    return step


class ResourceRequestSearchTests(unittest.TestCase):
    def incomparable_higher_trace(self, options=None):
        trace = ResourceTrace(maximum=16, options=options)
        trace.until(lambda r, _: r['measured_window'] == 2)
        trace.until(lambda r, _: any(e['kind'] == 'work_comparison'
            and e['candidate']['window'] == 4 and e['evidence']['outcome'] is None
            for e in r['evaluations']), count=160, work=(200, 8, 40, 160))
        return trace

    def test_incomparable_higher_measurement_does_not_force_a_smaller_window(self):
        trace = self.incomparable_higher_trace()
        report = trace.controller.report()
        self.assertEqual(report['window'], 4)
        self.assertEqual(report['measured_window'], 4)
        self.assertIsNone(report['verified_performance_window'])
        self.assertEqual(report['accepted_steps'], 0)
        self.assertIsNone(report['performance_stop_reason'])
        self.assertGreater(trace.tick().window, 4)

    def test_work_drift_alone_does_not_restart_an_incomparable_lower_comparison(self):
        trace, first = self.incomparable_lower_trace()
        changes = trace.controller.report()['changes']
        for _ in range(12):
            trace.tick(32, elapsed=1., work=(130, 20, 54, 76), physical=(100., 10.))
        report = trace.controller.report()
        self.assertEqual(report['window'], first)
        self.assertEqual(report['changes'], changes)
        self.assertEqual(report['performance_stop_reason'], 'work_not_comparable')
        self.assertFalse(any(e['kind'] == 'held_performance_changed'
                             for e in report['evaluations']))

    def test_inconclusive_higher_measurement_keeps_pressure_and_unknown_returns(self):
        for signal in ('unknown', 'pressure', 'owner'):
            with self.subTest(signal=signal):
                trace = self.incomparable_higher_trace(ResourceRequestConfig(max_changes=4))
                self.assertEqual(trace.controller.current_window, 4)
                self.assertEqual(trace.tick().window, 8)
                observation = ({'sample_age_s': 2.} if signal == 'unknown' else
                               {'memory_pressure': True} if signal == 'pressure' else {})
                decision = trace.tick(observation=observation,
                    permit={'request_limit': 4} if signal == 'owner' else None)
                self.assertEqual(decision.window, 4)
                self.assertEqual(trace.controller.report()['changes'], 4)
                self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def test_inconclusive_ascent_and_lower_return_hold_at_the_applied_ceiling(self):
        for signal in ('owner', 'tail', 'budget', 'unknown'):
            with self.subTest(signal=signal):
                trace = ResourceTrace(maximum=16, options=ResourceRequestConfig(max_changes=5))
                for _ in range(40):
                    window = trace.controller.current_window
                    work = (125, 12, 59, 66) if window == 4 else (130, 20, 55, 75)
                    trace.tick(32, elapsed=1., work=work, physical=(100., 10.),
                        permit={'request_limit': 8})
                    report = trace.controller.report()
                    if report['window'] == 8 and report['stop_reason'] == 'work_not_comparable':
                        break
                else:
                    self.fail('incomparable ascent and lower return did not reach the applied ceiling')
                self.assertEqual([(x['before'], x['window']) for x in report['history']],
                                 [(1, 2), (2, 4), (4, 8), (8, 4), (4, 8)])
                for _ in range(24):
                    trace.tick(32, elapsed=1., work=(130, 20, 54, 76),
                        physical=(100., 10.), permit={'request_limit': 8})
                report = trace.controller.report()
                self.assertEqual((report['window'], report['changes'], report['accepted_steps']), (8, 5, 0))
                self.assertIsNone(report['verified_performance_window'])
                observation, permit = {}, {'request_limit': 16}
                if signal == 'owner':
                    observation['pending_items'] = 0
                    permit['request_limit'] = 4
                elif signal == 'tail':
                    observation['pending_items'] = 0
                elif signal == 'unknown':
                    observation['sample_age_s'] = 2.
                decision = trace.tick(physical=(20., 10.), observation=observation, permit=permit)
                self.assertEqual(decision.window, 4 if signal == 'owner' else 8)
                if signal != 'unknown':
                    self.assertEqual(decision.reason, {'owner': 'resource_pressure',
                        'tail': 'no_pending_work', 'budget': 'change_budget'}[signal])
                self.assertIsNone(trace.controller.report()['verified_performance_window'])

    def incomparable_lower_trace(self, options=None):
        trace = ResourceTrace(maximum=136, options=options)
        trace.tick(physical=(86., 10.))
        first = trace.controller.current_window
        trace.until(lambda r, _: r['window'] < first, count=32,
            work=(130, 20, 55, 75), physical=(100., 10.))
        trace.until(lambda r, _: r['stop_reason'] == 'work_not_comparable', count=32,
            work=(125, 12, 59, 66), physical=(100., 10.))
        return trace, first

    def test_incomparable_lower_work_does_not_repeat_without_changed_context(self):
        for at_ceiling in (False, True):
            with self.subTest(at_ceiling=at_ceiling):
                trace, first = self.incomparable_lower_trace()
                for _ in range(14):
                    work = ((130, 20, 55, 75) if trace.controller.current_window == first
                            else (125, 12, 59, 66))
                    trace.tick(32, elapsed=1., work=work, physical=(100., 10.),
                        permit={'request_limit': first} if at_ceiling else None)
                report = trace.controller.report()
                self.assertEqual(sum(row['before'] == first and row['window'] == first//2
                                     for row in report['history']), 1)
                self.assertEqual(report['window'], first)
                self.assertEqual(report['performance_stop_reason'], 'work_not_comparable')
                self.assertIsNone(report['verified_performance_window'])
                self.assertEqual(report['accepted_steps'], 0)

    def test_fresh_resources_reopen_incomparable_lower_work_without_new_completions(self):
        for physical_count, transition_pending in ((2, 0), (1, 0), (2, 1)):
            with self.subTest(physical_count=physical_count, transition_pending=transition_pending):
                trace, first = self.incomparable_lower_trace()
                completed = trace.completed
                decision = trace.tick(physical=(20., 10.), physical_count=physical_count,
                    observation={'transition_pending': transition_pending})
                self.assertGreater(decision.window, first)
                self.assertEqual(decision.reason, 'resource_trial')
                self.assertEqual(trace.completed, completed)
                report = trace.controller.report()
                self.assertIsNone(report['verified_performance_window'])
                self.assertEqual(report['accepted_steps'], 0)
                self.assertFalse(report['resource_trials'][-1]['performance_reference_present'])

    def test_incomparable_lower_restart_still_requires_current_resource_permission(self):
        for case in ('stale', 'repeat', 'owner_limit', 'queue', 'backpressure', 'identity'):
            with self.subTest(case=case):
                trace, first = self.incomparable_lower_trace()
                observation, permit, physical_count = {}, {}, 2
                if case == 'stale':
                    observation['sample_age_s'] = 2.
                elif case == 'repeat':
                    observation['sample_sequence'] = trace.sequence
                    permit.update(sample_sequence=trace.sequence, native_sequence=trace.sequence)
                elif case == 'owner_limit':
                    permit['request_limit'] = first
                elif case == 'queue':
                    observation['waiting'] = 1
                elif case == 'backpressure':
                    observation['consumer_backpressure'] = True
                elif case == 'identity':
                    permit['engine_uid'] = 'other-engine'
                decision = trace.tick(physical=(20., 10.), observation=observation,
                    permit=permit, physical_count=physical_count)
                self.assertEqual(decision.window, first)

    def test_pressure_preempts_a_resource_restart_after_incomparable_lower_work(self):
        for reduced_limit in (False, True):
            with self.subTest(reduced_limit=reduced_limit):
                trace, first = self.incomparable_lower_trace()
                decision = trace.tick(physical=(20., 10.),
                    observation={} if reduced_limit else {'memory_pressure': True},
                    permit={'request_limit': first-1} if reduced_limit else None)
                self.assertEqual(decision.reason, 'resource_pressure')
                self.assertLess(decision.window, first)

    def test_incomparable_lower_resource_restart_reserves_its_explicit_return_budget(self):
        for max_changes in (3, 4, 5):
            with self.subTest(max_changes=max_changes):
                trace, first = self.incomparable_lower_trace(
                    ResourceRequestConfig(max_changes=max_changes))
                self.assertEqual(trace.controller.report()['changes'], 3)
                decision = trace.tick(physical=(20., 10.))
                if max_changes < 5:
                    self.assertEqual((decision.window, decision.reason), (first, 'change_budget'))
                    self.assertEqual(trace.controller.report()['changes'], 3)
                else:
                    self.assertGreater(decision.window, first)
                    returned = trace.tick(observation={'sample_age_s': 2.})
                    self.assertEqual(returned.window, first)
                    self.assertEqual(trace.controller.report()['changes'], 5)

    def test_active_resource_trial_keeps_its_last_explicit_change_for_return(self):
        trace = ResourceTrace(maximum=136, options=ResourceRequestConfig(max_changes=2))
        self.assertGreater(trace.tick(physical=(86., 10.)).window, 1)
        decision = trace.tick(physical=(20., 10.))
        self.assertEqual((decision.window, decision.reason), (1, 'change_budget'))
        self.assertEqual(trace.controller.report()['changes'], 2)

    def test_full_busy_read_does_not_reduce_native_geometric_comparisons(self):
        trace = work_sequence(ResourceTrace(maximum=256), 100)
        report = trace.controller.report()
        self.assertEqual(report['window'], 256)
        self.assertFalse(report['local_plateau_confirmed'])
        self.assertFalse(report['physical_capacity_known'])

    def test_first_large_flat_trial_checks_smaller_positions_until_one(self):
        trace = ResourceTrace(maximum=256)
        trace.tick(physical=(20., 10.))
        first = trace.controller.current_window
        self.assertGreater(first, 128)
        trace.until(lambda r, _: r['stop_reason'] == 'minimum_measured_window', count=256)
        report = trace.controller.report()
        positions = [row['window'] for row in report['history']]
        self.assertEqual(positions[0], first)
        self.assertTrue(all(a > b for a, b in zip(positions, positions[1:])))
        self.assertLessEqual(len(positions)-1, math.ceil(math.log2(first)))
        self.assertEqual(report['verified_performance_window'], 1)
        self.assertFalse(report['smaller_positions_pending'])
        self.assertFalse(report['learned_global_optimum'])

    def test_first_large_trial_measures_smaller_positions_on_a_single_peak_fixture(self):
        trace = work_sequence(ResourceTrace(maximum=256), 100, physical=(20., 10.),
            rate=lambda c: 16*c if c <= 32 else 16*32*32/c)
        report = trace.controller.report()
        first = report['history'][0]['window']
        self.assertTrue(any(point['window'] < first for point in report['measurements']))
        self.assertLess(report['window'], first)
        self.assertFalse(report['learned_global_optimum'])

    def test_returned_position_gets_current_work_before_the_next_comparison(self):
        trace = ResourceTrace(maximum=256)
        trace.tick(physical=(20., 10.))
        first = trace.controller.current_window
        trace.until(lambda r, _: len(r['history']) >= 3, count=lambda c: 16*c)
        self.assertEqual(trace.controller.current_window, first)
        returned_at = trace.completed
        trace.until(lambda r, _: any(row['kind'] == 'work_comparison'
            and row['candidate']['window'] > first for row in r['evaluations']), count=lambda c: 16*c)
        comparison = next(row for row in trace.controller.report()['evaluations']
            if row['kind'] == 'work_comparison' and row['candidate']['window'] > first)
        self.assertGreaterEqual(comparison['reference']['completed_begin'], returned_at)
        self.assertEqual(comparison['reference']['window'], first)

    def test_incomparable_lower_work_reopens_on_a_new_rate_context(self):
        trace = ResourceTrace(maximum=256)
        trace.tick(physical=(20., 10.))
        first = trace.controller.current_window
        trace.until(lambda r, _: r['window'] < first, count=256)
        trace.until(lambda r, _: r['stop_reason'] == 'work_not_comparable', count=256,
            work=(200, 8, 160, 40))
        self.assertTrue(trace.controller.report()['smaller_positions_pending'])
        changes = trace.controller.report()['changes']
        for _ in range(8):
            trace.tick(256, elapsed=1., work=(200, 8, 160, 40))
        self.assertEqual(trace.controller.report()['changes'], changes)
        trace.until(lambda r, _: any(row['kind'] == 'work_comparison'
            and row['candidate']['window'] < row['reference']['window']
            and row['evidence']['outcome'] is not None for row in r['evaluations']),
            count=512, work=(200, 8, 160, 40))
        self.assertFalse(trace.controller.report()['local_plateau_confirmed'])

    def test_ready_censored_comparison_does_not_restart_on_busy_or_source_changes(self):
        trace = work_sequence(ResourceTrace(maximum=256), 200, supply={'ready_items': 1})
        report = trace.controller.report()
        self.assertEqual(report['stop_reason'], 'comparison_censored')
        self.assertFalse(report['local_plateau_confirmed'])
        self.assertIn('ready', report['censored_frontier']['clip_causes'])
        changes = report['changes']
        for busy in ((0., 0.), (49., 50.), (50., 51.), (100., 100.)):
            work_sequence(trace, 20, physical=busy, supply={'ready_items': 1})
            self.assertEqual(trace.controller.report()['changes'], changes)

    def test_ready_expansion_reopens_with_current_reference_work(self):
        trace = work_sequence(ResourceTrace(maximum=256), 200, supply={'ready_items': 1})
        before, resumed_at = trace.controller.current_window, trace.completed
        work_sequence(trace, 100)
        report = trace.controller.report()
        self.assertGreater(report['window'], before)
        comparison = next(row for row in report['evaluations'] if row['kind'] == 'work_comparison'
            and row['candidate']['window'] > before and row['candidate']['window'] > 35)
        self.assertGreaterEqual(comparison['reference']['completed_begin'], resumed_at)
        self.assertEqual(sum(row['kind'] == 'measurable_range_expanded' for row in report['evaluations']), 1)

    def test_rate_or_work_changes_do_not_reopen_the_same_censored_range(self):
        for work in ((100, 4, 80, 20), (200, 8, 160, 40)):
            with self.subTest(work=work):
                trace = work_sequence(ResourceTrace(maximum=256), 200, supply={'ready_items': 1})
                trace.until(lambda r, decision: decision.reason == 'held_window_measured',
                    count=lambda c: 16*c, supply={'ready_items': 1})
                before = trace.controller.report()
                work_sequence(trace, 40, rate=lambda c: 32*c,
                    supply={'ready_items': 1}, work=work)
                report = trace.controller.report()
                self.assertEqual(report['changes'], before['changes'])
                self.assertEqual(report['censored_frontier'], before['censored_frontier'])
                self.assertIsNone(report['verified_performance_window'])
                self.assertFalse(report['local_plateau_confirmed'])
                self.assertTrue(any(row['kind'] == 'held_performance_changed' for row in report['evaluations']))

    def test_pending_expansion_is_a_new_opportunity_after_censored_comparison(self):
        trace = work_sequence(ResourceTrace(maximum=256), 200, observation={'pending_items': 1})
        before = trace.controller.current_window
        self.assertIn('pending', trace.controller.report()['censored_frontier']['clip_causes'])
        work_sequence(trace, 100)
        self.assertGreater(trace.controller.current_window, before)

    def test_mixed_held_windows_do_not_hide_a_changed_verified_selection(self):
        for work in ((100, 4, 80, 20), (200, 8, 160, 40)):
            with self.subTest(work=work):
                trace = work_sequence(ResourceTrace(maximum=256), 200, supply={'ready_items': 1})
                before = trace.controller.report()
                # Start between held points, so the first point contains both
                # old and new work. Later complete points use only new work.
                work_sequence(trace, 40, rate=lambda c: 32*c,
                    supply={'ready_items': 1}, work=work)
                report = trace.controller.report()
                self.assertEqual(report['changes'], before['changes'])
                self.assertEqual(report['censored_frontier'], before['censored_frontier'])
                self.assertIsNone(report['verified_performance_window'])
                self.assertEqual(report['last_verified_performance_window'],
                    before['last_verified_performance_window'])

    def test_owner_129_then_256_is_not_a_permanent_128_selection(self):
        trace = work_sequence(ResourceTrace(maximum=256), 100, physical=False,
            permit={'request_limit': 129})
        report = trace.controller.report()
        self.assertEqual(report['censored_frontier']['tested_end'], 129)
        self.assertFalse(report['local_plateau_confirmed'])
        work_sequence(trace, 100, physical=False)
        self.assertEqual(trace.controller.current_window, 256)

    def test_fresh_costed_prefix_can_reopen_a_censored_comparison(self):
        trace = ResourceTrace(maximum=256)
        work_sequence(trace, 200, resource_step=costed_prefix(trace, 1))
        before = trace.controller.current_window
        self.assertIn('costed_ready_prefix', trace.controller.report()['censored_frontier']['clip_causes'])
        work_sequence(trace, 100, resource_step=costed_prefix(trace, 256))
        self.assertGreater(trace.controller.current_window, before)

    def test_missing_or_invalid_costed_data_does_not_pretend_the_prefix_expanded(self):
        for invalid in ('missing', 'foreign', 'old'):
            with self.subTest(invalid=invalid):
                trace = ResourceTrace(maximum=256)
                work_sequence(trace, 200, resource_step=costed_prefix(trace, 1))
                changes = trace.controller.report()['changes']
                good = costed_prefix(trace, 256)
                def unavailable(o, s, p):
                    if invalid == 'missing':
                        return None
                    step = good(o, s, p)
                    return replace(step, evidence=dict(step.evidence,
                        **({'engine_uid': 'foreign'} if invalid == 'foreign' else {'sample_age_s': 2.})))
                work_sequence(trace, 40, resource_step=unavailable)
                self.assertEqual(trace.controller.report()['changes'], changes)
                self.assertIsNotNone(trace.controller.report()['censored_frontier'])

    def test_censored_search_does_not_delay_pressure_or_retain_scores(self):
        for observation, permit in (({'preemptions_total': 1}, {}), ({'memory_pressure': True}, {}),
                ({}, {'rejected_requests_total': 1}), ({}, {'request_limit': 2})):
            with self.subTest(observation=observation, permit=permit):
                trace = work_sequence(ResourceTrace(maximum=256), 200, supply={'ready_items': 1})
                before = trace.controller.current_window
                decision = trace.tick(elapsed=.001, observation=observation, permit=permit, physical=True)
                self.assertEqual(decision.reason, 'resource_pressure')
                self.assertLess(decision.window, before)
                self.assertIsNone(trace.controller.report()['verified_performance_window'])
                self.assertIsNone(trace.controller.report()['censored_frontier'])

    def test_fresh_resources_can_probe_again_after_reference_loss_without_claiming_gain(self):
        trace = ResourceTrace(maximum=256)
        trace.tick(physical=(20., 10.))
        self.assertEqual(trace.tick(observation={'preemptions_total': None}).window, 1)
        trace.until(lambda r, _: r['window'] > 1, count=256, physical=(0., 0.))
        self.assertEqual(trace.controller.current_window, 256)

    def test_explicit_time_budget_reverts_an_unmeasured_large_trial(self):
        trace = ResourceTrace(maximum=256, options=ResourceRequestConfig(exploration_s=3.))
        self.assertGreater(trace.tick(physical=(20., 10.)).window, 128)
        work_sequence(trace, 5, rate=lambda c: 0, physical=(20., 10.))
        report = trace.controller.report()
        self.assertEqual(report['window'], 1)
        self.assertEqual(report['stop_reason'], 'exploration_budget')
        self.assertIsNone(report['verified_performance_window'])
        self.assertEqual(trace.controller.sampling_interval_s(0), .25)


if __name__ == '__main__':
    unittest.main()
