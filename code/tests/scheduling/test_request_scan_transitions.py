"""State transitions over actual controller decisions and synthetic useful work.

Resource percentages, ready counts and rates are declared CPU fixtures. No
model call, hardware observation or real query performance is measured here.
"""
from copy import deepcopy
from dataclasses import replace
import unittest

from src.scheduling.submission_control.request_scan import AscendingRequestController
from src.scheduling.submission_control.stage_work import default_request_feedback_options
from tests.scheduling.test_ascending_request_scan import ScanTrace
from tests.scheduling import test_resource_probe_variable_step as resource_fixtures


_RESOURCES = resource_fixtures.ResourceProbeVariableStepTests()
_CONFIRMED_AND_R = ((2, 2), (2, 7), (4, 4), (4, 9), (8, 8), (8, 17))
_READY_TRANSITIONS = ((4, 1, 3), (9, 2, 6), (17, 3, 11), (64, 4, 100))


class RequestScanTransitionTests(unittest.TestCase):
    def seed_selected(self, confirmed, maximum):
        trace = ScanTrace(maximum=maximum)
        stop = 'capacity_limit' if confirmed == maximum else 'local_plateau'
        trace.run_until(lambda report, _: report['stop_reason'] == stop
                        and report['recommended_window'] == confirmed,
                        rates=lambda window: 20*min(window, confirmed))
        return trace

    def seed_ready(self, maximum, ready, *, options=None):
        trace = ScanTrace(maximum=maximum)
        if options is not None:
            trace.controller = AscendingRequestController(maximum, options, clock=lambda: trace.now)
            trace.controller.reset(1, 100000)
        report = _RESOURCES.drive(trace, ticks=100, ready=ready)
        self.assertEqual((report['changes'], report['recommended_window']), (2, 1))
        return trace

    def reuse(self, previous):
        hint = previous.controller.export_hint()
        self.assertIsNotNone(hint)
        trace = ScanTrace(maximum=previous.controller.config.max_window)
        trace.controller = AscendingRequestController(previous.controller.config.max_window,
            previous.controller.options, clock=lambda: trace.now)
        trace.controller.reset(hint['selected_window'], 100000, search_hint=deepcopy(hint))
        return trace

    def test_original_more_ready_same_hardware_reopens_a_supply_limited_trial(self):
        with self.subTest(boundary='reviewer_ready_frontier'):
            trace = ScanTrace(maximum=64)
            before = _RESOURCES.drive(trace, ticks=100, ready=1)
            self.assertEqual((before['changes'], before['recommended_window']), (2, 1))
            now = trace.now+1
            proposal = _RESOURCES.opportunity(current=1, maximum=64, ready=100, now=now, index=int(now))
            self.assertEqual(proposal.probe_target_window, 48)
            after = _RESOURCES.drive(trace, ticks=100, ready=100)
            self.assertGreater(after['changes'], before['changes'],
                'new real ready work exposes an untested legal target even with unchanged physical percentages')

    def test_original_same_query_gap_cache_change_still_checks_lower_neighbor(self):
        rates = {1: 120, 2: 100, 4: 80}
        for gap in (False, True):
            with self.subTest(boundary='reviewer_same_query_work_change', gap=gap):
                trace = ScanTrace(maximum=4)
                before = trace.run_until(lambda report, _: report['stop_reason'] == 'capacity_limit'
                                         and report['recommended_window'] == 4)
                count = len(before['history'])
                if gap:
                    trace.tick(rates[trace.controller.current_window], work=(100, 4, 40, 60),
                               fresh=False, sample_age_s=2)
                for _ in range(60):
                    trace.tick(rates[trace.controller.current_window], work=(100, 4, 40, 60))
                after = trace.controller.report()
                self.assertGreater(after['work_epoch'], 0)
                self.assertTrue(any(event['window'] < 4 for event in after['history'][count:]),
                    'detecting a changed cache mix after a gap must still perform local lower-neighbor validation')

    def test_more_ready_frontier_is_actionable_in_the_same_query_or_a_healthy_hint(self):
        for maximum, small, large in _READY_TRANSITIONS:
            for reused in (False, True):
                with self.subTest(R=maximum, ready_before=small, ready_after=large, reused=reused):
                    trace = self.seed_ready(maximum, small)
                    if reused:
                        trace = self.reuse(trace)
                        _RESOURCES.drive(trace, ticks=60, ready=small)
                        self.assertFalse(trace.controller.report()['fresh_state_validation_pending'])
                    before = trace.controller.report()
                    now, window = trace.now+1, trace.controller.current_window
                    proposal = _RESOURCES.opportunity(current=window, maximum=maximum, ready=large,
                                                     now=now, index=int(now))
                    tested_before = {row['window'] for row in before['history']}
                    self.assertNotIn(proposal.probe_target_window, tested_before)
                    after = _RESOURCES.drive(trace, ticks=100, ready=large)
                    self.assertGreater(after['changes'], before['changes'])
                    self.assertTrue(any(row['window'] == proposal.probe_target_window
                                        for row in after['history'][len(before['history']):]),
                        'new ready demand must expose its previously untested legal trial, without inventing a hardware change')
                    self.assertTrue(all(1 <= row['window'] <= maximum for row in after['history']))

    def test_condition_change_and_native_gap_have_the_same_local_validation_path(self):
        for confirmed, maximum in _CONFIRMED_AND_R:
            for reused in (False, True):
                for change in ('cache_mix', 'rate'):
                    for gap in (False, True):
                        with self.subTest(C=confirmed, R=maximum, reused=reused, change=change, gap=gap):
                            trace = self.seed_selected(confirmed, maximum)
                            if reused:
                                trace = self.reuse(trace)
                                trace.run_until(lambda report, _: not report['fresh_state_validation_pending'],
                                    rates=lambda window: 20*min(window, confirmed))
                            before = trace.controller.report()
                            work = (100, 4, 40, 60) if change == 'cache_mix' else (100, 4, 80, 20)

                            def rate(window):
                                count = 20*confirmed+5*(confirmed-window)
                                return count if change == 'cache_mix' else 2*count

                            if gap:
                                trace.tick(rate(trace.controller.current_window), work=work,
                                           fresh=False, sample_age_s=2)
                            for _ in range(60):
                                trace.tick(rate(trace.controller.current_window), work=work)
                            after = trace.controller.report()
                            self.assertGreater(after['work_epoch'], before['work_epoch'])
                            transitions = after['history'][len(before['history']):]
                            self.assertTrue(any(row['window'] < confirmed for row in transitions),
                                'a changed same-query condition needs local downward evidence even when a gap cleared the reference')
                            self.assertTrue(all(1 <= row['window'] <= maximum for row in after['history']))

    def test_ready_decline_unknown_coverage_and_return_to_the_same_frontier_do_not_rescan(self):
        for maximum, _, ready in _READY_TRANSITIONS:
            for reused in (False, True):
                with self.subTest(R=maximum, known_ready=ready, reused=reused):
                    trace = self.seed_ready(maximum, ready)
                    if reused:
                        trace = self.reuse(trace)
                        _RESOURCES.drive(trace, ticks=60, ready=ready)
                    before = trace.controller.report()
                    _RESOURCES.drive(trace, ticks=12, ready=0, gpu=None, host=None)
                    _RESOURCES.drive(trace, ticks=12, ready=1)
                    after = _RESOURCES.drive(trace, ticks=100, ready=ready)
                    self.assertEqual(after['changes'], before['changes'],
                        'a lower or unobserved ready frontier must not erase the previously tested larger frontier')

    def test_current_quota_changes_do_not_count_as_more_ready_work(self):
        for maximum, ready in ((4, 1), (9, 3), (17, 5), (64, 16)):
            with self.subTest(R=maximum, constant_ready=ready):
                trace = self.seed_ready(maximum, ready)
                first = trace.controller.report()
                self.assertGreater(first['history'][0]['window'], first['history'][0]['before'])
                after = _RESOURCES.drive(trace, ticks=150, ready=ready)
                self.assertEqual(after['changes'], 2,
                    'quota itself changes the candidate interval, but it is not an arrival of more ready records')

    def test_more_ready_does_not_refund_spent_candidate_attempts_in_either_lifecycle(self):
        options = replace(default_request_feedback_options(), max_candidate_attempts=1)
        for maximum, small, large in ((9, 2, 6), (17, 3, 11)):
            for reused in (False, True):
                with self.subTest(R=maximum, ready_before=small, ready_after=large, reused=reused):
                    trace = self.seed_ready(maximum, small, options=options)
                    if reused:
                        trace = self.reuse(trace)
                        _RESOURCES.drive(trace, ticks=60, ready=small)
                    spent = {key: row['attempts'] for key, row in trace.controller._search.points.items()}
                    before = trace.controller.report()
                    after = _RESOURCES.drive(trace, ticks=100, ready=large)
                    self.assertGreater(after['changes'], before['changes'])
                    for key, count in spent.items():
                        self.assertGreaterEqual(trace.controller._search.points[key]['attempts'], count)
                    self.assertTrue(all(row['attempts'] <= 1 for row in trace.controller._search.points.values()))

    def test_same_query_explicit_change_budget_survives_more_supply_and_feedback_gap(self):
        options = replace(default_request_feedback_options(), max_changes=2, max_candidate_attempts=1)
        for maximum, small, large in ((9, 2, 6), (17, 3, 11)):
            for gap in (False, True):
                with self.subTest(R=maximum, ready_before=small, ready_after=large, gap=gap):
                    trace = self.seed_ready(maximum, small, options=options)
                    spent = {key: row['attempts'] for key, row in trace.controller._search.points.items()}
                    before = trace.controller.report()
                    if gap:
                        trace.tick(40, fresh=False, sample_age_s=2)
                    after = _RESOURCES.drive(trace, ticks=100, ready=large)
                    self.assertEqual(after['changes'], 2)
                    self.assertGreaterEqual(after['exploration_wall_s'], before['exploration_wall_s'])
                    for key, count in spent.items():
                        self.assertEqual(trace.controller._search.points[key]['attempts'], count)

    def test_unchanged_healthy_hint_retains_its_qualified_selection_without_a_new_scan(self):
        for confirmed, maximum in _CONFIRMED_AND_R:
            with self.subTest(C=confirmed, R=maximum):
                trace = self.reuse(self.seed_selected(confirmed, maximum))
                self.assertFalse(trace.controller.report()['comparable_selection'])
                for _ in range(60):
                    trace.tick(20*confirmed)
                report = trace.controller.report()
                self.assertTrue(report['state_reused'])
                self.assertFalse(report['fresh_state_validation_pending'])
                self.assertTrue(report['comparable_selection'])
                self.assertEqual((report['window'], report['recommended_window'], report['changes']),
                                 (confirmed, confirmed, 0))

    def test_pressure_before_hint_validation_does_not_apply_old_quota_rates_to_the_reduced_quota(self):
        for confirmed, maximum in ((2, 2), (4, 4), (8, 17)):
            for pressure in ('memory', 'native_KV'):
                with self.subTest(C=confirmed, R=maximum, pressure=pressure):
                    trace = self.reuse(self.seed_selected(confirmed, maximum))
                    if pressure == 'memory':
                        decision = trace.tick(20*confirmed, memory_pressure=True)
                    else:
                        trace.tick(20*confirmed, kv_usage=.99)
                        decision = trace.tick(20*confirmed, kv_usage=.99)
                    self.assertEqual(decision.reason, 'resource_pressure')
                    reduced = trace.controller.current_window
                    self.assertLess(reduced, confirmed)
                    self.assertIsNone(trace.controller.export_hint())
                    for _ in range(30):
                        trace.tick(20*min(trace.controller.current_window, confirmed))
                    report = trace.controller.report()
                    self.assertTrue(report['measurements'])
                    self.assertEqual(report['measurements'][0]['window'], reduced)
                    self.assertEqual(report['measurements'][0]['rate_min'], 20*reduced)
                    self.assertNotIn('cached_performance_changed', [event['kind'] for event in report['evaluations']],
                        'a slower rate at a smaller quota is not a changed condition at the imported larger quota')

    def test_invalid_resource_frontier_is_rejected_and_legacy_absence_keeps_tried_range(self):
        previous = self.seed_ready(17, 3)
        hint = previous.controller.export_hint()
        frontier = hint['condition_evidence']['resource_frontier']
        malformed = (
            ('not_object', []),
            ('missing_fields', {}),
            ('negative_ready', dict(frontier, ready_items=-1)),
            ('boolean_ready', dict(frontier, ready_items=True)),
            ('zero_window', dict(frontier, window=0)),
            ('window_above_range', dict(frontier, window=5, range_end=4)),
            ('range_exceeds_R', dict(frontier, range_end=18)),
            ('extra_field', dict(frontier, unrelated=1)),
            ('untried_non_geometric_window', dict(ready_items=5, window=5, range_end=6)),
            ('untried_geometric_window', dict(ready_items=4, window=4, range_end=5)),
        )
        for kind, value in malformed:
            with self.subTest(frontier=kind):
                changed = deepcopy(hint)
                changed['condition_evidence']['resource_frontier'] = value
                current = ScanTrace(maximum=17)
                with self.assertRaises(ValueError):
                    current.controller.reset(changed['selected_window'], 100000, search_hint=changed)
        for absence in ('omitted', 'unknown'):
            with self.subTest(legacy_frontier=absence):
                legacy = deepcopy(hint)
                if absence == 'omitted':
                    legacy['condition_evidence'].pop('resource_frontier')
                else:
                    legacy['condition_evidence']['resource_frontier'] = None
                current = ScanTrace(maximum=17)
                current.controller.reset(legacy['selected_window'], 100000, search_hint=legacy)
                unchanged = _RESOURCES.drive(current, ticks=60, ready=3)
                self.assertTrue(unchanged['comparable_selection'])
                self.assertEqual(unchanged['changes'], 0, 'missing frontier data does not make already tried quota three new')
                before_attempts = {key: row['attempts'] for key, row in current.controller._search.points.items()}
                expanded = _RESOURCES.drive(current, ticks=100, ready=11)
                self.assertGreater(expanded['changes'], unchanged['changes'])
                for key, count in before_attempts.items():
                    self.assertGreaterEqual(current.controller._search.points[key]['attempts'], count)

    def test_new_work_epoch_discards_old_resource_frontier_without_refunding_attempts(self):
        for maximum in (8, 17):
            for reused in (False, True):
                with self.subTest(R=maximum, reused=reused):
                    trace = ScanTrace(maximum=maximum)
                    for _ in range(150):
                        window, now = trace.controller.current_window, trace.now+1
                        opportunity = _RESOURCES.opportunity(current=window, maximum=maximum,
                                                             now=now, index=int(now))
                        trace.tick(20*window, resource_opportunity=opportunity)
                        report = trace.controller.report()
                        if report['stop_reason'] == 'capacity_limit' and report['recommended_window'] == maximum:
                            break
                    before = trace.controller.report()
                    self.assertEqual(before['recommended_window'], maximum)
                    self.assertIsNotNone(before['resource_frontier'])
                    if reused:
                        trace = self.reuse(trace)
                        self.assertIsNotNone(trace.controller.report()['resource_frontier'])
                    spent = {key: row['attempts'] for key, row in trace.controller._search.points.items()}
                    initial_changes = trace.controller.report()['changes']
                    for _ in range(60):
                        window, now = trace.controller.current_window, trace.now+1
                        opportunity = _RESOURCES.opportunity(current=window, maximum=maximum,
                                                             now=now, index=int(now))
                        trace.tick(20*window, work=(100, 4, 40, 60), resource_opportunity=opportunity)
                        if trace.controller.report()['changes'] > initial_changes:
                            break
                    after = trace.controller.report()
                    self.assertGreater(after['work_epoch'], 0)
                    self.assertLess(after['window'], maximum, 'the work change must actually start local lower-quota validation')
                    self.assertIsNone(after['resource_frontier'],
                        'the old work condition cannot declare the new work condition already explored through R')
                    fresh_hint = trace.controller.export_hint()
                    self.assertIsNotNone(fresh_hint)
                    self.assertIsNone(fresh_hint['condition_evidence']['resource_condition'])
                    for key, count in spent.items():
                        self.assertGreaterEqual(trace.controller._search.points[key]['attempts'], count)


if __name__ == '__main__':
    unittest.main()
