"""Natural liveness and budget deferral over declared CPU-only observations.

All work counters, clocks and resource values are synthetic. These tests never
execute a model or infer real hardware headroom or query performance.
"""
from copy import deepcopy
from dataclasses import replace
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.request_scan import AscendingRequestController
from src.scheduling.submission_control.stage_work import default_request_feedback_options
from tests.scheduling.test_ascending_request_scan import ScanTrace
from tests.scheduling import test_resource_probe_variable_step as resource_fixtures


_RESOURCES = resource_fixtures.ResourceProbeVariableStepTests()
_CURRENT_WORK = (100, 4, 40, 60)


class RequestScanLivenessTests(unittest.TestCase):
    def trace(self, maximum, options=None, hint=None):
        trace = ScanTrace(maximum=maximum)
        options = default_request_feedback_options() if options is None else options
        trace.controller = AscendingRequestController(maximum, options, clock=lambda: trace.now)
        trace.controller.reset(1 if hint is None else hint['selected_window'], 100000,
                               search_hint=deepcopy(hint))
        return trace

    def resource_tick(self, trace, work, *, improved=False, count=40, **changes):
        now, current = trace.now+1, trace.controller.current_window
        opportunity = _RESOURCES.opportunity(current=current, maximum=trace.controller.config.max_window,
            ready=100, gpu=5. if improved else 20., host=5. if improved else 25.,
            now=now, index=int(now), observation_changes={
                key: value for key, value in changes.items() if key in AdmissionObservation.__dataclass_fields__})
        return trace.tick(count, work=work, resource_opportunity=opportunity, **changes)

    def seed_exhausted(self, maximum=16):
        options = replace(default_request_feedback_options(), max_candidate_attempts=1)
        trace = self.trace(maximum, options)
        for block in range(12):
            work = (100, 4, 80, 20) if block % 2 == 0 else _CURRENT_WORK
            for _ in range(50):
                self.resource_tick(trace, work)
            if trace.controller.report()['stop_reason'] == 'candidate_evidence_exhausted':
                return trace, work
        self.fail('the useful work must naturally exhaust the declared one-attempt candidates')

    def seed_capacity(self, maximum, options=None):
        trace = self.trace(maximum, options)
        trace.run_until(lambda report, _: report['stop_reason'] == 'capacity_limit'
                        and report['recommended_window'] == maximum)
        return trace

    def pause_lower(self, maximum=4, *, options=None, phase='probing'):
        trace = self.seed_capacity(maximum, options)
        for _ in range(60):
            trace.tick(80, work=_CURRENT_WORK)
            controller = trace.controller
            probing = (controller.current_window == maximum//2 and controller._probe_at is not None
                       and not controller._confirming)
            confirming = (controller._confirming and controller._probe_candidate is not None
                          and controller._probe_candidate['window'] == maximum//2)
            if probing if phase == 'probing' else confirming:
                return trace
        self.fail('the useful work must naturally reach the requested pending-lower phase')

    def interrupt_or_reuse(self, trace, mode):
        if mode == 'healthy_hint':
            hint = trace.controller.export_hint()
            self.assertIsNotNone(hint)
            return self.trace(trace.controller.config.max_window, trace.controller.options, hint)
        trace.tick(80, work=_CURRENT_WORK, fresh=False, sample_age_s=2)
        self.assertEqual(trace.tick(80, work=_CURRENT_WORK).reason, 'feedback_gap')
        return trace

    def test_exhausted_evidence_native_gap_still_allows_a_new_legal_resource_target(self):
        for mode in ('no_gap', 'native_gap', 'healthy_hint'):
            with self.subTest(mode=mode):
                trace, work = self.seed_exhausted()
                spent = {key: row['attempts'] for key, row in trace.controller._search.points.items()}
                self.assertNotIn(14, spent)
                if mode == 'native_gap':
                    self.resource_tick(trace, work, fresh=False, sample_age_s=2)
                elif mode == 'healthy_hint':
                    hint = trace.controller.export_hint()
                    self.assertIsNotNone(hint)
                    trace = self.trace(16, trace.controller.options, hint)
                start = len(trace.controller.report()['history'])
                for _ in range(100):
                    self.resource_tick(trace, work, improved=True)
                report = trace.controller.report()
                self.assertIn(14, [row['window'] for row in report['history'][start:]],
                    'an exhausted-evidence stop cannot permanently block fresh reference validation after a native gap')
                for key, count in spent.items():
                    self.assertGreaterEqual(trace.controller._search.points[key]['attempts'], count)

    def test_budget_blocked_lower_target_remains_pending_for_the_next_healthy_query(self):
        with self.subTest(R=4, max_changes=5, interruption='native_gap'):
            options = replace(default_request_feedback_options(), max_changes=5)
            trace = self.trace(4, options)
            trace.run_until(lambda report, _: report['stop_reason'] == 'capacity_limit'
                            and report['recommended_window'] == 4)
            for _ in range(50):
                trace.tick(80, work=_CURRENT_WORK)
                if trace.controller.current_window == 2 and trace.controller._probe_at is not None:
                    break
            self.assertEqual((trace.controller.current_window, trace.controller.report()['changes']), (2, 3))
            trace.tick(80, work=_CURRENT_WORK, fresh=False, sample_age_s=2)
            trace.tick(80, work=_CURRENT_WORK)
            self.assertEqual(trace.controller.current_window, 4)
            self.assertEqual(trace.controller.report()['local_probe_pending'], 2)
            for _ in range(60):
                trace.tick(80, work=_CURRENT_WORK)
            report = trace.controller.report()
            self.assertEqual((report['stop_reason'], report['changes']), ('exploration_budget', 4))
            hint = trace.controller.export_hint()
            self.assertIsNotNone(hint)
            self.assertEqual(hint['local_probe_pending'], 2)
            following = self.trace(4, options, hint)
            for _ in range(60):
                following.tick(80, work=_CURRENT_WORK)
            self.assertTrue(any(row['window'] < 4 for row in following.controller.report()['history']))

    def test_original_fourteen_budget_boundaries_keep_lower_intent_and_censor_controls(self):
        for maximum, baseline_changes in ((4, 2), (8, 3)):
            for gap, extra in ((False, 1), (False, 2), (False, 3),
                               (True, 2), (True, 3), (True, 4), (True, 5)):
                limit = baseline_changes+extra
                with self.subTest(R=maximum, max_changes=limit, gap=gap):
                    options = replace(default_request_feedback_options(), max_changes=limit)
                    trace = self.trace(maximum, options)
                    trace.run_until(lambda report, _: report['stop_reason'] == 'capacity_limit'
                                    and report['recommended_window'] == maximum)
                    self.assertEqual(trace.controller.report()['changes'], baseline_changes)
                    if gap:
                        for _ in range(60):
                            trace.tick(80, work=_CURRENT_WORK)
                            if trace.controller.current_window == maximum//2:
                                break
                        self.assertEqual(trace.controller.current_window, maximum//2)
                        trace.tick(80, work=_CURRENT_WORK, fresh=False, sample_age_s=2)
                        trace.tick(80, work=_CURRENT_WORK)
                    for _ in range(90):
                        trace.tick(80, work=_CURRENT_WORK)
                    report, hint = trace.controller.report(), trace.controller.export_hint()
                    self.assertLessEqual(report['changes'], limit)
                    self.assertEqual(hint is None, gap and extra == 2,
                        'only the two fully spent reference-validation controls are censored; other deferrals keep a qualified reference')
                    if hint is None:
                        # A spent budget can censor the reference itself. Do
                        # not demand publication before its fresh validation.
                        self.assertTrue(report['fresh_state_validation_pending'])
                        self.assertEqual(report['stop_reason'], 'exploration_budget')
                        continue
                    following = self.trace(maximum, options, hint)
                    for _ in range(90):
                        following.tick(80, work=_CURRENT_WORK)
                    after = following.controller.report()
                    self.assertLessEqual(after['changes'], limit)
                    if report['stop_reason'] == 'exploration_budget':
                        self.assertIsNotNone(hint['local_probe_pending'],
                            'budget-only deferral preserves unfinished lower work instead of calling it a platform')
                        self.assertTrue(any(row['window'] < maximum for row in after['history']))
                    else:
                        self.assertLess(report['recommended_window'], maximum)
                        self.assertIsNone(hint['local_probe_pending'])

    def test_exhausted_candidate_liveness_extends_to_other_hard_bounds(self):
        for maximum in (8, 33):
            for mode in ('no_gap', 'native_gap', 'healthy_hint'):
                with self.subTest(R=maximum, mode=mode):
                    trace, work = self.seed_exhausted(maximum)
                    spent = {key: row['attempts'] for key, row in trace.controller._search.points.items()}
                    if mode == 'native_gap':
                        self.resource_tick(trace, work, fresh=False, sample_age_s=2)
                    elif mode == 'healthy_hint':
                        hint = trace.controller.export_hint()
                        self.assertIsNotNone(hint)
                        trace = self.trace(maximum, trace.controller.options, hint)
                    now = trace.now+1
                    target = _RESOURCES.opportunity(current=trace.controller.current_window, maximum=maximum,
                        ready=100, gpu=5., host=5., now=now, index=int(now)).probe_target_window
                    self.assertIsNotNone(target)
                    self.assertNotIn(target, spent)
                    start = len(trace.controller.report()['history'])
                    for _ in range(100):
                        self.resource_tick(trace, work, improved=True)
                    report = trace.controller.report()
                    self.assertIn(target, [row['window'] for row in report['history'][start:]])
                    self.assertFalse(report['fresh_state_validation_pending'])
                    self.assertIsNone(report['query_exploration_pause'])
                    for key, count in spent.items():
                        self.assertGreaterEqual(trace.controller._search.points[key]['attempts'], count)

    def test_held_platform_regression_and_capacity_states_restore_the_right_kind_of_progress(self):
        for state in ('local_plateau', 'performance_regression', 'capacity_limit'):
            for mode in ('native_gap', 'healthy_hint'):
                with self.subTest(state=state, mode=mode):
                    maximum = 4 if state == 'capacity_limit' else 8
                    trace = self.trace(maximum)
                    rate = (lambda window: 20*window) if state == 'capacity_limit' else (
                        (lambda _: 40) if state == 'local_plateau' else (lambda window: 40 if window == 1 else 20))
                    trace.run_until(lambda report, _: report['stop_reason'] == state, rates=rate)
                    if mode == 'native_gap':
                        trace.tick(rate(trace.controller.current_window), fresh=False, sample_age_s=2)
                    else:
                        hint = trace.controller.export_hint()
                        self.assertIsNotNone(hint)
                        trace = self.trace(maximum, trace.controller.options, hint)
                    start = len(trace.controller.report()['history'])
                    for _ in range(80):
                        self.resource_tick(trace, (100, 4, 80, 20), improved=True,
                                           count=rate(trace.controller.current_window))
                    report = trace.controller.report()
                    self.assertFalse(report['fresh_state_validation_pending'])
                    self.assertTrue(report['comparable_selection'])
                    self.assertIsNotNone(trace.controller.export_hint())
                    if state == 'capacity_limit':
                        self.assertEqual(report['window'], maximum)
                        self.assertEqual(len(report['history']), start, 'no larger legal quota exists at the hard bound')
                    else:
                        self.assertTrue(any(row['window'] > 2 for row in report['history'][start:]),
                            'a newly qualified opportunity must be actionable after the fresh held reference is measured')

    def test_time_budget_defers_initial_or_confirmation_lower_work_until_a_healthy_query(self):
        for maximum, seconds, phase in ((4, 20., 'initial'), (4, 24., 'confirmation'),
                                        (8, 25., 'initial'), (8, 29., 'confirmation')):
            with self.subTest(R=maximum, seconds=seconds, phase=phase):
                options = replace(default_request_feedback_options(), exploration_s=seconds)
                trace = self.seed_capacity(maximum, options)
                for _ in range(90):
                    trace.tick(80, work=_CURRENT_WORK)
                report, hint = trace.controller.report(), trace.controller.export_hint()
                self.assertEqual(report['query_exploration_pause'], 'exploration_budget')
                self.assertEqual(report['continuation_state'], 'query_paused')
                self.assertEqual(report['local_probe_pending'], maximum//2)
                self.assertIsNotNone(hint)
                spent = {key: row['attempts'] for key, row in trace.controller._search.points.items()}
                following = self.trace(maximum, options, hint)
                self.assertIsNone(following.controller.report()['query_exploration_pause'])
                for _ in range(30):
                    following.tick(80, work=_CURRENT_WORK)
                    if following.controller.report()['history']:
                        break
                fresh = following.controller.report()
                self.assertEqual((fresh['measurements'][0]['window'], fresh['measurements'][0]['rate_min']),
                                 (maximum, 80.))
                self.assertEqual(fresh['history'][0]['window'], maximum//2)
                for key, count in spent.items():
                    self.assertGreaterEqual(following.controller._search.points[key]['attempts'], count)

    def test_exhausted_lower_attempts_pause_explicitly_and_keep_intent_across_healthy_query(self):
        options = replace(default_request_feedback_options(), max_candidate_attempts=2)
        for mode in ('native_gap', 'healthy_hint'):
            for phase in ('probing', 'confirming'):
                with self.subTest(mode=mode, phase=phase):
                    trace = self.interrupt_or_reuse(self.pause_lower(options=options, phase=phase), mode)
                    spent = {key: row['attempts'] for key, row in trace.controller._search.points.items()}
                    self.assertEqual(spent[2], 2)
                    for _ in range(60):
                        trace.tick(80, work=_CURRENT_WORK)
                    report, hint = trace.controller.report(), trace.controller.export_hint()
                    self.assertEqual(report['local_probe_pending'], 2)
                    self.assertEqual(report['local_probe_blocked_by'], 'candidate_attempt_limit')
                    self.assertEqual(report['continuation_state'], 'local_probe_blocked')
                    self.assertFalse(report['unconfirmed_trial'])
                    self.assertIsNotNone(hint)
                    for key, count in spent.items():
                        self.assertEqual(trace.controller._search.points[key]['attempts'], count)
                    following = self.trace(4, options, hint)
                    for _ in range(60):
                        following.tick(80, work=_CURRENT_WORK)
                    self.assertEqual(following.controller.report()['local_probe_pending'], 2)
                    self.assertEqual(following.controller.report()['local_probe_blocked_by'], 'candidate_attempt_limit')
                    self.assertEqual(following.controller._search.points[2]['attempts'], 2)

    def test_missing_native_evidence_preserves_intent_without_actions_or_unverified_hint_publication(self):
        for mode in ('native_gap', 'healthy_hint'):
            for phase in ('probing', 'confirming'):
                with self.subTest(mode=mode, phase=phase):
                    trace = self.interrupt_or_reuse(self.pause_lower(phase=phase), mode)
                    before = trace.controller.report()
                    spent = {key: row['attempts'] for key, row in trace.controller._search.points.items()}
                    for _ in range(30):
                        trace.tick(80, work=_CURRENT_WORK, fresh=False, sample_age_s=2)
                    report = trace.controller.report()
                    self.assertEqual(report['local_probe_pending'], 2)
                    self.assertEqual(report['continuation_state'], 'reference_pending')
                    self.assertEqual(report['changes'], before['changes'])
                    self.assertTrue(report['fresh_state_validation_pending'])
                    self.assertIsNone(trace.controller.export_hint())
                    for key, count in spent.items():
                        self.assertEqual(trace.controller._search.points[key]['attempts'], count)

    def test_real_pressure_or_new_work_replaces_the_old_lower_intent(self):
        for mode in ('native_gap', 'healthy_hint'):
            for event in ('pressure', 'new_work'):
                with self.subTest(mode=mode, event=event):
                    trace = self.interrupt_or_reuse(self.pause_lower(), mode)
                    before = trace.controller.report()
                    if event == 'pressure':
                        trace.tick(80, work=_CURRENT_WORK, memory_pressure=True)
                        self.assertIsNone(trace.controller.report()['local_probe_pending'])
                        self.assertIsNone(trace.controller.export_hint())
                    else:
                        for _ in range(60):
                            trace.tick(80, work=(100, 4, 80, 20))
                            if trace.controller.report()['changes'] > before['changes']:
                                break
                        report = trace.controller.report()
                        self.assertGreater(report['work_epoch'], before['work_epoch'])
                        self.assertEqual(report['history'][len(before['history'])]['reason'], 'condition_changed_probe',
                            'a new work epoch chooses its own local validation instead of resuming the old score context')


if __name__ == '__main__':
    unittest.main()
