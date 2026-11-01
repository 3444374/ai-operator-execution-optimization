"""Further hint/coverage boundaries with deterministic useful-work fixtures.

Rates and physical fields are declared synthetic inputs. No model, hardware
observation or real query performance claim is involved.
"""
from copy import deepcopy
from dataclasses import replace
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.request_scan import AscendingRequestController
from src.scheduling.submission_control.resource_probe import ResourceProbeView, resource_probe_opportunity
from src.scheduling.submission_control.stage_work import SupplyState, default_request_feedback_options
from tests.scheduling.test_ascending_request_scan import ScanTrace


class F195ReviewBoundaryTests(unittest.TestCase):
    def test_gap_after_first_fresh_sample_does_not_relabel_old_resolved_work(self):
        prior = ScanTrace(maximum=4)
        prior.run_until(lambda report, _: report['stop_reason'] == 'capacity_limit'
                        and report['recommended_window'] == 4)
        hint = prior.controller.export_hint()
        reports = {}
        for gap in (False, True):
            current = ScanTrace(maximum=4, hint=deepcopy(hint))
            work = (100, 4, 40, 60)
            if gap:
                current.tick(80, work=work)
                current.tick(80, work=work, fresh=False, sample_age_s=2)
                current.tick(80, work=work)
            for _ in range(50):
                current.tick(80, work=work)
            reports[gap] = current.controller.report()
        self.assertGreater(reports[False]['work_epoch'], 0)
        self.assertGreater(reports[True]['work_epoch'], 0,
            'a validation gap cannot make old computed/cached 80/20 outcomes valid for the new 40/60 mix')

    def test_unfinished_hint_still_validates_observed_performance_shift(self):
        prior = ScanTrace(maximum=8)
        prior.run_until(lambda report, _: report['window'] >= 4 and report['recommended_window'] == 2)
        hint = prior.controller.export_hint()
        self.assertIsNone(hint['terminal'])
        current = ScanTrace(maximum=8, hint=hint)
        rates = {1: 120, 2: 100, 4: 50, 8: 25}
        for _ in range(100):
            current.tick(rates[current.controller.current_window])
        self.assertGreater(current.controller.report()['work_epoch'], 0,
            'terminal=None does not authorize bypassing a changed rate condition on a reused hint')

    def test_completed_hint_at_capacity_rechecks_lower_neighbor_after_performance_change(self):
        prior = ScanTrace(maximum=2)
        prior.run_until(lambda report, _: report['stop_reason'] == 'capacity_limit'
                        and report['recommended_window'] == 2)
        current = ScanTrace(maximum=2, hint=prior.controller.export_hint())
        for _ in range(60):
            current.tick(120 if current.controller.current_window == 1 else 100)
        self.assertTrue(any(event['window'] == 1 for event in current.controller.report()['history']),
            'a changed condition at the upper capacity bound still needs local lower-neighbor confirmation')

    def test_unknown_physical_coverage_does_not_reopen_the_same_no_gain_scan_repeatedly(self):
        reports = {}
        for periodic in (False, True):
            trace = ScanTrace(maximum=8)
            for index in range(200):
                now, window = trace.now+1, trace.controller.current_window
                unknown = periodic and (index//8) % 2 == 1
                observation = AdmissionObservation(now, True, window, window, 0, .1,
                    sample_age_s=0, preemptions_total=0)
                supply = SupplyState(now, 100, 200, 100, 1, True, False, active_requests=window)
                pair = tuple(ResourceProbeView('CPU-root-resource', 1, index*2+i+1,
                    now-.4+i*.1, now-.3+i*.1, .05,
                    ('GPU-00000000-0000-0000-0000-000000000001',),
                    None if unknown else 20., None if unknown else 25.) for i in (0, 1))
                opportunity = resource_probe_opportunity(observation, supply,
                    current_window=window, hard_limit=8, max_age_s=1.,
                    expected_engine_uid='CPU-root-resource', expected_session_id=1,
                    physical_samples=pair)
                trace.tick(40, resource_opportunity=opportunity)
            reports[periodic] = trace.controller.report()
        self.assertEqual(reports[False]['changes'], 2,
            'the continuous-observation control should make one useful flat comparison and its return')
        self.assertEqual(reports[True]['changes'], reports[False]['changes'],
            '8-known/8-unknown coverage with unchanged 20/25 values cannot manufacture new resource opportunities')

    def test_native_gap_during_reused_hint_validation_preserves_spent_attempts(self):
        options = replace(default_request_feedback_options(), max_candidate_attempts=1)
        prior = ScanTrace(maximum=4)
        prior.controller = AscendingRequestController(4, options, clock=lambda: prior.now)
        prior.controller.reset(1, 100000)
        prior.run_until(lambda report, _: report['window'] == 4 and report['recommended_window'] == 2)
        hint = prior.controller.export_hint()
        spent = {row['window']: row['attempts'] for row in hint['search_state']['points']}
        self.assertEqual(spent, {2: 1, 4: 1})
        current = ScanTrace(maximum=4)
        current.controller = AscendingRequestController(4, options, clock=lambda: current.now)
        current.controller.reset(hint['selected_window'], 100000, search_hint=hint)
        current.tick(40)
        current.tick(40, fresh=False, sample_age_s=2)
        current.tick(40)
        self.assertGreater(current.controller.report()['feedback_gaps'], 0)
        self.assertEqual({key: row['attempts'] for key, row in current.controller._search.points.items()}, spent,
            'a feedback gap cannot reset the already charged attempts in a compatible reused hint')
        for _ in range(20):
            current.tick(20*current.controller.current_window)
        self.assertEqual({key: row['attempts'] for key, row in current.controller._search.points.items()}, spent,
            'fresh same-work validation must retain the explicit attempt limit and its consumed budget')


if __name__ == '__main__':
    unittest.main()
