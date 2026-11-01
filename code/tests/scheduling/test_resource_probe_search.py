"""Resource opportunities reopen local search without a request cost model."""
from copy import deepcopy
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.resource_probe import resource_probe_opportunity
from src.scheduling.submission_control.stage_work import SupplyState
from tests.scheduling.test_ascending_request_scan import ScanTrace


class ResourceSearchTests(unittest.TestCase):
    def test_temporary_handoff_is_not_a_new_resource_condition_after_selection(self):
        trace = ScanTrace()
        for _ in range(70):
            now, window = trace.now+1, trace.controller.current_window
            handoff = trace.controller._confirming
            active = 0 if handoff else window
            o = AdmissionObservation(now, True, active, active, 0, .1, sample_age_s=0)
            s = SupplyState(now, 100, 200, 100, 1, True, False, active_requests=active)
            opportunity = resource_probe_opportunity(o, s, current_window=window,
                hard_limit=8, max_age_s=1, expected_engine_uid='CPU-resource-engine', expected_session_id=1)
            trace.tick(40, inflight=active, active_requests=active, resource_opportunity=opportunity)
        report = trace.controller.report()
        self.assertEqual(report['stop_reason'], 'local_plateau')
        self.assertEqual(report['changes'], 2, 'a transient unqualified view must not erase the known resource condition')

    def test_uncosted_native_opportunity_reopens_once_then_uses_throughput_to_hold(self):
        trace = ScanTrace()
        trace.run_until(lambda r, _: r['stop_reason'] == 'local_plateau', rates=lambda _: 40)
        original = trace.controller.report()['changes']

        def tick():
            now, window = trace.now+1, trace.controller.current_window
            observation = AdmissionObservation(now, True, window, window, 0, .1, sample_age_s=0)
            supply = SupplyState(now, 100, 200, 100, 1, True, False, active_requests=window)
            opportunity = resource_probe_opportunity(observation, supply, current_window=window,
                hard_limit=8, max_age_s=1, expected_engine_uid='CPU-resource-engine', expected_session_id=1)
            return trace.tick(40, resource_opportunity=opportunity)

        for _ in range(45):tick()
        report = trace.controller.report()
        self.assertGreater(report['changes'], original)
        self.assertTrue(any(row['source'] == 'resource_feedback_probe' and row['evidence'] is None
                            for row in report['step_decisions']))
        self.assertEqual(report['stop_reason'], 'local_plateau')
        self.assertEqual(report['recommended_window'], 1, 'no throughput gain: do not optimize busy alone')
        stable_changes = report['changes']
        for _ in range(30):tick()
        self.assertEqual(trace.controller.report()['changes'], stable_changes,
                         'identical resource polls must not keep reopening the same failed trial')

    def test_old_terminal_without_condition_evidence_is_not_claimed_freshly_validated(self):
        prior = ScanTrace()
        prior.run_until(lambda r, _: r['stop_reason'] == 'local_plateau', rates=lambda _: 40)
        hint = prior.controller.export_hint()
        hint.pop('condition_evidence')  # Exact pre-fix in-memory schema.
        following = ScanTrace(hint=hint)
        for _ in range(7):following.tick(40)
        report = following.controller.report()
        self.assertTrue(any(row['kind'] == 'cached_condition_unverified' for row in report['evaluations']))
        self.assertGreater(report['changes'], 0)

    def test_invalid_historical_band_is_rejected_instead_of_becoming_a_current_score(self):
        prior = ScanTrace()
        prior.run_until(lambda r, _: r['stop_reason'] == 'local_plateau', rates=lambda _: 40)
        original = prior.controller.export_hint()
        for value in (None, -1, float('nan'), float('inf'), True):
            hint = deepcopy(original)
            hint['condition_evidence']['rate_min'] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'condition evidence'):
                ScanTrace(hint=hint)


if __name__ == '__main__':
    unittest.main()
