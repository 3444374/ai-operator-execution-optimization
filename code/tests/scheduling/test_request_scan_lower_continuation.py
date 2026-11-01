"""Resume an interrupted lower trial without manufacturing a new condition.

Only direction/target may continue. Fresh current-query work must establish its
confirmed reference before a pending lower target is measured again.
"""
from copy import deepcopy
import unittest

from tests.scheduling.test_ascending_request_scan import ScanTrace
from tests.scheduling import test_request_scan_transitions as transition_fixtures


_SEEDS = transition_fixtures.RequestScanTransitionTests()
_CURRENT_WORK = (100, 4, 40, 60)
_CONFIRMED_AND_R = ((4, 4), (8, 17))


class RequestScanLowerContinuationTests(unittest.TestCase):
    def pause_lower_trial(self, confirmed, maximum, phase):
        trace = _SEEDS.seed_selected(confirmed, maximum)
        lower = confirmed//2
        for _ in range(60):
            trace.tick(80, work=_CURRENT_WORK)
            controller = trace.controller
            probing = (controller.current_window == lower and controller._probe_at is not None
                       and not controller._confirming)
            confirming = (controller._confirming and controller._probe_candidate is not None
                          and controller._probe_candidate['window'] == lower
                          and controller.current_window == confirmed)
            if probing if phase == 'probing' else confirming:
                self.assertEqual(controller.report()['recommended_window'], confirmed)
                return trace
        self.fail('the original work did not naturally reach the requested lower-trial phase')

    def assert_fresh_continuation(self, trace, *, confirmed, lower, history_size, work_epoch, spent):
        for _ in range(60):
            trace.tick(80, work=_CURRENT_WORK)
            report = trace.controller.report()
            if len(report['history']) > history_size:
                break
        changes = report['history'][history_size:]
        self.assertTrue(changes, 'validation must continue the unfinished lower direction before another upward probe')
        self.assertEqual((changes[0]['before'], changes[0]['window'], changes[0]['reason']),
                         (confirmed, lower, 'local_probe_resume'))
        self.assertEqual(report['work_epoch'], work_epoch,
            'unchanged work and confirmed-quota rate must not manufacture another condition change to restart the lower probe')
        point = report['measurements'][-1]
        self.assertEqual((point['window'], point['rate_min'], point['rate_max']), (confirmed, 80., 80.))
        self.assertEqual(trace.controller._search.points[lower]['attempts'], spent[lower]+1)
        for key, count in spent.items():
            self.assertGreaterEqual(trace.controller._search.points[key]['attempts'], count)

    def test_healthy_hint_preserves_the_pending_lower_target_and_remeasures_it_first(self):
        for confirmed, maximum in _CONFIRMED_AND_R:
            for phase in ('probing', 'confirming'):
                with self.subTest(C=confirmed, R=maximum, phase=phase):
                    paused = self.pause_lower_trial(confirmed, maximum, phase)
                    hint = paused.controller.export_hint()
                    self.assertIsNotNone(hint)
                    self.assertEqual(hint['selected_window'], confirmed)
                    self.assertEqual(hint['local_probe_pending'], confirmed//2)
                    self.assertIs(type(hint['local_probe_pending']), int, 'pending metadata is a target, not an old score')
                    self.assertIsNone(hint['terminal'])
                    self.assertEqual((hint['condition_evidence']['rate_min'], hint['condition_evidence']['rate_max']),
                                     (80., 80.))
                    spent = {row['window']: row['attempts'] for row in hint['search_state']['points']}
                    current = ScanTrace(maximum=maximum, hint=deepcopy(hint))
                    self.assertEqual(current.controller.current_window, confirmed)
                    self.assertIsNone(current.controller.export_hint(), 'the next query must validate its own fresh reference')
                    self.assert_fresh_continuation(current, confirmed=confirmed, lower=confirmed//2,
                        history_size=0, work_epoch=0, spent=spent)

    def test_same_query_native_gap_preserves_the_pending_lower_direction(self):
        for confirmed, maximum in _CONFIRMED_AND_R:
            for phase in ('probing', 'confirming'):
                with self.subTest(C=confirmed, R=maximum, phase=phase):
                    trace = self.pause_lower_trial(confirmed, maximum, phase)
                    before = trace.controller.report()
                    spent = {key: row['attempts'] for key, row in trace.controller._search.points.items()}
                    trace.tick(80, work=_CURRENT_WORK, fresh=False, sample_age_s=2)
                    gap = trace.tick(80, work=_CURRENT_WORK)
                    self.assertEqual(gap.reason, 'feedback_gap')
                    self.assertEqual(trace.controller.current_window, confirmed)
                    self.assertIsNone(trace.controller.export_hint())
                    history_size = len(trace.controller.report()['history'])
                    self.assert_fresh_continuation(trace, confirmed=confirmed, lower=confirmed//2,
                        history_size=history_size, work_epoch=before['work_epoch'], spent=spent)


if __name__ == '__main__':
    unittest.main()
