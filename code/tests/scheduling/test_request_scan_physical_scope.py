"""Physical conditions must be compared at the same confirmed request quota."""
from copy import deepcopy
from dataclasses import replace
import unittest

from src.scheduling.submission_control.request_scan import AscendingRequestController
from src.scheduling.submission_control.stage_work import default_request_feedback_options
from tests.scheduling.test_ascending_request_scan import ScanTrace
from tests.scheduling import test_resource_probe_variable_step as resource_fixtures


class RequestScanPhysicalScopeTests(unittest.TestCase):
    def test_exhausted_condition_is_held_without_refunding_attempts_or_reopening_each_poll(self):
        resources = resource_fixtures.ResourceProbeVariableStepTests()
        trace = ScanTrace(maximum=4)
        trace.controller = AscendingRequestController(4,
            replace(default_request_feedback_options(), max_candidate_attempts=1), clock=lambda: trace.now)
        trace.controller.reset(1, 100000)

        def drive(work):
            for _ in range(80):
                now, window = trace.now+1, trace.controller.current_window
                opportunity = resources.opportunity(current=window, maximum=4, now=now, index=int(now))
                trace.tick(40, work=work, resource_opportunity=opportunity)
            return trace.controller.report()

        for work in ((100, 4, 80, 20), (100, 4, 40, 60), (100, 4, 80, 20), (100, 4, 40, 60)):
            before = drive(work)
        self.assertEqual(before['stop_reason'], 'candidate_evidence_exhausted')
        after = drive((100, 4, 40, 60))
        self.assertEqual((after['changes'], after['work_epoch']), (before['changes'], before['work_epoch']))
        self.assertTrue(all(row['attempts'] <= 1 for row in trace.controller._search.points.values()))

    def test_quota_change_rebaselines_physical_conditions_without_hiding_later_headroom(self):
        resources = resource_fixtures.ResourceProbeVariableStepTests()

        def tick(trace, *, improved=False):
            now, window = trace.now+1, trace.controller.current_window
            busy = 5. if window == 1 else 20. if improved else 30.
            kv = .05 if window == 1 else .1
            opportunity = resources.opportunity(current=window, maximum=16, ready=4,
                gpu=busy, host=busy, kv=kv, now=now, index=int(now))
            return trace.tick(20*min(window, 4), ready_items=4, kv_usage=kv,
                              resource_opportunity=opportunity)

        for reused in (False, True):
            with self.subTest(reused=reused):
                trace = ScanTrace(maximum=16)
                for _ in range(100):
                    tick(trace)
                before = trace.controller.report()
                self.assertEqual((before['recommended_window'], before['window']), (4, 4))
                hint = trace.controller.export_hint()
                condition = hint['condition_evidence']['resource_condition']
                self.assertEqual(condition['observed_window'], 4)
                self.assertAlmostEqual(condition['idle_share'], .7,
                    msg='the historical C1 idle share .95 cannot be the monitoring baseline at C4')
                if reused:
                    trace = ScanTrace(maximum=16, hint=deepcopy(hint))
                for _ in range(60):
                    tick(trace)
                stable = trace.controller.report()
                self.assertEqual(stable['changes'], 0 if reused else before['changes'])
                for _ in range(60):
                    tick(trace, improved=True)
                after = trace.controller.report()
                self.assertGreater(after['changes'], stable['changes'],
                    'fresh idle share .8 at C4 improves on .7 even though it remains below the old C1 .95')
                self.assertTrue(any(row['window'] == 7 for row in after['history'][len(stable['history']):]))


if __name__ == '__main__':
    unittest.main()
