"""Review boundary regressions using deterministic original-work observations.

Native KV frames and costs here are explicitly synthetic, fully declared CPU
fixtures. They do not establish resource headroom or performance on hardware.
"""
from dataclasses import replace
import unittest

from src.scheduling.submission_control.request_scan import AscendingRequestController
from src.scheduling.submission_control.resource_step import NativeKVFrame, RequestKVWork, propose_kv_window
from src.scheduling.submission_control.stage_work import (
    RequestSupplyFeedbackController, default_request_feedback_options,
)
from tests.scheduling.test_ascending_request_scan import ScanTrace


class AscendingScanReviewTests(unittest.TestCase):
    def test_fresh_resource_growth_proposal_reopens_held_platform(self):
        trace = ScanTrace(maximum=8)
        before = trace.run_until(lambda report, _: report['stop_reason'] == 'local_plateau',
                                 rates=lambda _: 40)
        for index in range(40):
            now, window = trace.now+1, trace.controller.current_window
            proposal = propose_kv_window(
                NativeKVFrame('CPU-resource-change', 7, index, now, now, 1000, 16, .1,
                    clock_domain='driver.monotonic', cost_model='single_pool_token_blocks'),
                now_s=now, max_age_s=1, expected_engine_uid='CPU-resource-change',
                expected_session_id=7, current_window=window, hard_limit=8,
                committed=tuple(RequestKVWork(('active', i), 8, 8) for i in range(window)),
                ready=tuple(RequestKVWork(('ready', i), 8, 8) for i in range(5)))
            self.assertIsNotNone(proposal.target_window)
            trace.tick(40, resource_step=proposal)
        self.assertGreater(trace.controller.report()['changes'], before['changes'],
            'a newly costed opportunity must wake a held selection even with steady work and delivery rate')

    def test_observed_cross_query_performance_shift_reopens_prior_platform(self):
        prior = ScanTrace(maximum=8)
        prior.run_until(lambda report, _: report['stop_reason'] == 'local_plateau', rates=lambda _: 40)
        current = ScanTrace(maximum=8, hint=prior.controller.export_hint())
        for _ in range(100):
            current.tick(100*current.controller.current_window)
        self.assertGreater(current.controller.report()['changes'], 0,
            'fresh C1 rate changed from 40 to 100 for identical work; descriptors alone cannot validate the old plateau')

    def test_native_KV_pressure_at_minimum_invalidates_old_platform(self):
        trace = ScanTrace(maximum=8)
        before = trace.run_until(lambda report, _: report['stop_reason'] == 'local_plateau',
                                 rates=lambda _: 40)
        trace.tick(40, kv_usage=.99)
        trace.tick(40, kv_usage=.99)
        during = trace.controller.report()
        for _ in range(100):
            trace.tick(100*trace.controller.current_window)
        self.assertNotEqual(during['stop_reason'], 'local_plateau',
            'native pressure is still an invalidation event when the quota cannot shrink below one')
        self.assertFalse(during['comparable_selection'])
        self.assertGreater(trace.controller.report()['changes'], before['changes'],
            'subsequent fresh low-pressure work must permit renewed exploration')

    def test_costed_resource_retry_respects_attempt_limit_and_hint_roundtrip(self):
        options = replace(default_request_feedback_options(), max_candidate_attempts=1)
        trace = ScanTrace(maximum=4)
        trace.controller = AscendingRequestController(4, options, clock=lambda: trace.now)
        trace.controller.reset(1, 100000)
        conflict_seen = False
        for _ in range(70):
            controller, window, now = trace.controller, trace.controller.current_window, trace.now+1
            count = 20 if window == 1 and (controller._confirming or conflict_seen) else 40
            proposal = propose_kv_window(
                NativeKVFrame('CPU-reviewed-engine', 7, int(now), now, now, 1000, 16, .1,
                    clock_domain='driver.monotonic', cost_model='single_pool_token_blocks'),
                now_s=now, max_age_s=1, expected_engine_uid='CPU-reviewed-engine',
                expected_session_id=7, current_window=window, hard_limit=4,
                committed=tuple(RequestKVWork(i, 8, 8) for i in range(window)),
                ready=(RequestKVWork(window, 8, 8),))
            trace.tick(count, resource_step=proposal)
            if any(event['kind'] == 'reference_order_conflict' for event in controller.evaluations):
                conflict_seen = True
            if controller._search.points[2]['attempts'] > 1 and controller.report()['recommended_window'] >= 2:
                break
        self.assertTrue(conflict_seen, 'the original work trace must actually reach an order conflict')
        self.assertLessEqual(controller._search.points[2]['attempts'], 1,
            'a costed resource target must obey the same explicit candidate-attempt limit as geometric proposals')
        hint = controller.export_hint()
        self.assertIsNotNone(hint)
        following = AscendingRequestController(4, options, clock=lambda: 0.)
        following.reset(hint['selected_window'], 100000, search_hint=hint)

    def test_explicit_remaining_override_keeps_its_opt_in_amortization_meaning(self):
        decisions = {}
        for kind in (RequestSupplyFeedbackController, AscendingRequestController):
            trace = ScanTrace(maximum=4)
            options = replace(default_request_feedback_options(), min_remaining=99,
                search_policy='reference' if kind is RequestSupplyFeedbackController else 'ascending')
            trace.controller = kind(4, options, clock=lambda: trace.now)
            trace.controller.reset(1, 200)
            if kind is RequestSupplyFeedbackController:
                update = trace.controller.update_supply
                trace.controller.update_supply = lambda observation, supply, **_: update(observation, supply)
            for index in range(5):
                decision = trace.tick(20, pending_items=200-(index+1)*20)
            decisions[kind] = decision
        self.assertEqual(decisions[RequestSupplyFeedbackController].reason, 'insufficient_remaining_benefit')
        self.assertEqual(decisions[AscendingRequestController].reason,
                         decisions[RequestSupplyFeedbackController].reason,
            'retain the explicit min_remaining contract; do not restore that gate for ordinary defaults')
        self.assertEqual(decisions[AscendingRequestController].window, 1)


if __name__ == '__main__':
    unittest.main()
