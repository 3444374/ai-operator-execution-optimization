"""Rate support, clock windows and pressure cadence; deterministic, zero HTTP."""

import unittest
from dataclasses import replace

from src.execution_provider.adapters.replica_feedback import ReplicaCompletionFeedback
from src.scheduling.endpoint_routing.replica_adaptation import (
    ReplicaAdaptationConfig, ReplicaAdaptivePolicy, ReplicaCapacity, ReplicaFeedback,
)


class FeedbackSupportTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.0

    def feedback(self, alpha=0.3, **kw):
        return ReplicaCompletionFeedback(("a",), ReplicaAdaptationConfig(rate_alpha=alpha, **kw),
            clock=lambda: self.now)

    def finish(self, feedback, at, work=1, successful=True):
        self.now = at
        feedback.finished("a", work, successful=successful)

    def test_nearly_simultaneous_finishes_use_whole_supported_window(self):
        for alpha in (0.3, 1):
            with self.subTest(alpha=alpha):
                self.now = 0
                feedback = self.feedback(alpha)
                feedback.started("a")
                feedback.started("a")
                self.finish(feedback, 0.250)
                self.assertIsNone(feedback.latest("a").service_rate_work_units_s)
                self.finish(feedback, 0.251)
                sample = feedback.latest("a")
                self.assertAlmostEqual(sample.service_rate_work_units_s, 2 / .251)
                self.assertEqual((sample.rate_completed_requests, sample.rate_completed_work), (2, 2))
                self.assertAlmostEqual(sample.rate_support_s, .251)
                self.assertAlmostEqual(sample.rate_mean_active_requests, .501 / .251)
                self.assertEqual(feedback.diagnostics("a")["successful_work"], 2)

    def test_short_tail_replaces_last_window_contribution_for_all_alphas(self):
        for alpha in (.3, 1):
            with self.subTest(alpha=alpha):
                self.now = 0
                feedback = self.feedback(alpha)
                for _ in range(3):
                    feedback.started("a")
                self.finish(feedback, .250)
                self.finish(feedback, .250)
                self.assertAlmostEqual(feedback.latest("a").service_rate_work_units_s, 8)
                self.finish(feedback, .251)
                self.assertAlmostEqual(feedback.latest("a").service_rate_work_units_s, 3 / .251)
                diagnostics = feedback.diagnostics("a")
                self.assertEqual(diagnostics["tail_corrections"], 1)
                self.assertEqual(diagnostics["successful_work"], 3)
                self.assertAlmostEqual(diagnostics["last_segment_rate"], 3 / .251)

    def test_zero_time_tail_merges_work_without_dividing_by_zero(self):
        feedback = self.feedback(1)
        for _ in range(3):
            feedback.started("a")
        for _ in range(3):
            self.finish(feedback, .25)
        self.assertEqual(feedback.latest("a").service_rate_work_units_s, 12)
        self.assertEqual(feedback.latest("a").rate_completed_work, 3)
        self.now = 1
        cold = self.feedback()
        cold.started("a")
        cold.finished("a", 1, successful=True)
        self.assertIsNone(cold.latest("a").service_rate_work_units_s)
        self.assertEqual(cold.diagnostics("a")["successful_work"], 1)

    def test_prior_requires_the_same_support_as_rate_routing(self):
        for prior in (ReplicaFeedback(1, 1), ReplicaFeedback(1, 1,
                rate_support_s=.5, rate_completed_requests=2)):
            with self.assertRaises(ValueError):
                ReplicaCompletionFeedback(("a",), ReplicaAdaptationConfig(), prior={"a": prior})

    def test_single_completion_windows_accumulate_before_qualification(self):
        feedback = self.feedback()
        feedback.started("a")
        feedback.started("a")
        self.finish(feedback, .3)
        self.assertIsNone(feedback.latest("a").rate_observed_at_s)
        self.finish(feedback, .6)
        self.assertAlmostEqual(feedback.latest("a").service_rate_work_units_s, 2 / .6)

    def test_serial_k1_segments_update_across_queries_and_exclude_idle(self):
        feedback = self.feedback(1)
        for i in range(8):
            feedback.started("a")
            self.finish(feedback, self.now + .3)
            if i % 2 == 1:
                self.assertAlmostEqual(feedback.latest("a").service_rate_work_units_s, 1 / .3)
                self.assertEqual(feedback.latest("a").rate_completed_requests, 2)
        self.assertEqual(feedback.diagnostics("a")["rate_updates"], 4)
        self.now += 10
        feedback.started("a")
        self.finish(feedback, self.now + .001)
        old_at = feedback.latest("a").rate_observed_at_s
        self.now += 10
        feedback.started("a")
        self.finish(feedback, self.now + .001)
        self.assertEqual(feedback.latest("a").rate_observed_at_s, old_at)

    def test_serial_failed_segment_remains_in_unpublished_busy_support(self):
        feedback = self.feedback(1)
        for at, successful in ((.3, True), (.6, False), (.9, True)):
            feedback.started("a")
            self.finish(feedback, at, successful=successful)
        self.assertAlmostEqual(feedback.latest("a").service_rate_work_units_s, 2 / .9)
        self.assertAlmostEqual(feedback.latest("a").rate_support_s, .9)

    def test_higher_support_floor_retains_unpublished_work_across_windows(self):
        feedback = self.feedback(1, sample_interval_s=.125, min_rate_support_s=.75)
        feedback.started("a")
        self.finish(feedback, .25)
        for _ in range(4):
            feedback.started("a")
        for at in (.375, .375, 1, 1):
            self.finish(feedback, at)
        self.assertEqual(feedback.latest("a").rate_completed_work, 5)
        self.assertEqual(feedback.latest("a").rate_support_s, 1)
        self.assertEqual(feedback.latest("a").service_rate_work_units_s, 5)

    def test_higher_support_floor_coalesces_last_tail_and_multiple_short_segments(self):
        feedback = self.feedback(1, sample_interval_s=.125, min_rate_support_s=.75)
        for _ in range(4):
            feedback.started("a")
        for at in (.75, .75, .875, .875):
            self.finish(feedback, at)
        self.assertAlmostEqual(feedback.latest("a").service_rate_work_units_s, 4 / .875)
        self.assertEqual(feedback.diagnostics("a")["tail_corrections"], 1)
        self.now = 0
        serial = self.feedback(1, sample_interval_s=.125, min_rate_support_s=.75)
        for _ in range(6):
            serial.started("a")
            serial.started("a")
            self.finish(serial, self.now + .125)
            self.finish(serial, self.now)
        self.assertEqual(serial.latest("a").rate_completed_work, 12)
        self.assertEqual(serial.latest("a").rate_support_s, .75)
        self.assertEqual(serial.latest("a").service_rate_work_units_s, 16)

    def test_real_short_cold_episode_is_visible_but_not_qualified(self):
        feedback = self.feedback()
        feedback.started("a")
        self.finish(feedback, .001)
        sample = feedback.latest("a")
        policy = ReplicaAdaptivePolicy((ReplicaCapacity("a", 1, 1),), 1,
            config=ReplicaAdaptationConfig(), feedback=lambda _: sample, clock=lambda: self.now)
        self.assertEqual(sample.service_rate_work_units_s, 1000)
        self.assertFalse(policy._usable_rate(sample, self.now))
        self.assertEqual(feedback.diagnostics("a")["tail_corrections"], 0)

    def test_short_episode_holds_qualified_prior_without_refreshing_age(self):
        feedback = self.feedback()
        feedback.started("a")
        feedback.started("a")
        self.finish(feedback, .25)
        self.finish(feedback, .5)
        prior = feedback.latest("a")
        self.now = 10
        feedback.started("a")
        self.finish(feedback, 10.001)
        self.assertEqual(feedback.latest("a"), prior)
        self.assertEqual(feedback.diagnostics("a")["low_support_holds"], 1)
        self.assertEqual(feedback.diagnostics("a")["successful_work"], 3)
        self.assertAlmostEqual(feedback.diagnostics("a")["busy_s"], .501)

    def test_online_windows_and_completed_segment_are_separate(self):
        feedback = self.feedback(1)
        for _ in range(4):
            feedback.started("a")
        for at in (.25, .25, .75, .75):
            self.finish(feedback, at)
        self.assertAlmostEqual(feedback.latest("a").service_rate_work_units_s, 4)
        self.assertAlmostEqual(feedback.diagnostics("a")["last_segment_rate"], 4 / .75)
        self.assertEqual(feedback.diagnostics("a")["successful_requests"], 4)

    def test_failed_tail_adds_busy_time_without_successful_work_or_core_settlement(self):
        feedback = self.feedback(1)
        for _ in range(3):
            feedback.started("a")
        self.finish(feedback, .25)
        self.finish(feedback, .25)
        self.finish(feedback, .5, 99, False)
        self.assertEqual(feedback.latest("a").service_rate_work_units_s, 4)
        self.assertEqual(feedback.diagnostics("a")["successful_work"], 2)
        with self.assertRaises(ValueError):
            feedback.finished("a", 1, successful=True)

    def test_supply_is_opt_in_and_respects_real_observation_cadence(self):
        samples = [ReplicaFeedback(pressure_observed_at_s=0, waiting=1, kv_usage=.1)]
        capacity = (ReplicaCapacity("a", 4, 4),)
        policy = ReplicaAdaptivePolicy(capacity, 4, config=ReplicaAdaptationConfig(),
            feedback=lambda _: samples[0], clock=lambda: self.now)
        self.assertEqual(policy.decide(0).limit, 4)
        policy = ReplicaAdaptivePolicy(capacity, 4, config=ReplicaAdaptationConfig(adapt_supply=True),
            feedback=lambda _: samples[0], clock=lambda: self.now)
        self.assertEqual(policy.decide(0).limit, 3)
        for _ in range(20):
            self.assertEqual(policy.decide(0).limit, 3)
        self.now = .1
        samples[0] = replace(samples[0], pressure_observed_at_s=.1)
        self.assertEqual(policy.decide(0).limit, 3)
        self.assertEqual(policy.diagnostics()["a"]["state"], "cadence_hold")
        self.now = .25
        samples[0] = replace(samples[0], pressure_observed_at_s=.25)
        self.assertEqual(policy.decide(0).limit, 2)
        self.now = .5
        samples[0] = replace(samples[0], pressure_observed_at_s=.5, waiting=0)
        self.assertEqual(policy.decide(0).limit, 3)
        self.now = 3
        self.assertEqual(policy.decide(0).limit, 4)
        samples[0] = ReplicaFeedback()
        policy.decide(0)
        counts = policy.diagnostics()["a"]
        self.assertEqual((counts["down"], counts["recovery"], counts["fallback"]), (2, 1, 2))


if __name__ == "__main__":
    unittest.main()
