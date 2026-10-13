"""Candidate-local completion-rate decisions; no HTTP, model, or Core mutations."""

from dataclasses import replace
import unittest

from src.scheduling.core.errors import EndpointCapacityUnavailable
from src.scheduling.core.models import BatchRequest, EndpointSnapshot, TopologySnapshot
from src.scheduling.endpoint_routing.replica_adaptation import (
    ReplicaAdaptationConfig, ReplicaAdaptivePolicy, ReplicaCapacity, ReplicaFeedback,
)


def request(work=2):
    return BatchRequest("original", "job", "ai_complete", 1, 0, 0, "", 0, 0,
                        "original", work_units=work, work_unit="request_count")


def endpoint(endpoint_id, running=0, work=0, **changes):
    return replace(EndpointSnapshot(endpoint_id, "http://localhost/" + endpoint_id,
                                   "model", "", True, running, 0, None, 1,
                                   estimated_active_work=work), **changes)


def qualified_rate(rate, *, observed=1.0):
    return ReplicaFeedback(
        rate_observed_at_s=observed, service_rate_work_units_s=rate,
        rate_support_s=0.25, rate_completed_requests=2, rate_completed_work=2,
        rate_mean_active_requests=2, rate_peak_active_requests=2,
        rate_sample_started_at_s=max(0.0, observed - 0.25),
        rate_provenance="calibration",
    )


class ReplicaCandidateRateTests(unittest.TestCase):
    def setUp(self):
        self.now = 1.0
        self.samples = {"a": qualified_rate(1), "b": qualified_rate(8),
                        "c": ReplicaFeedback()}
        self.capacities = tuple(ReplicaCapacity(k, 4, 12) for k in ("a", "b", "c"))

    def policy(self, *, drain=None):
        return ReplicaAdaptivePolicy(self.capacities, 12,
            config=ReplicaAdaptationConfig(target_drain_s=drain),
            feedback=self.samples.__getitem__, clock=lambda: self.now)

    def route(self, *endpoints, policy=None, work=2):
        return (policy or self.policy()).route(
            request(work), TopologySnapshot(tuple(endpoints), self.now), "model")

    def test_ineligible_third_replica_does_not_disable_known_rate_comparison(self):
        variants = (
            endpoint("c", healthy=False), endpoint("c", available=False),
            endpoint("c", pool_id="other"), endpoint("c", running=4, work=4),
            endpoint("c", running=1, work=11),
        )
        for unavailable in variants:
            with self.subTest(endpoint=unavailable):
                decision = self.route(endpoint("a"), endpoint("b", running=1, work=1), unavailable)
                self.assertEqual(decision.endpoint_id, "b")
                self.assertEqual(decision.reason, "replica_adaptive_least_work")

    def test_legal_missing_or_stale_occupied_rate_preserves_known_comparison(self):
        for sample in (ReplicaFeedback(), qualified_rate(100, observed=0.5)):
            with self.subTest(sample=sample):
                self.now = 3 if sample.rate_observed_at_s is not None else 1
                self.samples.update(a=qualified_rate(1, observed=self.now),
                                    b=qualified_rate(8, observed=self.now), c=sample)
                decision = self.route(endpoint("a"), endpoint("b", running=1, work=1),
                                      endpoint("c", running=1, work=0))
                self.assertEqual(decision.endpoint_id, "b")
                self.assertEqual(decision.reason, "replica_adaptive_partial_rates")

    def test_legal_idle_unknown_uses_one_original_task_for_probe(self):
        original = request()
        for sample in (ReplicaFeedback(), qualified_rate(100, observed=0.5),
                       replace(qualified_rate(100), rate_completed_requests=1),
                       ReplicaFeedback(rate_observed_at_s=1, service_rate_work_units_s=100)):
            with self.subTest(sample=sample):
                self.now = 3 if sample.rate_observed_at_s == 0.5 else 1
                self.samples.update(a=qualified_rate(1, observed=self.now),
                                    b=qualified_rate(8, observed=self.now), c=sample)
                topology = TopologySnapshot(tuple(endpoint(k) for k in ("a", "b", "c")), self.now)
                decision = self.policy().route(original, topology, "model")
                self.assertEqual(decision.endpoint_id, "c")
                self.assertEqual(decision.reason, "replica_adaptive_probe_original")
                self.assertEqual(original.request_id, "original")
                self.assertEqual(tuple(e.running for e in topology.endpoints), (0, 0, 0))

    def test_idle_unknown_with_higher_estimated_work_does_not_displace_known_candidate(self):
        decision = self.route(endpoint("a"), endpoint("b", running=1, work=1),
                              endpoint("c", work=3))
        self.assertEqual(decision.endpoint_id, "b")
        self.assertEqual(decision.reason, "replica_adaptive_partial_rates")

    def test_all_known_rates_use_same_unit_including_healthy_third_replica(self):
        self.samples["c"] = qualified_rate(100)
        decision = self.route(endpoint("a"), endpoint("b", running=1, work=1),
                              endpoint("c", running=2, work=2))
        self.assertEqual(decision.endpoint_id, "c")
        self.assertEqual(decision.reason, "replica_adaptive_least_work")

    def test_full_known_replicas_leave_unknown_original_task_send_legal(self):
        decision = self.route(endpoint("a", running=4, work=4),
                              endpoint("b", running=4, work=4), endpoint("c", running=1, work=1))
        self.assertEqual(decision.endpoint_id, "c")
        self.assertEqual(decision.reason, "replica_adaptive_neutral_work")

    def test_all_unknown_candidates_use_neutral_work_without_synthetic_rate(self):
        self.samples = {k: ReplicaFeedback() for k in ("a", "b", "c")}
        decision = self.route(endpoint("a", running=1, work=6),
                              endpoint("b", running=2, work=2), endpoint("c", running=1, work=4))
        self.assertEqual(decision.endpoint_id, "b")
        self.assertEqual(decision.reason, "replica_adaptive_neutral_work")

    def test_all_unknown_idle_ties_use_original_opportunities_without_starvation(self):
        self.samples = {k: ReplicaFeedback() for k in ("a", "b", "c")}
        policy = self.policy()
        empty = tuple(endpoint(k) for k in ("a", "b", "c"))
        self.assertEqual([self.route(*empty, policy=policy).endpoint_id for _ in range(6)],
                         ["a", "b", "c", "a", "b", "c"])

    def test_soft_horizon_excludes_only_occupied_qualified_candidates(self):
        self.samples["c"] = qualified_rate(1)
        decision = self.route(endpoint("a"), endpoint("b", running=1, work=1),
                              endpoint("c", running=1, work=4), policy=self.policy(drain=1))
        self.assertEqual(decision.endpoint_id, "b")
        self.assertEqual(decision.reason, "replica_adaptive_least_work")

    def test_soft_horizon_does_not_use_unqualified_or_missing_rate(self):
        for sample in (ReplicaFeedback(),
                       ReplicaFeedback(rate_observed_at_s=1, service_rate_work_units_s=0.1)):
            with self.subTest(sample=sample):
                self.samples["c"] = sample
                decision = self.route(endpoint("a", running=4, work=4),
                                      endpoint("b", running=4, work=4),
                                      endpoint("c", running=1, work=9), policy=self.policy(drain=1))
                self.assertEqual(decision.endpoint_id, "c")
                self.assertEqual(decision.reason, "replica_adaptive_neutral_work")

    def test_soft_horizon_allows_one_hard_legal_idle_original_task(self):
        self.samples["c"] = qualified_rate(1)
        decision = self.route(endpoint("a", running=1, work=4),
                              endpoint("b", running=1, work=4), endpoint("c"),
                              policy=self.policy(drain=0.1), work=10)
        self.assertEqual(decision.endpoint_id, "c")
        self.assertEqual(decision.reason, "replica_adaptive_least_work")

    def test_no_legal_candidate_preserves_capacity_failure(self):
        with self.assertRaises(EndpointCapacityUnavailable):
            self.route(*(endpoint(k, running=4, work=4) for k in ("a", "b", "c")))

    def test_topology_identity_and_hard_request_limit_still_fail_explicitly(self):
        with self.assertRaises(ValueError):
            self.route(endpoint("a"), endpoint("b"))
        with self.assertRaises(ValueError):
            self.route(*(endpoint(k) for k in ("a", "b", "c")), work=13)


if __name__ == "__main__":
    unittest.main()
