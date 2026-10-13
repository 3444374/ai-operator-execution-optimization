"""Single-step replica decisions with deterministic observations; no network/model."""

from dataclasses import replace
import unittest

from src.execution_provider.adapters.replica_feedback import ReplicaCompletionFeedback
from src.execution_provider.adapters.replica_metrics import service_pressure
from src.scheduling.core.errors import EndpointCapacityUnavailable
from src.scheduling.core.models import BatchRequest, EndpointSnapshot, TopologySnapshot
from src.scheduling.endpoint_routing.replica_adaptation import (
    ReplicaAdaptationConfig, ReplicaAdaptivePolicy, ReplicaCapacity, ReplicaFeedback,
)


def request(work=1):
    return BatchRequest("1:1", "job", "ai_complete", 1, 0, 0, "", 0, 0, "1:1", work_units=work)


def topology(a=0, b=0, a_work=None, b_work=None):
    return TopologySnapshot(tuple(EndpointSnapshot(k, "http://localhost/" + k, "model", "", True,
        count, 0, None, 0, count if work is None else work)
        for k, count, work in (("a", a, a_work), ("b", b, b_work))), 0)


class ReplicaAdaptationTests(unittest.TestCase):
    def setUp(self):
        self.now = 1.0
        self.samples = dict(a=ReplicaFeedback(), b=ReplicaFeedback())

    def policy(self, *, drain=None, maximum=6):
        return ReplicaAdaptivePolicy((ReplicaCapacity("a", 3, 12), ReplicaCapacity("b", 3, 12)),
            maximum, config=ReplicaAdaptationConfig(target_drain_s=drain),
            feedback=self.samples.__getitem__, clock=lambda: self.now)

    def test_one_step_supply_applies_each_service_sample_once_and_restores_after_stale(self):
        self.samples["a"] = ReplicaFeedback(pressure_observed_at_s=1, waiting=2, kv_usage=0.2)
        policy = self.policy()
        self.assertEqual(policy.decide(5).limit, 5)
        for _ in range(20):
            policy.route(request(), topology(), "model")
        self.assertEqual(policy.supply(), dict(a=2, b=3))
        self.now = 2
        self.samples["a"] = replace(self.samples["a"], pressure_observed_at_s=2)
        self.assertEqual(policy.decide(4).limit, 4)
        self.assertFalse(policy.decide(4).allowed)
        self.now = 5
        self.assertEqual(policy.decide(4).limit, 6)

    def test_queue_and_kv_pressure_restore_one_step_without_exceeding_global_limit(self):
        policy = self.policy(maximum=4)
        for t in (1, 2):
            self.now = t
            self.samples = {k: ReplicaFeedback(pressure_observed_at_s=t, waiting=0, kv_usage=0.95)
                            for k in ("a", "b")}
            policy.feedback = self.samples.__getitem__
            policy.decide(0)
        self.assertEqual(policy.limit, 2)
        self.now = 3
        self.samples.update({k: ReplicaFeedback(pressure_observed_at_s=3, waiting=0, kv_usage=0.2)
                             for k in ("a", "b")})
        self.assertEqual(policy.decide(0).limit, 4)

    def test_rates_compare_same_work_unit_and_use_predicted_drain(self):
        self.samples.update(a=ReplicaFeedback(1, 1), b=ReplicaFeedback(1, 8))
        policy = self.policy()
        self.assertEqual(policy.route(request(2), topology(0, 1), "model").endpoint_id, "b")
        self.assertEqual(policy.route(request(2), topology(0, 3), "model").endpoint_id, "a")

    def test_missing_rate_uses_work_and_probes_unmeasured_idle_replica(self):
        policy = self.policy(maximum=1)
        self.assertEqual(policy.route(request(), topology(), "model").endpoint_id, "a")
        self.samples["a"] = ReplicaFeedback(1, 100)
        self.assertEqual(policy.route(request(), topology(), "model").endpoint_id, "b")
        self.assertEqual(policy.route(request(), topology(1, 0, 1, 8), "model").endpoint_id, "a")

    def test_soft_work_target_allows_one_hard_legal_task_on_idle_replica(self):
        self.samples.update(a=ReplicaFeedback(1, 1), b=ReplicaFeedback(1, 1))
        policy = self.policy(drain=1)
        with self.assertRaises(EndpointCapacityUnavailable):
            policy.route(request(2), topology(1, 1), "model")
        self.assertEqual(policy.route(request(10), topology(0, 1), "model").endpoint_id, "a")
        with self.assertRaises(ValueError):
            policy.route(request(13), topology(), "model")

    def test_unknown_or_cancelled_work_still_blocks_endpoint_from_core_snapshot(self):
        policy = self.policy()
        self.assertEqual(policy.route(request(), topology(3, 0), "model").endpoint_id, "b")
        with self.assertRaises(EndpointCapacityUnavailable):
            policy.route(request(), topology(3, 3), "model")

    def test_invalid_feedback_is_rejected(self):
        for kw in (dict(waiting=-1), dict(waiting=True), dict(kv_usage=float("nan")),
                   dict(rate_observed_at_s=1, service_rate_work_units_s=0)):
            with self.assertRaises(ValueError):
                ReplicaFeedback(**kw)
        for kw in (dict(target_drain_s=0), dict(sample_interval_s=0), dict(rate_alpha=2)):
            with self.assertRaises(ValueError):
                ReplicaAdaptationConfig(**kw)

    def test_completion_rate_divides_by_busy_union_and_excludes_idle_and_unknown_work(self):
        feedback = ReplicaCompletionFeedback(("a",), ReplicaAdaptationConfig(sample_interval_s=10,
            rate_alpha=1), clock=lambda: self.now)
        feedback.started("a")
        feedback.started("a")
        self.now = 2
        feedback.finished("a", 3, successful=True)
        self.now = 3
        feedback.finished("a", 5, successful=True)
        self.assertEqual(feedback.latest("a").service_rate_work_units_s, 4)
        self.now = 100
        feedback.started("a")
        self.now = 101
        feedback.finished("a", 99, successful=False)
        self.assertEqual(feedback.latest("a").rate_observed_at_s, 3)

    def test_pressure_parser_requires_complete_counts_and_does_not_mix_token_rates(self):
        metrics = {"vllm:num_requests_running": 4, "vllm:num_requests_waiting": 2,
                   "vllm:kv_cache_usage_perc": 0.2, "vllm:generation_tokens_total": 9999}
        sample = service_pressure(metrics)
        self.assertEqual((sample.running, sample.waiting), (4, 2))
        self.assertIsNone(sample.completed_tokens_total)
        self.assertIsNone(service_pressure({"vllm:num_requests_running": 4}))
        self.assertIsNone(service_pressure(dict(metrics, **{"vllm:num_requests_running": 1.5})))


if __name__ == "__main__":
    unittest.main()
