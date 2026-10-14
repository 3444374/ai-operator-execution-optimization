"""Destination policy checks with declared logical capacities and local clocks."""

import unittest
from dataclasses import replace

from src.modalities.image.hse_dispatch import image_model_routing_request
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.runtime.stage_dispatch import (
    StageActiveWork, StageActorCapacity, StageCapacity,
    StageDestination, StageExecutionGroupCapacity, StageModelDispatcher,
)
from src.scheduling.runtime.stage_broker import BoundedStageBroker, StageBrokerLimits
from tests.scheduling.test_stage_broker import _descriptor


def dispatcher(*, count=2, actor_work=8, shared=False, group_requests=None,
               group_work=None, observed=False, estimator=None):
    actors = tuple(StageActorCapacity(
        index, StageDestination(f"actor-{index}", "shared" if shared else f"group-{index}",
                                f"runtime-{index}" if observed else None,
                                "node" if observed else None,
                                (str(index),) if observed else ()),
        StageCapacity(1, actor_work),
    ) for index in range(count))
    groups = ((StageExecutionGroupCapacity(
        "shared", StageCapacity(group_requests if group_requests is not None else count,
                                group_work if group_work is not None else count * actor_work)),)
              if shared else tuple(StageExecutionGroupCapacity(
                  f"group-{index}", StageCapacity(1, actor_work)) for index in range(count)))
    return StageModelDispatcher(actors, groups, feedback_ttl_s=5,
                                service_rate_ttl_s=10,
                                placement_estimator=estimator)


def choose(policy, *, descriptor=None, available=None, active=(), now=2):
    descriptor = _descriptor(0) if descriptor is None else descriptor
    return policy.choose(descriptor, image_model_routing_request(descriptor),
                         available_actor_indices=({item.actor_index for item in policy.actors}
                                                  if available is None else available),
                         active=active, now_s=now)


def learn(policy, *, seconds=(4, 1), descriptor=None, now=1):
    for index, service_s in enumerate(seconds):
        policy.observe_completion(index, _descriptor(0) if descriptor is None else descriptor,
                                  service_s=service_s, now_s=now)


class Estimates:
    def __init__(self, result=None, error=None):
        self.calls, self.result, self.error = [], result, error

    def __call__(self, descriptor, destination, now_s):
        self.calls.append((descriptor, destination, now_s))
        if self.error:
            raise self.error
        return self.result


class StageModelDispatchTest(unittest.TestCase):
    def test_broker_peek_shares_fifo_without_leasing_charging_or_reordering(self):
        broker = BoundedStageBroker(StageBrokerLimits(20, 80, 8, 2, 1))
        descriptors = (_descriptor(0), _descriptor(1, job_id="job-b"))
        for descriptor in descriptors:
            broker.enqueue_encoded(descriptor)
            prepare = broker.lease_prepare(now_s=2)
            broker.complete_prepare(prepare.lease_id, replace(descriptor, representation="ready"), now_s=3)
        before = broker.snapshot()
        self.assertEqual(broker.peek_model().block_id, "block-0")
        self.assertEqual(broker.peek_model(preferred_job_id="job-b").block_id, "block-1")
        self.assertIsNone(broker.peek_model(preferred_job_id="absent"))
        self.assertEqual(broker.snapshot(), before)
        model = broker.lease_model(now_s=4, block_id=broker.peek_model().block_id)
        self.assertIsNone(broker.peek_model())
        broker.complete_model(model.lease_id, output_row_ids=descriptors[0].row_ids)
        self.assertEqual(broker.peek_model().block_id, "block-1")

    def test_confirmed_heterogeneous_rates_choose_predicted_finish(self):
        policy = dispatcher()
        learn(policy)
        self.assertEqual(choose(policy).actor_index, 1)
        self.assertEqual(choose(policy).reason, "least_work")
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [0, 1])

    def test_missing_rates_sample_each_actor_without_mixed_units(self):
        policy = dispatcher()
        self.assertEqual(choose(policy).actor_index, 0)
        policy.observe_completion(0, _descriptor(0), service_s=0.04, now_s=1)
        self.assertEqual(choose(policy).actor_index, 1)
        self.assertEqual(choose(policy).reason, "sample_rotation")
        policy.observe_completion(1, _descriptor(0), service_s=0.02, now_s=2)
        self.assertEqual(choose(policy, now=3).actor_index, 1)

    def test_zero_nan_and_overflow_samples_rotate_without_refreshing_rate(self):
        policy = dispatcher()
        for service_s in (0, float("nan"), float("inf"), True, -1, 1e-320):
            with self.subTest(service_s=service_s):
                policy.reset_service_feedback()
                self.assertFalse(policy.observe_completion(0, _descriptor(0),
                    service_s=service_s, now_s=1))
                self.assertEqual(choose(policy).actor_index, 1)
                self.assertFalse(policy.observe_completion(1, _descriptor(0),
                    service_s=0, now_s=2))
                self.assertEqual(choose(policy, now=3).actor_index, 0)
                self.assertEqual(policy.snapshot()["sampled_actor_indices"], [])

    def test_invalid_sample_actor_does_not_starve_actor_with_valid_rate(self):
        policy = dispatcher()
        learn(policy, seconds=(0, 1))
        self.assertEqual(choose(policy).actor_index, 0)
        policy.observe_completion(0, _descriptor(0), service_s=0, now_s=3)
        self.assertEqual(choose(policy, now=4).actor_index, 1)

    def test_rate_expiry_unit_calibration_and_model_mismatch_fall_back(self):
        for change in (
            {"now": 11},
            {"descriptor": replace(_descriptor(0), model_signature="other")},
            {"descriptor": replace(_descriptor(0), work=WorkDescriptor(
                (StageWork("model", 4, "frames"),), "model", "sig"))},
            {"descriptor": replace(_descriptor(0), work=WorkDescriptor(
                (StageWork("model", 4, "pixels"),), "model", "new-calibration"))},
        ):
            with self.subTest(change=change):
                policy = dispatcher()
                learn(policy)
                self.assertEqual(choose(policy, **change).reason, "sample_rotation")
        policy = dispatcher()
        learn(policy, now=5)
        self.assertEqual(choose(policy, now=4).reason, "sample_rotation")

    def test_only_current_capacity_eligible_rates_are_required(self):
        policy = dispatcher()
        policy.observe_completion(1, _descriptor(0), service_s=1, now_s=1)
        self.assertEqual(choose(policy, available={1}).reason, "least_work")
        policy.observe_actor_capacity(0, StageCapacity(0, 0), observed_at_s=1)
        self.assertEqual(choose(policy).actor_index, 1)
        self.assertEqual(choose(policy).reason, "least_work")

    def test_group_occupancy_includes_unknown_calls_and_is_never_settled_here(self):
        policy = dispatcher(shared=True, group_requests=1)
        unknown = (StageActiveWork(0, 4),)
        before = policy.snapshot()
        blocked = choose(policy, available={1}, active=unknown)
        self.assertIsNone(blocked.actor_index)
        self.assertIsNone(blocked.retry_at_s)
        self.assertEqual(choose(policy, available={1}, active=unknown).actor_index, None)
        self.assertEqual(choose(policy, available={1}, active=()).actor_index, 1)
        self.assertEqual(before["actors"], policy.snapshot()["actors"])
        self.assertEqual(before["groups"], policy.snapshot()["groups"])

    def test_actor_and_group_work_are_counted_separately_from_ready_work(self):
        policy = dispatcher(shared=True, group_work=6)
        self.assertIsNone(choose(policy, active=(StageActiveWork(0, 4),)).actor_index)
        self.assertEqual(choose(policy, active=()).actor_index, 0)

    def test_capacity_downshift_retains_active_usage_then_expires_exactly(self):
        policy = dispatcher(shared=True)
        active = (StageActiveWork(0, 4),)
        self.assertTrue(policy.observe_group_capacity("shared", StageCapacity(1, 2), observed_at_s=1))
        blocked = choose(policy, available={1}, active=active)
        self.assertIsNone(blocked.actor_index)
        self.assertEqual(blocked.retry_at_s, 6)
        recovered = choose(policy, available={1}, active=active, now=6)
        self.assertEqual(recovered.actor_index, 1)
        self.assertIsNone(recovered.retry_at_s)

    def test_bad_future_or_expired_capacity_feedback_uses_declared_limits(self):
        policy = dispatcher()
        self.assertFalse(policy.observe_actor_capacity(0, StageCapacity(2, 8), observed_at_s=1))
        self.assertFalse(policy.observe_actor_capacity(0, StageCapacity(1, 9), observed_at_s=1))
        self.assertFalse(policy.observe_actor_capacity(0, StageCapacity(0, 0), observed_at_s=float("nan")))
        self.assertTrue(policy.observe_actor_capacity(0, StageCapacity(0, 0), observed_at_s=5))
        self.assertFalse(policy.observe_actor_capacity(0, StageCapacity(0, 0), observed_at_s=4))
        self.assertEqual(choose(policy, now=4).actor_index, 0)
        self.assertEqual(choose(policy, now=5).actor_index, 1)
        self.assertEqual(choose(policy, now=10).actor_index, 0)

    def test_permanently_oversized_head_fails_without_bypassing_to_small_work(self):
        policy = dispatcher(actor_work=3)
        with self.assertRaisesRegex(BufferError, "every declared"):
            choose(policy)
        policy = dispatcher(shared=True, group_work=3)
        with self.assertRaises(BufferError):
            choose(policy)

    def test_large_head_waits_for_busy_large_actor_with_small_actor_free(self):
        policy = dispatcher(actor_work=8)
        policy = StageModelDispatcher((policy.actors[0], replace(policy.actors[1], capacity=StageCapacity(1, 4))),
            (policy.groups[0], replace(policy.groups[1], capacity=StageCapacity(1, 4))))
        large = replace(_descriptor(0), work=WorkDescriptor(
            (StageWork("model", 8, "pixels"),), "model", "sig"))
        decision = choose(policy, descriptor=large, available={1}, active=(StageActiveWork(0, 8),))
        self.assertIsNone(decision.actor_index)
        self.assertIsNone(decision.retry_at_s)
        self.assertEqual(choose(policy, descriptor=large, available={0}).actor_index, 0)

    def test_reuses_data_owner_callback_signature_with_unknown_result(self):
        estimates = Estimates()
        policy = dispatcher(estimator=estimates)
        learn(policy)
        self.assertEqual(choose(policy).actor_index, 1)
        self.assertEqual(len(estimates.calls), 2)
        self.assertEqual(estimates.calls[0], (_descriptor(0), policy.actors[0].destination, 2))
        self.assertEqual(policy.snapshot()["locality_observations"], {"unknown": 2})
        self.assertIsNone(estimates.calls[0][1].runtime_actor_id)

    def test_unsupported_and_faulty_advisory_results_never_change_choice_or_capacity(self):
        for estimates in (Estimates(result=100), Estimates(result=float("nan")),
                          Estimates(error=RuntimeError("advisory lookup failed"))):
            with self.subTest(estimates=estimates):
                policy = dispatcher(actor_work=4, estimator=estimates)
                learn(policy)
                self.assertEqual(choose(policy).actor_index, 1)
                self.assertEqual(choose(policy).reason, "least_work")
                self.assertEqual(policy.actors[1].capacity.max_active_work, 4)

    def test_no_locality_callback_is_needed_for_sampling(self):
        policy = dispatcher()
        self.assertEqual(choose(policy).reason, "sample_rotation")
        self.assertEqual(policy.snapshot()["locality_observations"], {})

    def test_numeric_actor_indices_preserve_static_rotation_for_ten_or_more(self):
        policy = dispatcher(count=12)
        self.assertEqual(choose(policy, available={10, 2}).actor_index, 2)

    def test_invalid_identity_capacity_and_duplicate_declarations_are_rejected(self):
        with self.assertRaises(ValueError):
            StageDestination("", "group")
        with self.assertRaises(ValueError):
            StageDestination("endpoint", "group", accelerator_ids=("0", "0"))
        for capacity in ((True, 4), (1, -1), (1, 1.5)):
            with self.assertRaises(ValueError):
                StageCapacity(*capacity)
        original = dispatcher()
        with self.assertRaises(ValueError):
            StageModelDispatcher((original.actors[0], original.actors[0]), original.groups)
        with self.assertRaises(ValueError):
            StageModelDispatcher(original.actors, original.groups, feedback_ttl_s=float("inf"))
        with self.assertRaises(ValueError):
            dispatcher(shared=True, group_requests=3)


if __name__ == "__main__":
    unittest.main()
