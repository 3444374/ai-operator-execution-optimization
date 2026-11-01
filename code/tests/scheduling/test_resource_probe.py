"""Source-qualified tentative probing with pure fixtures, no hardware calls."""

from dataclasses import replace
import math
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.resource_probe import (
    ResourceProbeView, resource_probe_opportunity,
)
from src.scheduling.submission_control.stage_work import SupplyState


class ResourceProbeTests(unittest.TestCase):
    def setUp(self):
        self.observation = AdmissionObservation(20., True, 2, 2, 0, .2,
            sample_age_s=.2, preemptions_total=0, pending_items=100)
        self.supply = SupplyState(20., 50, 100, 50, 1, True, False,
                                  active_requests=2)

    def view(self, index=1, **changes):
        view = ResourceProbeView("engine-a", 7, index, 19.2+index*.2,
            19.35+index*.2, .1, ("GPU-a",), 20., 25.)
        return replace(view, **changes)

    def opportunity(self, samples=(), *, observation=None, supply=None, **changes):
        arguments = dict(current_window=2, hard_limit=37, max_age_s=1.,
                         expected_engine_uid="engine-a", expected_session_id=7)
        arguments.update(changes)
        return resource_probe_opportunity(observation or self.observation,
            supply or self.supply, physical_samples=samples, **arguments)

    def test_native_opportunity_does_not_require_a_request_cost_model(self):
        self.assertIsNone(self.observation.completed_input_work_total)
        self.assertEqual(self.supply.ready_work, ())
        result = self.opportunity()
        self.assertEqual((result.kind, result.extra_geometric_steps), ("native_headroom", 0))
        self.assertFalse(result.evidence["cost_model_required"])
        self.assertTrue(result.evidence["tentative"])
        self.assertFalse(result.evidence["full_query_benefit_proven"])

    def test_two_fresh_device_and_host_idle_signals_allow_one_extra_geometric_step(self):
        result = self.opportunity((self.view(1), self.view(2)))
        self.assertEqual((result.kind, result.extra_geometric_steps), ("idle_device_headroom", 1))
        self.assertEqual(result.evidence["physical_request_indices"], [1, 2])
        self.assertEqual(result.condition["gpu_uuids"], ["GPU-a"])
        self.assertFalse(result.evidence["capacity_estimate"])
        self.assertFalse(result.evidence["GPU_busy_is_MFU"])
        self.assertFalse(result.evidence["free_VRAM_to_request_conversion"])

    def test_one_low_gpu_value_cannot_trigger_the_extra_step(self):
        result = self.opportunity((self.view(1, gpu_busy_pct=0.),))
        self.assertEqual((result.kind, result.extra_geometric_steps), ("native_headroom", 0))
        self.assertEqual(result.reason, "two_distinct_physical_samples_required")

    def test_unknown_physical_metrics_preserve_ordinary_native_opportunity(self):
        for changes in ({"gpu_busy_pct": None}, {"host_peak_cpu_pct": None},
                        {"gpu_busy_pct": math.nan}, {"host_peak_cpu_pct": math.inf}):
            with self.subTest(changes=changes):
                result = self.opportunity((self.view(1), self.view(2, **changes)))
                self.assertEqual((result.kind, result.extra_geometric_steps), ("native_headroom", 0))
                self.assertEqual(result.reason, "physical_busy_metrics_unknown_or_invalid")

    def test_a_host_hotspot_or_busy_gpu_prevents_only_the_accelerated_trial(self):
        for changes in ({"host_peak_cpu_pct": 50.}, {"host_peak_cpu_pct": 95.},
                        {"gpu_busy_pct": 50.}, {"gpu_busy_pct": 90.}):
            with self.subTest(changes=changes):
                result = self.opportunity((self.view(1), self.view(2, **changes)))
                self.assertEqual((result.kind, result.extra_geometric_steps), ("native_headroom", 0))
                self.assertEqual(result.reason, "physical_idle_majority_not_observed")

    def test_latest_hot_sample_is_not_ignored_in_favour_of_older_idle_samples(self):
        result = self.opportunity((self.view(1), self.view(2),
                                  self.view(3, gpu_busy_pct=90.)))
        self.assertEqual(result.kind, "native_headroom")
        self.assertEqual(result.reason, "physical_idle_majority_not_observed")

    def test_duplicate_snapshot_delivery_is_not_independent_physical_evidence(self):
        result = self.opportunity((self.view(1), self.view(1)))
        self.assertEqual(result.kind, "native_headroom")
        self.assertEqual(result.reason, "two_distinct_physical_samples_required")

    def test_conflicting_same_request_index_is_not_chosen_by_its_lower_busy_value(self):
        result = self.opportunity((self.view(1), self.view(2),
                                  self.view(2, gpu_busy_pct=95.)))
        self.assertEqual(result.kind, "native_headroom")
        self.assertEqual(result.reason, "physical_request_index_conflict")

    def test_wrong_physical_source_or_session_preserves_only_native_evidence(self):
        for changes in ({"engine_uid": "engine-b"}, {"session_id": 8},
                        {"session_id": None}, {"request_index": None},
                        {"request_index": 0}, {"request_index": True}):
            with self.subTest(changes=changes):
                result = self.opportunity((self.view(1), self.view(2, **changes)))
                self.assertEqual(result.kind, "native_headroom")
                self.assertEqual(result.reason, "physical_source_identity_invalid")

    def test_physical_uuid_binding_is_required_and_must_be_stable(self):
        for devices in ((), ("0",), ("GPU-",), ("GPU-a", "GPU-a")):
            with self.subTest(devices=devices):
                result = self.opportunity((self.view(1), self.view(2, gpu_uuids=devices)))
                self.assertEqual(result.kind, "native_headroom")
                self.assertEqual(result.reason, "physical_source_identity_invalid")
        result = self.opportunity((self.view(1), self.view(2, gpu_uuids=("GPU-b",))))
        self.assertEqual(result.kind, "native_headroom")
        self.assertEqual(result.reason, "physical_device_binding_changed")

    def test_uuid_order_is_not_a_resource_condition_change(self):
        result = self.opportunity((self.view(1, gpu_uuids=("GPU-a", "GPU-b")),
                                  self.view(2, gpu_uuids=("GPU-b", "GPU-a"))))
        self.assertEqual(result.kind, "idle_device_headroom")
        self.assertEqual(result.condition["gpu_uuids"], ["GPU-a", "GPU-b"])

    def test_physical_source_duration_must_fit_the_actual_rpc_envelope(self):
        for changes in ({"source_elapsed_s": None}, {"source_elapsed_s": -.1},
                        {"source_elapsed_s": .5}, {"source_elapsed_s": math.nan},
                        {"requested_at_s": None}, {"received_at_s": None},
                        {"requested_at_s": 19.9}, {"received_at_s": 20.1}):
            with self.subTest(changes=changes):
                result = self.opportunity((self.view(1), self.view(2, **changes)))
                self.assertEqual(result.kind, "native_headroom")
                self.assertEqual(result.reason, "physical_RPC_time_bounds_invalid_or_stale")

    def test_fresh_receive_cannot_hide_an_old_possible_sampling_time(self):
        result = self.opportunity((self.view(1),
            self.view(2, requested_at_s=18., received_at_s=19.9)))
        self.assertEqual(result.kind, "native_headroom")
        self.assertEqual(result.reason, "physical_RPC_time_bounds_invalid_or_stale")

    def test_rpc_bounds_need_no_actor_absolute_clock_mapping(self):
        observation = replace(self.observation, observed_at_s=1_000_002.)
        supply = replace(self.supply, observed_at_s=1_000_002.)
        samples = tuple(self.view(i, requested_at_s=1_000_001.+i*.2,
                        received_at_s=1_000_001.15+i*.2, source_elapsed_s=.125) for i in (1, 2))
        result = self.opportunity(samples, observation=observation, supply=supply)
        self.assertEqual(result.kind, "idle_device_headroom")
        self.assertEqual(result.evidence["physical_considered"][0]["source_elapsed_s"], .125)

    def test_native_queue_pressure_and_preemption_win_over_idle_physical_samples(self):
        samples = (self.view(1), self.view(2))
        for changes in ({"waiting": 1}, {"memory_pressure": True}, {"kv_usage": .51}):
            with self.subTest(changes=changes):
                result = self.opportunity(samples, observation=replace(self.observation, **changes))
                self.assertEqual((result.kind, result.extra_geometric_steps), ("none", 0))
        self.assertEqual(self.opportunity(samples, preemption_increased=True).kind, "none")

    def test_unknown_or_old_native_feedback_does_not_prove_a_probe_opportunity(self):
        for changes in ({"fresh": False}, {"sample_age_s": None}, {"sample_age_s": 1.01},
                        {"running": None}, {"waiting": None}, {"kv_usage": None}):
            with self.subTest(changes=changes):
                result = self.opportunity((self.view(1), self.view(2)),
                    observation=replace(self.observation, **changes))
                self.assertEqual(result.kind, "none")
                self.assertEqual(result.reason, "native_feedback_not_fresh")

    def test_supply_and_native_timestamps_must_overlap_the_freshness_bound(self):
        for at in (19.9, 21.):
            with self.subTest(supply_at=at):
                result = self.opportunity(supply=replace(self.supply, observed_at_s=at))
                self.assertEqual(result.kind, "none")

    def test_no_ready_supply_is_not_spare_compute_even_at_source_eof(self):
        for changes in ({"ready_items": 0}, {"ready_items": 0, "source_exhausted": True},
                        {"exhausted": True}):
            with self.subTest(changes=changes):
                result = self.opportunity(supply=replace(self.supply, **changes))
                self.assertEqual(result.kind, "none")
                self.assertEqual(result.reason, "no_ready_supply")
        # Real prepared backlog remains actionable after the source reaches EOF.
        self.assertEqual(self.opportunity(supply=replace(self.supply,
            source_exhausted=True, upstream_pending=False)).kind, "native_headroom")

    def test_handoff_gaps_unsettled_requests_and_unknown_active_counts_are_not_full_supply(self):
        cases = ((replace(self.observation, inflight=0), replace(self.supply, active_requests=0)),
                 (replace(self.observation, inflight=1), self.supply),
                 (self.observation, replace(self.supply, active_requests=1)),
                 (self.observation, replace(self.supply, active_requests=None)),
                 (replace(self.observation, transition_pending=1), self.supply))
        for observation, supply in cases:
            with self.subTest(observation=observation, supply=supply):
                result = self.opportunity(observation=observation, supply=supply)
                self.assertEqual(result.kind, "none")
                self.assertEqual(result.reason, "current_window_not_fully_inflight")

    def test_byte_transport_and_consumer_backpressure_remain_independent_gates(self):
        for blocked in ("result_bytes", "input_bytes", "pending_transport_admission", "member_R"):
            with self.subTest(blocked=blocked):
                result = self.opportunity(supply=replace(self.supply, capacity_blocked=blocked))
                self.assertEqual(result.kind, "none")
                self.assertEqual(result.reason, "consumer_or_capacity_blocked")
        result = self.opportunity(observation=replace(self.observation, consumer_backpressure=True))
        self.assertEqual(result.kind, "none")

    def test_non_power_of_two_R_is_a_hard_bound_not_a_measured_plateau(self):
        result = self.opportunity((self.view(1), self.view(2)), current_window=32,
            observation=replace(self.observation, inflight=32),
            supply=replace(self.supply, active_requests=32))
        self.assertEqual((result.kind, result.extra_geometric_steps), ("idle_device_headroom", 1))
        result = self.opportunity(current_window=37,
            observation=replace(self.observation, inflight=37),
            supply=replace(self.supply, active_requests=37))
        self.assertEqual((result.kind, result.reason), ("none", "request_capacity_boundary"))

    def test_draining_active_requests_at_R_do_not_create_a_growth_opportunity(self):
        result = self.opportunity(observation=replace(self.observation, inflight=37),
                                  supply=replace(self.supply, active_requests=37))
        self.assertEqual(result.kind, "none")
        self.assertEqual(result.reason, "request_capacity_boundary")

    def test_condition_omits_poll_sequence_clock_session_and_changing_load_counters(self):
        first = self.opportunity((self.view(1), self.view(2)))
        samples = tuple(self.view(i, session_id=8, requested_at_s=20.+i*.05,
                        received_at_s=20.1+i*.05) for i in (8, 9))
        later = self.opportunity(samples, current_window=4, expected_session_id=8,
            observation=replace(self.observation, observed_at_s=21., inflight=4, running=4),
            supply=replace(self.supply, observed_at_s=21., active_requests=4, ready_items=20))
        self.assertEqual(first.kind, later.kind)
        self.assertEqual(first.condition, later.condition)

    def test_invalid_caller_limits_and_missing_native_identity_are_explicit(self):
        for changes in ({"current_window": 0}, {"hard_limit": True}, {"hard_limit": 1},
                        {"max_age_s": 0}, {"low_kv_usage": math.nan},
                        {"preemption_increased": None}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.opportunity(**changes)
        for changes in ({"expected_engine_uid": None}, {"expected_session_id": None}):
            with self.subTest(changes=changes):
                result = self.opportunity(**changes)
                self.assertEqual((result.kind, result.reason), ("none", "native_identity_unknown"))


if __name__ == "__main__":
    unittest.main()
