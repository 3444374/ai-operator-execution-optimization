"""Deterministic native-resource proposals, without a model or GPU probe."""

from dataclasses import replace
import math
import unittest

from src.scheduling.submission_control.resource_step import (
    NativeKVFrame, RequestKVWork, propose_kv_window,
)


class ResourceStepTests(unittest.TestCase):
    def frame(self, **changes):
        frame = NativeKVFrame("engine-a", 7, 9, 10., 10.1, 1000, 16, .25,
                              cost_model="single_pool_token_blocks")
        return replace(frame, **changes)

    def work(self, key, prompt=8, output=8):
        return RequestKVWork(key, prompt, output)

    def propose(self, frame="default", **changes):
        arguments = dict(now_s=10.2, max_age_s=1., expected_engine_uid="engine-a",
            expected_session_id=7, current_window=1, hard_limit=32,
            ready=tuple(self.work(i) for i in range(1, 13)),
            committed=(self.work(0),))
        arguments.update(changes)
        return propose_kv_window(self.frame() if frame == "default" else frame, **arguments)

    def test_costed_headroom_can_skip_the_adjacent_geometric_point(self):
        result = self.propose()
        self.assertEqual(result.target_window, 13)
        self.assertEqual(result.reason, "native_kv_costed_ready_prefix")
        self.assertEqual(result.evidence["fitting_ready_prefix"], 12)
        self.assertFalse(result.evidence["fallback_required"])

    def test_request_hard_limit_remains_binding(self):
        result = self.propose(hard_limit=5)
        self.assertEqual(result.target_window, 5)
        self.assertEqual(result.evidence["fitting_ready_prefix"], 4)

    def test_only_the_costed_ready_prefix_is_proposed(self):
        result = self.propose(ready=(self.work(1), self.work(2)))
        self.assertEqual(result.target_window, 3)
        self.assertEqual(result.evidence["ready_requests"], 2)

    def test_blocks_round_up_and_committed_requests_are_fully_reserved(self):
        result = self.propose(self.frame(total_blocks=10, usage_fraction=.5),
            committed=(self.work(0, 16, 1),), ready=(self.work(1, 16, 1), self.work(2, 16, 1)))
        self.assertEqual(result.target_window, 2)
        self.assertEqual(result.evidence["sampled_free_blocks"], 5)
        self.assertEqual(result.evidence["reserved_committed_blocks"], 2)
        self.assertEqual(result.evidence["available_blocks"], 3)
        self.assertEqual(result.evidence["prefix_blocks"], 2)

    def test_no_reordering_past_a_costly_ready_request(self):
        result = self.propose(self.frame(total_blocks=10, usage_fraction=.5),
            ready=(self.work(1), self.work(2, 64, 16), self.work(3)))
        self.assertEqual(result.target_window, 2)
        self.assertEqual(result.evidence["fitting_ready_prefix"], 1)

    def test_full_committed_liability_does_not_become_negative_headroom(self):
        result = self.propose(self.frame(total_blocks=10, usage_fraction=.5),
            committed=(self.work(0, 128, 16),))
        self.assertIsNone(result.target_window)
        self.assertEqual(result.evidence["available_blocks"], 0)

    def test_draining_old_requests_does_not_itself_propose_an_increase(self):
        result = self.propose(ready=(), committed=tuple(self.work(i) for i in range(8)))
        self.assertIsNone(result.target_window)
        self.assertEqual(result.reason, "native_kv_no_costed_growth")

    def test_draining_cannot_bypass_the_hard_request_limit(self):
        result = self.propose(hard_limit=3, committed=tuple(self.work(i) for i in range(8)),
                              ready=(self.work(9),))
        self.assertIsNone(result.target_window)

    def test_full_pool_does_not_supply_a_growth_proposal(self):
        result = self.propose(self.frame(usage_fraction=1.))
        self.assertIsNone(result.target_window)
        self.assertEqual(result.evidence["available_blocks"], 0)

    def test_unknown_frame_is_unknown_rather_than_idle(self):
        result = self.propose(None)
        self.assertIsNone(result.target_window)
        self.assertEqual(result.reason, "native_kv_frame_unknown")

    def test_wrong_engine_or_session_is_not_current_capacity(self):
        for changes in ({"engine_uid": "engine-b"}, {"session_id": 8},
                        {"session_id": None}, {"sequence": None}, {"sequence": -1}):
            with self.subTest(changes=changes):
                result = self.propose(self.frame(**changes))
                self.assertIsNone(result.target_window)
                self.assertEqual(result.reason, "native_kv_identity_unknown_or_mismatched")

    def test_stale_resource_sample_is_not_free_capacity(self):
        result = self.propose(self.frame(observed_at_s=8.5))
        self.assertIsNone(result.target_window)
        self.assertEqual(result.reason, "native_kv_frame_stale")

    def test_receive_time_cannot_make_an_old_source_sample_fresh(self):
        result = self.propose(self.frame(observed_at_s=5., received_at_s=10.19))
        self.assertIsNone(result.target_window)
        self.assertEqual(result.reason, "native_kv_frame_stale")

    def test_unknown_unmapped_or_invalid_clocks_are_rejected(self):
        cases = ({"clock_domain": None}, {"clock_domain": "remote.raw-clock"},
                 {"observed_at_s": None}, {"observed_at_s": math.nan},
                 {"observed_at_s": 10.15}, {"received_at_s": 10.3})
        for changes in cases:
            with self.subTest(changes=changes):
                result = self.propose(self.frame(**changes))
                self.assertIsNone(result.target_window)
                self.assertEqual(result.reason, "native_kv_clock_unknown_or_invalid")

    def test_missing_or_invalid_pool_configuration_is_unknown(self):
        for changes in ({"total_blocks": None}, {"total_blocks": 0},
                        {"block_tokens": None}, {"block_tokens": True},
                        {"usage_fraction": None}, {"usage_fraction": math.inf},
                        {"usage_fraction": -.1}, {"usage_fraction": 1.1}):
            with self.subTest(changes=changes):
                result = self.propose(self.frame(**changes))
                self.assertIsNone(result.target_window)
                self.assertEqual(result.reason, "native_kv_capacity_unknown_or_invalid")

    def test_layout_and_extra_engine_reservations_need_a_valid_cost_model(self):
        for model in (None, "hybrid_pool", "speculative_decode"):
            with self.subTest(cost_model=model):
                result = self.propose(self.frame(cost_model=model))
                self.assertIsNone(result.target_window)
                self.assertEqual(result.reason, "native_kv_cost_model_unknown_or_unsupported")

    def test_unknown_ready_or_committed_cost_is_never_zero(self):
        for field in ("ready", "committed"):
            for work in (self.work(1, None), self.work(1, output=None), self.work(1, output=0),
                         self.work(1, prompt=True), self.work(1, prompt=-1), None):
                with self.subTest(field=field, work=work):
                    result = self.propose(**{field: (work,)})
                    self.assertIsNone(result.target_window)
                    self.assertEqual(result.reason, "prepared_request_cost_unknown_or_invalid")

    def test_missing_sequence_of_committed_requests_is_unknown(self):
        result = self.propose(committed=None)
        self.assertIsNone(result.target_window)
        self.assertEqual(result.reason, "request_cost_sequences_unknown")

    def test_duplicate_request_identity_is_not_double_counted_or_reused(self):
        for ready in ((self.work(0),), (self.work(1), self.work(1)),
                      (self.work(None),), (self.work([]),)):
            with self.subTest(ready=ready):
                result = self.propose(ready=ready)
                self.assertIsNone(result.target_window)
                self.assertEqual(result.reason, "request_identity_missing_or_duplicated")

    def test_compute_and_physical_resource_limits_are_not_inferred_from_kv(self):
        result = self.propose()
        self.assertIsNone(result.evidence["physical_headroom"])
        self.assertIsNone(result.evidence["compute_headroom"])
        self.assertFalse(result.evidence["performance_platform_proven"])

    def test_invalid_caller_limits_or_clock_bounds_are_programming_errors(self):
        for changes in ({"current_window": 0}, {"hard_limit": 0}, {"hard_limit": True},
                        {"current_window": 33}, {"now_s": math.nan}, {"max_age_s": 0}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.propose(**changes)


if __name__ == "__main__":
    unittest.main()
