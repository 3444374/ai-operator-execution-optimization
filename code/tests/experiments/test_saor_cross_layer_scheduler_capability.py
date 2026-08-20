"""Pure tests for the blocked SAOR versus DRR/VTC cross-layer capability."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.experiments.saor.cross_layer_scheduler_capability import (
    HEADLINE_ARM_IDS,
    audit_cross_layer_capability,
    build_cross_layer_evidence_report,
    load_cross_layer_capability,
)
from src.experiments.saor.in_engine_scheduler_logic import (
    DrrPolicy,
    FairRequest,
    FcfsPolicy,
    RequestIdentityError,
    VtcPolicy,
    decode_request_identity,
    encode_request_identity,
    recover_unique_client_ids,
)
from src.experiments.saor.vllm_0251_source_audit import (
    audit_installed_vllm_0251,
)


def request(
    request_id: str,
    client_id: str,
    *,
    prompt: int = 0,
    cap: int = 1,
    arrival: int = 0,
) -> FairRequest:
    return FairRequest(request_id, client_id, prompt, cap, arrival)


class RequestIdentityTest(unittest.TestCase):
    def test_job_identity_round_trip_is_unique_and_does_not_collapse(self) -> None:
        ids = [
            encode_request_identity("job0", "0" * 32),
            encode_request_identity("job1", "1" * 32),
        ]
        self.assertEqual(
            recover_unique_client_ids(
                ids, expected_clients={"job0", "job1"}
            ),
            ("job0", "job1"),
        )
        self.assertEqual(decode_request_identity(ids[0]), ("job0", "0" * 32))

    def test_missing_illegal_or_duplicate_identity_fails_closed(self) -> None:
        valid = encode_request_identity("job0", "a" * 32)
        for invalid in (
            None,
            "",
            "job0",
            "saor-xlayer.v1/job-x/" + "a" * 32,
            "saor-xlayer.v1/job0/not-unique",
        ):
            with self.subTest(invalid=invalid), self.assertRaises(RequestIdentityError):
                decode_request_identity(invalid)
        with self.assertRaisesRegex(RequestIdentityError, "not unique"):
            recover_unique_client_ids([valid, valid])
        with self.assertRaisesRegex(RequestIdentityError, "collapsed"):
            recover_unique_client_ids(
                [valid, encode_request_identity("job0", "b" * 32)],
                expected_clients={"job0", "job1"},
            )
        with self.assertRaises(RequestIdentityError):
            encode_request_identity("default", "b" * 32)


class PurePolicyTest(unittest.TestCase):
    def test_fcfs_orders_by_arrival_then_enqueue_without_client_bias(self) -> None:
        policy = FcfsPolicy()
        policy.enqueue(request("late", "job0", arrival=2))
        policy.enqueue(request("first", "job1", arrival=1))
        policy.enqueue(request("second", "job0", arrival=1))
        self.assertEqual(
            [policy.pop_next().request_id for _ in range(3)],
            ["first", "second", "late"],
        )

    def test_drr_uses_prompt_plus_frozen_cap_and_retains_deficit_while_backlogged(self) -> None:
        policy = DrrPolicy(quantum=100)
        policy.enqueue(request("a0", "job0", prompt=50, cap=100))
        policy.enqueue(request("a1", "job0", prompt=50, cap=100))
        policy.enqueue(request("b0", "job1", prompt=0, cap=100))
        self.assertEqual(policy.pop_next().request_id, "b0")
        self.assertEqual(policy.pop_next().request_id, "a0")
        self.assertEqual(policy.deficit("job0"), 50)
        self.assertEqual(policy.pop_next().request_id, "a1")
        self.assertIsNone(policy.pop_next())

    def test_drr_has_no_completion_correction_or_saor_state(self) -> None:
        policy = DrrPolicy(quantum=10)
        item = request("a", "job0", prompt=2, cap=8)
        policy.enqueue(item)
        self.assertEqual(policy.pop_next().estimated_work, 10)
        self.assertFalse(hasattr(policy, "complete"))
        self.assertFalse(hasattr(policy, "debt"))

    def test_vtc_lifts_reactivated_client_to_active_floor(self) -> None:
        policy = VtcPolicy()
        policy.enqueue(request("a0", "job0", prompt=10, cap=100))
        self.assertEqual(policy.pop_next().request_id, "a0")
        policy.complete("a0", actual_output_tokens=90)

        policy.enqueue(request("b0", "job1", prompt=10, cap=100))
        policy.enqueue(request("b1", "job1", prompt=10, cap=100, arrival=1))
        self.assertEqual(policy.pop_next().request_id, "b0")
        policy.complete("b0", actual_output_tokens=190)
        self.assertEqual(policy.virtual_counter("job1"), 200)

        policy.enqueue(request("a1", "job0", prompt=1, cap=100, arrival=2))
        self.assertEqual(policy.virtual_counter("job0"), 200)

    def test_vtc_accounts_actual_service_not_output_oracle(self) -> None:
        policy = VtcPolicy()
        policy.enqueue(request("a", "job0", prompt=10, cap=100))
        policy.pop_next()
        policy.account_output("a", 1)
        policy.complete("a", actual_output_tokens=2)
        self.assertEqual(policy.accumulated_service("job0"), 12)
        self.assertEqual(policy.virtual_counter("job0"), 12)

    def test_vtc_overload_is_work_conserving_and_ties_are_deterministic(self) -> None:
        policy = VtcPolicy()
        for index in range(20):
            policy.enqueue(request(f"a{index}", "job0", arrival=index))
            policy.enqueue(request(f"b{index}", "job1", arrival=index))
        selected = []
        while policy.has_backlog:
            item = policy.pop_next()
            self.assertIsNotNone(item)
            selected.append(item.client_id)
            policy.complete(item.request_id, actual_output_tokens=0)
        self.assertEqual(selected[:4], ["job0", "job1", "job0", "job1"])
        self.assertEqual(len(selected), 40)

    def test_vtc_proportional_service_uses_normalized_counter(self) -> None:
        policy = VtcPolicy({"job0": 1.0, "job1": 2.0})
        for index in range(100):
            policy.enqueue(request(f"a{index}", "job0", prompt=1, arrival=index))
            policy.enqueue(request(f"b{index}", "job1", prompt=1, arrival=index))
        for _ in range(30):
            item = policy.pop_next()
            policy.complete(item.request_id, actual_output_tokens=0)
        service0 = policy.accumulated_service("job0")
        service1 = policy.accumulated_service("job1")
        self.assertLessEqual(abs(service1 - 2 * service0), 2)
        self.assertLessEqual(
            abs(policy.virtual_counter("job0") - policy.virtual_counter("job1")),
            1,
        )

    def test_vtc_equal_weight_service_difference_is_bounded_in_synthetic_case(self) -> None:
        policy = VtcPolicy()
        max_request_service = 5
        for index in range(100):
            policy.enqueue(request(f"a{index}", "job0", prompt=3, arrival=index))
            policy.enqueue(request(f"b{index}", "job1", prompt=3, arrival=index))
        for _ in range(41):
            item = policy.pop_next()
            policy.complete(item.request_id, actual_output_tokens=2)
        difference = abs(
            policy.accumulated_service("job0")
            - policy.accumulated_service("job1")
        )
        self.assertLessEqual(difference, max_request_service)


class CapabilityContractTest(unittest.TestCase):
    @property
    def repository(self) -> Path:
        return Path(__file__).resolve().parents[3]

    @property
    def config_path(self) -> Path:
        return (
            self.repository
            / "deploy/autodl/saor_cross_layer_scheduler_capability.example.json"
        )

    def test_release_config_is_four_arm_blocked_capability(self) -> None:
        config = load_cross_layer_capability(self.config_path)
        self.assertEqual(
            tuple(arm.arm_id for arm in config.headline_arms), HEADLINE_ARM_IDS
        )
        audit = audit_cross_layer_capability(config)
        self.assertEqual(audit["capability_status"], "blocked")
        self.assertFalse(audit["formal_authorized"])
        self.assertFalse(audit["performance_ranking_published"])
        with self.assertRaisesRegex(PermissionError, "blocked"):
            build_cross_layer_evidence_report(config, [])

    def test_service_layer_reproduction_rejects_project_controls(self) -> None:
        raw = json.loads(self.config_path.read_text(encoding="utf-8"))
        raw["headline_arms"][1]["controls"]["shared_credit"] = True
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw["scheduler_module"]["path"] = str(
                self.repository
                / "code/src/experiments/saor/vllm_scheduler_plugin.py"
            )
            path = root / "capability.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Project/SAOR controls"):
                load_cross_layer_capability(path)

    def test_current_machine_installed_source_audit_fails_closed(self) -> None:
        audit = audit_installed_vllm_0251()
        self.assertIn(audit["status"], {
            "blocked_runtime_not_installed", "blocked_source_drift", "passed"
        })
        if audit["status"] != "passed":
            self.assertTrue(audit["errors"])


if __name__ == "__main__":
    unittest.main()
