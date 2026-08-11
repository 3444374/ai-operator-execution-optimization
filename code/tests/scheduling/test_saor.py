from __future__ import annotations

import sys
import unittest
from pathlib import Path


CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.core.control import CapacityArm  # noqa: E402
from src.scheduling.submission_control.saor import (  # noqa: E402
    SaorAction,
    SaorControlState,
    SaorJobState,
    SaorPolicy,
    SaorReleaseCandidate,
    build_single_release_actions,
    update_fairness_debts,
)


class SaorPolicyTests(unittest.TestCase):
    @staticmethod
    def policy(*, eta_f: float = 1.0) -> SaorPolicy:
        return SaorPolicy(
            v=1.0,
            eta_f=eta_f,
            tail_weight=1.0,
            energy_weight=0.0,
            switch_weight=1.0,
        )

    def test_fairness_debt_breaks_equal_queue_tie(self) -> None:
        arm = CapacityArm(2, 200)
        fallback = SaorAction("fallback", "gpu0", arm)
        jobs = (
            SaorJobState("a", weight=1.0, ready_work=100),
            SaorJobState(
                "b",
                weight=1.0,
                ready_work=100,
                fairness_debt=80.0,
            ),
        )
        actions = tuple(
            SaorAction(
                f"release-{job_id}",
                "gpu0",
                arm,
                (
                    SaorReleaseCandidate(
                        f"{job_id}0",
                        job_id,
                        "gpu0",
                        100,
                    ),
                ),
                ((job_id, 100.0),),
            )
            for job_id in ("a", "b")
        )
        state = SaorControlState(
            jobs,
            actions,
            fallback,
            current_arm=arm,
            observed_at_s=10.0,
            calibration_signature="sig",
        )

        decision = self.policy(eta_f=1.0).select(
            state,
            now_s=10.1,
            max_age_s=1.0,
            calibration_signature="sig",
        )

        self.assertEqual(decision.action.action_id, "release-b")
        self.assertEqual(decision.reason, "minimum_drift_plus_penalty")

    def test_stale_state_fails_closed_to_fallback(self) -> None:
        arm = CapacityArm(1, 100)
        fallback = SaorAction("fallback", "gpu0", arm)
        action = SaorAction(
            "release",
            "gpu0",
            arm,
            (SaorReleaseCandidate("r0", "a", "gpu0", 100),),
            (("a", 100.0),),
        )
        state = SaorControlState(
            (SaorJobState("a", 1.0, 100),),
            (action,),
            fallback,
            current_arm=arm,
            observed_at_s=1.0,
            calibration_signature="sig",
        )

        decision = self.policy().select(
            state,
            now_s=3.0,
            max_age_s=1.0,
            calibration_signature="sig",
        )

        self.assertEqual(decision.action.action_id, "fallback")
        self.assertEqual(decision.reason, "stale_observation")

    def test_single_release_actions_respect_request_and_work_capacity(self) -> None:
        small = CapacityArm(1, 100)
        large = CapacityArm(2, 200)
        actions = build_single_release_actions(
            endpoint_id="gpu0",
            arms=(small, large),
            current_arm=small,
            ready_heads=(
                SaorReleaseCandidate("fits", "a", "gpu0", 90),
                SaorReleaseCandidate("too-large", "b", "gpu0", 150),
            ),
            active_requests=0,
            active_work=0,
            predicted_incremental_service_by_request={
                "fits": 90.0,
                "too-large": 150.0,
            },
            predicted_goodput_delta_by_arm={small: 90.0, large: 120.0},
            tail_risk_delta_by_arm={small: 0.0, large: 0.0},
            energy_delta_by_arm={small: 0.0, large: 0.0},
            switch_cost_by_arm={small: 0.0, large: 0.0},
        )

        release_ids = {
            action.releases[0].request_id
            for action in actions
            if action.releases
        }
        self.assertEqual(release_ids, {"fits", "too-large"})
        self.assertFalse(
            any(
                action.arm == small
                and action.releases
                and action.releases[0].request_id == "too-large"
                for action in actions
            )
        )
        self.assertTrue(any(not action.releases for action in actions))

    def test_action_builder_rejects_missing_cost_configuration(self) -> None:
        arm = CapacityArm(1, 100)

        with self.assertRaisesRegex(ValueError, "missing energy delta"):
            build_single_release_actions(
                endpoint_id="gpu0",
                arms=(arm,),
                current_arm=arm,
                ready_heads=(
                    SaorReleaseCandidate("r0", "a", "gpu0", 100),
                ),
                active_requests=0,
                active_work=0,
                predicted_incremental_service_by_request={"r0": 50.0},
                predicted_goodput_delta_by_arm={arm: 10.0},
                tail_risk_delta_by_arm={arm: 0.0},
                energy_delta_by_arm={},
                switch_cost_by_arm={arm: 0.0},
            )

    def test_weighted_fairness_debt_uses_only_active_jobs(self) -> None:
        jobs = (
            SaorJobState("vip", 3.0, ready_work=100),
            SaorJobState("regular", 1.0, ready_work=100),
            SaorJobState("idle", 1.0, ready_work=0),
        )

        updated = update_fairness_debts(
            jobs,
            {"vip": 0, "regular": 100, "idle": 0},
        )
        by_job = {job.job_id: job for job in updated}

        self.assertEqual(by_job["vip"].fairness_debt, 75.0)
        self.assertEqual(by_job["regular"].fairness_debt, 0.0)
        self.assertEqual(by_job["idle"].fairness_debt, 0.0)

    def test_policy_uses_predicted_service_not_release_work(self) -> None:
        arm = CapacityArm(2, 2_000)
        fallback = SaorAction("fallback", "gpu0", arm)
        jobs = (
            SaorJobState("a", 1.0, ready_work=1_000),
            SaorJobState("b", 1.0, ready_work=1_000),
        )
        actions = (
            SaorAction(
                "large-release-low-service",
                "gpu0",
                arm,
                (SaorReleaseCandidate("a0", "a", "gpu0", 1_000),),
                (("a", 10.0),),
            ),
            SaorAction(
                "small-release-high-service",
                "gpu0",
                arm,
                (SaorReleaseCandidate("b0", "b", "gpu0", 100),),
                (("b", 20.0),),
            ),
        )
        state = SaorControlState(
            jobs,
            actions,
            fallback,
            current_arm=arm,
            observed_at_s=1.0,
            calibration_signature="sig",
        )

        selected = self.policy(eta_f=0.0).select(
            state,
            now_s=1.0,
            max_age_s=1.0,
            calibration_signature="sig",
        )

        self.assertEqual(
            selected.action.action_id,
            "small-release-high-service",
        )


if __name__ == "__main__":
    unittest.main()
