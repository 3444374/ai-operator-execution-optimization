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
    SaorBoundedHeadState,
    SaorControlState,
    SaorJobState,
    SaorPolicy,
    SaorReleaseCandidate,
    SaorReleaseConfig,
    SaorReleaseState,
    build_single_release_actions,
    select_bounded_saor_release,
    select_saor_release_job,
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

    def test_fixed_envelope_release_reclaims_future_share_for_new_job(self) -> None:
        selection = select_saor_release_job(
            (
                SaorReleaseState(
                    "bulk",
                    1.0,
                    active_requests=4,
                    active_work=400,
                    waiting_work=1_000,
                    fairness_debt=0.0,
                    oldest_waiting_age_s=10.0,
                    slo_target_s=None,
                    arrival_order=0,
                ),
                SaorReleaseState(
                    "foreground",
                    1.0,
                    active_requests=0,
                    active_work=0,
                    waiting_work=100,
                    fairness_debt=0.0,
                    oldest_waiting_age_s=1.0,
                    slo_target_s=None,
                    arrival_order=1,
                ),
            ),
            request_limit=4,
            work_limit=400,
            config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

        self.assertEqual(selection.job_id, "foreground")
        self.assertGreater(selection.entitlement_deficit, 0.0)

    def test_fixed_envelope_release_ignores_nonfitting_underentitled_head(self) -> None:
        selection = select_saor_release_job(
            (
                SaorReleaseState(
                    "large",
                    1.0,
                    active_requests=0,
                    active_work=0,
                    waiting_work=200,
                    fairness_debt=0.0,
                    oldest_waiting_age_s=2.0,
                    slo_target_s=None,
                    arrival_order=0,
                    eligible=False,
                ),
                SaorReleaseState(
                    "small",
                    1.0,
                    active_requests=1,
                    active_work=50,
                    waiting_work=50,
                    fairness_debt=0.0,
                    oldest_waiting_age_s=1.0,
                    slo_target_s=None,
                    arrival_order=1,
                ),
            ),
            request_limit=2,
            work_limit=100,
            config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

        self.assertEqual(selection.job_id, "small")

    @staticmethod
    def bounded_head(
        job_id: str,
        *,
        debt: float = 0.0,
        cap: float | None = None,
        head_work: int = 50,
        fits: bool = True,
        ready: bool = True,
        priority: int = 0,
        remaining_slo_s: float | None = None,
        priority_window_s: float | None = None,
        recovery_inflight: bool = False,
        recovery_inflight_work: int = 0,
        active_work: int = 0,
        arrival_order: int = 0,
    ) -> SaorBoundedHeadState:
        if recovery_inflight and recovery_inflight_work == 0:
            recovery_inflight_work = head_work
        active_work = max(active_work, recovery_inflight_work)
        return SaorBoundedHeadState(
            release=SaorReleaseState(
                job_id,
                1.0,
                active_requests=0,
                active_work=active_work,
                waiting_work=head_work if ready else 0,
                fairness_debt=debt,
                oldest_waiting_age_s=1.0,
                slo_target_s=None,
                arrival_order=arrival_order,
                eligible=fits and ready,
            ),
            priority=priority,
            remaining_slo_budget_s=remaining_slo_s,
            priority_window_s=priority_window_s,
            debt_cap=cap,
            head_work=head_work if ready else 0,
            ready=ready,
            recovery_inflight=recovery_inflight,
            recovery_inflight_work=recovery_inflight_work,
        )

    def test_bounded_release_debt_recovery_precedes_slo_priority(self) -> None:
        selection = select_bounded_saor_release(
            (
                self.bounded_head("bulk", debt=100, cap=100),
                self.bounded_head(
                    "foreground",
                    priority=1,
                    remaining_slo_s=1.0,
                    priority_window_s=30.0,
                    arrival_order=1,
                ),
            ),
            request_limit=2,
            work_limit=200,
            active_work=100,
            config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

        self.assertEqual(selection.action, "grant")
        self.assertEqual(selection.tier, "debt_recovery")
        self.assertEqual(selection.job_id, "bulk")
        self.assertTrue(selection.constraint_conflict)

    def test_bounded_release_holds_only_for_concrete_debt_head(self) -> None:
        selection = select_bounded_saor_release(
            (
                self.bounded_head(
                    "bulk",
                    debt=125,
                    cap=100,
                    head_work=80,
                    fits=False,
                ),
                self.bounded_head("small", head_work=20, arrival_order=1),
            ),
            request_limit=2,
            work_limit=200,
            active_work=150,
            config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

        self.assertEqual(selection.action, "hold")
        self.assertEqual(selection.tier, "guard_reclaim_hold")
        self.assertEqual(selection.job_id, "bulk")
        self.assertEqual(selection.reclaim_debt, 30)

    def test_bounded_release_keeps_recovery_mode_while_debt_is_critical(
        self,
    ) -> None:
        selection = select_bounded_saor_release(
            (
                self.bounded_head(
                    "bulk",
                    debt=150,
                    cap=100,
                    recovery_inflight=True,
                ),
                self.bounded_head(
                    "foreground",
                    priority=1,
                    remaining_slo_s=2.0,
                    priority_window_s=30.0,
                    arrival_order=1,
                ),
            ),
            request_limit=2,
            work_limit=200,
            active_work=100,
            config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

        self.assertEqual(selection.tier, "debt_recovery")
        self.assertEqual(selection.job_id, "bulk")

    def test_bounded_release_counts_ordinary_own_inflight_repayment(self) -> None:
        selection = select_bounded_saor_release(
            (
                self.bounded_head(
                    "bulk",
                    debt=120,
                    cap=100,
                    active_work=80,
                ),
                self.bounded_head(
                    "foreground",
                    priority=1,
                    remaining_slo_s=2.0,
                    priority_window_s=30.0,
                    arrival_order=1,
                ),
            ),
            request_limit=4,
            work_limit=400,
            active_work=80,
            config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

        # Hand oracle: 120 - (1 - 1/2) * 80 = 80 < H=100.
        self.assertEqual(selection.tier, "slo_priority")
        self.assertEqual(selection.job_id, "foreground")

    def test_bounded_release_counts_foreign_residual_debt_growth(self) -> None:
        selection = select_bounded_saor_release(
            (
                self.bounded_head("bulk", debt=80, cap=100),
                self.bounded_head(
                    "foreground",
                    active_work=100,
                    priority=1,
                    remaining_slo_s=2.0,
                    priority_window_s=30.0,
                    arrival_order=1,
                ),
            ),
            request_limit=4,
            work_limit=400,
            active_work=100,
            config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

        # Hand oracle: 80 + (1/2) * 100 = 130 >= H=100.
        self.assertEqual(selection.tier, "debt_recovery")
        self.assertEqual(selection.job_id, "bulk")

    def test_bounded_release_recomputes_share_when_active_set_changes(self) -> None:
        bulk = self.bounded_head("bulk", debt=60, cap=100)
        foreground = self.bounded_head(
            "foreground",
            active_work=100,
            priority=1,
            remaining_slo_s=2.0,
            priority_window_s=30.0,
            arrival_order=1,
        )
        config = SaorReleaseConfig(1.0, 0.0, 1.0, 0.0)

        two_job = select_bounded_saor_release(
            (bulk, foreground),
            request_limit=4,
            work_limit=400,
            active_work=100,
            config=config,
        )
        three_job = select_bounded_saor_release(
            (
                bulk,
                foreground,
                self.bounded_head("newcomer", arrival_order=2),
            ),
            request_limit=4,
            work_limit=400,
            active_work=100,
            config=config,
        )

        # Two Jobs: 60 + 1/2*100 = 110. Three Jobs: 60 + 1/3*100 < 100.
        self.assertEqual(two_job.tier, "debt_recovery")
        self.assertEqual(three_job.tier, "slo_priority")

    def test_bounded_release_allows_only_one_discrete_crossing_quantum(
        self,
    ) -> None:
        states = (
            self.bounded_head("bulk", debt=130, cap=100, head_work=80),
            self.bounded_head("foreground", arrival_order=1),
        )

        selected = select_bounded_saor_release(
            states,
            request_limit=4,
            work_limit=400,
            active_work=0,
            config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

        # Hand oracle: D_after=130-(1/2)*80=90; overshoot=10 <= 40.
        self.assertEqual(selected.tier, "debt_recovery")
        self.assertEqual(selected.job_id, "bulk")

    def test_bounded_release_unready_debt_and_nonfitting_priority_do_not_hold(
        self,
    ) -> None:
        selection = select_bounded_saor_release(
            (
                self.bounded_head(
                    "bulk",
                    debt=100,
                    cap=100,
                    ready=False,
                    fits=False,
                ),
                self.bounded_head(
                    "foreground",
                    head_work=80,
                    fits=False,
                    priority=1,
                    remaining_slo_s=1.0,
                    priority_window_s=30.0,
                    arrival_order=1,
                ),
                self.bounded_head("fallback", head_work=20, arrival_order=2),
            ),
            request_limit=2,
            work_limit=200,
            active_work=150,
            config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

        self.assertEqual(selection.action, "grant")
        self.assertEqual(selection.tier, "saor_fallback")
        self.assertEqual(selection.job_id, "fallback")

    def test_bounded_release_priority_uses_priority_then_remaining_budget(self) -> None:
        selection = select_bounded_saor_release(
            (
                self.bounded_head(
                    "urgent-low-priority",
                    priority=1,
                    remaining_slo_s=-1.0,
                    priority_window_s=30.0,
                ),
                self.bounded_head(
                    "less-urgent-high-priority",
                    priority=2,
                    remaining_slo_s=5.0,
                    priority_window_s=30.0,
                    arrival_order=1,
                ),
                self.bounded_head(
                    "outside-window",
                    priority=3,
                    remaining_slo_s=31.0,
                    priority_window_s=30.0,
                    arrival_order=2,
                ),
            ),
            request_limit=3,
            work_limit=300,
            active_work=0,
            config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

        self.assertEqual(selection.tier, "slo_priority")
        self.assertEqual(selection.job_id, "less-urgent-high-priority")

    def test_bounded_release_rejects_oversized_head_and_missing_priority_slo(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "exceeds work limit"):
            select_bounded_saor_release(
                (self.bounded_head("oversized", head_work=201, fits=False),),
                request_limit=1,
                work_limit=200,
                active_work=0,
                config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
            )
        with self.assertRaisesRegex(ValueError, "priority requires"):
            self.bounded_head("priority", priority=1)

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
