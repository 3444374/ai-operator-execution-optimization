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

from src.scheduling.submission_control.shared_credit import (  # noqa: E402
    FairEndpointCreditCoordinator,
)
from src.scheduling.submission_control.saor import (  # noqa: E402
    SaorReleaseConfig,
)


class SharedCreditCoordinatorTests(unittest.TestCase):
    @staticmethod
    def bounded_coordinator(
        *,
        request_limit: int = 3,
        work_limit: int = 300,
    ) -> FairEndpointCreditCoordinator:
        return FairEndpointCreditCoordinator(
            {"gpu0": (request_limit, work_limit)},
            quantum=100,
            policy="saor_bounded_priority",
            saor_release_config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )

    def test_bounded_priority_requires_explicit_stable_job_contract(self) -> None:
        coordinator = self.bounded_coordinator(request_limit=1, work_limit=100)

        with self.assertRaisesRegex(ValueError, "priority requires"):
            coordinator.try_acquire(
                request_id="invalid",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=10,
                priority=1,
            )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="valid",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=10,
                priority=1,
                slo_budget_remaining_s=20.0,
                priority_window_s=30.0,
            )
        )
        with self.assertRaisesRegex(ValueError, "stable priority window"):
            coordinator.try_acquire(
                request_id="changed",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=10,
                priority=1,
                slo_budget_remaining_s=19.0,
                priority_window_s=10.0,
            )

    def test_bounded_priority_emits_one_recovery_until_actual_completion(
        self,
    ) -> None:
        coordinator = self.bounded_coordinator()
        self.assertTrue(
            coordinator.try_acquire(
                request_id="bulk-active",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=100,
                fairness_debt_cap=100.0,
            )
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="foreground-active",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=100,
                priority=1,
                slo_budget_remaining_s=5.0,
                priority_window_s=30.0,
            )
        )
        coordinator.release(
            "foreground-active",
            job_id="foreground",
            actual_work=200,
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="bulk-recovery",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=80,
                fairness_debt_cap=100.0,
            )
        )
        coordinator.try_acquire(
            request_id="bulk-followup",
            job_id="bulk",
            endpoint_id="gpu0",
            estimated_work=20,
            fairness_debt_cap=100.0,
        )

        events = coordinator.drain_release_events("gpu0")
        recovery = [event for event in events if event.tier == "debt_recovery"]
        self.assertEqual([event.selected_request_id for event in recovery], ["bulk-recovery"])
        self.assertEqual(
            coordinator.snapshot("gpu0").recovery_inflight_by_job,
            (("bulk", "bulk-recovery"),),
        )
        self.assertEqual(coordinator.drain_release_events("gpu0"), ())

        coordinator.release("bulk-recovery", job_id="bulk", actual_work=80)
        self.assertNotIn(
            ("bulk", "bulk-recovery"),
            coordinator.snapshot("gpu0").recovery_inflight_by_job,
        )

    def test_bounded_priority_reclaim_hold_targets_ready_head_then_resumes(
        self,
    ) -> None:
        coordinator = self.bounded_coordinator(request_limit=4, work_limit=200)
        self.assertTrue(
            coordinator.try_acquire(
                request_id="bulk-active",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=100,
                fairness_debt_cap=100.0,
            )
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="foreground-active",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=50,
                priority=1,
                slo_budget_remaining_s=5.0,
                priority_window_s=30.0,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="bulk-recovery",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=120,
                fairness_debt_cap=100.0,
            )
        )
        coordinator.release(
            "foreground-active",
            job_id="foreground",
            actual_work=200,
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="foreground-waiting",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=20,
                priority=1,
                slo_budget_remaining_s=4.0,
                priority_window_s=30.0,
            )
        )
        held = coordinator.snapshot("gpu0")
        self.assertEqual(held.guard_hold_target_job_id, "bulk")
        self.assertEqual(held.guard_reclaim_debt, 20)

        coordinator.release("bulk-active", job_id="bulk", actual_work=100)

        resumed = coordinator.snapshot("gpu0")
        self.assertEqual(
            resumed.active_by_job,
            (("bulk", 1), ("foreground", 1)),
        )
        self.assertEqual(resumed.guard_hold_target_job_id, "")
        events = coordinator.drain_release_events("gpu0")
        actions = [(event.action, event.tier) for event in events]
        self.assertIn(("hold_start", "guard_reclaim_hold"), actions)
        self.assertIn(("hold_end", "guard_reclaim_hold"), actions)
        self.assertIn(("grant", "debt_recovery"), actions)
        self.assertLess(
            next(i for i, item in enumerate(actions) if item == ("hold_end", "guard_reclaim_hold")),
            next(
                i
                for i, event in enumerate(events)
                if event.action == "grant"
                and event.tier == "slo_priority"
                and event.selected_request_id == "foreground-waiting"
            ),
        )

    def test_bounded_priority_unready_debt_and_nonfitting_priority_do_not_hold(
        self,
    ) -> None:
        coordinator = self.bounded_coordinator(request_limit=2, work_limit=100)
        self.assertTrue(
            coordinator.try_acquire(
                request_id="active",
                job_id="other",
                endpoint_id="gpu0",
                estimated_work=80,
            )
        )
        coordinator._fairness_debt["gpu0"]["bulk"] = 100.0
        self.assertFalse(
            coordinator.try_acquire(
                request_id="priority-too-large",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=80,
                priority=1,
                slo_budget_remaining_s=1.0,
                priority_window_s=30.0,
            )
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="fallback",
                job_id="other",
                endpoint_id="gpu0",
                estimated_work=20,
            )
        )

        snapshot = coordinator.snapshot("gpu0")
        self.assertEqual(snapshot.guard_hold_target_job_id, "")
        self.assertEqual(snapshot.active_by_job, (("other", 2),))

    def test_bounded_priority_debt_uses_actual_completion_work(self) -> None:
        coordinator = self.bounded_coordinator(request_limit=2, work_limit=200)
        for job_id in ("a", "b"):
            self.assertTrue(
                coordinator.try_acquire(
                    request_id=f"{job_id}-active",
                    job_id=job_id,
                    endpoint_id="gpu0",
                    estimated_work=100,
                )
            )

        coordinator.release("a-active", job_id="a", actual_work=60)

        self.assertEqual(
            coordinator.snapshot("gpu0").fairness_debt_by_job,
            (("a", 0.0), ("b", 30.0)),
        )

    def test_bounded_priority_hold_event_is_auditable_and_capacity_is_safe(
        self,
    ) -> None:
        now = [0.0]
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (3, 200)},
            quantum=100,
            policy="saor_bounded_priority",
            saor_release_config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
            clock=lambda: now[0],
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="bulk-active",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=100,
                fairness_debt_cap=100.0,
            )
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="foreground-active",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=50,
                priority=1,
                slo_budget_remaining_s=5.0,
                priority_window_s=30.0,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="bulk-recovery",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=120,
                fairness_debt_cap=100.0,
            )
        )
        coordinator.release(
            "foreground-active",
            job_id="foreground",
            actual_work=200,
        )
        held = coordinator.snapshot("gpu0")
        self.assertLessEqual(held.active_requests, held.request_limit)
        self.assertLessEqual(held.active_work, held.work_limit)

        now[0] = 2.5
        coordinator.release("bulk-active", job_id="bulk", actual_work=100)
        events = coordinator.drain_release_events("gpu0")
        hold_end = next(event for event in events if event.action == "hold_end")
        self.assertEqual(hold_end.target_job_id, "bulk")
        self.assertEqual(hold_end.head_work, 120)
        self.assertEqual(hold_end.reclaim_debt, 20)
        self.assertEqual(hold_end.hold_duration_s, 2.5)
        self.assertFalse(hold_end.avoidable_idle)
        self.assertFalse(hold_end.foreign_grant_over_debt_critical)

    def test_bounded_priority_rejects_oversized_work_before_queueing(self) -> None:
        coordinator = self.bounded_coordinator(request_limit=1, work_limit=100)

        with self.assertRaisesRegex(ValueError, "exceeds endpoint work limit"):
            coordinator.try_acquire(
                request_id="oversized",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=101,
                fairness_debt_cap=25.0,
            )

        self.assertEqual(coordinator.snapshot("gpu0").waiting_requests, 0)

    def test_strict_priority_reclaims_future_releases_without_preemption(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (2, 200)},
            quantum=100,
            policy="strict_priority",
        )
        for request_id in ("bulk-active-0", "bulk-active-1"):
            self.assertTrue(
                coordinator.try_acquire(
                    request_id=request_id,
                    job_id="bulk",
                    endpoint_id="gpu0",
                    estimated_work=100,
                    priority=0,
                )
            )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="bulk-waiting",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=100,
                priority=0,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="foreground",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=100,
                priority=1,
            )
        )

        coordinator.release("bulk-active-0", job_id="bulk", actual_work=100)

        snapshot = coordinator.snapshot("gpu0")
        self.assertEqual(
            snapshot.active_by_job,
            (("bulk", 1), ("foreground", 1)),
        )
        self.assertEqual(snapshot.waiting_by_job, (("bulk", 1),))

    def test_strict_priority_requires_stable_job_priority(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (1, 100)},
            quantum=100,
            policy="strict_priority",
        )
        coordinator.try_acquire(
            request_id="first",
            job_id="foreground",
            endpoint_id="gpu0",
            estimated_work=100,
            priority=1,
        )

        with self.assertRaisesRegex(ValueError, "stable priority"):
            coordinator.try_acquire(
                request_id="second",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=100,
                priority=2,
            )

    def test_strict_priority_holds_bulk_during_foreground_refill_gap(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (2, 200)},
            quantum=100,
            policy="strict_priority",
        )
        for request_id in ("bulk-active-0", "bulk-active-1"):
            self.assertTrue(
                coordinator.try_acquire(
                    request_id=request_id,
                    job_id="bulk",
                    endpoint_id="gpu0",
                    estimated_work=100,
                    priority=0,
                )
            )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="bulk-waiting",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=100,
                priority=0,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="foreground-only",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=100,
                priority=1,
            )
        )
        coordinator.release("bulk-active-0", job_id="bulk")

        coordinator.release("bulk-active-1", job_id="bulk")

        self.assertEqual(
            coordinator.snapshot("gpu0").active_by_job,
            (("foreground", 1),),
        )
        self.assertEqual(
            coordinator.snapshot("gpu0").waiting_by_job,
            (("bulk", 1),),
        )
        coordinator.release("foreground-only", job_id="foreground")
        self.assertEqual(coordinator.snapshot("gpu0").active_requests, 0)
        coordinator.finish_job("foreground")
        self.assertEqual(
            coordinator.snapshot("gpu0").active_by_job,
            (("bulk", 1),),
        )

    def test_finish_job_rejects_outstanding_credit(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (1, 100)},
            quantum=100,
            policy="strict_priority",
        )
        coordinator.try_acquire(
            request_id="active",
            job_id="foreground",
            endpoint_id="gpu0",
            estimated_work=100,
            priority=1,
        )

        with self.assertRaisesRegex(ValueError, "outstanding credit"):
            coordinator.finish_job("foreground")

    def test_capacity_downshift_drains_active_leases_without_revocation(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (2, 200)},
            quantum=100,
        )
        for index in range(2):
            self.assertTrue(
                coordinator.try_acquire(
                    request_id=f"active{index}",
                    job_id="job",
                    endpoint_id="gpu0",
                    estimated_work=100,
                )
            )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="waiting",
                job_id="job",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )

        snapshot = coordinator.update_capacity(
            "gpu0",
            request_limit=1,
            work_limit=100,
        )

        self.assertEqual(snapshot.active_requests, 2)
        self.assertEqual(snapshot.request_limit, 1)
        coordinator.release("active0", job_id="job")
        self.assertEqual(coordinator.snapshot("gpu0").active_requests, 1)
        self.assertEqual(coordinator.snapshot("gpu0").waiting_requests, 1)
        coordinator.release("active1", job_id="job")
        self.assertEqual(coordinator.snapshot("gpu0").active_requests, 1)
        self.assertEqual(coordinator.snapshot("gpu0").waiting_requests, 0)

    def test_vtc_selects_least_attained_service_and_corrects_actual_work(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (1, 100)},
            quantum=25,
            policy="vtc",
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="a0",
                job_id="a",
                endpoint_id="gpu0",
                estimated_work=50,
            )
        )
        for request_id, job_id in (("a1", "a"), ("b0", "b")):
            self.assertFalse(
                coordinator.try_acquire(
                    request_id=request_id,
                    job_id=job_id,
                    endpoint_id="gpu0",
                    estimated_work=50,
                )
            )

        coordinator.release("a0", job_id="a", actual_work=80)

        self.assertTrue(
            coordinator.try_acquire(
                request_id="b0",
                job_id="b",
                endpoint_id="gpu0",
                estimated_work=50,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="a1",
                job_id="a",
                endpoint_id="gpu0",
                estimated_work=50,
            )
        )
        self.assertEqual(
            coordinator.snapshot("gpu0").attained_service_by_job,
            (("a", 80), ("b", 100)),
        )

    def test_vtc_reactivated_job_is_lifted_to_active_floor(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (1, 100)},
            quantum=25,
            policy="vtc",
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="old",
                job_id="idle",
                endpoint_id="gpu0",
                estimated_work=50,
            )
        )
        coordinator.release("old", job_id="idle", actual_work=50)
        self.assertTrue(
            coordinator.try_acquire(
                request_id="busy0",
                job_id="busy",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="idle1",
                job_id="idle",
                endpoint_id="gpu0",
                estimated_work=50,
            )
        )

        self.assertEqual(
            coordinator.snapshot("gpu0").attained_service_by_job,
            (("busy", 100), ("idle", 100)),
        )

    def test_vtc_reactivation_lift_uses_normalized_weighted_service(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (1, 200)},
            quantum=25,
            policy="vtc",
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="weighted0",
                job_id="weighted",
                endpoint_id="gpu0",
                estimated_work=100,
                weight=2,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="new0",
                job_id="new",
                endpoint_id="gpu0",
                estimated_work=50,
                weight=1,
            )
        )

        self.assertEqual(
            coordinator.snapshot("gpu0").attained_service_by_job,
            (("new", 50), ("weighted", 100)),
        )

    def test_fifo_policy_preserves_global_waiter_order(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (1, 100)},
            quantum=100,
            policy="fifo",
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="active",
                job_id="a",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )
        for request_id, job_id in (("first", "b"), ("second", "a")):
            self.assertFalse(
                coordinator.try_acquire(
                    request_id=request_id,
                    job_id=job_id,
                    endpoint_id="gpu0",
                    estimated_work=100,
                )
            )

        coordinator.release("active", job_id="a")

        self.assertTrue(
            coordinator.try_acquire(
                request_id="first",
                job_id="b",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="second",
                job_id="a",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )

    def test_saor_reclaims_only_future_credit_for_new_active_job(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (2, 200)},
            quantum=100,
            policy="saor",
            saor_release_config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )
        for index in range(2):
            self.assertTrue(
                coordinator.try_acquire(
                    request_id=f"bulk-active-{index}",
                    job_id="bulk",
                    endpoint_id="gpu0",
                    estimated_work=100,
                )
            )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="bulk-waiting",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="foreground",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )

        coordinator.release("bulk-active-0", job_id="bulk", actual_work=100)

        snapshot = coordinator.snapshot("gpu0")
        self.assertEqual(
            snapshot.active_by_job,
            (("bulk", 1), ("foreground", 1)),
        )
        self.assertEqual(snapshot.waiting_by_job, (("bulk", 1),))

    def test_saor_single_job_borrows_and_reborrows_full_envelope(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (2, 200)},
            quantum=100,
            policy="saor",
            saor_release_config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )
        for request_id in ("bulk-0", "bulk-1"):
            self.assertTrue(
                coordinator.try_acquire(
                    request_id=request_id,
                    job_id="bulk",
                    endpoint_id="gpu0",
                    estimated_work=100,
                )
            )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="foreground",
                job_id="foreground",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )
        coordinator.release("bulk-0", job_id="bulk", actual_work=100)
        coordinator.release("bulk-1", job_id="bulk", actual_work=100)
        coordinator.release("foreground", job_id="foreground", actual_work=100)

        for request_id in ("bulk-2", "bulk-3"):
            self.assertTrue(
                coordinator.try_acquire(
                    request_id=request_id,
                    job_id="bulk",
                    endpoint_id="gpu0",
                    estimated_work=100,
                )
            )
        self.assertEqual(
            coordinator.snapshot("gpu0").active_by_job,
            (("bulk", 2),),
        )

    def test_single_job_borrows_all_idle_endpoint_capacity(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (3, 300)},
            quantum=100,
        )

        granted = [
            coordinator.try_acquire(
                request_id=f"r{index}",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=100,
            )
            for index in range(3)
        ]

        self.assertEqual(granted, [True, True, True])
        snapshot = coordinator.snapshot("gpu0")
        self.assertEqual(snapshot.active_requests, 3)
        self.assertEqual(snapshot.active_work, 300)
        self.assertEqual(snapshot.active_by_job, (("bulk", 3),))
        self.assertEqual(snapshot.active_work_by_job, (("bulk", 300),))
        self.assertEqual(snapshot.max_active_requests_seen, 3)
        self.assertEqual(snapshot.max_active_work_seen, 300)
        self.assertEqual(snapshot.granted_requests_by_job, (("bulk", 3),))
        self.assertEqual(snapshot.granted_work_by_job, (("bulk", 300),))

    def test_waiting_jobs_receive_credit_after_completion(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (1, 100)},
            quantum=100,
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="active",
                job_id="bulk",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="interactive",
                job_id="interactive",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )

        coordinator.release("active", job_id="bulk")

        self.assertTrue(
            coordinator.try_acquire(
                request_id="interactive",
                job_id="interactive",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )
        self.assertEqual(
            coordinator.snapshot("gpu0").active_by_job,
            (("interactive", 1),),
        )
        snapshot = coordinator.snapshot("gpu0")
        self.assertEqual(
            snapshot.granted_requests_by_job,
            (("bulk", 1), ("interactive", 1)),
        )
        self.assertEqual(
            snapshot.granted_work_by_job,
            (("bulk", 100), ("interactive", 100)),
        )

    def test_idempotent_polling_does_not_double_count_grants(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (1, 100)},
            quantum=100,
        )
        for _ in range(3):
            self.assertTrue(
                coordinator.try_acquire(
                    request_id="same",
                    job_id="job",
                    endpoint_id="gpu0",
                    estimated_work=100,
                )
            )

        snapshot = coordinator.snapshot("gpu0")
        self.assertEqual(snapshot.granted_requests_by_job, (("job", 1),))
        self.assertEqual(snapshot.granted_work_by_job, (("job", 100),))

    def test_snapshot_tracks_waiting_request_and_work_by_job(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (1, 100)},
            quantum=100,
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="active",
                job_id="a",
                endpoint_id="gpu0",
                estimated_work=100,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="waiting",
                job_id="b",
                endpoint_id="gpu0",
                estimated_work=80,
            )
        )

        snapshot = coordinator.snapshot("gpu0")
        self.assertEqual(snapshot.waiting_requests, 1)
        self.assertEqual(snapshot.waiting_work, 80)
        self.assertEqual(snapshot.waiting_by_job, (("b", 1),))
        self.assertEqual(snapshot.waiting_work_by_job, (("b", 80),))
        self.assertEqual(snapshot.waiting_head_work_by_job, (("b", 80),))

    def test_work_cap_blocks_large_head_until_capacity_is_released(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (4, 100)},
            quantum=25,
        )
        self.assertTrue(
            coordinator.try_acquire(
                request_id="small",
                job_id="a",
                endpoint_id="gpu0",
                estimated_work=75,
            )
        )
        self.assertFalse(
            coordinator.try_acquire(
                request_id="large",
                job_id="b",
                endpoint_id="gpu0",
                estimated_work=80,
            )
        )

        coordinator.release("small", job_id="a")

        self.assertTrue(
            coordinator.try_acquire(
                request_id="large",
                job_id="b",
                endpoint_id="gpu0",
                estimated_work=80,
            )
        )

    def test_single_request_cannot_exceed_endpoint_work_limit(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (4, 100)},
            quantum=25,
        )

        with self.assertRaisesRegex(
            ValueError,
            "exceeds endpoint work limit",
        ):
            coordinator.try_acquire(
                request_id="oversized",
                job_id="job",
                endpoint_id="gpu0",
                estimated_work=101,
            )

        snapshot = coordinator.snapshot("gpu0")
        self.assertEqual(snapshot.active_requests, 0)
        self.assertEqual(snapshot.waiting_requests, 0)

    def test_job_weight_must_remain_stable(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (1, 100)},
            quantum=100,
        )
        coordinator.try_acquire(
            request_id="r1",
            job_id="job",
            endpoint_id="gpu0",
            estimated_work=100,
            weight=1,
        )

        with self.assertRaisesRegex(ValueError, "stable weight"):
            coordinator.try_acquire(
                request_id="r2",
                job_id="job",
                endpoint_id="gpu0",
                estimated_work=100,
                weight=2,
            )

    def test_request_ids_are_isolated_between_jobs(self) -> None:
        coordinator = FairEndpointCreditCoordinator(
            {"gpu0": (2, 200)},
            quantum=100,
        )

        for job_id in ("job-a", "job-b"):
            self.assertTrue(
                coordinator.try_acquire(
                    request_id="batch-0",
                    job_id=job_id,
                    endpoint_id="gpu0",
                    estimated_work=100,
                )
            )

        self.assertEqual(coordinator.snapshot("gpu0").active_requests, 2)
        coordinator.release("batch-0", job_id="job-a")
        self.assertEqual(coordinator.snapshot("gpu0").active_requests, 1)


if __name__ == "__main__":
    unittest.main()
