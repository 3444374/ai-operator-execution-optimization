from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.shared_credit import (  # noqa: E402
    FairEndpointCreditCoordinator,
)


class SharedCreditCoordinatorTests(unittest.TestCase):
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
