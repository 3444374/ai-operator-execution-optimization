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
from src.scheduling.core.models import BatchRequest, PayloadEnvelope  # noqa: E402
from src.scheduling.submission_control.ordered_release import (  # noqa: E402
    OrderedReleaseCoordinator,
)
from src.scheduling.submission_control.saor import (  # noqa: E402
    SaorAction,
    SaorDecision,
    SaorReleaseCandidate,
)


def envelope(request_id: str, job_id: str, work: int) -> PayloadEnvelope:
    return PayloadEnvelope(
        BatchRequest(
            request_id=request_id,
            job_id=job_id,
            operator="ai_complete",
            row_count=1,
            prompt_tokens=0,
            estimated_output_tokens=0,
            prefix_key="",
            first_arrival_s=0.0,
            oldest_arrival_s=0.0,
            payload_id=f"payload-{request_id}",
            work_units=work,
            work_unit="work",
        ),
        f"payload-{request_id}",
    )


def decision(*releases: SaorReleaseCandidate) -> SaorDecision:
    action = SaorAction(
        "selected",
        "gpu0",
        CapacityArm(2, 200),
        tuple(releases),
    )
    return SaorDecision(action, 0.0, "test", ((action.action_id, 0.0),))


class OrderedReleaseCoordinatorTests(unittest.TestCase):
    def test_publish_preserves_job_head_and_assigns_monotonic_sequence(self) -> None:
        coordinator = OrderedReleaseCoordinator(("gpu0",))
        coordinator.enqueue(envelope("a0", "a", 100))
        coordinator.enqueue(envelope("a1", "a", 100))
        coordinator.enqueue(envelope("b0", "b", 100))

        first = coordinator.publish(
            decision(SaorReleaseCandidate("a0", "a", "gpu0", 100))
        )
        second = coordinator.publish(
            decision(SaorReleaseCandidate("b0", "b", "gpu0", 100))
        )

        self.assertEqual(first[0].release_seq, 0)
        self.assertEqual(second[0].release_seq, 1)
        self.assertEqual(
            [item.request_id for item in coordinator.ready_heads("gpu0")],
            ["a1"],
        )

    def test_publish_rejects_non_head_request_without_mutating_queue(self) -> None:
        coordinator = OrderedReleaseCoordinator(("gpu0",))
        coordinator.enqueue(envelope("a0", "a", 100))
        coordinator.enqueue(envelope("a1", "a", 100))

        with self.assertRaisesRegex(ValueError, "head"):
            coordinator.publish(
                decision(SaorReleaseCandidate("a1", "a", "gpu0", 100))
            )

        self.assertEqual(
            [item.request_id for item in coordinator.ready_heads("gpu0")],
            ["a0"],
        )

    def test_capacity_is_checked_before_state_mutation(self) -> None:
        coordinator = OrderedReleaseCoordinator(("gpu0",))
        coordinator.enqueue(envelope("a0", "a", 150))
        too_small = SaorAction(
            "small",
            "gpu0",
            CapacityArm(1, 100),
            (SaorReleaseCandidate("a0", "a", "gpu0", 150),),
        )

        with self.assertRaisesRegex(ValueError, "capacity"):
            coordinator.publish(
                SaorDecision(too_small, 0.0, "test", (("small", 0.0),))
            )

        self.assertEqual(coordinator.snapshot().active_requests, 0)
        self.assertEqual(coordinator.snapshot().ready_requests, 1)

    def test_completion_releases_capacity_and_reports_actual_work(self) -> None:
        coordinator = OrderedReleaseCoordinator(("gpu0",))
        coordinator.enqueue(envelope("a0", "a", 80))
        coordinator.publish(
            decision(SaorReleaseCandidate("a0", "a", "gpu0", 80))
        )

        released = coordinator.complete("a0", actual_work=120)

        self.assertEqual(released.request_id, "a0")
        self.assertEqual(coordinator.snapshot().active_requests, 0)
        self.assertEqual(coordinator.drain_completed_work(), {"a": 120})
        self.assertEqual(coordinator.drain_completed_work(), {})

    def test_invalid_completion_work_does_not_release_capacity(self) -> None:
        coordinator = OrderedReleaseCoordinator(("gpu0",))
        coordinator.enqueue(envelope("a0", "a", 80))
        coordinator.publish(
            decision(SaorReleaseCandidate("a0", "a", "gpu0", 80))
        )

        with self.assertRaisesRegex(ValueError, "actual_work"):
            coordinator.complete("a0", actual_work=0)

        self.assertEqual(coordinator.snapshot().active_requests, 1)
        self.assertEqual(coordinator.drain_completed_work(), {})

    def test_same_core_accepts_non_token_work(self) -> None:
        coordinator = OrderedReleaseCoordinator(("gpu0",))
        image = envelope("image0", "image-job", 224 * 224)
        coordinator.enqueue(image)

        head = coordinator.ready_heads("gpu0")[0]

        self.assertEqual(head.estimated_work, 224 * 224)
        self.assertEqual(head.job_id, "image-job")


if __name__ == "__main__":
    unittest.main()
