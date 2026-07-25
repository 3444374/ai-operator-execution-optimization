from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.admission import StaticAdmissionController  # noqa: E402
from src.scheduling.models import (  # noqa: E402
    BatchRequest,
    EndpointSnapshot,
    PayloadEnvelope,
    SubmissionCompletion,
    TopologySnapshot,
)
from src.scheduling.routing import RoundRobinEndpointRouter  # noqa: E402
from src.scheduling.scheduler import SynchronousScheduler  # noqa: E402


def envelope(index: int) -> PayloadEnvelope:
    request = BatchRequest(
        f"r{index}",
        "j1",
        "ai_complete",
        1,
        10,
        5,
        "",
        float(index),
        float(index),
        f"p{index}",
    )
    return PayloadEnvelope(request, f"payload-{index}")


def topology() -> TopologySnapshot:
    endpoints = tuple(
        EndpointSnapshot(
            endpoint_id,
            f"http://localhost/{endpoint_id}",
            "default",
            "0",
            True,
            0,
            0,
            0.0,
            1.0,
        )
        for endpoint_id in ("e1", "e2")
    )
    return TopologySnapshot(endpoints, 1.0)


class FakeSubmissionAdapter:
    def __init__(self) -> None:
        self.submitted: list[tuple[str, str]] = []

    def submit(self, envelope: PayloadEnvelope, endpoint_id: str) -> object:
        handle = (envelope.request.request_id, endpoint_id)
        self.submitted.append(handle)
        return handle

    def wait_one(self, pending: list[tuple[object, PayloadEnvelope]]) -> tuple[object, SubmissionCompletion]:
        handle, pending_envelope = pending[0]
        completion = SubmissionCompletion(
            request_id=pending_envelope.request.request_id,
            status="completed",
            result=pending_envelope.payload,
        )
        return handle, completion


class SchedulerTests(unittest.TestCase):
    def test_scheduler_completes_each_request_once_with_bounded_inflight(self) -> None:
        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=2),
            router=RoundRobinEndpointRouter(),
            adapter=adapter,
            pool_id="default",
        )

        result = scheduler.run([envelope(index) for index in range(5)], topology())

        self.assertEqual([item.request_id for item in result.completions], ["r0", "r1", "r2", "r3", "r4"])
        self.assertEqual(len(set(item.request_id for item in result.completions)), 5)
        self.assertEqual(result.max_inflight_seen, 2)
        self.assertEqual(
            adapter.submitted,
            [("r0", "e1"), ("r1", "e2"), ("r2", "e1"), ("r3", "e2"), ("r4", "e1")],
        )

    def test_scheduler_preserves_failed_completion_without_retry(self) -> None:
        class FailingAdapter(FakeSubmissionAdapter):
            def wait_one(self, pending):
                handle, pending_envelope = pending[0]
                return handle, SubmissionCompletion(
                    request_id=pending_envelope.request.request_id,
                    status="failed",
                    error="synthetic failure",
                )

        scheduler = SynchronousScheduler(
            StaticAdmissionController(1),
            RoundRobinEndpointRouter(),
            FailingAdapter(),
            "default",
        )

        result = scheduler.run([envelope(0)], topology())

        self.assertEqual(result.completions[0].status, "failed")
        self.assertEqual(result.completions[0].error, "synthetic failure")


if __name__ == "__main__":
    unittest.main()
