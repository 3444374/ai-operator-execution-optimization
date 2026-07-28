from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.admission import StaticAdmissionController  # noqa: E402
from src.scheduling.models import (  # noqa: E402
    AdmissionDecision,
    BatchRequest,
    CollectedSubmission,
    EndpointSnapshot,
    PayloadEnvelope,
    SubmissionCompletion,
    TopologySnapshot,
)
from src.scheduling.routing import (  # noqa: E402
    LeastQueuedEndpointRouter,
    RequestPoolRouter,
    RoundRobinEndpointRouter,
)
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

    def wait_one(self, pending: list[tuple[object, PayloadEnvelope]]) -> CollectedSubmission:
        handle, pending_envelope = pending[0]
        return CollectedSubmission(
            handle=handle,
            completion=SubmissionCompletion(
                request_id=pending_envelope.request.request_id,
                status="completed",
                result=pending_envelope.payload,
            ),
            wait_s=0.1,
            result_s=0.05,
        )


class FailingAdapter(FakeSubmissionAdapter):
    def wait_one(self, pending):
        handle, pending_envelope = pending[0]
        return CollectedSubmission(
            handle=handle,
            completion=SubmissionCompletion(
                request_id=pending_envelope.request.request_id,
                status="failed",
                error="synthetic failure",
            ),
            wait_s=0.0,
            result_s=0.0,
        )


class SequenceClock:
    def __init__(self, values: list[float]) -> None:
        self._values = iter(values)

    def __call__(self) -> float:
        return next(self._values)


class SchedulerTests(unittest.TestCase):
    def test_scheduler_fails_when_initial_admission_cannot_progress(self) -> None:
        class AlwaysDenyAdmission:
            limit = 1

            def decide(
                self,
                inflight: int,
                *,
                hol_age_s: float | None = None,
            ) -> AdmissionDecision:
                del inflight, hol_age_s
                return AdmissionDecision(
                    False,
                    self.limit,
                    "hold",
                    "synthetic",
                )

        scheduler = SynchronousScheduler(
            AlwaysDenyAdmission(),
            RoundRobinEndpointRouter(),
            FakeSubmissionAdapter(),
            "default",
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "no in-flight submission",
        ):
            scheduler.run([envelope(0)], topology())

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
        self.assertEqual(result.operator_invocations, 5)
        self.assertEqual(result.max_inflight_seen, 2)
        self.assertEqual(result.applied_limit, 2)
        self.assertAlmostEqual(result.bounded_wait_s, 0.3)
        self.assertAlmostEqual(result.avg_bounded_wait_s, 0.1)
        self.assertAlmostEqual(result.fanin_s, 0.25)
        self.assertGreaterEqual(result.submit_s, 0.0)
        self.assertEqual(
            adapter.submitted,
            [("r0", "e1"), ("r1", "e2"), ("r2", "e1"), ("r3", "e2"), ("r4", "e1")],
        )

    def test_scheduler_preserves_failed_completion_without_retry(self) -> None:
        scheduler = SynchronousScheduler(
            StaticAdmissionController(1),
            RoundRobinEndpointRouter(),
            FailingAdapter(),
            "default",
        )

        result = scheduler.run([envelope(0)], topology())

        self.assertEqual(result.completions[0].status, "failed")
        self.assertEqual(result.completions[0].error, "synthetic failure")

    def test_scheduler_records_submission_lifecycle_in_source_order(self) -> None:
        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=2),
            router=RoundRobinEndpointRouter(),
            adapter=adapter,
            pool_id="default",
            epoch_clock=SequenceClock([10.0, 10.5, 11.0, 20.0, 21.0]),
        )

        result = scheduler.run([envelope(0), envelope(1)], topology())

        self.assertEqual(
            [event.submission_id for event in result.submission_events],
            ["r0", "r1"],
        )
        self.assertEqual(
            [
                (
                    event.pool_id,
                    event.endpoint_id,
                    event.gpu_id,
                    event.submit_epoch_s,
                    event.completion_epoch_s,
                    event.status,
                )
                for event in result.submission_events
            ],
            [
                ("default", "e1", "0", 10.0, 20.0, "completed"),
                ("default", "e2", "0", 11.0, 21.0, "completed"),
            ],
        )

    def test_scheduler_records_failed_submission_without_retry(self) -> None:
        scheduler = SynchronousScheduler(
            StaticAdmissionController(1),
            RoundRobinEndpointRouter(),
            FailingAdapter(),
            "default",
            epoch_clock=SequenceClock([10.0, 20.0]),
        )

        result = scheduler.run([envelope(0)], topology())

        self.assertEqual(result.submission_events[0].status, "failed")
        self.assertEqual(
            result.submission_events[0].error,
            "synthetic failure",
        )

    def test_scheduler_normalizes_out_of_order_fanin_to_submission_order(self) -> None:
        class ReverseCompletionAdapter(FakeSubmissionAdapter):
            def wait_one(self, pending):
                handle, pending_envelope = pending[-1]
                return CollectedSubmission(
                    handle=handle,
                    completion=SubmissionCompletion(
                        request_id=pending_envelope.request.request_id,
                        status="completed",
                        result=pending_envelope.payload,
                    ),
                    wait_s=0.0,
                    result_s=0.0,
                )

        scheduler = SynchronousScheduler(
            StaticAdmissionController(3),
            RoundRobinEndpointRouter(),
            ReverseCompletionAdapter(),
            "default",
        )

        result = scheduler.run([envelope(index) for index in range(3)], topology())

        self.assertEqual(
            [item.request_id for item in result.completions],
            ["r0", "r1", "r2"],
        )

    def test_scheduler_rejects_completion_for_different_request(self) -> None:
        class WrongCompletionAdapter(FakeSubmissionAdapter):
            def wait_one(self, pending):
                collected = super().wait_one(pending)
                return CollectedSubmission(
                    collected.handle,
                    SubmissionCompletion("wrong", "completed"),
                    collected.wait_s,
                    collected.result_s,
                )

        scheduler = SynchronousScheduler(
            StaticAdmissionController(1),
            RoundRobinEndpointRouter(),
            WrongCompletionAdapter(),
            "default",
        )

        with self.assertRaisesRegex(RuntimeError, "completion request_id"):
            scheduler.run([envelope(0)], topology())

    def test_scheduler_composes_request_pool_and_endpoint_routing(self) -> None:
        adapter = FakeSubmissionAdapter()
        dynamic_topology = TopologySnapshot(
            (
                EndpointSnapshot("prefix-1", "ray://prefix", "prefix", "0", True, 0, 0, None, 1.0),
                EndpointSnapshot("long-1", "ray://long", "long", "0", True, 0, 0, None, 1.0),
                EndpointSnapshot("short-2", "ray://short-2", "short", "0", True, 1, 0, None, 1.0),
                EndpointSnapshot("short-1", "ray://short-1", "short", "0", True, 0, 0, None, 1.0),
            ),
            1.0,
        )
        requests = [
            PayloadEnvelope(
                BatchRequest(
                    "prefix-request",
                    "j1",
                    "ai_complete",
                    1,
                    200,
                    10,
                    "shared",
                    0.0,
                    0.0,
                    "prefix-payload",
                ),
                "prefix",
            ),
            PayloadEnvelope(
                BatchRequest(
                    "long-request",
                    "j1",
                    "ai_complete",
                    1,
                    100,
                    10,
                    "",
                    0.0,
                    0.0,
                    "long-payload",
                ),
                "long",
            ),
            PayloadEnvelope(
                BatchRequest(
                    "short-request",
                    "j1",
                    "ai_complete",
                    1,
                    10,
                    5,
                    "",
                    0.0,
                    0.0,
                    "short-payload",
                ),
                "short",
            ),
        ]
        scheduler = SynchronousScheduler(
            StaticAdmissionController(3),
            LeastQueuedEndpointRouter(),
            adapter,
            "short",
            pool_router=RequestPoolRouter(long_request_tokens=100),
        )

        scheduler.run(requests, dynamic_topology)

        self.assertEqual(
            adapter.submitted,
            [
                ("prefix-request", "prefix-1"),
                ("long-request", "long-1"),
                ("short-request", "short-1"),
            ],
        )

    def test_least_queued_tracks_scheduler_local_inflight(self) -> None:
        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            StaticAdmissionController(4),
            LeastQueuedEndpointRouter(),
            adapter,
            "default",
        )

        scheduler.run([envelope(index) for index in range(4)], topology())

        self.assertEqual(
            adapter.submitted,
            [
                ("r0", "e1"),
                ("r1", "e2"),
                ("r2", "e1"),
                ("r3", "e2"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
