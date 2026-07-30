from __future__ import annotations

import sys
import threading
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
    RoutingDecision,
    SubmissionCompletion,
    TopologySnapshot,
)
from src.scheduling.routing import (  # noqa: E402
    LeastQueuedEndpointRouter,
    PinnedEndpointRouter,
    RequestPoolRouter,
    RoundRobinEndpointRouter,
)
from src.scheduling.scheduler import SynchronousScheduler  # noqa: E402
from src.scheduling.shared_credit import (  # noqa: E402
    FairEndpointCreditCoordinator,
)


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
    def test_collects_ready_completion_while_source_waits_for_next_arrival(
        self,
    ) -> None:
        class PollingAdapter(FakeSubmissionAdapter):
            def __init__(self) -> None:
                super().__init__()
                self.first_collected = threading.Event()

            def poll_one(self, pending):
                if not pending:
                    return None
                collected = super().wait_one(pending)
                self.first_collected.set()
                return collected

            def wait_one(self, pending):
                collected = super().wait_one(pending)
                self.first_collected.set()
                return collected

        adapter = PollingAdapter()
        collected_before_second_arrival = []

        def delayed_envelopes():
            yield envelope(0)
            collected_before_second_arrival.append(
                adapter.first_collected.wait(timeout=0.1)
            )
            yield envelope(1)

        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=8),
            router=RoundRobinEndpointRouter(),
            adapter=adapter,
            pool_id="default",
        )

        result = scheduler.run(delayed_envelopes(), topology())

        self.assertEqual(collected_before_second_arrival, [True])
        self.assertEqual(
            [item.request_id for item in result.completions],
            ["r0", "r1"],
        )

    def test_per_endpoint_limit_prevents_one_endpoint_from_consuming_global_k(
        self,
    ) -> None:
        class FirstHealthyRouter:
            def route(self, request, topology, pool_id):
                del request
                endpoint = next(
                    item
                    for item in topology.endpoints
                    if (
                        item.healthy
                        and item.available
                        and item.pool_id == pool_id
                    )
                )
                return RoutingDecision(endpoint.endpoint_id, pool_id, "first")

        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=4),
            router=FirstHealthyRouter(),
            adapter=adapter,
            pool_id="default",
            per_endpoint_limit=2,
        )

        result = scheduler.run(
            [envelope(index) for index in range(6)],
            topology(),
        )

        self.assertEqual(result.max_inflight_seen, 4)
        self.assertEqual(
            adapter.submitted[:4],
            [("r0", "e1"), ("r1", "e1"), ("r2", "e2"), ("r3", "e2")],
        )

    def test_per_endpoint_admission_applies_independent_windows(
        self,
    ) -> None:
        class FirstHealthyRouter:
            def route(self, request, topology, pool_id):
                del request
                endpoint = next(
                    item
                    for item in topology.endpoints
                    if item.healthy and item.available
                )
                return RoutingDecision(endpoint.endpoint_id, pool_id, "first")

        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=4),
            router=FirstHealthyRouter(),
            adapter=adapter,
            pool_id="default",
            per_endpoint_admission={
                "e1": StaticAdmissionController(limit=1),
                "e2": StaticAdmissionController(limit=2),
            },
        )

        result = scheduler.run(
            [envelope(index) for index in range(5)],
            topology(),
        )

        self.assertEqual(result.max_inflight_seen, 3)
        self.assertEqual(
            adapter.submitted[:3],
            [("r0", "e1"), ("r1", "e2"), ("r2", "e2")],
        )

    def test_per_endpoint_admission_requires_complete_topology_mapping(
        self,
    ) -> None:
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=2),
            router=RoundRobinEndpointRouter(),
            adapter=FakeSubmissionAdapter(),
            pool_id="default",
            per_endpoint_admission={
                "e1": StaticAdmissionController(limit=1),
            },
        )

        with self.assertRaisesRegex(ValueError, "exactly one policy"):
            scheduler.run([envelope(0)], topology())

    def test_per_endpoint_limit_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be positive"):
            SynchronousScheduler(
                admission=StaticAdmissionController(limit=2),
                router=RoundRobinEndpointRouter(),
                adapter=FakeSubmissionAdapter(),
                pool_id="default",
                per_endpoint_limit=0,
            )

    def test_active_work_cap_releases_credit_on_each_completion(self) -> None:
        class FirstHealthyRouter:
            def route(self, request, topology, pool_id):
                del request
                endpoint = next(
                    item
                    for item in topology.endpoints
                    if (
                        item.healthy
                        and item.available
                        and item.pool_id == pool_id
                    )
                )
                return RoutingDecision(endpoint.endpoint_id, pool_id, "first")

        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=8),
            router=FirstHealthyRouter(),
            adapter=adapter,
            pool_id="default",
            per_endpoint_work_limit=20,
        )

        result = scheduler.run(
            [envelope(index) for index in range(5)],
            topology(),
        )

        self.assertEqual(result.max_inflight_seen, 2)
        self.assertEqual(result.max_active_work_per_endpoint_seen, 15)
        self.assertEqual(
            adapter.submitted[:3],
            [("r0", "e1"), ("r1", "e2"), ("r2", "e1")],
        )

    def test_pinned_endpoint_waits_for_its_own_work_credit(self) -> None:
        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=8),
            router=PinnedEndpointRouter(),
            adapter=adapter,
            pool_id="default",
            per_endpoint_work_limit=20,
        )
        pinned = [
            PayloadEnvelope(
                BatchRequest(
                    request_id=f"r{index}",
                    job_id="j1",
                    operator="ai_complete",
                    row_count=1,
                    prompt_tokens=10,
                    estimated_output_tokens=5,
                    prefix_key="",
                    first_arrival_s=float(index),
                    oldest_arrival_s=float(index),
                    payload_id=f"p{index}",
                    preferred_endpoint_id="e1",
                ),
                f"payload-{index}",
            )
            for index in range(2)
        ]

        result = scheduler.run(pinned, topology())

        self.assertEqual(adapter.submitted, [("r0", "e1"), ("r1", "e1")])
        self.assertEqual(result.max_inflight_seen, 1)
        self.assertEqual(result.max_active_work_per_endpoint_seen, 15)

    def test_pinned_endpoint_keeps_its_pool_while_capacity_is_full(
        self,
    ) -> None:
        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=8),
            router=PinnedEndpointRouter(),
            adapter=adapter,
            pool_id="short",
            pool_router=RequestPoolRouter(long_request_tokens=100),
            per_endpoint_limit=1,
        )
        multi_pool_topology = TopologySnapshot(
            (
                EndpointSnapshot(
                    "long-1",
                    "http://localhost/long-1",
                    "long",
                    "0",
                    True,
                    0,
                    0,
                    0.0,
                    1.0,
                ),
                EndpointSnapshot(
                    "short-1",
                    "http://localhost/short-1",
                    "short",
                    "1",
                    True,
                    0,
                    0,
                    0.0,
                    1.0,
                ),
            ),
            1.0,
        )
        pinned = [
            PayloadEnvelope(
                BatchRequest(
                    request_id=f"r{index}",
                    job_id="j1",
                    operator="ai_complete",
                    row_count=1,
                    prompt_tokens=10,
                    estimated_output_tokens=5,
                    prefix_key="",
                    first_arrival_s=float(index),
                    oldest_arrival_s=float(index),
                    payload_id=f"p{index}",
                    preferred_endpoint_id="long-1",
                ),
                f"payload-{index}",
            )
            for index in range(2)
        ]

        scheduler.run(pinned, multi_pool_topology)

        self.assertEqual(
            adapter.submitted,
            [("r0", "long-1"), ("r1", "long-1")],
        )

    def test_fixed_pool_waits_when_only_another_pool_has_capacity(
        self,
    ) -> None:
        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=8),
            router=RoundRobinEndpointRouter(),
            adapter=adapter,
            pool_id="default",
            per_endpoint_limit=1,
        )
        multi_pool_topology = TopologySnapshot(
            (
                EndpointSnapshot(
                    "default-1",
                    "http://localhost/default-1",
                    "default",
                    "0",
                    True,
                    0,
                    0,
                    0.0,
                    1.0,
                ),
                EndpointSnapshot(
                    "other-1",
                    "http://localhost/other-1",
                    "other",
                    "1",
                    True,
                    0,
                    0,
                    0.0,
                    1.0,
                ),
            ),
            1.0,
        )

        scheduler.run([envelope(0), envelope(1)], multi_pool_topology)

        self.assertEqual(
            adapter.submitted,
            [("r0", "default-1"), ("r1", "default-1")],
        )

    def test_shared_credit_rejects_oversized_request_before_submit(
        self,
    ) -> None:
        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=1),
            router=RoundRobinEndpointRouter(),
            adapter=adapter,
            pool_id="default",
            per_endpoint_work_limit=100,
            shared_credit=FairEndpointCreditCoordinator(
                {"e1": (1, 100), "e2": (1, 100)},
                quantum=100,
            ),
        )
        oversized = PayloadEnvelope(
            BatchRequest(
                request_id="oversized",
                job_id="j1",
                operator="ai_complete",
                row_count=1,
                prompt_tokens=101,
                estimated_output_tokens=0,
                prefix_key="",
                first_arrival_s=0.0,
                oldest_arrival_s=0.0,
                payload_id="oversized-payload",
            ),
            "payload",
        )

        with self.assertRaisesRegex(
            ValueError,
            "exceeds endpoint work limit",
        ):
            scheduler.run([oversized], topology())

        self.assertEqual(adapter.submitted, [])

    def test_observed_endpoint_work_counts_toward_work_cap(self) -> None:
        observed = TopologySnapshot(
            (
                EndpointSnapshot(
                    "e1",
                    "http://localhost/e1",
                    "default",
                    "0",
                    True,
                    0,
                    0,
                    0.0,
                    1.0,
                    estimated_active_work=18,
                ),
            ),
            1.0,
        )

        capped = SynchronousScheduler._topology_with_local_inflight(
            observed,
            {},
            per_endpoint_work_limit=20,
            request_work=5,
        )

        self.assertTrue(capped.endpoints[0].healthy)
        self.assertFalse(capped.endpoints[0].available)
        self.assertEqual(capped.endpoints[0].estimated_active_work, 18)

    def test_shared_credit_is_acquired_before_submit_and_released_after(self) -> None:
        class DelayedSharedCredit:
            def __init__(self) -> None:
                self.attempts = 0
                self.released: list[tuple[str, str]] = []

            def try_acquire(self, **_kwargs):
                self.attempts += 1
                return self.attempts >= 2

            def release(self, request_id, *, job_id):
                self.released.append((job_id, request_id))

        shared = DelayedSharedCredit()
        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=1),
            router=RoundRobinEndpointRouter(),
            adapter=adapter,
            pool_id="default",
            shared_credit=shared,
            shared_credit_poll_s=0.0001,
        )

        result = scheduler.run([envelope(0)], topology())

        self.assertEqual(result.operator_invocations, 1)
        self.assertEqual(shared.attempts, 2)
        self.assertEqual(shared.released, [("j1", "r0")])

    def test_failed_completion_releases_shared_credit_once(self) -> None:
        class RecordingSharedCredit:
            def __init__(self) -> None:
                self.acquired: list[tuple[str, str]] = []
                self.released: list[tuple[str, str]] = []

            def try_acquire(self, *, request_id, job_id, **_kwargs):
                self.acquired.append((job_id, request_id))
                return True

            def release(self, request_id, *, job_id):
                self.released.append((job_id, request_id))

        shared = RecordingSharedCredit()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=1),
            router=RoundRobinEndpointRouter(),
            adapter=FailingAdapter(),
            pool_id="default",
            shared_credit=shared,
        )

        result = scheduler.run([envelope(0)], topology())

        self.assertEqual(shared.acquired, [("j1", "r0")])
        self.assertEqual(shared.released, [("j1", "r0")])
        self.assertEqual(result.completions[0].status, "failed")
        self.assertEqual(result.submission_events[0].status, "failed")

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
