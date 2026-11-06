"""Concrete multi-job coordinator integration with the synchronous research consumer."""

import unittest
import time

from src.research.multitask.saor import SaorReleaseConfig
from src.research.multitask.shared_credit import FairEndpointCreditCoordinator
from src.scheduling.core.models import BatchRequest, PayloadEnvelope, CollectedSubmission, SubmissionCompletion
from src.research.multitask.semloom.scheduler import SharedCreditScheduler
from src.research.multitask.semloom.scheduler_types import (
    JobSchedulingContract, ReadyWindowConfig, SharedCreditConfig, SharedSchedulerConfig,
)
from src.scheduling.core.scheduler_types import SchedulerConfig, EndpointCapacityConfig
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.submission_control.admission import StaticAdmissionController
from tests.scheduling.test_scheduler import (
    FakeSubmissionAdapter, FailingAdapter, envelope, topology,
)


class SharedCreditSchedulerTests(unittest.TestCase):
    def test_shared_credit_rejects_oversized_request_before_submit(
        self,
    ) -> None:
        adapter = FakeSubmissionAdapter()
        scheduler = SharedCreditScheduler(
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


    def test_bounded_priority_requires_arrival_epoch_and_acquire_timeout(self) -> None:
        shared = FairEndpointCreditCoordinator(
            {"e1": (1, 100), "e2": (1, 100)},
            quantum=100,
            policy="saor_bounded_priority",
            saor_release_config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )
        with self.assertRaisesRegex(ValueError, "acquire timeout"):
            SharedCreditScheduler(
                StaticAdmissionController(1),
                RoundRobinEndpointRouter(),
                FakeSubmissionAdapter(),
                "default",
                shared_credit=shared,
                shared_credit_acquire_timeout_s=0.0,
            )
        scheduler = SharedCreditScheduler(
            StaticAdmissionController(1),
            RoundRobinEndpointRouter(),
            FakeSubmissionAdapter(),
            "default",
            shared_credit=shared,
            job_priority=1,
            job_slo_target_s=30.0,
            job_priority_window_s=30.0,
            shared_credit_acquire_timeout_s=1.0,
        )

        with self.assertRaisesRegex(ValueError, "arrival epoch"):
            scheduler.run([envelope(0)], topology())


    def test_shared_credit_is_acquired_before_submit_and_released_after(self) -> None:
        class DelayedSharedCredit:
            def __init__(self) -> None:
                self.attempts = 0
                self.released: list[tuple[str, str]] = []

            def try_acquire(self, **_kwargs):
                self.attempts += 1
                return self.attempts >= 2

            def release(self, request_id, *, job_id, actual_work=None):
                self.released.append((job_id, request_id, actual_work))

        shared = DelayedSharedCredit()
        adapter = FakeSubmissionAdapter()
        scheduler = SharedCreditScheduler(
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
        self.assertEqual(shared.released, [("j1", "r0", None)])


    def test_shared_ready_window_registers_multiple_candidates_before_waiting(
        self,
    ) -> None:
        class TwoCandidateSharedCredit:
            def __init__(self) -> None:
                self.registered: list[str] = []
                self.released: list[str] = []

            def try_acquire(self, **kwargs):
                request_id = kwargs["request_id"]
                if request_id not in self.registered:
                    self.registered.append(request_id)
                return len(self.registered) >= 2

            def release(self, request_id, *, job_id, actual_work=None):
                del job_id, actual_work
                self.released.append(request_id)

        shared = TwoCandidateSharedCredit()

        class InspectingAdapter(FakeSubmissionAdapter):
            def submit(self, item, endpoint_id):
                self.assert_ready_set_visible()
                return super().submit(item, endpoint_id)

            @staticmethod
            def assert_ready_set_visible():
                if shared.registered != ["r0", "r1"]:
                    raise AssertionError(
                        "both ready candidates must be registered before submit"
                    )

        adapter = InspectingAdapter()
        scheduler = SharedCreditScheduler(
            admission=StaticAdmissionController(limit=2),
            router=RoundRobinEndpointRouter(),
            adapter=adapter,
            pool_id="default",
            per_endpoint_work_limit=100,
            shared_credit=shared,
            shared_credit_poll_s=0.0001,
            shared_credit_acquire_timeout_s=0.01,
            shared_ready_request_limit=2,
            shared_ready_work_limit=100,
        )

        result = scheduler.run([envelope(0), envelope(1)], topology())

        self.assertEqual(
            [item.request_id for item in result.completions],
            ["r0", "r1"],
        )
        self.assertEqual(adapter.submitted, [("r0", "e1"), ("r1", "e2")])
        self.assertEqual(shared.released, ["r0", "r1"])
        self.assertEqual(result.max_ready_requests_seen, 2)
        first_event = result.submission_events[0]
        self.assertIsNotNone(first_event.ready_epoch_s)
        self.assertIsNotNone(first_event.credit_registered_epoch_s)
        self.assertIsNotNone(first_event.credit_granted_epoch_s)
        self.assertLessEqual(
            first_event.ready_epoch_s,
            first_event.credit_registered_epoch_s,
        )
        self.assertLessEqual(
            first_event.credit_registered_epoch_s,
            first_event.credit_granted_epoch_s,
        )
        self.assertLessEqual(
            first_event.credit_granted_epoch_s,
            first_event.submit_epoch_s,
        )


    def test_shared_ready_window_rejects_unbounded_work(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires a work limit"):
            SharedCreditScheduler(
                admission=StaticAdmissionController(limit=2),
                router=RoundRobinEndpointRouter(),
                adapter=FakeSubmissionAdapter(),
                pool_id="default",
                shared_credit=object(),
                shared_ready_request_limit=2,
            )


    def test_shared_ready_window_cancels_all_waiters_on_timeout(self) -> None:
        class AlwaysDenySharedCredit:
            def __init__(self) -> None:
                self.registered: list[str] = []
                self.cancelled: list[str] = []

            def try_acquire(self, **kwargs):
                request_id = kwargs["request_id"]
                if request_id not in self.registered:
                    self.registered.append(request_id)
                return False

            def release(self, request_id, *, job_id, actual_work=None):
                raise AssertionError("denied requests must not be released")

            def cancel_waiter(self, request_id, *, job_id):
                del job_id
                self.cancelled.append(request_id)
                return True

        shared = AlwaysDenySharedCredit()
        scheduler = SharedCreditScheduler(
            admission=StaticAdmissionController(limit=2),
            router=RoundRobinEndpointRouter(),
            adapter=FakeSubmissionAdapter(),
            pool_id="default",
            shared_credit=shared,
            shared_credit_poll_s=0.0001,
            shared_credit_acquire_timeout_s=0.001,
            shared_ready_request_limit=2,
            shared_ready_work_limit=100,
        )

        with self.assertRaisesRegex(TimeoutError, "shared credit acquire"):
            scheduler.run([envelope(0), envelope(1)], topology())

        self.assertEqual(shared.registered, ["r0", "r1"])
        self.assertEqual(shared.cancelled, ["r0", "r1"])


    def test_shared_ready_window_does_not_release_submitted_credit_on_abort(
        self,
    ) -> None:
        class OneGrantSharedCredit:
            def __init__(self) -> None:
                self.released: list[str] = []
                self.cancelled: list[str] = []

            def try_acquire(self, **kwargs):
                if kwargs["request_id"] == "r0":
                    return True
                time.sleep(0.002)
                return False

            def release(self, request_id, *, job_id, actual_work=None):
                del job_id, actual_work
                self.released.append(request_id)

            def cancel_waiter(self, request_id, *, job_id):
                del job_id
                self.cancelled.append(request_id)
                return True

        shared = OneGrantSharedCredit()
        adapter = FakeSubmissionAdapter()
        scheduler = SharedCreditScheduler(
            admission=StaticAdmissionController(limit=2),
            router=RoundRobinEndpointRouter(),
            adapter=adapter,
            pool_id="default",
            shared_credit=shared,
            shared_credit_poll_s=0.0001,
            shared_credit_acquire_timeout_s=0.001,
            shared_ready_request_limit=2,
            shared_ready_work_limit=100,
        )

        with self.assertRaisesRegex(TimeoutError, "shared credit acquire"):
            scheduler.run([envelope(0), envelope(1)], topology())

        self.assertEqual(adapter.submitted, [("r0", "e1")])
        self.assertEqual(shared.cancelled, ["r1"])
        self.assertEqual(shared.released, [])


    def test_shared_credit_uses_epoch_arrival_for_remaining_slo_budget(self) -> None:
        class RecordingSharedCredit:
            def __init__(self) -> None:
                self.acquires = []

            def try_acquire(self, **kwargs):
                self.acquires.append(kwargs)
                return True

            def release(self, request_id, *, job_id, actual_work=None):
                del request_id, job_id, actual_work

        shared = RecordingSharedCredit()
        request = BatchRequest(
            request_id="epoch-request",
            job_id="foreground",
            operator="ai_complete",
            row_count=1,
            prompt_tokens=10,
            estimated_output_tokens=5,
            prefix_key="",
            first_arrival_s=7.0,
            oldest_arrival_s=7.0,
            payload_id="epoch-payload",
            oldest_arrival_epoch_s=100.0,
        )
        scheduler = SharedCreditScheduler(
            admission=StaticAdmissionController(limit=1),
            router=RoundRobinEndpointRouter(),
            adapter=FakeSubmissionAdapter(),
            pool_id="default",
            epoch_clock=lambda: 110.0,
            shared_credit=shared,
            job_priority=1,
            job_slo_target_s=30.0,
            job_priority_window_s=30.0,
            job_fairness_debt_cap=25.0,
            shared_credit_acquire_timeout_s=1.0,
        )

        scheduler.run([PayloadEnvelope(request, "payload")], topology())

        self.assertEqual(request.oldest_arrival_s, 7.0)
        self.assertEqual(shared.acquires[0]["slo_budget_remaining_s"], 20.0)
        self.assertEqual(shared.acquires[0]["priority_window_s"], 30.0)
        self.assertEqual(shared.acquires[0]["fairness_debt_cap"], 25.0)


    def test_shared_credit_preserves_negative_slo_slack(self) -> None:
        class RecordingSharedCredit:
            def __init__(self) -> None:
                self.remaining = None

            def try_acquire(self, **kwargs):
                self.remaining = kwargs["slo_budget_remaining_s"]
                return True

            def release(self, request_id, *, job_id, actual_work=None):
                del request_id, job_id, actual_work

        shared = RecordingSharedCredit()
        request = BatchRequest(
            "late",
            "foreground",
            "ai_complete",
            1,
            10,
            5,
            "",
            7.0,
            7.0,
            "late-payload",
            oldest_arrival_epoch_s=100.0,
        )
        scheduler = SharedCreditScheduler(
            StaticAdmissionController(1),
            RoundRobinEndpointRouter(),
            FakeSubmissionAdapter(),
            "default",
            epoch_clock=lambda: 145.0,
            shared_credit=shared,
            job_priority=1,
            job_slo_target_s=30.0,
            job_priority_window_s=30.0,
            shared_credit_acquire_timeout_s=1.0,
        )

        scheduler.run([PayloadEnvelope(request, "payload")], topology())

        self.assertEqual(shared.remaining, -15.0)


    def test_shared_credit_wait_times_out_without_silent_fallback(self) -> None:
        class AlwaysDenySharedCredit:
            def __init__(self):
                self.cancelled = []

            def try_acquire(self, **_kwargs):
                return False

            def release(self, request_id, *, job_id, actual_work=None):
                del request_id, job_id, actual_work

            def cancel_waiter(self, request_id, *, job_id):
                self.cancelled.append((job_id, request_id))
                return True

        shared = AlwaysDenySharedCredit()
        scheduler = SharedCreditScheduler(
            StaticAdmissionController(1),
            RoundRobinEndpointRouter(),
            FakeSubmissionAdapter(),
            "default",
            shared_credit=shared,
            shared_credit_poll_s=0.0001,
            shared_credit_acquire_timeout_s=0.001,
        )

        with self.assertRaisesRegex(TimeoutError, "shared credit acquire"):
            scheduler.run([envelope(0)], topology())
        self.assertEqual(shared.cancelled, [("j1", "r0")])


    def test_shared_credit_release_receives_actual_completed_token_work(self) -> None:
        class TokenResultAdapter(FakeSubmissionAdapter):
            def wait_one(self, pending):
                handle, pending_envelope = pending[0]
                return CollectedSubmission(
                    handle=handle,
                    completion=SubmissionCompletion(
                        request_id=pending_envelope.request.request_id,
                        status="completed",
                        result={"token_count": 37},
                    ),
                    wait_s=0.0,
                    result_s=0.0,
                )

        class RecordingSharedCredit:
            def __init__(self) -> None:
                self.released = []

            def try_acquire(self, **_kwargs):
                return True

            def release(self, request_id, *, job_id, actual_work=None):
                self.released.append((job_id, request_id, actual_work))

        shared = RecordingSharedCredit()
        scheduler = SharedCreditScheduler(
            admission=StaticAdmissionController(limit=1),
            router=RoundRobinEndpointRouter(),
            adapter=TokenResultAdapter(),
            pool_id="default",
            shared_credit=shared,
            actual_work_extractor=lambda completion: completion.result.get(
                "token_count"
            ),
        )

        scheduler.run([envelope(0)], topology())

        self.assertEqual(shared.released, [("j1", "r0", 37)])


    def test_failed_completion_releases_shared_credit_once(self) -> None:
        class RecordingSharedCredit:
            def __init__(self) -> None:
                self.acquired: list[tuple[str, str]] = []
                self.released: list[tuple[str, str]] = []

            def try_acquire(self, *, request_id, job_id, **_kwargs):
                self.acquired.append((job_id, request_id))
                return True

            def release(self, request_id, *, job_id, actual_work=None):
                self.released.append((job_id, request_id, actual_work))

        shared = RecordingSharedCredit()
        scheduler = SharedCreditScheduler(
            admission=StaticAdmissionController(limit=1),
            router=RoundRobinEndpointRouter(),
            adapter=FailingAdapter(),
            pool_id="default",
            shared_credit=shared,
        )

        result = scheduler.run([envelope(0)], topology())

        self.assertEqual(shared.acquired, [("j1", "r0")])
        self.assertEqual(shared.released, [("j1", "r0", None)])
        self.assertEqual(result.completions[0].status, "failed")
        self.assertEqual(result.submission_events[0].status, "failed")

    def test_grouped_shared_configuration_uses_the_existing_executor(self):
        credit = FairEndpointCreditCoordinator({"e1": (1, 100), "e2": (1, 100)}, quantum=100)
        scheduler = SharedCreditScheduler.from_config(
            StaticAdmissionController(3), RoundRobinEndpointRouter(), FakeSubmissionAdapter(),
            "default", config=SharedSchedulerConfig(
                execution=SchedulerConfig(endpoint_capacity=EndpointCapacityConfig(request_limit=1)),
                shared_credit=SharedCreditConfig(policy=credit, poll_interval_s=0.0001,
                    ready_window=ReadyWindowConfig(),
                    job=JobSchedulingContract(weight=2, priority=1)),
            ),
        )
        result = scheduler.run([envelope(index) for index in range(3)], topology())
        self.assertEqual(result.max_inflight_seen, 2)
        self.assertEqual([item.request_id for item in result.completions], ["r0", "r1", "r2"])
        self.assertEqual(credit.snapshot("e1").active_requests, 0)
        self.assertEqual(credit.snapshot("e2").active_requests, 0)
