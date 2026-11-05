"""Concrete multi-job coordinator integration with the synchronous research consumer."""

import unittest

from src.research.multitask.saor import SaorReleaseConfig
from src.research.multitask.shared_credit import FairEndpointCreditCoordinator
from src.scheduling.core.models import BatchRequest, PayloadEnvelope
from src.scheduling.core.scheduler import SynchronousScheduler
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.submission_control.admission import StaticAdmissionController
from tests.scheduling.test_scheduler import FakeSubmissionAdapter, envelope, topology


class SharedCreditSchedulerTests(unittest.TestCase):
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


    def test_bounded_priority_requires_arrival_epoch_and_acquire_timeout(self) -> None:
        shared = FairEndpointCreditCoordinator(
            {"e1": (1, 100), "e2": (1, 100)},
            quantum=100,
            policy="saor_bounded_priority",
            saor_release_config=SaorReleaseConfig(1.0, 0.0, 1.0, 0.0),
        )
        with self.assertRaisesRegex(ValueError, "acquire timeout"):
            SynchronousScheduler(
                StaticAdmissionController(1),
                RoundRobinEndpointRouter(),
                FakeSubmissionAdapter(),
                "default",
                shared_credit=shared,
                shared_credit_acquire_timeout_s=0.0,
            )
        scheduler = SynchronousScheduler(
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
