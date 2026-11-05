"""Explicit research profiling still wires its configured coordinator."""

import unittest

from src.observability.profiling.ray_scheduler import _shared_credit_client
from tests.research.multitask.test_shared_credit_ray import _FakeRay


class ProfilerCreditIntegrationTests(unittest.TestCase):
    def test_explicit_saor_configuration_acquires_and_returns_shared_capacity(self):
        client = _shared_credit_client(
            _FakeRay(),
            ["e1"],
            {
                "name": "research-credit",
                "namespace": "fixture",
                "request_limit": 1,
                "work_limit": 100,
                "quantum": 100,
                "policy": "saor",
                "saor_release": {
                    "entitlement_weight": 1.0,
                    "queue_weight": 0.0,
                    "fairness_weight": 1.0,
                    "slo_weight": 0.0,
                },
            },
        )
        self.assertTrue(client.try_acquire(
            request_id="r", job_id="j", endpoint_id="e1", estimated_work=20,
        ))
        self.assertEqual(client.snapshot("e1").active_work, 20)
        client.release("r", job_id="j", actual_work=15)
        self.assertEqual(client.snapshot("e1").active_work, 0)
        client.finish_job("j")
