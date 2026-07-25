from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.admission import StaticAdmissionController  # noqa: E402
from src.scheduling.models import BatchRequest, EndpointSnapshot, TopologySnapshot  # noqa: E402
from src.scheduling.routing import RoundRobinEndpointRouter  # noqa: E402
from src.scheduling.topology import healthy_endpoints  # noqa: E402


def request() -> BatchRequest:
    return BatchRequest("r1", "j1", "ai_complete", 1, 10, 5, "", 1.0, 1.0, "p1")


def endpoint(endpoint_id: str, *, healthy: bool = True, pool_id: str = "default") -> EndpointSnapshot:
    return EndpointSnapshot(
        endpoint_id,
        f"http://localhost/{endpoint_id}",
        pool_id,
        "0",
        healthy,
        0,
        0,
        0.0,
        1.0,
    )


class SchedulingPolicyTests(unittest.TestCase):
    def test_healthy_endpoints_filters_pool_and_health(self) -> None:
        topology = TopologySnapshot(
            (endpoint("e1"), endpoint("e2", healthy=False), endpoint("e3", pool_id="long")),
            1.0,
        )

        self.assertEqual(
            [item.endpoint_id for item in healthy_endpoints(topology, "default")],
            ["e1"],
        )

    def test_round_robin_skips_unhealthy_endpoints(self) -> None:
        topology = TopologySnapshot(
            (endpoint("e1"), endpoint("e2", healthy=False), endpoint("e3")),
            1.0,
        )
        router = RoundRobinEndpointRouter()

        selected = [router.route(request(), topology, "default").endpoint_id for _ in range(3)]

        self.assertEqual(selected, ["e1", "e3", "e1"])

    def test_round_robin_fails_when_pool_has_no_healthy_endpoint(self) -> None:
        topology = TopologySnapshot((endpoint("e1", healthy=False),), 1.0)

        with self.assertRaisesRegex(RuntimeError, "no healthy endpoint"):
            RoundRobinEndpointRouter().route(request(), topology, "default")

    def test_static_admission_allows_below_limit(self) -> None:
        controller = StaticAdmissionController(limit=2)

        decision = controller.decide(inflight=1)

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.limit, 2)
        self.assertEqual(decision.reason, "below_static_limit")

    def test_static_admission_blocks_at_limit(self) -> None:
        decision = StaticAdmissionController(limit=2).decide(inflight=2)

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "at_static_limit")

    def test_static_admission_rejects_non_positive_limit(self) -> None:
        with self.assertRaisesRegex(ValueError, "limit must be positive"):
            StaticAdmissionController(limit=0)


if __name__ == "__main__":
    unittest.main()
