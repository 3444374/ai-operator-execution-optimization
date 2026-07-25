from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.admission import StaticAdmissionController  # noqa: E402
from src.scheduling.models import BatchRequest, EndpointSnapshot, TopologySnapshot  # noqa: E402
from src.scheduling.routing import (  # noqa: E402
    LeastQueuedEndpointRouter,
    PrefixAffinityEndpointRouter,
    RequestPoolRouter,
    RoundRobinEndpointRouter,
)
from src.scheduling.topology import healthy_endpoints  # noqa: E402


def request(
    *,
    prompt_tokens: int = 10,
    output_tokens: int = 5,
    prefix_key: str = "",
) -> BatchRequest:
    return BatchRequest(
        "r1",
        "j1",
        "ai_complete",
        1,
        prompt_tokens,
        output_tokens,
        prefix_key,
        1.0,
        1.0,
        "p1",
    )


def endpoint(
    endpoint_id: str,
    *,
    healthy: bool = True,
    pool_id: str = "default",
    running: int = 0,
    waiting: int = 0,
) -> EndpointSnapshot:
    return EndpointSnapshot(
        endpoint_id,
        f"http://localhost/{endpoint_id}",
        pool_id,
        "0",
        healthy,
        running,
        waiting,
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

    def test_least_queued_uses_load_then_endpoint_id_tie_break(self) -> None:
        topology = TopologySnapshot(
            (
                endpoint("e2", running=1, waiting=1),
                endpoint("e1", running=2, waiting=0),
                endpoint("e3", running=3, waiting=0),
            ),
            1.0,
        )

        decision = LeastQueuedEndpointRouter().route(
            request(), topology, "default"
        )

        self.assertEqual(decision.endpoint_id, "e1")
        self.assertEqual(decision.reason, "least_queued")

    def test_request_pool_router_applies_prefix_long_short_precedence(self) -> None:
        topology = TopologySnapshot(
            (
                endpoint("prefix-1", pool_id="prefix"),
                endpoint("long-1", pool_id="long"),
                endpoint("short-1", pool_id="short"),
            ),
            1.0,
        )
        router = RequestPoolRouter(long_request_tokens=100)

        prefix = router.route(
            request(prompt_tokens=200, prefix_key="shared"), topology
        )
        long_request = router.route(request(prompt_tokens=100), topology)
        short_request = router.route(request(prompt_tokens=20), topology)

        self.assertEqual(prefix.pool_id, "prefix")
        self.assertEqual(long_request.pool_id, "long")
        self.assertEqual(short_request.pool_id, "short")

    def test_request_pool_router_falls_back_to_available_healthy_pool(self) -> None:
        topology = TopologySnapshot(
            (
                endpoint("prefix-1", pool_id="prefix", healthy=False),
                endpoint("short-1", pool_id="short"),
            ),
            1.0,
        )

        decision = RequestPoolRouter(long_request_tokens=100).route(
            request(prompt_tokens=200, prefix_key="shared"),
            topology,
        )

        self.assertEqual(decision.pool_id, "short")
        self.assertEqual(decision.reason, "fallback_available_pool")

    def test_prefix_affinity_is_stable_and_unhealthy_winner_falls_back(self) -> None:
        router = PrefixAffinityEndpointRouter()
        topology = TopologySnapshot(
            (
                endpoint("e1"),
                endpoint("e2", running=0, waiting=2),
                endpoint("e3", running=0, waiting=1),
            ),
            1.0,
        )
        prefixed = request(prefix_key="tenant-a")

        first = router.route(prefixed, topology, "default")
        second = router.route(prefixed, topology, "default")
        unhealthy_topology = TopologySnapshot(
            tuple(
                endpoint(
                    item.endpoint_id,
                    healthy=item.endpoint_id != first.endpoint_id,
                    running=item.running,
                    waiting=item.waiting,
                )
                for item in topology.endpoints
            ),
            2.0,
        )
        fallback = router.route(prefixed, unhealthy_topology, "default")

        self.assertEqual(first.endpoint_id, second.endpoint_id)
        self.assertNotEqual(fallback.endpoint_id, first.endpoint_id)
        self.assertEqual(fallback.reason, "prefix_unhealthy_least_queued_fallback")


if __name__ == "__main__":
    unittest.main()
