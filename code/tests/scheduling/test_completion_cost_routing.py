from dataclasses import replace
import unittest

from src.scheduling.core.models import EndpointSnapshot, TopologySnapshot
from src.scheduling.endpoint_routing.completion_cost import CompletionCostEndpointRouter, EndpointCost


class CompletionCostRoutingTests(unittest.TestCase):
    def test_transfer_and_rebuild_can_outweigh_a_shorter_queue(self):
        endpoints = (EndpointSnapshot('local', 'http://localhost:1', 'p', '0', True, 1, 0, 0, 0),
                     EndpointSnapshot('remote', 'http://localhost:2', 'p', '1', True, 0, 0, 0, 0))
        costs = {'local': EndpointCost(3, 0, 1, 'measured-fixture'),
                 'remote': EndpointCost(0, 4, 5, 'measured-fixture')}
        router = CompletionCostEndpointRouter(lambda request, endpoint: costs[endpoint.endpoint_id])
        self.assertEqual(router.route(None, TopologySnapshot(endpoints, 0), 'p').endpoint_id, 'local')
        unavailable = (replace(endpoints[0], available=False), endpoints[1])
        self.assertEqual(router.route(None, TopologySnapshot(unavailable, 0), 'p').endpoint_id, 'remote')


if __name__ == '__main__':
    unittest.main()
