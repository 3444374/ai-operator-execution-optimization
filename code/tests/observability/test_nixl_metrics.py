"""Native scheduler-stat totals are observations, not synthetic transfer evidence."""
import unittest

from src.observability.metrics.nixl import NativeNixlTotals


class NixlMetricsTests(unittest.TestCase):
    def test_empty_iterations_can_record_actual_transfer_stats(self):
        from types import SimpleNamespace
        from src.serving.backends.vllm_async_engine import NativeStats
        owner = NativeStats('cpu-observer', enable_nixl_metrics=True)
        values = dict(bytes_transferred=[1024, 2048], transfer_duration=[.01, .02],
            post_duration=[.001, .002], num_descriptors=[2, 4],
            num_failed_transfers=[], num_failed_notifications=[], num_kv_expired_reqs=[])
        owner.record(SimpleNamespace(num_running_reqs=2, num_waiting_reqs=1,
            kv_cache_usage=.2, kv_connector_stats=values), None)
        result = owner.snapshot()['nixl']
        self.assertEqual(result['transfers'], 2)
        self.assertEqual(result['bytes'], 3072)
        self.assertAlmostEqual(result['transfer_s'], .03)

    def test_missing_or_unsupported_statistics_are_explicit(self):
        owner = NativeNixlTotals('cpu-observer')
        owner.record(None)
        self.assertEqual(owner.snapshot()['status'], 'no_transfer_update')
        owner.record({'unknown': []})
        self.assertEqual(owner.snapshot()['status'], 'unavailable')


if __name__ == '__main__':
    unittest.main()
