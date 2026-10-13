"""Public runtime evidence follows the actual context's disconnect, including errors."""
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace
import unittest
from unittest import mock

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.experiments.postgresql.native_adapter_query import _runtime


class NativeAdapterRuntimeEvidenceTests(unittest.TestCase):
    def test_all_ray_owners_keep_the_post_exit_disconnect_fact(self):
        ray = SimpleNamespace(__version__='2.56.1',
            get_runtime_context=lambda: SimpleNamespace(gcs_address='127.0.0.1:46520'))
        for arm in ('fixed-map-native-ray', 'supplier-native-ray', 'fixed-map-semloom'):
            for fail in (False, True):
                report = dict(driver_disconnected=False, shared_cluster_shutdown_requested=False)
                @contextmanager
                def lifetime(*_args):
                    try:
                        yield report
                    finally:
                        report['driver_disconnected'] = True
                physical = RayMapConfig('127.0.0.1:46520', 2, 1, 2048, 8192,
                                        payload_backend='arrow', response_mode='full')
                with self.subTest(arm=arm, fail=fail), mock.patch.dict('sys.modules', {'ray': ray}), \
                        mock.patch('src.experiments.postgresql.native_adapter_query.native_ray_runtime', lifetime):
                    observed = None
                    try:
                        with ExitStack() as stack:
                            _, observed = _runtime(stack, arm, NativeGraphOptions(), physical, None,
                                                   '127.0.0.1:46520')
                            self.assertIs(observed, report)
                            self.assertFalse(observed['driver_disconnected'])
                            if fail:
                                raise ValueError('query failed after connection')
                    except ValueError:
                        if not fail:
                            raise
                    self.assertTrue(observed['driver_disconnected'])
                    self.assertFalse(observed['shared_cluster_shutdown_requested'])
