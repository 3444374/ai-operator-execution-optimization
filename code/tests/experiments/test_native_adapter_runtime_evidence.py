"""Public runtime evidence follows the actual context's disconnect, including errors."""
from contextlib import ExitStack, contextmanager
from dataclasses import asdict
import os
from types import SimpleNamespace
import unittest
from unittest import mock

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.experiments.postgresql.native_adapter_query import _runtime
from src.experiments.postgresql.query_config import NativeRayRuntimeConfig, QueryConfig


class NativeAdapterRuntimeEvidenceTests(unittest.TestCase):
    def test_prepared_owners_forward_runtime_controls_without_sql_reader_constraints(self):
        ray = SimpleNamespace(__version__='2.56.1',
            get_runtime_context=lambda: SimpleNamespace(gcs_address='127.0.0.1:46520'))
        physical = RayMapConfig('127.0.0.1:46520', 1, 1, MAX_FRAME_BYTES+24, 4*MAX_FRAME_BYTES,
                                payload_backend='arrow', response_mode='full')
        cases = (
            ('supplier-native-ray', NativeGraphOptions(concurrency=1, num_threads=1, ray_batch_rows=256), None),
            ('fixed-map-semloom', NativeGraphOptions(concurrency=1, num_threads=1, ray_batch_rows=256), physical),
            ('fixed-map-native-ray', NativeGraphOptions(concurrency=1, num_threads=1, ray_actors=1,
                ray_async_batches_per_actor=1, source_blocks=2), None),
            ('fixed-map-native-ray', NativeGraphOptions(concurrency=4, num_threads=8, source_blocks=2), None),
        )
        for arm, options, selected_physical in cases:
            recorded = []
            @contextmanager
            def lifetime(_ray, config, _root):
                recorded.append(config)
                yield {}
            with self.subTest(arm=arm, options=options), mock.patch.dict(os.environ), \
                    mock.patch.dict('sys.modules', {'ray': ray}), \
                    mock.patch('src.experiments.postgresql.native_adapter_query.native_ray_runtime', lifetime):
                with ExitStack() as stack:
                    updated, _ = _runtime(stack, arm, options, selected_physical, None, 'localhost:46520')
                self.assertEqual(os.environ['RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY'],
                                 str(options.ray_async_batches_per_actor))
            self.assertEqual(len(recorded), 1)
            self.assertIs(type(recorded[0]), NativeRayRuntimeConfig)
            self.assertEqual(asdict(recorded[0]), dict(ray_num_cpus=options.num_threads,
                ray_async_batches_per_actor=options.ray_async_batches_per_actor,
                ray_address='localhost:46520', ray_object_store_bytes=256*1048576))
            if selected_physical is not None:
                self.assertEqual(updated, physical)

    def test_ray_data_envelope_is_rejected_before_runtime_or_import_setting_changes(self):
        for options in (NativeGraphOptions(concurrency=3),
                        NativeGraphOptions(concurrency=4, ray_batch_rows=2)):
            with self.subTest(options=options), mock.patch.dict(os.environ,
                    {'RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY': '4'}), \
                    mock.patch('src.experiments.postgresql.native_adapter_query.native_ray_runtime') as runtime:
                with ExitStack() as stack, self.assertRaisesRegex(ValueError, 'batch request envelope'):
                    _runtime(stack, 'fixed-map-native-ray', options, None, None, None)
                runtime.assert_not_called()
                self.assertEqual(os.environ['RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY'], '4')

    def test_invalid_addresses_are_rejected_before_runtime_start(self):
        for address in ('auto', 'local', 'ray://localhost:1', 'localhost', 'localhost:0',
                        'localhost:65536', 'localhost:123456', '256.0.0.1:1', 123):
            with self.subTest(address=address), \
                    mock.patch('src.experiments.postgresql.native_adapter_query.native_ray_runtime') as runtime:
                with ExitStack() as stack, self.assertRaises(ValueError):
                    _runtime(stack, 'supplier-native-ray', NativeGraphOptions(concurrency=1, num_threads=1),
                             None, None, address)
                runtime.assert_not_called()

    def test_runtime_resources_are_typed_and_bounded(self):
        base = dict(ray_num_cpus=1, ray_async_batches_per_actor=1)
        for changes in ({'ray_num_cpus': 0}, {'ray_num_cpus': True}, {'ray_num_cpus': 257},
                        {'ray_num_cpus': 1.0}, {'ray_async_batches_per_actor': 0},
                        {'ray_async_batches_per_actor': True}, {'ray_async_batches_per_actor': 257},
                        {'ray_object_store_bytes': True}, {'ray_object_store_bytes': 256*1048576.0},
                        {'ray_object_store_bytes': 80*1048576-1}, {'ray_object_store_bytes': 4*1073741824+1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                NativeRayRuntimeConfig(**dict(base, **changes))
        for address in (None, 'localhost:1', '127.0.0.1:65535'):
            for size in (80*1048576, 4*1073741824):
                with self.subTest(address=address, size=size):
                    config = NativeRayRuntimeConfig(256, 256, address, size)
                    self.assertEqual(config.ray_object_store_bytes, size)

    def test_sql_reader_keeps_its_cpu_reserve_and_actor_batch_capacity_checks(self):
        base = dict(unit_id='sql-fixture', arm='ray-data', task='map', table='unused')
        for resources in (dict(concurrency=1, ray_num_cpus=1, ray_actors=1),
                          dict(concurrency=4, ray_num_cpus=2, ray_async_batches_per_actor=2),
                          dict(concurrency=3, ray_num_cpus=8, ray_async_batches_per_actor=2)):
            with self.subTest(resources=resources), self.assertRaisesRegex(ValueError, 'CPU slots for SQL reading'):
                QueryConfig(**dict(base, **resources))

    def test_runtime_config_preserves_job_environment_resources_and_disconnect(self):
        from src.experiments.postgresql.query_execution import native_ray_runtime
        for fail in (False, True):
            initialized = False
            def initialize(**_kwargs):
                nonlocal initialized
                initialized = True
            def shutdown():
                nonlocal initialized
                initialized = False
            ray = SimpleNamespace(__version__='2.56.1', is_initialized=lambda: initialized,
                init=mock.Mock(side_effect=initialize), shutdown=mock.Mock(side_effect=shutdown),
                cluster_resources=lambda: dict(CPU=1, GPU=0, object_store_memory=256*1048576))
            with self.subTest(fail=fail), mock.patch('src.experiments.postgresql.query_execution.Path') as path:
                path.return_value.exists.return_value = False
                observed = None
                try:
                    with native_ray_runtime(ray, NativeRayRuntimeConfig(1, 2), '/tmp/runtime-fixture') as report:
                        observed = report
                        self.assertFalse(report['driver_disconnected'])
                        if fail:
                            raise ValueError('query failed after connection')
                except ValueError:
                    if not fail:
                        raise
            ray.init.assert_called_once_with(address='local', num_cpus=1, num_gpus=0,
                include_dashboard=False, _node_ip_address='127.0.0.1', object_store_memory=256*1048576,
                _temp_dir='/tmp/runtime-fixture', runtime_env={'env_vars':
                    {'RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY': '2'}})
            ray.shutdown.assert_called_once_with()
            self.assertTrue(observed['driver_disconnected'])
            self.assertFalse(observed['shared_cluster_shutdown_requested'])

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
