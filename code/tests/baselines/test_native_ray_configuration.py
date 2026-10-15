"""Capture actual adapter API arguments; Ray API is an explicit stand-in."""
from types import ModuleType, SimpleNamespace
from importlib.util import find_spec
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.native_calls import NativeCall, _GraphControl, _ray_rows
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions


class NativeRayConfigurationTests(unittest.TestCase):
    def graph(self, capacity):
        recorded = {}
        ray, data, llm, planner = (ModuleType(name) for name in
            ('ray', 'ray.data', 'ray.data.llm', 'ray.data._internal.planner.plan_udf_map_op'))
        ray.__version__, ray.data = '2.56.1', data
        ray.is_initialized = lambda: True
        planner.DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY = 2
        def from_items(rows, **kwargs):
            recorded['source'] = kwargs
            return rows
        data.from_items = from_items
        def config(**kwargs):
            recorded['config'] = kwargs
            return SimpleNamespace(**kwargs)
        llm.HttpRequestProcessorConfig = config
        class Processor:
            stages = {'http': SimpleNamespace(map_batches_kwargs={})}
            def __call__(self, dataset):
                recorded['stage'] = dict(self.stages['http'].map_batches_kwargs)
                return SimpleNamespace(iter_rows=lambda: iter(()))
        llm.build_processor = lambda *args, **kwargs: Processor()
        calls = tuple(NativeCall(str(i), i, b'{}', 'http://localhost/fixture', (), .1) for i in range(8))
        modules = {m.__name__:m for m in (ray, data, llm, planner)}
        with patch.dict('sys.modules', modules):
            list(_ray_rows(calls, NativeGraphOptions(concurrency=capacity), _GraphControl()))
        return recorded

    def test_public_capacity_is_an_allowance_not_the_ray_parallelism_knob(self):
        lower, higher = self.graph(8), self.graph(64)
        self.assertEqual(lower, higher)
        self.assertEqual(lower['config']['concurrency'], (2,2))
        self.assertEqual(lower['config']['batch_size'], 1)
        self.assertEqual(lower['source']['override_num_blocks'], 4)
        self.assertEqual(lower['stage']['max_concurrency'], 1)

    def test_http_allowance_is_independent_of_serial_rows_in_each_batch(self):
        with self.assertRaisesRegex(ValueError, 'active HTTP upper bound'):
            NativeGraphOptions(concurrency=3, ray_actors=2, ray_async_batches_per_actor=2,
                ray_batch_rows=4).validate_backend('ray-data')
        NativeGraphOptions(concurrency=4, ray_actors=2, ray_async_batches_per_actor=2,
            ray_batch_rows=4).validate_backend('ray-data')

    @unittest.skipUnless(find_spec('ray') is not None, 'pinned Ray is unavailable for fresh-import validation')
    def test_fresh_ray_import_reads_environment_override_and_upstream_default(self):
        command = ('import ray\n'
            'from ray.data._internal.planner.plan_udf_map_op import DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY as batches\n'
            'if ray.__version__ != "2.56.1": raise RuntimeError("pinned Ray 2.56.1 required")\n'
            'print(batches)')
        for configured, expected in ((None, 4), ('2', 2)):
            environment = dict(os.environ)
            environment.pop('RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY', None)
            if configured is not None:
                environment['RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY'] = configured
            with self.subTest(configured=configured):
                imported = subprocess.run([sys.executable, '-c', command], env=environment,
                    check=True, capture_output=True, text=True, timeout=30)
                self.assertEqual(imported.stdout.strip(), str(expected))


if __name__ == '__main__':
    unittest.main()
