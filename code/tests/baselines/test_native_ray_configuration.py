"""Capture actual adapter API arguments; Ray API is an explicit stand-in."""
from types import ModuleType, SimpleNamespace
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

    def test_batch_rows_must_fit_the_declared_request_envelope(self):
        with self.assertRaisesRegex(ValueError, 'batch request envelope'):
            NativeGraphOptions(concurrency=8, ray_actors=2, ray_async_batches_per_actor=2, ray_batch_rows=4)
        NativeGraphOptions(concurrency=16, ray_actors=2, ray_async_batches_per_actor=2, ray_batch_rows=4)


if __name__ == '__main__':
    unittest.main()
