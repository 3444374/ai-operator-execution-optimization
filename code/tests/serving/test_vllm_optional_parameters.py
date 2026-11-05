"""Requested native options and actual ready values through CPU engine stubs."""

import asyncio
from dataclasses import asdict
import json
from types import SimpleNamespace
import unittest

from src.baselines.text.frameworks.sync_reference_engine import ReferenceVllmEngine
from src.serving.backends import vllm_engine as sync, vllm_async_engine as asynchronous
from tests.serving.test_model_context import native_stub
from tests.serving.test_sync_model_context import native_api


OPTIONS = ({}, {'dtype': None, 'gpu_memory_utilization': None},
           {'dtype': 'auto', 'gpu_memory_utilization': .63},
           {'dtype': 'float32', 'gpu_memory_utilization': 1.0})


def spec(**values):
    return sync.EngineSpec('/tmp/CPU-native-options-model', '/tmp/model-python',
                           '/tmp/driver-python', **values)


def resolved_config(model, options):
    # These are CPU fixture values, not vLLM defaults or hardware observations.
    model.dtype = 'torch.float32' if options.get('dtype') == 'float32' else 'torch.float16'
    return SimpleNamespace(model_config=model, cache_config=SimpleNamespace(
        gpu_memory_utilization=options.get('gpu_memory_utilization') or .71))


class NativeOptionalParameterTests(unittest.TestCase):
    def test_missing_null_explicit_and_invalid_options_survive_config_roundtrip(self):
        for options in OPTIONS:
            with self.subTest(options=options):
                value = spec(**options)
                copied = sync.EngineSpec.from_dict(json.loads(json.dumps(asdict(value))))
                self.assertEqual(copied.dtype, options.get('dtype'))
                self.assertEqual(copied.gpu_memory_utilization, options.get('gpu_memory_utilization'))
        for field, invalid in [('dtype', ''), ('dtype', 'bf16'), ('dtype', True),
                               ('gpu_memory_utilization', True), ('gpu_memory_utilization', 0),
                               ('gpu_memory_utilization', 1.01), ('gpu_memory_utilization', '0.8'),
                               ('gpu_memory_utilization', float('nan'))]:
            with self.subTest(field=field, invalid=invalid), self.assertRaises((TypeError, ValueError)):
                spec(**{field: invalid})

    def test_sync_and_reference_omit_unset_options_and_report_actual_configuration(self):
        for owner_type in (sync.VllmEngine, ReferenceVllmEngine):
            for options in OPTIONS:
                with self.subTest(owner=owner_type.__name__, options=options), native_api() as (calls, closes):
                    owner = owner_type(spec(**options), sync.GenerationSpec())
                    try:
                        native = resolved_config(owner._llm.model_config, options)
                        owner._llm.llm_engine.vllm_config = native
                        for field in ('dtype', 'gpu_memory_utilization'):
                            if options.get(field) is None:
                                self.assertNotIn(field, calls[0])
                            else:
                                self.assertEqual(calls[0][field], options[field])
                        report = owner.ready()
                        self.assertEqual(report['native_capacity']['dtype'], native.model_config.dtype)
                        self.assertEqual(report['native_capacity']['gpu_memory_utilization'],
                                         native.cache_config.gpu_memory_utilization)
                        owner._llm.llm_engine.vllm_config = None
                        missing = owner.ready()['native_capacity']
                        self.assertIsNone(missing['dtype'])
                        self.assertIsNone(missing['gpu_memory_utilization'])
                    finally:
                        owner.close(timeout=1)
                    self.assertEqual(len(closes), 1)

    def test_async_omits_unset_options_and_reports_actual_configuration(self):
        for options in OPTIONS:
            with self.subTest(options=options), native_stub(8192) as captured:
                async def run():
                    owner = asynchronous.VllmAsyncEngine(spec(**options), sync.GenerationSpec())
                    try:
                        native = resolved_config(owner._llm.model_config, options)
                        owner._llm.vllm_config = native
                        kwargs, = captured['constructors']
                        for field in ('dtype', 'gpu_memory_utilization'):
                            if options.get(field) is None:
                                self.assertNotIn(field, kwargs)
                            else:
                                self.assertEqual(kwargs[field], options[field])
                        report = owner.ready()
                        self.assertEqual(report['native_capacity']['dtype'], native.model_config.dtype)
                        self.assertEqual(report['native_capacity']['gpu_memory_utilization'],
                                         native.cache_config.gpu_memory_utilization)
                        owner._llm.vllm_config = None
                        missing = owner.ready()['native_capacity']
                        self.assertIsNone(missing['dtype'])
                        self.assertIsNone(missing['gpu_memory_utilization'])
                    finally:
                        self.assertEqual((await owner.close(timeout=1))['status'], 'closed')
                asyncio.run(run())
                self.assertEqual(captured['shutdowns'], 1)
