"""Native context resolution through CPU stubs and real private child IPC."""

import asyncio
from contextlib import contextmanager, ExitStack
from dataclasses import asdict, replace
from pathlib import Path
import sys
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.serving.backends import vllm_async_engine as kernel
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.model_context import checked_model_context, model_context_identity, model_context_limit, model_context_record
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.serving.test_async_engine_process import row


def spec(**changes):
    return EngineSpec('/tmp/cpu-context-model', sys.executable, '/tmp/cpu-driver', **changes)


@contextmanager
def native_stub(limit, *, prompt_tokens=1):
    names = ('vllm', 'vllm.engine.arg_utils', 'vllm.entrypoints.chat_utils', 'vllm.renderers',
             'vllm.sampling_params', 'vllm.utils.mistral', 'vllm.v1.engine.async_llm')
    modules = {name: ModuleType(name) for name in names}
    modules['vllm'].__version__ = '0.25.1'
    modules['vllm'].SamplingParams = object
    modules['vllm.engine.arg_utils'].AsyncEngineArgs = lambda **kwargs: SimpleNamespace(**kwargs)
    modules['vllm.entrypoints.chat_utils'].load_chat_template = lambda _: None
    modules['vllm.renderers'].ChatParams = lambda **kwargs: kwargs
    modules['vllm.sampling_params'].RequestOutputKind = SimpleNamespace(FINAL_ONLY='final')
    modules['vllm.sampling_params'].StructuredOutputsParams = object
    modules['vllm.utils.mistral'].is_mistral_tokenizer = lambda _: False
    captured = dict(constructors=[], generated=[], shutdowns=0)

    async def render(*_):
        return [], [dict(type='token', prompt_token_ids=list(range(prompt_tokens)))]

    async def generate(prompt, sampling, request_id):
        captured['generated'].append(request_id)
        yield SimpleNamespace(finished=True)

    def create(args, **kwargs):
        captured['constructors'].append(vars(args))
        model = SimpleNamespace(max_model_len=limit, enable_prompt_embeds=False)
        return SimpleNamespace(renderer=SimpleNamespace(tokenizer=object(), render_chat_async=render),
            model_config=model, vllm_config=SimpleNamespace(model_config=model), generate=generate,
            shutdown=lambda **_: captured.update(shutdowns=captured['shutdowns']+1))

    modules['vllm.v1.engine.async_llm'].AsyncLLM = SimpleNamespace(from_engine_args=create)
    with ExitStack() as stack:
        stack.enter_context(patch.dict(sys.modules, modules))
        stack.enter_context(patch.object(EngineSpec, 'validate_local', return_value={}))
        stack.enter_context(patch.object(kernel.common, '_validate_current_runtime'))
        stack.enter_context(patch.object(kernel.importlib.metadata, 'version', return_value='0.25.1'))
        stack.enter_context(patch.object(kernel.VllmAsyncEngine, '_sampling', return_value=SimpleNamespace()))
        stack.enter_context(patch.object(kernel.VllmAsyncEngine, '_convert',
            return_value=dict(status='succeeded', engine_terminal_confirmed=True)))
        yield captured


class ModelContextTests(unittest.TestCase):
    def test_omitted_and_null_are_delegated_and_explicit_value_round_trips(self):
        omitted = spec()
        self.assertIsNone(omitted.max_model_len)
        self.assertEqual(EngineSpec.from_dict(asdict(omitted)), omitted)
        self.assertEqual(spec(max_model_len=None), omitted)
        self.assertEqual(spec(max_model_len=8192).max_model_len, 8192)
        for value in (0, -1, True, 'auto', '8192'):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                spec(max_model_len=value)

    def test_ready_context_binds_actual_engine_and_requested_configuration(self):
        engine = spec()
        ready = dict(engine_uid='cpu-one', model_context=model_context_record(engine, 'cpu-one', 32768))
        self.assertEqual(model_context_limit(engine, ready), 32768)
        for change in (dict(engine_uid='foreign'), dict(model_context=dict(ready['model_context'], max_model_len=True)),
                       dict(model_context=dict(ready['model_context'], max_model_len=8192)),
                       dict(model_context=dict(ready['model_context'], requested_max_model_len=4096))):
            with self.subTest(change=change), self.assertRaises((ValueError, RuntimeError)):
                checked_model_context(engine, dict(ready, **change))
        with self.assertRaises(ValueError):
            checked_model_context(replace(engine, seed=1), ready)
        with self.assertRaises(RuntimeError):
            model_context_limit(engine)

    def test_effective_context_changes_cache_identity_without_process_id_noise(self):
        engine = spec()
        def ready(uid, length):
            return dict(engine_uid=uid, model_context=model_context_record(engine, uid, length))
        a = model_context_identity(engine, ready('first', 2048))
        self.assertEqual(a, model_context_identity(engine, ready('second', 2048)))
        self.assertNotEqual(a, model_context_identity(engine, ready('first', 32768)))
        for changes in ({'dtype': 'auto'}, {'dtype': 'float32'}, {'gpu_memory_utilization': .8}):
            changed = spec(**changes)
            report = dict(engine_uid='first', model_context=model_context_record(changed, 'first', 2048))
            self.assertNotEqual(a, model_context_identity(changed, report))

    def test_async_constructor_omits_unknown_context_and_keeps_explicit_value(self):
        for requested, actual in ((None, 2048), (None, 32768), (1024, 1024)):
            with self.subTest(requested=requested, actual=actual), native_stub(actual) as calls:
                async def run():
                    engine = kernel.VllmAsyncEngine(spec(max_model_len=requested), GenerationSpec(), limits=kernel.AsyncEngineLimits(max_inflight=8))
                    self.assertEqual(engine.ready()['model_context']['max_model_len'], actual)
                    self.assertEqual(engine.spec.max_model_len, requested)
                asyncio.run(run())
                kwargs, = calls['constructors']
                if requested is None:
                    self.assertNotIn('max_model_len', kwargs)
                else:
                    self.assertEqual(kwargs['max_model_len'], requested)
                self.assertEqual((kwargs['tensor_parallel_size'], kwargs['pipeline_parallel_size']), (1,1))

    def test_unknown_invalid_or_different_native_limit_cannot_be_ready(self):
        for requested, actual in ((None,None),(None,True),(None,0),(1024,2048)):
            with self.subTest(requested=requested, actual=actual), native_stub(actual):
                async def run():
                    engine = kernel.VllmAsyncEngine(spec(max_model_len=requested), GenerationSpec(), limits=kernel.AsyncEngineLimits(max_inflight=8))
                    with self.assertRaises(ValueError):
                        engine.ready()
                asyncio.run(run())

    def test_default_generation_must_fit_native_limit_before_ready(self):
        with native_stub(2048):
            async def run():
                engine = kernel.VllmAsyncEngine(spec(), GenerationSpec(max_tokens=2049), limits=kernel.AsyncEngineLimits(max_inflight=8))
                with self.assertRaisesRegex(ValueError, 'effective model context'):
                    engine.ready()
            asyncio.run(run())

    def test_exact_rendered_prompt_plus_output_cap_checks_native_context(self):
        for limit, prompt, cap, succeeds in ((2048,2040,8,True),(2048,2041,8,False),
                (32768,5000,8,True),(32768,32760,8,True),(32768,32761,8,False)):
            with self.subTest(limit=limit,prompt=prompt), native_stub(limit,prompt_tokens=prompt) as calls:
                async def run():
                    engine = kernel.VllmAsyncEngine(spec(), GenerationSpec(max_tokens=cap), limits=kernel.AsyncEngineLimits(max_inflight=8))
                    engine.ready()
                    result = await engine.generate_prepared(row(0,'fixture'), request_id='cpu-row',
                        deadline_monotonic=time.monotonic()+3)
                    self.assertEqual(result['status'], 'succeeded' if succeeds else 'failed')
                    self.assertTrue(result['engine_terminal_confirmed'])
                    self.assertEqual(engine._reserved_model_calls, int(succeeds))
                asyncio.run(run())
                self.assertEqual(len(calls['generated']), int(succeeds))


class CPUProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable, str(Path(__file__).with_name('async_engine_fixture.py')), *map(str,descriptors)]


class ContextIPCTests(unittest.TestCase):
    def test_invalid_ready_cannot_bind_unknown_context(self):
        for report in ({'engine_uid':'CPU-missing'},
                dict(engine_uid='CPU-local',model_context=model_context_record(spec(),'CPU-foreign',32768))):
            process=object.__new__(AsyncEngineProcess)
            process.spec,process.generation=spec(),GenerationSpec()
            process.ready_report=None
            process._condition=threading.Condition()
            with self.subTest(report=report),self.assertRaises((ValueError,RuntimeError)):
                process._receive(dict(op='ready',report=report),results=False)
            self.assertIsNone(process.ready_report)

    def test_real_open_ready_and_submit_keep_model_resolved_context(self):
        with patch.object(EngineSpec,'validate_driver_environment',return_value={}):
            process = CPUProcess(spec(), GenerationSpec(), startup_timeout_s=5,
                                limits=kernel.AsyncEngineLimits(max_inflight=1))
        try:
            self.assertIsNone(process.spec.max_model_len)
            self.assertEqual(checked_model_context(process.spec,process.ready_report)['max_model_len'],32768)
            self.assertEqual(process.submit(row(0,'fixture'), generation=GenerationSpec(max_tokens=8192),
                deadline_monotonic=time.monotonic()+3).status,'queued')
            with self.assertRaises(ValueError):
                process.submit(row(1,'fixture'), generation=GenerationSpec(max_tokens=32769),
                    deadline_monotonic=time.monotonic()+3)
            self.assertEqual(process.snapshot()['issued_submissions'],1)
        finally:
            self.assertEqual(process.close(timeout=3)['status'],'closed')


if __name__=='__main__':
    unittest.main()
