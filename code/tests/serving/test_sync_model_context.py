"""Sync context receipts from a CPU native-API stub; no model or GPU."""
from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.serving.backends import vllm_engine as sync
from src.serving.backends.model_context import (
    checked_model_context, model_context_identity, model_context_limit,
)


@contextmanager
def native_api(*, actual=8192, model_config=True):
    vllm, sampling = ModuleType('vllm'), ModuleType('vllm.sampling_params')
    vllm.__version__ = sync.VLLM_VERSION
    vllm.SamplingParams = lambda **values: values
    sampling.StructuredOutputsParams = lambda **values: values
    calls, closes = [], []
    def create(**values):
        calls.append(values)
        llm = SimpleNamespace(llm_engine=SimpleNamespace(engine_core=SimpleNamespace(
            shutdown=lambda **options: closes.append(options))))
        if model_config:
            llm.model_config = SimpleNamespace(max_model_len=actual, runner_type='generate')
        return llm
    vllm.LLM = create
    with patch.dict(sys.modules, {'vllm': vllm, 'vllm.sampling_params': sampling}), \
            patch.object(sync.EngineSpec, 'validate_local', return_value={}), \
            patch.object(sync, '_validate_current_runtime'), \
            patch.object(sync.importlib.metadata, 'version', return_value=sync.VLLM_VERSION):
        yield calls, closes


@contextmanager
def kernel(spec, *, actual=8192, model_config=True):
    with native_api(actual=actual, model_config=model_config) as (calls, closes):
        owner = sync.VllmEngine(spec, sync.GenerationSpec())
        try:
            yield owner, calls, closes
        finally:
            owner.close(timeout=1)


def cpu_process_type(actual):
    from src.serving.backends.vllm_sync_process import EngineProcess
    program = '''import json,sys
from tests.serving.test_sync_model_context import native_api
from src.serving.backends.vllm_sync_process import serve
with native_api(actual=json.loads(sys.argv[2])):
    serve(int(sys.argv[1]))
'''
    class CPUProcess(EngineProcess):
        @staticmethod
        def _command(engine, result_fd):
            return [engine.model_python, '-B', '-c', program, str(result_fd), json.dumps(actual)]
    return CPUProcess


class SyncModelContextTests(unittest.TestCase):
    def spec(self, **options):
        return sync.EngineSpec('/tmp/sync-context-model', '/tmp/model-python', '/tmp/driver-python', **options)

    def test_ready_uses_the_actual_native_config_and_preserves_unspecified_intent(self):
        spec = self.spec()
        with kernel(spec, actual=8192) as (owner, calls, _):
            ready = json.loads(json.dumps(owner.ready()))
            observed = checked_model_context(spec, ready)
            self.assertEqual(observed['max_model_len'], 8192)
            self.assertIsNone(observed['requested_max_model_len'])
            self.assertEqual(observed['engine_uid'], owner.engine_uid)
            self.assertEqual(model_context_limit(spec, ready), 8192)
            for name in ('max_model_len', 'max_num_seqs', 'max_num_batched_tokens'):
                self.assertNotIn(name, calls[0])

    def test_explicit_context_matches_actual_or_rejects_before_a_ready_receipt(self):
        spec = self.spec(max_model_len=4096)
        with kernel(spec, actual=4096) as (owner, _, __):
            self.assertEqual(checked_model_context(spec, owner.ready())['max_model_len'], 4096)
        with kernel(spec, actual=8192) as (owner, _, closes):
            with self.assertRaisesRegex(ValueError, 'differs from explicit'):
                owner.ready()
            self.assertEqual(owner.close(timeout=1)['status'], 'closed')
            self.assertEqual(closes, [dict(timeout=1)])


    def test_missing_or_invalid_native_context_is_not_a_legacy_fallback(self):
        for actual in (None, 0, -1, True, False, 4096., '4096'):
            with self.subTest(actual=actual), kernel(self.spec(), actual=actual) as (owner, _, closes):
                with self.assertRaisesRegex((RuntimeError, ValueError), 'native model context'):
                    owner.ready()
                self.assertFalse(owner._closed)
                self.assertEqual(owner.close(timeout=1)['status'], 'closed')
                self.assertEqual(closes, [dict(timeout=1)])
        with kernel(self.spec(), model_config=False) as (owner, _, __):
            with self.assertRaisesRegex(RuntimeError, 'native model context'):
                owner.ready()

    def test_ready_rejects_context_from_another_spec_or_owner(self):
        spec = self.spec()
        with kernel(spec) as (owner, _, __):
            ready = owner.ready()
            for changed in (dict(ready, engine_uid='another-owner'),
                            dict(ready, model_context=dict(ready.get('model_context', {}), max_model_len=1024))):
                with self.subTest(changed=changed), self.assertRaises(ValueError):
                    checked_model_context(spec, changed)
            with self.assertRaises(ValueError):
                checked_model_context(replace(spec, seed=1), ready)

    def test_equivalent_instances_share_context_identity_but_other_limits_do_not(self):
        spec = self.spec()
        with kernel(spec, actual=8192) as (first, first_calls, first_closes), \
                kernel(spec, actual=8192) as (second, second_calls, second_closes), \
                kernel(spec, actual=16384) as (other, other_calls, other_closes):
            self.assertNotEqual(first.engine_uid, second.engine_uid)
            self.assertEqual(model_context_identity(spec, first.ready()), model_context_identity(spec, second.ready()))
            self.assertNotEqual(model_context_identity(spec, first.ready()), model_context_identity(spec, other.ready()))

    def test_output_limit_must_fit_the_actual_context_before_readiness(self):
        with kernel(self.spec(), actual=4) as (owner, _, __):
            with self.assertRaisesRegex(ValueError, 'max_tokens exceeds resolved model context'):
                owner.ready()

    def test_context_change_is_rejected_and_caller_mutation_does_not_change_the_owner(self):
        spec = self.spec()
        with kernel(spec) as (owner, _, __):
            original = owner.ready()['model_context']
            original['max_model_len'] = 1
            self.assertEqual(checked_model_context(spec, owner.ready())['max_model_len'], 8192)
            owner._llm.model_config.max_model_len = 16384
            with self.assertRaisesRegex(RuntimeError, 'changed during this engine lifetime'):
                owner.ready()

    def test_closed_owner_keeps_its_context_and_does_not_claim_physical_exit(self):
        spec = self.spec()
        with kernel(spec) as (owner, _, closes):
            original = owner.ready()
            report = owner.close(timeout=1)
            self.assertTrue(report['engine_api_shutdown_confirmed'])
            self.assertIsNone(report['gpu_exit_confirmed'])
            after = owner.ready()
            self.assertTrue(after['closed'])
            self.assertEqual(after['model_context'], original['model_context'])
            self.assertEqual(closes, [dict(timeout=1)])

    def test_unconfirmed_shutdown_cannot_publish_ready_until_cleanup_is_confirmed(self):
        spec = self.spec()
        with kernel(spec) as (owner, _, closes):
            original = owner.ready()['model_context']
            shutdown = owner._llm.llm_engine.engine_core.shutdown
            attempts = []
            def fail_once(**options):
                attempts.append(options)
                if len(attempts) == 1:
                    raise RuntimeError('CPU fixture shutdown is unconfirmed')
                shutdown(**options)
            owner._llm.llm_engine.engine_core.shutdown = fail_once
            report = owner.close(timeout=1)
            self.assertEqual(report['status'], 'unknown')
            self.assertFalse(report['engine_api_shutdown_confirmed'])
            with self.assertRaisesRegex(RuntimeError, 'cleanup is uncertain'):
                owner.ready()
            self.assertEqual(owner.close(timeout=1)['status'], 'closed')
            after = owner.ready()
            self.assertTrue(after['closed'])
            self.assertEqual(after['model_context'], original)
            self.assertEqual(closes, [dict(timeout=1)])


class SyncContextPipeTests(unittest.TestCase):
    def spec(self):
        return sync.EngineSpec('/tmp/sync-pipe-model', sys.executable, sys.executable)

    def test_ready_context_crosses_the_existing_private_pipe_with_its_identity(self):
        spec = self.spec()
        with tempfile.TemporaryDirectory(prefix='sync-context-pipe-') as directory:
            owner = cpu_process_type(8192)(spec, sync.GenerationSpec(), Path(directory),
                time.monotonic()+10, 5)
            try:
                ready = owner.ready_report
                self.assertEqual(ready['kind'], 'ready')
                self.assertEqual(checked_model_context(spec, ready)['max_model_len'], 8192)
                self.assertIsNone(ready['model_context']['requested_max_model_len'])
                report = owner.close(2)
                self.assertTrue(report['engine_api']['engine_api_shutdown_confirmed'], report)
                self.assertTrue(report['process_exit_confirmed'], report)
                self.assertIsNone(report['engine_api']['gpu_exit_confirmed'])
            finally:
                owner.close(2)

    def test_invalid_native_context_never_produces_ready_and_the_child_is_collected(self):
        with tempfile.TemporaryDirectory(prefix='sync-context-pipe-failed-') as directory:
            control = Path(directory)
            with self.assertRaisesRegex(RuntimeError, 'initialization failed: ValueError'):
                cpu_process_type(None)(self.spec(), sync.GenerationSpec(), control,
                    time.monotonic()+10, 5)
            reports = [json.loads(path.read_text()) for path in control.glob('engine-*.json')]
            self.assertTrue(reports)
            self.assertTrue(all(value['process_exit_confirmed'] for value in reports), reports)
            self.assertTrue(all((value.get('engine_ready') or {}).get('kind') != 'ready' for value in reports))


if __name__ == '__main__':
    unittest.main()
