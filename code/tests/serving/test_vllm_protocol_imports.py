"""Shared request values do not require loading a synchronous model owner."""

from dataclasses import asdict
import os
import pickle
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch


class ProtocolImportsTests(unittest.TestCase):
    def test_old_imports_and_old_pickle_names_resolve_to_the_same_shared_values(self):
        from src.serving.backends import vllm_engine as sync, vllm_protocol as protocol

        for name in ('EngineSpec', 'GenerationSpec', 'result_record',
                     'result_from_output', '_prepared', '_token_ids'):
            self.assertIs(getattr(sync, name), getattr(protocol, name), name)
        for name in ('EngineSpec', 'GenerationSpec'):
            old_name = ('csrc.serving.backends.vllm_engine\n'+name+'\n.').encode()
            self.assertIs(pickle.loads(old_name), getattr(protocol, name))
        spec = protocol.EngineSpec('/tmp/protocol-model', '/tmp/model-python', '/tmp/driver-python')
        self.assertEqual(sync.EngineSpec.from_dict(asdict(spec)), spec)
        self.assertEqual(protocol.GenerationSpec().kwargs(), sync.GenerationSpec().kwargs())

    def test_async_sampling_does_not_borrow_a_synchronous_owner_method(self):
        from src.serving.backends import vllm_async_engine as async_kernel, vllm_engine as sync

        engine = async_kernel.VllmAsyncEngine.__new__(async_kernel.VllmAsyncEngine)
        engine._sampling_type = lambda **kwargs: SimpleNamespace(**kwargs)
        engine._structured_outputs_type = lambda **kwargs: dict(kwargs)
        engine._final_only = 'fixture-final-only'
        with patch.object(sync.VllmEngine, '_new_sampling', side_effect=AssertionError('sync owner loaded')):
            result = engine._sampling(dict(max_tokens=13, structured_outputs=dict(json_object=True)))
        self.assertEqual(result.max_tokens, 13)
        self.assertEqual(result.structured_outputs, dict(json_object=True))
        self.assertEqual(result.output_kind, 'fixture-final-only')

    def test_async_kernel_and_IPC_import_without_sync_owner_or_model_packages(self):
        script = '''
import importlib.abc, sys
class ForbiddenImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if (fullname in ('src.serving.backends.vllm_engine', 'src.serving.backends.common',
                         'src.serving.backends.completion', 'src.serving.backends.embedding')
                or fullname.split('.')[0] in ('vllm', 'ray', 'lotus', 'torch', 'transformers')):
            raise AssertionError('unexpected runtime import: '+fullname)
sys.meta_path.insert(0, ForbiddenImports())
from src.serving.backends import vllm_async_engine, async_engine_process, vllm_protocol
assert vllm_async_engine.common is vllm_protocol
assert async_engine_process.common is vllm_protocol
assert 'src.serving.backends.vllm_engine' not in sys.modules
'''
        result = subprocess.run([sys.executable, '-B', '-c', script], env=dict(os.environ),
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
