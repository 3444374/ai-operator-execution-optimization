"""Record iteration loads without query factories, model actors or feedback."""
import os
import subprocess
import sys
import unittest


class RecordsQueryRuntimeTests(unittest.TestCase):
    def test_runtime_import_is_independent_of_query_owner_construction(self):
        probe = '''
import importlib.abc
import sys
class NoOwners(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'src.execution_provider.adapters.lotus_records_core',
            'src.execution_provider.adapters.lotus_record_chain_core',
            'src.execution_provider.adapters.lotus_records_async_actor',
            'src.execution_provider.adapters.lotus_chain_token_batches',
        }:
            raise AssertionError('query iteration loaded owner construction: '+fullname)
sys.meta_path.insert(0, NoOwners())
from src.execution_provider.adapters.records_query_runtime import _RecordLocalRows, _check
assert _RecordLocalRows is not None
try:
    _check(float('inf'), lambda: True)
except InterruptedError:
    pass
else:
    raise AssertionError('query cancellation was not checked')
'''
        result = subprocess.run([sys.executable, '-B', '-c', probe],
            env=dict(os.environ, CUDA_VISIBLE_DEVICES='', HF_HUB_OFFLINE='1',
                TRANSFORMERS_OFFLINE='1', LITELLM_LOCAL_MODEL_COST_MAP='True'),
            text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
