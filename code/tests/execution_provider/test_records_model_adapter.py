"""Model adapters can load independently of query orchestration; CPU only."""
import os
import pickle
import subprocess
import sys
import unittest


class RecordsModelAdapterTests(unittest.TestCase):
    def test_async_actor_does_not_import_query_host(self):
        probe = '''
import importlib.abc
import sys
class NoQueryHost(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'src.execution_provider.adapters.lotus_records_core':
            raise AssertionError('model adapter imported query orchestration')
sys.meta_path.insert(0, NoQueryHost())
from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
assert 'src.execution_provider.adapters.lotus_records_core' not in sys.modules
'''
        result = subprocess.run([sys.executable, '-B', '-c', probe],
            env=dict(os.environ, CUDA_VISIBLE_DEVICES='', HF_HUB_OFFLINE='1',
                TRANSFORMERS_OFFLINE='1', LITELLM_LOCAL_MODEL_COST_MAP='True'),
            text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_original_actor_locations_and_helpers_keep_the_same_objects(self):
        from src.execution_provider.adapters import lotus_records_core as public
        from src.execution_provider.adapters import records_model_adapter as model
        from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
        for name in ('_ActorOwner', '_CoreModelActor', '_encode', '_capacity_failure',
                     '_check_result_capacity', '_measure'):
            self.assertIs(getattr(public, name), getattr(model, name))
        self.assertTrue(issubclass(AsyncRecordsActorOwner, model._ActorOwner))
        for name in ('_ActorOwner', '_CoreModelActor'):
            old = ('csrc.execution_provider.adapters.lotus_records_core\n'+name+'\n.').encode()
            self.assertIs(pickle.loads(old), getattr(model, name))
            self.assertIn(b'lotus_records_core', pickle.dumps(getattr(model, name), protocol=0))


if __name__ == '__main__':
    unittest.main()
