"""Model adapters can load independently of query orchestration; CPU only."""
import os
import pickle
import subprocess
import sys
import tempfile
import unittest


class RecordsModelAdapterTests(unittest.TestCase):
    def test_async_actor_does_not_import_query_host(self):
        probe = '''
import importlib.abc
import pickle
import sys
class NoQueryHost(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'src.execution_provider.adapters.lotus_records_core':
            raise AssertionError('model adapter imported query orchestration')
sys.meta_path.insert(0, NoQueryHost())
from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.execution_provider.adapters.records_model_adapter import _ActorOwner, _CoreModelActor
for value in (_ActorOwner, _CoreModelActor):
    assert pickle.loads(pickle.dumps(value, protocol=0)) is value
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
            value = getattr(model, name)
            old = ('csrc.execution_provider.adapters.lotus_records_core\n'+name+'\n.').encode()
            self.assertIs(pickle.loads(old), value)
            self.assertIs(pickle.loads(pickle.dumps(value, protocol=0)), value)


@unittest.skipUnless(os.environ.get('SEMLOOM_REAL_RAY_CPU') == '1', 'explicit real Ray CPU driver required')
class RealRayAdapterSerializationTests(unittest.TestCase):
    def test_production_objects_round_trip_through_a_real_worker(self):
        import ray
        from src.execution_provider.adapters.records_capacity import (
            CoreChainCapacity, CoreRecordsCapacity, RecordsCapacityBudget,
        )
        from src.execution_provider.adapters.records_model_adapter import _ActorOwner, _CoreModelActor
        from src.execution_provider.adapters.records_transport import _RecordTransport, _await_result, _check, close_prepared_core

        if ray.is_initialized():
            raise RuntimeError('serialization regression requires its own fresh driver')
        values = (CoreChainCapacity, CoreRecordsCapacity, RecordsCapacityBudget, _ActorOwner, _CoreModelActor,
                  _RecordTransport, _await_result, _check, close_prepared_core)
        with tempfile.TemporaryDirectory(prefix='ras-',
                dir=os.environ.get('SEMLOOM_REAL_RAY_TMPDIR', '/tmp')) as directory:
            try:
                ray.init(address='local', num_cpus=1, num_gpus=0, include_dashboard=False, log_to_driver=False,
                    _temp_dir=directory, object_store_memory=80*1024**2)

                @ray.remote(num_cpus=1, max_retries=0)
                def echo(objects):
                    return objects

                returned = ray.get(echo.remote(values), timeout=30)
                self.assertEqual(len(returned), len(values))
                for expected, actual in zip(values, returned):
                    with self.subTest(name=expected.__name__):
                        self.assertIs(actual, expected)
            finally:
                ray.shutdown()


if __name__ == '__main__':
    unittest.main()
