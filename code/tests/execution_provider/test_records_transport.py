"""Chain transport loads independently of the single-filter query host."""
import os
import pickle
import subprocess
import sys
import unittest


class RecordsTransportTests(unittest.TestCase):
    def test_chain_backend_and_host_do_not_load_single_filter_host(self):
        probe = '''
import importlib.abc
import pickle
import sys
class NoFilterHost(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'src.execution_provider.adapters.lotus_records_core':
            raise AssertionError('chain execution imported the single-filter query host')
sys.meta_path.insert(0, NoFilterHost())
from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity, RecordsCapacityBudget
from src.execution_provider.adapters.records_transport import _RecordTransport, _await_result, _check, close_prepared_core
values = (_RecordTransport, _await_result, _check, close_prepared_core,
          CoreRecordsCapacity, RecordsCapacityBudget)
for value in values:
    assert pickle.loads(pickle.dumps(value, protocol=0)) is value
assert 'src.execution_provider.adapters.lotus_records_core' not in sys.modules
'''
        result = subprocess.run([sys.executable, '-B', '-c', probe],
            env=dict(os.environ, CUDA_VISIBLE_DEVICES='', HF_HUB_OFFLINE='1',
                TRANSFORMERS_OFFLINE='1', LITELLM_LOCAL_MODEL_COST_MAP='True'),
            text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_previous_transport_imports_and_serialized_helpers_remain_available(self):
        from src.execution_provider.adapters import lotus_records_core as public
        from src.execution_provider.adapters import records_transport as transport
        from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
        self.assertTrue(issubclass(LotusChainActorBackend, transport._RecordTransport))
        for name in ('_RecordTransport', '_await_result', '_check', 'close_prepared_core'):
            value = getattr(transport, name)
            self.assertIs(getattr(public, name), value)
            old = ('csrc.execution_provider.adapters.lotus_records_core\n'+name+'\n.').encode()
            self.assertIs(pickle.loads(old), value)
            self.assertIs(pickle.loads(pickle.dumps(value, protocol=0)), value)
        for name in ('TaskKey', 'BoundedAsyncBackend', 'ConfirmedTaskFailure'):
            self.assertIs(getattr(public, name), getattr(transport, name))


if __name__ == '__main__':
    unittest.main()
