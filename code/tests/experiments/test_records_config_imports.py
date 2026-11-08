"""CPU configuration checks can load without query or model owners."""
import os
import subprocess
import sys
import unittest


GUARD = '''
import importlib.abc
import sys
class NoQueryOwners(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'src.execution_provider.adapters.lotus_records_core',
            'src.execution_provider.adapters.lotus_record_chain_core',
            'src.execution_provider.adapters.lotus_records_async_actor',
        }:
            raise AssertionError('configuration imported query/model ownership: '+fullname)
sys.meta_path.insert(0, NoQueryOwners())
'''


class RecordsConfigImportTests(unittest.TestCase):
    def probe(self, source):
        result = subprocess.run([sys.executable, '-B', '-c', GUARD+source],
            env=dict(os.environ, CUDA_VISIBLE_DEVICES='', HF_HUB_OFFLINE='1',
                TRANSFORMERS_OFFLINE='1', LITELLM_LOCAL_MODEL_COST_MAP='True'),
            text=True, capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_capacity_profile_and_IPC_helpers_do_not_load_owners(self):
        self.probe('''
from dataclasses import asdict
from src.experiments import movie_records_config as config
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
engine = EngineSpec('/tmp/config-only-model', sys.executable, sys.executable)
resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=7)
value = dict(backend=config.ASYNC_CORE_BACKEND, engine=asdict(engine),
    core_profile=dict(mode='async', max_inflight=3),
    core_capacity=dict(item_input_bytes=65536, item_result_bytes=65536),
    async_limits={})
capacity = config.core_capacity(value)
configured = config.records_runtime(value, resources=resources, engine=engine)
limits = configured.async_limits
assert limits.max_inflight == 7
assert limits.input_bytes == configured.capacity.budget.input_bytes
assert configured.capacity.budget.active_requests == 7
assert value['async_limits'] == {}
''')

    def test_public_session_check_reads_fixture_sources_without_loading_owners(self):
        self.probe('''
from tests.experiments.test_movie_records_session import PlanTests
from src.experiments.movie_records_session import check_plan
case = PlanTests('test_q1_q2_share_preparation_method_with_fresh_task_counts')
case.setUp()
try:
    case.plan['profile'] = dict(mode='async', max_inflight=3)
    case.plan['resources']['max_active_batches'] = 7
    checked = check_plan(case.plan)
    assert checked['status'] == 'checked_cpu_only'
    assert checked['expected_calls'] == 3
    assert checked['inference_calls'] == 0
    assert checked['core_execution']['max_inflight'] == 3
finally:
    case.doCleanups()
''')


if __name__ == '__main__':
    unittest.main()
