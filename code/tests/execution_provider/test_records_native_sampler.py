"""The retained native sampler loads without query construction or policy."""
import os
import subprocess
import sys
import unittest


class NativeSamplerModuleTests(unittest.TestCase):
    def test_sampler_can_load_without_query_owner_or_window_policy(self):
        probe = '''
import importlib.abc
import sys
class NoOwnerOrPolicy(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'src.execution_provider.adapters.lotus_records_core',
            'src.execution_provider.adapters.lotus_record_chain_core',
            'src.execution_provider.adapters.lotus_chain_token_batches',
            'src.scheduling.submission_control.adaptive',
            'src.scheduling.submission_control.stage_work',
            'src.scheduling.submission_control.request_scan',
        }:
            raise AssertionError('native sampling loaded ownership/policy: '+fullname)
sys.meta_path.insert(0, NoOwnerOrPolicy())
from src.execution_provider.adapters.records_native_sampler import RecordsNativeSampler
sampler = RecordsNativeSampler(object())
assert sampler.sample() is None
assert sampler.ref is None and sampler.future is None
'''
        result = subprocess.run([sys.executable, '-B', '-c', probe],
            env=dict(os.environ, CUDA_VISIBLE_DEVICES='', HF_HUB_OFFLINE='1',
                TRANSFORMERS_OFFLINE='1', LITELLM_LOCAL_MODEL_COST_MAP='True'),
            text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_existing_feedback_reuses_one_sampler_implementation(self):
        from src.execution_provider.adapters.lotus_chain_token_batches import BatchSupplyFeedback
        from src.execution_provider.adapters.records_native_sampler import RecordsNativeSampler
        self.assertTrue(issubclass(BatchSupplyFeedback, RecordsNativeSampler))
        for name in ('sample', '_consume_snapshot', '_cancel_snapshot', 'collect_ready'):
            self.assertIs(getattr(BatchSupplyFeedback, name), getattr(RecordsNativeSampler, name))


if __name__ == '__main__':
    unittest.main()
