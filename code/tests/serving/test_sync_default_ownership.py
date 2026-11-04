"""Current synchronous omission and explicit native-reference capacity; CPU only."""
import unittest

from src.serving.backends import vllm_engine as sync
from tests.serving.test_sync_model_context import native_api


class SyncDefaultOwnershipTests(unittest.TestCase):
    def spec(self, **options):
        return sync.EngineSpec('/tmp/CPU-default-model', '/tmp/model-python', '/tmp/driver-python', **options)

    def test_current_omission_does_not_supply_legacy_capacity_parameters(self):
        with native_api(actual=32768) as (calls, closes):
            owner = sync.VllmEngine(self.spec(), sync.GenerationSpec(max_tokens=8192))
            try:
                for name in ('max_model_len', 'max_num_seqs', 'max_num_batched_tokens'):
                    self.assertNotIn(name, calls[0])
                self.assertEqual(owner.ready()['model_context']['max_model_len'], 32768)
            finally:
                owner.close(timeout=1)

    def test_current_explicit_parameters_are_forwarded_exactly(self):
        spec = self.spec(max_model_len=16384, max_num_seqs=5, max_num_batched_tokens=9000)
        with native_api(actual=16384) as (calls, closes):
            owner = sync.VllmEngine(spec, sync.GenerationSpec())
            try:
                for name in ('max_model_len', 'max_num_seqs', 'max_num_batched_tokens'):
                    self.assertEqual(calls[0][name], getattr(spec, name))
                self.assertEqual(owner.ready()['model_context']['requested_max_model_len'], 16384)
            finally:
                owner.close(timeout=1)

    def test_native_reference_owns_its_original_optional_defaults(self):
        from src.baselines.text.frameworks.sync_reference_engine import ReferenceVllmEngine
        with native_api(actual=4096) as (calls, closes):
            owner = ReferenceVllmEngine(self.spec(), sync.GenerationSpec())
            try:
                self.assertEqual({name:calls[0][name] for name in
                    ('max_model_len', 'max_num_seqs', 'max_num_batched_tokens')},
                    dict(max_model_len=4096, max_num_seqs=8, max_num_batched_tokens=4096))
                self.assertIsNone(owner.ready()['model_context']['requested_max_model_len'])
                self.assertEqual(owner.ready()['constructor_policy'], 'native_reference_capacity')
            finally:
                owner.close(timeout=1)

    def test_native_reference_keeps_explicit_values_and_early_output_check(self):
        from src.baselines.text.frameworks.sync_reference_engine import ReferenceVllmEngine
        spec = self.spec(max_model_len=8192, max_num_seqs=3, max_num_batched_tokens=12000)
        with native_api(actual=8192) as (calls, closes):
            owner = ReferenceVllmEngine(spec, sync.GenerationSpec())
            try:
                self.assertEqual(calls[0]['max_num_seqs'], 3)
                self.assertEqual(calls[0]['max_num_batched_tokens'], 12000)
                self.assertEqual(calls[0]['max_model_len'], 8192)
            finally:
                owner.close(timeout=1)
        with native_api(actual=32768) as (calls, closes):
            with self.assertRaisesRegex(ValueError, 'max_tokens exceeds max_model_len'):
                ReferenceVllmEngine(self.spec(), sync.GenerationSpec(max_tokens=8192))
            self.assertEqual(calls, [])


if __name__ == '__main__':
    unittest.main()
