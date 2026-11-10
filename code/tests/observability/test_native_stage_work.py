from types import SimpleNamespace
import unittest

from src.experiments.native_stage_diagnostics import scheduled_position_split
from src.observability.native_gpu_decisions import observe_encoder_cache


class NativeStageWorkTests(unittest.TestCase):
    def test_prompt_positions_are_separate_from_decode_or_speculative_positions(self):
        row = dict(status='observed', actual_members={'prefill': 7, 'decode': 3},
                   prompt_token_counts={'prefill': 20, 'decode': 10},
                   computed_before={'prefill': 15, 'decode': 12})
        report = scheduled_position_split(row)
        self.assertEqual(report['scheduled_prompt_positions'], 5)
        self.assertEqual(report['scheduled_nonprompt_positions'], 5)
        self.assertNotIn('cache_hits', report)

    def test_missing_prompt_evidence_does_not_become_zero_work(self):
        result = scheduled_position_split(dict(status='observed', actual_members={'a': 2}))
        self.assertEqual(result['status'], 'unavailable')

    def test_encoder_observer_reads_metadata_without_copying_or_holding_values(self):
        class Tensor:
            shape, dtype = (5, 16), 'float16'
            def element_size(self): return 2
            def cpu(self): raise AssertionError('no device copy')
            def tolist(self): raise AssertionError('no device values')
        runner = SimpleNamespace(encoder_cache={'opaque-input': Tensor()})
        events = []
        with observe_encoder_cache(runner, 3, lambda event, **fields: events.append((event, fields))):
            runner.encoder_cache.clear()
        self.assertEqual(events[0][1]['logical_bytes'], 160)
        self.assertEqual(events[1][1]['retained_entries'], 0)

    def test_sink_failure_preserves_native_exception(self):
        def failing_sink(*args, **kwargs): raise RuntimeError('sink')
        original = ValueError('native')
        with self.assertRaises(ValueError) as got:
            with observe_encoder_cache(SimpleNamespace(encoder_cache={}), 1, failing_sink):
                raise original
        self.assertIs(got.exception, original)


if __name__ == '__main__':
    unittest.main()
