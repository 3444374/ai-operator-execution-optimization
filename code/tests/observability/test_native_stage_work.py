from types import SimpleNamespace
import unittest

from src.experiments.native_stage_diagnostics import scheduled_position_split, match_request_iterations
from src.observability.native_gpu_decisions import observe_encoder_cache


class NativeStageWorkTests(unittest.TestCase):
    def test_native_aliases_select_business_work_without_name_guessing(self):
        row=dict(status='observed',actual_members={'unrelated-internal':5},stage_positions=dict(status='observed',
            scheduled_prompt_positions=4,scheduled_nonprompt_positions=1),cudagraph_mode='FULL',
            raw_tokens=5,padding_tokens=3,cascade_supported=False,pid=8,observer_execute_id=3)
        startup=dict(row,actual_members={'startup':5})
        results=dict(status='completed',calls=[dict(status='succeeded',engine_request_id='external')])
        alias=dict(event='native_api_native_add_return',request_id='external',collector_request_id='unrelated-internal',
            native_identity=dict(mapped_internal_ids=['unrelated-internal'],present_internal_ids=['unrelated-internal']))
        report=match_request_iterations([startup,row],[alias],results,capture_status='closed_without_recorded_loss')
        self.assertEqual((report['status'],report['covered_requests'],report['workload_iterations'],report['other_iterations']),('observed',1,1,1))
        self.assertEqual(report['scheduled_prompt_positions'],4)
        self.assertEqual(report['cascade_unsupported_iterations'],1)
        self.assertEqual(match_request_iterations([row],[],results,capture_status='closed_without_recorded_loss')['status'],'unavailable')
        self.assertEqual(match_request_iterations([row],[alias,alias],results,capture_status='closed_without_recorded_loss')['status'],'unavailable')

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
