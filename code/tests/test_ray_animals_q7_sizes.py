"""Finite Q7 source sizes; software images/Ray replies, no real model or server."""
import gzip
import importlib
import json
import os
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.image.ray_animals_q7 import binding


class FrozenSizeTests(unittest.TestCase):
    def test_two_complete_passes_and_no_dispatch_on_size_or_budget_error(self):
        for count in (1,7,13):
            execute=Mock(side_effect=lambda rows:[dict(call_id=r['call_id'],generated_text=str(i))
                for i,r in reversed(list(enumerate(rows)))])
            two=binding.TwoPassBinding(execute,count)
            with self.assertRaises(ValueError):two(['message']*(count+1))
            execute.assert_not_called()
            for _ in range(2):
                self.assertEqual(two(['message']*count),[str(i) for i in range(count)])
            two.require_complete()
            with self.assertRaises(ValueError):two(['message']*count)
            self.assertEqual(execute.call_count,2)
            calls=[row['call_id'] for call in execute.call_args_list for row in call.args[0]]
            self.assertEqual(len(set(calls)),2*count)
        for cap in (0,-2,1,7,True,2.0,float('inf')):
            with self.subTest(cap=cap),self.assertRaises(ValueError):binding.rows_per_pass(cap)
        self.assertEqual(binding.rows_per_pass(14),7)

    def test_native_processor_restores_exact_occurrences_for_small_sources(self):
        with tempfile.TemporaryDirectory() as raw:
            config=Path(raw)/'config.json';config.write_text('{}')
            ray=ModuleType('ray');data=ModuleType('ray.data');data.from_items=lambda rows:rows;ray.data=data
            with patch.dict(os.environ,Q7_CONFIG=str(config)),patch.dict('sys.modules',{'ray':ray,'ray.data':data}):
                native=importlib.import_module('src.baselines.image.ray_animals_q7.native_query')
                for count in (1,7,13):
                    rows=[dict(call_id=str(i)) for i in range(count)]
                    predictions=[dict(call_id=r['call_id'],generated_text='True') for r in rows[::-1]]
                    pipeline=Mock(return_value=SimpleNamespace(materialize=lambda:SimpleNamespace(take_all=lambda:predictions)))
                    with patch.object(native,'build',return_value=pipeline):
                        self.assertEqual(native.execute(rows,'software-only'),predictions)
                        self.assertEqual(pipeline.call_args.args[0],rows)
                        predictions.pop()
                        with self.assertRaises(AssertionError):native.execute(rows,'software-only')

    def test_two_native_lifecycles_and_original_sink_order_for_small_sources(self):
        from tests.test_ray_animals_q7_sink import Q7SinkTests
        from src.baselines.image.ray_animals_q7 import runner
        for count in (1,7,13):
            helper=Q7SinkTests();helper.setUp()
            try:
                helper.count=count;helper.bound['physical_attempt_limit']=2*count
                report=runner.run_query(helper.root,helper.bound,helper.supervise)
                self.assertEqual(report['logical_occurrences'],2*count)
                self.assertEqual(helper.actions,['start-0','close-0','host-cleanup-0','start-1',
                    'original-full-sink','close-1','host-cleanup-1','binding-audit','evaluation'])
                self.assertEqual(len(report['stage_lifecycle']),2)
                self.assertLessEqual(report['timing']['sink_ready_monotonic_ns'],
                    report['timing']['final_close_started_monotonic_ns'])
            finally:helper.doCleanups()


@unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT'),
    'requires pinned c814 source and existing original LOTUS/evaluator dependencies')
class OriginalSmallSourceTests(unittest.TestCase):
    def test_original_two_pass_messages_parser_full_city_sink_and_capture(self):
        from tests.baselines.image.test_sembench_animals_original import OriginalAnimalsImageTests
        from src.baselines.image.ray_animals_q7 import capture_cpu
        helper=OriginalAnimalsImageTests();helper.checkout=Path(os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'])
        helper.setUp();self.addCleanup(helper.doCleanups)
        for count in (1,7,13):
            dataset=helper.make_dataset(count=count)
            root=helper.root/f'capture-{count}'
            with patch.dict(os.environ,CUDA_VISIBLE_DEVICES='',Q7_OUTPUT=str(root),
                    Q7_MANIFEST=str(dataset.manifest),Q7_CHECKOUT=str(dataset.checkout)):
                capture_cpu.main()
            report=json.loads((root/'CPU-NATIVE-CAPTURE.json').read_text())
            messages=json.loads(gzip.decompress((root/'MESSAGES-PRIVATE.json.gz').read_bytes()))
            answers=['True','False','no verdict','False then True']
            replies=[answers[(i+stage)%4] for stage in range(2) for i in range(count)]
            expected,quality,batches=helper.run_query(7,replies,dataset=dataset)
            self.assertEqual(report['result'],expected.to_dict(orient='split'))
            self.assertEqual(report['quality'],quality)
            self.assertEqual(messages,[message for batch in batches for message in batch])
            self.assertEqual(report['images'],2*count)
            self.assertEqual(report['preflight']['role'],'software_fixture')
            execute=Mock()
            with self.assertRaisesRegex(ValueError,'complete frozen source'):
                binding.run(dataset,execute,physical_attempt_limit=2*(count+1))
            execute.assert_not_called()


if __name__=='__main__':unittest.main()
