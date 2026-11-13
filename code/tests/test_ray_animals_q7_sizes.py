"""Finite Q7 source sizes; software images/Ray replies, no real model or server."""
import gzip
from copy import deepcopy
import importlib
import json
import os
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.baselines.image.ray_animals_q7 import binding


class FrozenSizeTests(unittest.TestCase):
    def test_single_filter_failures_release_the_owner_and_preserve_only_observed_sink(self):
        from tests.test_ray_animals_q7_sink import Q7SinkTests
        for phase in ('native_failure', 'parse_failure', 'close_failure', 'after_sink_failure'):
            helper = Q7SinkTests(); helper.setUp()
            try:
                helper.count = 7
                helper.bound.update(query=1, physical_attempt_limit=7)
                failure = RuntimeError('single Filter ' + phase)
                setattr(helper, phase, failure)
                report = helper.failed(failure)
                self.assertEqual(report['timing']['sink_ready_monotonic_ns'] is not None,
                    phase in ('close_failure', 'after_sink_failure'))
                self.assertFalse((helper.root/'input-1.json.gz').exists())
            finally:
                helper.doCleanups()

    def test_single_filter_tasks_reuse_the_same_worker_release_and_cleanup_flow(self):
        from tests.test_ray_animals_q7_sink import Q7SinkTests
        from src.baselines.image.ray_animals_q7 import runner
        for query_id in (1, 3, 10):
            for count in (1, 7, 13):
                helper = Q7SinkTests(); helper.setUp()
                try:
                    helper.count = count
                    helper.bound.update(query=query_id, physical_attempt_limit=count)
                    report = runner.run_query(helper.root, helper.bound, helper.supervise)
                    self.assertEqual(helper.actions, ['start-0', 'original-full-sink', 'close-0',
                        'host-cleanup-0', 'binding-audit', 'evaluation'])
                    self.assertEqual(len(report['stage_lifecycle']), 1)
                    self.assertEqual(report['logical_occurrences'], count)
                    self.assertFalse((helper.root/'input-1.json.gz').exists())
                    self.assertLessEqual(report['timing']['sink_ready_monotonic_ns'],
                        report['timing']['final_close_started_monotonic_ns'])
                finally:
                    helper.doCleanups()

    def test_two_complete_passes_and_no_dispatch_on_size_or_budget_error(self):
        for count in (1,7,13):
            execute=Mock(side_effect=lambda rows:[dict(call_id=r['call_id'],generated_text=str(i))
                for i,r in reversed(list(enumerate(rows)))])
            two=binding.FilterBinding(execute,count)
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
    def test_query_clock_keeps_real_source_and_interstage_cleanup_but_excludes_preflight(self):
        from lotus.models import LM
        from src.baselines.image import sembench_animals
        from tests.baselines.image.test_sembench_animals_original import OriginalAnimalsImageTests

        helper = OriginalAnimalsImageTests()
        helper.checkout = Path(os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'])
        helper.setUp(); self.addCleanup(helper.doCleanups)
        dataset = helper.make_dataset(count=7)
        preflight, bind_frame = type(dataset).preflight, type(dataset).bind_frame
        bind_query = sembench_animals.bind_original_query
        verify, image_identity = sembench_animals.verify_animals_checkout, sembench_animals.image_identity
        initialize, digest = LM.__init__, binding.message_digest
        evaluate = sembench_animals.evaluate_original

        for query_id in (1, 3, 7, 10):
            with self.subTest(query=query_id):
                clock, active, stages, events = [0], [False], [], {}

                def advance(seconds):
                    clock[0] += seconds * 10**9

                def checked_source(current, query):
                    self.assertFalse(active[0])
                    advance(7)
                    return preflight(current, query)

                def bind_source(current):
                    self.assertFalse(active[0])
                    advance(11)
                    load = bind_frame(current)
                    def scan():
                        self.assertTrue(active[0])
                        advance(2)
                        return load()
                    return scan

                def checked_binding(*args):
                    self.assertFalse(active[0])
                    advance(13)
                    return bind_query(*args)

                def initialized(current, *args, **kwargs):
                    self.assertFalse(active[0])
                    advance(90)
                    return initialize(current, *args, **kwargs)

                def outside_clock(function):
                    def checked(*args, **kwargs):
                        self.assertFalse(active[0], 'extra source/pixel audit entered query clock')
                        return function(*args, **kwargs)
                    return checked

                def started(timestamp):
                    self.assertEqual(timestamp, 121 * 10**9)
                    events['start'] = timestamp
                    active[0] = True

                def execute(rows):
                    self.assertTrue(active[0])
                    advance(3)
                    if query_id == 7 and not stages:
                        advance(5)  # Required first native lifecycle cleanup.
                    stages.append(rows)
                    return [dict(call_id=row['call_id'], generated_text='False') for row in rows[::-1]]

                def ready(result, timestamp):
                    self.assertTrue(active[0])
                    active[0] = False
                    events['sink'] = timestamp
                    advance(23)  # Final native close occurs after the complete sink.

                def summarized(message):
                    self.assertFalse(active[0])
                    advance(17)
                    return digest(message)

                def evaluated(*args):
                    self.assertFalse(active[0])
                    advance(19)
                    return evaluate(*args)

                with patch.object(type(dataset), 'preflight', checked_source), \
                     patch.object(type(dataset), 'bind_frame', bind_source), \
                     patch.object(sembench_animals, 'bind_original_query', checked_binding), \
                     patch.object(sembench_animals, 'verify_animals_checkout', outside_clock(verify)), \
                     patch.object(sembench_animals, 'image_identity', outside_clock(image_identity)), \
                     patch.object(LM, '__init__', initialized), \
                     patch.object(binding.time, 'monotonic_ns', side_effect=lambda: clock[0]), \
                     patch.object(binding, 'message_digest', summarized), \
                     patch.object(sembench_animals, 'evaluate_original', evaluated):
                    _, _, passes, timing = binding.run(dataset, execute, query_id=query_id,
                        physical_attempt_limit=7 * binding.filter_passes(query_id),
                        on_source_started=started, on_result_ready=ready)
                expected = 13 if query_id == 7 else 5
                self.assertEqual((events['sink'] - events['start']) / 1e9, expected)
                self.assertEqual(timing['source_started_monotonic_ns'], events['start'])
                self.assertEqual(timing['sink_ready_monotonic_ns'], events['sink'])
                self.assertGreater(timing['evaluation_finished_monotonic_ns'], events['sink'])
                self.assertEqual(len(passes), binding.filter_passes(query_id))

    def test_single_filter_tasks_keep_original_messages_parser_result_and_evaluator(self):
        from tests.baselines.image.test_sembench_animals_original import OriginalAnimalsImageTests
        helper = OriginalAnimalsImageTests(); helper.checkout = Path(os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'])
        helper.setUp(); self.addCleanup(helper.doCleanups)
        for query_id in (1, 3, 10):
            for count in (1, 7, 13):
                with self.subTest(query=query_id, count=count):
                    dataset = helper.make_dataset(count=count)
                    answers = [['True', 'False', 'no verdict', 'False then True'][i % 4] for i in range(count)]
                    expected, quality, batches = helper.run_query(query_id, answers, dataset=dataset)
                    def execute(rows):
                        return [dict(call_id=row['call_id'], generated_text=answers[i])
                                for i, row in reversed(list(enumerate(rows)))]
                    native = Mock(side_effect=execute)
                    result, actual_quality, passes, _ = binding.run(dataset, native,
                        query_id=query_id, physical_attempt_limit=count)
                    pd.testing.assert_frame_equal(result, expected)
                    observed, reference = deepcopy(actual_quality), deepcopy(quality)
                    # The original gold SQL has no ORDER BY; retain all gold
                    # rows without making their incidental order a requirement.
                    observed_gold, reference_gold = observed.pop('ground_truth'), reference.pop('ground_truth')
                    if isinstance(observed_gold, list):
                        self.assertCountEqual(observed_gold, reference_gold)
                    else:
                        self.assertEqual(observed_gold, reference_gold)
                    self.assertEqual(observed, reference)
                    self.assertEqual(len(passes), 1)
                    self.assertEqual([r['messages'] for r in native.call_args.args[0]],
                        [message for batch in batches for message in batch])

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
