"""Independent MMQA image Join/Extract contracts, CPU software only."""

import base64
from collections import deque
from copy import deepcopy
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.image.sembench_native_mmqa import MMQAVisualJoinTask


GOLD_IMAGE = '117d500aaa630023c4038b8268b309c0.png'


class NativeMMQAVisualTests(unittest.TestCase):
    def checkout(self):
        checkout = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')
        if not checkout:
            self.skipTest('bind the fixed public SemBench checkout for reference/score checks')
        return checkout

    def inputs(self):
        import pandas as pd
        from PIL import Image

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root/'images').mkdir()
        Image.new('RGB', (5, 3), (0, 0, 255)).save(root/'images'/GOLD_IMAGE)
        Image.new('RGB', (5, 3), (255, 0, 0)).save(root/'images'/'other.jpg')
        (root/'images'/'ignored.PNG').write_text('not a selected extension')
        pd.DataFrame(dict(ID=[0, 5, 6, 9, 10], Track=['software racetrack']*5)).to_csv(root/'ap_warrior.csv', index=False)
        pd.DataFrame(dict(Airlines=['DomesticOnly', 'British Airways', 'British Airways'],
                          Destinations=['Chicago', 'London', 'Berlin'])).to_csv(root/'tampa_international_airport.csv', index=False)
        return root

    @staticmethod
    def image_uri(messages):
        return next(part['image_url']['url'] for message in messages if isinstance(message['content'], list)
                    for part in message['content'] if part['type'] == 'image_url')

    def blue(self, messages):
        from PIL import Image

        uri = self.image_uri(messages)
        with Image.open(io.BytesIO(base64.b64decode(uri.split(',', 1)[1]))) as image:
            self.assertEqual(image.size, (5, 3))
            return image.convert('RGB').getpixel((0, 0)) == (0, 0, 255)

    def drive(self, task, source, *, none_match=False, color='blue'):
        generator = task.query_batches(source)
        batches = []
        requests = next(generator)
        while True:
            batches.append(requests)
            extract = bool(requests and '-extract-' in requests[0]['call_id'])
            predictions = [dict(call_id=row['call_id'], generated_text=json.dumps(
                {'color': color} if extract else {'keep': self.blue(row['messages']) and not none_match}))
                for row in requests]
            try:
                requests = generator.send(list(reversed(predictions)))
            except StopIteration as done:
                return done.value, batches

    def test_count_order_and_actual_two_stage_survivors(self):
        for query in ('Q2a', 'Q2b', 'Q7'):
            task = MMQAVisualJoinTask(self.checkout(), self.inputs(), query, 'occurrences')
            source = task.read_source()
            count = task.model_request_count(source)
            self.assertEqual(count, 6 if query == 'Q7' else 10)
            with patch.object(MMQAVisualJoinTask, 'requests', side_effect=AssertionError('count built pairs')):
                self.assertEqual(task.model_request_count(source), count)
            before = task.describe(source)
            self.assertIsNone(before['actual_followup_extracts']) if query == 'Q2b' else self.assertEqual(before['actual_followup_extracts'], 0)
            result, batches = self.drive(task, source)
            self.assertEqual([len(batch) for batch in batches], [10, 5] if query == 'Q2b' else [count])
            self.assertEqual(len({row['call_id'] for batch in batches for row in batch}), sum(map(len, batches)))
            self.assertEqual(result.columns.tolist(), ['Airlines', 'image_id'] if query == 'Q7' else
                             ['ID', 'image_id', 'color'] if query == 'Q2b' else ['ID', 'image_id'])
            self.assertEqual(result['image_id'].tolist(), [GOLD_IMAGE]*len(result))
            if query == 'Q7':
                self.assertEqual(result['Airlines'].tolist(), ['DomesticOnly', 'British Airways', 'British Airways'])
            if query == 'Q2b':
                self.assertEqual(task.describe(source)['actual_followup_extracts'], 5)
                self.assertEqual(len(source.followup_reads), 5)

    def test_original_image_order_and_only_actual_image_and_subject_enter_messages(self):
        root = self.inputs()
        original = os.listdir
        order = ['other.jpg', 'ignored.PNG', GOLD_IMAGE]
        with patch.object(os, 'listdir', side_effect=lambda path: order if Path(path) == root/'images' else original(path)):
            for query in ('Q2a', 'Q7'):
                task = MMQAVisualJoinTask(self.checkout(), root, query)
                source = task.read_source()
                self.assertEqual([Path(path).name for path in source.image_paths], ['other.jpg', GOLD_IMAGE])
                requests = task.requests(source)
                self.assertEqual([self.blue(row['messages']) for row in requests],
                                 [False]*5+[True]*5 if query == 'Q2a' else [False, True]*3)
                for row in requests:
                    self.assertEqual([item['role'] for item in row['messages']], ['system', 'user'])
                    text = row['messages'][1]['content'][0]['text']
                    payload = json.loads(text)
                    self.assertEqual(set(payload), {'Airlines'} if query == 'Q7' else {'Track'})
                    self.assertNotIn(GOLD_IMAGE, text)
                    self.assertNotIn('Destinations', text)

    def test_empty_raw_extract_error_and_strict_occurrence_and_json_contracts(self):
        task = MMQAVisualJoinTask(self.checkout(), self.inputs(), 'Q2b')
        source = task.read_source()
        with self.assertRaisesRegex(KeyError, 'color'):
            self.drive(task, source, none_match=True)
        self.assertEqual(task.describe(source)['actual_followup_extracts'], 0)
        for query in ('Q2a', 'Q7'):
            task = MMQAVisualJoinTask(self.checkout(), self.inputs(), query)
            empty, _ = self.drive(task, task.read_source(), none_match=True)
            self.assertTrue(empty.empty)
        task = MMQAVisualJoinTask(self.checkout(), self.inputs(), 'Q2b')
        source = task.read_source()
        for kind in ('missing', 'duplicate', 'foreign', 'bad-json'):
            generator = task.query_batches(source)
            rows = next(generator)
            predictions = [dict(call_id=row['call_id'], generated_text='{"keep":true}') for row in rows]
            if kind == 'missing': predictions.pop()
            elif kind == 'duplicate': predictions.append(predictions[0])
            elif kind == 'foreign': predictions[-1]['call_id'] = 'foreign'
            else: predictions[0]['generated_text'] = '{"keep":1}'
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                generator.send(predictions)
        generator = task.query_batches(source)
        rows = next(generator)
        extracts = generator.send([dict(call_id=row['call_id'], generated_text='{"keep":true}') for row in rows])
        for value in ('{"color":1}', '{"color":"a","color":"b"}', '{"color":[],"extra":1}'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                other = task.query_batches(source)
                first = next(other)
                second = other.send([dict(call_id=row['call_id'], generated_text='{"keep":true}') for row in first])
                other.send([dict(call_id=row['call_id'], generated_text=value) for row in second])
        self.assertEqual(len(extracts), 10)

    def test_frozen_original_gold_score_and_post_query_identity_are_independent(self):
        import pandas as pd

        for query in ('Q2a', 'Q2b', 'Q7'):
            task = MMQAVisualJoinTask(self.checkout(), self.inputs(), query)
            evaluation = task.prepare_evaluation()
            source = task.read_source()
            result, _ = self.drive(task, source)
            before = result.copy(deep=True)
            original_read = Path.read_bytes
            gold = Path(self.checkout())/'files/mmqa/query/natural_language'/('q'+task.query+'.json')
            with patch.object(Path, 'read_bytes', side_effect=lambda path: (_ for _ in ()).throw(
                    AssertionError('mutable gold reread')) if path == gold else original_read(path), autospec=True):
                quality = task.evaluate(source, result, evaluation=evaluation)
            self.assertIn('f1_score', quality['original_metric'])
            if query != 'Q7': self.assertEqual(quality['original_metric']['f1_score'], 1)
            pd.testing.assert_frame_equal(result, before)
            task.verify_source(source)
            (task.data_dir/'images'/GOLD_IMAGE).write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'changed'):
                task.verify_source(source)
            with self.assertRaisesRegex(ValueError, 'prepare_evaluation'):
                task.evaluate(source, result)

    def test_complete_relations_match_pinned_query_and_real_SDK_with_software_replies(self):
        import pandas as pd
        import lotus
        import lotus.models.lm as lm_module
        from lotus.models import LM
        from lotus.sem_ops.sem_join import SemJoinDataframe
        from lotus.types import ReasoningStrategy
        from litellm.types.utils import ModelResponse
        from src.baselines.text.sembench_source import load_module

        module = load_module(self.checkout(), 'native_mmqa_visual_software_reference',
                             'src/scenario/mmqa/runner/lotus_runner/lotus_runner.py')
        original_join = SemJoinDataframe.__call__
        def join(accessor, *args, **kwargs):
            if kwargs.get('strategy') == 'zs-cot':
                kwargs['strategy'] = ReasoningStrategy.ZS_COT
            return original_join(accessor, *args, **kwargs)
        for query in ('Q2a', 'Q2b', 'Q7'):
            root = self.inputs()
            task = MMQAVisualJoinTask(self.checkout(), root, query)
            source = task.read_source()
            actual, batches = self.drive(task, source)
            pending = deque('Answer: True' if self.blue(row['messages']) else 'Answer: False' for row in batches[0])
            if query == 'Q2b':
                pending.extend(json.dumps(dict(color='blue')) for _ in batches[1])
            lm = LM('openai/CPU-native-mmqa-reference', max_tokens=32, temperature=0)
            lm.max_batch_size = 64
            reference_batches = []
            def reply(*args, **kwargs):
                messages = args[1] if len(args) > 1 else kwargs['messages']
                reference_batches.append(deepcopy(messages))
                return [ModelResponse(model=lm.model, object='chat.completion', choices=[dict(index=0,
                    finish_reason='stop', message=dict(role='assistant', content=pending.popleft()))],
                    usage=dict(prompt_tokens=1, completion_tokens=1, total_tokens=2)) for _ in messages]
            owner = object.__new__(module.LotusRunner)
            owner.data_path, owner.policy = str(root), 'exact'
            owner.load_data = lambda name, **kwargs: pd.read_csv(root/name, **kwargs)
            with lotus.settings.context(lm=lm, enable_cache=False), \
                    patch.object(SemJoinDataframe, '__call__', join), \
                    patch.object(lm_module, 'batch_completion', reply), \
                    patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')):
                expected = getattr(owner, '_execute_q'+task.query)()
            self.assertFalse(pending)
            pd.testing.assert_frame_equal(actual, expected)
            self.assertEqual([len(batch) for batch in batches], [len(batch) for batch in reference_batches])
            for actual_batch, reference_batch in zip(batches, reference_batches, strict=True):
                self.assertEqual([self.image_uri(row['messages']) for row in actual_batch],
                                 [self.image_uri(messages) for messages in reference_batch])

    def test_fresh_process_complete_query_and_official_score_import_no_LOTUS_or_native_SDK(self):
        root = self.inputs()
        program = r'''
import importlib.abc,json,socket,sys
class NoSDK(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if (name.split('.')[0] in {'lotus','ray','daft','torch','transformers','vllm'}
      or name=='src.baselines.image.sembench_mmqa' or name.startswith('src.semantic_methods.lotus')):
   raise AssertionError('independent MMQA imported semantic/native implementation: '+name)
sys.meta_path.insert(0,NoSDK())
def offline(event,args):
 if event in ('socket.connect','socket.bind') and args[0].family in (socket.AF_INET,socket.AF_INET6):
  raise AssertionError('software fixture attempted INET')
sys.addaudithook(offline)
from src.baselines.image.sembench_native_mmqa import MMQAVisualJoinTask
for query in ('Q2a','Q2b','Q7'):
 task=MMQAVisualJoinTask(sys.argv[1],sys.argv[2],query)
 gold=task.prepare_evaluation();source=task.read_source();program=task.query_batches(source)
 rows=next(program)
 while True:
  values=[dict(call_id=row['call_id'],generated_text=json.dumps(
   {'color':'blue'} if '-extract-' in row['call_id'] else {'keep':True})) for row in rows]
  try:rows=program.send(values)
  except StopIteration as done:result=done.value;break
 assert len(result)==(6 if query=='Q7' else 10)
 assert 'f1_score' in task.evaluate(source,result,evaluation=gold)['original_metric']
 task.verify_source(source)
print('three complete MMQA visual queries and original scores; software only')
'''
        completed = subprocess.run([sys.executable, '-B', '-c', program, self.checkout(), str(root)],
            env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[3])),
            capture_output=True, text=True, timeout=60)
        self.assertEqual(completed.returncode, 0, completed.stdout+completed.stderr)
        self.assertIn('three complete', completed.stdout)


if __name__ == '__main__':
    unittest.main()
