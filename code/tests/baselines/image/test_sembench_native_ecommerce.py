"""Original E-Commerce relation/score parity with independent JSON software replies."""

from collections import deque
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.image.sembench_native_ecommerce import EcommerceVisualTask


CHECKOUT = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')


@unittest.skipUnless(CHECKOUT, 'requires the existing pinned SemBench checkout and original SDK')
class NativeEcommerceTests(unittest.TestCase):
    def setUp(self):
        import pandas as pd
        from PIL import Image

        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data = Path(self.temporary.name)
        (self.data / 'images').mkdir()
        rows = []
        for product, master, sub, article, color, brand in (
                (1, 'Footwear', 'Shoes', 'Sports Shoes', 'Yellow', 'ADIDAS'),
                (2, 'Apparel', 'Topwear', 'Tshirts', 'Blue', 'PUMA'),
                (3, 'Accessories', 'Bags', 'Bags', 'Black', 'OTHER'),
                (4, 'Apparel', 'Saree', 'Sarees', 'White', 'OTHER'),
                (5, 'Home', 'Other', 'Other', 'Orange', 'OTHER')):
            Image.new('RGB', (3, 2), ('red', 'blue', 'green', 'yellow', 'black')[product - 1]).save(
                self.data / 'images' / (str(product) + '.png'))
            rows.append(dict(id=product, masterCategory={'typeName': master}, subCategory={'typeName': sub},
                articleType={'typeName': article}, baseColour=color, colour1='Silver' if product == 1 else '', colour2='',
                brandName=brand, productDisplayName=('keep-' if product <= 2 else 'drop-') + str(product),
                productDescriptors={'description': {'value': 'product description ' + str(product)}},
                gender='Men', usage='Sports', season='Summer',
                articleAttributes={'Sleeve Length': ['Short Sleeves'], 'Neck': ['Round Neck'], 'Pattern': ['Striped']},
                gold='REFERENCE_ONLY_DO_NOT_SEND'))
        pd.DataFrame(rows).to_parquet(self.data / 'styles_details.parquet', index=False)
        pd.DataFrame(dict(id=['1', '1', '2', '3', '4', '5'],
            filename=['1.png', '1.png', '2.png', '3.png', '4.png', '5.png'])).to_parquet(
                self.data / 'image_mapping.parquet', index=False)
        connect, bind = socket.socket.connect, socket.socket.bind
        def offline(original):
            def call(sock, address):
                if sock.family in (socket.AF_INET, socket.AF_INET6):
                    raise AssertionError('E-Commerce CPU fixture accessed INET')
                return original(sock, address)
            return call
        for scope in (patch.object(socket.socket, 'connect', offline(connect)), patch.object(socket.socket, 'bind', offline(bind))):
            scope.start()
            self.addCleanup(scope.stop)

    def task(self, query):
        return EcommerceVisualTask(self.data, query, execution_id='ecomm-native')

    @staticmethod
    def predictions(requests, values):
        return [dict(call_id=row['call_id'], generated_text=json.dumps(value))
                for row, value in zip(requests, values, strict=True)]

    @staticmethod
    def images(messages):
        return [part['image_url']['url'] for row in messages for message in row
                if isinstance(message['content'], list) for part in message['content'] if part['type'] == 'image_url']

    def original(self, query, responses):
        import pandas as pd
        import lotus
        from lotus.models import LM
        from litellm.types.utils import ModelResponse
        import lotus.models.lm as lm_module
        from src.baselines.text.sembench_ecomm import bind_original_query

        pending, batches = deque(responses), []
        lm = LM('openai/CPU-ecommerce-fixture', max_tokens=64, temperature=0)
        lm.max_batch_size = 64
        def reply(*args, **kwargs):
            messages = args[1] if len(args) > 1 else kwargs['messages']
            batches.append(deepcopy(messages))
            return [ModelResponse(model=lm.model, object='chat.completion',
                choices=[dict(index=0, finish_reason='stop', message=dict(role='assistant', content=pending.popleft()))],
                usage=dict(prompt_tokens=11, completion_tokens=2, total_tokens=13)) for _ in messages]
        execute = bind_original_query(CHECKOUT, query,
            lambda name, **kwargs: pd.read_parquet(self.data / name, **kwargs), data_dir=self.data)
        with lotus.settings.context(lm=lm, enable_cache=False), patch.object(lm_module, 'batch_completion', reply):
            result = execute()
        self.assertFalse(pending)
        return result, batches

    def original_quality(self, query, result):
        from src.baselines.text.sembench_ecomm import evaluate_original

        raw = (self.data / 'styles_details.parquet').read_bytes()
        return evaluate_original(CHECKOUT, query, result, data_dir=self.data,
            source_identity=dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))

    def drive(self, task, source, plans):
        batches, generator = [], task.query_batches(source)
        requests = next(generator)
        for values in plans:
            requests = list(requests)
            batches.append(deepcopy(requests))
            predictions = list(reversed(self.predictions(requests, values)))
            try:
                requests = generator.send(predictions)
            except StopIteration as end:
                return end.value, batches
        self.fail('the task did not reach its complete relation sink')

    def test_static_filters_extract_and_map_keep_original_sources_relations_scores_and_images(self):
        cases = (
            ('Q2', [dict(keep=x) for x in [True, True, False, False, False, False]], ['True', 'True', 'False', 'False', 'False', 'False']),
            ('Q4', [dict(category=x) for x in ['Blue', 'Black', 'White', 'Orange']],
             [json.dumps(dict(category=x)) for x in ['Blue', 'Black', 'White', 'Orange']]),
            ('Q6', [dict(category='Topwear')], ['Topwear']),
            ('Q13', [dict(keep=x) for x in [False, False, True, False, False, False]],
             ['False', 'False', 'True', 'False', 'False', 'False']),
        )
        for query, values, old_values in cases:
            with self.subTest(query=query):
                task = self.task(query)
                evaluation = task.prepare_evaluation()
                source = task.read_source()
                result, batches = self.drive(task, source, [values])
                expected, originals = self.original(query, old_values)
                self.assertEqual(task.encode_result(result), task.encode_result(expected))
                self.assertEqual(task.evaluate(source, result, checkout=CHECKOUT, evaluation=evaluation),
                                 self.original_quality(query, expected))
                self.assertEqual(self.images([r['messages'] for r in batches[0]]), self.images(originals[0]))
                self.assertEqual(task.model_request_count(source), len(values))
                self.assertEqual(task.describe(source)['expected_model_calls'], len(values))
                self.assertEqual(len({r['call_id'] for r in batches[0]}), len(values))
                self.assertNotIn('REFERENCE_ONLY', json.dumps(batches))
                if query != 'Q13':
                    self.assertTrue(all(json.loads(r['messages'][1]['content'][0]['text']) == {} for r in batches[0]))
                if query == 'Q4':
                    self.assertNotIn('Black', batches[0][0]['messages'][0]['content'])
                    self.assertEqual(result.index.tolist(), [2, 3, 4, 5])
                task.verify_source(source)

    def test_q12_yields_only_real_survivors_with_id_then_materializes_original_JSON_Series(self):
        task = self.task('Q12')
        evaluation = task.prepare_evaluation()
        source = task.read_source()
        self.assertEqual(source.image_reads, [])
        self.assertEqual(task.model_request_count(source), 5)
        self.assertIsNone(task.describe(source)['expected_model_calls'])
        self.assertIsNone(task.describe(source)['logical_model_requests'])
        self.assertIsNone(task.describe(source)['actual_followup_requests'])
        self.assertEqual((task.describe(source)['minimum_model_calls'], task.describe(source)['maximum_model_calls']), (5, 10))
        products = [dict(category='footwear', brand='adidas', id=1), dict(brand='adidas', id=1, category='footwear'),
                    dict(category='apparel', id=2, brand='puma')]
        result, batches = self.drive(task, source, [[dict(keep=x) for x in [True, True, True, False, False]], products])
        canonical = [json.dumps({k: value[k] for k in ('id', 'brand', 'category')}, separators=(',', ':')) for value in products]
        expected, originals = self.original('Q12', ['True', 'True', 'True', 'False', 'False'] +
            [json.dumps(dict(generated_json=value)) for value in canonical])
        self.assertEqual(task.encode_result(result), task.encode_result(expected))
        self.assertEqual(result.name, 'id')
        self.assertEqual(result.index.tolist(), [0, 1, 2])
        self.assertEqual([len(batch) for batch in batches], [5, 3])
        self.assertEqual([len(batch) for batch in originals], [5, 3])
        self.assertEqual(len(source.image_reads), 3)
        self.assertEqual(self.images([r['messages'] for r in batches[1]]), self.images(originals[1]))
        self.assertTrue(all(isinstance(r['messages'][1]['content'], str) for r in batches[0]))
        for row in batches[1]:
            evidence = json.loads(row['messages'][1]['content'][0]['text'])
            self.assertIn(evidence['id'], (1, 2))
            self.assertNotIn('brandName', evidence)
        self.assertEqual(task.evaluate(source, result, checkout=CHECKOUT, evaluation=evaluation), self.original_quality('Q12', expected))
        task.verify_source(source)
        # Generated identities/values belong to the model, including wrong ones.
        wrong = dict(category='unlisted', brand='Módel Value', id=999)
        result, _ = self.drive(task, task.read_source(),
            [[dict(keep=x) for x in [True, False, False, False, False]], [wrong]])
        self.assertEqual(result.tolist(), ['{"id":999,"brand":"Módel Value","category":"unlisted"}'])

    def test_q4_original_extract_later_writes_cover_all_repeated_Parquet_index_labels(self):
        import pandas as pd

        path = self.data / 'image_mapping.parquet'
        images = pd.read_parquet(path)
        images.index = [6, 8, 9, 2, 9, 3]
        images.to_parquet(path, index=True)
        task = self.task('Q4')
        evaluation = task.prepare_evaluation()
        source = task.read_source()
        self.assertEqual(source.frame.index.tolist(), [9, 2, 9, 3])
        colors = ['Blue', 'Black', 'White', 'Orange']
        result, batches = self.drive(task, source, [[dict(category=color) for color in colors]])
        expected, original_batches = self.original('Q4', [json.dumps(dict(category=color)) for color in colors])
        self.assertEqual(expected['category'].tolist(), ['White', 'Black', 'White', 'Orange'])
        self.assertEqual(task.encode_result(result), task.encode_result(expected))
        self.assertEqual(task.evaluate(source, result, checkout=CHECKOUT, evaluation=evaluation),
                         self.original_quality('Q4', expected))
        self.assertEqual(self.images([r['messages'] for r in batches[0]]), self.images(original_batches[0]))
        task.verify_source(source)

    def test_q12_lazy_images_do_not_require_non_survivor_assets_and_empty_keeps_original_error(self):
        task = self.task('Q12')
        (self.data / 'images/3.png').unlink()
        task.prepare_evaluation()
        source = task.read_source()
        result, batches = self.drive(task, source, [[dict(keep=x) for x in [True, True, True, False, False]],
            [dict(id=1, brand='adidas', category='footwear'), dict(id=1, brand='adidas', category='footwear'),
             dict(id=2, brand='puma', category='apparel')]])
        self.assertEqual(len(result), 3)
        self.assertEqual([len(b) for b in batches], [5, 3])
        source = task.read_source()
        generator = task.query_batches(source)
        first = next(generator)
        second = generator.send(self.predictions(first, [dict(keep=False)] * len(first)))
        self.assertEqual(second, [])
        with self.assertRaises(KeyError) as actual:
            generator.send([])
        with self.assertRaises(KeyError) as original:
            self.original('Q12', ['False'] * 5)
        self.assertEqual(str(actual.exception), str(original.exception))
        self.assertEqual(source.image_reads, [])

    def test_empty_extract_and_map_follow_original_column_creation(self):
        import pandas as pd

        # Keep the real schema, but make both ordinary predicates empty.
        styles = pd.read_parquet(self.data / 'styles_details.parquet')
        styles['baseColour'] = 'Yellow'
        styles['masterCategory'] = [{'typeName': 'Home'}] * len(styles)
        styles.to_parquet(self.data / 'styles_details.parquet', index=False)
        for query in ('Q4', 'Q6'):
            task = self.task(query)
            task.prepare_evaluation()
            source = task.read_source()
            self.assertEqual(task.requests(source), [])
            if query == 'Q4':
                with self.assertRaises(KeyError) as actual:
                    task.finish(source, [])
                with self.assertRaises(KeyError) as expected:
                    self.original(query, [])
                self.assertEqual(str(actual.exception), str(expected.exception))
            else:
                expected, _ = self.original(query, [])
                self.assertEqual(task.encode_result(task.finish(source, [])), task.encode_result(expected))

    def test_strict_occurrence_JSON_errors_and_unsupported_algorithms_are_explicit(self):
        task = self.task('Q4')
        source = task.read_source()
        requests = task.requests(source)
        predictions = self.predictions(requests, [dict(category='Blue')] * len(requests))
        for wrong in (predictions[:-1], predictions + predictions[:1], [dict(call_id='other', generated_text='{}')]):
            with self.subTest(wrong=wrong), self.assertRaises(ValueError):
                task.finish(source, wrong)
        for value in ('{"category":1}', '{"category":"x","category":"y"}', '{"category":"x","extra":0}'):
            wrong = deepcopy(predictions)
            wrong[0]['generated_text'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                task.finish(source, wrong)
        for query in ('Q14',):
            with self.assertRaisesRegex(ValueError, 'TopK'):
                self.task(query)
        mixed = self.task('Q12')
        for value in ('{"id":true,"brand":"x","category":"y"}',
                      '{"id":1,"brand":"x","category":"y","extra":0}',
                      '{"id":1,"id":2,"brand":"x","category":"y"}'):
            generator = mixed.query_batches(mixed.read_source())
            first = next(generator)
            second = generator.send(self.predictions(first, [dict(keep=x) for x in [True, False, False, False, False]]))
            with self.subTest(product=value), self.assertRaises(ValueError):
                generator.send([dict(call_id=second[0]['call_id'], generated_text=value)])

    def join_inputs(self, query, *, repeated_labels=False, missing_ids=False):
        import pandas as pd

        if not hasattr(self, 'join_original_files'):
            self.join_original_files = {name: (self.data / name).read_bytes()
                for name in ('styles_details.parquet', 'image_mapping.parquet')}
        for name, raw in self.join_original_files.items():
            (self.data / name).write_bytes(raw)
        path = self.data / 'styles_details.parquet'
        styles = pd.read_parquet(path)
        if query == 'Q8':
            for i in (0, 1):
                styles.at[i, 'productDescriptors'] = {'description': {'value': 'complete product evidence ' * 150}}
            if missing_ids:
                styles.at[0, 'id'] = float('nan')
        else:
            styles['price'] = 80
            styles.at[0, 'baseColour'] = 'Blue'
            styles.at[0, 'colour1'] = ''
        if repeated_labels:
            styles.index = [9, 9, 2, 7, 5]
        styles.to_parquet(path, index=True)
        path = self.data / 'image_mapping.parquet'
        images = pd.read_parquet(path)
        if missing_ids:
            images.loc[0, 'id'] = None
        if repeated_labels:
            images.index = [9, 2, 9, 7, float('nan'), 5]
        images.to_parquet(path, index=True)

    def test_exact_join_counter_precedes_pair_images_and_full_original_relation_score(self):
        for query, count in [('Q8', 12), ('Q9', 36)]:
            with self.subTest(query=query):
                self.join_inputs(query)
                task = self.task(query)
                evaluation = task.prepare_evaluation()
                source = task.read_source()
                with patch.object(task, '_image', side_effect=AssertionError('counter formatted image payload')):
                    self.assertEqual(task.model_request_count(source), count)
                    self.assertEqual(task.describe(source)['expected_model_calls'], count)
                flags = [i % 3 == 0 for i in range(count)]
                result, batches = self.drive(task, source, [[dict(keep=keep) for keep in flags]])
                expected, original_batches = self.original(query, ['True' if keep else 'False' for keep in flags])
                self.assertEqual(task.encode_result(result), task.encode_result(expected))
                self.assertEqual(task.evaluate(source, result, checkout=CHECKOUT, evaluation=evaluation),
                                 self.original_quality(query, expected))
                self.assertEqual(len({r['call_id'] for r in batches[0]}), count)

                self.assertEqual(self.images([r['messages'] for r in batches[0]]), self.images(original_batches[0]))
                self.assertEqual(len(source.image_reads), len(source.right_frame))
                self.assertNotIn('REFERENCE_ONLY', json.dumps(batches))
                self.assertIn('not author-original or LOTUS Join', task.describe(source)['join_method'])
                self.assertIn('does not repair', task.describe(source)['original_LOTUS_multi_column_limit'])
                if query == 'Q8':
                    evidence = json.loads(batches[0][0]['messages'][1]['content'][0]['text'])
                    self.assertEqual(set(evidence), {'productDisplayName', 'productDescriptors'})
                    self.assertGreater(len(evidence['productDescriptors']), 3000)
                task.verify_source(source)

    def outfit_inputs(self):
        import pandas as pd

        path = self.data / 'styles_details.parquet'
        styles = pd.read_parquet(path)
        styles['baseColour'] = 'Black'
        styles['price'] = [80, 80, 80, 80, 700]
        styles.to_parquet(path, index=False)

    def test_dependent_outfits_match_complete_original_relations_and_actual_stage_counts(self):
        self.outfit_inputs()
        footwear = [True, True, False, False, False, False]
        bottomwear = [False, False, True, False, False, False]
        topwear = [False, False, False, True, False, False]
        for query in ('Q10', 'Q11'):
            for mode in ('match', 'first-join-empty', 'no-filter-match'):
                with self.subTest(query=query, mode=mode):
                    task = self.task(query)
                    evaluation = task.prepare_evaluation()
                    source = task.read_source()
                    self.assertEqual(task.model_request_count(source), 23 if query == 'Q11' else 18)
                    self.assertIsNone(task.describe(source)['expected_model_calls'])
                    filters = [footwear, bottomwear, topwear]
                    if query == 'Q11':
                        filters.append([False, False, False, False, True])
                    if mode == 'no-filter-match':
                        filters = [[False]*len(flags) for flags in filters]
                    first = [True, False] if mode == 'match' else [False, False] if mode == 'first-join-empty' else []
                    joins = [first]
                    if query == 'Q11':
                        joins.append([True] if mode != 'no-filter-match' else [])
                    joins.append([True] if mode == 'match' else [])
                    plans = [[dict(keep=value) for value in flags] for flags in filters+joins]
                    actual, batches = self.drive(task, source, plans)
                    expected, original = self.original(query,
                        ['True' if value else 'False' for flags in filters+joins for value in flags])
                    self.assertEqual(task.encode_result(actual), task.encode_result(expected))
                    self.assertEqual([len(rows) for rows in batches], [len(flags) for flags in filters+joins])
                    self.assertEqual([len(rows) for rows in original], [len(flags) for flags in filters+joins])
                    self.assertEqual(source.query_batch_counts, [len(flags) for flags in filters+joins])
                    self.assertEqual(task.evaluate(source, actual, checkout=CHECKOUT, evaluation=evaluation),
                                     self.original_quality(query, expected))
                    self.assertEqual(len({row['call_id'] for rows in batches for row in rows}), sum(map(len, batches)))
                    for rows in batches[len(filters):]:
                        for row in rows:
                            self.assertEqual(len(self.images([row['messages']])), 2)
                    task.verify_source(source)

    def test_dependent_pair_count_does_not_materialize_messages_or_read_pair_images(self):
        self.outfit_inputs()
        for query, filters in [('Q10', 3), ('Q11', 4)]:
            task = self.task(query)
            source = task.read_source()
            program = task.query_batches(source)
            requests = next(program)
            for index in range(filters):
                count = len(requests)
                predictions = self.predictions(requests, [dict(keep=True)]*count)
                with patch.object(task, '_product_pair_messages', side_effect=AssertionError('expanded pair work')):
                    requests = program.send(predictions)
            reads = len(source.image_reads)
            with patch.object(task, '_image', side_effect=AssertionError('pair count decoded an image')):
                self.assertEqual(len(requests), 30 if query == 'Q11' else 36)
                self.assertEqual(len(source.image_reads), reads)
            program.close()

    def test_join_repeated_index_labels_IDs_and_NaNs_keep_original_joinback_expansion(self):
        for query, missing_ids in [('Q8', True), ('Q9', False)]:
            with self.subTest(query=query):
                self.join_inputs(query, repeated_labels=True, missing_ids=missing_ids)
                task = self.task(query)
                evaluation = task.prepare_evaluation()
                source = task.read_source()
                count = task.model_request_count(source)
                result, batches = self.drive(task, source, [[dict(keep=True)] * count])
                expected, _ = self.original(query, ['True'] * count)
                self.assertEqual(task.encode_result(result), task.encode_result(expected))
                self.assertEqual(json.dumps(task.evaluate(source, result, checkout=CHECKOUT, evaluation=evaluation), sort_keys=True),
                                 json.dumps(self.original_quality(query, expected), sort_keys=True))
                self.assertEqual(len({r['call_id'] for r in batches[0]}), count)
                self.assertGreater(len(result), count)
                if query == 'Q9':
                    self.assertFalse(any(left == right for left, right in (value.split('-') for value in result)))
                    self.assertNotEqual(result.index.tolist(), list(range(len(result))))
                task.verify_source(source)

    def test_exact_join_empty_and_no_matches_keep_original_empty_series_and_all_pairs(self):
        import pandas as pd

        for query in ('Q8', 'Q9'):
            with self.subTest(query=query):
                self.join_inputs(query)
                task = self.task(query)
                source = task.read_source()
                count = task.model_request_count(source)
                result, batches = self.drive(task, source, [[dict(keep=False)] * count])
                expected, _ = self.original(query, ['False'] * count)
                self.assertEqual(task.encode_result(result), task.encode_result(expected))
                self.assertEqual(len(batches[0]), count)
                path = self.data / 'styles_details.parquet'
                styles = pd.read_parquet(path)
                if query == 'Q8':
                    styles['productDescriptors'] = [{'description': {'value': 'short'}}] * len(styles)
                else:
                    styles['price'] = 1000
                styles.to_parquet(path, index=True)
                source = task.read_source()
                self.assertEqual(task.model_request_count(source), 0)
                result, batches = self.drive(task, source, [[]])
                expected, _ = self.original(query, [])
                self.assertEqual(task.encode_result(result), task.encode_result(expected))
                self.assertEqual(batches, [[]])
                self.assertEqual(source.image_reads, [])

    def test_q9_missing_raw_ID_keeps_original_int_conversion_error_before_any_request(self):
        self.join_inputs('Q9', missing_ids=True)
        with self.assertRaises((ValueError, TypeError)) as actual:
            self.task('Q9').read_source()
        with self.assertRaises((ValueError, TypeError)) as original:
            self.original('Q9', [])
        self.assertEqual(type(actual.exception), type(original.exception))
        self.assertEqual(str(actual.exception), str(original.exception))

    def test_fresh_process_exact_join_and_official_score_need_no_LOTUS(self):
        self.join_inputs('Q9')
        script = '''
import importlib.abc,json,socket,sys
class RejectLOTUS(importlib.abc.MetaPathFinder):
    def find_spec(self,name,*args):
        if name=='lotus' or name.startswith('lotus.') or name.startswith('src.semantic_methods.lotus') or name in {'src.baselines.text.sembench_text','src.baselines.text.sembench_prepared'}:
            raise AssertionError('independent join imported '+name)
sys.meta_path.insert(0,RejectLOTUS())
def denied(*args,**kwargs):raise AssertionError('fresh join accessed network')
socket.create_connection=denied
from src.baselines.image.sembench_native_ecommerce import EcommerceVisualTask
task=EcommerceVisualTask(sys.argv[1],'Q9','fresh-join')
evaluation=task.prepare_evaluation();source=task.read_source()
assert task.model_request_count(source)==36 and not source.image_reads
requests=task.requests(source)
assert len(requests)==36 and len(source.image_reads)==6
predictions=[{'call_id':r['call_id'],'generated_text':'{"keep":true}'} for r in reversed(requests)]
result=task.finish(source,predictions);task.verify_source(source)
quality=task.evaluate(source,result,checkout=sys.argv[2],evaluation=evaluation)
assert quality['sembench_commit']=='c814e3807e72d4cf876b852b17e77f3cc94575c2'
assert not any(name=='lotus' or name.startswith('lotus.') for name in sys.modules)
print('fresh noLOTUS exact36 plus full original score passed')
'''
        completed = subprocess.run([sys.executable, '-B', '-c', script, str(self.data), str(CHECKOUT)],
                                   capture_output=True, text=True, timeout=45)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_missing_needed_image_and_postquery_changes_fail_without_gold_or_ID_repair(self):
        task = self.task('Q2')
        evaluation = task.prepare_evaluation()
        source = task.read_source()
        path = self.data / 'images/5.png'
        raw = path.read_bytes()
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            task.requests(source)
        path.write_bytes(raw)
        source = task.read_source()
        requests = task.requests(source)
        result = task.finish(source, self.predictions(requests, [dict(keep=False)] * len(requests)))
        quality = task.evaluate(source, result, checkout=CHECKOUT, evaluation=evaluation)
        (self.data / 'styles_details.parquet').write_bytes(b'not the frozen gold')
        self.assertEqual(task.evaluate(source, result, checkout=CHECKOUT, evaluation=evaluation), quality)
        path.write_bytes(b'changed after the actual image read')
        with self.assertRaisesRegex(ValueError, 'image changed'):
            task.verify_source(source)
        path.write_bytes(raw)
        mapping = self.data / 'image_mapping.parquet'
        mapping.write_bytes(mapping.read_bytes() + b'changed')
        with self.assertRaisesRegex(ValueError, 'source changed'):
            task.verify_source(source)

    def test_fresh_process_runs_mixed_task_and_official_score_without_LOTUS(self):
        script = '''
import importlib.abc,json,socket,sys
class RejectLOTUS(importlib.abc.MetaPathFinder):
    def find_spec(self,name,*args):
        if name=='lotus' or name.startswith('lotus.') or name.startswith('src.semantic_methods.lotus') or name in {'src.baselines.text.sembench_text','src.baselines.text.sembench_prepared'}:
            raise AssertionError('independent task imported '+name)
sys.meta_path.insert(0,RejectLOTUS())
def denied(*args,**kwargs):raise AssertionError('fresh task accessed network')
socket.create_connection=denied
from src.baselines.image.sembench_native_ecommerce import EcommerceVisualTask
task=EcommerceVisualTask(sys.argv[1],'Q12','fresh')
evaluation=task.prepare_evaluation();source=task.read_source();batches=task.query_batches(source)
first=next(batches)
answers=[{'call_id':r['call_id'],'generated_text':json.dumps({'keep':json.loads(r['messages'][1]['content'])['productDisplayName'].startswith('keep-')})} for r in first]
second=batches.send(answers);answers=[]
for r in second:
    value=json.loads(r['messages'][1]['content'][0]['text']);product=value['id']
    answers.append({'call_id':r['call_id'],'generated_text':json.dumps({'id':product,'brand':'adidas' if product==1 else 'puma','category':'footwear' if product==1 else 'apparel'})})
try:batches.send(answers)
except StopIteration as end:result=end.value
assert len(first)==5 and len(second)==3 and result.name=='id'
task.verify_source(source)
quality=task.evaluate(source,result,checkout=sys.argv[2],evaluation=evaluation)
assert quality['original_metric']['accuracy']==1
assert not any(name=='lotus' or name.startswith('lotus.') for name in sys.modules)
print('fresh noLOTUS mixed5+3 and original score passed')
'''
        completed = subprocess.run([sys.executable, '-B', '-c', script, str(self.data), str(CHECKOUT)],
                                   capture_output=True, text=True, timeout=45)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)


if __name__ == '__main__':
    unittest.main()
