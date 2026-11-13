"""Independent E-Commerce scalar text tasks and fixed original CPU references."""

from contextlib import ExitStack, redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

from src.baselines.text.sembench_native_ecommerce import EcommerceTextTask, ecommerce_text_task


class NativeEcommerceTextTests(unittest.TestCase):
    def inputs(self, *, duplicate_ids=False):
        import pandas as pd

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        frame = pd.DataFrame(dict(id=[11, 11 if duplicate_ids else 22, 33, 44, 55, 66],
            productDisplayName=['same title', 'same title', None, 'excluded watch', 'excluded sari', 'excluded set'],
            productDescriptors=[{'description': {'value': 'whole nested «好»\ntext'}, 'extra': 'keep whole object'}]*6,
            masterCategory=[{'typeName': value} for value in
                ['Apparel', 'Apparel', 'Apparel', 'Accessories', 'Apparel', 'Apparel']],
            subCategory=[{'typeName': value} for value in
                ['Topwear', 'Topwear', 'Socks', 'Watches', 'Saree', 'Apparel Set']],
            articleType=[{'typeName': value} for value in
                ['Backpacks', 'Backpacks', 'Socks', 'Watches', 'Sarees', 'Shirts']],
            price=[300, 300, 200, 700, 1000, 600], brandName=['Reebok', 'Reebok', 'Zen', 'Acme', 'Acme', 'Zen']),
            index=[9, 2, 9, 6, 4, 8])
        frame.to_parquet(root/'styles_details.parquet')
        return root

    @staticmethod
    def predictions(task, source, values):
        key = 'keep' if task.query == '1' else 'category'
        return [dict(call_id=row['call_id'], generated_text=json.dumps({key: value}))
                for row, value in zip(task.requests(source), values, strict=True)]

    def checkout(self):
        checkout = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')
        if not checkout:
            self.skipTest('bind fixed public SemBench checkout for original scorer/reference checks')
        return checkout

    def test_construct_without_io_and_only_implemented_scalar_families(self):
        with patch.object(Path, 'read_bytes', side_effect=AssertionError('constructor I/O')):
            for query in ('Q1', 'Q3', 'Q5'):
                task = ecommerce_text_task('/not-read', query, 'ecomm-'+query)
                self.assertIs(type(task), EcommerceTextTask)
                self.assertEqual(task.query, query[1:])
            for query in ('Q2', 'Q7', 'Q12', True):
                with self.subTest(query=query), self.assertRaises(ValueError):
                    EcommerceTextTask('/not-read', query)

    def test_original_selection_projection_index_and_duplicate_occurrences(self):
        import pandas as pd

        root = self.inputs(duplicate_ids=True)
        for query in ('Q1', 'Q3', 'Q5'):
            task = EcommerceTextTask(root, query)
            source = task.read_source()
            self.assertEqual(task.model_request_count(source), 3 if query == 'Q5' else 6)
            values = ([True, True, False, False, False, False] if query == 'Q1' else
                      ['Topwear', 'Topwear', 'Socks'] if query == 'Q5' else ['Reebok', None, 'a', 'b', 'c', 'd'])
            result = task.finish(source, reversed(self.predictions(task, source, values)))
            if query == 'Q1':
                pd.testing.assert_series_equal(result, pd.Series([11, 11], index=[9, 2], name='id'))
            else:
                self.assertEqual(result.index.tolist(), [9, 2, 9] if query == 'Q5' else [9, 2, 9, 6, 4, 8])
                expected = [str(value) for value in values]
                if query == 'Q3':
                    expected[0] = expected[2]
                self.assertEqual(result['category'].tolist(), expected)
                self.assertEqual(result.columns.tolist(), ['id', 'category'])

    def test_only_whole_title_descriptor_evidence_enters_messages_not_gold_or_ids(self):
        for query in ('Q1', 'Q3', 'Q5'):
            task = EcommerceTextTask(self.inputs(), query, 'occurrences-'+query)
            source = task.read_source()
            requests = task.requests(source)
            self.assertEqual(len({row['call_id'] for row in requests}), len(requests))
            self.assertEqual(requests[0]['messages'], requests[1]['messages'])
            self.assertNotEqual(requests[0]['call_id'], requests[1]['call_id'])
            for request in requests:
                payload = json.loads(request['messages'][1]['content'])
                evidence = payload['text'] if query == 'Q1' else payload
                self.assertEqual(set(evidence), {'productDisplayName', 'productDescriptors'})
                self.assertIn('keep whole object', evidence['productDescriptors'])
                self.assertIn('description', evidence['productDescriptors'])
                self.assertIn('«好»', evidence['productDescriptors'])
            evidence = json.loads(requests[2]['messages'][1]['content'])
            if query == 'Q1':
                evidence = evidence['text']
            self.assertEqual(evidence['productDisplayName'], '')
            self.assertEqual(task.response_format, {'type': 'json_object'})

    def test_strict_json_and_complete_occurrences_are_not_silent_defaults(self):
        for query in ('Q1', 'Q3', 'Q5'):
            task = EcommerceTextTask(self.inputs(), query)
            source = task.read_source()
            count = task.model_request_count(source)
            rows = self.predictions(task, source, [True if query == 'Q1' else 'value']*count)
            for altered in (rows[:-1], rows+[rows[0]], rows[:-1]+[dict(rows[-1], call_id='foreign')]):
                with self.subTest(query=query, rows=altered), self.assertRaises(ValueError):
                    task.finish(source, altered)
            key = 'keep' if query == 'Q1' else 'category'
            for answer in (None, 'not JSON', '[]', '{}', '{"'+key+'":3}', '{"'+key+'":[]}',
                           '{"'+key+'":"a","'+key+'":"b"}', '{"'+key+'":null,"extra":1}',
                           '{"keep":null}' if query == 'Q1' else '{"category":true}'):
                with self.subTest(query=query, answer=answer), self.assertRaises(ValueError):
                    task.finish(source, [dict(rows[0], generated_text=answer)]+rows[1:])
            if query != 'Q1':
                mapped = task.finish(source, self.predictions(task, source, [None]*count))
                self.assertEqual(mapped['category'].tolist(), ['None']*count)
                arbitrary = task.finish(source, self.predictions(task, source, ['  unknown label  ']*count))
                self.assertEqual(arbitrary['category'].tolist(), ['  unknown label  ']*count)

    def test_empty_frontier_and_extract_zero_output_error_preserve_original_contracts(self):
        import pandas as pd

        root = self.inputs()
        frame = pd.read_parquet(root/'styles_details.parquet')
        frame.iloc[3:].to_parquet(root/'styles_details.parquet')
        task = EcommerceTextTask(root, 'Q5')
        source = task.read_source()
        self.assertEqual(task.model_request_count(source), 0)
        result = task.finish(source, [])
        self.assertEqual(result.columns.tolist(), ['id', 'category'])
        self.assertEqual(str(result['category'].dtype), 'float64')
        frame.iloc[:0].to_parquet(root/'styles_details.parquet')
        q1 = EcommerceTextTask(root, 'Q1')
        source = q1.read_source()
        pd.testing.assert_series_equal(q1.finish(source, []), source.frame['id'])
        q3 = EcommerceTextTask(root, 'Q3')
        with self.assertRaisesRegex(KeyError, 'category'):
            q3.finish(q3.read_source(), [])

    def test_missing_columns_and_changed_source_are_explicit_and_metadata_has_no_io(self):
        import pandas as pd

        root = self.inputs()
        task = EcommerceTextTask(root, 'Q5')
        source = task.read_source()
        with patch.object(Path, 'read_bytes', side_effect=AssertionError('metadata I/O')):
            self.assertEqual(task.model_request_count(source), 3)
            metadata = task.describe(source)
        self.assertEqual(metadata['expected_model_calls'], 3)
        self.assertEqual(metadata['source']['styles_details.parquet']['sha256'],
                         hashlib.sha256(source.raw_bytes).hexdigest())
        path = root/'styles_details.parquet'
        source.frame.iloc[:1].to_parquet(path)
        with self.assertRaisesRegex(ValueError, 'changed'):
            task.verify_source(source)
        task.verify_source(task.read_source())
        source.frame.drop(columns=['productDescriptors']).to_parquet(path)
        with self.assertRaisesRegex(ValueError, 'productDescriptors'):
            task.read_source()
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            task.read_source()
        with self.assertRaises(FileNotFoundError):
            task.prepare_evaluation()
        pd.DataFrame(dict(id=[1], productDisplayName=['evidence'],
                          productDescriptors=[{'description': {'value': 'evidence'}}])).to_parquet(path)
        with self.assertRaisesRegex(ValueError, 'evaluation'):
            task.prepare_evaluation()

    def test_frozen_same_byte_gold_uses_original_sql_metrics_and_leaves_raw_sink_unchanged(self):
        import pandas as pd

        for query in ('Q1', 'Q3', 'Q5'):
            task = EcommerceTextTask(self.inputs(), query)
            evaluation = task.prepare_evaluation()
            source = task.read_source()
            values = ([True, True, False, False, False, False] if query == 'Q1' else
                      ['Topwear', 'Topwear', 'Socks'] if query == 'Q5' else ['Reebok', 'Reebok', 'Reebok', 'Acme', 'Acme', 'Zen'])
            result = task.finish(source, self.predictions(task, source, values))
            before = result.copy(deep=True)
            # Path mutation cannot change this already frozen source/gold run.
            (task.data_dir/'styles_details.parquet').write_bytes(b'changed after execution')
            quality = task.evaluate(source, result, checkout=self.checkout(), evaluation=evaluation)
            self.assertEqual(quality['original_metric']['metric_type'],
                             'f1-score' if query == 'Q1' else 'adjusted-rand-index')
            if query != 'Q3':
                self.assertEqual(quality['original_metric']['accuracy'], 1)
            self.assertEqual(quality['gold_source']['sha256'], hashlib.sha256(evaluation.raw_bytes).hexdigest())
            if query == 'Q1':
                pd.testing.assert_series_equal(result, before)
            else:
                pd.testing.assert_frame_equal(result, before)
            with self.assertRaisesRegex(ValueError, 'prepare_evaluation'):
                task.evaluate(source, result, checkout=self.checkout())
            with self.assertRaisesRegex(ValueError, 'changed'):
                task.verify_source(source)

    def test_query_gold_snapshot_mismatch_and_original_duplicate_id_metric_error_are_not_masked(self):
        import pandas as pd

        root = self.inputs()
        task = EcommerceTextTask(root, 'Q3')
        evaluation = task.prepare_evaluation()
        frame = pd.read_parquet(root/'styles_details.parquet')
        frame.iloc[:1].to_parquet(root/'styles_details.parquet')
        source = task.read_source()
        result = task.finish(source, self.predictions(task, source, ['Reebok']))
        with self.assertRaisesRegex(ValueError, 'frozen evaluation'):
            task.evaluate(source, result, checkout=self.checkout(), evaluation=evaluation)
        task = EcommerceTextTask(self.inputs(duplicate_ids=True), 'Q3')
        source = task.read_source()
        result = task.finish(source, self.predictions(task, source, ['brand']*6))
        with self.assertRaises(ValueError):
            task.evaluate(source, result, checkout=self.checkout(), evaluation=task.prepare_evaluation())
        self.assertEqual(len(task.encode_result(result)['data']), 6)

    def test_shared_full_original_query_sdk_differential_including_empty_and_null_relations(self):
        checkout = self.checkout()
        if any(importlib.util.find_spec(name) is None for name in ('lotus', 'litellm')):
            self.skipTest('real LOTUS SDK is required only for the original-query software reference')
        with ExitStack() as scope:
            scope.enter_context(patch.dict(os.environ, dict(HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1',
                HF_DATASETS_OFFLINE='1', LITELLM_LOCAL_MODEL_COST_MAP='True', CUDA_VISIBLE_DEVICES='')))
            scope.enter_context(patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')))
            import lotus
            import pandas as pd
            from tests.experiments.test_sembench_text_original import OriginalSemBenchTextTests
            from src.serving.backends.vllm_protocol import GenerationSpec
            from src.serving.backends.vllm_engine import VllmEngine
            from src.serving.backends.vllm_async_engine import VllmAsyncEngine

            for engine in (VllmEngine, VllmAsyncEngine):
                scope.enter_context(patch.object(engine, '__init__', side_effect=AssertionError('model forbidden')))
            scope.enter_context(lotus.settings.context(enable_cache=False))
            original = OriginalSemBenchTextTests()
            original.generation = GenerationSpec(max_tokens=32, temperature=0, seed=0)
            for query in ('Q1', 'Q3', 'Q5'):
                for mode in ('duplicates', 'negative-or-null', 'empty-frontier', 'empty-source'):
                    with self.subTest(query=query, mode=mode), redirect_stdout(io.StringIO()):
                        root = self.inputs(duplicate_ids=True)
                        path = root/'styles_details.parquet'
                        frame = pd.read_parquet(path)
                        if mode == 'empty-frontier':
                            frame.iloc[3:].to_parquet(path)
                        elif mode == 'empty-source':
                            frame.iloc[:0].to_parquet(path)
                        task = EcommerceTextTask(root, query)
                        source = task.read_source()
                        count = task.model_request_count(source)
                        values = ([mode != 'negative-or-null' and i % 2 == 0 for i in range(count)] if query == 'Q1'
                                  else [None if mode == 'negative-or-null' else ['  a  ', 'b', 'c'][i % 3]
                                        for i in range(count)])
                        answers = (['True' if value else 'False' for value in values] if query == 'Q1' else
                                   [json.dumps({'category': value}) for value in values] if query == 'Q3' else
                                   [str(value) for value in values])
                        original.data = root
                        expected_error = actual_error = None
                        try:
                            reference = original._query('ecomm', task.query)
                            self.assertEqual(reference.operation.describe()['expected_model_calls'], count)
                            expected, batches = original._reference(reference, answers)
                        except (KeyError, ValueError) as error:
                            expected_error = error
                        try:
                            actual = task.finish(source, reversed(self.predictions(task, source, values)))
                        except (KeyError, ValueError) as error:
                            actual_error = error
                        self.assertEqual(type(actual_error), type(expected_error))
                        if expected_error is not None:
                            self.assertEqual(str(actual_error), str(expected_error))
                        else:
                            self.assertEqual(sum(len(messages) for messages, _ in batches), count)
                            if query == 'Q1':
                                pd.testing.assert_series_equal(actual, expected)
                            else:
                                pd.testing.assert_frame_equal(actual, expected)

    def test_fresh_task_and_original_sql_scoring_forbid_lotus_and_network(self):
        script = textwrap.dedent('''
            import importlib.abc,json,socket,sys
            class NoLotus(importlib.abc.MetaPathFinder):
                def find_spec(self,fullname,path=None,target=None):
                    blocked=('lotus','semantic_methods.lotus','src.semantic_methods.lotus',
                        'src.baselines.text.sembench_text','src.baselines.text.sembench_prepared',
                        'src.experiments','tests')
                    if any(fullname==name or fullname.startswith(name+'.') for name in blocked):
                        raise AssertionError('forbidden runtime dependency: '+fullname)
            sys.meta_path.insert(0,NoLotus())
            def no_network(*args,**kwargs): raise AssertionError('network forbidden')
            socket.socket.connect=no_network
            from src.baselines.text.sembench_native_ecommerce import ecommerce_text_task
            for query in ('Q1','Q3','Q5'):
                task=ecommerce_text_task(sys.argv[1],query,'fresh-'+query)
                gold=task.prepare_evaluation()
                source=task.read_source()
                requests=task.requests(source)
                values=([True,True,False,False,False,False] if query=='Q1' else
                    ['Topwear','Topwear','Socks'] if query=='Q5' else ['Reebok','Reebok','Reebok','Acme','Acme','Zen'])
                key='keep' if query=='Q1' else 'category'
                predictions=[dict(call_id=row['call_id'],generated_text=json.dumps({key:value}))
                    for row,value in zip(requests,values,strict=True)]
                result=task.finish(source,predictions)
                task.verify_source(source)
                json.dumps(task.encode_result(result),allow_nan=False)
                quality=task.evaluate(source,result,checkout=sys.argv[2],evaluation=gold)
                assert 'original_metric' in quality
                assert not any(name=='lotus' or name.startswith('lotus.') for name in sys.modules)
            print('fresh-ecommerce-no-lotus: passed')
        ''')
        environment = dict(os.environ, CUDA_VISIBLE_DEVICES='', HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1',
                           LITELLM_LOCAL_MODEL_COST_MAP='True')
        with tempfile.TemporaryDirectory() as cache:
            environment['MPLCONFIGDIR'] = cache
            result = subprocess.run([sys.executable, '-c', script, str(self.inputs()), self.checkout()],
                text=True, capture_output=True, timeout=60, env=environment)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertIn('fresh-ecommerce-no-lotus: passed', result.stdout)


if __name__ == '__main__':
    unittest.main()
