"""Independent Medical task contracts and original-query CPU references."""

from contextlib import ExitStack, redirect_stdout
import csv
import hashlib
import importlib.util
import io
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

from src.baselines.text.sembench_native_medical import MedicalTextTask, medical_text_task


class NativeMedicalTextTests(unittest.TestCase):
    @staticmethod
    def write(path, columns, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        stream = io.StringIO(newline='')
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerows(rows)
        path.write_bytes(stream.getvalue().encode())

    def inputs(self, scale_factor=11112):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        suffix = '' if scale_factor == 11112 else '_'+str(scale_factor)
        self.write(root/'data'/('text_symptoms_data'+suffix+'.csv'),
            ['patient_id', 'symptoms', 'symptom_id'],
            [(7, 'same symptom evidence «好»\nsecond line', 'hidden-one'),
             (3, 'skin evidence', 'hidden-two'),
             (7, 'same symptom evidence «好»\nsecond line', 'hidden-three'),
             (5, None, 'hidden-four')])
        self.write(root/'data'/('patient_data'+suffix+'.csv'), ['patient_id', 'age'],
            [(3, 10), (7, 40), (7, 60), (5, 100), (9, 90)])
        self.write(root/'data'/('patient_data_with_labels'+suffix+'.csv'),
            ['patient_id', 'age', 'gender', 'smoking_history', 'did_family_have_cancer', 'text_diagnosis'],
            [(3, 10, 'F', 'Never', 0, 'acne'), (7, 40, 'M', 'Never', 0, 'allergy'),
             (5, 100, 'F', 'Never', 0, 'none'), (9, 90, 'M', 'Never', 0, 'none')])
        return root

    @staticmethod
    def predictions(task, source, values):
        key = 'text_diagnosis' if task.query == '10' else 'keep'
        return [dict(call_id=row['call_id'], generated_text=json.dumps({key: value}))
                for row, value in zip(task.requests(source), values, strict=True)]

    def checkout(self):
        checkout = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')
        if not checkout:
            self.skipTest('bind fixed public SemBench checkout for original scorer/reference checks')
        return checkout

    def test_constructor_is_io_free_and_scale_selects_only_original_filenames(self):
        with patch.object(Path, 'read_bytes', side_effect=AssertionError('constructor I/O')):
            for query in ('Q1', 'Q4', 'Q10'):
                task = medical_text_task('/not-read', query, 'medical-'+query)
                self.assertIs(type(task), MedicalTextTask)
                self.assertEqual(task.scale_factor, 11112)
                self.assertEqual(task.query, query[1:])
            for invalid in ('Q2', 'Q3', 'Q5', 'Q8', True):
                with self.subTest(query=invalid), self.assertRaises(ValueError):
                    MedicalTextTask('/not-read', invalid)
            for invalid in (0, True, '37', 37.0):
                with self.subTest(scale=invalid), self.assertRaises(ValueError):
                    MedicalTextTask('/not-read', scale_factor=invalid)
        task = MedicalTextTask(self.inputs(37), 'Q4', scale_factor=37)
        source = task.read_source()
        self.assertEqual(task.model_request_count(source), 6)
        self.assertEqual(set(source.raw_files), {'data/text_symptoms_data_37.csv', 'data/patient_data_37.csv'})

    def test_q1_preserves_series_and_q4_joins_all_occurrences_before_filter_and_mean(self):
        import pandas as pd

        root = self.inputs()
        q1 = MedicalTextTask(root, 'Q1', 'series')
        source = q1.read_source()
        result = q1.finish(source, reversed(self.predictions(q1, source, [True, False, True, False])))
        pd.testing.assert_series_equal(result, pd.Series([7, 7], index=[0, 2], name='patient_id'))
        self.assertEqual(q1.encode_result(result), dict(kind='series', name='patient_id',
            index=[0, 2], data=[7, 7], dtype='int64'))
        q4 = MedicalTextTask(root, 'Q4', 'average')
        source = q4.read_source()
        self.assertEqual(source.relation.index.tolist(), [0, 1, 1, 2, 2, 3])
        self.assertEqual(source.relation['patient_id'].tolist(), [3, 7, 7, 7, 7, 5])
        self.assertEqual(q4.model_request_count(source), 6)
        result = q4.finish(source, self.predictions(q4, source, [True]*6))
        self.assertEqual(result.iloc[0, 0], 310/6)
        self.assertEqual(result.columns.tolist(), ['average_acne_age'])
        empty = q4.finish(source, self.predictions(q4, source, [False]*6))
        self.assertTrue(math.isnan(empty.iloc[0, 0]))
        self.assertEqual(q4.encode_result(empty)['data'], [['NaN']])

    def test_extract_preserves_all_rows_strings_null_and_unrecognized_labels(self):
        import pandas as pd

        task = MedicalTextTask(self.inputs(), 'Q10')
        source = task.read_source()
        result = task.finish(source, reversed(self.predictions(task, source,
            ['ALLERGY', 'a label outside the requested set', None, 'DIMORPHIC HEMORRHOID'])))
        expected = pd.DataFrame(dict(patient_id=[7, 3, 7, 5], text_diagnosis=[
            'ALLERGY', 'a label outside the requested set', 'None', 'DIMORPHIC HEMORRHOID']))
        pd.testing.assert_frame_equal(result, expected)
        self.assertNotIn('text_diagnosis', source.relation.columns)

    def test_messages_use_only_symptoms_and_occurrences_have_unique_ids(self):
        for query in ('Q1', 'Q4', 'Q10'):
            task = MedicalTextTask(self.inputs(), query, 'unique-'+query)
            source = task.read_source()
            requests = task.requests(source)
            self.assertEqual(len(requests), task.model_request_count(source))
            self.assertEqual(len({row['call_id'] for row in requests}), len(requests))
            texts = []
            for request in requests:
                payload = json.loads(request['messages'][1]['content'])
                self.assertEqual(set(payload), {'text'} if query == 'Q10' else {'condition', 'text'})
                texts.append(payload['text'])
            self.assertIn('', texts)
            self.assertGreater(texts.count('same symptom evidence «好»\nsecond line'), 1)
            messages = json.dumps([row['messages'] for row in requests])
            for forbidden in ('patient_id', 'symptom_id', 'hidden-one', 'gender', 'smoking_history', 'age'):
                self.assertNotIn('"'+forbidden+'"', messages)
            self.assertEqual(task.response_format, {'type': 'json_object'})
            self.assertIn('research', requests[0]['messages'][0]['content'].lower()+
                          requests[0]['messages'][1]['content'].lower())

    def test_bad_json_or_incomplete_occurrences_fail_without_default_predictions(self):
        for query in ('Q1', 'Q4', 'Q10'):
            task = MedicalTextTask(self.inputs(), query)
            source = task.read_source()
            rows = self.predictions(task, source, ['allergy']*4 if query == 'Q10' else
                                    [True]*task.model_request_count(source))
            for altered in (rows[:-1], rows+[rows[0]], rows[:-1]+[dict(rows[-1], call_id='foreign')]):
                with self.subTest(query=query, rows=altered), self.assertRaises(ValueError):
                    task.finish(source, altered)
            key = 'text_diagnosis' if query == 'Q10' else 'keep'
            bad_values = [None, 'not JSON', '[]', '{}', '{"'+key+'":1}',
                '{"'+key+'":[]}', '{"'+key+'":"a","'+key+'":"b"}',
                '{"'+key+'":null,"extra":true}']
            bad_values += ['{"text_diagnosis":true}'] if query == 'Q10' else ['{"keep":null}', '{"keep":"true"}']
            for response in bad_values:
                with self.subTest(query=query, response=response), self.assertRaises(ValueError):
                    task.finish(source, [dict(rows[0], generated_text=response)]+rows[1:])

    def test_empty_filter_and_original_empty_extract_projection_failure(self):
        import pandas as pd

        root = self.inputs()
        self.write(root/'data/text_symptoms_data.csv', ['patient_id', 'symptoms', 'symptom_id'], [])
        for query in ('Q1', 'Q4', 'Q10'):
            task = MedicalTextTask(root, query)
            source = task.read_source()
            self.assertEqual(task.model_request_count(source), 0)
            self.assertEqual(task.requests(source), [])
            if query == 'Q10':
                with self.assertRaisesRegex(KeyError, 'text_diagnosis'):
                    task.finish(source, [])
            elif query == 'Q1':
                pd.testing.assert_series_equal(task.finish(source, []),
                    source.frames['data/text_symptoms_data.csv']['patient_id'])
            else:
                self.assertTrue(math.isnan(task.finish(source, []).iloc[0, 0]))

    def test_source_and_gold_failures_are_separate_and_metadata_does_not_reread(self):
        root = self.inputs()
        task = MedicalTextTask(root, 'Q4')
        source = task.read_source()
        with patch.object(Path, 'read_bytes', side_effect=AssertionError('metadata I/O')):
            metadata = task.describe(source)
            self.assertEqual(task.model_request_count(source), 6)
        self.assertEqual(metadata['expected_model_calls'], 6)
        self.assertEqual(metadata['source']['data/patient_data.csv']['sha256'],
            hashlib.sha256(source.raw_files['data/patient_data.csv']).hexdigest())
        symptoms = root/'data/text_symptoms_data.csv'
        symptoms.write_bytes(symptoms.read_bytes()+b'\n')
        with self.assertRaisesRegex(ValueError, 'changed'):
            task.verify_source(source)
        task.verify_source(task.read_source())
        self.write(symptoms, ['patient_id'], [(7,)])
        with self.assertRaisesRegex(ValueError, 'symptoms'):
            task.read_source()
        symptoms.unlink()
        with self.assertRaises(FileNotFoundError):
            task.read_source()
        gold = root/'data/patient_data_with_labels.csv'
        self.write(gold, ['patient_id'], [(7,)])
        with self.assertRaisesRegex(ValueError, 'evaluation'):
            task.prepare_evaluation()
        gold.unlink()
        with self.assertRaises(FileNotFoundError):
            task.prepare_evaluation()

    def test_actual_original_scores_use_frozen_gold_and_do_not_mutate_sink(self):
        import pandas as pd
        from src.baselines.text.sembench_medical import evaluate_original

        for query in ('Q1', 'Q4', 'Q10'):
            root = self.inputs()
            task = MedicalTextTask(root, query)
            evaluation = task.prepare_evaluation()
            source = task.read_source()
            values = (['ALLERGY', 'ACNE', 'ALLERGY', None] if query == 'Q10' else
                      [True, False, True, False] if query == 'Q1' else [True]+[False]*5)
            result = task.finish(source, self.predictions(task, source, values))
            before = result.copy(deep=True)
            expected = evaluate_original(self.checkout(), task.query, result.copy(deep=True),
                evaluation.frame.copy(deep=True), source.frames['data/text_symptoms_data.csv'] if query == 'Q10' else None)
            gold = root/'data/patient_data_with_labels.csv'
            gold.write_text('changed after preparation')
            with patch.object(Path, 'read_bytes', side_effect=AssertionError('gold reread')):
                quality = task.evaluate(source, result, checkout=self.checkout(), evaluation=evaluation)
            self.assertEqual(quality['original_metric'], expected['original_metric'])
            self.assertEqual(quality['gold_source']['sha256'], hashlib.sha256(evaluation.raw_bytes).hexdigest())
            if query == 'Q1':
                pd.testing.assert_series_equal(result, before)
            else:
                pd.testing.assert_frame_equal(result, before)
            if query == 'Q10':
                self.assertEqual(quality['original_metric']['f1_score'], 1)
            with self.assertRaisesRegex(ValueError, 'prepare_evaluation'):
                task.evaluate(source, result, checkout=self.checkout())

    def test_complete_original_functions_and_real_sdk_software_answers_match_all_three_queries(self):
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
            for query in ('Q1', 'Q4', 'Q10'):
                for mode in ('ordinary-duplicates', 'negative-or-null', 'empty', 'null-input'):
                    with self.subTest(query=query, mode=mode), redirect_stdout(io.StringIO()):
                        root = self.inputs()
                        if mode == 'empty':
                            self.write(root/'data/text_symptoms_data.csv', ['patient_id', 'symptoms', 'symptom_id'], [])
                        elif mode == 'null-input':
                            self.write(root/'data/text_symptoms_data.csv', ['patient_id', 'symptoms'], [(7, None), (3, None)])
                        task = MedicalTextTask(root, query)
                        source = task.read_source()
                        count = task.model_request_count(source)
                        values = ([None if mode == 'negative-or-null' else ['ALLERGY', 'ACNE', 'unknown'][i % 3]
                                   for i in range(count)] if query == 'Q10' else
                                  [mode != 'negative-or-null' and i % 2 == 0 for i in range(count)])
                        answers = ([json.dumps({'text_diagnosis': value}) for value in values] if query == 'Q10' else
                                   ['True' if value else 'False' for value in values])
                        original.data = root
                        reference = original._query('medical', task.query)
                        self.assertEqual(reference.operation.describe()['expected_model_calls'], count)
                        expected_error = actual_error = None
                        try:
                            expected, batches = original._reference(reference, answers)
                        except KeyError as error:
                            expected_error = error
                        try:
                            actual = task.finish(source, reversed(self.predictions(task, source, values)))
                        except KeyError as error:
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

    def test_fresh_task_and_actual_original_evaluation_forbid_lotus_and_network(self):
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
            from src.baselines.text.sembench_native_medical import medical_text_task
            for query in ('Q1','Q4','Q10'):
                task=medical_text_task(sys.argv[1],query,'fresh-'+query)
                gold=task.prepare_evaluation()
                source=task.read_source()
                requests=task.requests(source)
                values=(['ALLERGY','ACNE','ALLERGY',None] if query=='Q10' else
                    [True,False,True,False] if query=='Q1' else [True]+[False]*5)
                key='text_diagnosis' if query=='Q10' else 'keep'
                predictions=[dict(call_id=row['call_id'],generated_text=json.dumps({key:value}))
                    for row,value in zip(requests,values,strict=True)]
                result=task.finish(source,predictions)
                task.verify_source(source)
                json.dumps(task.encode_result(result),allow_nan=False)
                quality=task.evaluate(source,result,checkout=sys.argv[2],evaluation=gold)
                assert 'original_metric' in quality
                assert not any(name=='lotus' or name.startswith('lotus.') for name in sys.modules)
            print('fresh-medical-no-lotus: passed')
        ''')
        environment = dict(os.environ, CUDA_VISIBLE_DEVICES='', HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1',
                           LITELLM_LOCAL_MODEL_COST_MAP='True')
        with tempfile.TemporaryDirectory() as cache:
            environment['MPLCONFIGDIR'] = cache
            result = subprocess.run([sys.executable, '-c', script, str(self.inputs()), self.checkout()],
                text=True, capture_output=True, timeout=60, env=environment)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertIn('fresh-medical-no-lotus: passed', result.stdout)


if __name__ == '__main__':
    unittest.main()
