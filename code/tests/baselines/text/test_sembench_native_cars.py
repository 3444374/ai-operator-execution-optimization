"""Cars task/source/scorer contracts; all model answers are software fixtures."""

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

from src.baselines.text.sembench_cars import FULL_SCALE_FACTOR, text_files
from src.baselines.text.sembench_native_cars import CarsTextTask, cars_text_task


class NativeCarsTextTests(unittest.TestCase):
    @staticmethod
    def write(path, columns, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        value = io.StringIO(newline='')
        writer = csv.writer(value)
        writer.writerow(columns)
        writer.writerows(rows)
        path.write_bytes(value.getvalue().encode())

    def inputs(self, scale=7):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        cars, complaints, full_cars, full_complaints = text_files(scale)
        self.write(root/cars, ['car_id','year'], [(1,2010),(2,2020),(4,None)])
        self.write(root/complaints, ['car_id','complaint_id','summary'], [
            (1,101,'engine crash «好»\nfirst'), (1,102,'engine second'),
            (2,103,'engine third'), (4,104,None), (9,105,'unmatched crash')])
        self.write(root/full_cars, ['car_id','year','car_reference'], [
            (1,2010,'REFERENCE_ONLY'),(2,2020,'REFERENCE_ONLY'),
            (4,None,'REFERENCE_ONLY'),(8,1800,'REFERENCE_ONLY')])
        self.write(root/full_complaints, ['car_id','complaint_id','summary','crash','component_class','private_label'], [
            (1,101,'engine crash «好»\nfirst',True,'ENGINE','REFERENCE_ONLY'),
            (1,102,'engine second',False,'ENGINE','REFERENCE_ONLY'),
            (2,103,'engine third',False,'ENGINE','REFERENCE_ONLY'),
            (4,104,None,False,'STEERING','REFERENCE_ONLY'),
            (9,105,'unmatched crash',True,'ELECTRICAL SYSTEM','REFERENCE_ONLY'),
            (8,106,'outside sample',True,'ENGINE','REFERENCE_ONLY')])
        return root

    @staticmethod
    def predictions(task, source, values):
        field = 'problem_category' if task.query == '10' else 'keep'
        return [dict(call_id=request['call_id'], generated_text=json.dumps({field:value}))
                for request,value in zip(task.requests(source),values,strict=True)]

    def checkout(self):
        value = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')
        if not value:
            self.skipTest('bind fixed public SemBench for original source/scorer checks')
        return value

    def test_constructor_is_IO_free_and_authored_scale_changes_paths_not_rows(self):
        self.assertEqual(FULL_SCALE_FACTOR,157376)
        with patch.object(Path,'read_bytes',side_effect=AssertionError('constructor I/O')):
            task = cars_text_task('/not-read','Q4','cars')
            self.assertIs(type(task),CarsTextTask)
            self.assertEqual(task.scale_factor,157376)
            for query in ('Q2','Q3','Q5','Q8',True):
                with self.subTest(query=query),self.assertRaises(ValueError):
                    CarsTextTask('/not-read',query)
            for scale in (0,-1,True,7.0,'7'):
                with self.subTest(scale=scale),self.assertRaises(ValueError):
                    CarsTextTask('/not-read',scale_factor=scale)
        for scale in (7,29,157376):
            task = CarsTextTask(self.inputs(scale),'Q4',scale_factor=scale)
            source = task.read_source()
            self.assertEqual(task.model_request_count(source),4)
            self.assertEqual(set(source.raw_files),set(text_files(scale)[:2]))

    def test_original_series_and_duplicate_weighted_literal_2026_mean(self):
        import pandas as pd
        root = self.inputs()
        q1 = CarsTextTask(root,'Q1','series',scale_factor=7)
        source = q1.read_source()
        result = q1.finish(source,reversed(self.predictions(q1,source,[True,False,False,False,True])))
        pd.testing.assert_series_equal(result,pd.Series([1,9],index=[0,4],name='car_id'))
        self.assertEqual(q1.encode_result(result),dict(kind='series',name='car_id',index=[0,4],data=[1,9],dtype='int64'))
        q4 = CarsTextTask(root,'Q4','mean',scale_factor=7)
        source = q4.read_source()
        self.assertEqual(source.relation['car_id'].tolist(),[1,1,2,4])
        result = q4.finish(source,self.predictions(q4,source,[True,True,True,False]))
        self.assertEqual(result.columns.tolist(),['average_age'])
        self.assertEqual(result.iloc[0,0],2026-(2010+2010+2020)/3)
        empty = q4.finish(source,self.predictions(q4,source,[False]*4))
        self.assertTrue(math.isnan(empty.iloc[0,0]))
        self.assertEqual(q4.encode_result(empty)['data'],[['NaN']])

    def test_full_extract_keeps_null_unknown_labels_and_only_summary_in_messages(self):
        import pandas as pd
        root = self.inputs()
        for query in ('Q1','Q4','Q10'):
            task = CarsTextTask(root,query,'messages-'+query,scale_factor=7)
            source = task.read_source()
            requests = task.requests(source)
            self.assertEqual(len({r['call_id'] for r in requests}),task.model_request_count(source))
            for request in requests:
                payload = json.loads(request['messages'][1]['content'])
                self.assertEqual(set(payload),{'text'} if query=='Q10' else {'condition','text'})
            self.assertIn('',[json.loads(row['messages'][1]['content'])['text'] for row in requests])
            text = json.dumps([row['messages'] for row in requests])
            for key in ('car_id','complaint_id','year','component_class','crash','private_label','REFERENCE_ONLY'):
                self.assertNotIn('"'+key+'"',text)
            self.assertEqual(task.response_format,{'type':'json_object'})
            if query=='Q10':
                result = task.finish(source,reversed(self.predictions(task,source,
                    ['ENGINE','a category outside the list',None,'STEERING'])))
                pd.testing.assert_frame_equal(result,pd.DataFrame(dict(car_id=[1,1,2,4],
                    problem_category=['ENGINE','a category outside the list','None','STEERING'])))
                self.assertNotIn('problem_category',source.relation.columns)

    def test_strict_JSON_and_complete_occurrences_fail_without_filling_answers(self):
        for query in ('Q1','Q4','Q10'):
            task = CarsTextTask(self.inputs(),query,scale_factor=7)
            source = task.read_source()
            count = task.model_request_count(source)
            rows = self.predictions(task,source,['ENGINE']*count if query=='Q10' else [True]*count)
            for altered in (rows[:-1],rows+[rows[0]],rows[:-1]+[dict(rows[-1],call_id='foreign')]):
                with self.subTest(query=query),self.assertRaises(ValueError):
                    task.finish(source,altered)
            key = 'problem_category' if query=='Q10' else 'keep'
            bad = [None,'not JSON','[]','{}','{"'+key+'":1}','{"'+key+'":[]}',
                   '{"'+key+'":"a","'+key+'":"b"}','{"'+key+'":null,"extra":true}']
            bad += ['{"problem_category":true}'] if query=='Q10' else ['{"keep":null}','{"keep":"true"}']
            for response in bad:
                with self.subTest(query=query,response=response),self.assertRaises(ValueError):
                    task.finish(source,[dict(rows[0],generated_text=response)]+rows[1:])

    def test_source_and_gold_missing_are_distinct_and_Q1_needs_no_car_tables(self):
        root = self.inputs()
        cars,complaints,full_cars,full_complaints = text_files(7)
        (root/cars).unlink();(root/full_cars).unlink()
        task = CarsTextTask(root,'Q1',scale_factor=7)
        task.prepare_evaluation()
        source = task.read_source()
        self.assertEqual(task.model_request_count(source),5)
        with patch.object(Path,'read_bytes',side_effect=AssertionError('metadata reread')):
            described = task.describe(source)
            self.assertEqual(described['source'][complaints]['sha256'],hashlib.sha256(source.raw_files[complaints]).hexdigest())
        (root/complaints).write_bytes((root/complaints).read_bytes()+b'\n')
        with self.assertRaisesRegex(ValueError,'changed'): task.verify_source(source)
        self.write(root/complaints,['car_id','complaint_id'],[(1,101)])
        with self.assertRaisesRegex(ValueError,'summary'): task.read_source()
        (root/complaints).unlink()
        with self.assertRaises(FileNotFoundError): task.read_source()
        self.write(root/full_complaints,['car_id'],[(1,)])
        with self.assertRaisesRegex(ValueError,'evaluation'): task.prepare_evaluation()
        (root/full_complaints).unlink()
        with self.assertRaises(FileNotFoundError): task.prepare_evaluation()
        for query in ('Q4','Q10'):
            task = CarsTextTask(self.inputs(),query,scale_factor=7)
            task.prepare_evaluation()
            (task.data_dir/text_files(7)[0]).unlink()
            with self.assertRaises(FileNotFoundError): task.read_source()

    def test_empty_series_NaN_and_original_empty_extract_error_are_preserved(self):
        import pandas as pd
        root = self.inputs()
        self.write(root/text_files(7)[1],['car_id','complaint_id','summary'],[])
        for query in ('Q1','Q4','Q10'):
            task = CarsTextTask(root,query,scale_factor=7)
            source = task.read_source()
            self.assertEqual(task.model_request_count(source),0)
            self.assertEqual(task.requests(source),[])
            if query=='Q10':
                with self.assertRaisesRegex(KeyError,'problem_category'): task.finish(source,[])
            elif query=='Q1':
                pd.testing.assert_series_equal(task.finish(source,[]),source.relation['car_id'])
            else:
                self.assertTrue(math.isnan(task.finish(source,[]).iloc[0,0]))

    def test_original_scores_use_frozen_full_gold_and_current_execution_membership(self):
        import pandas as pd
        from src.baselines.text.sembench_cars import evaluate_original
        for query in ('Q1','Q4','Q10'):
            root = self.inputs()
            task = CarsTextTask(root,query,scale_factor=7)
            gold = task.prepare_evaluation()
            source = task.read_source()
            values = ['ENGINE','ENGINE','ENGINE','STEERING'] if query=='Q10' else (
                [True,False,False,False,True] if query=='Q1' else [True,True,True,False])
            result = task.finish(source,self.predictions(task,source,values))
            before = result.copy(deep=True)
            frames = {name:frame.copy(deep=True) for name,frame in (gold.frames|source.frames).items()}
            with redirect_stdout(io.StringIO()):
                expected = evaluate_original(self.checkout(),query,result.copy(deep=True),frames,scale_factor=7)
            for name in gold.raw_files:
                (root/name).write_text('changed after freeze')
            # Membership must use this actual execution table, not re-read a
            # later sample or guess that scale_factor is its row count.
            (root/text_files(7)[1]).write_text('changed after complete sink')
            with patch.object(Path,'read_bytes',side_effect=AssertionError('gold/sample reread')),redirect_stdout(io.StringIO()):
                quality = task.evaluate(source,result,checkout=self.checkout(),evaluation=gold)
            self.assertEqual(quality['original_metric'],expected['original_metric'])
            for name,raw in gold.raw_files.items():
                self.assertEqual(quality['gold_source'][name]['sha256'],hashlib.sha256(raw).hexdigest())
            (pd.testing.assert_series_equal if query=='Q1' else pd.testing.assert_frame_equal)(result,before)
            if query=='Q4':
                self.assertAlmostEqual(quality['original_metric']['absolute_error'],abs(result.iloc[0,0]-11))
                self.assertGreater(quality['original_metric']['absolute_error'],0)
            else:
                self.assertEqual(quality['original_metric']['f1_score'],1)
            with self.assertRaisesRegex(ValueError,'prepare_evaluation'):
                task.evaluate(source,result,checkout=self.checkout())

    def test_original_Q10_metric_keeps_duplicate_sample_membership_length_failure(self):
        import pandas as pd
        root = self.inputs()
        path = root/text_files(7)[1]
        frame = pd.read_csv(path)
        pd.concat([frame,frame.iloc[[0]]],ignore_index=True).to_csv(path,index=False)
        task = CarsTextTask(root,'Q10',scale_factor=7)
        gold,source = task.prepare_evaluation(),task.read_source()
        result = task.finish(source,self.predictions(task,source,['ENGINE']*task.model_request_count(source)))
        with self.assertRaisesRegex(ValueError,'inconsistent numbers'):
            task.evaluate(source,result,checkout=self.checkout(),evaluation=gold)

    def test_original_functions_and_real_SDK_software_answers_match_empty_duplicates_and_NaN(self):
        checkout = self.checkout()
        if any(importlib.util.find_spec(name) is None for name in ('lotus','litellm')):
            self.skipTest('existing LOTUS SDK needed only for original-function software reference')
        with ExitStack() as scope:
            scope.enter_context(patch.dict(os.environ,dict(HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',
                HF_DATASETS_OFFLINE='1',LITELLM_LOCAL_MODEL_COST_MAP='True',CUDA_VISIBLE_DEVICES='')))
            scope.enter_context(patch.object(socket.socket,'connect',side_effect=AssertionError('network forbidden')))
            import lotus
            import pandas as pd
            from src.baselines.text.sembench_text import SemBenchTextQuery
            from tests.experiments.test_sembench_text_original import OriginalSemBenchTextTests
            from src.serving.backends.vllm_protocol import GenerationSpec
            from src.serving.backends.vllm_engine import VllmEngine
            from src.serving.backends.vllm_async_engine import VllmAsyncEngine
            for engine in (VllmEngine,VllmAsyncEngine):
                scope.enter_context(patch.object(engine,'__init__',side_effect=AssertionError('model forbidden')))
            scope.enter_context(lotus.settings.context(enable_cache=False))
            original = OriginalSemBenchTextTests()
            original.generation = GenerationSpec(max_tokens=32,temperature=0,seed=0)
            for query in ('Q1','Q4','Q10'):
                for mode in ('ordinary','duplicate-cars','negative-or-null','empty','NaN-join'):
                    with self.subTest(query=query,mode=mode),redirect_stdout(io.StringIO()):
                        root = self.inputs()
                        cars,complaints,*_ = text_files(7)
                        if mode=='empty':
                            self.write(root/complaints,['car_id','complaint_id','summary'],[])
                        elif mode=='duplicate-cars':
                            frame = pd.read_csv(root/cars)
                            pd.concat([frame,pd.DataFrame(dict(car_id=[1],year=[2000]))],ignore_index=True).to_csv(root/cars,index=False)
                        elif mode=='NaN-join':
                            frame = pd.read_csv(root/cars)
                            pd.concat([frame,pd.DataFrame(dict(car_id=[float('nan')],year=[float('nan')]))],ignore_index=True).to_csv(root/cars,index=False)
                            frame = pd.read_csv(root/complaints)
                            pd.concat([frame,pd.DataFrame(dict(car_id=[float('nan')],complaint_id=[107],summary=[None]))],ignore_index=True).to_csv(root/complaints,index=False)
                        task = CarsTextTask(root,query,scale_factor=7)
                        source = task.read_source()
                        count = task.model_request_count(source)
                        values = ([None if mode=='negative-or-null' else ['ENGINE','STEERING','unknown'][i%3]
                                   for i in range(count)] if query=='Q10' else
                                  [mode!='negative-or-null' and i%2==0 for i in range(count)])
                        answers = [json.dumps({'problem_category':value}) for value in values] if query=='Q10' else [
                            'True' if value else 'False' for value in values]
                        reference = SemBenchTextQuery(checkout,root,'cars',task.query,execution_id='cars-reference',
                            model_name='openai/local-fixture',generation=original.generation,scale_factor=7)
                        self.assertEqual(reference.operation.describe()['expected_model_calls'],count)
                        expected_error = actual_error = None
                        try:
                            expected,batches = original._reference(reference,answers)
                        except KeyError as error:
                            expected_error = error
                        try:
                            actual = task.finish(source,reversed(self.predictions(task,source,values)))
                        except KeyError as error:
                            actual_error = error
                        self.assertEqual(type(actual_error),type(expected_error))
                        if expected_error is not None:
                            self.assertEqual(str(actual_error),str(expected_error))
                        else:
                            self.assertEqual(sum(len(messages) for messages,_ in batches),count)
                            (pd.testing.assert_series_equal if query=='Q1' else pd.testing.assert_frame_equal)(actual,expected)

    def test_fresh_task_and_original_scoring_forbid_LOTUS_and_network(self):
        script = textwrap.dedent('''
            import importlib.abc,json,socket,sys
            class Guard(importlib.abc.MetaPathFinder):
                def find_spec(self,name,path=None,target=None):
                    blocked=('lotus','semantic_methods.lotus','src.semantic_methods.lotus',
                        'src.baselines.text.sembench_text','src.baselines.text.sembench_prepared','src.experiments','tests')
                    if any(name==item or name.startswith(item+'.') for item in blocked):
                        raise AssertionError('forbidden runtime dependency: '+name)
            sys.meta_path.insert(0,Guard())
            def forbidden(*args,**kwargs): raise AssertionError('network forbidden')
            socket.socket.connect=forbidden
            from src.baselines.text.sembench_native_cars import cars_text_task
            for query in ('Q1','Q4','Q10'):
                task=cars_text_task(sys.argv[1],query,'fresh-'+query,scale_factor=7)
                gold=task.prepare_evaluation();source=task.read_source()
                values=(['ENGINE','ENGINE','ENGINE','STEERING'] if query=='Q10' else
                    [True,False,False,False,True] if query=='Q1' else [True,True,True,False])
                key='problem_category' if query=='Q10' else 'keep'
                predictions=[dict(call_id=row['call_id'],generated_text=json.dumps({key:value}))
                    for row,value in zip(task.requests(source),values,strict=True)]
                result=task.finish(source,predictions)
                task.verify_source(source)
                json.dumps(task.encode_result(result),allow_nan=False)
                quality=task.evaluate(source,result,checkout=sys.argv[2],evaluation=gold)
                assert 'original_metric' in quality
                assert not any(name=='lotus' or name.startswith('lotus.') for name in sys.modules)
            print('fresh-cars-no-lotus: passed')
        ''')
        environment = dict(os.environ,CUDA_VISIBLE_DEVICES='',HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',
                           HF_DATASETS_OFFLINE='1',LITELLM_LOCAL_MODEL_COST_MAP='True')
        with tempfile.TemporaryDirectory() as cache:
            environment['MPLCONFIGDIR']=cache
            result=subprocess.run([sys.executable,'-B','-c',script,str(self.inputs()),self.checkout()],
                capture_output=True,text=True,timeout=60,env=environment)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn('fresh-cars-no-lotus: passed',result.stdout)


if __name__=='__main__':
    unittest.main()
