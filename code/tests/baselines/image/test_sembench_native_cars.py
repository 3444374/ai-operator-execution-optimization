"""Cars visual task parity with original queries, RGB payloads and full gold."""

import base64
from collections import deque
from contextlib import redirect_stdout
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

import pandas as pd
from PIL import Image

from src.baselines.image.sembench_native_cars import CarsVisualTask, cars_visual_task
from src.baselines.text.sembench_cars import FULL_SCALE_FACTOR, evaluate_original, visual_files
from src.baselines.text.sembench_source import load_module


CHECKOUT=os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT') or os.environ.get('SEMBENCH_CHECKOUT')


class NativeCarsVisualTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name)
        self.paths=[]
        for index,color in enumerate(((255,0,0,64),(0,0,255,128))):
            path=self.root/('image-'+str(index)+'.png')
            Image.new('RGBA',(4+index,3),color).save(path)
            self.paths.append(str(path))
        self.inputs()

    def write(self,name,columns,rows):
        path=self.root/name
        path.parent.mkdir(parents=True,exist_ok=True)
        pd.DataFrame(rows,columns=columns).to_csv(path,index=False)
        return path

    def inputs(self,scale=7):
        cars,images,full_cars,full_images=visual_files(scale)
        self.write(cars,['car_id','transmission','vin','private_execution'],[
            (1,'Manual','V1','DO_NOT_SEND_1'),(1,'Manual','V1-duplicate','DO_NOT_SEND_duplicate'),
            (2,'Automatic','V2','DO_NOT_SEND_2'),(3,None,'V3','DO_NOT_SEND_3')])
        self.write(images,['image_id','car_id','image_path'],[
            ('i1',1,self.paths[0]),('i2',1,self.paths[1]),('i3',2,self.paths[0]),
            ('i4',9,str(self.root/'unmatched-missing.png'))])
        self.write(full_cars,['car_id','transmission','vin','private_gold'],[
            (1,'Manual','V1','DO_NOT_SEND_gold1'),(2,'Automatic','V2','DO_NOT_SEND_gold2'),
            (3,'Manual','V3','DO_NOT_SEND_gold3'),(9,'Manual','V9','DO_NOT_SEND_gold9'),(99,'Manual','V99','DO_NOT_SEND_other')])
        self.write(full_images,['image_id','car_id','damage_status'],[
            ('i1',1,'no_damage'),('i2',1,'paint_scratches;puncture'),('i3',2,'no_damage'),
            ('i4',9,'puncture;paint_scratches'),('i-other',99,'no_damage')])

    def task(self,query='Q3',scale=7):
        return CarsVisualTask(self.root,query,'native-cars',scale)

    def predictions(self,task,source,flags):
        return [dict(call_id=row['call_id'],generated_text=json.dumps(dict(keep=flag)))
                for row,flag in zip(task.requests(source),flags,strict=True)]

    def require_checkout(self):
        if not CHECKOUT:self.skipTest('bind fixed c814 SemBench for original scoring/reference')
        return Path(CHECKOUT)

    def reference(self,query,flags,scale=7):
        checkout=self.require_checkout()
        if any(importlib.util.find_spec(name) is None for name in ('lotus','litellm')):
            self.skipTest('existing LOTUS SDK needed only for original-query software reference')
        import lotus
        import lotus.models.lm as lm_module
        from lotus.models import LM
        from litellm.types.utils import ModelResponse

        module=load_module(checkout,'cars-native-visual-oracle-'+str(query),
                           'files/cars/query/lotus/Q'+str(query).removeprefix('Q')+'.py')
        lm=LM('openai/CPU-vision-fixture',max_tokens=8,temperature=0)
        lm.max_batch_size=64
        replies=deque(flags)
        batches=[]
        def batch(*args,**kwargs):
            messages=args[1] if len(args)>1 else kwargs['messages']
            batches.append(deepcopy(messages))
            return [ModelResponse(model=lm.model,object='chat.completion',choices=[dict(index=0,finish_reason='stop',
                message=dict(role='assistant',content='True' if replies.popleft() else 'False'))],
                usage=dict(prompt_tokens=1,completion_tokens=1,total_tokens=2)) for _ in messages]
        with patch.object(socket.socket,'connect',side_effect=AssertionError('network forbidden')), \
                lotus.settings.context(lm=lm,enable_cache=False),patch.object(lm_module,'batch_completion',side_effect=batch):
            result=module.run(str(self.root),scale)
        self.assertFalse(replies)
        return result,[part['image_url']['url'] for batch in batches for messages in batch for message in messages
                       if isinstance(message['content'],list) for part in message['content'] if part['type']=='image_url']

    def test_constructor_is_IO_free_and_default_scale_selects_paths_without_a_row_cap(self):
        with patch.object(Path,'read_bytes',side_effect=AssertionError('construction did IO')):
            default=cars_visual_task(self.root)
        self.assertEqual(default.scale_factor,FULL_SCALE_FACTOR)
        self.inputs(scale=FULL_SCALE_FACTOR)
        source=default.read_source()
        self.assertEqual(len(source.relation),4)
        self.assertTrue(all(str(FULL_SCALE_FACTOR) in name for name in source.raw_files))
        for kwargs in (dict(query='Q9'),dict(scale_factor=0),dict(execution_id='')):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):CarsVisualTask(self.root,**kwargs)

    def test_both_original_functions_and_real_SDK_keep_merge_manual_selection_and_exact_RGB_PNG(self):
        for query in ('Q3','Q8'):
            for mode in ('mixed','negative','empty','null-key'):
                with self.subTest(query=query,mode=mode),redirect_stdout(io.StringIO()):
                    self.inputs()
                    cars,images,_,_=visual_files(7)
                    if mode=='empty':
                        frame=pd.read_csv(self.root/images).iloc[:0]
                        frame.to_csv(self.root/images,index=False)
                    elif mode=='null-key':
                        for name in (cars,images):
                            frame=pd.read_csv(self.root/name)
                            frame.loc[frame['car_id']==1,'car_id']=float('nan')
                            frame.to_csv(self.root/name,index=False)
                    task=self.task(query)
                    source=task.read_source()
                    flags=[mode!='negative' and index%2==0 for index in range(len(source.relation))]
                    actual=task.finish(source,list(reversed(self.predictions(task,source,flags))))
                    expected,uris=self.reference(query,flags)
                    if query=='Q3':pd.testing.assert_frame_equal(actual,expected)
                    else:pd.testing.assert_series_equal(actual,expected)
                    self.assertEqual(list(source.image_uris),uris)
                    requests=task.requests(source)
                    self.assertNotIn('DO_NOT_SEND',json.dumps(requests))
                    self.assertNotIn('car_id',json.dumps(requests))
                    self.assertNotIn('image_id',json.dumps(requests))
                    self.assertNotIn(self.paths[0],json.dumps(requests))
                    self.assertEqual(task.response_format,{'type':'json_object'})
                    self.assertFalse(task.describe(source)['lotus_sdk_required'])
                    task.verify_source(source)

    def test_all_duplicate_occurrences_are_predicted_before_original_head_and_RGB_no_resize(self):
        for query,count,limit in (('Q3',12,10),('Q8',103,100)):
            with self.subTest(query=query):
                self.inputs()
                images=visual_files(7)[1]
                self.write(images,['image_id','car_id','image_path'],[(i,1,self.paths[0]) for i in range(count)])
                task=self.task(query)
                source=task.read_source()
                requests=task.requests(source)
                self.assertEqual(len(requests),2*count)
                self.assertEqual(len({row['call_id'] for row in requests}),2*count)
                result=task.finish(source,self.predictions(task,source,[True]*len(requests)))
                self.assertEqual(len(result),limit)
                with Image.open(io.BytesIO(base64.b64decode(source.image_uris[0].split(',',1)[1]))) as actual, \
                        Image.open(self.paths[0]) as original:
                    self.assertEqual((actual.mode,actual.size),('RGB',original.size))
                    self.assertEqual(actual.tobytes(),original.convert('RGB').tobytes())

    def test_strict_complete_Boolean_predictions_and_empty_sink_schema(self):
        for query in ('Q3','Q8'):
            with self.subTest(query=query):
                task=self.task(query)
                source=task.read_source()
                values=self.predictions(task,source,[False]*len(source.relation))
                result=task.finish(source,values)
                self.assertTrue(result.empty)
                if query=='Q3':self.assertEqual(list(result.columns),['vin'])
                else:self.assertEqual(result.name,'car_id')
                for invalid in (values[:-1],values+values[:1],[dict(values[0],call_id='unknown'),*values[1:]]):
                    with self.assertRaisesRegex(ValueError,'occurrence|cover every'):task.finish(source,invalid)
                for answer in ('True','{"keep":1}','{"keep":true,"keep":false}','{"keep":false,"extra":1}'):
                    with self.assertRaises((ValueError,json.JSONDecodeError)):
                        task.finish(source,[dict(values[0],generated_text=answer),*values[1:]])

    def test_official_scores_freeze_full_gold_but_use_this_source_sample_membership_and_copy_sink(self):
        checkout=self.require_checkout()
        for query in ('Q3','Q8'):
            with self.subTest(query=query),redirect_stdout(io.StringIO()):
                self.inputs()
                task=self.task(query)
                evaluation=task.prepare_evaluation()
                source=task.read_source()
                result=task.finish(source,self.predictions(task,source,[True]*len(source.relation)))
                saved=result.copy(deep=True)
                frames={name:frame.copy(deep=True) for name,frame in (evaluation.frames|source.frames).items()}
                expected=evaluate_original(checkout,query,result.copy(deep=True),frames,scale_factor=7)
                for name in evaluation.raw_files:(self.root/name).write_text('broken after frozen gold')
                quality=task.evaluate(source,result,checkout=checkout,evaluation=evaluation)
                self.assertEqual(quality['original_metric'],expected['original_metric'])
                if query=='Q3':pd.testing.assert_frame_equal(result,saved)
                else:pd.testing.assert_series_equal(result,saved)
                self.assertEqual(set(quality['gold_source']),set(evaluation.raw_files))
        self.inputs()
        (self.root/visual_files(7)[2]).unlink()
        with self.assertRaises(FileNotFoundError):self.task().prepare_evaluation()
        self.inputs()
        self.write(visual_files(7)[3],['image_id','car_id'],[('i1',1)])
        with self.assertRaisesRegex(ValueError,'gold columns'):self.task().prepare_evaluation()

    def test_full_scale_gold_is_not_artificially_restricted_to_small_execution_fixture(self):
        checkout=self.require_checkout()
        self.inputs(scale=FULL_SCALE_FACTOR)
        task=self.task('Q3',FULL_SCALE_FACTOR)
        gold=task.prepare_evaluation()
        source=task.read_source()
        result=task.finish(source,self.predictions(task,source,[True]*len(source.relation)))
        frames={name:frame.copy(deep=True) for name,frame in (gold.frames|source.frames).items()}
        with redirect_stdout(io.StringIO()):
            expected=evaluate_original(checkout,'Q3',result.copy(deep=True),frames,scale_factor=FULL_SCALE_FACTOR)
            actual=task.evaluate(source,result,checkout=checkout,evaluation=gold)
        self.assertEqual(actual['original_metric'],expected['original_metric'])
        self.assertIn('authored full scale',task.describe(source)['label_scope'])

    def test_fresh_image_bytes_missing_files_and_metadata_changes_are_not_empty_success(self):
        task=self.task('Q8')
        source=task.read_source()
        path=Path(self.paths[0]);old=path.read_bytes()
        Image.new('RGB',(9,7),'green').save(path)
        fresh=task.read_source()
        self.assertNotEqual(source.image_uris,fresh.image_uris)
        path.write_bytes(old)
        with self.assertRaisesRegex(ValueError,'actual Cars image bytes'):task.verify_source(fresh)
        task.verify_source(source)
        path.unlink()
        with self.assertRaises(FileNotFoundError):task.read_source()
        self.inputs()
        (self.root/visual_files(7)[0]).write_text('changed')
        with self.assertRaisesRegex(ValueError,'source changed'):task.verify_source(source)

    def test_fresh_process_scores_without_LOTUS_or_any_model_backend(self):
        checkout=self.require_checkout()
        script=r'''
import importlib.abc,json,socket,sys
class Deny(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,*args):
  if any(fullname==name or fullname.startswith(name+'.') for name in ('lotus','litellm','ray','daft','vllm','torch','transformers','src.semantic_methods.lotus','src.baselines.text.sembench_text','src.experiments')):
   raise AssertionError('forbidden dependency '+fullname)
sys.meta_path.insert(0,Deny())
def no_network(*args,**kwargs):raise AssertionError('network forbidden')
socket.socket.connect=no_network
from src.baselines.image.sembench_native_cars import CarsVisualTask
for query in ('Q3','Q8'):
 task=CarsVisualTask(sys.argv[2],query,scale_factor=7)
 gold=task.prepare_evaluation();source=task.read_source();requests=task.requests(source)
 result=task.finish(source,[dict(call_id=r['call_id'],generated_text='{"keep":true}') for r in requests])
 quality=task.evaluate(source,result,checkout=sys.argv[1],evaluation=gold)
 task.verify_source(source)
 assert 'original_metric' in quality
assert not any(name=='lotus' or name.startswith('lotus.') for name in sys.modules)
print(json.dumps(dict(status='passed',actual_models=0)))
'''
        outcome=subprocess.run([sys.executable,'-B','-c',script,str(checkout),str(self.root)],
            env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[3]),LITELLM_LOCAL_MODEL_COST_MAP='True'),
            capture_output=True,text=True,timeout=45)
        self.assertEqual(outcome.returncode,0,outcome.stdout+outcome.stderr)
        self.assertEqual(json.loads(outcome.stdout.splitlines()[-1])['actual_models'],0)


if __name__=='__main__':
    unittest.main()
