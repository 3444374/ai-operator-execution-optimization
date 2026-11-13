"""Medical image task contracts and pinned original SDK software references."""

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

from src.baselines.image.sembench_native_medical import MedicalVisualTask, medical_visual_task
from src.baselines.text.sembench_medical import FULL_SCALE_FACTOR, evaluate_original, visual_files
from src.baselines.text.sembench_source import load_module


CHECKOUT = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT') or os.environ.get('SEMBENCH_CHECKOUT')


class NativeMedicalVisualTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.paths = []
        for index,color in enumerate(((255,0,0,64),(0,0,255,128))):
            path = self.root/('image-'+str(index)+'.png')
            Image.new('RGBA',(4+index,3),color).save(path)
            self.paths.append(str(path))
        self.inputs()

    def write(self,name,columns,rows):
        path = self.root/name
        path.parent.mkdir(parents=True,exist_ok=True)
        pd.DataFrame(rows,columns=columns).to_csv(path,index=False)
        return path

    def inputs(self,scale=FULL_SCALE_FACTOR):
        names,labels=visual_files('Q9',scale)
        self.write(names[0],['patient_id','did_family_have_cancer','private_patient'],[
            (7,1,'DO_NOT_SEND_7'),(7,1,'DO_NOT_SEND_duplicate'),(9,0,'DO_NOT_SEND_9'),(11,1,'DO_NOT_SEND_unmatched')])
        self.write(names[1],['patient_id','image_path','skin_image_id'],[
            (7,self.paths[0],'skin-a'),(7,self.paths[1],'skin-b'),(9,self.paths[1],'skin-c')])
        self.write(names[2],['patient_id','image_path','xray_id'],[
            (7,self.paths[1],'xray-a'),(7,self.paths[0],'xray-b'),(9,self.paths[0],'xray-c')])
        self.write(labels,['patient_id','did_family_have_cancer','skin_cancer_diagnosis','x_ray_diagnosis'],[
            (index,1,'malignant','disease') for index in range(1,121)])

    def task(self,query='Q3',scale=FULL_SCALE_FACTOR):
        return MedicalVisualTask(self.root,query,'native-medical',scale)

    def predictions(self,task,source,flags):
        return [dict(call_id=row['call_id'],generated_text=json.dumps(dict(keep=flag)))
                for row,flag in zip(task.requests(source),flags,strict=True)]

    def require_checkout(self):
        if not CHECKOUT:
            self.skipTest('bind fixed c814 SemBench for original scoring/reference')
        return Path(CHECKOUT)

    def reference(self,query,flags,scale=FULL_SCALE_FACTOR):
        checkout=self.require_checkout()
        if any(importlib.util.find_spec(name) is None for name in ('lotus','litellm')):
            self.skipTest('existing LOTUS SDK needed only for original-query software reference')
        import lotus
        import lotus.models.lm as lm_module
        from lotus.models import LM
        from litellm.types.utils import ModelResponse

        module=load_module(checkout,'medical-native-visual-oracle-'+str(query),
                           'files/medical/query/lotus/Q'+str(query).removeprefix('Q')+'.py')
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
        # LOTUS parse_cols uses list(set(...)); its per-column text labels,
        # rather than that process's incidental image order, identify each URI.
        uris=[]
        columns=('image','imagex') if str(query).removeprefix('Q')=='9' else ('image',)
        for batch in batches:
            for messages in batch:
                images={}
                label=None
                for message in messages:
                    if not isinstance(message['content'],list):continue
                    for part in message['content']:
                        if part['type']=='text' and part['text'].strip().lower() in ('[image]:','[imagex]:'):
                            label=part['text'].strip().lower()[1:-2]
                        elif part['type']=='image_url':
                            self.assertIn(label,columns)
                            images[label]=part['image_url']['url']
                self.assertEqual(set(images),set(columns))
                uris.extend(images[column] for column in columns)
        return result,uris

    def test_constructor_is_IO_free_and_scale_only_selects_original_files(self):
        with patch.object(Path,'read_bytes',side_effect=AssertionError('construction did IO')):
            task=medical_visual_task(self.root,'Q9',scale_factor=7)
        self.assertEqual(task.scale_factor,7)
        self.inputs(scale=7)
        source=task.read_source()
        self.assertEqual(len(source.relation),9)
        self.assertTrue(all('_7.csv' in name for name in source.raw_files))
        self.assertEqual(task.model_request_count(source),9)
        for kwargs in (dict(query='Q1'),dict(scale_factor=0),dict(execution_id='')):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):
                MedicalVisualTask(self.root,**kwargs)

    def test_three_original_functions_and_real_SDK_keep_joins_Series_and_exact_RGB_PNG(self):
        for query in ('Q3','Q8','Q9'):
            for mode in ('mixed','negative','empty','null-key'):
                with self.subTest(query=query,mode=mode),redirect_stdout(io.StringIO()):
                    self.inputs()
                    names,_=visual_files(query)
                    if mode=='empty':
                        frame=pd.read_csv(self.root/names[1]).iloc[:0]
                        frame.to_csv(self.root/names[1],index=False)
                    elif mode=='null-key':
                        for name in names:
                            frame=pd.read_csv(self.root/name)
                            frame.loc[frame['patient_id']==7,'patient_id']=float('nan')
                            frame.to_csv(self.root/name,index=False)
                    task=self.task(query)
                    source=task.read_source()
                    flags=[mode!='negative' and index%2==0 for index in range(len(source.relation))]
                    actual=task.finish(source,list(reversed(self.predictions(task,source,flags))))
                    expected,uris=self.reference(query,flags)
                    pd.testing.assert_series_equal(actual,expected)
                    self.assertEqual([uri for row in source.image_uris for uri in row],uris)
                    self.assertEqual(len(uris),len(source.relation)*(2 if query=='Q9' else 1))
                    requests=task.requests(source)
                    self.assertNotIn('DO_NOT_SEND',json.dumps(requests))
                    self.assertNotIn('patient_id',json.dumps(requests))
                    self.assertNotIn(self.paths[0],json.dumps(requests))
                    self.assertEqual(task.response_format,{'type':'json_object'})
                    self.assertFalse(task.describe(source)['lotus_sdk_required'])
                    task.verify_source(source)

    def test_Limits_follow_all_occurrences_and_Q9_keeps_skin_xray_pair_order(self):
        for query,count,limit in (('Q3',12,5),('Q8',103,100)):
            with self.subTest(query=query):
                self.inputs()
                names,_=visual_files(query)
                image_id='xray_id' if query=='Q3' else 'skin_image_id'
                self.write(names[1],['patient_id','image_path',image_id],[(7,self.paths[0],i) for i in range(count)])
                task=self.task(query)
                source=task.read_source()
                requests=task.requests(source)
                self.assertEqual(len(requests),2*count)
                result=task.finish(source,self.predictions(task,source,[True]*len(requests)))
                self.assertEqual(len(result),limit)
                self.assertEqual(len({row['call_id'] for row in requests}),2*count)
        self.inputs()
        task=self.task('Q9')
        source=task.read_source()
        self.assertEqual(len(source.relation),9)
        self.assertEqual(source.relation['patient_id'].tolist(),[7]*8+[9])
        for uris,paths in zip(source.image_uris,source.relation[['image_path','image_path_xray']].itertuples(index=False,name=None),strict=True):
            for uri,path in zip(uris,paths,strict=True):
                with Image.open(io.BytesIO(base64.b64decode(uri.split(',',1)[1]))) as actual,Image.open(path) as original:
                    self.assertEqual(actual.mode,'RGB')
                    self.assertEqual(actual.size,original.size)
                    self.assertEqual(actual.tobytes(),original.convert('RGB').tobytes())

    def test_strict_complete_Boolean_relation_and_empty_series_remain_original(self):
        task=self.task('Q9')
        source=task.read_source()
        values=self.predictions(task,source,[False]*len(source.relation))
        empty=task.finish(source,values)
        self.assertEqual(empty.name,'patient_id')
        self.assertEqual(len(empty),0)
        for invalid in (values[:-1],values+values[:1],[dict(values[0],call_id='unknown'),*values[1:]]):
            with self.subTest(invalid=invalid),self.assertRaisesRegex(ValueError,'occurrence|cover every'):
                task.finish(source,invalid)
        for answer in ('True','{"keep":1}','{"keep":true,"keep":false}','{"keep":false,"extra":1}'):
            with self.subTest(answer=answer),self.assertRaises((ValueError,json.JSONDecodeError)):
                task.finish(source,[dict(values[0],generated_text=answer),*values[1:]])

    def test_actual_scores_use_frozen_gold_and_do_not_mutate_the_complete_sink(self):
        checkout=self.require_checkout()
        for query in ('Q3','Q8','Q9'):
            with self.subTest(query=query),redirect_stdout(io.StringIO()):
                self.inputs()
                task=self.task(query)
                evaluation=task.prepare_evaluation()
                source=task.read_source()
                result=task.finish(source,self.predictions(task,source,[True]*len(source.relation)))
                saved=result.copy(deep=True)
                expected=evaluate_original(checkout,query,result.copy(deep=True),evaluation.frame.copy(deep=True))
                labels=visual_files(query)[1]
                (self.root/labels).write_text('broken after frozen gold')
                quality=task.evaluate(source,result,checkout=checkout,evaluation=evaluation)
                self.assertEqual(quality['original_metric'],expected['original_metric'])
                pd.testing.assert_series_equal(result,saved)
                self.assertEqual(quality['gold_source']['bytes'],len(evaluation.raw_bytes))
                self.assertEqual(task.encode_result(result)['index'],result.index.tolist())
        self.inputs()
        labels=visual_files('Q9')[1]
        (self.root/labels).unlink()
        with self.assertRaises(FileNotFoundError):self.task().prepare_evaluation()
        self.write(labels,['patient_id'],[(7,)])
        with self.assertRaisesRegex(ValueError,'gold columns'):self.task().prepare_evaluation()

    def test_original_small_gold_ValueError_is_preserved_after_a_valid_empty_sink(self):
        checkout=self.require_checkout()
        for query in ('Q3','Q8'):
            with self.subTest(query=query),redirect_stdout(io.StringIO()):
                self.inputs()
                labels=visual_files(query)[1]
                frame=pd.read_csv(self.root/labels).head(2)
                frame.to_csv(self.root/labels,index=False)
                task=self.task(query)
                evaluation=task.prepare_evaluation()
                source=task.read_source()
                result=task.finish(source,self.predictions(task,source,[False]*len(source.relation)))
                with self.assertRaises(ValueError):
                    task.evaluate(source,result,checkout=checkout,evaluation=evaluation)
                self.assertTrue(result.empty)

    def test_fresh_image_bytes_and_metadata_are_checked_after_execution(self):
        task=self.task('Q9')
        source=task.read_source()
        path=Path(self.paths[0])
        old=path.read_bytes()
        Image.new('RGB',(9,7),'green').save(path)
        fresh=task.read_source()
        self.assertNotEqual(source.image_uris,fresh.image_uris)
        path.write_bytes(old)
        with self.assertRaisesRegex(ValueError,'actual Medical image bytes'):task.verify_source(fresh)
        task.verify_source(source)
        path.unlink()
        with self.assertRaises(FileNotFoundError):task.read_source()
        self.inputs()
        names,_=visual_files('Q9')
        (self.root/names[0]).write_text('broken CSV')
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
from src.baselines.image.sembench_native_medical import MedicalVisualTask
for query in ('Q3','Q8','Q9'):
 task=MedicalVisualTask(sys.argv[2],query)
 gold=task.prepare_evaluation();source=task.read_source();requests=task.requests(source)
 result=task.finish(source,[dict(call_id=r['call_id'],generated_text='{"keep":true}') for r in requests])
 quality=task.evaluate(source,result,checkout=sys.argv[1],evaluation=gold)
 task.verify_source(source)
 assert 'original_metric' in quality
 assert task.encode_result(result)['kind']=='series'
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
