"""Original multi-stage queries over real Arrow/provider/worker and software SQL."""
import asyncio
from collections import Counter
from copy import deepcopy
import json
import threading
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

from integrations.imlane_duckdb import model_ipc_worker as worker
from src.experiments import native_imlane_sembench as entry
from src.infrastructure.private_artifacts import content_digest, read_json
from tests.baselines.text.test_imlane_duckdb_model_ipc_worker import FakeSampling
from tests.experiments import test_native_imlane_aggregate as carrier
from tests.experiments import test_sembench_lotus_image_query as fixture


@unittest.skipUnless(fixture.CHECKOUT and fixture.HAS_SDK,'requires pinned SemBench/LOTUS SDK')
class OriginalQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.NativeLotusImageQueryTests.setUpClass()
        carrier.NativeAggregateTests.setUpClass()

    def setUp(self):
        self.visual=fixture.NativeLotusImageQueryTests();self.visual.setUp();self.addCleanup(self.visual.doCleanups)
        self.native=carrier.NativeAggregateTests();self.native.setUp();self.addCleanup(self.native.doCleanups)
        self.engines=[]

    def config(self,scenario,query,inputs,*,allowance=64,scale_factor=None):
        scale={} if scale_factor is None else dict(scale_factor=scale_factor)
        service,_=self.visual.config(scenario,query,**scale)
        task=dict(scenario=scenario,query=query)
        task.update(dict(image_join=True) if scenario=='mmqa' else dict(visual_query=True))
        task.update({name:str(value) if isinstance(value,Path) else value for name,value in inputs.items() if name!='data_dir'})
        task.update(scale)
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.serving.backends.vllm_protocol import GenerationSpec
        original=SemBenchTextQuery(self.visual.checkout,inputs.get('data_dir'),scenario,query,
            execution_id='original-native-query',model_name=service['model_name'],
            generation=GenerationSpec.from_dict(service['generation']),
            **{k:v for k,v in task.items() if k not in ('scenario','query')})
        config=dict(schema=entry.QUERY_SCHEMA,query_id='original-native-query',task=task,
            sembench_checkout=str(self.visual.checkout),data_dir=None if 'data_dir' not in inputs else str(inputs['data_dir']),
            input_sha256={name:identity['sha256'] for name,identity in original.files.items()},
            model_name=service['model_name'],tokenizer=str(self.native.model),generation=service['generation'],
            model_service=dict(model_python='CPU-no-model',model=str(self.native.model),
                served_model_name='local-aggregate-fixture',dtype='bfloat16',engine_options=dict(max_model_len=32768)),
            max_model_calls=allowance,query_timeout_s=60,cleanup_s=30,stop_grace_s=2,request_timeout_s=60)
        return service,config

    def replies(self,policy,*,wrong_image_type=False,wrong_text_tokens=False):
        case=self
        async def execute(request,unit):
            class Renderer:
                def __init__(self):self.calls=[]
                async def render_chat_async(self,conversations,params):
                    index=len(self.calls);self.calls.append(deepcopy(conversations[0]))
                    image=entry._image_message(conversations[0])
                    prompt=dict(type='multimodal' if image and not wrong_image_type else 'token',
                        prompt_token_ids=[7,8,9,10] if image else [7,99] if wrong_text_tokens else [7,8],fixture_index=index)
                    if image:
                        prompt.update(mm_kwargs={'image':object()},mm_hashes={'image':['software-pixel-identity']},
                                      mm_placeholders={'image':[dict(offset=2,length=2)]})
                    return conversations,[prompt]
            class Engine:
                def __init__(self):
                    self.renderer=Renderer();self.calls=[]
                    self.vllm_config=NS(model_config=NS(max_model_len=case.native.last.services[0].ready_info['max_model_len']))
                async def generate(self,prompt,sampling,request_id):
                    import httpx
                    self.calls.append((prompt,sampling,request_id))
                    body=dict(request['sampling_params'],messages=self.renderer.calls[prompt['fixture_index']],model='local-image-fixture')
                    if body.pop('structured_outputs',None)=={'json_object':True}:body['response_format']={'type':'json_object'}
                    response=policy.respond(httpx.Request('POST','http://localhost:18473/v1/chat/completions',
                        headers=dict(authorization='Bearer '+fixture.KEY_VALUE),json=body))
                    response.raise_for_status()
                    text=response.json()['choices'][0]['message']['content']
                    yield NS(finished=True,request_id=request_id,prompt_token_ids=prompt['prompt_token_ids'],
                        outputs=[NS(index=0,text=text,token_ids=[10],finish_reason='stop')])
                async def abort(self,ids):pass
            engine=Engine();case.engines.append(engine)
            model=worker.NativeModel(engine,FakeSampling,'final-only',object(),'software-fixture',FakeSampling)
            return await worker.ModelWorker({},model,asyncio.Event(),budget=unit).batch(request)
        return lambda request,unit:asyncio.run(execute(request,unit))

    def compare(self,scenario,query,inputs,*,mode='success',ordered_result=True,allowance=64,scale_factor=None):
        import numpy as np
        state=np.random.get_state()
        self.addCleanup(np.random.set_state,state)
        self.last_rng_state=[state[0],state[1].tolist(),int(state[2]),int(state[3]),float(state[4])]
        service,config=self.config(scenario,query,inputs,allowance=allowance,scale_factor=scale_factor)
        reference=fixture.NativeHTTPReplies(self,scenario,query=query,mode=mode)
        expected,_,_=self.visual.invoke(service,inputs,reference,allowance=allowance)
        actual=fixture.NativeHTTPReplies(self,scenario,query=query,mode=mode)
        np.random.set_state(state)
        report=self.native.invoke(config,reply_batch=self.replies(actual))
        self.assertEqual(report['status'],'completed')
        if ordered_result:
            self.assertEqual(report['result'],expected['result'])
        else:
            # SemBench scores/writes values without the pandas index. Preserve
            # actual indices in both reports; tied duplicate positions may vary.
            self.assertEqual(report['result']['columns'],expected['result']['columns'])
            self.assertCountEqual(report['result']['data'],expected['result']['data'])
        actual_quality,expected_quality=deepcopy(report['quality']),deepcopy(expected['quality'])
        if scenario=='animals' and query in ('Q3','Q10'):
            # The original gold SQL returns every tied maximum without ORDER BY.
            self.assertCountEqual(actual_quality.pop('ground_truth'),expected_quality.pop('ground_truth'))
        self.assertEqual(actual_quality,expected_quality)
        self.assertEqual(Counter(content_digest(r['body']['messages']) for r in actual.requests),
                         Counter(content_digest(r['body']['messages']) for r in reference.requests))
        self.assertEqual(report['budget']['consumed_requests'],expected['model_calls'])
        self.assertEqual(report['model_calls'],expected['model_calls'])
        self.assertIsNone(report['cost']);self.assertFalse(report['cost_available'])
        self.assertFalse(report['ipc']['independent_image_processor_reference_verified'])
        self.assertEqual(report['query_clock_version'],'lotus_imlane.source_to_sink.v1')
        events=self.native.last.events
        closes=[stamp for name,stamp in events if name=='model-close']
        sql_closes=[stamp for name,stamp in events if name=='sql-close']
        self.assertEqual(len(closes),len(self.native.last.services))
        if sql_closes:self.assertLessEqual(max(sql_closes),report['result_ready_monotonic_ns'])
        if closes:self.assertLessEqual(report['result_ready_monotonic_ns'],closes[0])
        directories=[item['directory'] for item in report['provider_batches']]
        self.assertEqual(len(directories),len(set(directories)))
        for metadata in report['provider_batches']:
            provider=read_json(Path(metadata['directory'])/'provider.json')
            self.assertEqual(provider['sampling_params'],metadata['native_sampling'])
            self.assertEqual(provider['receipt_dir'],metadata['directory'])
        return report,actual,config

    def assert_native_batches(self,report):
        batches=read_json(self.native.last.output/'original-lm-batches.json')
        ids=[row['call_id'] for batch in batches for row in batch['requests']]
        self.assertEqual(len(ids),len(set(ids)))
        self.assertEqual(len(ids),report['model_calls'])
        attempts=[call['response']['budget_attempt'] for call in report['ipc']['calls']]
        self.assertEqual(sorted(attempts),list(range(1,len(ids)+1)))
        self.assertEqual(len(report['provider_batches']),len(report['prediction_batches']))
        for provider,sql in zip(report['provider_batches'],report['prediction_batches'],strict=True):
            self.assertEqual(sql['status'],'completed')
            self.assertLessEqual(sql['full_sink_observed_monotonic_ns'],sql['sink_release']['published_monotonic_ns'])
            self.assertLessEqual(sql['sink_release']['published_monotonic_ns'],sql['driver_exit_observed_monotonic_ns'])
            self.assertEqual(sql['input_rows'],sum(len(read_json(path)['calls']) for path in
                Path(provider['directory']).glob('*.terminal.json')))
        return batches

    def compare_evaluation_failure(self,scenario,query,inputs,*,scale_factor=None):
        service,config=self.config(scenario,query,inputs,scale_factor=scale_factor)
        reference=fixture.NativeHTTPReplies(self,scenario,query=query)
        with self.assertRaises(ValueError) as expected:self.visual.invoke(service,inputs,reference,allowance=64)
        policy=fixture.NativeHTTPReplies(self,scenario,query=query)
        with self.assertRaises(ValueError) as actual:self.native.invoke(config,reply_batch=self.replies(policy))
        self.assertEqual(str(actual.exception),str(expected.exception))
        self.assertEqual(entry.result_value(actual.exception.query_result),entry.result_value(expected.exception.query_result))
        self.assertEqual(Counter(content_digest(row['body']['messages']) for row in policy.requests),
                         Counter(content_digest(row['body']['messages']) for row in reference.requests))
        failed=read_json(self.native.last.output/'worker.json')
        self.assertIn('full_sink_observed_monotonic_ns',failed)
        self.assertNotIn('quality',failed);self.assertEqual(failed['status'],'failed')
        self.assertEqual(failed['budget']['consumed_requests'],len(policy.requests))
        self.assertTrue((self.native.last.output/'full-sink-observed.json').exists())
        self.assertEqual(sum(name=='model-close' for name,_ in self.native.last.events),1)
        self.assertEqual(sum(name=='sql-open' for name,_ in self.native.last.events),
                         sum(name=='sql-close' for name,_ in self.native.last.events))
        print('CPU_EXPECTED_ORIGINAL_ERROR',scenario,query,type(actual.exception).__name__,str(actual.exception))
        return failed,policy,config

    def test_visual_input_kinds_share_original_factory_and_complete_native_relation(self):
        import pandas as pd
        cases=[('animals',query,self.visual.animals_inputs,None) for query in ('Q3','Q10')]
        cases += [('mmqa',query,self.visual.mmqa_inputs,None) for query in ('Q2a','Q7')]
        cases += [('cars',query,self.visual.cars_inputs,7) for query in ('Q3','Q8')]
        cases += [('medical',query,self.visual.medical_inputs,29) for query in ('Q3','Q9')]
        cases += [('ecomm',query,lambda q=query:self.visual.ecomm_outfit_inputs(q),None)
                  for query in ('Q2','Q4','Q6','Q8','Q9','Q13')]
        for scenario,query,source,scale in cases:
            with self.subTest(scenario=scenario,query=query):
                inputs=source()
                if scenario=='ecomm' and query=='Q8':
                    # Reuse the established Ray fixture's long-description input;
                    # the original Q8 predicate must select a real left row.
                    path=Path(inputs['data_dir'])/'styles_details.parquet'
                    frame=pd.read_parquet(path)
                    frame.at[0,'productDescriptors']={'description':{'value':'long description '*200}}
                    frame.to_parquet(path,index=False)
                if scenario=='ecomm' and query=='Q4':
                    failed,policy,_=self.compare_evaluation_failure(scenario,query,inputs)
                    self.assertEqual(len(policy.requests),5) # duplicate ids/images are not removed
                    self.assertEqual(failed['provider_batches'][0]['native_sampling']['structured_outputs'],{'json_object':True})
                    continue
                report,policy,config=self.compare(scenario,query,inputs,scale_factor=scale)
                batches=self.assert_native_batches(report)
                if scale is not None:
                    self.assertIs(type(config['task']['scale_factor']),int)
                    self.assertEqual(report['source']['visual_inputs']['scale_factor'],scale)
                if scenario=='cars':self.assertGreater(report['model_calls'],10)
                if scenario=='medical' and query=='Q9' or scenario=='ecomm' and query=='Q9':
                    self.assertTrue(all(len(policy.image_uris(row['body']))==2 for row in policy.requests))
                if scenario=='ecomm' and query=='Q6':self.assertEqual(report['model_calls'],2)
                if scenario=='ecomm' and query=='Q9':self.assertEqual(report['model_calls'],25)
                print('CPU_VISUAL_INPUT',scenario,query,'scale',scale,'batch_sizes',
                    [len(batch['requests']) for batch in batches],'result_rows',len(report['result']['data']))

    def test_medical_small_gold_keeps_original_error_and_full_gold_keeps_prelimit_input(self):
        self.compare_evaluation_failure('medical','Q8',self.visual.medical_inputs(),scale_factor=29)
        report,_,_=self.compare('medical','Q8',self.visual.medical_inputs(103),scale_factor=29,allowance=128)
        self.assertGreater(report['model_calls'],100)
        self.assertEqual(len(report['result']['data']),100)
        self.assert_native_batches(report)
        print('CPU_MEDICAL_PRELIMIT','model_calls',report['model_calls'],'sink_rows',len(report['result']['data']))

    def test_empty_visual_relations_keep_actual_model_allocation_and_original_sink(self):
        report,_,_=self.compare('medical','Q9',self.visual.medical_inputs(),scale_factor=29,mode='no_matches')
        self.assertEqual(report['result']['data'],[])
        inputs=self.visual.ecomm_outfit_inputs('Q8') # no >=3000-character descriptions
        report,_,_=self.compare('ecomm','Q8',inputs)
        self.assertEqual(report['model_calls'],0);self.assertEqual(report['provider_batches'],[])
        self.assertEqual(report['result']['data'],[])
        # Expected total is unknown at preflight; keep the real allocated owner
        # visible even though this actual original graph made no model requests.
        self.assertIsNone(report['source']['expected_model_calls'])
        self.assertEqual(len(self.native.last.services),1)
        print('CPU_EMPTY_VISUAL','ecomm Q8','model_calls',report['model_calls'],'allocated_owners',len(self.native.last.services))

    def test_missing_visual_asset_fails_before_any_native_model_or_sql(self):
        inputs=self.visual.cars_inputs()
        service,config=self.config('cars','Q3',inputs,scale_factor=7)
        import pandas as pd
        from src.baselines.text.sembench_cars import visual_files
        images=visual_files(7)[1]
        Path(pd.read_csv(Path(inputs['data_dir'])/images).iloc[0]['image_path']).unlink()
        reference=fixture.NativeHTTPReplies(self,'cars',query='Q3')
        with self.assertRaises(FileNotFoundError) as expected:self.visual.invoke(service,inputs,reference,allowance=64)
        with self.assertRaises(FileNotFoundError) as actual:
            self.native.invoke(config,reply_batch=self.replies(fixture.NativeHTTPReplies(self,'cars',query='Q3')))
        self.assertEqual(str(actual.exception),str(expected.exception))
        self.assertEqual(self.native.last.events,[]);self.assertEqual(self.native.last.services,[])
        self.assertFalse(self.native.last.output.exists())

    def test_original_branch_joins_keep_all_filters_actual_inputs_and_full_relations(self):
        for query,filters in (('Q10',3),('Q11',4)):
            with self.subTest(query=query):
                report,_,_=self.compare('ecomm',query,self.visual.ecomm_outfit_inputs(query))
                batches=self.assert_native_batches(report)
                self.assertEqual([len(batch['requests']) for batch in batches[:filters]],[5]*filters)
                self.assertEqual(len(report['semantic']['native_join_inputs']),2 if query=='Q10' else 3)
                self.assertTrue(all(not call['all_declared_columns_selected'] for call in
                    report['semantic']['native_join_inputs']))
                self.assertIsNone(report['source']['expected_model_calls'])
                self.assertIsNone(report['source']['maximum_model_calls'])
                print('CPU_BRANCH_QUERY',query,'batch_sizes',[len(batch['requests']) for batch in batches],
                      'result',report['result'])

    def test_groupby_topk_threads_share_serial_sql_and_failed_groups_keep_original_failure(self):
        from src.experiments import native_imlane_topk as carrier_sql
        from integrations.imlane_duckdb.model_provider import ModelBatchError
        main=threading.get_ident()
        lock=threading.Lock();gate=threading.Barrier(2)
        stats=dict(active=0,maximum=0,group_entries=0,group_threads=set(),sql_threads=set())
        original_make=entry.make_query_lm;original_sql=carrier_sql.SQLPredictions.__call__
        def make(query,predict,**options):
            def observed(requests,**kwargs):
                group=threading.get_ident()!=main
                with lock:
                    if group:
                        stats['group_entries']+=1;stats['group_threads'].add(threading.get_ident())
                    synchronize=group and stats['group_entries']<=2
                # Only the first two real SDK worker callbacks rendezvous,
                # outside the product's SQL lock. No SDK executor is replaced.
                if synchronize:gate.wait(timeout=5)
                return predict(requests,**kwargs)
            return original_make(query,observed,**options)
        def sql(owner,requests):
            with lock:
                stats['active']+=1;stats['maximum']=max(stats['maximum'],stats['active'])
                stats['sql_threads'].add(threading.get_ident())
            try:return original_sql(owner,requests)
            finally:
                with lock:stats['active']-=1
        with patch.object(entry,'make_query_lm',side_effect=make),patch.object(carrier_sql.SQLPredictions,'__call__',sql):
            report,_,config=self.compare('ecomm','Q14',self.visual.ecomm_outfit_inputs('Q14'),ordered_result=False)
        self.assertGreaterEqual(len(stats['group_threads']),2)
        self.assertTrue(stats['group_threads']<=stats['sql_threads'])
        self.assertEqual(stats['maximum'],1);self.assertEqual(stats['active'],0)
        self.assertEqual(stats['group_entries'],4)
        batches=self.assert_native_batches(report)
        self.assertEqual([len(batch['requests']) for batch in batches[:2]],[5,8])
        self.assertEqual(sorted(len(batch['requests']) for batch in batches[2:]),[1,1,1,1])
        print('CPU_GROUPBY_QUERY',json.dumps(dict(rng_state=self.last_rng_state,
            native_group_callbacks=stats['group_entries'],group_worker_threads=len(stats['group_threads']),
            maximum_simultaneous_sql_scopes=stats['maximum'],batch_sizes=[len(b['requests']) for b in batches],
            result_values=report['result']['data']),sort_keys=True))
        policy=fixture.NativeHTTPReplies(self,'ecomm',query='Q14')
        response=policy.respond
        def fail_group(request):
            body=json.loads(request.content)
            if 'Document 1' in json.dumps(body) and 'Document 2' in json.dumps(body):
                raise RuntimeError('software original group model failure')
            return response(request)
        with patch.object(policy,'respond',side_effect=fail_group),self.assertRaises(ModelBatchError):
            self.native.invoke(config,reply_batch=self.replies(policy))
        failed=read_json(self.native.last.output/'worker.json')
        self.assertEqual(failed['status'],'failed');self.assertNotIn('quality',failed)
        self.assertNotIn('full_sink_observed_monotonic_ns',failed)
        calls=read_json(self.native.last.output/'original-lm-batches.json')
        self.assertTrue(any(call['status']=='failed' for call in calls))
        events=self.native.last.events
        self.assertEqual(sum(name=='sql-open' for name,_ in events),sum(name=='sql-close' for name,_ in events))
        self.assertEqual(sum(name=='model-close' for name,_ in events),1)
        self.assertLessEqual(max(stamp for name,stamp in events if name=='sql-close'),
                             next(stamp for name,stamp in events if name=='model-close'))

    def test_complete_profile_requires_plain_base_without_changing_legacy_configuration(self):
        _,config=self.config('mmqa','Q2b',self.visual.mmqa_inputs())
        self.assertEqual(entry.configuration(config)[0],config)
        for change in (dict(response_format={'type':'json_object'}),dict(logprobs=0),dict(repetition_penalty=1.3)):
            bad=dict(config,generation=dict(config['generation'],**change))
            with self.assertRaisesRegex(ValueError,'plain base generation'):entry.configuration(bad)
        for change in (dict(visual_query='false'),dict(image_join=1)):
            with self.assertRaisesRegex(ValueError,'explicit booleans'):
                entry.configuration(dict(config,task=dict(config['task'],**change)))
        legacy=dict(config,schema=entry.TEXT_SCHEMA,task=dict(scenario='mmqa',query='4'),
                    generation=dict(config['generation'],response_format={'type':'json_object'}))
        self.assertEqual(entry.configuration(legacy)[0],legacy)
        self.assertEqual(entry.configuration(legacy)[1].response_format,{'type':'json_object'})

    def test_animals_manifest_original_single_and_independent_filters(self):
        for query in ('Q1','Q7'):
            with self.subTest(query=query):
                report,_,config=self.compare('animals',query,self.visual.animals_inputs())
                self.assertIsNone(config['data_dir'])
                path=self.native.source.root/('cli-'+query+'.json');path.write_text(json.dumps(config))
                output=self.native.source.root/('check-'+query)
                self.assertEqual(entry.main(['check','--config',str(path),'--output',str(output)]),0)
                checked=read_json(output/'CHECK.json')
                self.assertFalse(checked['target_VLM_qualified'])
                self.assertEqual(checked['model_calls'],0)
                self.assertEqual([b['occurrences'] for b in report['native_lm_batches']],[4,4] if query=='Q7' else [4])
                self.assertEqual(report['ipc']['image_occurrences'],report['model_calls'])

    def test_mmqa_nonempty_original_join_drives_json_extract_with_actual_sampling(self):
        report,policy,_=self.compare('mmqa','Q2b',self.visual.mmqa_inputs())
        self.assertEqual([b['occurrences'] for b in report['native_lm_batches']],[4,4])
        self.assertIsNone(report['source']['expected_model_calls'])
        self.assertIsNone(report['provider_batches'][0]['original_sampling'].get('response_format'))
        self.assertEqual(report['provider_batches'][1]['original_sampling']['response_format'],{'type':'json_object'})
        self.assertEqual(report['provider_batches'][1]['native_sampling']['structured_outputs'],{'json_object':True})
        self.assertNotIn('response_format',report['provider_batches'][1]['native_sampling'])
        self.assertTrue(all(type(call[1].structured_outputs) is FakeSampling for call in self.engines[-1].calls))

    def test_model_session_repeats_original_nonempty_join_then_json_extract_without_restarting(self):
        _,config=self.config('mmqa','Q2b',self.visual.mmqa_inputs(),allowance=32)
        config['lifecycle']='model-session'
        policy=fixture.NativeHTTPReplies(self,'mmqa',query='Q2b')
        report=self.native.invoke(config,reply_batch=self.replies(policy))
        self.assertEqual(len(self.native.last.services),1)
        self.assertTrue(report['warm_qualified'])
        self.assertEqual(report['budget']['consumed_requests'],16)
        for value in report['queries']:
            self.assertEqual([b['occurrences'] for b in value['native_lm_batches']],[4,4])
            self.assertEqual(value['provider_batches'][1]['native_sampling']['structured_outputs'],{'json_object':True})
            self.assertEqual(value['model_calls'],8)
            self.assertEqual(value['ipc']['image_occurrences'],8)
        self.assertEqual(sum(name=='model-close' for name,_ in self.native.last.events),1)

    def test_model_session_zero_work_never_qualifies_a_warm_model(self):
        _,config=self.config('ecomm','Q8',self.visual.ecomm_outfit_inputs('Q8'),allowance=8)
        config['lifecycle']='model-session'
        policy=fixture.NativeHTTPReplies(self,'ecomm',query='Q8')
        report=self.native.invoke(config,reply_batch=self.replies(policy))
        self.assertEqual([value['model_calls'] for value in report['queries']],[0,0])
        self.assertFalse(report['warm_qualified'])
        self.assertEqual(report['budget']['consumed_requests'],0)
        self.assertEqual(sum(name=='model-close' for name,_ in self.native.last.events),1)

    def test_mmqa_empty_join_keeps_native_KeyError_and_closes_sql_and_model(self):
        inputs = self.visual.mmqa_inputs()
        service, config = self.config('mmqa', 'Q2b', inputs, allowance=4)
        original = fixture.NativeHTTPReplies(self, 'mmqa', query='Q2b', mode='no_matches')
        with self.assertRaises(KeyError) as reference:
            self.visual.invoke(service, inputs, original, allowance=4)
        replies = fixture.NativeHTTPReplies(self, 'mmqa', query='Q2b', mode='no_matches')
        with self.assertRaises(KeyError) as native:
            self.native.invoke(config, reply_batch=self.replies(replies))
        self.assertEqual(str(native.exception), str(reference.exception))
        failed = read_json(self.native.last.output/'worker.json')
        self.assertEqual(failed['status'], 'failed')
        self.assertNotIn('quality', failed)
        self.assertNotIn('full_sink_observed_monotonic_ns', failed)
        self.assertEqual(failed['budget']['consumed_requests'], 4)
        events = self.native.last.events
        self.assertEqual(sum(name=='sql-open' for name,_ in events), 1)
        self.assertEqual(sum(name=='sql-close' for name,_ in events), 1)
        self.assertEqual(sum(name=='model-close' for name,_ in events), 1)

    def test_ecomm_text_survivors_reach_original_image_extract_and_empty_keeps_error(self):
        report,_,_=self.compare('ecomm','Q12',self.visual.ecomm_inputs())
        self.assertEqual([b['occurrences'] for b in report['native_lm_batches']],[4,3])
        self.assertEqual(report['ipc']['image_occurrences'],3)
        # Native-only image evidence may never disable a text row's reference.
        batch=read_json(self.native.last.output/'original-lm-batches.json')[0]
        provider=report['provider_batches'][0]
        with self.assertRaisesRegex(ValueError,'image occurrence'):
            entry.audit_ipc(Path(provider['directory']),batch['requests'],[None]*4,batch['predictions'],
                provider['native_sampling'],attempt_offset=provider['attempt_offset'],native_input_only=True)
        service,config=self.config('ecomm','Q12',self.visual.ecomm_inputs())
        original=fixture.NativeHTTPReplies(self,'ecomm',query='Q12',mode='no_matches')
        with self.assertRaises(KeyError) as reference:self.visual.invoke(service,dict(data_dir=Path(config['data_dir'])),original,allowance=64)
        replies=fixture.NativeHTTPReplies(self,'ecomm',query='Q12',mode='no_matches')
        with self.assertRaises(KeyError) as native:self.native.invoke(config,reply_batch=self.replies(replies))
        self.assertEqual(str(native.exception),str(reference.exception))
        failed=read_json(self.native.last.output/'worker.json')
        self.assertEqual(failed['status'],'failed');self.assertNotIn('full_sink_observed_monotonic_ns',failed)
        self.assertEqual(failed['budget']['consumed_requests'],4)
        self.assertEqual(sum(name=='model-close' for name,_ in self.native.last.events),1)

    def test_receipt_token_or_native_input_corruption_fails_after_retaining_full_result(self):
        for image in (False,True):
            _,config=self.config('ecomm','Q12',self.visual.ecomm_inputs())
            replies=fixture.NativeHTTPReplies(self,'ecomm',query='Q12')
            with self.assertRaisesRegex(ValueError,'tokens|multimodal'):
                self.native.invoke(config,reply_batch=self.replies(replies,wrong_image_type=image,wrong_text_tokens=not image))
            failed=read_json(self.native.last.output/'worker.json')
            self.assertEqual(failed['status'],'failed');self.assertIn('full_sink_observed_monotonic_ns',failed)
            self.assertTrue((self.native.last.output/'full-sink-observed.json').exists())
            self.assertNotIn('quality',failed)

    def test_finite_budget_failure_preserves_close_and_original_config_rejects_other_json(self):
        _,config=self.config('animals','Q7',self.visual.animals_inputs(),allowance=8)
        # The known two-pass minimum must be covered before execution.
        config['max_model_calls']=5
        replies=fixture.NativeHTTPReplies(self,'animals',query='Q7')
        with self.assertRaises(ValueError):entry.prepare_query(config)
        _,config=self.config('mmqa','Q2b',self.visual.mmqa_inputs(),allowance=5)
        replies=fixture.NativeHTTPReplies(self,'mmqa',query='Q2b')
        from src.experiments.attempt_ledger import BudgetExhausted
        with self.assertRaises(BudgetExhausted):self.native.invoke(config,reply_batch=self.replies(replies))
        failed=read_json(self.native.last.output/'worker.json')
        self.assertEqual(failed['budget']['consumed_requests'],4)
        self.assertEqual(sum(name=='model-close' for name,_ in self.native.last.events),1)
        with self.assertRaisesRegex(ValueError,'JSON-object'):
            entry._native_sampling(dict(response_format={'type':'json_schema'}))


if __name__=='__main__':unittest.main()
