"""Original multi-stage queries over real Arrow/provider/worker and software SQL."""
import asyncio
from collections import Counter
from copy import deepcopy
import json
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

    def config(self,scenario,query,inputs,*,allowance=64):
        service,_=self.visual.config(scenario,query)
        task=dict(scenario=scenario,query=query)
        task.update(dict(image_join=True) if scenario=='mmqa' else dict(visual_query=True))
        task.update({name:str(value) for name,value in inputs.items() if name!='data_dir'})
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

    def compare(self,scenario,query,inputs,*,mode='success'):
        service,config=self.config(scenario,query,inputs)
        reference=fixture.NativeHTTPReplies(self,scenario,query=query,mode=mode)
        expected,_,_=self.visual.invoke(service,inputs,reference,allowance=64)
        actual=fixture.NativeHTTPReplies(self,scenario,query=query,mode=mode)
        report=self.native.invoke(config,reply_batch=self.replies(actual))
        self.assertEqual(report['status'],'completed')
        self.assertEqual(report['result'],expected['result'])
        self.assertEqual(report['quality'],expected['quality'])
        self.assertEqual(Counter(content_digest(r['body']['messages']) for r in actual.requests),
                         Counter(content_digest(r['body']['messages']) for r in reference.requests))
        self.assertEqual(report['budget']['consumed_requests'],expected['model_calls'])
        self.assertEqual(report['model_calls'],expected['model_calls'])
        self.assertIsNone(report['cost']);self.assertFalse(report['cost_available'])
        self.assertFalse(report['ipc']['independent_image_processor_reference_verified'])
        self.assertEqual(report['query_clock_version'],'lotus_imlane.source_to_sink.v1')
        events=self.native.last.events
        self.assertEqual(sum(name=='model-close' for name,_ in events),1)
        self.assertLessEqual(max(stamp for name,stamp in events if name=='sql-close'),report['result_ready_monotonic_ns'])
        self.assertLessEqual(report['result_ready_monotonic_ns'],next(stamp for name,stamp in events if name=='model-close'))
        directories=[item['directory'] for item in report['provider_batches']]
        self.assertEqual(len(directories),len(set(directories)))
        for metadata in report['provider_batches']:
            provider=read_json(Path(metadata['directory'])/'provider.json')
            self.assertEqual(provider['sampling_params'],metadata['native_sampling'])
            self.assertEqual(provider['receipt_dir'],metadata['directory'])
        return report,actual,config

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
