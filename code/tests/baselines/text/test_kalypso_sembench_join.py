"""Pinned CP/Filter bodies, repeated texts and original whole-query Join sinks."""
from src.baselines.text.frameworks.kalypso_native.query_runner import KalypsoRunner
import asyncio
import copy
import json
import os
from pathlib import Path
from types import ModuleType,SimpleNamespace as NS
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import adapter,join_binding,join_inputs,join_cpu,map_observation,native_server,sembench_inputs,sembench_join
from src.baselines.text.sembench_prepared import result_value
from src.experiments.kalypso_query_lifecycle import query_driver_type
from tests.baselines.text.kalypso_join_author import methods
from tests.experiments import test_sembench_text_original as originals

READY=originals.READY and os.environ.get('SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT')


@unittest.skipUnless(READY,'requires pinned original SemBench and author05024 objects')
class NativeJoinContracts(unittest.TestCase):
    def setUp(self):
        self.helper=originals.OriginalSemBenchTextTests();self.helper.setUp();self.addCleanup(self.helper.doCleanups)
        self.helper._write_ecomm_inputs()
        self.manager=NS(token_length=lambda text:len(str(text)))

    def bound(self,scenario,number,qid='q'):
        spec=dict(task='sembench-join',scenario=scenario,query=number,query_id=qid)
        query=sembench_inputs.SemBenchTextQuery(os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'],self.helper.data,
            **native_server.query_task(spec),execution_id=qid,model_name='openai/local-fixture',generation=self.helper.generation)
        count=len(query.operation.records);spec.update(expected_calls=count,max_native_calls=count)
        unit=dict(call_cap=count,queries=[spec])
        cfg=dict(sembench=os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'],data_dir=str(self.helper.data),
            model='m',model_name='openai/local-fixture',native_trace=str(self.helper.data/(qid+'-trace/native-trace.jsonl')),
            map_prompt_root=str(self.helper.data/(qid+'-prompts')),units=[unit],execution_enabled=False,CPU_fixture_only=True,
            source_data_sha256={name:adapter.sha(self.helper.data/name) for name in query.files})
        return cfg,unit,spec

    def run_query(self,cfg,unit,spec,answers):
        trace=Path(cfg['native_trace']).parent;trace.mkdir()
        (trace/'loaded-model.json').write_text(json.dumps(dict(model_uid='u')))
        emitted=[];messages={};self_test=self
        def transport(payload,qid,directory):
            self_test.assertEqual(payload['ops'][0]['op'],'join')
            with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],self.manager) as author:
                prompt_module=ModuleType('vllm.kalypso.sem_ops.prompt_utils');prompt_module.__dict__.update(
                    {name:value for name,value in vars(author).items() if not name.startswith('__')})
                base=ModuleType('vllm.kalypso.sem_ops.base');base.OpName=author.OpName
                joins=ModuleType('vllm.kalypso.sem_ops.joins');joins.CartesianProduct=author.CartesianProduct
                class Executor:
                    model='m'
                    async def chatcomplete(self,raw_request,prompt,max_tokens,pin=False,priority=0):
                        idx=native_server.ROW.get();messages[idx]=copy.deepcopy(prompt)
                        return NS(text=answers[idx],request_id=qid+'-'+str(idx),finish_reason='stop')
                    async def execute(self,**kwargs):return await self.chatcomplete(**kwargs)
                modules={m.__name__:m for m in (prompt_module,base,joins)}
                with patch.dict('sys.modules',modules),patch.object(map_observation,'install_response_observation',return_value=[]):
                    counter=native_server.install_hooks(cfg,unit,trace,Executor,author.SemFilter)
                executor=Executor();raw=NS(headers={'X-Kalypso-Observation':qid})
                left=list(author._data_source(raw,payload['data_path'],executor))
                right=list(author._data_source(None,payload['ops'][0]['args']['right_table'],None))
                product=author.CartesianProduct(right);predicate=author.SemFilter(payload['ops'][0]['args']['instruction'])
                children=[child for ctx in left for child in product(ctx)]
                self_test.assertEqual([ctx.state.idx for ctx in children],list(range(len(answers))))
                async def run():
                    result=[]
                    # Complete in reverse order; repeated texts retain distinct pair IDs.
                    for ctx in reversed(children):
                        if await predicate(ctx):result.append(ctx)
                    with self_test.assertRaisesRegex(RuntimeError,'budget'):
                        await executor.chatcomplete(raw,[],8)
                    return result
                output=asyncio.run(run())
                self_test.assertEqual(counter['reserved_calls'],len(answers))
                return dict(model_name='m',num_output_rows=len(output),latency_sec=.1,
                    results=[dict(idx=ctx.state.idx,input_context=ctx.input.data,
                        outputs=[dict(value=next(iter(item.values()))) for item in ctx.output]) for ctx in output])
        def save(path,value):path.write_text(json.dumps(value))
        with patch.object(adapter,'load_dependencies',return_value=save):
            driver=KalypsoRunner(cfg,unit,self.helper.data/(spec['query_id']+'-run'),transport)
            driver.model_uid='u';report=driver.query(spec,emitted.append)
        return report,emitted,messages

    def test_original_movie_and_multifield_ecomm_joins_keep_full_sink_and_score(self):
        for index,(scenario,number) in enumerate([('movie','5'),('movie','6'),('movie','7'),('ecomm','7')]):
            with self.subTest(scenario=scenario,query=number):
                cfg,unit,spec=self.bound(scenario,number,str(index))
                self.assertIs(query_driver_type(unit),KalypsoRunner)
                query=sembench_inputs.make_query(cfg,str(index),spec,kind='join')
                answers=[('true','false and true','unparseable')[i%3] for i in range(spec['expected_calls'])]
                canonical=['False' if 'false' in text else 'True' for text in answers]
                expected,_=self.helper._join_reference(query,canonical);quality=query.evaluate(expected)
                with patch.object(join_inputs,'binding',return_value={'explicit_CPU_fixture':True}):
                    frozen=join_inputs.prepare_inputs(cfg,unit,self.helper.data/(str(index)+'-cpu'))
                    cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
                    _,loaded=join_inputs.load_inputs(cfg)
                table=loaded[0]['table']
                self.assertEqual(len(table['left'])*len(table['right']),len(answers))
                report,events,messages=self.run_query(cfg,unit,spec,answers)
                self.assertEqual(report['result'],result_value(expected))
                self.assertEqual(report['quality']['original_metric'],quality['original_metric'])
                self.assertEqual(report['physical_native_calls'],len(answers))
                self.assertEqual(len(messages),len(answers))
                self.assertEqual(events,['memory_sink_ready','result_ready'])
                self.assertEqual(report['memory_sink_timing']['query_clock_version'],'sembench.source_to_sink.v3')
                if scenario=='ecomm':
                    self.assertEqual(set(table['left'][0]['fields']),{'productDisplayName:left','productDescriptors:left'})
                    self.assertEqual(set(table['right'][0]['fields']),{'productDisplayName:right','productDescriptors:right'})
                    self.assertIn(table['left'][0]['fields']['productDescriptors:left'],messages[0][-1]['content'])

    def test_all_negative_original_join_restores_empty_relation_without_retry(self):
        cfg,unit,spec=self.bound('movie','5')
        query=sembench_inputs.make_query(cfg,'q',spec,kind='join')
        expected,_=self.helper._join_reference(query,['False']*spec['expected_calls'])
        report,events,_=self.run_query(cfg,unit,spec,['false']*spec['expected_calls'])
        self.assertEqual(report['result'],result_value(expected))
        self.assertEqual(report['physical_call_cap'],spec['expected_calls'])

    def test_nonjoin_query_is_rejected_before_native_model(self):
        cfg,unit,spec=self.bound('ecomm','7')
        for number in ('1','3','5'):
            with self.assertRaisesRegex(ValueError,'ordinary text Join'):
                sembench_inputs.make_query(cfg,'q',dict(spec,query=number),kind='join')

    @unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_TOKENIZER'),'requires an existing local tokenizer')
    def test_native_planner_full_pair_token_preflight_and_context_boundary(self):
        from transformers import AutoTokenizer
        tokenizer=AutoTokenizer.from_pretrained(os.environ['SEMLOOM_NATIVE_TEST_TOKENIZER'],local_files_only=True,trust_remote_code=False)
        cfg,unit,spec=self.bound('ecomm','7')
        with patch.object(join_inputs,'binding',return_value={'explicit_CPU_fixture':True}):
            frozen=join_inputs.prepare_inputs(cfg,unit,self.helper.data/'cpu-inputs')
            cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
            class Manager:
                bytes_per_token=1
                def __init__(self):self.tokenizer=tokenizer;self.registrations=[]
                def capacity(self):return 6*1024**3
                def reset_stages(self):self.registrations.clear()
                def register_stage(self,*args,**kwargs):self.registrations.append((args,kwargs))
                def apply_chat_template(self,messages):return tokenizer.apply_chat_template(messages,tokenize=False,add_generation_prompt=True)
                def token_length(self,text):return len(tokenizer.encode(text))
            manager=Manager()
            with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],manager,planning=True) as author:
                prompt_module=ModuleType('vllm.kalypso.sem_ops.prompt_utils');prompt_module.__dict__.update(
                    {name:value for name,value in vars(author).items() if not name.startswith('__')})
                base=ModuleType('vllm.kalypso.sem_ops.base');base.OpName=author.OpName
                joins=ModuleType('vllm.kalypso.sem_ops.joins');joins.CartesianProduct=author.CartesianProduct
                filters=ModuleType('vllm.kalypso.sem_ops.filters');filters.SemFilter=author.SemFilter
                executor=NS(_build_chat_request=lambda prompt,cap,pin,priority:NS(messages=prompt,max_tokens=cap,
                    add_generation_prompt=True,add_special_tokens=False))
                output=self.helper.data/'cpu-preview';output.mkdir()
                modules={m.__name__:m for m in (prompt_module,base,joins,filters)}
                with patch.dict('sys.modules',modules),patch.object(author.SemFilter,'_run_single',
                        side_effect=AssertionError('CPU preflight submitted a model')):
                    report=join_cpu.check(cfg,output,executor,manager,author.SemanticPlan,NS(_data_source=author._data_source),100000)
                self.assertEqual(report['plans'][0]['logical_occurrences'],spec['expected_calls'])
                self.assertEqual(report['plans'][0]['plan_segments'],2)
                self.assertEqual(len(manager.registrations),1)
                previews=json.loads((output/'q-native-prompts.json').read_text())
                for row in previews:
                    expected=tokenizer.apply_chat_template(row['messages'],tokenize=True,add_generation_prompt=True)
                    self.assertEqual(row['token_ids'],expected)
                limit=report['plans'][0]['max_prompt_tokens']+7
                # Restore unwrapped classes for a separate preflight; nested
                # observation hooks are not a native configuration.
            with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],manager,planning=True) as author:
                for module,cls,name in [(joins,author.CartesianProduct,'CartesianProduct'),(filters,author.SemFilter,'SemFilter')]:
                    setattr(module,name,cls)
                prompt_module.__dict__.update({name:value for name,value in vars(author).items() if not name.startswith('__')})
                base.OpName=author.OpName
                with patch.dict('sys.modules',modules),self.assertRaisesRegex(ValueError,'output cap'):
                    join_cpu.check(cfg,output,executor,manager,author.SemanticPlan,NS(_data_source=author._data_source),limit)

    def test_rectangular_identical_contexts_keep_pair_identity_and_native_preplan_estimate(self):
        with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],self.manager) as author:
            qid='rect';root=self.helper.data/'rect-prompts';root.mkdir()
            table=dict(query_id=qid,instruction='Compare {text:left} to {text:right}',
                left=[dict(data='same {text:right}',fields={'text:left':'same {text:right}'})]*2,
                right=[dict(data='same',fields={'text:right':'same'})]*3)
            (root/(qid+'.json')).write_text(json.dumps(table))
            cfg=dict(map_prompt_root=str(root));unit=dict(queries=[dict(query_id=qid,expected_calls=6)])
            prompt_module=ModuleType('vllm.kalypso.sem_ops.prompt_utils');prompt_module.__dict__.update(
                {name:value for name,value in vars(author).items() if not name.startswith('__')})
            base=ModuleType('vllm.kalypso.sem_ops.base');base.OpName=author.OpName
            raw=NS(headers={'X-Kalypso-Observation':qid})
            left=[author.SemContext(input=author.SemanticInput(data=row['data']),
                state=author.ExecutionState(raw_request=raw,idx=i)) for i,row in enumerate(table['left'])]
            right=[author.SemContext(input=author.SemanticInput(data=row['data']),state=author.ExecutionState(idx=i))
                for i,row in enumerate(table['right'])]
            product=author.CartesianProduct(right);predicate=author.SemFilter(table['instruction'])
            original_run=author.SemFilter._run_single
            self.assertEqual([ctx.state.idx for ctx in product(left[0])],[-1,-1,-1])
            preplan=predicate._build_prompts(left[0])
            with patch.dict('sys.modules',{prompt_module.__name__:prompt_module,base.__name__:base}):
                join_binding.install(cfg,unit,native_server,author.CartesianProduct,author.SemFilter)
            self.assertIs(author.SemFilter._run_single,original_run)
            self.assertEqual(predicate._build_prompts(left[0]),preplan)
            children=[child for ctx in left for child in product(ctx)]
            self.assertEqual([ctx.state.idx for ctx in children],list(range(6)))
            self.assertEqual([ctx.state.idx for ctx in product(left[0])],[0,1,2])
            for ctx in children:
                _,prompt=predicate._build_prompts(ctx)
                self.assertIn('Compare same {text:right} to same',prompt[-1]['content'])
            token=native_server.ROW.set(0)
            try:
                with self.assertRaisesRegex(ValueError,'lacks its right context'):predicate._build_prompts(left[0])
            finally:native_server.ROW.reset(token)


if __name__=='__main__':unittest.main()
