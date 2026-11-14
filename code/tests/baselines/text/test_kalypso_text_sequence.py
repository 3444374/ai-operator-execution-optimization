"""One Kalypso owner across native text queries; real author methods, fake model."""
import asyncio
import json
import os
from pathlib import Path
from types import ModuleType, SimpleNamespace as NS
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import (
    adapter, map_observation, movie_map_cpu, movie_map_inputs, native_server, sembench_inputs,
)
from src.baselines.text.frameworks.kalypso_native.query_runner import KalypsoRunner
from src.baselines.text.sembench_prepared import result_value
from src.experiments.kalypso_query_lifecycle import query_driver_type
from tests.baselines.text.kalypso_join_author import methods
from tests.experiments import test_sembench_text_original as originals


@unittest.skipUnless(originals.READY and os.environ.get('SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'),
                     'requires pinned public SemBench, SDK and author05024 Git objects')
class NativeTextSequenceTests(unittest.TestCase):
    def setUp(self):
        self.helper=originals.OriginalSemBenchTextTests()
        self.helper.setUp();self.addCleanup(self.helper.doCleanups)
        self.manager=NS(bytes_per_token=1,capacity=lambda:1024**3,
            token_length=lambda value:len(str(value)),apply_chat_template=lambda value:str(value),
            reset_stages=lambda:None,register_stage=lambda *args,**kwargs:None)
        self.manager.tokenizer=NS(apply_chat_template=lambda value,**kwargs:str(value),
                                  encode=lambda text,**kwargs:list(range(len(text))))

    def binding(self, *, filter_first=False):
        cases=[('map-first','sembench-map','movie','8','POSITIVE'),
               ('filter','sembench-filter','mmqa','3a','True'),
               ('extract','sembench-extract-map','mmqa','1','{"director":"Michael Ritchie"}'),
               ('map-repeat','sembench-map','movie','8','POSITIVE')]
        if filter_first:cases[0],cases[1]=cases[1],cases[0]
        specs, answers, files=[],{},set()
        for qid,task,scenario,key,text in cases:
            original=self.helper._query(scenario,key)
            count=len(original.operation.records)
            spec=dict(query_id=qid,task=task,scenario=scenario,query=key,expected_calls=count)
            if task in native_server.MAP_TASKS:spec['max_native_calls']=2*count
            specs.append(spec);answers[qid]=text;files.update(original.files)
        unit=dict(call_cap=sum(native_server.physical_call_caps(dict(call_cap=0,queries=specs)).values()),queries=specs)
        cfg=dict(sembench=originals.CHECKOUT,data_dir=str(self.helper.data),model='m',model_name='openai/local-fixture',
            units=[unit],source_commit=native_server.AUTHOR_SOURCE_COMMIT,model_revision='software',expected_template_sha256='software',
            execution_enabled=False,CPU_fixture_only=True,native_trace=str(self.helper.data/'trace/native-trace.jsonl'),
            map_prompt_root=str(self.helper.data/'prompts'),source_data_sha256={f:adapter.sha(self.helper.data/f) for f in files})
        return cfg,unit,answers

    @staticmethod
    def native_modules(author):
        result={}
        for name,attributes in [('prompt_utils',vars(author)),('base',{'OpName':author.OpName}),
                ('filters',{'SemFilter':author.SemFilter}),('map',{'SemMap':author.SemMap})]:
            module=ModuleType('vllm.kalypso.sem_ops.'+name);module.__dict__.update(attributes)
            result['vllm.kalypso.sem_ops.'+name]=module
        estimator=ModuleType('vllm.kalypso.controller.map_estimator');estimator.MapRatioEstimator=author.MapRatioEstimator
        context=ModuleType('vllm.kalypso.context');context.RETRY_TASK=author.RETRY_TASK
        result.update({estimator.__name__:estimator,context.__name__:context})
        return result

    def test_one_owner_keeps_original_query_sinks_and_per_query_native_policies(self):
        cfg,unit,answers=self.binding();self.assertIs(query_driver_type(unit),KalypsoRunner)
        expected={}
        for spec in unit['queries']:
            kind='extract' if spec['task']=='sembench-extract-map' else 'filter' if spec['task']=='sembench-filter' else 'map'
            query=sembench_inputs.make_query(cfg,spec['query_id'],spec,kind=kind)
            frame,_=self.helper._reference(query,[answers[spec['query_id']]]*spec['expected_calls'])
            expected[spec['query_id']]=(result_value(frame),query.evaluate(frame))
        root=Path(cfg['native_trace']).parent;root.mkdir();(root/'loaded-model.json').write_text('{"model_uid":"owner"}')
        calls=[]
        with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],self.manager,planning=True,mapping=True) as author:
            class Executor:
                model='m'
                def _build_chat_request(self,prompt,max_tokens,pin,priority):
                    return NS(messages=prompt,max_tokens=max_tokens,model='m',temperature=0,top_p=1,seed=42,
                        frequency_penalty=.5,repetition_penalty=1.3,stop=[],add_generation_prompt=True,add_special_tokens=False)
                async def chatcomplete(self,raw_request,prompt,max_tokens,pin=False,priority=0):
                    qid=raw_request.headers['X-Kalypso-Observation'];self._build_chat_request(prompt,max_tokens,pin,priority)
                    calls.append(dict(query=qid,idx=native_server.ROW.get(),max_tokens=max_tokens))
                    retry=qid=='map-first' and len(calls)==1 and max_tokens<8192
                    return NS(text=answers[qid],request_id='software-'+str(len(calls)),finish_reason='length' if retry else 'stop')
                async def execute(self,**kwargs):return await self.chatcomplete(**kwargs)
            with patch.dict('sys.modules',self.native_modules(author)), patch.object(
                    map_observation,'install_response_observation',return_value=[]) as observe, patch.object(
                    adapter,'load_dependencies',return_value=lambda p,v:p.write_text(json.dumps(v))):
                foreign=dict(unit,queries=[dict(q,task='movie-q1') if q['query_id']=='filter' else q
                                           for q in unit['queries']])
                with self.assertRaisesRegex(ValueError,'sequences'):
                    native_server.install_hooks(cfg,foreign,root,Executor,author.SemFilter)
                counters=native_server.install_hooks(cfg,unit,root,Executor,author.SemFilter)
                executor=Executor()
                def transport(payload,qid,directory):
                    raw=NS(headers={'X-Kalypso-Observation':qid})
                    contexts=list(author._data_source(raw,payload['data_path'],executor))
                    planner=author.SemanticPlan(executor,virtual_pinning=True,blocking=False)
                    ops=planner.parse_ops(payload['ops']);planner.build(contexts,ops)
                    output=[]
                    async def execute():
                        for ctx in contexts:
                            value=await ops[0]._run_single(ctx)
                            if value is author.RETRY_TASK:value=await ops[0]._run_single(ctx)
                            if value is True:output.append(ctx)
                            elif value is not False and value is not None:output.append(value)
                    asyncio.run(execute());author.MapRatioEstimator.instance().reset()
                    return dict(model_name='m',num_output_rows=len(output),latency_sec=.1,results=[dict(idx=ctx.state.idx,
                        input_context=ctx.input.data,right_input_context=ctx.input.right_data,
                        outputs=[dict(op=key,value=value) for row in ctx.output for key,value in row.items()]) for ctx in output])
                driver=KalypsoRunner(cfg,unit,self.helper.data/'run',transport);driver.model_uid='owner'
                reports=[driver.query(spec,lambda event:None) for spec in unit['queries']]
                observe.assert_called_once()
                self.assertEqual(counters['reserved_calls'],len(calls))
                self.assertEqual(len(driver.request_ids),len(calls));self.assertEqual(driver.next_query,4)
                for spec,report in zip(unit['queries'],reports,strict=True):
                    self.assertEqual(report['result'],expected[spec['query_id']][0])
                    self.assertEqual(report['quality']['original_metric'],expected[spec['query_id']][1]['original_metric'])
                    self.assertEqual(report['physical_native_calls'],counters['query_calls'][spec['query_id']])
                    self.assertEqual(report['memory_sink_timing']['query_clock_version'],'kalypso.source_to_sink.v1')
                self.assertEqual(counters['query_calls']['map-first'],unit['queries'][0]['expected_calls']+1)
                self.assertTrue(all(c['max_tokens']==8 for c in calls if c['query']=='filter'))
                self.assertEqual(counters['observation_errors'],[])
        events=[json.loads(line) for line in (root/'map-trace.jsonl').read_text().splitlines()]
        resets=[e['query_id'] for e in events if e['event']=='estimator_after_native_reset']
        self.assertEqual(resets,['map-first','extract','map-repeat'])

    def test_frozen_CPU_dispatch_is_per_query_even_when_filter_is_first(self):
        cfg,unit,_=self.binding(filter_first=True)
        with patch.object(movie_map_inputs,'source_binding',return_value={'explicit_CPU_source_fixture':True}):
            frozen=movie_map_inputs.prepare_inputs(cfg,unit,self.helper.data/'frozen')
            cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
            with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],self.manager,planning=True,mapping=True) as author, \
                    patch.dict('sys.modules',self.native_modules(author)):
                class Executor:
                    def _build_chat_request(self,prompt,max_tokens,pin,priority):
                        return NS(messages=prompt,max_tokens=max_tokens,add_generation_prompt=True,add_special_tokens=False)
                output=self.helper.data/'cpu';output.mkdir()
                report=movie_map_cpu.check(cfg,output,Executor(),self.manager,author.SemanticPlan,
                    NS(_data_source=author._data_source),32768)
        self.assertEqual([p['operator'] for p in report['plans']],['filter','map','map','map'])
        self.assertEqual(report['plans'][0]['native_retry'],'none')
        self.assertEqual([p['query_id'] for p in report['plans']],[q['query_id'] for q in unit['queries']])
        self.assertEqual(report['native_retry'],'per-query original policies; see plans')
