"""Original text families and author predicate around one native Filter owner."""
from src.baselines.text.frameworks.kalypso_native.query_runner import KalypsoRunner
import ast
import asyncio
import json
import os
from pathlib import Path
import subprocess
import typing
from types import ModuleType, SimpleNamespace as NS
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import adapter, field_binding, map_observation, native_server, sembench_filter, sembench_inputs, movie_map_inputs
from src.baselines.text.sembench_prepared import result_value
from src.experiments.kalypso_query_lifecycle import query_driver_type
from tests.experiments import test_sembench_text_original as originals


@unittest.skipUnless(originals.READY,'requires the original SemBench source and driver SDK')
class NativeFilterContracts(unittest.TestCase):
    def setUp(self):
        self.helper=originals.OriginalSemBenchTextTests();self.helper.setUp();self.addCleanup(self.helper.doCleanups)
        self.helper._write_ecomm_inputs();self.helper._write_cars_inputs()

    def bound(self,scenario,number,qid='q'):
        spec=dict(task='sembench-filter',scenario=scenario,query=number,query_id=qid,
                  **(dict(scale_factor=7) if scenario=='cars' else {}))
        query=sembench_inputs.SemBenchTextQuery(os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'],self.helper.data,
            **native_server.query_task(spec),execution_id=qid,model_name='openai/local-fixture',
            generation=self.helper.generation)
        count=len(query.operation.records)
        spec.update(expected_calls=count,max_native_calls=count)
        unit=dict(call_cap=count,queries=[spec])
        cfg=dict(sembench=os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'],data_dir=str(self.helper.data),
            model='m',model_name='openai/local-fixture',native_trace=str(self.helper.data/'trace.jsonl'),
            map_prompt_root=str(self.helper.data/(qid+'-prompts')),units=[unit],execution_enabled=False,CPU_fixture_only=True,
            source_data_sha256={name:adapter.sha(self.helper.data/name) for name in query.files})
        return cfg,unit,spec

    def run_query(self,cfg,unit,spec,answers,*,corrupt=False):
        selected=[i for i,text in enumerate(answers) if 'false' not in text.strip().lower()]
        events=[]
        for i,text in enumerate(answers):
            events.extend([dict(event='filter_result',idx=i,decision=i in selected,
                verdict=text.strip().lower(),query_id=spec['query_id']),
                dict(event='native_API_end',idx=i,status='succeeded',request_id=str(i),query_id=spec['query_id'],
                     pin=False,max_tokens=8,model_uid='foreign' if corrupt else 'u')])
        response=dict(model_name='m',num_output_rows=len(selected),
                      results=[dict(idx=i) for i in reversed(selected)],latency_sec=.1)
        emitted=[]
        def transport(payload,qid,directory):
            self.assertEqual(payload['ops'][0]['op'],'sem_filter')
            return response
        def save(path,value):path.write_text(json.dumps(value))
        with patch.object(adapter,'load_dependencies',return_value=save),patch.object(adapter,'read_trace',return_value=events):
            driver=KalypsoRunner(cfg,unit,self.helper.data/(spec['query_id']+'-run'),transport)
            driver.model_uid='u'
            try:return driver.query(spec,emitted.append),emitted
            except BaseException as error:error.observed_events=emitted;raise

    def test_five_source_families_keep_full_query_membership_and_original_scoring(self):
        cases=[('movie','1'),('movie','4'),('mmqa','3a'),('mmqa','6a'),
               ('medical','1'),('medical','4'),('cars','1'),('cars','4'),('ecomm','1')]
        for index,(scenario,number) in enumerate(cases):
            with self.subTest(scenario=scenario,query=number):
                cfg,unit,spec=self.bound(scenario,number,str(index))
                self.assertIs(query_driver_type(unit),KalypsoRunner)
                self.assertEqual(native_server.physical_call_caps(unit),{str(index):spec['expected_calls']})
                query=sembench_inputs.make_query(cfg,spec['query_id'],spec,kind='filter')
                answers=[('true','false and true','unknown verdict')[i%3] for i in range(spec['expected_calls'])]
                # Native predicate owns the decisions. The official LOTUS
                # reference receives only canonical decisions for relation parity.
                canonical=['False' if 'false' in text else 'True' for text in answers]
                expected,_=self.helper._reference(query,canonical)
                quality=query.evaluate(expected)
                with patch.object(movie_map_inputs,'binding',return_value={'explicit_CPU_fixture':True}):
                    frozen=movie_map_inputs.prepare_inputs(cfg,unit,self.helper.data/(str(index)+'-inputs'))
                    cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
                    _,loaded=movie_map_inputs.load_inputs(cfg)
                self.assertEqual(len(loaded[0]['table']['rows']),len(answers))
                report,events=self.run_query(cfg,unit,spec,answers)
                self.assertEqual(report['result'],result_value(expected))
                self.assertEqual(report['quality']['original_metric'],quality['original_metric'])
                self.assertEqual(report['physical_native_calls'],len(answers))
                self.assertEqual(report['physical_call_cap'],len(answers))
                self.assertEqual(report['memory_sink_timing']['query_clock_version'],'kalypso.source_to_sink.v1')
                self.assertEqual(events,['memory_sink_ready','result_ready'])
                self.assertIn('not LOTUS parser',report['prompt_parser_identity'])

    def test_all_negative_native_response_keeps_the_original_empty_relation(self):
        cfg,unit,spec=self.bound('mmqa','3a')
        query=sembench_inputs.make_query(cfg,'q',spec,kind='filter')
        answers=['false']*spec['expected_calls'];expected,_=self.helper._reference(query,['False']*len(answers))
        report,_=self.run_query(cfg,unit,spec,answers)
        self.assertEqual(report['result'],result_value(expected))
        self.assertEqual(report['quality']['native_nonliteral_verdicts'],0)

    def test_empty_original_frontier_completes_with_no_model_calls(self):
        import pandas as pd
        path=self.helper.data/'lizzy_caplan_text_data.csv'
        pd.read_csv(path).iloc[:0].to_csv(path,index=False)
        cfg,unit,spec=self.bound('mmqa','3a')
        unit['call_cap']=0
        self.assertEqual(native_server.physical_call_caps(unit),{'q':0})
        query=sembench_inputs.make_query(cfg,'q',spec,kind='filter')
        expected,_=self.helper._reference(query,[])
        report,_=self.run_query(cfg,unit,spec,[])
        self.assertEqual(report['result'],result_value(expected))
        self.assertEqual(report['physical_native_calls'],0)
        self.assertEqual(report['physical_call_cap'],0)
        self.assertEqual(report['native_API_units'],[])
        if os.environ.get('SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'):
            from tests.baselines.text.kalypso_join_author import empty_cpu_plan
            with patch.object(movie_map_inputs,'binding',return_value={'explicit_CPU_fixture':True}):
                frozen=movie_map_inputs.prepare_inputs(cfg,unit,self.helper.data/'zero-cpu-inputs')
                cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
                cpu=empty_cpu_plan(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],cfg,
                    self.helper.data/'zero-cpu',kind='filter')
            self.assertIsNone(cpu['plans'][0]['max_prompt_tokens'])

    def test_wrong_model_audit_retains_sink_and_does_not_publish_a_success(self):
        cfg,unit,spec=self.bound('medical','1')
        with self.assertRaisesRegex(ValueError,'Owner') as caught:
            self.run_query(cfg,unit,spec,['true']*spec['expected_calls'],corrupt=True)
        self.assertEqual(caught.exception.observed_events,['memory_sink_ready'])
        report=caught.exception.query_report
        self.assertEqual(report['status'],'failed')
        self.assertIsNotNone(report['memory_sink_timing']['memory_sink_ready_ns'])
        self.assertEqual(report['memory_sink_timing']['trace_audit_status'],'failed')

    def test_nonfilter_original_operator_cannot_be_mislabeled(self):
        cfg,unit,spec=self.bound('ecomm','1')
        for number in ('3','5','7'):
            with self.subTest(query=number),self.assertRaisesRegex(ValueError,'ordinary text Filter'):
                sembench_inputs.make_query(cfg,'q',dict(spec,query=number),kind='filter')

    def test_native_filter_budget_stops_before_an_extra_call_without_map_retry_allowance(self):
        cfg,unit,spec=self.bound('ecomm','1')
        count=spec['expected_calls']
        changed=dict(unit,queries=[dict(spec,max_native_calls=2*count)])
        with self.assertRaisesRegex(ValueError,'Only Map'):native_server.physical_call_caps(changed)
        class Executor:
            model='m';calls=0
            async def chatcomplete(self,*args,**kwargs):
                self.calls+=1;return NS(request_id=str(self.calls),finish_reason='stop')
        class Filter:
            def _build_prompts(self,ctx):return [],[]
            async def _run_single(self,*a,**kw):raise AssertionError('budget test ran a Filter task')
        helpers=ModuleType('vllm.kalypso.sem_ops.prompt_utils')
        helpers.get_prompt=helpers.get_data_prompt=lambda *a,**kw:[]
        base=ModuleType('vllm.kalypso.sem_ops.base');base.OpName=NS(SEM_FILTER='sem_filter')
        directory=self.helper.data/'budget';directory.mkdir()
        (directory/'loaded-model.json').write_text(json.dumps(dict(model_uid='u')))
        with patch.dict('sys.modules',{helpers.__name__:helpers,base.__name__:base}), \
                patch.object(map_observation,'install_response_observation',return_value=[]):
            counter=native_server.install_hooks(cfg,unit,directory,Executor,Filter)
        executor=Executor();raw=NS(headers={'X-Kalypso-Observation':'q'})
        async def calls():
            for _ in range(count):await executor.chatcomplete(raw,[],8)
            with self.assertRaisesRegex(RuntimeError,'budget'):await executor.chatcomplete(raw,[],8)
        asyncio.run(calls())
        self.assertEqual(executor.calls,count)
        self.assertEqual(counter['reserved_calls'],count)

    @unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'),'requires author05024 Git objects')
    def test_author_filter_body_and_predicate_keep_native_messages_and_nonliteral_verdicts(self):
        checkout=os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT']
        def source(name):
            return subprocess.check_output(['git','-C',checkout,'show',
                native_server.AUTHOR_SOURCE_COMMIT+':vllm/kalypso/sem_ops/'+name],text=True)
        scope={};exec(compile(source('base.py'),'author05024/base.py','exec'),scope)
        tree=ast.parse(source('prompt_utils.py'));tree.body=[node for node in tree.body if not isinstance(node,(ast.Import,ast.ImportFrom))]
        exec(compile(tree,'author05024/prompt_utils.py','exec'),scope)
        tree=ast.parse(source('filters.py'));tree.body=[node for node in tree.body if isinstance(node,ast.ClassDef) and node.name=='SemFilter']
        scope.update(SemContext=object,List=typing.List)
        exec(compile(tree,'author05024/filters.py','exec'),scope)
        Filter=scope['SemFilter'];original_run=Filter._run_single
        cfg,unit,spec=self.bound('ecomm','1')
        query=sembench_inputs.make_query(cfg,'q',spec,kind='filter')
        rows,_=sembench_inputs.prepare(cfg,spec,self.helper.data/'author-inputs',query.operation,kind='filter')
        helpers=ModuleType('vllm.kalypso.sem_ops.prompt_utils');helpers.__dict__.update(scope)
        with patch.dict('sys.modules',{helpers.__name__:helpers}):
            field_binding.install_prompt(cfg,unit,Filter,'_build_prompts',scope['OpName'].SEM_FILTER)
        self.assertIs(Filter._run_single,original_run)
        operator=Filter(query.operation.stage.method.instruction)
        self.assertEqual(operator.max_tokens,8)
        observed=[]
        async def run():
            for i,(row,text) in enumerate(zip(rows,['true','false and true','unparseable']*2,strict=True)):
                async def execute(**kwargs):observed.append(kwargs);return NS(text=text)
                ctx=NS(input=NS(data=scope['get_data_prompt'](row['data'].strip()),right_data=[]),output=[],
                    state=NS(idx=i,raw_request=NS(headers={'X-Kalypso-Observation':'q'}),pin_req_id=None,
                             executor=NS(execute=execute)))
                expected=scope['get_prompt'](operator.instruction.format_map(row['fields']),ctx.input.data,scope['OpName'].SEM_FILTER)
                passed=await operator._run_single(ctx)
                self.assertEqual(passed,'false' not in text)
                self.assertEqual(observed[-1]['prompt'],expected)
                self.assertEqual(observed[-1]['max_tokens'],8)
        asyncio.run(run())
        self.assertEqual(len(observed),len(rows))


if __name__=='__main__':unittest.main()
