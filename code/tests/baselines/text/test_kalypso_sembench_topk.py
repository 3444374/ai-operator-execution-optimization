"""Native insertion/merge ranking with duplicate rows and original full-query sink."""
import asyncio
import copy
import json
import os
from pathlib import Path
from types import ModuleType,SimpleNamespace as NS
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import adapter,native_server,sembench_inputs,sembench_topk,topk_binding,movie_map_inputs,topk_cpu,map_observation
from src.baselines.text.sembench_prepared import result_value
from src.experiments.kalypso_query_lifecycle import query_driver_type
from tests.baselines.text.kalypso_join_author import methods
from tests.experiments import test_sembench_text_original as originals
pd=originals.pd if originals.READY else None

READY=originals.READY and os.environ.get('SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT')


@unittest.skipUnless(READY,'requires pinned original SemBench and author05024 objects')
class NativeTopKContracts(unittest.TestCase):
    def setUp(self):
        self.helper=originals.OriginalSemBenchTextTests();self.helper.setUp();self.addCleanup(self.helper.doCleanups)
        self.manager=NS(token_length=lambda text:len(str(text)))
        self.original_source=pd.read_csv(self.helper.data/'Reviews.csv')

    def bound(self,n,qid='q'):
        path=self.helper.data/'Reviews.csv';original=self.original_source
        rows=original.iloc[[0]*n].copy();rows['id']='ant_man_and_the_wasp_quantumania'
        rows['reviewId']=list(range(n));rows['reviewText']='identical review text';rows.to_csv(path,index=False)
        cap=n*(n-1)//2
        spec=dict(task='sembench-topk',scenario='movie',query='9',query_id=qid,
            expected_rows=n,expected_calls=cap,max_native_calls=cap)
        unit=dict(call_cap=max(1,cap),queries=[spec])
        cfg=dict(sembench=os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'],data_dir=str(self.helper.data),
            model='m',model_name='openai/local-fixture',native_trace=str(self.helper.data/(qid+'-trace/native-trace.jsonl')),
            map_prompt_root=str(self.helper.data/(qid+'-prompts')),units=[unit],execution_enabled=False,CPU_fixture_only=True,
            source_data_sha256={'Reviews.csv':adapter.sha(path)})
        return cfg,unit,spec

    @staticmethod
    def modules(author):
        prompt=ModuleType('vllm.kalypso.sem_ops.prompt_utils')
        prompt.__dict__.update({k:v for k,v in vars(author).items() if not k.startswith('__')})
        reducers=ModuleType('vllm.kalypso.sem_ops.reducers');reducers.SemTopK=author.SemTopK
        return {m.__name__:m for m in (prompt,reducers)}

    def run_query(self,cfg,unit,spec,*,bad_response=False,fail=False):
        trace=Path(cfg['native_trace']).parent;trace.mkdir()
        for name in ('native-trace.jsonl','topk-trace.jsonl'):(trace/name).touch()
        (trace/'loaded-model.json').write_text(json.dumps(dict(model_uid='u')))
        seen=[];emitted=[];self_test=self
        def transport(payload,qid,directory):
            with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],self.manager,planning=True,ranking=True) as author:
                class Executor:
                    model='m'
                    async def chatcomplete(self,raw_request,prompt,max_tokens,pin=False,priority=0):
                        pair=native_server.ROW.get();seen.append(dict(pair=pair,prompt=copy.deepcopy(prompt),cap=max_tokens))
                        if fail:raise RuntimeError('fixture native comparison failure')
                        text='A' if pair[0]<pair[1] else 'B'
                        return NS(text=text,request_id=qid+'-'+str(len(seen)),finish_reason='stop')
                    async def execute(self,**kwargs):return await self.chatcomplete(**kwargs)
                with patch.dict('sys.modules',self.modules(author)),patch.object(map_observation,'install_response_observation',return_value=[]):
                    counter=native_server.install_hooks(cfg,unit,trace,Executor,author.SemFilter)
                executor=Executor();raw=NS(headers={'X-Kalypso-Observation':qid})
                contexts=list(author._data_source(raw,payload['data_path'],executor))
                planner=author.SemanticPlan(executor,virtual_pinning=True,blocking=False)
                ops=planner.parse_ops(payload['ops']);plan=planner.build(contexts,ops)
                self_test.assertEqual(plan,list(ops));self_test.assertEqual(ops[0].k,len(contexts))
                self_test.assertEqual(ops[0].concurrency,20)
                output=asyncio.run(ops[0](contexts))
                self_test.assertEqual(counter['reserved_calls'],len(seen))
                if spec['expected_calls']==0:
                    with self_test.assertRaisesRegex(RuntimeError,'budget'):
                        asyncio.run(executor.chatcomplete(raw,[],5))
                indices=[ctx.state.idx for ctx in output]
                if bad_response:indices=indices[:-1]
                return dict(model_name='m',num_output_rows=len(indices),latency_sec=.1,
                    results=[dict(idx=i) for i in indices])
        with patch.object(adapter,'load_dependencies',return_value=lambda p,v:p.write_text(json.dumps(v))):
            driver=sembench_topk.TopKDriver(cfg,unit,self.helper.data/(spec['query_id']+'-run'),transport)
            driver.model_uid='u';report=driver.query(spec,emitted.append)
        return report,seen,emitted

    def test_full_original_sink_keeps_native_insertion_merge_and_duplicate_occurrences(self):
        for n in (0,1,2,7,8,13):
            with self.subTest(n=n):
                cfg,unit,spec=self.bound(n,'n'+str(n))
                self.assertIs(query_driver_type(unit),sembench_topk.TopKDriver)
                query=sembench_inputs.make_query(cfg,spec['query_id'],spec,kind='topk')
                # The original query owns rank-to-score and evaluator. Its accessor
                # receives the actual native order; no LOTUS sorting is executed.
                expected,_=query.execute(lambda op,invoke:dict(frame=op.frame.copy(),native_stats={}))
                report,seen,events=self.run_query(cfg,unit,spec)
                self.assertEqual(report['result'],result_value(expected))
                from src.infrastructure.private_artifacts import json_metric
                self.assertEqual(json_metric(report['quality']),json_metric(query.evaluate(expected)))
                self.assertEqual(report['ranked_source_positions'],list(range(n)))
                self.assertEqual(report['physical_native_calls'],len(seen))
                self.assertLessEqual(len(seen),n*(n-1)//2)
                self.assertEqual(len({frozenset(row['pair']) for row in seen}),len(seen))
                self.assertTrue(all(row['cap']==5 for row in seen))
                self.assertEqual(events,['memory_sink_ready','result_ready'])
                if n==1:
                    self.assertEqual(seen,[]);self.assertEqual(report['result']['data'][0][-1],3.0)
                if n>=8:self.assertLess(len(seen),spec['expected_calls'])

    @unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_TOKENIZER'),'requires existing local tokenizer')
    def test_bound_frozen_inputs_and_all_pair_token_gate_use_original_blocking_plan(self):
        from transformers import AutoTokenizer
        cfg,unit,spec=self.bound(8)
        tokenizer=AutoTokenizer.from_pretrained(os.environ['SEMLOOM_NATIVE_TEST_TOKENIZER'],local_files_only=True,trust_remote_code=False)
        class Manager:
            bytes_per_token=1
            def reset_stages(self):pass
            def token_length(self,text):return len(str(text))
        manager=Manager();manager.tokenizer=tokenizer
        with patch.object(movie_map_inputs,'binding',return_value=dict(explicit_CPU_fixture=True)):
            frozen=movie_map_inputs.prepare_inputs(cfg,unit,self.helper.data/'cpu-inputs')
            cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
            _,loaded=movie_map_inputs.load_inputs(cfg)
            self.assertEqual(len(loaded[0]['table']['rows']),8)
            with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],manager,planning=True,ranking=True) as author:
                class Executor:
                    def _build_chat_request(self,prompt,cap,pin,priority):
                        return NS(messages=prompt,max_tokens=cap,add_generation_prompt=True,add_special_tokens=False)
                output=self.helper.data/'native-cpu';output.mkdir()
                with patch.dict('sys.modules',self.modules(author)):
                    report=topk_cpu.check(cfg,output,Executor(),manager,author.SemanticPlan,
                        NS(_data_source=author._data_source),10000)
                self.assertEqual(report['plans'][0]['previewed_orientations'],56)
                self.assertEqual(report['plans'][0]['physical_call_cap'],28)
                self.assertEqual(report['model_calls'],0)
                previews=[json.loads(line) for line in (output/'q-native-comparisons.jsonl').read_text().splitlines()]
                self.assertTrue(all(row['token_ids'] and row['max_tokens']==5 for row in previews))
                short=self.helper.data/'short-context';short.mkdir()
                with patch.dict('sys.modules',self.modules(author)),self.assertRaisesRegex(ValueError,'exceed model context'):
                    topk_cpu.check(cfg,short,Executor(),manager,author.SemanticPlan,
                        NS(_data_source=author._data_source),previews[0]['prompt_tokens']+4)

    def test_incomplete_full_ranking_and_model_failure_have_no_success_sink(self):
        cfg,unit,spec=self.bound(2,'missing')
        with self.assertRaisesRegex(ValueError,'lost source occurrences'):
            self.run_query(cfg,unit,spec,bad_response=True)
        cfg,unit,spec=self.bound(2,'failed')
        with self.assertRaisesRegex(RuntimeError,'native comparison failure') as caught:
            self.run_query(cfg,unit,spec,fail=True)
        self.assertIsNone(caught.exception.query_report['memory_sink_timing']['memory_sink_ready_ns'])

    def test_empty_and_single_source_freeze_without_inventing_a_model_comparison(self):
        for n in (0,1):
            cfg,unit,spec=self.bound(n,'n'+str(n))
            with patch.object(movie_map_inputs,'binding',return_value=dict(explicit_CPU_fixture=True)):
                frozen=movie_map_inputs.prepare_inputs(cfg,unit,self.helper.data/('cpu'+str(n)))
                cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
                manifest,loaded=movie_map_inputs.load_inputs(cfg)
                self.assertEqual(len(loaded[0]['table']['rows']),n)
                self.assertEqual(manifest['model_calls'],0)
                self.assertEqual(native_server.physical_call_caps(unit),{spec['query_id']:0})

    def test_budget_is_comparison_upper_bound_and_audit_rejects_unknown_or_changed_pairs(self):
        cfg,unit,spec=self.bound(7)
        wrong=dict(unit,queries=[dict(spec,max_native_calls=7)])
        with self.assertRaisesRegex(ValueError,'comparison upper bound'):native_server.physical_call_caps(wrong)
        self.assertEqual(native_server.physical_call_caps(unit),{'q':21})
        report,_,_=self.run_query(cfg,unit,spec)
        events=adapter.read_trace(cfg['native_trace'],'q');comparisons=adapter.read_trace(
            Path(cfg['native_trace']).with_name('topk-trace.jsonl'),'q')
        for changed in (events[:-1],copy.deepcopy(events)):
            if len(changed)==len(events):changed[-1]['model_uid']='another-owner'
            with self.assertRaises(ValueError):
                topk_binding.audit(report['ranked_source_positions'],changed,comparisons,
                    rows=7,cap=21,model_uid='u',previous_ids=set())


if __name__=='__main__':unittest.main()
