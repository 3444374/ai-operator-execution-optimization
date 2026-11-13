"""Pinned SemAgg/BlockingExecutor and original Q5; explicit software API/KV only."""
import asyncio
import copy
import json
import os
from pathlib import Path
from types import ModuleType, SimpleNamespace as NS
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import (
    adapter, aggregate_binding, aggregate_cpu, map_observation, movie_map_inputs,
    native_server, sembench_inputs,
)
from src.baselines.text.frameworks.kalypso_native.query_runner import KalypsoRunner
from src.baselines.text.sembench_prepared import result_value
from src.experiments.kalypso_query_lifecycle import query_driver_type
from src.infrastructure.private_artifacts import json_metric
from tests.baselines.text.kalypso_join_author import methods
from tests.experiments import test_sembench_lotus_mmqa_aggregate as fixture

READY = fixture.CHECKOUT and fixture.HAS_SDK and os.environ.get('SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT')


class SoftwareManager:
    """Character costs exercise native chunking; not target tokenizer qualification."""
    bytes_per_token = 1
    def __init__(self):
        self.tokenizer = self
        self.allocations, self.releases = [], []
    def token_length(self, value):
        return len(value if isinstance(value, str) else json.dumps(value))
    def apply_chat_template(self, value, **kwargs): return json.dumps(value)
    def encode(self, value, **kwargs): return list(range(len(value)))
    def reset_stages(self): pass
    async def can_admit(self, budget): return True
    async def allocate(self, budget): self.allocations.append(budget)
    async def release(self, budget): self.releases.append(budget)


@unittest.skipUnless(READY, 'requires pinned original SemBench/LOTUS and author05024 objects')
class NativeAggregateContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls): fixture.NativeMMQAAggregateTests.setUpClass()
    def setUp(self):
        self.source = fixture.NativeMMQAAggregateTests()
        self.source.setUp()
        self.addCleanup(self.source.doCleanups)
        self.manager = SoftwareManager()

    def bound(self, count, qid='q'):
        data = self.source.source(selected=count, words=200)
        spec = dict(task='sembench-aggregate', scenario='mmqa', query='Q5', query_id=qid,
            expected_rows=count, expected_calls=None, max_native_calls=max(0, count-1))
        unit = dict(call_cap=max(0, count-1), queries=[spec])
        cfg = dict(sembench=str(self.source.checkout), data_dir=str(data), model='m',
            model_name='openai/local-fixture', native_trace=str(data/'trace/native-trace.jsonl'),
            map_prompt_root=str(data/'prompts'), units=[unit], execution_enabled=False, CPU_fixture_only=True,
            source_data_sha256={'lizzy_caplan_text_data.csv':adapter.sha(data/'lizzy_caplan_text_data.csv')})
        return cfg, unit, spec

    @staticmethod
    def modules(author):
        prompt = ModuleType('vllm.kalypso.sem_ops.prompt_utils')
        prompt.__dict__.update({k:v for k,v in vars(author).items() if not k.startswith('__')})
        reducers = ModuleType('vllm.kalypso.sem_ops.reducers')
        reducers.SemAgg = author.SemAgg
        author.SemAgg.__module__ = reducers.__name__
        return {m.__name__:m for m in (prompt, reducers)}

    def transport(self, cfg, unit, seen, *, observe=True, long=False, fail=False, bad_response=False, bad_owner=False):
        def execute(payload, qid, directory):
            with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'], self.manager,
                    planning=True, aggregation=True) as author:
                class Executor:
                    model = 'm'
                    async def chatcomplete(self, raw_request, prompt, max_tokens, pin=False, priority=0):
                        seen.append(dict(prompt=copy.deepcopy(prompt), max_tokens=max_tokens,
                                         indices=copy.deepcopy(native_server.ROW.get())))
                        if fail: raise RuntimeError('software native aggregate API failed')
                        return NS(text='Lizzy Caplan'+(' detail'*430 if long else ''),
                            request_id=qid+'-'+str(len(seen)), finish_reason='stop')
                    async def execute(self, **kwargs): return await self.chatcomplete(**kwargs)
                with patch.dict('sys.modules', self.modules(author)), patch.object(
                        map_observation, 'install_response_observation', return_value=[]):
                    if observe:
                        counter = native_server.install_hooks(cfg, unit, Path(cfg['native_trace']).parent,
                                                            Executor, author.SemFilter)
                    executor = Executor()
                    raw = NS(headers={'X-Kalypso-Observation':qid})
                    contexts = list(author._data_source(raw, payload['data_path'], executor))
                    planner = author.SemanticPlan(executor, virtual_pinning=True, blocking=False)
                    ops = planner.parse_ops(payload['ops'])
                    plan = planner.build(contexts, ops)
                    self.assertEqual(plan, list(ops))
                    self.assertEqual((ops[0].max_tokens, ops[0].concurrency), (8192,8))
                    output = asyncio.run(asyncio.wait_for(ops[0](contexts), 3))
                    if observe:self.assertEqual(counter['reserved_calls'],len(seen))
                response = dict(model_name='m', num_output_rows=len(output), latency_sec=.1,
                    results=[dict(idx=ctx.state.idx, input_context=ctx.input.data,
                        right_input_context=ctx.input.right_data,
                        outputs=[dict(op=key,value=value) for row in ctx.output for key,value in row.items()])
                        for ctx in output])
                if bad_response: response['results'][0]['idx'] = 999
                if bad_owner:
                    path = Path(cfg['native_trace'])
                    events = [json.loads(line) for line in path.read_text().splitlines()]
                    events[-1]['model_uid'] = 'different-owner'
                    path.write_text(''.join(json.dumps(e)+'\n' for e in events))
                return response
        return execute

    def run_query(self, cfg, unit, spec, **kwargs):
        trace = Path(cfg['native_trace']).parent;trace.mkdir()
        (trace/'loaded-model.json').write_text(json.dumps(dict(model_uid='u')))
        seen, emitted = [], []
        output = Path(cfg['data_dir'])/'run'
        self.last = NS(output=output, seen=seen, emitted=emitted)
        with patch.object(adapter, 'load_dependencies', return_value=lambda path,value:path.write_text(json.dumps(value))):
            driver = KalypsoRunner(cfg, unit, output, self.transport(cfg,unit,seen,**kwargs))
            driver.model_uid='u'
            return driver.query(spec, emitted.append), seen

    def test_original_q5_full_sink_and_evaluator_match_direct_native_reduction(self):
        for count,long in ((0,False),(1,False),(2,False),(16,False),(16,True)):
            with self.subTest(rows=count,long=long):
                cfg, unit, spec = self.bound(count)
                query = sembench_inputs.make_query(cfg,'q',spec,kind='aggregate')
                self.assertIs(query_driver_type(unit), KalypsoRunner)
                self.assertIsNone(query.describe()['expected_model_calls'])
                self.assertCountEqual(query.operation.columns,('title','text'))
                reference = dict(cfg,map_prompt_root=str(Path(cfg['data_dir'])/'direct-prompts'))
                direct_calls=[]
                direct_transport=self.transport(reference,unit,direct_calls,observe=False,long=long)
                def direct(op, invoke):
                    rows,path=sembench_inputs.prepare(reference,spec,Path(cfg['data_dir'])/'direct',op,kind='aggregate')
                    payload=dict(data_path=str(path),ops=[dict(op='sem_agg',args=dict(
                        instruction=op.instruction.format_map({name:name for name in op.columns})))],blocking=False)
                    response=direct_transport(payload,'q',None)
                    text=aggregate_binding.response_text(response,'m',rows)
                    import pandas as pd
                    return dict(frame=pd.DataFrame({'_output':[] if not rows else [text]}))
                expected,_=query.execute(direct)
                report,seen=self.run_query(cfg,unit,spec,long=long)
                self.assertEqual(report['result'],result_value(expected))
                self.assertEqual(json_metric(report['quality']),json_metric(query.evaluate(expected)))
                self.assertEqual([s['prompt'] for s in seen],[s['prompt'] for s in direct_calls])
                self.assertTrue(all(s['max_tokens']==8192 for s in seen))
                self.assertEqual(report['physical_native_calls'],len(seen))
                self.assertLessEqual(len(seen),max(0,count-1))
                self.assertEqual(self.last.emitted,['memory_sink_ready','result_ready'])
                timing=report['memory_sink_timing']
                self.assertEqual(timing['query_clock_version'],'kalypso.source_to_sink.v1')
                self.assertLessEqual(timing['memory_sink_ready_ns'],timing['phases']['trace_audit']['started_ns'])
                if count<=1:self.assertEqual(seen,[])
                if count>1:self.assertEqual(report['native_API_units'][-1]['generated_text'],report['result']['data'][0][0])
                if count>2:
                    self.assertTrue(any(-1 in s['indices'] for s in seen))
                    self.assertTrue(all('EXCLUDED_SOURCE_MARKER' not in json.dumps(s['prompt']) for s in seen))
                    self.assertIn('title',json.dumps(seen[0]['prompt']))
                self.assertEqual(len(self.manager.allocations),len(self.manager.releases))

    def test_actual_summary_size_changes_original_number_of_reductions(self):
        totals=[]
        for long in (False,True):
            cfg,unit,spec=self.bound(16)
            report,seen=self.run_query(cfg,unit,spec,long=long)
            totals.append(len(seen))
            self.assertIsNone(report['expected_model_calls'])
        self.assertGreater(totals[1],totals[0])

    def test_frozen_inputs_and_native_initial_plan_do_not_invent_future_prompts(self):
        for count in (0,1,16):
            cfg,unit,spec=self.bound(count)
            with patch.object(movie_map_inputs,'binding',return_value=dict(explicit_software_CPU=True)):
                frozen=movie_map_inputs.prepare_inputs(cfg,unit,Path(cfg['data_dir'])/'cpu-inputs')
                cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
                _,loaded=movie_map_inputs.load_inputs(cfg)
                self.assertEqual(len(loaded[0]['table']['rows']),count)
                output=Path(cfg['data_dir'])/'cpu-result';output.mkdir()
                with methods(os.environ['SEMLOOM_NATIVE_TEST_KALYPSO_CHECKOUT'],self.manager,
                             planning=True,aggregation=True) as author:
                    class Executor:
                        def _build_chat_request(self,prompt,cap,pin,priority):
                            return NS(messages=prompt,max_tokens=cap,add_generation_prompt=True,add_special_tokens=False)
                    with patch.dict('sys.modules',self.modules(author)):
                        report=aggregate_cpu.check(cfg,output,Executor(),self.manager,author.SemanticPlan,
                                                  NS(_data_source=author._data_source),1000000)
                self.assertEqual(report['model_calls'],0)
                self.assertFalse(report['complete_generated_prompt_qualification'])
                self.assertIsNone(report['plans'][0]['expected_model_calls'])
                self.assertEqual(report['plans'][0]['physical_call_cap'],max(0,count-1))
                if count<=1:
                    self.assertEqual(report['plans'][0]['initial_reductions'],0)
                    self.assertIsNone(report['plans'][0]['max_prompt_tokens'])
                table=loaded[0]['paths']['prompt'];table.write_text(table.read_text()+' ')
                with self.assertRaisesRegex(ValueError,'file changed'):movie_map_inputs.load_inputs(cfg)

    def test_model_failure_and_finite_owner_budget_never_publish_a_complete_sink(self):
        for failure in ('api','budget','response'):
            cfg,unit,spec=self.bound(2)
            if failure=='budget':unit['call_cap']=0
            with self.assertRaises(Exception) as caught:
                self.run_query(cfg,unit,spec,fail=failure=='api',bad_response=failure=='response')
            self.assertIsNone(caught.exception.query_report['memory_sink_timing']['memory_sink_ready_ns'])
            self.assertNotIn('quality',caught.exception.query_report)
            self.assertEqual(self.last.emitted,[])
        cfg,unit,spec=self.bound(2)
        with self.assertRaisesRegex(ValueError,'N-1'):
            native_server.physical_call_caps(dict(unit,queries=[dict(spec,expected_calls=1)]))

    def test_rejected_reducer_cannot_qualify_the_authors_partial_return(self):
        cfg,unit,spec=self.bound(15)
        unit['call_cap']=1
        with self.assertRaisesRegex(ValueError,'rejected') as caught:self.run_query(cfg,unit,spec)
        timing=caught.exception.query_report['memory_sink_timing']
        self.assertIsInstance(timing['memory_sink_ready_ns'],int)
        self.assertEqual(timing['trace_audit_status'],'failed')
        self.assertTrue((self.last.output/'q/result.json').exists())
        self.assertEqual(len(self.last.seen),1)
        self.assertNotIn('quality',caught.exception.query_report)

    def test_post_sink_owner_audit_keeps_result_and_exact_context_decoder(self):
        cfg,unit,spec=self.bound(2)
        with self.assertRaisesRegex(ValueError,'owner') as caught:self.run_query(cfg,unit,spec,bad_owner=True)
        timing=caught.exception.query_report['memory_sink_timing']
        self.assertIsInstance(timing['memory_sink_ready_ns'],int)
        self.assertEqual(timing['trace_audit_status'],'failed')
        self.assertTrue((self.last.output/'q/result.json').exists())
        self.assertNotIn('quality',caught.exception.query_report)
        text='literal } and "quotes"\n'+aggregate_binding.SUFFIX
        messages=[dict(role='user',type='text',content=aggregate_binding.PREFIX+text+aggregate_binding.SUFFIX)]
        self.assertEqual(aggregate_binding.context_text(messages),text)
        messages[0]['content']=messages[0]['content'].rstrip()
        with self.assertRaisesRegex(ValueError,'wrapper'):aggregate_binding.context_text(messages)


if __name__=='__main__':unittest.main()
