"""Original LOTUS queries on one software Serve owner, without Ray/model/INET."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import pandas as pd

from src.experiments import native_ray_sembench as runner, sembench_text_query as entry
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from tests.experiments.test_sembench_native_ray import SoftwareServeSession
from tests.experiments import test_sembench_lotus_image_query as visual


@unittest.skipUnless(visual.CHECKOUT and visual.HAS_SDK, 'requires fixed original queries and LOTUS SDK')
class LotusRayServeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        visual.NativeLotusImageQueryTests.setUpClass()

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.data = self.root/'data'
        self.data.mkdir()
        pd.DataFrame(dict(id=['taken_3']*3+['outside'], reviewId=['dup','dup','c','d'],
            reviewText=['good first','bad','good second','outside'],
            scoreSentiment=['POSITIVE','NEGATIVE','POSITIVE','POSITIVE'],
            originalScore=['5/5','1/5','4/5','5/5'])).to_csv(self.data/'Reviews.csv',index=False)
        self.owners, self.samples = [], []
        self.reply = lambda messages, sampling: 'True' if 'good' in messages[-1]['content'] else 'False'

    def config(self, *, query='Q3', limit=6, max_calls=3, scenario='movie', **options):
        ledger = CellBudgetLedger.create(self.root/'budget.sqlite',AttemptBudget('lotus-ready',limit),deadline_utc=time.time()+120)
        ledger.reserve_unit('session',limit)
        config = dict(schema='semloom.sembench_text_run.v1', scenario=scenario, query=query,
            query_id='lotus-ready', backend='ray-data', model_name='openai/software-model',
            lifecycle='serve-session', generation=dict(max_tokens=8192,stop=[]),
            engine=dict(model_path='CPU-no-model',model_python=sys.executable,driver_python=sys.executable,
                max_model_calls=max_calls), resources=dict(startup_timeout_s=10), profile={},
            ray_native=dict(engine_kwargs={},processor_options={},serve=dict(model='software-model',
                app_name='owned-fixture',deployment_name='model-fixture',startup_timeout_s=10,
                session_timeout_s=45,cleanup_s=3)),
            budget=dict(path=str(ledger.path),id='lotus-ready',limit=limit,unit_id='session'),**options)
        return config, ledger

    def owner(self, **options):
        case = self
        class Owner(SoftwareServeSession):
            def answer(self, prompt, sampling):
                value = deepcopy(vars(sampling))
                case.samples.append(value)
                return case.reply(prompt, value)
        owner = Owner(**options)
        self.owners.append(owner)
        return owner

    @staticmethod
    def identity():
        return dict(observed_monotonic_ns=time.monotonic_ns(),node_id='software-node',
            model_actor_id='software-actor',model_actor_pid=123,model_actor_create_time=1.0,
            model_processes=[dict(pid=456,create_time=2.0)])

    def checked(self, config, data=None):
        path = self.root/'config.json'
        path.write_text(json.dumps(config))
        cfg, engine, generation, resources = entry.configuration(path)
        query = entry.query_from_config(cfg,visual.CHECKOUT,data or self.data,generation)
        return cfg, query, engine, resources

    def run_session(self, config, *, data=None, identity=None):
        cfg, query, engine, resources = self.checked(config,data)
        return runner.run_serve_session(cfg,query,engine,resources,self.root/'session',
            session_factory=self.owner,identity_snapshot=identity or self.identity)

    def test_two_fresh_original_queries_share_model_and_unit_with_unique_occurrence_ids(self):
        config, ledger = self.config()
        report = self.run_session(config)
        self.assertEqual(report['status'],'completed')
        self.assertEqual(len(self.owners),1)
        self.assertEqual(self.owners[0].events,['start','predict','predict','close'])
        self.assertTrue(self.owners[0].closed_unit)
        self.assertTrue(report['model_owner_reused'])
        self.assertTrue(report['warm_qualified'])
        self.assertIsNone(report['target_native_model_qualified'])
        self.assertEqual(report['physical_attempts_reserved'],6)
        self.assertEqual([r['phase'] for r in report['queries']],['ready-first','same-owner-repeat'])
        self.assertEqual([r['physical_attempts_reserved'] for r in report['queries']],[3,3])
        self.assertEqual([r['result']['data'] for r in report['queries']],[[[2]],[[2]]])
        self.assertEqual([len(r['native']['batches']) for r in report['queries']],[1,1])
        ids = [[r['call_id'] for r in batch] for batch in self.owners[0].inputs]
        self.assertFalse(set(ids[0]) & set(ids[1]))
        self.assertEqual([r['messages'] for r in self.owners[0].inputs[0]],
                         [r['messages'] for r in self.owners[0].inputs[1]])
        self.assertTrue(all('structured_outputs' not in s and 'response_format' not in s for s in self.samples))
        for index, row in enumerate(report['queries'],1):
            self.assertTrue(row['model_ready_before_source'])
            self.assertEqual(row['query_clock_version'],'lotus_ray_serve.source_to_sink.v1')
            self.assertLessEqual(row['sink_ready_ns'],row['semantic_audit_started_ns'])
            self.assertEqual(json.loads((self.root/f'session/round-{index}/query.json').read_text())['status'],'completed')
        self.assertEqual(self.owners[0].unit.attempts,6)
        self.assertEqual(ledger.snapshot()['units'][0]['claimed'],1)

    def test_original_quick_algorithm_keeps_multiple_batches_and_per_query_engine_limit(self):
        import numpy as np
        state=np.random.get_state();self.addCleanup(np.random.set_state,state)
        pd.DataFrame(dict(id=['ant_man_and_the_wasp_quantumania']*8,reviewId=range(8),
            reviewText=['review']*8,originalScore=[str(i%5+1)+'/5' for i in range(8)])).to_csv(self.data/'Reviews.csv',index=False)
        self.reply=lambda messages,sampling:'Document 1'
        config,_=self.config(query='Q9',ranking='topk',limit=56,max_calls=28)
        report=self.run_session(config)
        self.assertEqual(report['physical_attempts_reserved'],56)
        self.assertEqual([r['logical_requests'] for r in report['queries']],[28,28])
        for row in report['queries']:
            self.assertEqual([b['occurrences'] for b in row['native_lm_batches']],[7,6,5,4,3,2,1])
            self.assertEqual(len(row['result']['data']),8)
        self.assertEqual(self.owners[0].events.count('start'),1)
        self.assertEqual(self.owners[0].events.count('close'),1)
        self.assertTrue(all('response_format' not in s and 'structured_outputs' not in s for s in self.samples))

    def test_actual_image_join_survivors_reach_extract_with_original_sampling(self):
        import httpx
        source=visual.NativeLotusImageQueryTests();source.setUp();self.addCleanup(source.doCleanups)
        inputs=source.mmqa_inputs()
        replies=visual.NativeHTTPReplies(self,'mmqa',query='Q2b')
        def answer(messages,sampling):
            body=dict(sampling,model='local-image-fixture',messages=messages)
            if body.pop('structured_outputs',None)=={'json_object':True}:
                body['response_format']={'type':'json_object'}
            return replies.respond(httpx.Request('POST','http://localhost:18473/v1/chat/completions',
                headers={'authorization':'Bearer '+visual.KEY_VALUE},json=body)).json()['choices'][0]['message']['content']
        self.reply=answer
        config,_=self.config(scenario='mmqa',query='Q2b',limit=16,max_calls=8)
        config['generation']['max_tokens']=32
        report=self.run_session(config,data=inputs['data_dir'])
        self.assertEqual(report['status'],'completed')
        self.assertEqual([len(q['native_lm_batches']) for q in report['queries']],[2,2])
        self.assertTrue(all(q['native_lm_batches'][1]['occurrences']>0 for q in report['queries']))
        self.assertTrue(any(s.get('response_format')=={'type':'json_object'} or
            s.get('structured_outputs')=={'json_object':True} for s in self.samples))
        self.assertTrue(any(not s.get('response_format') and not s.get('structured_outputs') for s in self.samples))
        self.assertEqual(report['physical_attempts_reserved'],len(replies.requests))

    def test_changed_model_after_first_sink_stops_repeat_and_retains_complete_result(self):
        config,_=self.config()
        calls=[]
        def snapshot():
            value=self.identity();calls.append(1)
            if len(calls)>2:value['model_processes']=[dict(pid=999,create_time=3.)]
            return value
        with self.assertRaisesRegex(RuntimeError,'model owner changed') as caught:
            self.run_session(config,identity=snapshot)
        report=caught.exception.query_report
        self.assertEqual(len(self.owners[0].inputs),1)
        self.assertEqual(report['queries'][0]['result']['data'],[[2]])
        self.assertNotIn('quality',report['queries'][0])
        self.assertIsNone(report['warm_qualified'])
        self.assertEqual(self.owners[0].events[-1],'close')

    def test_check_starts_no_owner_and_rejects_a_misleading_per_query_timeout(self):
        from contextlib import redirect_stdout
        from io import StringIO
        config,ledger=self.config()
        path=self.root/'config.json';path.write_text(json.dumps(config))
        before=ledger.snapshot()
        with patch('src.experiments.native_ray_serve_process.NativeServeProcess.start',side_effect=AssertionError('no start')),redirect_stdout(StringIO()) as out:
            self.assertEqual(entry.main(['check','--config',str(path),'--sembench-checkout',visual.CHECKOUT,'--data-dir',str(self.data)]),0)
        checked=json.loads(out.getvalue())
        self.assertEqual(checked['query_clock_version'],'lotus_ray_serve.source_to_sink.v1')
        self.assertEqual(checked['native_serve_configuration']['model_source'],'CPU-no-model')
        self.assertNotIn('structured_outputs',checked['native_serve_configuration']['sampling_params'])
        self.assertEqual(ledger.snapshot(),before)
        self.assertFalse((self.root/'session').exists())
        config['resources']['query_timeout_s']=1
        path.write_text(json.dumps(config))
        with self.assertRaisesRegex(ValueError,'complete session'):
            entry.configuration(path)

    def test_existing_cli_worker_runs_both_original_queries_and_publishes_one_session(self):
        from src.experiments import native_ray_serve_process as lifecycle
        config,_=self.config()
        path=self.root/'config.json';path.write_text(json.dumps(config))
        directory=self.root/'worker';directory.mkdir()
        native_factory=lifecycle._native_serve_process
        def factory(value,path,runtime):
            if not runtime:
                return native_factory(value,path,runtime)
            return self.owner(model_runtime_env=runtime)
        with patch.object(lifecycle,'_native_serve_process',side_effect=factory):
            self.assertEqual(entry.main(['run','--config',str(path),'--sembench-checkout',visual.CHECKOUT,
                '--data-dir',str(self.data),'--output-dir',str(directory),'--execute-model','--worker']),0)
        report=json.loads((directory/'session/session.json').read_text())
        self.assertEqual(report['baseline_identity'],'lotus-over-ray-data-serve')
        self.assertEqual([q['result']['data'] for q in report['queries']],[[[2]],[[2]]])
        self.assertEqual(self.owners[0].events[-1],'close')

    def test_two_empty_original_queries_do_not_qualify_warm_model_work(self):
        config,_=self.config()
        frame=pd.read_csv(self.data/'Reviews.csv');frame['id']='outside'
        frame.to_csv(self.data/'Reviews.csv',index=False)
        report=self.run_session(config)
        self.assertEqual(report['physical_attempts_reserved'],0)
        self.assertEqual([q['logical_requests'] for q in report['queries']],[0,0])
        self.assertIsNone(report['warm_qualified'])

    def test_evaluation_error_and_close_error_keep_primary_and_complete_sink(self):
        config,_=self.config()
        failure=RuntimeError('original evaluation failed')
        factory=self.owner
        def owner(**options):
            value=factory(**options);value.close_failure=True
            return value
        cfg,query,engine,resources=self.checked(config)
        with patch.object(runner.SemBenchTextQuery,'evaluate',side_effect=failure),self.assertRaises(RuntimeError) as caught:
            runner.run_serve_session(cfg,query,engine,resources,self.root/'session',
                session_factory=owner,identity_snapshot=self.identity)
        self.assertIs(caught.exception,failure)
        report=failure.query_report
        self.assertEqual(report['physical_attempts_reserved'],3)
        self.assertEqual(report['queries'][0]['result']['data'],[[2]])
        self.assertNotIn('quality',report['queries'][0])
        self.assertTrue(any('owned Serve close' in note for note in failure.__notes__))


if __name__=='__main__':
    unittest.main()
