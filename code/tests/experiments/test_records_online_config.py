"""Current config + complete Movie callback + real Core/CPU feedback IPC.

Ray, external lease/birth checks and the upstream quality evaluator are explicit
fixtures. The installed LOTUS formatter/parser and source are real.
"""
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict, replace
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource
from src.baselines.text.movie_tasks import MovieTask
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.experiments.movie_matrix_support import run_record_admission, validate_record_admission_cache
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.semantic_methods.lotus import engine as methods
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.experiments import test_movie_records_session as session_fixtures
from tests.experiments import test_movie_calibration_cli as cli_fixtures
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.serving.test_native_resource_feedback import CPUFeedbackProcess


class CPUFeedbackWithBirth(CPUFeedbackProcess):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.ready_report=dict(self.ready_report,pid=self.process.pid)


class OnlineConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.fixture=session_fixtures.PlanTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.root=Path(self.fixture.manifests['movie-q3']).parent.parent
        self.source=MovieRecordSource.from_manifest(self.fixture.manifests['movie-q3'])
        self.method=MovieTask.from_config('movie-q3').method('openai/cpu-fixture')
        self.engine=EngineSpec('/tmp/CPU','/tmp/model','/tmp/driver',max_num_seqs=4,max_model_calls=4)
        self.resources=RecordsResources(prepare_batch_rows=1,model_batch_rows=1,max_active_batches=4,
            startup_timeout_s=5,query_timeout_s=10)
        self.profile=dict(mode='async',max_inflight=2,admission=dict(kind='resource_feedback',
            search_mode='geometric',allow_exploration=True,poll_interval_s=.003,
            window_s=.01,min_samples=2,min_remaining=32,exploration_s=1))
        self.cache=dict(cache_dir=str(self.root/'admission-cache'),reuse=True,
            identity={k:{'CPU_fixture':'same-environment'} for k in
                      ('hardware','model','generation','execution','workload','measurement')})
        self.cache['identity']['measurement']['work_unit']='model_calls'

    def runtime(self):
        stack=ExitStack();self.addCleanup(stack.close)
        # Capture the real method functions before the graph fixture replaces them.
        formatter,binder=methods.LotusFilterMethod,methods.bind_filter_method
        runtime=stack.enter_context(ThreadedRuntime(self.source.rows()))
        ready=[False]
        runtime.ray.init=lambda **kw:ready.__setitem__(0,True)
        runtime.ray.is_initialized=lambda:ready[0]
        runtime.ray.shutdown=lambda:ready.__setitem__(0,False)
        stack.enter_context(patch.object(methods,'LotusFilterMethod',formatter))
        stack.enter_context(patch.object(methods,'bind_filter_method',binder))
        stack.enter_context(patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackWithBirth))
        stack.enter_context(patch.object(movie,'verify_checkout',return_value='CPU-upstream'))
        stack.enter_context(patch.object(movie,'evaluate_original',return_value={'CPU_fixture_quality':True}))
        return runtime,stack

    def config(self):
        return dict(schema='semloom.movie_records_run.v1',backend='semloom-local-model-core-async',
            query_id='first-opaque',task='movie-q3',method=asdict(self.method),generation=asdict(GenerationSpec()),
            engine=asdict(self.engine),resources=asdict(self.resources),core_profile=self.profile,
            async_limits=dict(max_inflight=4,result_slots=4),admission_cache=self.cache)

    def test_existing_config_accepts_initial_two_owner_four_and_explicit_cache(self):
        file=self.root/'config.json';file.write_text(json.dumps(self.config()))
        value,method,generation,resources,engine=movie.read_config(file)
        result=core.resolve_core_profile(engine,resources,movie._core_profile(value))
        self.assertEqual((result['initial_window'],result['owner_max_inflight']),(2,4))
        fixed=core.resolve_core_profile(engine,resources,dict(mode='async',max_inflight=2))
        self.assertNotIn('admission',fixed)
        with self.assertRaisesRegex(ValueError,'explicit async'):
            validate_record_admission_cache(self.cache,fixed)

    def test_isolated_worker_actual_complete_query_first_then_cache_reuse(self):
        runtime,stack=self.runtime()
        config=self.config();reports=[]
        for number in range(2):
            config=dict(config,query_id='opaque-'+str(number))
            directory=self.root/('isolated-'+str(number));directory.mkdir()
            movie._model_worker(config,self.source,'CPU-upstream',self.method,
                GenerationSpec(),self.resources,self.engine,directory)
            reports.append(json.loads((directory/'query.json').read_text()))
        self.assertEqual([r['status'] for r in reports],['completed','completed'])
        self.assertEqual([r['admission_selection']['cache_hit'] for r in reports],[False,True])
        self.assertEqual(len(runtime.async_actors),2)
        self.assertEqual(sum(a.instance.engine._sequence for a in runtime.async_actors),2)
        for report in reports:
            self.assertEqual(report['calls']['counts']['succeeded'],1)
            self.assertEqual(report['backend_close']['core_usage']['held_tasks'],0)
            self.assertTrue(report['admission_selection']['online_exploration_already_in_total'])
            self.assertGreaterEqual(report['admission_selection']['request_wait_s'],
                                    report['admission_selection']['total_wall_s'])
            self.assertEqual(report['owner_close']['status'],'closed')
        self.assertEqual(reports[0]['result'],reports[1]['result'])

    def test_resident_session_warmup_does_not_publish_then_first_and_reuse_one_model(self):
        runtime,stack=self.runtime()
        stack.enter_context(patch('integrations.imlane_records.chain_runtime.check_external_lease',return_value={}))
        stack.enter_context(patch('psutil.Process',side_effect=lambda pid:SimpleNamespace(pid=pid,create_time=lambda:1.)))
        plan=deepcopy(self.fixture.plan)
        plan.update(profile=self.profile,admission_cache=self.cache,engine=asdict(self.engine),
                    resources=asdict(self.resources),queries=[dict(query_id='phase-'+str(i),task='movie-q3',
                    manifest=str(self.source.manifest_path),phase='warmup' if i==0 else 'measured') for i in range(3)])
        checked=session.check_plan(plan)
        bindings={q['query_id']:q for q in checked['queries']}
        driver=session._MovieDriver(plan,dict(owner_context={},checked_queries=bindings,
            claims={q['query_id']:None for q in plan['queries']},sembench_checkout='CPU-upstream'),
            self.root/'resident',plan['queries'][0])
        driver.start()
        try:
            reports=[]
            for query in plan['queries']:
                reports.append(driver.query(query,lambda event:None))
                if query['phase']=='warmup':
                    self.assertFalse((self.root/'admission-cache').exists())
            self.assertEqual([r['admission_selection']['cache_hit'] for r in reports],[False,False,True])
            self.assertEqual(len(runtime.async_actors),1)
            self.assertEqual(runtime.async_actors[0].instance.engine._sequence,3)
            self.assertEqual([r['calls']['counts']['succeeded'] for r in reports],[1,1,1])
            self.assertEqual([r['backend_close']['admission_feedback']['exploration_enabled'] for r in reports],
                             [False,True,False])
        finally:self.assertEqual(driver.close()['status'],'closed')

    def test_unknown_borrowing_or_incomplete_calls_never_publish_cache(self):
        resolved=core.resolve_core_profile(self.engine,self.resources,self.profile)
        for reason in ('unknown','held'):
            calls=[]
            def execute(window,**kw):
                calls.append((window,kw))
                return dict(status='completed',calls=dict(counts=dict(failed=0,unknown=int(reason=='unknown'),missing=0)),
                    backend_close=dict(status='closed',source_closed=True,cleanup_errors=[],
                        core_usage=dict(held_tasks=int(reason=='held'))))
            result=run_record_admission(self.cache,resolved,self.source,self.method,GenerationSpec(),
                self.engine,self.resources,execute)
            self.assertEqual(len(calls),1)
            self.assertNotIn('saved_hint',result['admission_selection'])
            self.assertFalse((self.root/'admission-cache').exists())

    def test_cache_binding_uses_actual_method_generation_and_model(self):
        resolved=core.resolve_core_profile(self.engine,self.resources,self.profile)
        captured=[]
        def policy(identity,*args,**kwargs):
            captured.append(identity)
            return dict(result=dict(query_report=dict(status='CPU-capture')))
        with patch('src.scheduling.runtime.calibration.run_admission_query',side_effect=policy):
            for method,generation,engine in (
                (self.method,GenerationSpec(),self.engine),
                (replace(self.method,instruction='Different task on {reviewText}'),GenerationSpec(),self.engine),
                (self.method,replace(GenerationSpec(),max_tokens=16),self.engine),
                (self.method,GenerationSpec(),replace(self.engine,model_path='/tmp/other-model'))):
                run_record_admission(self.cache,resolved,self.source,method,generation,engine,
                                     self.resources,lambda *a,**k:None)
        from src.scheduling.runtime.calibration import identity_signature
        self.assertEqual(len({identity_signature(value) for value in captured}),4)
        self.assertEqual(captured[0]['workload']['manifest_sha256'],
                         captured[3]['workload']['manifest_sha256'])

    def test_maintained_cli_executes_opt_in_once_and_reuses_complete_query_hint(self):
        fixture=cli_fixtures.CLIIntegrationTests()
        fixture.setUpClass();fixture.setUp();self.addCleanup(fixture.doCleanups)
        fixture.group.pop('calibration')
        fixture.group['profile']='onepass-optin'
        fixture.plan.update(profile=self.profile,admission_cache=self.cache)
        for query in fixture.plan['queries']:query['phase']='measured'
        fixture.write(fixture.packet/'session.json',fixture.plan)
        report,directory,capacities=fixture.run_cli('onepass-cache')
        self.assertEqual(report['status'],'completed')
        self.assertEqual((report['scan_succeeded_calls'],report['target_succeeded_calls']),(0,8))
        self.assertEqual(capacities,[4])
        queries=json.loads((directory/'units/g000/query.json').read_text())['queries']
        self.assertEqual([q['backend_report']['admission_selection']['cache_hit'] for q in queries],[False,True])
        self.assertEqual([q['backend_report']['calls']['counts']['succeeded'] for q in queries],[4,4])


if __name__=='__main__':unittest.main()
