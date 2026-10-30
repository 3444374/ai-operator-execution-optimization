from src.baselines.text.movie_tasks import MovieTask
"""Production Core + real CPU child; Ray/Daft and model are explicit fixtures."""
from dataclasses import replace
import json
import time
import unittest
from unittest.mock import patch
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.semantic_methods.lotus.records import RecordsResources, FilterMethod
from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider.test_lotus_records_core import Source
from tests.serving.test_native_resource_feedback import CPUFeedbackProcess


class CompletedWorkTests(unittest.TestCase):
    def test_optional_audit_clock_failure_does_not_discard_valid_service_feedback(self):
        from tests.execution_provider.test_core_feedback_cadence import CadenceTests
        f=CadenceTests().make();clock=core.time.monotonic;failed=[]
        f.ref=f.owner._actors.model_actor.snapshot.remote();f.future=f.ref.future()
        f.ref_expired=False;f.ref_deadline=clock()+1
        def capture():
            if not failed:
                failed.append(True);raise RuntimeError('CPU audit clock failure')
            return clock()
        with patch.object(core.time,'monotonic',side_effect=capture):sample=f.sample()
        self.assertIsNotNone(sample);self.assertIsNone(sample.received_at_s)
        self.assertEqual(f.sample_errors,0);self.assertEqual(f.samples,1)

    def stream(self):
        from types import SimpleNamespace
        from src.semantic_methods.lotus.records import identity
        value=identity('work-query','row',0)
        stream=object.__new__(core._CoreRows)
        stream.expected={value['call_id']:value};stream.received=set()
        stream.owner=SimpleNamespace(_feedback=None);stream.completed_work=(0,0,0)
        return stream,dict(value,status='succeeded')

    def test_existing_optional_usage_can_remain_missing_without_a_business_error(self):
        stream,value=self.stream()
        self.assertEqual(stream._value(json.dumps(value))['status'],'succeeded')
        self.assertEqual(stream.completed_work,(1,None,None))

    def test_actual_work_is_counted_once_after_call_identity_validation(self):
        stream,value=self.stream();value.update(prompt_tokens=12,completion_tokens=4)
        stream._value(json.dumps(value));self.assertEqual(stream.completed_work,(1,12,4))
        with self.assertRaises(ValueError):stream._value(json.dumps(value))
        self.assertEqual(stream.completed_work,(1,12,4))

    def test_sampler_requires_progress_and_work_from_same_completed_prefix(self):
        from tests.execution_provider.test_core_feedback_cadence import CadenceTests
        fixture=CadenceTests();f=fixture.make();f.progress=(10,90,0,False)
        f.completed_work=(10,120,40);sample=f.sample()
        self.assertEqual((sample.completed_input_work_total,sample.completed_output_work_total),(120,40))
        f.completed_work=(11,132,44);sample=f.sample()
        self.assertIsNone(sample.completed_input_work_total)
        self.assertIsNone(sample.completed_output_work_total)


class CoreFeedbackTests(unittest.TestCase):
    def test_short_query_hint_stays_unresolved_without_repeating_calls_or_probes(self):
        from pathlib import Path
        import tempfile
        from src.scheduling.runtime.calibration import run_admission_query
        rows=self.rows(24)
        identity={k:{'fixture':'one-owner-CPU'} for k in ('hardware','model','generation','execution','workload','measurement')}
        identity['measurement']['work_unit']='model_calls'
        with tempfile.TemporaryDirectory() as tmp,ThreadedRuntime(rows) as fake, \
                patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess):
            owner=self.owner(initial=2,maximum=4,options=dict(min_remaining=32))
            reports=[]
            def execute(window,*,allow_exploration):
                stream=owner.evaluate(Source(rows),query_id='reuse-'+str(len(reports)),
                    max_inflight=window,allow_exploration=allow_exploration)
                values=list(stream);report=stream.last_close_report;reports.append(report)
                self.assertEqual(len(values),24);self.assertEqual(len({r['call_id'] for r in values}),24)
                return dict(status='completed',reuse_eligible=report['status']=='closed',
                    admission_feedback=report['admission_feedback'])
            try:
                results=[run_admission_query(identity,execute,initial_window=2,maximum=4,cache_dir=Path(tmp)) for _ in range(3)]
                self.assertEqual([r['cache_hit'] for r in results],[False,True,True])
                self.assertEqual([r['admission_feedback']['exploration_enabled'] for r in reports],[True,True,True])
                self.assertTrue(all(r['admission_feedback']['exploration_censored'] for r in reports))
                self.assertTrue(all(r['admission_feedback']['changes']==0 for r in reports))
                self.assertEqual(len(fake.async_actors),1);self.assertEqual(fake.async_actors[0].instance.engine._sequence,72)
                self.assertEqual(owner.core.capacity.usage().held_tasks,0);self.assertEqual(owner.transport.retained_refs(),0)
                self.assertTrue(owner._feedback.base_options.allow_exploration)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def rows(self,n,text='fast'):
        return [dict(row_id='r'+str(i),source_position=i,reviewId='review-'+str(i),
            id='taken_3',reviewText=text) for i in range(n)]

    def owner(self,*,initial=1,maximum=4,admission=True,options=None,method=None):
        profile=dict(mode='async',max_inflight=initial)
        if admission:profile['admission']=dict(dict(kind='resource_feedback',poll_interval_s=.003,
            window_s=.01,min_samples=2,min_remaining=4,exploration_s=4,allow_exploration=True),**(options or {}))
        return core.create_records_core(method or FilterMethod('fixture', 'Evaluate {reviewText}'),
            EngineSpec('/tmp/CPU-fixture','/tmp/model-python','/tmp/driver-python',max_num_seqs=8),
            GenerationSpec(),RecordsResources(cpu_workers=2,prepare_batch_rows=1,model_batch_rows=1,
                max_active_batches=maximum,startup_timeout_s=5,query_timeout_s=10),profile=profile)

    def test_unbudgeted_feedback_grows_on_CPU_IPC_once_and_matches_fixed_results(self):
        rows=self.rows(64);outputs=[];prompts=[]
        for enabled in (False,True):
            with ThreadedRuntime(rows) as fake, patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess):
                owner=self.owner(admission=enabled,options=dict(min_remaining=0,
                    exploration_s=None,max_changes=None))
                try:
                    stream=owner.evaluate(Source(rows),query_id='same-input');values=list(stream)
                    outputs.append([(r['source_position'],r['text'],r['total_tokens']) for r in sorted(values,key=lambda r:r['source_position'])])
                    prompts.append([row['messages_json'] for e in fake.events if e[:2]==('submit','AsyncCoreModelActor.run_core')
                        for row in e[3][0].value.result(2)])
                    self.assertEqual(len(values),64);self.assertEqual(len({r['call_id'] for r in values}),64)
                    self.assertEqual(fake.async_actors[0].instance.engine._sequence,64)
                    self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                    self.assertEqual(owner.transport.retained_refs(),0)
                    if enabled:
                        self.assertIsNone(owner._feedback.options.exploration_s)
                        self.assertIsNone(owner._feedback.options.max_changes)
                        self.assertEqual(owner._feedback.options.min_remaining,0)
                        feedback=stream.last_close_report['admission_feedback']
                        self.assertGreater(feedback['changes'],0,feedback)
                        self.assertGreater(feedback['peak_native_running'],1,feedback)
                        self.assertLessEqual(feedback['peak_native_running'],4)
                        self.assertTrue(feedback['sampling_paused'])
                        self.assertEqual(feedback['sample_errors'],0)
                        self.assertEqual(owner.resources.max_active_batches,4)
                    else:self.assertEqual(owner.resources.max_active_batches,1)
                finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')
        self.assertEqual(outputs[0],outputs[1]);self.assertEqual(prompts[0],prompts[1])
        self.assertEqual(sum(text=='True' for _,text,_ in outputs[1]),32)

    def test_short_and_empty_queries_do_not_explore_or_create_extra_model_work(self):
        for n in (0,3):
            rows=self.rows(n)
            with ThreadedRuntime(rows) as fake,patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess):
                owner=self.owner()
                try:
                    stream=owner.evaluate(Source(rows),query_id='short');self.assertEqual(len(list(stream)),n)
                    self.assertEqual(stream.last_close_report['admission_feedback']['changes'],0)
                    self.assertEqual(len(fake.async_actors),int(n>0))
                    if n:self.assertEqual(fake.async_actors[0].instance.engine._sequence,n)
                finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_owner_reuses_model_but_resets_feedback_and_query_window(self):
        rows=self.rows(12)
        with ThreadedRuntime(rows) as fake,patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess):
            owner=self.owner()
            try:
                for qid,initial in [('Q1',1),('Q3',2),('Q2',1),('Q3-again',3)]:
                    stream=owner.evaluate(Source(rows),query_id=qid,max_inflight=initial)
                    values=list(stream)
                    self.assertEqual(len(values),12)
                    self.assertEqual({r['query_id'] for r in values},{qid})
                    report=stream.last_close_report['admission_feedback']
                    self.assertEqual(report['total_items'],12)
                    self.assertTrue(report['sampling_paused'])
                    self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                self.assertEqual(len(fake.async_actors),1)
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,48)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_missing_stats_preserve_initial_limit_without_waiting_or_retrying(self):
        rows=self.rows(8)
        # Existing fixture has no native feedback API; that is explicitly missing.
        with ThreadedRuntime(rows) as fake:
            owner=self.owner(initial=2)
            try:
                stream=owner.evaluate(Source(rows),query_id='missing');values=list(stream)
                self.assertEqual(len(values),8)
                self.assertEqual(stream.last_close_report['admission_feedback']['changes'],0)
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,8)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_cancel_stops_sampler_keeps_known_usage_and_original_cleanup(self):
        rows=self.rows(4,'hold')
        with ThreadedRuntime(rows) as fake,patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess):
            owner=self.owner(initial=2)
            try:
                stream=owner.evaluate(Source(rows),query_id='cancel');stream._fill()
                deadline=time.monotonic()+3
                while fake.async_actors[0].instance.engine.snapshot()['accepted_requests']<2:
                    stream.session.advance(1);self.assertLess(time.monotonic(),deadline)
                    owner.core.wake.wait(owner.core.wake.generation,.003)
                while (fake.async_actors[0].instance.engine.snapshot()["native_feedback"] or {}).get("gated_requests") != 2:
                    self.assertLess(time.monotonic(),deadline)
                    time.sleep(.003)
                stream.close()
                self.assertIsNone(owner._feedback.stream)
                while owner.core.capacity.usage().held_tasks:
                    owner.core.reap(owner.capacity.step_actions);self.assertLess(time.monotonic(),deadline)
                    owner.core.wake.wait(owner.core.wake.generation,.003)
                stream.collect_receipts()
                self.assertEqual(len(stream.unconsumed_call_results),2)
                self.assertEqual(sum(r['prompt_tokens'] for r in stream.unconsumed_call_results),22)
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,2)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_sync_or_invalid_admission_rejects_without_model(self):
        spec=EngineSpec('/tmp/cpu','/tmp/model','/tmp/driver')
        resources=RecordsResources(prepare_batch_rows=1,model_batch_rows=1,max_active_batches=4)
        for value in (dict(mode='sync',admission=dict(kind='resource_feedback')),
                      dict(mode='async',admission=dict(kind='other')),
                      dict(mode='async',admission=dict(kind='resource_feedback',window_s=0))):
            with self.assertRaises((ValueError,TypeError)):core.resolve_core_profile(spec,resources,value)

    def test_cli_and_both_session_lifecycles_keep_soft_start_separate_from_owner_capacity(self):
        from copy import deepcopy
        from dataclasses import asdict
        from pathlib import Path
        from types import SimpleNamespace
        import sys,tempfile
        from src.execution_provider.adapters import lotus_records_async_actor as async_actor
        from src.experiments import lotus_records_movie as movie,movie_records_session as session
        from tests.experiments.test_movie_records_session import PlanTests
        profile=dict(mode='async',max_inflight=2,admission=dict(kind='resource_feedback'))
        fixture=PlanTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        for lifecycle in ('isolated','resident'):
            plan=deepcopy(fixture.plan);plan['lifecycle']=lifecycle
            plan['resources'].update(max_active_batches=4,prepare_batch_rows=1,model_batch_rows=1)
            plan['engine']['max_num_seqs']=4;plan['profile']=profile
            directory=Path(fixture.manifests['movie-q1']).parent.parent/lifecycle
            ray=SimpleNamespace(is_initialized=lambda:False,init=lambda **kw:None)
            process=SimpleNamespace(pid=41,create_time=lambda:10.)
            with patch.dict(sys.modules,{'ray':ray}),patch.object(core,'SemLoomRecordsCore') as owner, \
                    patch.object(async_actor,'AsyncRecordsActorOwner') as actor, \
                    patch('psutil.Process',return_value=process):
                owner.return_value.start.return_value=dict(pid=41,engine_uid='CPU-fixture')
                driver=session._MovieDriver(plan,{},directory,plan['queries'][0]);driver.start()
                self.assertEqual(actor.call_args.args[3].max_active_batches,4)
                self.assertEqual(owner.call_args.kwargs['admission']['kind'],'resource_feedback')
                self.assertEqual(driver.core_execution['initial_window'],2)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'config.json'
            engine=EngineSpec('/tmp/model','/tmp/model-python','/tmp/driver-python',max_num_seqs=4)
            config=dict(schema='semloom.movie_records_run.v1',backend='semloom-local-model-core',
                query_id='CPU-profile',method=asdict(MovieTask.from_config('movie-q3').method('fixture')),engine=asdict(engine),
                resources=asdict(RecordsResources(prepare_batch_rows=1,model_batch_rows=1,max_active_batches=4)),
                generation=asdict(GenerationSpec()),core_profile=profile,
                async_limits=dict(max_inflight=4,result_slots=4))
            path.write_text(json.dumps(config));loaded=movie.read_config(path)
            value=core.resolve_core_profile(loaded[4],loaded[3],movie._core_profile(loaded[0]))
            self.assertEqual((value['owner_max_inflight'],value['initial_window']),(4,2))

    def test_public_exact_cache_initializes_window_without_ordinary_query_scans(self):
        from pathlib import Path
        import tempfile
        from src.scheduling.runtime import calibration as runtime
        rows=self.rows(6);queries=[];measurements=[]
        identity=dict(hardware=dict(cpu_fixture=True),model=dict(revision='CPU-fixed'),
            generation=dict(fixed=True),execution=dict(owner_capacity=4,admission='resource_feedback-v1'),
            workload=dict(input='six-fixed-rows'),measurement=dict(work_unit='rows',objective='fixture-full-query'))
        candidates=[dict(max_inflight=1),dict(max_inflight=4)]
        with tempfile.TemporaryDirectory() as tmp,ThreadedRuntime(rows) as fake, \
                patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess):
            owner=self.owner()
            def execute(configuration):
                qid='cache-'+str(len(queries));queries.append(configuration['max_inflight'])
                stream=owner.evaluate(Source(rows),query_id=qid,max_inflight=configuration['max_inflight'])
                values=list(stream)
                self.assertEqual(stream.execution_profile['owner_max_inflight'],4)
                self.assertEqual(stream.execution_profile['initial_window'],configuration['max_inflight'])
                self.assertEqual(len(values),6)
                return values
            def measure(configuration,**options):
                measurements.append(configuration);execute(configuration)
                return runtime.Measurement(10/configuration['max_inflight'],'ok',True,6)
            common=dict(default=candidates[0],cache_dir=Path(tmp),repetitions=1,work_per_sample=6)
            try:
                miss=runtime.run_query(identity,candidates,execute,measure=measure,**common)
                self.assertEqual(miss['configuration_source'],'explicit_default');self.assertEqual(measurements,[])
                runtime.run_query(identity,candidates,execute,mode='offline',measure=measure,
                    budget=runtime.ScanBudget(10,12),**common)
                hit=runtime.run_query(identity,candidates,execute,measure=measure,**common)
                self.assertEqual(hit['configuration_source'],'calibration_cache')
                self.assertEqual(queries,[1,1,4,4]);self.assertEqual(len(measurements),2)
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,24)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_actual_new_method_prompts_Q1_Q3_Q2_Q3_reuse_one_model(self):
        from src.baselines.text.movie_tasks import MovieTask
        from src.semantic_methods.lotus import engine as methods
        from src.semantic_methods.lotus.records import identity
        formatter,bind=methods.LotusFilterMethod,methods.bind_filter_method
        tasks=[MovieTask(n).method('fixture') for n in (1,3,2,3)]
        rows=self.rows(4);captured=[]
        with ThreadedRuntime(rows) as fake,patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess), \
                patch.object(methods,'LotusFilterMethod',formatter),patch.object(methods,'bind_filter_method',side_effect=bind):
            owner=self.owner(method=tasks[0])
            try:
                for index,method in enumerate(tasks):
                    qid='method-'+str(index)
                    stream=owner.evaluate(Source(rows),query_id=qid,method=method)
                    values=list(stream)
                    prepared=[row for event in fake.events if event[:2]==('submit','AsyncCoreModelActor.run_core')
                              for row in event[3][0].value.result(2) if row['query_id']==qid]
                    expected=formatter(bind(method)).prepare_rows([dict(row,**identity(qid,row['row_id'],row['source_position'])) for row in rows])
                    actual={row['row_id']:row['messages_json'] for row in prepared}
                    self.assertEqual(actual,{row['row_id']:row['messages_json'] for row in expected})
                    captured.append(actual['r0']);self.assertEqual(len(values),4)
                self.assertNotEqual(captured[0],captured[1]);self.assertNotEqual(captured[1],captured[2])
                self.assertEqual(captured[1],captured[3]);self.assertEqual(len(fake.async_actors),1)
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,16)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')
