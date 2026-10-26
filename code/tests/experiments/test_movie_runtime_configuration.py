"""Nondefault runtime declarations, with CPU-only model/host fixtures."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.experiments import movie_records_matrix as matrix
from src.experiments import movie_matrix_support as support
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from src.execution_provider.adapters import lotus_records_core as core
from src.scheduling.core.session_contract import TaskKey
from tests.experiments import test_movie_records_matrix as fixtures
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider.test_lotus_records_core import Source


def budget(calls=7, queries=2):
    return dict(max_new_calls=calls, query_count=queries, max_total_wall_s=900,
                launch_deadline_s=780, cleanup_reserve_s=120,
                automatic_retries=0, model_repair_reserve=0)


class RuntimeConfigurationTests(unittest.TestCase):
    def test_actual_source_preflight_drives_small_q1_q2_reservations(self):
        from tests.experiments import test_movie_records_session as session_fixture
        f=session_fixture.PlanTests();f.setUp();self.addCleanup(f.doCleanups)
        with tempfile.TemporaryDirectory() as folder:
            controller=fixtures.ControllerFixture(Path(folder))
            cfg,_,groups=controller.ns['bound'].return_value
            groups[0]['max_calls']=3
            controller.ns['bound'].return_value=(cfg,dict(budget=budget(3,2)),groups)
            controller.ns['read'].return_value=f.plan
            controller.ns['check_plan']=matrix.check_plan  # Actual finite CSV/source preflight.
            controller.ledger.snapshot.return_value=dict(allocated_requests=3)
            controller.run()
            self.assertEqual(controller.ledger.reserve_unit.call_args_list,
                [unittest.mock.call('q1',2),unittest.mock.call('q2',1)])

    def test_fresh_campaign_reserves_declared_occurrences_not_task_name_constants(self):
        with tempfile.TemporaryDirectory() as folder:
            f=fixtures.ControllerFixture(Path(folder))
            cfg,_,groups=f.ns['bound'].return_value
            groups[0]['max_calls']=7
            f.ns['bound'].return_value=(cfg,dict(budget=budget()),groups)
            f.ledger.snapshot.return_value=dict(allocated_requests=7)
            # Different finite input sizes for existing tasks; no new backend.
            declarations=[dict(query_id='small-q1',task='movie-q1',expected_calls=3),
                          dict(query_id='small-q3',task='movie-q3',expected_calls=4)]
            f.ns['read'].return_value=dict(queries=declarations)
            f.ns['check_plan']=Mock(return_value=dict(queries=declarations,expected_calls=7))
            report=f.run()
            self.assertEqual(f.ledger.reserve_unit.call_args_list,
                             [unittest.mock.call('small-q1',3),unittest.mock.call('small-q3',4)])
            self.assertEqual(f.ns['AttemptBudget'].call_args.args[1],7)
            self.assertEqual(report['max_calls'],7)
            self.assertEqual(report['max_wall_s'],900)
            self.assertEqual(f.ns['run_group'].call_args.args[5],900)

    def test_nondefault_query_count_and_budget_are_checked_without_requiring_old_matrix(self):
        ns,values=fixtures.BoundTests().fixture()
        values['matrix.json']['budget']=budget(7,2)
        ns['verify'].return_value=dict(budget_limit=7,wall_s=900)
        values['owner-groups.json'][0]['max_calls']=7
        values['session.json']['queries']=[dict(query_id='q0'),dict(query_id='q1')]
        ns['bound']('packet',host_checks=False)
        values['matrix.json']['budget']['query_count']=3
        with self.assertRaises((ValueError,AssertionError)):
            ns['bound']('packet',host_checks=False)

    def test_launch_limits_and_paths_come_from_existing_declarations(self):
        f=fixtures.SharedLaunchTests();f.setUp();self.addCleanup(f.doCleanups)
        f.cfg.update(budget_limit=7,gpu_idle_max_mib=3,ipc_prefix_base='/s12340000')
        limits=dict(cpu_affinity_slots=[1,3,5],max_sampled_owned_rss_bytes=123456,
                    max_owned_shm_bytes=654321)
        f.packet.SOURCE=f.root/'source'
        f.packet.read=lambda p:dict(shared_resources=limits)
        context=support.build_launch_context(f.cfg,'packet',2,f.group,
            SimpleNamespace(path=f.lease),f.output,800,dict(uuid='GPU-fixture'),
            packet=f.packet,budget_path=f.root/'another output/budget.sqlite',budget_id='other')
        self.assertEqual(context['budget']['limit'],7)
        self.assertEqual(context['budget']['path'],str(f.root/'another output/budget.sqlite'))
        self.assertEqual(context['ipc_prefix'],'/s123400020')
        self.assertEqual(context['owner_context']['gpu_idle_max_mib'],3)
        self.assertEqual(context['owner_context']['owner_resource_limits'],
                         dict(cpu_cores=3,rss_bytes=123456,shm_bytes=654321))

    def audit_fixture(self, folder):
        here=Path(folder)/'packet';here.mkdir()
        from dataclasses import asdict
        generation=asdict(GenerationSpec(max_tokens=17,temperature=.25,stop=('END',)))
        engine=dict(model_path=str(Path(folder)/'alternate model'),tokenizer_path=None,
                    chat_template='a different inline template')
        cfg=dict(engine=engine,expected_template_sha256=hashlib.sha256(engine['chat_template'].encode()).hexdigest(),
                 finite_generation_profiles=[asdict(GenerationSpec()),generation])
        plan=dict(backend='semloom-core',engine=engine,generation=generation,queries=[dict(query_id='q')])
        report=dict(backend='semloom-core',queries=[dict(query_id='q',task='movie-q3',
                  backend_report=dict(generation=deepcopy(generation)))])
        values={'base-config.json':cfg,'owner-groups.json':[dict(plan='session.json')],
                'session.json':plan,'input-bindings.json':{'movie-q3':dict(calls=3,token_reference='tokens',messages='msgs')},
                'tokens':dict(messages_sha256='sha',rows={'r':{}}),'report.json':report}
        driver=object.__new__(matrix.MovieMatrix)
        driver.packet=SimpleNamespace(HERE=here,read=lambda p:values[Path(p).name],
             read_report=lambda p:values[Path(p).name],sha=lambda p:'sha',save=Mock())
        driver.audit=SimpleNamespace(audit_rows=Mock(return_value=[{}]))
        tokenizer=SimpleNamespace(chat_template='old default')
        factory=Mock(return_value=tokenizer)
        modules={'transformers':SimpleNamespace(AutoTokenizer=SimpleNamespace(from_pretrained=factory)),
                 'torch':SimpleNamespace(cuda=SimpleNamespace(is_initialized=lambda:False))}
        return driver,values,factory,modules

    def test_token_audit_uses_selected_generation_and_model_tokenizer_fallback(self):
        with tempfile.TemporaryDirectory() as folder,patch.dict(os.environ,CUDA_VISIBLE_DEVICES=''):
            d,v,factory,modules=self.audit_fixture(folder)
            with patch.dict(sys.modules,modules):d.token_audit(Path('report.json'),Path('out.json'))
            self.assertEqual(factory.call_args.args[0],v['session.json']['engine']['model_path'])
            self.assertEqual(d.audit.audit_rows.call_args.args[-1],v['session.json']['generation'])
            self.assertEqual(d.audit.audit_rows.call_args.args[-2].chat_template,'a different inline template')

    def test_wrong_report_generation_is_rejected_before_token_success(self):
        with tempfile.TemporaryDirectory() as folder,patch.dict(os.environ,CUDA_VISIBLE_DEVICES=''):
            d,v,factory,modules=self.audit_fixture(folder)
            factory.return_value.chat_template=v['session.json']['engine']['chat_template']
            v['report.json']['queries'][0]['backend_report']['generation']['max_tokens']=8
            with patch.dict(sys.modules,modules),self.assertRaises((ValueError,AssertionError)):
                d.token_audit(Path('report.json'),Path('out.json'))
            d.packet.save.assert_not_called()

    def test_three_prepare_workers_and_nondefault_async_capacity_reach_actual_cpu_child(self):
        rows=[dict(row_id='r'+str(i),source_position=i,reviewId='review',id='taken_3',reviewText=text)
              for i,text in enumerate(['hold','hold','fast','fast'])]
        spec=EngineSpec('/tmp/another model','/tmp/alternate python','/tmp/driver python',
                        max_num_seqs=5,max_model_calls=4)
        resources=RecordsResources(cpu_workers=3,prepare_batch_rows=1,model_batch_rows=1,
             max_active_batches=6,query_timeout_s=8,startup_timeout_s=5)
        with ThreadedRuntime(rows) as runtime:
            owner=core.create_records_core(FilterMethod('other-model', instruction="Keep {reviewText}"),spec,GenerationSpec(max_tokens=17),
                       resources,profile=dict(mode='async',max_inflight=3))
            try:
                stream=owner.evaluate(Source(rows),query_id='nondefault')
                self.assertEqual({next(stream)['source_position'],next(stream)['source_position']},{2,3})
                self.assertEqual(runtime.async_actors[0].instance.engine._sequence,4)
                self.assertEqual(runtime.async_actors[0].instance.engine.snapshot()['active_requests'],2)
                self.assertLessEqual(stream.peak_usage['held_tasks'],3)
                for sequence in (0,1):
                    owner._actors.cancel_core(TaskKey(stream.session.session_id,sequence)).future().result(2)
                self.assertEqual({r['source_position'] for r in stream},{0,1})
                self.assertEqual(owner.transport.retained_refs(),0)
                prepares=[e for e in runtime.events if e[:2]==('actor','_PrepareActor')]
                model=[e for e in runtime.events if e[:2]==('actor','AsyncCoreModelActor')]
                self.assertEqual(len(prepares),3)
                self.assertTrue(all(e[2]['num_cpus']==1 for e in prepares))
                self.assertEqual(model[0][2]['num_gpus'],1)
                self.assertEqual(model[0][2]['max_concurrency'],5)
                self.assertEqual(owner._actors.engine.model_path,'/tmp/another model')
                self.assertEqual(owner._actors.generation.max_tokens,17)
            finally:
                self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_unsupported_gpu_count_still_rejects_instead_of_fake_scaling(self):
        for count in (0,2):
            with self.subTest(gpu_workers=count),self.assertRaisesRegex(ValueError,'GPU|positive'):
                RecordsResources(gpu_workers=count)

    def test_both_public_lifecycles_forward_model_paths_prepare_count_and_profile(self):
        from src.experiments import movie_records_session as session
        from src.execution_provider.adapters import lotus_records_async_actor as actor
        from tests.experiments import test_movie_records_session as session_fixture
        f=session_fixture.PlanTests();f.setUp();self.addCleanup(f.doCleanups)
        for lifecycle in ('resident','isolated'):
            with self.subTest(lifecycle=lifecycle):
                plan=deepcopy(f.plan);plan['lifecycle']=lifecycle
                plan['resources'].update(cpu_workers=3,max_active_batches=7)
                plan['engine'].update(model_path='/tmp/alternate 模型',tokenizer_path=None,max_num_seqs=5)
                plan['generation']['max_tokens']=17
                plan['profile']=dict(mode='async',max_inflight=3)
                ray=SimpleNamespace(is_initialized=lambda:False,init=Mock())
                with patch.dict(sys.modules,{'ray':ray}),patch.object(core,'SemLoomRecordsCore') as owner,\
                     patch.object(actor,'AsyncRecordsActorOwner') as actors,\
                     patch('psutil.Process',return_value=SimpleNamespace(pid=41,create_time=lambda:10.)):
                    owner.return_value.start.return_value=dict(pid=41,engine_uid='fixture')
                    d=session._MovieDriver(plan,{},Path(f.manifests['movie-q1']).parent/lifecycle,plan['queries'][0])
                    d.start()
                self.assertEqual(ray.init.call_args.kwargs['num_cpus'],5)
                self.assertEqual(ray.init.call_args.kwargs['num_gpus'],1)
                method,engine,generation,resources=actors.call_args.args
                self.assertEqual(engine.model_path,'/tmp/alternate 模型')
                self.assertIsNone(engine.tokenizer_path)
                self.assertEqual(generation.max_tokens,17)
                self.assertEqual(resources.cpu_workers,3)
                self.assertEqual(resources.max_active_batches,3)

    def test_configured_timeouts_and_output_scratch_replace_input_size_heuristic(self):
        with tempfile.TemporaryDirectory() as folder,patch.dict(os.environ,{}):
            f=fixtures.GroupFixture(Path(folder))
            original=f.ns['read']
            def read(path):
                value=original(path)
                if path.name=='session.json':
                    value.update(lifecycle='resident',resources=dict(startup_timeout_s=40,query_timeout_s=30))
                if path.name=='matrix.json':value['budget']=budget(1,1)
                return value
            f.ns['read']=read
            f.run()
            self.assertEqual(f.supervise.call_args.kwargs['timeout_s'],70)
            self.assertEqual(os.environ['TMPDIR'],str(Path(folder)/'runtime/tmp'))
            self.assertTrue(Path(os.environ['MPLCONFIGDIR']).is_dir())
            self.assertFalse((Path(folder)/'packet/runtime').exists())

    def test_tokenizer_accepts_template_file_and_keeps_model_fallback(self):
        with tempfile.TemporaryDirectory() as folder:
            template=Path(folder)/'a custom template.jinja';template.write_text('file template')
            fake=SimpleNamespace(chat_template='old')
            factory=Mock(return_value=fake)
            with patch.dict(sys.modules,{'transformers':SimpleNamespace(AutoTokenizer=SimpleNamespace(from_pretrained=factory))}):
                tokenizer=support.audit_tokenizer(dict(model_path='/fixture/other model',tokenizer_path=None,chat_template=str(template)))
            self.assertEqual(tokenizer.chat_template,'file template')
            factory.assert_called_once_with('/fixture/other model',local_files_only=True,trust_remote_code=False)


if __name__=='__main__':unittest.main()
