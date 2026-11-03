"""CPU calibration and owner-lifetime fixtures; no model, Ray or GPU runtime."""

from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.experiments.calibration import core
from src.experiments import movie_records_matrix as matrix, movie_records_session as session
from src.experiments.movie_matrix_support import materialize_core_plan, scan_jobs
from src.serving.backends.model_context import model_context_identity, model_context_record
from src.serving.backends.vllm_engine import EngineSpec
from tests.experiments import test_core_calibration as fixtures
from tests.experiments import test_movie_records_session as session_fixtures
from tests.experiments import test_records_online_config as online_fixtures


def ready(plan, uid='CPU-offline-owner', length=32768):
    engine = EngineSpec.from_dict(plan['engine'])
    return dict(engine_uid=uid, pid=os.getpid(), physical_model=False,
        model_context=model_context_record(engine, uid, length))


def observed(case):
    return dict(hardware=deepcopy(case.hardware), model=deepcopy(case.model), software=deepcopy(case.software))


def report_for(plan, checked, length=32768, *, uid='CPU-offline-owner'):
    owner_ready = ready(plan, uid, length)
    return dict(backend='semloom-core', lifecycle=plan['lifecycle'], status='completed',
        application_e2e_s=100., owners=[dict(owner_id='CPU-session-owner', ready=owner_ready)],
        queries=[dict(query_id=q['query_id'], task=q['task'], phase=q['phase'],
            owner_id='CPU-session-owner', status='completed', complete_result_observed=True,
            full_query_seconds=20. if plan['profile']['max_inflight']==1 else 10.,
            backend_report=dict(status='completed', core_execution=dict(mode='async',
                max_inflight=plan['profile']['max_inflight']), calls=dict(
                    counts=dict(expected=2, recorded=2, succeeded=2, failed=0, unknown=0,
                        missing=0, submission_unconfirmed=0),
                    records=[dict(engine_uid=uid) for _ in range(2)]))) for q in checked['queries']])


class OfflineContextTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.CoreCalibrationTests(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root=self.fixture.directory
        self.plan=deepcopy(self.fixture.plan); self.plan['engine']['max_model_len']=None
        self.observed=observed(self.fixture)

    def checked(self, plan=None):
        value=self.fixture.checked_plan(plan or self.plan)
        value['expected_calls']=4
        return value

    def identity(self, plan=None, model_ready=None):
        return core.build_core_identity(plan or self.plan, self.checked(plan), **self.observed,
            model_ready=model_ready or ready(plan or self.plan))

    def test_candidate_and_measurement_recheck_the_same_observed_context(self):
        receipt=ready(self.plan); identity=self.identity(model_ready=receipt)
        candidate=core.core_candidate_plan(self.plan, self.checked(), dict(max_inflight=2,cpu_workers=1),
            identity=identity, model_ready=receipt)
        actual=report_for(candidate,self.checked(candidate),uid='CPU-actual-executing-owner')
        sample=core.core_measurement(candidate,self.checked(candidate),actual,identity=identity,
            repeat_index=1,resources_passed=True,model_ready=receipt)
        self.assertEqual((sample['status'],sample['recorded_model_calls']),('ok',4))
        with self.assertRaises(RuntimeError):
            core.core_candidate_plan(self.plan,self.checked(),dict(max_inflight=2,cpu_workers=1),identity=identity)
        with self.assertRaisesRegex(ValueError,'plan differs'):
            core.core_candidate_plan(self.plan,self.checked(),dict(max_inflight=2,cpu_workers=1),
                identity=identity,model_ready=ready(self.plan,length=8192))

    def test_ready_binds_configuration_but_cache_identity_ignores_only_call_quota_and_instance(self):
        engine=EngineSpec.from_dict(self.plan['engine']); second=replace(engine,max_model_calls=12)
        first_ready=ready(self.plan)
        second_plan=deepcopy(self.plan); second_plan['engine']['max_model_calls']=12
        second_ready=ready(second_plan,uid='CPU-second-owner')
        self.assertEqual(model_context_identity(engine,first_ready),model_context_identity(second,second_ready))
        self.assertEqual(self.identity(),self.identity(second_plan,second_ready))
        with self.assertRaises(ValueError):
            core.core_model_ready(second_plan,first_ready)
        changed=deepcopy(self.plan); changed['engine']['model_path']='/tmp/CPU-other-weights'
        with self.assertRaises(ValueError):
            core.core_model_ready(self.plan,ready(changed))

    def test_failed_or_unknown_ready_never_produces_a_valid_measurement(self):
        identity=self.identity(); expected=ready(self.plan)
        for change in ('missing','wrong_model','different_context','wrong_query_owner','unknown'):
            with self.subTest(change=change):
                report=report_for(self.plan,self.checked())
                if change=='missing': report['owners'][0]['ready']={}
                elif change=='wrong_model': report['owners'][0]['ready']['model_context']['engine_signature']='0'*64
                elif change=='different_context': report['owners'][0]['ready']=ready(self.plan,length=8192)
                elif change=='wrong_query_owner': report['queries'][0]['backend_report']['calls']['records'][0]['engine_uid']='CPU-other'
                else:
                    report['status']='failed'
                    report['queries'][0]['backend_report']['calls']['counts'].update(succeeded=1,unknown=1)
                sample=core.core_measurement(self.plan,self.checked(),report,identity=identity,
                    repeat_index=1,resources_passed=change!='unknown',model_ready=expected)
                self.assertEqual(sample['status'],'failed')
                self.assertEqual(sample['recorded_model_calls'],4)
                self.assertIsNone(sample['full_query_s'])
                if change=='unknown': self.assertEqual(sample['unknown_model_calls'],1)
                else: self.assertIn('executing_model_context_not_confirmed',sample['reasons'])
        for state in (dict(closed=True),dict(status='failed'),dict(status='unknown')):
            with self.subTest(state=state),self.assertRaises(RuntimeError):
                core.core_model_ready(self.plan,dict(ready(self.plan),**state))

    def matrix_run(self,name,*,length=32768,explicit=False,fail=None):
        plan=deepcopy(self.plan)
        if explicit: plan['engine']['max_model_len']=4096
        directory=self.root/name;directory.mkdir();(directory/'units').mkdir()
        packet=self.root/'packet';packet.mkdir(exist_ok=True)
        def save(path,value):
            path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value))
        save(packet/'session.json',plan)
        save(packet/'matrix.json',dict(budget=dict(max_new_calls=28,query_count=14,max_total_wall_s=600,
            launch_deadline_s=540,cleanup_reserve_s=60,automatic_retries=0,model_repair_reserve=0)))
        group=dict(plan='session.json',backend='semloom-core',calibration=dict(cache_dir=str(self.root/'cache'),
            candidates=[dict(max_inflight=1,cpu_workers=1),dict(max_inflight=2,cpu_workers=1)],
            repetitions=3,near_peak_ratio=.97,timeout_s=120))
        driver=matrix.MovieMatrix.__new__(matrix.MovieMatrix);driver.root=directory
        self.last_matrix_driver=driver
        driver.packet=SimpleNamespace(HERE=packet,COMMIT=self.observed['software']['source_revision'],
            read=lambda p:json.loads(Path(p).read_text()),read_report=lambda p:json.loads(Path(p).read_text()),
            save=save,sha=lambda p:matrix.sha(p))
        driver.host=SimpleNamespace(assigned_gpu=lambda _:dict(uuid='CPU-device'),api=lambda:None,
            identities=lambda _:set(),helper=SimpleNamespace(live_owned=lambda *_:[],
                error_record=lambda err:dict(error_type=type(err).__name__)))
        calls=[]
        def native(cfg,packet_sha,index,group,lease,deadline,emit,*,slot_index,plan_binding,work_deadline=None):
            calls.append((slot_index,deepcopy(plan_binding)))
            effective=materialize_core_plan(plan,group,index,plan_binding,
                source_revision=driver.packet.COMMIT,checked=self.checked(plan))
            output=directory/'units'/('g'+str(slot_index).zfill(3));output.mkdir()
            save(output/'independent-physical.json',dict(independent_physical_exit=fail!='cleanup'))
            actual=report_for(effective,self.checked(effective),4096 if explicit else length,
                uid='CPU-instance-'+str(len(calls)))
            if fail:
                actual.update(status='failed',queries=[])
                actual['owners'][0]['ready']={}
                save(output/'query.json',actual)
                raise RuntimeError('CPU original ready failure')
            if not explicit:
                core.core_model_ready(effective,actual['owners'][0]['ready'],
                    expected=plan_binding['observed'].get('model_ready'))
            save(output/'query.json',actual)
            return dict(status='completed',actual_calls=4)
        driver.run_group=native
        with patch.object(matrix,'check_plan',side_effect=self.checked), \
                patch('src.experiments.movie_records_session.check_plan',side_effect=self.checked), \
                patch.object(matrix,'core_observations',side_effect=lambda *_:deepcopy(self.observed)):
            result=driver.run_calibrated_group({},'CPU-packet',0,group,None,time.monotonic()+600,lambda _:None,slot_index=0)
        return result,calls

    def test_offline_miss_reuses_confirmation_sample_and_hit_records_its_actual_work(self):
        first,calls=self.matrix_run('first')
        self.assertEqual(len(calls),7)
        self.assertEqual(len({slot for slot,_ in calls}),7)
        self.assertEqual(len(first['scan_units']),6)
        calibration=first['calibration_run']
        self.assertFalse(calibration['calibration']['cache_hit'])
        self.assertTrue(calibration['model_context_confirmation']['sample_in_public_scan_cost'])
        self.assertEqual(calibration['calibration']['scan_cost']['reported_work'],24)
        self.assertEqual(sum(row['measurement']['recorded_model_calls'] for row in first['scan_units']),24)
        hit,calls=self.matrix_run('hit')
        self.assertEqual([slot for slot,_ in calls],[1,0])
        self.assertEqual(len(hit['scan_units']),1)
        calibration=hit['calibration_run']
        self.assertTrue(calibration['calibration']['cache_hit'])
        self.assertEqual(calibration['calibration']['scan_cost']['samples'],0)
        self.assertFalse(calibration['model_context_confirmation']['sample_in_public_scan_cost'])
        self.assertEqual(calibration['model_context_confirmation']['recorded_model_calls'],4)
        self.assertGreaterEqual(calibration['frontend_preparation_s'],calibration['model_context_confirmation']['elapsed_s'])
        different,calls=self.matrix_run('different',length=8192)
        self.assertEqual(len(calls),7)
        self.assertFalse(different['calibration_run']['calibration']['cache_hit'])

    def test_explicit_context_keeps_the_original_zero_scan_cache_hit(self):
        first,calls=self.matrix_run('explicit',explicit=True)
        self.assertEqual(len(calls),7)
        self.assertNotIn('model_context_confirmation',first['calibration_run'])
        hit,calls=self.matrix_run('explicit-hit',explicit=True)
        self.assertEqual([slot for slot,_ in calls],[0])
        self.assertTrue(hit['calibration_run']['calibration']['cache_hit'])
        self.assertEqual(hit['scan_units'],[])

    def test_omitted_context_and_declared_null_both_require_actual_confirmation(self):
        self.plan['engine'].pop('max_model_len')
        omitted,calls=self.matrix_run('omitted')
        self.assertTrue(calls[0][1]['resolve_model_context'])
        self.assertEqual(omitted['calibration_run']['model_context_confirmation']['model_context']['max_model_len'],32768)
        self.plan['engine']['max_model_len']=None
        declared_null,calls=self.matrix_run('declared-null')
        self.assertTrue(calls[0][1]['resolve_model_context'])
        self.assertEqual(declared_null['calibration_run']['model_context_confirmation']['model_context']['max_model_len'],32768)

    def test_confirmation_cannot_bypass_identity_for_an_undeclared_sample(self):
        group=dict(backend='semloom-core',calibration=dict(cache_dir=str(self.root/'cache'),
            candidates=[dict(max_inflight=1,cpu_workers=1)],repetitions=1,near_peak_ratio=.97,timeout_s=120))
        binding=dict(kind='scan',configuration=group['calibration']['candidates'][0],observed=self.observed,
            sample_id='not-declared',resolve_model_context=True)
        with self.assertRaisesRegex(ValueError,'not predeclared'):
            materialize_core_plan(self.plan,group,0,binding,source_revision=self.observed['software']['source_revision'],checked=self.checked())
        job=scan_jobs(group,0)[0];binding.update(sample_id=job['sample_id'],resolve_model_context=False)
        with self.assertRaises(RuntimeError):
            materialize_core_plan(self.plan,group,0,binding,source_revision=self.observed['software']['source_revision'],checked=self.checked())

    def test_failed_context_confirmation_stops_before_selector_or_target(self):
        with patch('src.scheduling.runtime.calibration.run_calibrated',side_effect=AssertionError('selector ran')), \
                self.assertRaisesRegex(RuntimeError,'CPU original ready failure'):
            self.matrix_run('failed-ready',fail=True)
        self.assertTrue((self.root/'failed-ready/calibration/g000/failed.json').is_file())
        self.assertFalse((self.root/'failed-ready/units/g000').exists())
        self.assertEqual(len(list((self.root/'failed-ready/units').iterdir())),1)
        failure=json.loads((self.root/'failed-ready/calibration/g000/failed.json').read_text())
        cost=failure['scan_units'][0]['measurement']
        self.assertEqual(cost['schema'],'semloom.core_context_failure.v1')
        self.assertIsNone(cost['identity_sha256'])
        with self.assertRaises(ValueError):core.core_runtime_measurement(cost)
        with patch('src.scheduling.runtime.calibration.run_calibrated',side_effect=AssertionError('selector ran')), \
                self.assertRaisesRegex(RuntimeError,'CPU original ready failure'):
            self.matrix_run('unknown-cleanup',fail='cleanup')
        self.assertFalse(self.last_matrix_driver.last_calibration_physical)
        self.assertFalse((self.root/'unknown-cleanup/units/g000').exists())
        self.assertEqual(len(list((self.root/'unknown-cleanup/units').iterdir())),1)

    def test_unresolved_later_owner_keeps_prior_warmup_and_unknown_calls_without_an_identity(self):
        report=report_for(self.plan,self.checked())
        report['owners'].append(dict(owner_id='CPU-late-owner',ready={}))
        report['queries'][1]['backend_report']['calls']['counts'].update(succeeded=1,unknown=1)
        with self.assertRaises(RuntimeError):core.core_session_model_ready(self.plan,report)
        cost=core.core_context_failure(self.checked(),report)
        self.assertEqual((cost['recorded_model_calls'],cost['warmup_recorded_model_calls'],
            cost['succeeded_model_calls'],cost['unknown_model_calls']),(4,2,3,1))
        self.assertIsNone(cost['identity_sha256'])
        with self.assertRaises(ValueError):core.core_runtime_measurement(cost)


class ExecutingOwnerTests(unittest.TestCase):
    def setUp(self):
        self.fixture=session_fixtures.PlanTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.plan=deepcopy(self.fixture.plan)
        self.plan.update(backend='semloom-core',lifecycle='resident',profile=dict(mode='async',max_inflight=2))
        self.plan['engine']['max_model_len']=None
        self.root=Path(self.fixture.manifests['movie-q3']).parent.parent

    def driver(self,name,*,actual_length=32768,expected_length=32768,resolving=False,context=None):
        plan=deepcopy(self.plan)
        context=context if context is not None else dict(plan_binding=dict(kind='scan',
            observed={} if resolving else dict(model_ready=ready(plan,'CPU-earlier-sample',expected_length)),
            **(dict(resolve_model_context=True) if resolving else {})))
        self.context=context
        receipt=ready(plan,'CPU-executing-'+name,actual_length)
        self.owner=SimpleNamespace(start=lambda:receipt,_actors=SimpleNamespace(model_ready=receipt),
            close=lambda **_:dict(status='closed'))
        ray=SimpleNamespace(is_initialized=lambda:False,init=lambda **_:None,shutdown=lambda:None)
        stack=ExitStack();self.addCleanup(stack.close)
        stack.enter_context(patch.dict(sys.modules,ray=ray))
        stack.enter_context(patch('src.execution_provider.adapters.lotus_records_core.create_records_core',return_value=self.owner))
        stack.enter_context(patch('src.infrastructure.cpu_allocation.checked_local_cpu_slots',side_effect=lambda n:n))
        query=plan['queries'][0]
        driver=session._MovieDriver(plan,context,self.root/name,query)
        return driver

    def test_actual_owner_start_rejects_context_drift_and_cleanup_still_uses_existing_lifecycle(self):
        driver=self.driver('drift',actual_length=8192)
        with self.assertRaisesRegex(ValueError,'executing model context differs'):
            driver.start()
        self.assertEqual(driver.close()['status'],'closed')

    def test_isolated_owners_each_supply_their_own_ready_and_keep_the_same_resolved_context(self):
        first=self.driver('isolated-first',resolving=True)
        first_ready=first.start()
        self.assertEqual(first_ready['engine_uid'],'CPU-executing-isolated-first')
        self.assertEqual(first.close()['status'],'closed')
        second=self.driver('isolated-second',context=self.context)
        second_ready=second.start()
        self.assertNotEqual(second_ready['engine_uid'],first_ready['engine_uid'])
        self.assertEqual(second.close()['status'],'closed')
        third=self.driver('isolated-drift',context=self.context,actual_length=8192)
        with self.assertRaisesRegex(ValueError,'executing model context differs'):
            third.start()
        self.assertEqual(third.close()['status'],'closed')


class CPUChildSessionTests(unittest.TestCase):
    def setUp(self):
        self.fixture=online_fixtures.OnlineConfigurationTests();self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def session_fixture(self,lifecycle):
        case=self.fixture;runtime,stack=case.runtime()
        stack.enter_context(patch('integrations.imlane_records.chain_runtime.check_external_lease',return_value={}))
        stack.enter_context(patch('psutil.Process',side_effect=lambda pid:SimpleNamespace(pid=pid,create_time=lambda:1.)))
        plan=deepcopy(case.fixture.plan)
        plan.update(lifecycle=lifecycle,profile=dict(mode='async',max_inflight=2),
            engine=asdict(case.engine),resources=asdict(case.resources),
            queries=[dict(query_id='CPU-query-'+str(i),task='movie-q3',
                manifest=str(case.source.manifest_path),phase='warmup' if i==0 else 'measured') for i in range(3)])
        checked=session.check_plan(plan);expected=ready(plan)
        context=dict(owner_context={},checked_queries={q['query_id']:q for q in checked['queries']},
            claims={q['query_id']:None for q in plan['queries']},sembench_checkout='CPU-upstream',
            plan_binding=dict(kind='scan',observed=dict(model_ready=expected)))
        return case,runtime,plan,checked,context,expected

    def check_session(self,lifecycle):
        case,runtime,plan,checked,context,expected=self.session_fixture(lifecycle)
        report=session.run_sequence(plan['queries'],
            lambda path,q:session._MovieDriver(plan,context,path,q),case.root/lifecycle,
            backend='semloom-core',lifecycle=lifecycle)
        actual=core.core_session_model_ready(plan,report,expected=expected)
        self.assertEqual(actual['model_context']['max_model_len'],32768)
        self.assertEqual(len(report['owners']),1 if lifecycle=='resident' else 3)
        self.assertTrue(all(o['status']=='closed' for o in report['owners']))
        self.assertEqual([q['backend_report']['calls']['counts']['succeeded'] for q in report['queries']],[1,1,1])
        self.assertEqual(len({q['backend_report']['calls']['records'][0]['engine_uid']
            for q in report['queries']}),1 if lifecycle=='resident' else 3)

    def test_real_CPU_child_resident_queries_reuse_one_resolved_owner(self):
        self.check_session('resident')

    def test_real_CPU_child_isolated_queries_each_supply_their_own_resolved_owner(self):
        self.check_session('isolated')

    def test_replacing_ready_with_another_instance_stops_before_a_second_CPU_submission(self):
        case,runtime,plan,checked,context,expected=self.session_fixture('resident')
        driver=session._MovieDriver(plan,context,case.root/'replacement',plan['queries'][0]);driver.start()
        original=driver.owner._actors.model_ready
        try:
            driver.query(plan['queries'][0],lambda _:None)
            self.assertEqual(runtime.async_actors[0].instance.engine._sequence,1)
            driver.owner._actors.model_ready=ready(plan,'CPU-replacement')
            with self.assertRaisesRegex(ValueError,'model owner changed'):
                driver.query(plan['queries'][1],lambda _:None)
            self.assertEqual(runtime.async_actors[0].instance.engine._sequence,1)
        finally:
            driver.owner._actors.model_ready=original
            self.assertEqual(driver.close()['status'],'closed')


if __name__=='__main__':unittest.main()
