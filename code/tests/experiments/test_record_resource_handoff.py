"""Zero-model probes for the bounded MMF handoff observation profile."""
from concurrent.futures import Future
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
from types import ModuleType,SimpleNamespace
import unittest
from unittest.mock import patch
from tests.experiments import test_record_resource_capture as fixtures


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.CaptureTests();self.fixture.setUp()
        self.observer=self.fixture.observer;self.observer.settings()['profile']='handoff'
    def tearDown(self):self.fixture.tearDown()
    def events(self):return self.fixture.events()
    def actor(self):
        ipc=fixtures.load('record_ipc_observer')
        with patch.object(self.observer,'install_actor',lambda:None):value=ipc.ObservedAsyncCoreModelActor()
        value._readers=0
        return ipc,value,sys.modules['src.execution_provider.adapters.lotus_records_async_actor'].AsyncCoreModelActor

    def test_future_notification_does_not_consume_result_or_change_exception(self):
        for error in (None,ValueError(fixtures.SECRET)):
            future=Future();calls=[]
            ref=SimpleNamespace(future=lambda:(calls.append(True),future)[1])
            self.observer.observe_ref_ready(ref,{'session_id':1,'sequence':2},[fixtures.IDENTITY])
            self.assertEqual(calls,[True])
            if error is None:
                value=object();future.set_result(value);self.assertIs(future.result(),value)
            else:
                future.set_exception(error)
                with self.assertRaises(ValueError)as caught:future.result()
                self.assertIs(caught.exception,error)
        markers=[e for e in self.events()if e['event']=='driver_future_done']
        self.assertEqual(len(markers),2);self.assertEqual(markers[0]['key'],{'session_id':1,'sequence':2})
        self.assertNotIn(fixtures.SECRET,json.dumps(self.events()))

    def test_ref_probe_failure_is_unknown_and_does_not_escape(self):
        def broken():raise RuntimeError(fixtures.SECRET)
        self.observer.observe_ref_ready(SimpleNamespace(future=broken),{'sequence':2},[fixtures.IDENTITY])
        self.assertEqual(self.events()[-1]['event'],'handoff_probe_unknown')
        self.assertNotIn(fixtures.SECRET,json.dumps(self.events()))

    def test_C2_interleaved_future_notifications_keep_actual_call_identity(self):
        futures=[Future(),Future()]
        for i,future in enumerate(futures):
            row=dict(fixtures.IDENTITY,call_id='call-'+str(i))
            self.observer.observe_ref_ready(SimpleNamespace(future=lambda f=future:f),{'session_id':1,'sequence':i},[row])
        futures[1].set_result(b'B');futures[0].set_result(b'A')
        rows=[e for e in self.events()if e['event']=='driver_future_done']
        self.assertEqual([(e['key']['sequence'],e['identities'][0]['call_id'])for e in rows],[(1,'call-1'),(0,'call-0')])

    def test_default_full_and_off_do_not_subscribe_extra_future(self):
        def forbidden():raise AssertionError('unexpected observation subscription')
        self.observer.settings()['profile']='full'
        self.observer.observe_ref_ready(SimpleNamespace(future=forbidden),{},[])
        self.observer.ENABLED=False
        self.observer.observe_ref_ready(SimpleNamespace(future=forbidden),{},[])
        self.observer.ENABLED=True
        self.assertEqual(self.events(),[])

    def test_light_does_not_install_gpu_cpu_span_or_byte_hooks(self):
        self.observer.install_cpu_hooks();self.observer.install_model()
        self.observer.memory_snapshot('probe')
        self.assertNotIn('cpu',self.observer._installed);self.assertNotIn('model',self.observer._installed)
        ipc=fixtures.load('record_ipc_observer');native=sys.modules['src.serving.backends.async_engine_process']
        original=native._frame;ipc.install_transport(child=False)
        self.assertIs(native._frame,original);self.assertFalse(ipc._installed)
        value=ipc.ObservedAsyncEngineProcess();expected=object()
        with patch.object(native.AsyncEngineProcess,'_io',lambda self:expected,create=True):self.assertIs(value._io(),expected)
        with patch.object(native.AsyncEngineProcess,'_receive',lambda self,value,**kwargs:value,create=True):
            self.assertIs(value._receive(expected,results=True),expected)
        self.assertFalse(any(e['event']in ('socket_io','cpu_span_start')for e in self.events()))

    def test_actor_return_preserves_original_arguments_and_payload_identity(self):
        ipc,actor,base=self.actor();rows=[dict(fixtures.IDENTITY,messages_json=fixtures.SECRET)]
        generation={'stop':[fixtures.SECRET]};result=fixtures.SECRET.encode();calls=[]
        def native(instance,*args):calls.append((instance,args));return result
        actor._readers=1
        with patch.object(base,'_run_core',native,create=True):
            self.assertIs(actor._run_core(rows,123,4096,[1,2],generation),result)
        self.assertIs(calls[0][1][0],rows);self.assertIs(calls[0][1][-1],generation)
        event=self.events()[-1];self.assertEqual(event['event'],'actor_result_return')
        self.assertEqual(event['key'],{'session_id':1,'sequence':2});self.assertEqual(actor._readers,1)
        self.assertNotIn(fixtures.SECRET,json.dumps(self.events()))

    def test_actor_business_exception_and_invalid_key_are_not_replaced_or_logged(self):
        ipc,actor,base=self.actor();business=LookupError(fixtures.SECRET);calls=[]
        def native(instance,*args):calls.append(args);raise business
        rows=iter([fixtures.SECRET])
        with patch.object(base,'_run_core',native,create=True):
            with self.assertRaises(LookupError)as caught:actor._run_core(rows,123,4096,[fixtures.SECRET,'invalid'])
        self.assertIs(caught.exception,business);self.assertEqual(len(calls),1)
        self.assertEqual(next(rows),fixtures.SECRET)
        self.assertIsNone(self.events()[-1]['key']);self.assertNotIn(fixtures.SECRET,json.dumps(self.events()))

    def test_proved_actor_close_flushes_before_owner_kill_and_is_idempotent(self):
        ipc,actor,base=self.actor();proof=dict(schema='semloom.async_actor_quiescence.v1',confirmed=True,input_gate_closed=True,active_readers=0)
        report=dict(status='closed',producer_quiescence=proof);calls=[]
        def native(instance,*args,**kwargs):calls.append((args,kwargs));return report
        with patch.object(base,'close',native,create=True):
            self.assertIs(actor.close(1.2),report);self.assertIs(actor.close(timeout=1.2),report)
        self.assertEqual(len(calls),2)
        status=json.loads(next((self.fixture.directory/'events').glob('*.status.json')).read_text())
        self.assertTrue(status['closed']and status['flush_confirmed']);self.assertEqual(status['dropped'],0)
        self.assertEqual(sum(e['event']=='actor_sink_finalizing'for e in self.events()),1)

    def test_unknown_or_active_reader_close_never_claims_final_sink(self):
        ipc,actor,base=self.actor()
        for active in (0,1):
            report=dict(status='unknown',producer_quiescence=None)
            if active:report=dict(status='closed',producer_quiescence=dict(schema='semloom.async_actor_quiescence.v1',confirmed=True,input_gate_closed=True,active_readers=0))
            actor._readers=active
            with patch.object(base,'close',lambda self,*a,**k:report,create=True):self.assertIs(actor.close(1),report)
            self.assertFalse(self.observer._closed)
            self.assertFalse(list((self.fixture.directory/'events').glob('*.status.json')))
        self.observer.emit('still_observed')
        self.assertEqual(self.events()[-1]['event'],'still_observed')

    def test_actor_close_business_exception_is_unchanged(self):
        ipc,actor,base=self.actor();business=TimeoutError(fixtures.SECRET)
        def fail(*args,**kwargs):raise business
        with patch.object(base,'close',fail,create=True):
            with self.assertRaises(TimeoutError)as caught:actor.close(1)
        self.assertIs(caught.exception,business);self.assertFalse(self.observer._closed)

    def test_sink_flush_failure_does_not_replace_native_close_report(self):
        ipc,actor,base=self.actor();report=dict(status='closed',producer_quiescence=dict(
            schema='semloom.async_actor_quiescence.v1',confirmed=True,input_gate_closed=True,active_readers=0))
        with patch.object(base,'close',lambda *a,**k:report,create=True),patch.object(self.observer.os,'fsync',side_effect=OSError('fixture')):
            self.assertIs(actor.close(1),report)
        status=json.loads(next((self.fixture.directory/'events').glob('*.status.json')).read_text())
        self.assertFalse(status['flush_confirmed']);self.assertGreater(status['errors'],0)
        self.assertIsNone(self.observer._fd)

    def test_light_controller_keeps_native_command_and_original_environment(self):
        f=fixtures.PlanTests();f.setUp();config=deepcopy(f.base)
        config['observation'].update(mode='on',profile='handoff')
        command=f.controller.worker_command(config);env=f.controller.worker_environment(config,{'TMPDIR':'baseline-tmp'})
        self.assertNotIn('profile',command);self.assertEqual(command[0],config['movie_plan']['engine']['driver_python'])
        self.assertEqual(env['TMPDIR'],'baseline-tmp');self.assertEqual(env['SEM_RECORD_OBSERVATION'],'on')
        config['observation']['mode']='off'
        off=f.controller.worker_environment(config,{'TMPDIR':'baseline-tmp'})
        self.assertFalse(any(k.startswith('SEM_RECORD_')for k in off));self.assertEqual(off['TMPDIR'],'baseline-tmp')

    def test_unknown_observation_mode_or_profile_is_rejected_before_execution(self):
        f=fixtures.PlanTests();f.setUp();config=deepcopy(f.base);config['cpu_check']={}
        for field in ('mode','profile'):
            malformed=deepcopy(config);malformed['observation'][field]='unknown'
            with patch.object(f.controller,'checked_json',return_value={'status':'passed_CPU_fixtures'}):
                with self.assertRaisesRegex(ValueError,'unknown observation'):f.controller.check_material(malformed)

    def test_light_material_needs_only_event_bounds_and_full_runtime_freeze(self):
        import hashlib
        f=fixtures.PlanTests();f.setUp();config=deepcopy(f.base)
        config.update(cpu_check={},execution_enabled=False)
        config['observation']=dict(mode='on',profile='handoff',max_events_per_process=100,max_event_bytes=4096)
        config['observer_sha256']={name:hashlib.sha256((fixtures.ROOT/name).read_bytes()).hexdigest()for name in f.controller.RUNTIME_FILES}
        cpu=dict(status='passed_CPU_fixtures',observer_sha256=config['observer_sha256'])
        with patch.object(f.controller,'checked_json',return_value=cpu):f.controller.check_material(config)

    def test_light_worker_main_skips_sampling_preserves_input_generation_and_close(self):
        runner=fixtures.load('run_resource_C2');modules={}
        for name in ('integrations','integrations.imlane_records','integrations.imlane_records.chain_runtime',
                     'src.experiments','src.experiments.attempt_ledger','src.experiments.cell_budget',
                     'src.baselines','src.baselines.common','src.baselines.common.private_artifacts'):
            module=ModuleType(name);module.__path__=[];modules[name]=module
        leases=[];modules['integrations.imlane_records.chain_runtime'].check_external_lease=leases.append
        modules['src.experiments.attempt_ledger'].AttemptBudget=lambda *a:a
        modules['src.experiments.cell_budget'].CellBudgetLedger=lambda *a:object()
        modules['src.baselines.common.private_artifacts'].write_private_json=lambda p,v:Path(p).write_text(json.dumps(v))
        config=dict(execution_enabled=True,source_root='/fixture-source',cell_id='fixture-cell',artifact_root=str(self.fixture.directory),external_owner={'test':'lease'},
            observation=dict(self.observer.settings(),mode='on'),workload=dict(kind='record_chain',name='MMF',expected_calls=12),
            run_id='fixture',expected_source={'head':'frozen'},engine={'max_num_seqs':128,'max_model_calls':12},generation={'stop':[fixtures.SECRET]})
        (self.fixture.directory/'config.json').write_text(json.dumps(config));before=deepcopy(config);seen=[]
        def dispatch(value,ledger):
            seen.append(deepcopy(value));return dict(status='completed',calls=[dict(fixtures.IDENTITY,call_id=str(i))for i in range(12)])
        native_import=__import__
        def guard(name,*a,**k):
            if name in ('src.experiments.process_sampling','src.observability.records_telemetry','torch','vllm'):
                raise AssertionError('light worker imported heavy observation')
            return native_import(name,*a,**k)
        with patch.dict(sys.modules,modules),patch.object(sys,'path',list(sys.path)),patch.object(runner,'dispatch',dispatch),patch.object(self.observer,'install_driver',lambda:None),patch('builtins.__import__',side_effect=guard):
            runner.main(str(self.fixture.directory))
        self.assertEqual(seen[0]['engine'],before['engine']);self.assertEqual(seen[0]['generation'],before['generation'])
        result=json.loads((self.fixture.directory/'WORKER-RESULT.json').read_text())
        self.assertEqual(result['actual_calls'],12);self.assertNotIn('rss',result);self.assertNotIn('telemetry',result)
        self.assertTrue(self.observer._closed);self.assertEqual(leases,[before['external_owner']])


if __name__=='__main__':unittest.main(verbosity=2)
