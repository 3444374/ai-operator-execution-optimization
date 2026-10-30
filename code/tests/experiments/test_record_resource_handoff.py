"""Zero-model probes for the bounded MMF handoff observation profile."""
from concurrent.futures import Future
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import threading
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
        value._readers=0;value._condition=threading.Condition()
        value._held={};value._closed_pending=None;value.last_close_report=None
        return ipc,value,sys.modules['src.execution_provider.adapters.lotus_records_async_actor'].AsyncCoreModelActor

    def test_feedback_buffer_does_no_file_IO_and_drains_through_existing_sink(self):
        with patch.object(self.observer.os,'write',side_effect=AssertionError('hot-path I/O forbidden')):
            self.observer.emit_feedback('native_feedback_frame',engine_uid='a'*32,owner_uid='b'*32,
                session_id=7,source_sequence=3,source_observed_at_s=1.,received_at_s=1.1,
                stage='IPC_cache',accepted=True,reason='cached_for_consumer',prompt=fixtures.SECRET)
        self.assertEqual(self.events(),[])
        self.observer.close_events()
        event=self.events()[0]
        self.assertEqual(event['source_sequence'],3);self.assertEqual(event['session_id'],7)
        self.assertEqual(event['engine_uid'],'a'*32)
        self.assertLessEqual(event['observation_ns'],event['monotonic_ns'])
        self.assertNotIn(fixtures.SECRET,json.dumps(self.events()))
        status=json.loads(next((self.fixture.directory/'events').glob('*.status.json')).read_text())
        self.assertTrue(status['feedback_buffer']['drain_complete'])
        self.assertTrue(status['flush_confirmed']);self.assertEqual(status['feedback_buffer']['drained'],1)

    def test_feedback_buffer_overflow_and_bad_identity_are_visible_and_off_does_not_buffer(self):
        self.observer.settings()['max_events_per_process']=2
        for i in range(3):
            self.observer.emit_feedback('native_feedback_frame',engine_uid=fixtures.SECRET,
                session_id=True,source_sequence=i,reason='fixture')
        self.observer.ENABLED=False
        self.observer.emit_feedback('native_feedback_frame',source_sequence=99)
        self.observer.ENABLED=True;self.observer.close_events()
        self.assertEqual(len(self.events()),2)
        self.assertTrue(all(x['engine_uid']is None and x['session_id']is None for x in self.events()))
        status=json.loads(next((self.fixture.directory/'events').glob('*.status.json')).read_text())
        self.assertEqual(status['feedback_buffer']['attempted'],3)
        self.assertEqual(status['feedback_buffer']['dropped'],1)
        self.assertFalse(status['feedback_buffer']['drain_complete'])
        self.assertEqual(status['dropped'],1)
        self.assertNotIn(fixtures.SECRET,json.dumps(self.events()))

    def assert_concurrent_close(self, *, fail_flush=False):
        for sequence in (1,2):
            self.observer.emit_feedback('native_feedback_frame',source_sequence=sequence)
        paused,release,second_attempted=[threading.Event()for _ in range(3)]
        lock=self.observer._close_lock;original_emit=self.observer.emit;original_fsync=self.observer.os.fsync
        errors=[]
        class ClosingLock:
            def __enter__(unused):
                if threading.current_thread()is second:second_attempted.set()
                lock.acquire()
            def __exit__(unused,*args):lock.release()
        def wait_for_release():
            paused.set()
            if not release.wait(5):raise AssertionError('close probe was not released')
        def emit(event,**values):
            original_emit(event,**values)
            if not fail_flush and values.get('source_sequence')==1:wait_for_release()
        def fsync(descriptor):
            if fail_flush:
                wait_for_release();raise OSError('CPU flush failure')
            original_fsync(descriptor)
        def close():
            try:self.observer.close_events()
            except BaseException as error:errors.append(error)
        first=threading.Thread(target=close,daemon=True);second=threading.Thread(target=close,daemon=True)
        with patch.object(self.observer,'_close_lock',ClosingLock()), \
                patch.object(self.observer,'emit',emit),patch.object(self.observer.os,'fsync',side_effect=fsync)as flush:
            first.start()
            try:
                self.assertTrue(paused.wait(5),'first closer did not reach paused I/O')
                second.start()
                self.assertTrue(second_attempted.wait(5),'second closer did not attempt close')
                self.assertTrue(second.is_alive())
                self.assertFalse(list((self.fixture.directory/'events').glob('*.status.json')))
            finally:
                release.set();first.join(5)
                if second.ident is not None:second.join(5)
            self.assertFalse(first.is_alive());self.assertFalse(second.is_alive());self.assertEqual(errors,[])
            self.assertEqual(flush.call_count,1)
            status_path=next((self.fixture.directory/'events').glob('*.status.json'))
            original_status=status_path.read_bytes()
            for _ in range(3):self.observer.close_events()
            self.assertEqual(flush.call_count,1);self.assertEqual(status_path.read_bytes(),original_status)
        status=json.loads(original_status);buffer=status['feedback_buffer']
        self.assertEqual([e['source_sequence']for e in self.events()],[1,2])
        self.assertEqual((status['events_written'],status['events_attempted'],status['dropped']),(2,2,0))
        self.assertEqual((buffer['enqueued'],buffer['drained'],buffer['remaining'],buffer['dropped'],buffer['errors']),(2,2,0,0,0))
        self.assertEqual(self.observer._dropped,0);self.assertTrue(buffer['drain_complete'])
        self.assertEqual(status['errors'],int(fail_flush));self.assertEqual(status['flush_confirmed'],not fail_flush)

    def test_concurrent_close_drains_once_and_repeated_close_keeps_footer(self):
        self.assert_concurrent_close()

    def test_concurrent_close_with_failed_fsync_never_claims_flush(self):
        self.assert_concurrent_close(fail_flush=True)

    def test_feedback_enqueue_during_close_does_not_wait_for_disk_and_reports_drop(self):
        self.observer.emit_feedback('native_feedback_frame',source_sequence=1)
        paused,release,offered=[threading.Event()for _ in range(3)];errors=[]
        def fsync(descriptor):
            paused.set()
            if not release.wait(5):raise OSError('close probe was not released')
        def close():
            try:self.observer.close_events()
            except BaseException as error:errors.append(error)
        def offer():
            self.observer.emit_feedback('native_feedback_frame',source_sequence=2);offered.set()
        closer=threading.Thread(target=close,daemon=True);producer=threading.Thread(target=offer,daemon=True)
        with patch.object(self.observer.os,'fsync',side_effect=fsync):
            closer.start()
            try:
                self.assertTrue(paused.wait(5));producer.start()
                self.assertTrue(offered.wait(5),'feedback waited for disk I/O')
            finally:
                release.set();closer.join(5)
                if producer.ident is not None:producer.join(5)
        self.assertFalse(closer.is_alive());self.assertFalse(producer.is_alive());self.assertEqual(errors,[])
        status=json.loads(next((self.fixture.directory/'events').glob('*.status.json')).read_text())
        self.assertEqual([e['source_sequence']for e in self.events()],[1])
        self.assertEqual((status['feedback_buffer']['attempted'],status['feedback_buffer']['enqueued'],status['dropped']),(2,1,1))
        self.assertFalse(status['feedback_buffer']['drain_complete'])

    def test_disabled_close_never_waits_for_close_or_sink_locks(self):
        self.observer.ENABLED=False
        try:
            with patch.object(self.observer,'_close_lock',None),patch.object(self.observer,'_lock',None), \
                    patch.object(self.observer,'_drain_buffer',side_effect=AssertionError('disabled drain')):
                self.observer.close_events()
        finally:self.observer.ENABLED=True
        self.assertEqual(self.events(),[]);self.assertFalse(self.observer._closed)

    def test_actor_snapshot_keeps_session_and_source_identity_without_reading_payload(self):
        ipc,actor,base=self.actor()
        value=dict(session_id=4,ipc=dict(owner_uid='b'*32,native_feedback=dict(engine_uid='a'*32,
            sequence=9,observed_at_s=2.,running=3,waiting=0,kv_usage=.2,prompt=fixtures.SECRET)))
        with patch.object(base,'snapshot',return_value=value,create=True):
            self.assertIs(actor.snapshot(),value)
        self.observer.close_events();event=self.events()[0]
        self.assertEqual((event['session_id'],event['source_sequence'],event['engine_uid']),(4,9,'a'*32))
        self.assertIsNone(event['accepted']);self.assertEqual(event['reason'],'returned_before_Ray_delivery')
        self.assertNotIn(fixtures.SECRET,json.dumps(self.events()))

    def test_feedback_observation_clock_failure_preserves_original_receive_result(self):
        ipc=fixtures.load('record_ipc_observer');base=sys.modules['src.serving.backends.async_engine_process'].AsyncEngineProcess
        process=ipc.ObservedAsyncEngineProcess();process.ready_report={'engine_uid':'a'*32};process._owner_uid='b'*32
        value=dict(op='feedback',snapshot=dict(engine_uid='a'*32,sequence=1,observed_at_s=1.))
        def receive(instance,packet,**kw):instance._native_feedback=packet['snapshot'];return packet
        with patch.object(base,'_receive',receive,create=True), \
                patch.object(ipc.time,'monotonic',side_effect=RuntimeError('CPU clock failure')):
            self.assertIs(process._receive(value,results=False),value)
        self.observer.close_events()
        event=next(e for e in self.events()if e['event']=='native_feedback_frame')
        self.assertIsNone(event['received_at_s']);self.assertTrue(event['accepted'])

    def test_batch_pages_and_lost_ACK_replay_mark_each_member_once(self):
        ipc,actor,base=self.actor();group=(1,7);first=(1,0);second=(1,1)
        identities={first:fixtures.IDENTITY,second:dict(fixtures.IDENTITY,call_id='second')}
        actor._held={key:SimpleNamespace(identity=value,batch_key=group,poll_exposed=False)
                     for key,value in identities.items()}
        returned=fixtures.SECRET.encode();calls=[];selected=[first]
        def poll(instance,key):
            calls.append(key)
            for member in selected:instance._held[member].poll_exposed=True
            return returned
        with patch.object(base,'poll_prepared_batch',poll,create=True):
            self.assertIs(actor.poll_prepared_batch(group),returned)
            self.assertIs(actor.poll_prepared_batch(group),returned)  # Lost RPC/ACK, same page.
            selected[:]=[second]
            self.assertIs(actor.poll_prepared_batch(group),returned)
        markers=[e for e in self.events()if e['event']=='actor_result_return']
        self.assertEqual(len(calls),3);self.assertEqual(len(markers),2)
        self.assertEqual([e['key']['sequence']for e in markers],[0,1])
        self.assertTrue(all(e['batch_key']==dict(session_id=1,sequence=7)for e in markers))
        pages=[e for e in self.events()if e['event']=='actor_batch_poll_return']
        self.assertEqual([len(e['members'])for e in pages],[1,0,1])
        self.assertTrue(all(e['members_complete']is False for e in pages))
        self.assertNotIn(fixtures.SECRET,json.dumps(self.events()))

    def test_batch_poll_native_exception_is_not_replaced_and_key_iterable_not_consumed(self):
        ipc,actor,base=self.actor();business=ValueError(fixtures.SECRET);key=iter([fixtures.SECRET])
        calls=[]
        def poll(instance,value):calls.append(value);raise business
        with patch.object(base,'poll_prepared_batch',poll,create=True):
            with self.assertRaises(ValueError)as caught:actor.poll_prepared_batch(key)
        self.assertIs(caught.exception,business);self.assertEqual(calls,[key])
        self.assertEqual(next(key),fixtures.SECRET);self.assertNotIn(fixtures.SECRET,json.dumps(self.events()))

    def test_batch_off_and_full_delegate_without_handoff_state_access(self):
        ipc,actor,base=self.actor();result=object();del actor._condition
        with patch.object(base,'poll_prepared_batch',return_value=result,create=True)as native:
            self.observer.ENABLED=False;self.assertIs(actor.poll_prepared_batch([1,7]),result)
            self.observer.ENABLED=True;self.observer.settings()['profile']='full'
            self.assertIs(actor.poll_prepared_batch([1,7]),result)
        self.assertEqual(native.call_count,2);self.assertEqual(self.events(),[])

    def test_batch_metadata_failure_does_not_replace_native_return(self):
        ipc,actor,base=self.actor();actor._held=None;value=object()
        with patch.object(base,'poll_prepared_batch',return_value=value,create=True)as native:
            self.assertIs(actor.poll_prepared_batch([1,7]),value)
        self.assertEqual(native.call_count,1)
        self.assertTrue(any(e['event']=='handoff_probe_unknown'for e in self.events()))
        self.assertFalse(any(e['event']=='actor_result_return'for e in self.events()))

    def test_closed_batch_receipts_remain_observable_until_last_native_ACK(self):
        ipc,actor,base=self.actor();key=(1,0);group=(1,7)
        report=dict(status='closed',producer_quiescence=dict(schema='semloom.async_actor_quiescence.v1',
            confirmed=True,input_gate_closed=True,active_readers=0),unconsumed_core_results=[
                dict(request_key=list(key),batch_key=list(group),result=fixtures.IDENTITY)])
        actor.last_close_report=report;actor._closed_pending={key:False}
        with patch.object(base,'close',return_value=report,create=True):self.assertIs(actor.close(1),report)
        self.assertFalse(self.observer._closed)
        value=object()
        def poll(instance,group):instance._closed_pending[key]=True;return value
        def ack(instance,request):return instance._closed_pending.pop(tuple(request),False)
        with patch.object(base,'poll_prepared_batch',poll,create=True),patch.object(base,'acknowledge_core',ack,create=True):
            self.assertIs(actor.poll_prepared_batch(group),value)
            self.assertIs(actor.poll_prepared_batch(group),value)
            self.assertTrue(actor.acknowledge_core(key));self.assertTrue(self.observer._closed)
            self.assertFalse(actor.acknowledge_core(key))
        self.assertEqual(sum(e['event']=='actor_result_return'for e in self.events()),1)
        self.assertEqual(sum(e['event']=='actor_sink_finalizing'for e in self.events()),1)

    def test_batch_sink_and_clock_failures_preserve_native_result(self):
        ipc,actor,base=self.actor();value=object();calls=[]
        def poll(instance,group):calls.append(group);return value
        with patch.object(base,'poll_prepared_batch',poll,create=True):
            with patch.object(self.observer.os,'write',side_effect=OSError('CPU sink failure')):
                self.assertIs(actor.poll_prepared_batch([1,7]),value)
            with patch.object(self.observer.time,'monotonic_ns',side_effect=RuntimeError('CPU clock failure')):
                self.assertIs(actor.poll_prepared_batch([1,7]),value)
        self.assertEqual(calls,[[1,7],[1,7]]);self.assertGreaterEqual(self.observer._errors,2)

    def test_batch_future_notification_never_reads_payload_or_counts_members(self):
        future=Future();future.set_result(fixtures.SECRET)
        ref=SimpleNamespace(future=lambda:future)
        with patch.object(future,'result',side_effect=AssertionError('observer consumed RPC')):
            self.observer.observe_ref_ready(ref,None,None,batch_key=dict(session_id=1,sequence=7))
        event=self.events()[-1];self.assertEqual(event['event'],'driver_batch_future_done')
        self.assertIsNone(event['key']);self.assertIsNone(event['identities'])
        self.assertEqual(event['batch_key'],dict(session_id=1,sequence=7))
        self.assertNotIn(fixtures.SECRET,json.dumps(self.events()))

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
