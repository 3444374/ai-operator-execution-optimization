"""Actual records preparation/owner close; Ray and formatting are CPU fixtures."""
from contextlib import ExitStack
import json
import os
import subprocess
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_actor_owner as owners
from src.semantic_methods.lotus import engine as methods
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.experiments.test_record_resource_capture import load, SECRET


class PrepareFinalizationTests(unittest.TestCase):
    def setUp(self):
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.root=Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        (self.root/'config.json').write_text(json.dumps(dict(observation=dict(mode='on',profile='full',
            max_events_per_process=100,max_event_bytes=65536,max_pending_frame_metadata=4))))
        self.stack.enter_context(patch.dict(os.environ,dict(SEM_RECORD_OBSERVATION='on',SEM_RECORD_OBSERVER_ROOT=str(self.root))))
        saved={k:sys.modules.get(k)for k in ('record_observer','record_ipc_observer')}
        def restore_modules():
            for k,v in saved.items():
                if v is None:sys.modules.pop(k,None)
                else:sys.modules[k]=v
        self.stack.callback(restore_modules)
        self.observer=load('record_observer');self.stack.callback(self.observer.close_events)
        self.ipc=load('record_ipc_observer')
        self.stack.enter_context(patch.object(self.observer,'install_cpu_hooks',lambda:None))
        test=self
        class Formatter:
            def __init__(self,method):pass
            def prepare_rows(self,rows):return test.format(rows)
        self.observer.observe_method(Formatter,'prepare_rows','ordinary_prepare',lambda self,rows,args,kw:None)
        self.stack.enter_context(patch.object(methods,'LotusFilterMethod',Formatter))
        self.rows=[dict(identity('q','row',0),reviewText=SECRET)]
        self.result=[dict(identity('q','row',0),messages_json=SECRET)]
        self.format=lambda rows:self.result
        self.actor=self.ipc.ObservedPrepareActor(dict(model_name='fixture',instruction='Keep {reviewText}'))
        self.events=[];self.refs=[];self.dispatch_error=None;self.kill_error=None
        class Method:
            def remote(unused):
                test.events.append('close_rpc')
                if test.dispatch_error:raise test.dispatch_error
                ref=SimpleNamespace(resolve=test.actor.close_observation);test.refs.append(ref);return ref
        self.handle=SimpleNamespace(close_observation=Method())
        self.owner=owners.RecordsActorOwner(FilterMethod('fixture',instruction='Keep {reviewText}'),
            EngineSpec('/tmp/fixture',sys.executable,sys.executable),GenerationSpec(),RecordsResources())
        self.owner.prepare_actors=[self.handle]
        def kill(actor,**kwargs):
            test.events.append(('kill',test.footer_path().exists()))
            if test.kill_error:raise test.kill_error
        self.owner.ray=SimpleNamespace(kill=kill,wait=lambda refs,**kwargs:(refs,[]),get=lambda ref:ref.resolve())
        for name in ('close','_kill_actor'):
            if hasattr(owners.RecordsActorOwner,name):self.stack.enter_context(patch.object(owners.RecordsActorOwner,name,getattr(owners.RecordsActorOwner,name)))

    def install(self):
        hook=getattr(self.ipc,'install_prepare_close',None)
        if hook is not None:hook(owners.RecordsActorOwner)

    def footer_path(self):return self.root/'events'/('events-'+str(os.getpid())+'.status.json')

    def footer(self):return json.loads(self.footer_path().read_text())

    def prepare(self):return self.actor.prepare(self.rows,time.monotonic()+5)

    def test_actual_owner_flushes_prepare_footer_before_kill_and_repeats_no_RPC(self):
        self.install();self.assertIs(self.prepare(),self.result)
        report=self.owner.close();self.assertTrue(self.footer_path().exists(),'owner killed preparation actor without its final sink receipt')
        receipt=report['prepare_observation'][0]
        self.assertEqual(receipt['status'],'closed');self.assertEqual(receipt['rpc_terminal_status'],'returned')
        self.assertFalse(receipt['ref_retained']);self.assertTrue(receipt['sink']['flush_confirmed'])
        self.assertEqual(self.events,['close_rpc',('kill',True)])
        before=self.footer_path().read_bytes();self.assertEqual(report['status'],'closed')
        again=self.owner.close();self.assertEqual(again['prepare_observation'],report['prepare_observation'])
        self.assertEqual(self.footer_path().read_bytes(),before);self.assertEqual(len(self.refs),1)
        self.assertEqual(self.footer()['dropped'],0)
        self.assertNotIn(SECRET,json.dumps(report)+self.footer_path().read_text())

    def test_empty_EOF_still_has_explicit_prepare_role_footer(self):
        self.install();report=self.owner.close()
        self.assertEqual(report['prepare_observation'][0]['status'],'closed')
        self.assertTrue(self.footer()['flush_confirmed'])

    def test_preparation_error_or_cancel_is_unchanged_and_owner_still_closes(self):
        self.install();error=RuntimeError(SECRET)
        def fail(rows):raise error
        self.format=fail
        with self.assertRaises(RuntimeError)as caught:self.prepare()
        self.assertIs(caught.exception,error)
        report=self.owner.close();self.assertEqual(report['prepare_observation'][0]['status'],'closed')
        self.assertEqual(self.events[-1],('kill',True));self.assertEqual(self.footer()['errors'],0)

    def test_expired_preparation_still_finalizes_without_formatting(self):
        self.install()
        with patch.object(self.actor.method,'prepare_rows')as format:
            with self.assertRaises(TimeoutError):self.actor.prepare(self.rows,time.monotonic()-1)
        format.assert_not_called()
        self.assertEqual(self.owner.close()['prepare_observation'][0]['status'],'closed')

    def test_close_waits_for_active_prepare_and_late_prepare_keeps_result_or_error(self):
        self.install();entered,release,closing=[threading.Event()for _ in range(3)]
        results=[];errors=[]
        def format(rows):entered.set();self.assertTrue(release.wait(5));return self.result
        self.format=format
        def prepare():
            try:results.append(self.prepare())
            except BaseException as error:errors.append(error)
        def close():
            closing.set()
            try:results.append(self.actor.close_observation())
            except BaseException as error:errors.append(error)
        first=threading.Thread(target=prepare);second=threading.Thread(target=close)
        first.start()
        try:
            self.assertTrue(entered.wait(5));second.start();self.assertTrue(closing.wait(5))
            self.assertFalse(self.footer_path().exists())
        finally:
            release.set();first.join(5)
            if second.ident is not None:second.join(5)
        self.assertFalse(first.is_alive()or second.is_alive());self.assertEqual(errors,[])
        self.assertIs(results[0],self.result);self.assertEqual(results[1]['status'],'closed')
        before=self.footer_path().read_bytes();attempts=self.observer._attempts
        self.format=lambda rows:self.result;self.assertIs(self.prepare(),self.result)
        error=ValueError(SECRET)
        def fail(rows):raise error
        self.format=fail
        with self.assertRaises(ValueError)as caught:self.prepare()
        self.assertIs(caught.exception,error)
        self.assertEqual(self.observer._attempts,attempts);self.assertEqual(self.footer_path().read_bytes(),before)

    def test_failed_fsync_remains_failed_on_repeated_close_and_kill_proceeds(self):
        self.install();self.prepare()
        with patch.object(self.observer.os,'fsync',side_effect=OSError(SECRET))as flush:
            report=self.owner.close();again=self.owner.close()
        receipt=report['prepare_observation'][0]
        self.assertEqual(receipt['status'],'unknown');self.assertFalse(receipt['sink']['flush_confirmed'])
        self.assertEqual(receipt['rpc_terminal_status'],'returned')
        self.assertEqual(again['prepare_observation'],report['prepare_observation']);self.assertEqual(flush.call_count,1)
        self.assertEqual(self.events[-1],('kill',True));self.assertEqual(report['status'],'closed')

    def test_concurrent_prepare_closers_share_one_flush_and_same_receipt(self):
        self.install();self.prepare();paused,release,second_started=[threading.Event()for _ in range(3)]
        receipts=[];errors=[];original=self.observer.os.fsync
        def fsync(fd):paused.set();self.assertTrue(release.wait(5));return original(fd)
        def close(second=False):
            if second:second_started.set()
            try:receipts.append(self.actor.close_observation())
            except BaseException as error:errors.append(error)
        first=threading.Thread(target=close);second=threading.Thread(target=close,args=(True,))
        with patch.object(self.observer.os,'fsync',side_effect=fsync)as flush:
            first.start()
            try:
                self.assertTrue(paused.wait(5));second.start();self.assertTrue(second_started.wait(5))
                self.assertEqual(receipts,[]);self.assertFalse(self.footer_path().exists())
            finally:
                release.set();first.join(5)
                if second.ident is not None:second.join(5)
            self.assertEqual(flush.call_count,1)
        self.assertFalse(first.is_alive()or second.is_alive());self.assertEqual(errors,[])
        self.assertEqual(len(receipts),2);self.assertEqual(receipts[0],receipts[1]);self.assertEqual(receipts[0]['status'],'closed')

    def test_malformed_closed_RPC_is_returned_but_cannot_confirm_a_sink(self):
        self.install()
        with patch.object(self.actor,'close_observation',return_value=dict(role='prepare',status='closed')):
            report=self.owner.close()
        receipt=report['prepare_observation'][0]
        self.assertEqual(receipt['status'],'unknown');self.assertEqual(receipt['rpc_terminal_status'],'returned')
        self.assertFalse(receipt['ref_retained']);self.assertEqual(self.events[-1],('kill',False))

    def test_no_prepare_owner_and_legacy_owner_have_no_observation_RPC(self):
        self.install();self.owner.prepare_actors=[]
        self.assertNotIn('prepare_observation',self.owner.close());self.assertEqual(self.events,[])
        class Legacy:
            def close(self):return {'status':'closed'}
        original=Legacy.close;self.ipc.install_prepare_close(Legacy)
        self.assertIs(Legacy.close,original)

    def test_missing_footer_is_unknown_but_does_not_skip_original_kill(self):
        self.install();self.prepare()
        with patch.object(self.ipc,'close_events',lambda:None):report=self.owner.close()
        receipt=report['prepare_observation'][0]
        self.assertEqual(receipt['status'],'unknown');self.assertEqual(receipt['rpc_terminal_status'],'returned')
        self.assertEqual(self.events[-1],('kill',False));self.assertEqual(report['status'],'closed')

    def test_pending_close_retains_unique_ref_and_late_reply_is_not_fabricated_return(self):
        self.install();self.prepare()
        with patch.object(self.owner,'wait_ready',side_effect=TimeoutError(SECRET)):
            report=self.owner.close()
        receipt=report['prepare_observation'][0]
        self.assertEqual(receipt['status'],'unknown');self.assertEqual(receipt['rpc_terminal_status'],'unknown')
        self.assertTrue(receipt['ref_retained']);self.assertEqual(self.events[-1],('kill',False))
        self.refs[0].resolve()  # A late fixture reply does not rewrite an uncollected RPC receipt.
        self.assertEqual(self.owner.close()['prepare_observation'],report['prepare_observation'])
        self.assertEqual(len(self.refs),1)

    def test_dispatch_error_and_failed_kill_do_not_duplicate_unknown_submission(self):
        self.install();self.prepare();self.dispatch_error=RuntimeError(SECRET);self.kill_error=OSError(SECRET)
        first=self.owner.close();self.assertEqual(first['status'],'unknown')
        self.assertEqual(first['prepare_observation'][0]['rpc_terminal_status'],'unknown')
        self.kill_error=None;second=self.owner.close()
        self.assertEqual(self.events.count('close_rpc'),1)
        self.assertEqual(second['prepare_observation'],first['prepare_observation'])
        self.assertEqual(second['status'],'closed');self.assertNotIn(SECRET,json.dumps(second))

    def test_expired_owner_deadline_does_not_dispatch_a_new_RPC(self):
        self.install();self.owner.close_deadline=time.monotonic()-1
        report=self.owner.close();receipt=report['prepare_observation'][0]
        self.assertEqual(receipt['status'],'unknown');self.assertEqual(receipt['rpc_terminal_status'],'unsubmitted')
        self.assertEqual(self.events,[('kill',False)])

    def test_disabled_observation_keeps_owner_class_and_no_extra_RPC(self):
        before=(owners.RecordsActorOwner.close,owners.RecordsActorOwner._kill_actor)
        self.observer.ENABLED=self.ipc.ENABLED=False;self.install()
        self.assertEqual((owners.RecordsActorOwner.close,owners.RecordsActorOwner._kill_actor),before)
        report=self.owner.close();self.assertNotIn('prepare_observation',report)
        self.assertEqual(self.events,[('kill',False)])


class NormalPrepareEntryTests(unittest.TestCase):
    def test_original_records_isolated_session_on_off_and_prepare_failure(self):
        script=Path(__file__).with_name('probe_record_prepare_finalization.py')
        public=Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);reports=[]
            for scenario in ('off','on','prepare_error','prepare_error_off'):
                result=subprocess.run([sys.executable,'-B',str(script),str(public),str(root/scenario),scenario],
                    text=True,capture_output=True,timeout=90)
                self.assertEqual(result.returncode,0,result.stdout+'\n'+result.stderr)
                reports.append(json.loads((root/scenario/'PUBLIC-PREPARE.json').read_text()))
            self.assertEqual(reports[0]['business_sha256'],reports[1]['business_sha256'])
            self.assertEqual([r['prepare_close_RPCS']for r in reports],[0,1,1,0])
            self.assertTrue(reports[1]['footer_before_kill'])
            self.assertTrue(reports[2]['original_failure_preserved'])
            self.assertTrue(reports[3]['original_failure_preserved'])
            self.assertEqual(reports[2]['query_failure_type'],reports[3]['query_failure_type'])
            self.assertEqual(reports[2]['outer_close_status'],reports[3]['outer_close_status'])


if __name__=='__main__':unittest.main()
