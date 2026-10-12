"""Fault injection for the opt-in verifier; no authors, processes or HTTP start."""
import importlib.util
import json
from pathlib import Path
import signal
from types import SimpleNamespace
import tempfile
import threading
import unittest
from unittest.mock import Mock, MagicMock, patch
from src.baselines.text.frameworks.native_calls import NativeCall,NativeCallOutcome


_PATH=Path(__file__).resolve().parents[2]/'integrations/sema/verify_imlane_async_calls.py'
_SPEC=importlib.util.spec_from_file_location('imlane_async_verifier_fixture',_PATH)
verifier=importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(verifier)


class IMLaneAsyncVerifierTests(unittest.TestCase):
    def scope(self):
        temporary=tempfile.TemporaryDirectory(prefix='imlane-verifier-fault-')
        self.addCleanup(temporary.cleanup)
        args=SimpleNamespace(output=Path(temporary.name),scopes=[],
            stop_budget=verifier.StopBudget(threading.Event()))
        helper=SimpleNamespace(resources=Mock(return_value={}))
        report={'cleanup':[]}
        scope=verifier.AuthorScope(args,'ray',helper,report)
        scope.snapshot=Mock();scope.stop_survivors=Mock(return_value=([],[],[]))
        scope.service=SimpleNamespace(pid=101);scope.owner=SimpleNamespace(pid=102)
        scope.stop_root=Mock(return_value={'returncode':0,'forced':False})
        return scope,report

    def test_cleanup_continues_after_executor_and_coordinator_failures(self):
        scope,report=self.scope()
        first=SimpleNamespace(close=Mock(side_effect=RuntimeError('first close')))
        second=SimpleNamespace(close=Mock())
        scope.executors=[first,second]
        scope.stop_root.side_effect=[OSError('coordinator stop'),{'returncode':0,'forced':False}]
        with self.assertRaisesRegex(RuntimeError,'owned author cleanup failed'):scope.close()
        second.close.assert_called_once()
        self.assertEqual(scope.stop_root.call_count,2)
        scope.stop_survivors.assert_called_once()
        scope.helper.resources.assert_called_once_with(scope.namespace)
        self.assertTrue(scope.stop_sampler.is_set())
        cleanup,=report['cleanup']
        self.assertEqual([row['action'] for row in cleanup['errors']],['executor-0','coordinator'])
        self.assertEqual(cleanup['errors'][0]['error'],'first close')
        self.assertTrue((scope.root/'cleanup-001.json').is_file())
        self.assertFalse(cleanup['passed'])
        scope.close();self.assertEqual(scope.stop_root.call_count,2)

    def test_survivor_force_cleanup_retains_failure_despite_no_final_survivors(self):
        scope,report=self.scope();scope.stop_survivors.return_value=([(201,456)],[],[])
        with self.assertRaises(RuntimeError):scope.close()
        cleanup,=report['cleanup']
        self.assertEqual(cleanup['owned_survivors'],[])
        self.assertEqual(cleanup['forced_identities'],[{'pid':201,'start_ticks':456}])
        self.assertFalse(cleanup['passed'])

    def test_shared_memory_or_sampler_failure_is_not_clean(self):
        for resources in ({'owned':64},None):
            with self.subTest(resources=resources):
                scope,report=self.scope()
                scope.helper.resources.return_value=resources
                with self.assertRaises(RuntimeError):scope.close()
                self.assertFalse(report['cleanup'][0]['passed'])

    def test_deadline_or_term_during_cleanup_stops_work_and_finishes_all_teardown(self):
        for number in (signal.SIGALRM,signal.SIGTERM):
            with self.subTest(number=number):
                scope,report=self.scope()
                scope.executors=[SimpleNamespace(close=lambda:scope.args.stop_budget.signal(number,None))]
                scope.close()
                self.assertEqual(scope.stop_root.call_count,2)
                self.assertTrue(scope.args.stop_budget.stopped.is_set())
                self.assertEqual(scope.args.stop_budget.reason,signal.Signals(number).name)
                with self.assertRaises(TimeoutError):scope.args.stop_budget.check()
                self.assertEqual(scope.args.stop_budget.cleanup_depth,0)
                self.assertTrue(report['cleanup'][0]['passed'])

    def test_term_during_work_raises_and_already_stopped_scope_never_starts(self):
        scope,_report=self.scope()
        with self.assertRaises(TimeoutError):scope.args.stop_budget.signal(signal.SIGTERM,None)
        with patch.object(verifier.subprocess,'check_output') as probe:
            with self.assertRaises(TimeoutError):scope.__enter__()
        probe.assert_not_called()

    def test_reused_root_pid_is_not_walked_and_identity_pairs_are_preserved(self):
        scope,_report=self.scope();del scope.snapshot
        scope.roots={101:10};scope.known={(101,10)}
        with patch.object(verifier,'process_start_ticks',return_value=99), \
             patch('psutil.Process') as process:
            scope.snapshot()
        process.assert_not_called();self.assertEqual(scope.known,{(101,10)})
        child=SimpleNamespace(pid=201,is_running=lambda:True)
        root=SimpleNamespace(pid=101,children=lambda **_options:[child],is_running=lambda:True)
        with patch.object(verifier,'process_start_ticks',side_effect=lambda pid:10 if pid==101 else 30), \
             patch('psutil.Process',return_value=root):scope.snapshot()
        self.assertEqual(scope.known,{(101,10),(201,30)})

    def test_survivor_check_and_signal_never_target_reused_identity(self):
        scope,_report=self.scope();scope.known={(101,10)}
        with patch.object(verifier,'process_start_ticks',return_value=99), \
             patch('psutil.Process',return_value=Mock()),patch.object(verifier.os,'kill') as kill:
            self.assertEqual(scope.survivors(),[])
            scope.signal_identity((101,10),signal.SIGKILL)
        kill.assert_not_called()

    def test_owned_orphans_are_attempted_term_then_kill_with_finite_waits(self):
        scope,_report=self.scope();del scope.stop_survivors
        identity=(201,30)
        scope.survivors=Mock(side_effect=[[identity],[identity],[]])
        scope.signal_identity=Mock(return_value=None)
        with patch.object(verifier.time,'monotonic',side_effect=[0,3,4,7]):
            forced,remaining,errors=scope.stop_survivors()
        self.assertEqual(forced,[identity]);self.assertEqual(remaining,[])
        self.assertEqual(errors,[])
        self.assertEqual(scope.signal_identity.call_args_list[0].args,(identity,signal.SIGTERM))
        self.assertEqual(scope.signal_identity.call_args_list[1].args,(identity,signal.SIGKILL))

    def test_cleanup_failure_note_keeps_primary_query_error(self):
        scope,_report=self.scope()
        scope.close=Mock(side_effect=RuntimeError('cleanup failed'))
        primary=ValueError('first query error')
        scope.__exit__(ValueError,primary,None)
        self.assertEqual(str(primary),'first query error')
        self.assertEqual(primary.__notes__,['Author cleanup also failed: RuntimeError'])

    def test_without_execute_main_stops_before_any_runtime_probe(self):
        with patch.object(verifier.subprocess,'check_output') as probe:
            with self.assertRaisesRegex(ValueError,'explicit --execute'):
                verifier.main(['--author-root','/tmp/unused-author','--bridge-binary','/tmp/unused-bridge',
                               '--output','/tmp/unused-output','--source-manifest','/tmp/unused-manifest'])
        probe.assert_not_called()

    def test_post_allowance_refuses_over_budget_before_submission(self):
        allowance=verifier.PostAllowance(12,threading.Event(),Mock())
        call=NativeCall('normal',1,b'{}','http://localhost/ok',(),1.)
        allowance.reserve([call]*4)
        with self.assertRaisesRegex(ValueError,'before author submission'):allowance.reserve([call])
        self.assertEqual(allowance.reserved,4)

    def test_unexpected_or_extra_post_aborts_owned_author_immediately(self):
        for unexpected in (True,False):
            allowance=verifier.PostAllowance(384,threading.Event(),Mock())
            call=NativeCall('normal',1,b'{}','http://localhost/ok',(),1.)
            allowance.reserve([call])
            row=dict(method='POST',call_id='unknown' if unexpected else 'normal',
                     payload_sha256=verifier.hashlib.sha256(b'{}').hexdigest())
            allowance.receipt(row,1)
            if not unexpected:allowance.receipt(row,2)
            self.assertTrue(allowance.stopped.is_set());allowance.abort.assert_called_once()
            self.assertEqual(len(allowance.violations),1)

    def test_redirect_post_reservation_counts_both_original_bodies(self):
        allowance=verifier.PostAllowance(384,threading.Event(),Mock())
        call=NativeCall('redirect',1,b'{}','http://localhost/cookie-307',(),1.,follow_redirects=True)
        allowance.reserve([call])
        row=dict(method='POST',call_id='redirect',payload_sha256=verifier.hashlib.sha256(b'{}').hexdigest())
        allowance.receipt(row,1);allowance.receipt(row,2)
        self.assertEqual(allowance.reserved,2);self.assertEqual(allowance.available['redirect'],0)
        allowance.abort.assert_not_called()

    def test_one_identity_signal_failure_does_not_skip_remaining_identities(self):
        scope,_report=self.scope();scope.survivors=Mock(return_value=[(201,30),(202,40)])
        with patch.object(verifier,'process_start_ticks',side_effect=lambda pid:30 if pid==201 else 40), \
             patch.object(verifier.os,'kill',side_effect=[PermissionError('signal fixture'),None]) as kill:
            errors=scope.abort_owned()
        self.assertEqual(kill.call_count,2)
        self.assertEqual(errors[0]['pid'],201)

    def test_unsettled_cleanup_retries_with_distinct_attempt_evidence(self):
        scope,report=self.scope();scope.stop_survivors.return_value=([(201,30)],[(201,30)],[])
        with self.assertRaises(RuntimeError):scope.close()
        self.assertFalse(scope.closed)
        scope.stop_survivors.return_value=([],[],[]);scope.close()
        self.assertTrue(scope.closed)
        self.assertEqual([v['passed'] for v in report['cleanup']],[False,True])
        self.assertTrue((scope.root/'cleanup-001.json').is_file())
        self.assertTrue((scope.root/'cleanup-002.json').is_file())

    def test_startup_error_remains_first_when_cleanup_also_fails(self):
        scope,_report=self.scope();scope.mode='cpython';scope.args.author_root=Path('/tmp/unused-author')
        scope.helper.free_port=Mock(return_value=12345)
        scope.helper.spawn=Mock(side_effect=ValueError('first startup'))
        scope.close=Mock(side_effect=RuntimeError('later cleanup'))
        info=json.dumps(dict(site='/tmp/unused-site',lib='/tmp/unused-lib',base='/tmp/unused-base'))
        with patch.object(verifier.subprocess,'check_output',side_effect=[info,'/tmp/libstdc++.so.6']), \
             patch.object(verifier.threading,'Thread'):
            with self.assertRaisesRegex(ValueError,'first startup') as raised:scope.__enter__()
        self.assertEqual(raised.exception.__notes__,['Author startup cleanup also failed: RuntimeError'])

    def test_q1_payload_budget_keeps_duplicate_body_multiplicity(self):
        allowance=verifier.PostAllowance(4096,threading.Event(),Mock(),payload_identity=True)
        calls=[NativeCall(str(i),i,b'{}','http://localhost/v1/chat/completions',(),1.) for i in range(3)]
        allowance.reserve(calls)
        row=dict(method='POST',call_id=None,payload_sha256=verifier.hashlib.sha256(b'{}').hexdigest())
        for index in range(3):allowance.receipt(row,index+1)
        allowance.abort.assert_not_called();self.assertEqual(allowance.reserved,3)
        allowance.receipt(row,4);allowance.abort.assert_called_once()

    def test_owned_shm_fallback_preserves_forced_failure_and_unlinks_only_own_name(self):
        scope,report=self.scope();name=scope.namespace+'-lane'
        scope.helper.resources.side_effect=[{name:64},{}]
        with patch.object(Path,'unlink') as unlink:
            with self.assertRaises(RuntimeError):scope.close()
        unlink.assert_called_once_with(missing_ok=True)
        cleanup,=report['cleanup']
        self.assertEqual(cleanup['shared_memory_before_fallback'],{name:64})
        self.assertEqual(cleanup['forced_shared_memory'],[name]);self.assertEqual(cleanup['remaining_shm'],{})
        self.assertFalse(cleanup['passed']);self.assertTrue(scope.closed)

    def test_owned_shm_is_preserved_until_all_producers_exit(self):
        scope,report=self.scope();name=scope.namespace+'-lane'
        scope.helper.resources.return_value={name:64}
        scope.stop_survivors.return_value=([(201,30)],[(201,30)],[])
        with patch.object(Path,'unlink') as unlink:
            with self.assertRaises(RuntimeError):scope.close()
        unlink.assert_not_called();self.assertFalse(scope.closed)
        self.assertEqual(report['cleanup'][0]['remaining_shm'],{name:64})

    def test_q1_minimum_allowance_accepts_its_exact_planned_4000_posts(self):
        allowance=verifier.PostAllowance(4008,threading.Event(),Mock(),payload_identity=True)
        call=NativeCall('normal',1,b'{}','http://localhost/v1/chat/completions',(),1.)
        allowance.reserve([call]*4000)
        row=dict(method='POST',call_id=None,payload_sha256=verifier.hashlib.sha256(b'{}').hexdigest())
        for count in range(1,4001):allowance.receipt(row,count)
        allowance.abort.assert_not_called();self.assertFalse(allowance.stopped.is_set())

    def test_normal_matrix_rejects_unexpected_transport_outcome(self):
        empty=SimpleNamespace(iter_batch=Mock(side_effect=InterruptedError()))
        bad=SimpleNamespace(iter_batch=lambda values,**_kw:iter(NativeCallOutcome(c.call_id,c.row_id,None,'TimeoutError: fixture') for c in values),last_close_report={'clean':True})
        scope=SimpleNamespace(executor=Mock(side_effect=[empty,bad]))
        context=MagicMock();context.__enter__.return_value=scope
        fixture=SimpleNamespace(base='http://localhost',posts=0,receipts=[],allowance=SimpleNamespace(reserve=Mock()))
        with patch.object(verifier,'AuthorScope',return_value=context):
            with self.assertRaisesRegex(AssertionError,'unexpected native transport outcome'):
                verifier.run_mode(None,'cpython',None,fixture,threading.Event(),{})

    def test_q1_ready_marker_is_published_only_after_complete_private_json(self):
        spec=importlib.util.spec_from_file_location('q1_client_fixture',_PATH.with_name('imlane_async_q1_client.py'))
        client=importlib.util.module_from_spec(spec);spec.loader.exec_module(client)
        with tempfile.TemporaryDirectory(prefix='imlane-q1-publish-') as directory:
            root=Path(directory);ready=root/'prepared-ready.json';value={'rows':2000}
            original=client.write_private_json
            def observe(path,record):
                self.assertFalse(ready.exists());original(path,record);self.assertFalse(ready.exists())
            with patch.object(client,'write_private_json',observe):client.publish_prepared(root,value)
            self.assertEqual(json.loads(ready.read_text()),value)
            self.assertFalse((root/'prepared-ready.tmp').exists())

    def test_journal_failure_still_checks_budget_then_aborts_and_retains_error(self):
        fixture=object.__new__(verifier.Fixture);events=[]
        fixture.allowance=SimpleNamespace(receipt=lambda *_args:events.append('budget'))
        def failing_write(_value):events.append('journal');raise OSError('journal fixture')
        fixture.journal=SimpleNamespace(write=failing_write);fixture.journal_errors=[]
        fixture.lock=threading.Lock();fixture.stopped=threading.Event();fixture.abort=lambda:events.append('abort')
        self.assertFalse(fixture.accept_receipt({'method':'POST'},1))
        self.assertEqual(events,['budget','journal','abort']);self.assertTrue(fixture.stopped.is_set())
        self.assertEqual(fixture.journal_errors,[{'type':'OSError','error':'journal fixture'}])


if __name__=='__main__':unittest.main()
