"""Thin verifier protocol substitutes; no author Coordinator, Ray, model or server."""
import hashlib
import importlib.util
import json
import subprocess
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock,Mock,patch

from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig,IMLaneCallExecutor
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions

CODE=Path(__file__).resolve().parents[2]
SPEC=importlib.util.spec_from_file_location('session_verifier_tests',CODE/'integrations/sema/verify_imlane_session.py')
verifier=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(verifier)

PROTOCOL=r'''
import asyncio,json,sys
from pathlib import Path
sys.path.insert(0,CODE)
from src.baselines.text.frameworks.native_calls import _call_from_wire,_request,_outcome_wire
from src.execution_provider.adapters.full_response import encode_full_response
session='--session' in sys.argv
names=['max_batch_rows','max_input_payload_bytes','max_input_wire_bytes','max_item_payload_bytes','max_pending_calls','max_batch_response_bytes']
limits=dict(zip(names,map(int,sys.argv[4:10])))
def emit(tag,value):print('SEMA_IMLANE_'+tag+' '+json.dumps(value),flush=True)
emit('META',dict(native_executors=2,native_task_threads=4));emit('LIMITS',limits)
if session:emit('SESSION',dict(protocol=1))
count=0
def batch(raw,identity=None):
 global count
 count+=1;calls=[_call_from_wire(line) for line in raw.splitlines()];total=0
 for call in reversed(calls):
  value=asyncio.run(_request(call))
  total+=len(encode_full_response(value.response)) if value.response else len(value.transport_error.encode())
  wire=json.loads(_outcome_wire(value))
  emit('RESULT',dict(batch_id=identity,outcome=wire) if session else wire)
 stats=dict(submitted=len(calls),delivered=len(calls),pending_peak=min(len(calls),limits['max_pending_calls']),
  input_payload_bytes=sum(len(c.payload) for c in calls),stdin_wire_bytes=len(raw),delivered_response_bytes=total,
  timing_ns={k:1 for k in ['input_read_parse_ns','input_arrow_ns','context_setup_ns','function_register_ns',
   'author_execute_inclusive_ns','output_copy_encode_ns','output_copy_decode_ns','output_wire_ns','stdout_write_ns','batch_wall_ns']},
  lifecycle=dict(context_creations=int(count==1),registrations=int(count==1),author_calls=len(calls),output_copies=len(calls)))
 if session:
  stats['session_batches']=count;emit('BATCH_END',dict(batch_id=identity,stats=stats))
 else:emit('STATS',stats)
if not session:batch(sys.stdin.buffer.read())
else:
 for line in sys.stdin:
  command=json.loads(line)
  if command['kind']=='close':emit('SESSION_END',dict(protocol=1));break
  batch(Path(command['input_path']).read_bytes(),command['batch_id'])
'''

class LocalScope:
    def __init__(self,root,mode,reuse,binary,gateway_port):
        self.root=root;root.mkdir();self.mode=mode;self.values=[];self.processes={}
        udf=CODE/'integrations/sema/imlane_complete_call.py'
        bridge=dict(author_commit=verifier.tools.AUTHOR,bridge_binary=str(binary),bridge_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
            udf_path=str(udf),udf_sha256=hashlib.sha256(udf.read_bytes()).hexdigest(),gateway_port=gateway_port,
            work_root=str(root/'batches'),namespace=root.name,native_task_threads=4)
        path=root/'bridge.json';path.write_text(json.dumps(bridge))
        self.config=IMLaneCallConfig(mode,str(path),123,456,str(root/'unused-author-build.json'),max_batch_rows=16,reuse_bridge=reuse)
    def executor(self):
        value=IMLaneCallExecutor(self.config,NativeGraphOptions(concurrency=4));self.values.append(value);return value
    def track_root(self,p):self.processes[p.pid]=p
    def signal_identity(self,identity,number):self.processes[identity[0]].send_signal(number)
    def close(self):
        for v in self.values:v.close()

class SessionVerifierTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='imlane-session-verifier-');self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name);self.binary=self.root/'protocol'
        self.binary.write_text('#!'+sys.executable+'\n'+PROTOCOL.replace('CODE',repr(str(CODE))))
        self.binary.chmod(0o700);self.sequence=0
        self.verification=patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator',return_value={})
        self.verification.start();self.addCleanup(self.verification.stop)
    def scope(self,mode,reuse,fixture):
        self.sequence+=1;s=LocalScope(self.root/('scope-'+str(self.sequence)),mode,reuse,self.binary,fixture.server.server_port)
        self.addCleanup(s.close);return s

    def test_three_healthy_batches_same_executor_cover_all_four_mode_labels(self):
        # Both runtime labels are protocol substitutes, never actual author qualification.
        for mode in ('cpython','ray'):
            for reuse in (False,True):
                with self.subTest(mode=mode,reuse=reuse):
                    fixture=verifier.tools.Fixture(96,threading.Event(),Mock(),response_factory=verifier.fixture_body)
                    self.addCleanup(fixture.close)
                    scope=self.scope(mode,reuse,fixture);result=verifier.healthy_batches(scope,fixture)
                    self.assertTrue(result['passed']);self.assertTrue(result['output_lifecycle'])
                    self.assertEqual(len(scope.values),1);self.assertEqual(fixture.posts,12)
                    self.assertEqual([r['bridge_stats']['lifecycle']['context_creations'] for r in result['batches']],
                                     [1,0,0] if reuse else [1,1,1])
                    self.assertEqual([r['received'] for r in result['batches']],[4,4,4])

    def test_sent_faults_quarantine_and_reject_new_owner_for_both_paths(self):
        for reuse in (False,True):
            for case in ('transport','timeout','consumer','cancel','crash'):
                with self.subTest(reuse=reuse,case=case):
                    fixture=verifier.tools.Fixture(96,threading.Event(),Mock(),response_factory=verifier.fixture_body)
                    self.addCleanup(fixture.close);scope=self.scope('cpython',reuse,fixture)
                    with patch.object(verifier.tools,'process_start_ticks',side_effect=lambda pid:pid):
                        result=verifier.fault_batch(scope,fixture,case)
                    self.assertTrue(result['passed']);self.assertTrue(result['new_owner_rejected'])
                    self.assertTrue(scope.values[0]._uncertain.exists())
                    self.assertFalse(result['close']['clean']);self.assertEqual(result['close']['remote_http_stopped'],'unavailable')
                    if case=='consumer':self.assertEqual(result['first_error']['error'],'fixture first consumer failure')

    def test_wrong_lifecycle_and_false_clean_are_rejected(self):
        for report in ({'clean':True,'bridge_stats':{}},{'clean':False}):
            with self.assertRaises(AssertionError):verifier.check_lifecycle(report,True,1,4)

    def test_no_execute_never_probes_or_starts_the_author_environment(self):
        argv=['--runtime','ray','--author-root','/tmp/unused','--bridge-binary','/tmp/unused',
            '--lotus-python','/tmp/unused','--tokenizer','/tmp/unused','--checkout','/tmp/unused',
            '--source-manifest','/tmp/unused','--output','/tmp/unused','--lease-file','/tmp/unused',
            '--lease-sha256','0'*64,'--cpu-ids','16,17,18,19,20,21,22,23']
        with patch.object(verifier,'SessionScope') as scope:
            with self.assertRaisesRegex(ValueError,'explicit --execute'):verifier.main(argv)
        scope.assert_not_called()

    def test_affinity_or_128_core_claim_never_becomes_the_host_quota(self):
        for affinity,quota in ((set(range(128)),16),(set(range(8)),128)):
            args=SimpleNamespace(execute=True,cpu_ids='0,1,2,3,4,5,6,7',cpu_quota_cores=quota)
            with patch.object(verifier.sys,'platform','linux'),patch.object(verifier.os,'sched_getaffinity',return_value=affinity,create=True):
                with self.assertRaisesRegex(ValueError,'owner-confirmed'):verifier.verify_inputs(args)

    def test_cell_preserves_first_failure_even_when_fixture_cleanup_fails(self):
        first=ValueError('first cell failure');args=SimpleNamespace(scopes=[],stop_budget=verifier.tools.StopBudget(threading.Event()))
        fixture=SimpleNamespace(posts=0,allowance=SimpleNamespace(reserved=0,violations=[]),journal_errors=[],
            connections=set(),closed_connections=set(),close=Mock(side_effect=OSError('fixture close')))
        scope=MagicMock();report={'cells':[]}
        with patch.object(verifier.tools,'Fixture',return_value=fixture),patch.object(verifier,'SessionScope',return_value=scope), \
             patch.object(verifier,'healthy_batches',side_effect=first):
            with self.assertRaises(ValueError) as observed:verifier.run_cell(args,'ray',True,None,report)
        self.assertIs(observed.exception,first);self.assertIn('Fixture cleanup also failed: OSError',first.__notes__)
        self.assertFalse(report['cells'][0]['passed']);self.assertEqual(report['cells'][0]['first_error']['error'],'first cell failure')

    def test_scope_options_fix_sync_udf_reuse_and_small_rows_without_changing_old_defaults(self):
        args=SimpleNamespace(output=self.root,scopes=[],stop_budget=verifier.tools.StopBudget(threading.Event()))
        normal=verifier.tools.AuthorScope(args,'cpython',SimpleNamespace(),{'cleanup':[]})
        scope=verifier.SessionScope(args,'ray',SimpleNamespace(),{'cleanup':[]},reuse_bridge=True)
        self.assertEqual(scope.kernel,verifier.KERNEL);self.assertTrue(scope.reuse_bridge);self.assertEqual(scope.max_batch_rows,16)
        self.assertEqual(normal.kernel,verifier.tools.KERNEL);self.assertFalse(normal.reuse_bridge);self.assertEqual(normal.max_batch_rows,2000)

    def test_resource_or_lease_failure_stops_only_tracked_scope_and_records_the_cause(self):
        for reason in ('rss','shm','lease','deadline'):
            with self.subTest(reason=reason):
                output=self.root/reason;output.mkdir()
                stop=threading.Event()
                args=SimpleNamespace(output=output,scopes=[],stop_budget=verifier.tools.StopBudget(stop),
                    resource_errors=[],cell_deadline=None,
                    check_lease=Mock(side_effect=RuntimeError('lease changed') if reason=='lease' else None))
                if reason=='deadline':args.cell_deadline=time.monotonic()-1
                helper=SimpleNamespace(resources=Mock(return_value={'own':3*1024**3} if reason=='shm' else {}))
                scope=verifier.SessionScope(args,'ray',helper,{'cleanup':[]},reuse_bridge=True)
                scope.known={(100,200)};scope.snapshot=Mock();scope.abort_owned=Mock()
                scope.stop_sampler=SimpleNamespace(wait=Mock(side_effect=[False,True]))
                proc=SimpleNamespace(memory_info=lambda:SimpleNamespace(rss=9*1024**3 if reason=='rss' else 1024))
                with patch('psutil.Process',return_value=proc),patch.object(verifier.tools,'process_start_ticks',return_value=200):
                    scope.sample()
                self.assertTrue(stop.is_set());scope.abort_owned.assert_called_once()
                self.assertEqual(len(args.resource_errors),1)
                self.assertIn('lease changed' if reason=='lease' else 'deadline' if reason=='deadline' else 'RSS/SHM',
                              args.resource_errors[0]['error'])

    def test_zero_wall_limit_disables_cell_deadline_and_elapsed_bound_only(self):
        for limit,expected_pass in ((0,True),(1200,False)):
            with self.subTest(limit=limit):
                args=SimpleNamespace(scopes=[],wall_limit_s=limit,check_lease=Mock(),
                    stop_budget=verifier.tools.StopBudget(threading.Event()))
                fixture=SimpleNamespace(posts=12,allowance=SimpleNamespace(reserved=12,violations=[]),journal_errors=[],
                    connections=set(),closed_connections=set(),close=Mock())
                report={'cells':[]}
                def sdk(_args,_scope,case):return {'fixture':{'actual_posts':28 if case=='healthy' else 4}}
                with patch.object(verifier.tools,'Fixture',return_value=fixture),patch.object(verifier,'SessionScope'), \
                     patch.object(verifier,'healthy_batches',return_value={'passed':True}), \
                     patch.object(verifier,'fault_batch',return_value={'passed':True}), \
                     patch.object(verifier,'check_workers',return_value={'passed':True}), \
                     patch.object(verifier,'run_sdk',side_effect=sdk),patch.object(verifier.time,'monotonic',side_effect=[0,2000]):
                    result=verifier.run_cell(args,'cpython',True,None,report)
                self.assertEqual(result['passed'],expected_pass)
                self.assertEqual(args.cell_deadline,None if limit==0 else 540)
                self.assertEqual(result['wall_limit_s'],0 if limit==0 else 600)
                self.assertEqual(result['elapsed_seconds'],2000)
                self.assertEqual(result['post_limit'],128)
                fixture.close.assert_called_once()

    def test_sdk_zero_wall_limit_removes_healthy_wait_timeout_but_keeps_parser_limit(self):
        for limit,case,expected in ((0,'healthy',None),(1200,'healthy',120),(0,'parser',120)):
            with self.subTest(limit=limit,case=case):
                root=self.root/('sdk-wait-'+str(limit)+'-'+case);root.mkdir()
                output=root/('sdk-'+case);output.mkdir()
                result=dict(passed=True,runtime='cpython',reuse_bridge=True,models=0,gpu=0,
                    fixture=dict(actual_posts=28 if case=='healthy' else 4,reserved=28 if case=='healthy' else 4,violations=[]),
                    source=dict(candidate_commit='0'*40))
                (output/'summary.json').write_text(json.dumps(result))
                process=SimpleNamespace(wait=Mock(return_value=0));writer=SimpleNamespace(join=Mock())
                config=IMLaneCallConfig('cpython',str(root/'bridge.json'),123,456,str(root/'build.json'),reuse_bridge=True)
                scope=SimpleNamespace(root=root,config=config,env={},children=[],mode='cpython',track_root=Mock(),
                    helper=SimpleNamespace(spawn=Mock(return_value=(process,writer))))
                args=SimpleNamespace(wall_limit_s=limit,lotus_python=self.root/'venv/bin/python',tokenizer=self.root/'tokenizer',
                    checkout=self.root/'checkout',source_manifest=self.root/'source-lock.json',candidate_commit='0'*40)
                self.assertEqual(verifier.run_sdk(args,scope,case),result)
                process.wait.assert_called_once_with(timeout=expected)
                scope.track_root.assert_called_once_with(process)

    def test_zero_wall_limit_does_not_arm_total_timer_or_fail_on_elapsed_time(self):
        for limit,expected_pass in ((0,True),(1200,False)):
            with self.subTest(limit=limit):
                args=SimpleNamespace(output=self.root/('main-wall-'+str(limit)),runtime='cpython',path_mode='both',
                    candidate_commit='0'*40,wall_limit_s=limit,cpu_quota_cores=16,check_lease=Mock(),
                    source_manifest=self.root/'source-lock.json',bridge_binary=self.root/'bridge')
                with patch.object(verifier,'parse_args',return_value=args), \
                     patch.object(verifier,'verify_inputs',return_value=(None,{},list(range(8)))), \
                     patch.object(verifier,'run_cell',return_value={'passed':True}), \
                     patch.object(verifier.tools,'digest',return_value='0'*64), \
                     patch.object(verifier.signal,'signal'),patch.object(verifier.signal,'setitimer') as timer, \
                     patch.object(verifier.time,'monotonic',side_effect=[0,2000]),patch('builtins.print'):
                    code=verifier.main([])
                self.assertEqual(code,0 if expected_pass else 1)
                self.assertEqual([call.args[1] for call in timer.call_args_list],[0] if limit==0 else [1140,0])
                summary=json.loads((args.output/'summary.json').read_text())
                self.assertEqual(summary['passed'],expected_pass)
                self.assertEqual(summary['elapsed_seconds'],2000)

    def test_changed_or_released_shared_lease_rejects_before_author_preflight(self):
        for exists in (False,True):
            with self.subTest(exists=exists):
                lease=self.root/('lease-'+str(exists)+'.json')
                if exists:lease.write_text('{"owner":"another-window"}')
                args=SimpleNamespace(execute=True,cpu_ids='0,1,2,3,4,5,6,7',cpu_quota_cores=16,wall_limit_s=1200,
                    lease_sha256='0'*64)
                for n in ('author_root','bridge_binary','lotus_python','tokenizer','checkout','source_manifest','output','lease_file'):
                    setattr(args,n,lease)
                with patch.object(verifier.sys,'platform','linux'),patch.object(verifier.os,'sched_getaffinity',return_value=set(range(8)),create=True):
                    with self.assertRaisesRegex(RuntimeError,'lease changed or was released'):verifier.verify_inputs(args)

    def test_read_only_git_checkout_is_accepted_but_outputs_inside_it_are_rejected(self):
        checkout=self.root/'checkout';checkout.mkdir()
        subprocess.run(['git','init','--quiet',str(checkout)],check=True,capture_output=True)
        args=SimpleNamespace(checkout=checkout)
        for name in ('author_root','bridge_binary','lotus_python','tokenizer','source_manifest','output','lease_file'):
            setattr(args,name,self.root/name)
        verifier.resolve_inputs(args)
        self.assertEqual(args.checkout,checkout.resolve())
        args.output=checkout/'private-output'
        with self.assertRaises(ValueError):verifier.resolve_inputs(args)

    def test_sdk_manifest_requirements_are_checked_before_any_author_start(self):
        lease=self.root/'active-lease.json';lease.write_text('{"owner":"this-window"}')
        for limit in (0,1200):
            args=SimpleNamespace(execute=True,cpu_ids='0,1,2,3,4,5,6,7',cpu_quota_cores=16,wall_limit_s=limit,
                lease_sha256=hashlib.sha256(lease.read_bytes()).hexdigest())
            for name in ('author_root','bridge_binary','lotus_python','tokenizer','checkout','source_manifest','output','lease_file'):
                setattr(args,name,lease)
            with self.subTest(limit=limit),patch.object(verifier.sys,'platform','linux'), \
                 patch.object(verifier.os,'sched_getaffinity',return_value=set(range(8)),create=True), \
                 patch.object(verifier,'verify_source_manifest',side_effect=ValueError('SDK child source manifest is incomplete')) as sdk_source, \
                 patch.object(verifier,'verify_checkout') as checkout,patch.object(verifier,'SessionScope') as scope:
                with self.assertRaisesRegex(ValueError,'SDK child source manifest is incomplete'):verifier.verify_inputs(args)
            sdk_source.assert_called_once_with(lease.resolve());checkout.assert_not_called();scope.assert_not_called()

    def test_venv_python_symlink_keeps_its_selected_environment_path(self):
        venv=self.root/'selected-venv/bin';venv.mkdir(parents=True)
        python=venv/'python';python.symlink_to(sys.executable)
        args=SimpleNamespace(lotus_python=python)
        for name in ('author_root','bridge_binary','tokenizer','checkout','source_manifest','output','lease_file'):
            setattr(args,name,self.root/name)
        verifier.resolve_inputs(args)
        self.assertEqual(args.lotus_python,python.absolute())
        self.assertNotEqual(args.lotus_python,python.resolve())

    def test_consumer_close_error_keeps_first_failure_and_fails_the_probe(self):
        iterator=iter([object()])
        class BadClose:
            def __next__(self):return next(iterator)
            def close(self):raise OSError('fixture iterator close failed')
        executor=SimpleNamespace(iter_batch=Mock(side_effect=[iter([]),BadClose()]),last_close_report={'clean':True})
        scope=SimpleNamespace(executor=lambda:executor)
        fixture=SimpleNamespace(base='http://localhost',allowance=SimpleNamespace(reserve=Mock()),settled=Mock(),slow=threading.Event())
        with self.assertRaises(ValueError) as observed:verifier.fault_batch(scope,fixture,'consumer')
        self.assertEqual(str(observed.exception),'fixture first consumer failure')
        self.assertEqual(observed.exception.__notes__,['Consumer iterator cleanup also failed: OSError'])

    def test_expected_fault_cleanup_never_turns_healthy_force_or_real_cleanup_error_into_pass(self):
        for expected,errors,accepted in ((True,[],True),(False,[],False),(True,[{'action':'signal'}],False)):
            with self.subTest(expected=expected,errors=errors):
                output=self.root/('cleanup-'+str(expected)+'-'+str(bool(errors)));output.mkdir()
                args=SimpleNamespace(output=output,scopes=[])
                report={'cleanup':[]};scope=verifier.SessionScope(args,'cpython',None,report,reuse_bridge=True,expected_fault=expected)
                scope.closed=True
                def failed_close(_self):
                    report['cleanup'].append(dict(passed=False,errors=errors,owned_survivors=[],remaining_shm={},
                        owned_threads_stopped=True,forced_identities=[{'pid':100,'start_ticks':200}]))
                    raise RuntimeError('owned author cleanup failed')
                with patch.object(verifier.tools.AuthorScope,'close',failed_close):
                    if accepted:scope.close()
                    else:
                        with self.assertRaises(RuntimeError):scope.close()
                self.assertFalse(report['cleanup'][0]['passed'])
                self.assertEqual(report['cleanup'][0].get('expected_fault_owner_cleanup_verified',False),accepted)

if __name__=='__main__':unittest.main()
