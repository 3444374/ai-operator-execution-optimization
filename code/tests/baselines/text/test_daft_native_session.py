"""Real Daft CPU graphs with synthetic kernels; no model or GPU resource."""
from dataclasses import asdict
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import daft_lotus_records as original
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources, identity, RecordScan
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from src.baselines.text.frameworks.daft_engine_process import EngineProcess


class Source:
    def __init__(self,root,texts):
        import csv
        self.csv_path=root/'rows.csv'
        with self.csv_path.open('w') as f:
            w=csv.writer(f);w.writerow(['row_id','source_position','reviewText'])
            w.writerows([['row-'+str(i),i,t] for i,t in enumerate(texts)])
        self.texts=texts
    def preflight(self,method):method.describe()
    def expected(self,query_id):return [identity(query_id,'row-'+str(i),i) for i in range(len(self.texts))]
    def scan_spec(self,method):
        return RecordScan((('row_id','string'),('source_position','int64'),('reviewText','string')),('reviewText',))


class SyntheticKernel:
    instances=[]
    def __init__(self,engine,generation,control,deadline,startup_timeout_s):
        self.ready_report=dict(engine_uid='CPU-kernel-'+str(len(self.instances)),pid=os.getpid())
        self.calls=[];self.closes=0;self.control=Path(control)
        self.instances.append(self)
        self.report_path=self.control/'engine-CPU.json'
        self.report_path.write_text(json.dumps(dict(status='running',process_exit_confirmed=False,cleanup_errors=[])))
        def reap():
            while not self.closes and not (self.control/'stop').exists():time.sleep(.01)
            if not self.closes:self.close()
        self.reaper=threading.Thread(target=reap,daemon=True);self.reaper.start()
    def run_prepared(self,rows):
        self.calls.extend(rows)
        return [dict(**{k:r[k] for k in identity('fixture','fixture',0)},status='succeeded',text='True',
            submission_confirmed=True,prompt_tokens=3,completion_tokens=1,total_tokens=4,
            engine_uid=self.ready_report['engine_uid'],engine_request_id='CPU-'+r['call_id']) for r in rows]
    def close(self):
        if self.closes:return self.last_close_report
        self.closes+=1
        self.last_close_report=dict(status='closed',process_exit_confirmed=True,cleanup_errors=[],
            engine_ready=self.ready_report,gpu_exit_confirmed=None)
        self.report_path.write_text(json.dumps(self.last_close_report))
        return self.last_close_report


class CPUProcess(EngineProcess):
    instances=[]
    def __init__(self,*args):
        self.instances.append(self)
        super().__init__(*args)
    @staticmethod
    def _command(engine,result_fd):
        return [engine.model_python,'-m','tests.baselines.text.daft_session_cpu_engine',str(result_fd)]


class DaftNativeSessionTests(unittest.TestCase):
    def setUp(self):
        from src.baselines.text.frameworks.daft_lotus_session import DaftNativeSession
        self.session_type=DaftNativeSession
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name)
        self.engine=EngineSpec('/tmp/CPU-fixture-model',sys.executable,sys.executable,max_model_calls=64)
        self.generation=GenerationSpec()
        self.resources=RecordsResources(cpu_workers=1,prepare_batch_rows=2,model_batch_rows=2,query_timeout_s=10)
        self.method=FilterMethod('openai/cpu-fixture','Keep {reviewText}')
        SyntheticKernel.instances=[]
        self.environment=patch.object(EngineSpec,'validate_driver_environment',return_value={})
        self.environment.start();self.addCleanup(self.environment.stop)

    def owner(self,engine_type=SyntheticKernel,resources=None):
        owner=self.session_type(self.engine,self.generation,self.resources,control_dir=self.root/'session',
            deadline_monotonic=time.monotonic()+40,engine_type=engine_type,model_gpus=0)
        if resources is not None:owner.resources=resources
        def cleanup():
            owner.close()
            for instance in SyntheticKernel.instances:
                if instance.control==owner.control:instance.reaper.join(.3)
        self.addCleanup(cleanup)
        return owner

    def test_two_native_queries_share_kernel_without_replaying_prior_rows_or_prompt(self):
        from dataclasses import replace
        owner=self.owner();owner.start()
        rows=[]
        for i,method in enumerate([self.method,replace(self.method,instruction='Reverse {reviewText}')]):
            path=self.root/str(i);path.mkdir()
            rows.append(list(owner.evaluate(Source(path,['one','two','three','four']),method,query_id='query-'+str(i))))
            self.assertEqual(len(SyntheticKernel.instances),1)
            self.assertEqual(SyntheticKernel.instances[0].closes,0)
        self.assertEqual(len({r['engine_uid'] for batch in rows for r in batch}),1)
        self.assertFalse({r['call_id'] for r in rows[0]} & {r['call_id'] for r in rows[1]})
        self.assertEqual({r['query_id'] for r in rows[1]},{'query-1'})
        messages=[json.loads(r['messages_json']) for r in SyntheticKernel.instances[0].calls]
        self.assertTrue(any('Keep' in str(m) for m in messages[:4]))
        self.assertTrue(all('Reverse' in str(m) for m in messages[4:]))
        close=owner.close();self.assertEqual(close['status'],'closed')
        owner.close();self.assertEqual(SyntheticKernel.instances[0].closes,1)

    def test_real_engine_process_and_native_wrapper_reuse_child_then_close_api_once(self):
        CPUProcess.instances=[];owner=self.owner(CPUProcess);owner.start()
        result=[]
        for i,instruction in enumerate(['Keep {reviewText}','Reverse {reviewText}']):
            path=self.root/str(i);path.mkdir()
            result.extend(list(owner.evaluate(Source(path,['A','B']),FilterMethod('openai/cpu-fixture',instruction),query_id='pipe-'+str(i))))
            self.assertEqual(len(CPUProcess.instances),1)
            self.assertIsNone(CPUProcess.instances[0].process.poll())
        self.assertEqual(len({r['engine_uid'] for r in result}),1)
        child=CPUProcess.instances[0];close=owner.close()
        self.assertEqual(close['status'],'closed')
        self.assertTrue(close['engine_reports'][0]['engine_api']['engine_api_shutdown_confirmed'])
        self.assertIsNotNone(child.process.poll())
        self.assertEqual(owner.close(),close)

    def test_empty_then_nonempty_query_does_not_eagerly_construct_or_replay_model(self):
        owner=self.owner();owner.start()
        path=self.root/'empty';path.mkdir()
        self.assertEqual(list(owner.evaluate(Source(path,[]),self.method,query_id='empty')),[])
        self.assertEqual(SyntheticKernel.instances,[])
        path=self.root/'data';path.mkdir()
        rows=list(owner.evaluate(Source(path,['fresh']),self.method,query_id='fresh'))
        self.assertEqual([r['query_id'] for r in rows],['fresh'])
        self.assertEqual(owner.close()['status'],'closed')

    def test_query_id_reuse_and_overlapping_query_are_rejected_without_model_duplication(self):
        owner=self.owner();owner.start()
        path=self.root/'data';path.mkdir();source=Source(path,['A','B'])
        pending=owner.evaluate(source,self.method,query_id='one')
        with self.assertRaisesRegex(RuntimeError,'still active'):owner.evaluate(source,self.method,query_id='two')
        list(pending)
        with self.assertRaisesRegex(ValueError,'unique'):owner.evaluate(source,self.method,query_id='one')
        self.assertEqual(len(SyntheticKernel.instances),1)

    def test_native_inference_error_makes_session_unusable_and_preserves_original_error(self):
        class Failing(SyntheticKernel):
            def run_prepared(self,rows):raise ValueError('CPU inference failure')
        owner=self.owner(Failing);owner.start()
        path=self.root/'data';path.mkdir();source=Source(path,['A'])
        with self.assertRaises(Exception) as caught:list(owner.evaluate(source,self.method,query_id='failed'))
        self.assertIn('CPU inference failure',str(caught.exception))
        self.assertTrue(owner.broken)
        with self.assertRaisesRegex(RuntimeError,'not reusable'):owner.evaluate(source,self.method,query_id='next')

    def test_query_deadline_during_preparation_does_not_extend_session_deadline(self):
        from dataclasses import replace
        class Waiting(SyntheticKernel):
            def run_prepared(self,rows):
                while not (self.control/'stop').exists():time.sleep(.005)
                return super().run_prepared(rows)
        owner=self.owner(Waiting,replace(self.resources,query_timeout_s=3));owner.start()
        deadline=owner.deadline
        path=self.root/'data';path.mkdir()
        with self.assertRaises(Exception):list(owner.evaluate(Source(path,['wait']),self.method,query_id='deadline'))
        self.assertEqual(owner.deadline,deadline)
        self.assertTrue(owner.broken)

    def test_cancellation_after_inference_starts_retains_delivered_usage_and_stops_session(self):
        cancel=self.root/'cancel-requested'
        class Waiting(SyntheticKernel):
            def run_prepared(self,rows):
                cancel.touch()
                while not (self.control/'stop').exists():time.sleep(.005)
                return super().run_prepared(rows)
        owner=self.owner(Waiting);owner.start();path=self.root/'data';path.mkdir();received=[]
        with self.assertRaises(InterruptedError):
            for row in owner.evaluate(Source(path,['wait','done']),self.method,query_id='cancel-active',cancelled=cancel.exists):
                received.append(row)
        self.assertTrue(cancel.exists())
        self.assertEqual(len(received),2)
        self.assertEqual(sum(r['total_tokens'] for r in received),8)
        self.assertTrue(owner.broken)

    def test_original_process_graph_still_builds_and_closes_a_new_kernel_for_each_query(self):
        uids=[]
        for i in range(2):
            path=self.root/str(i);path.mkdir();control=self.root/('old-'+str(i));control.mkdir()
            source=Source(path,['original-A','original-B']);qid='old-'+str(i)
            expected=source.expected(qid);deadline=time.monotonic()+15
            graph=original._graph(source,self.method,self.engine,self.generation,self.resources,qid,
                expected,deadline,control,runner_threads=1,engine_type=CPUProcess,model_gpus=0)
            rows=original._DaftRows(graph,expected,deadline,None,control)
            result=list(rows)
            self.assertEqual(len(result),2)
            self.assertEqual({r['query_id'] for r in result},{qid})
            uids.append(result[0]['engine_uid'])
            self.assertEqual(rows.last_close_report['status'],'closed')
            self.assertTrue(rows.last_close_report['engine_reports'][0]['process_exit_confirmed'])
        self.assertNotEqual(uids[0],uids[1])

    def test_cancel_before_graph_allocates_no_kernel_and_still_closes_empty_session(self):
        owner=self.owner();owner.start();path=self.root/'data';path.mkdir()
        with self.assertRaises(InterruptedError):owner.evaluate(Source(path,['A']),self.method,query_id='cancelled',cancelled=lambda:True)
        self.assertEqual(SyntheticKernel.instances,[])
        self.assertEqual(owner.close()['status'],'closed')

    def test_early_iterator_close_stops_session_instead_of_retaining_an_unfinished_query(self):
        CPUProcess.instances=[];owner=self.owner(CPUProcess);owner.start();path=self.root/'data';path.mkdir()
        rows=owner.evaluate(Source(path,['A','B','C']),self.method,query_id='short-read')
        next(rows);rows.close()
        self.assertTrue(owner.broken)
        self.assertEqual(owner.active,None)
        close=owner.close()
        self.assertTrue(close['engine_reports'][0]['process_exit_confirmed'])
        self.assertIsNotNone(CPUProcess.instances[0].process.poll())

    def test_closed_session_cannot_restart_and_incompatible_runner_is_not_reconfigured(self):
        from src.infrastructure import daft_runtime
        owner=self.owner();owner.start();owner.close()
        with self.assertRaisesRegex(RuntimeError,'cannot restart'):owner.start()
        other=self.session_type(self.engine,self.generation,self.resources,control_dir=self.root/'other',
                               deadline_monotonic=time.monotonic()+10,model_gpus=0)
        with patch.object(daft_runtime,'_configured',('native',2)),patch.object(daft_runtime,'configure_daft_runner') as configure:
            with self.assertRaisesRegex(RuntimeError,'own compatible'):other.start()
            configure.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
