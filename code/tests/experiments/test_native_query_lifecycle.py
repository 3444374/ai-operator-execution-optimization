"""Characterize our finite adapter lifetime, not vendor reuse capability."""
from dataclasses import asdict
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
import time
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pyarrow as pa

from src.baselines.text.frameworks import ray_data_lotus_records as native
from src.experiments.movie_records_session import run_sequence
from src.semantic_methods.lotus.records import identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.experiments import test_movie_records_session as lifecycle_fixture
from src.experiments import movie_records_session as session
from src.experiments import lotus_records_movie as movie
from integrations.imlane_records import chain_runtime


class ComparableLifecycleTimingTests(unittest.TestCase):
    def fixture(self):
        case=lifecycle_fixture.LifecycleTests();case.setUp();self.addCleanup(case.doCleanups)
        return case

    def test_resident_startup_is_once_and_driver_return_is_after_evaluation(self):
        case=self.fixture();report=case.run_session('resident');t=report['lifecycle_timing']
        self.assertEqual([q['full_query_seconds'] for q in report['queries']],[36]*3)
        self.assertEqual([q['query_api_return_seconds'] for q in report['queries']],[53]*3)
        self.assertEqual(t['owner_startup_seconds_sum'],100)
        self.assertEqual(t['query_api_return_seconds_sum'],159)
        self.assertEqual(t['owner_shutdown_seconds_sum'],29)
        self.assertEqual(t['owner_startup_already_in_query_api_seconds'],0)
        self.assertEqual(t['owners'][0]['cold_first_api_return_seconds'],153)
        self.assertEqual(t['owners'][0]['cold_first_result_seconds'],136)
        self.assertIsNone(report['model_load_count_observed'])

    def test_isolated_startup_is_already_inside_each_driver_return(self):
        case=self.fixture();report=case.run_session('isolated');t=report['lifecycle_timing']
        self.assertEqual([q['full_query_seconds'] for q in report['queries']],[136]*3)
        self.assertEqual([q['query_api_return_seconds'] for q in report['queries']],[153]*3)
        self.assertEqual(t['owner_startup_seconds_sum'],300)
        self.assertEqual(t['query_api_return_seconds_sum'],459)
        self.assertEqual(t['owner_startup_already_in_query_api_seconds'],300)
        self.assertEqual(t['owner_shutdown_seconds_sum'],87)
        self.assertEqual(report['application_e2e_s'],546)
        self.assertEqual(len(t['owners']),3)

    def test_checkpoint_and_final_owner_close_are_not_model_or_query_api_time(self):
        case=self.fixture()
        report=run_sequence(case.queries,case.factory,case.output,backend='semloom-core',
            lifecycle='resident',clock=case.clock,checkpoint=lambda event:case.clock.advance(41))
        self.assertEqual([q['query_api_return_seconds'] for q in report['queries']],[53]*3)
        self.assertEqual(report['application_e2e_s'],411)
        self.assertEqual(report['lifecycle_timing']['owner_shutdown_seconds_sum'],29)

    def test_failed_call_has_no_invented_normal_return_or_zero_cost(self):
        case=self.fixture();case.query_error=ValueError('CPU query failure')
        with self.assertRaises(ValueError) as caught:case.run_session('resident')
        report=caught.exception.session_report
        self.assertIsNone(report['queries'][1]['query_api_return_ns'])
        self.assertIsNone(report['queries'][1]['query_api_return_seconds'])
        self.assertIsNone(report['lifecycle_timing']['query_api_return_seconds_sum'])
        self.assertEqual(report['lifecycle_timing']['query_api_timing_count'],1)
        self.assertEqual(report['skipped_queries'],['q2'])

    def test_partial_start_keeps_unknown_start_duration_and_cleanup_evidence(self):
        case=self.fixture();case.start_error=RuntimeError('CPU start failure')
        with self.assertRaises(RuntimeError) as caught:case.run_session('resident')
        report=caught.exception.session_report
        self.assertIsNone(report['lifecycle_timing']['owner_startup_seconds_sum'])
        self.assertIsNone(report['lifecycle_timing']['owners'][0]['cold_first_api_return_seconds'])
        self.assertEqual(case.drivers[0].closes,1)

    def test_runtime_only_readiness_does_not_claim_model_load_or_residency(self):
        case=self.fixture();original=case.factory
        class RuntimeOnly(original):
            def start(self):
                super().start()
                return dict(no_session_model=True,model_startup_scope='inside existing per-query adapter')
        report=run_sequence(case.queries,RuntimeOnly,case.output,backend='ray-data',
            lifecycle='isolated',clock=case.clock)
        owners=report['lifecycle_timing']['owners']
        self.assertTrue(all(o['owner_startup_scope']=='driver_runtime_only' for o in owners))
        self.assertTrue(all(o['model_initialization_scope']=='inside existing per-query adapter' for o in owners))
        self.assertIsNone(report['model_load_count_observed'])

    def test_resident_rejection_is_about_this_adapter_not_a_vendor_capability_claim(self):
        for backend in ('ray-data','daft'):
            case=self.fixture()
            with self.subTest(backend=backend),self.assertRaisesRegex(ValueError,'this adapter'):
                run_sequence(case.queries,case.factory,case.output,backend=backend,
                    lifecycle='resident',clock=case.clock)
            self.assertEqual(case.drivers,[])

    def test_existing_native_constructor_receipt_is_separate_from_runtime_start(self):
        case=self.fixture();original=case.factory
        class NativeReceipts(original):
            def start(self):
                super().start()
                return dict(no_session_model=True,model_startup_scope='inside native graph')
            def query(self,spec,emit):
                result=super().query(spec,emit)
                result['backend_close']=dict(engine_reports=[dict(
                    engine_ready=dict(engine_uid='CPU-'+spec['query_id']),startup_seconds=11)])
                return result
        report=run_sequence(case.queries,NativeReceipts,case.output,backend='daft',
            lifecycle='isolated',clock=case.clock)
        starts=report['lifecycle_timing']['native_query_model_startup']
        self.assertEqual([q['reports'][0]['startup_seconds'] for q in starts],[11]*3)
        self.assertEqual(len({q['reports'][0]['engine_uid'] for q in starts}),3)
        self.assertTrue(all(o['owner_startup_scope']=='driver_runtime_only'
                            for o in report['lifecycle_timing']['owners']))
        self.assertEqual(report['lifecycle_timing']['query_api_return_seconds_sum'],459)

    def test_missing_native_model_duration_is_not_backfilled_from_query_time(self):
        case=self.fixture();report=case.run_session()
        starts=report['lifecycle_timing']['native_query_model_startup']
        self.assertTrue(all(q['reports']==[] and q['missing_reason'] for q in starts))

    def test_generic_clock_keeps_existing_imlane_result_timing_semantics(self):
        case=self.fixture()
        report=run_sequence(case.queries,case.factory,case.output,backend='imlane',
            lifecycle='resident',clock=case.clock)
        self.assertEqual(report['resident_timing']['cold_first_result_seconds'],136)
        self.assertEqual(report['lifecycle_timing']['owners'][0]['cold_first_result_seconds'],136)
        self.assertEqual(report['lifecycle_timing']['owners'][0]['cold_first_api_return_seconds'],153)

    def test_invalid_backend_report_is_not_masked_by_optional_lifetime_metrics(self):
        case=self.fixture();original=case.factory
        class BadResult(original):
            def query(self,spec,emit):return None
        with self.assertRaises(AttributeError) as caught:
            run_sequence(case.queries,BadResult,case.output,backend='semloom-core',
                lifecycle='resident',clock=case.clock)
        self.assertEqual(caught.exception.session_report['status'],'failed')
        self.assertEqual(case.drivers[0].closes,1)


class RayRuntimeReuseTests(unittest.TestCase):
    def fixture(self):
        case=lifecycle_fixture.LifecycleTests();case.setUp();self.addCleanup(case.doCleanups)
        original=case.factory
        class RuntimeOnly(original):
            def start(self):
                super().start()
                return dict(no_session_model=True,model_startup_scope='inside each native Dataset')
        case.factory=RuntimeOnly
        return case

    def test_runtime_start_is_once_with_fresh_query_state_and_full_cost(self):
        case=self.fixture()
        report=run_sequence(case.queries,case.factory,case.output,backend='ray-data',
            lifecycle='runtime_reuse_only',clock=case.clock)
        self.assertEqual(report['status'],'completed')
        self.assertEqual(report['owner_start_count'],1)
        self.assertEqual([d.closes for d in case.drivers],[1])
        self.assertEqual([q['backend_report']['calls'] for q in report['queries']],[['q0'],['q1'],['q2']])
        self.assertEqual([q['query_api_return_seconds'] for q in report['queries']],[53]*3)
        self.assertEqual(report['application_e2e_s'],100+3*53+29)
        self.assertEqual(report['lifecycle_timing']['owners'][0]['cold_first_api_return_seconds'],153)
        self.assertEqual(report['lifecycle_timing']['owner_startup_already_in_query_api_seconds'],0)
        self.assertFalse(report['cross_query_model_reuse_requested'])
        self.assertIsNone(report['model_load_count_observed'])

    def test_runtime_reuse_failure_stops_queries_and_still_closes_runtime_once(self):
        case=self.fixture();case.query_error=InterruptedError('cancelled fixture query')
        with self.assertRaises(InterruptedError) as caught:
            run_sequence(case.queries,case.factory,case.output,backend='ray-data',
                lifecycle='runtime_reuse_only',clock=case.clock)
        self.assertEqual(caught.exception.session_report['skipped_queries'],['q2'])
        self.assertEqual(caught.exception.session_report['queries'][1]['backend_report']['unknown_calls'],1)
        self.assertEqual([d.closes for d in case.drivers],[1])

    def test_runtime_reuse_partial_start_failure_is_closed(self):
        case=self.fixture();case.start_error=RuntimeError('fixture startup')
        with self.assertRaises(RuntimeError) as caught:
            run_sequence(case.queries,case.factory,case.output,backend='ray-data',
                lifecycle='runtime_reuse_only',clock=case.clock)
        self.assertEqual(caught.exception.session_report['skipped_queries'],['q0','q1','q2'])
        self.assertEqual([d.closes for d in case.drivers],[1])

    def test_runtime_reuse_is_only_exposed_for_verified_ray_adapter(self):
        for backend in ('daft','semloom-core','lotus-native','imlane'):
            case=self.fixture()
            with self.subTest(backend=backend),self.assertRaisesRegex(ValueError,'runtime_reuse_only.*ray-data'):
                run_sequence(case.queries,case.factory,case.output,backend=backend,
                    lifecycle='runtime_reuse_only',clock=case.clock)
            self.assertFalse(case.output.exists())

    def test_plan_distinguishes_runtime_reuse_and_model_residency(self):
        case=lifecycle_fixture.PlanTests();case.setUp();self.addCleanup(case.doCleanups)
        case.plan.update(backend='ray-data',lifecycle='runtime_reuse_only')
        checked=session.check_plan(case.plan)
        self.assertFalse(checked['supports_resident'])
        self.assertTrue(checked['supports_runtime_reuse_only'])
        self.assertIn('fresh model',checked['model_cache_scope'])
        case.plan.update(backend='daft')
        with self.assertRaisesRegex(ValueError,'runtime_reuse_only.*ray-data'):
            session.check_plan(case.plan)


class RayDriverReuseTests(unittest.TestCase):
    def fixture(self, *, query_failure=False, startup_failure=False, replacement=False, shutdown_failure=False):
        case=lifecycle_fixture.PlanTests();case.setUp();self.addCleanup(case.doCleanups)
        case.plan.update(backend='ray-data',lifecycle='runtime_reuse_only')
        checked=session.check_plan(case.plan)
        state=dict(initialized=False,starts=0,closes=0,node='owned-node',graphs=[],models=[])
        def initialize(**kwargs):
            state.update(initialized=True,starts=state['starts']+1,resources=kwargs)
            if startup_failure:raise RuntimeError('partial runtime startup')
        def shutdown():
            state['closes']+=1
            if shutdown_failure:return
            state['initialized']=False
        ray=SimpleNamespace(is_initialized=lambda:state['initialized'],init=initialize,shutdown=shutdown,
            get_runtime_context=lambda:SimpleNamespace(get_node_id=lambda:state['node']))
        context=dict(owner_context={},sembench_checkout='CPU-checkout',
            checked_queries={q['query_id']:q for q in checked['queries']},
            claims={q['query_id']:object() for q in checked['queries']})
        class Kernel:
            def __init__(self,engine,generation):
                self.uid='CPU-model-'+str(len(state['models']));self.closes=0
                state['models'].append(self)
                case.assertEqual(asdict(engine),case.plan['engine'])
                case.assertEqual(asdict(generation),case.plan['generation'])
            def run_prepared(self,rows,**kwargs):
                return [dict(row,status='succeeded',text='True',engine_uid=self.uid) for row in rows]
            def close(self):
                self.closes+=1
                return dict(status='closed',engine_api_shutdown_confirmed=True,gpu_exit_confirmed=None)
        def graph(source,method,engine,generation,resources,*,query_id):
            state['graphs'].append(dict(query_id=query_id,instruction=method.instruction,
                resources=asdict(resources),runtime_node=state['node']))
            rows=[dict(row,messages_json='[]') for row in source.expected(query_id)]
            kernel=native.InferBatch(asdict(engine),asdict(generation),len(rows),time.monotonic()+10,3)
            return kernel(pa.Table.from_pylist(rows)).to_pylist()
        def collect(source,checkout,method,generation,execute,*,query_id,observer):
            rows=execute()
            if replacement:state['node']='foreign-replacement'
            if query_failure:raise InterruptedError('query cancelled')
            observer('result_ready')
            return dict(status='completed',calls=dict(records=rows))
        stack=ExitStack();self.addCleanup(stack.close)
        stack.enter_context(patch.dict(sys.modules,{'ray':ray}))
        stack.enter_context(patch.object(chain_runtime,'check_external_lease',return_value={}))
        stack.enter_context(patch.object(movie,'run_movie_query',side_effect=collect))
        stack.enter_context(patch.object(native,'evaluate',side_effect=graph))
        stack.enter_context(patch('src.serving.backends.vllm_engine.VllmEngine',Kernel))
        output=Path(case.manifests['movie-q1']).parent.parent/'ray-run'
        return case,context,state,output

    def run_plan(self,case,context,output):
        return run_sequence(case.plan['queries'],lambda d,q:session._MovieDriver(case.plan,context,d,q),
            output,backend='ray-data',lifecycle=case.plan['lifecycle'])

    def test_real_driver_reuses_runtime_but_native_kernel_is_new_and_closed_per_query(self):
        case,context,state,output=self.fixture()
        report=self.run_plan(case,context,output)
        self.assertEqual((state['starts'],state['closes']),(1,1))
        self.assertEqual(state['resources']['num_cpus'],case.plan['resources']['cpu_workers']+2)
        self.assertEqual(state['resources']['num_gpus'],1)
        self.assertEqual([m.closes for m in state['models']],[1,1])
        self.assertEqual(len({m.uid for m in state['models']}),2)
        self.assertEqual([g['query_id'] for g in state['graphs']],['q1','q2'])
        self.assertEqual({g['runtime_node'] for g in state['graphs']},{'owned-node'})
        from src.baselines.text.movie_tasks import MovieTask
        self.assertEqual([g['instruction'] for g in state['graphs']],
            [MovieTask.from_config(q['task']).instruction for q in case.plan['queries']])
        calls=[r for q in report['queries'] for r in q['backend_report']['calls']['records']]
        self.assertEqual(len(calls),3)
        self.assertEqual(len({r['call_id'] for r in calls}),3)
        close=report['owners'][0]['close']['receipts']['ray']
        self.assertTrue(close['runtime_shutdown_confirmed'])
        self.assertIsNone(close['independent_gpu_exit_confirmed'])

    def test_existing_isolated_mode_still_creates_and_closes_two_runtimes(self):
        case,context,state,output=self.fixture();case.plan['lifecycle']='isolated'
        self.assertEqual(self.run_plan(case,context,output)['owner_start_count'],2)
        self.assertEqual((state['starts'],state['closes']),(2,2))
        self.assertEqual([m.closes for m in state['models']],[1,1])

    def test_query_error_preserves_original_failure_and_stops_before_next_graph(self):
        case,context,state,output=self.fixture(query_failure=True)
        with self.assertRaises(InterruptedError) as caught:self.run_plan(case,context,output)
        self.assertEqual(caught.exception.session_report['skipped_queries'],['q2'])
        self.assertEqual((state['starts'],state['closes']),(1,1))
        self.assertEqual([m.closes for m in state['models']],[1])

    def test_partial_ray_initialization_is_still_cleaned_up(self):
        case,context,state,output=self.fixture(startup_failure=True)
        with self.assertRaisesRegex(RuntimeError,'partial runtime startup'):self.run_plan(case,context,output)
        self.assertEqual((state['starts'],state['closes']),(1,1))
        self.assertFalse(state['initialized']);self.assertEqual(state['models'],[])

    def test_changed_runtime_is_rejected_and_not_shutdown_as_our_original(self):
        case,context,state,output=self.fixture(replacement=True)
        with self.assertRaisesRegex(RuntimeError,'owned Ray runtime changed') as caught:
            self.run_plan(case,context,output)
        self.assertEqual(len(state['graphs']),1)
        self.assertEqual(state['closes'],0);self.assertTrue(state['initialized'])
        self.assertEqual(caught.exception.session_report['owners'][0]['status'],'unknown')

    def test_shutdown_return_does_not_claim_success_if_runtime_remains_initialized(self):
        case,context,state,output=self.fixture(shutdown_failure=True)
        with self.assertRaisesRegex(RuntimeError,'cleanup remains unconfirmed') as caught:
            self.run_plan(case,context,output)
        owner=caught.exception.session_report['owners'][0]
        self.assertEqual(owner['status'],'unknown')
        self.assertFalse(owner['close']['receipts']['ray']['runtime_shutdown_confirmed'])


class FiniteRayKernelLifecycleTests(unittest.TestCase):
    def test_two_isolated_queries_keep_new_engine_identity_and_close_each_engine(self):
        instances=[]
        class Kernel:
            def __init__(self,spec,generation):
                self.uid='CPU-kernel-'+str(len(instances));self.closed=0;self.calls=[]
                instances.append(self)
            def run_prepared(self,rows,*,deadline_monotonic):
                if self.closed:raise RuntimeError('closed fixture kernel')
                self.calls.extend(row['call_id'] for row in rows)
                return [dict(row,status='succeeded',text='True',engine_uid=self.uid) for row in rows]
            def close(self):
                self.closed+=1
                return dict(status='closed',engine_api_shutdown_confirmed=True,gpu_exit_confirmed=None)

        spec=asdict(EngineSpec('/tmp/CPU-fixture-model','/tmp/CPU-model-python','/tmp/CPU-driver-python'))
        generation=asdict(GenerationSpec())
        class Driver:
            def __init__(self,directory,first_query):pass
            def start(self):
                return dict(no_session_model=True,model_startup_scope='finite adapter call')
            def query(self,query,emit):
                rows=[dict(identity(query['query_id'],'row-'+str(i),i),messages_json='[]') for i in range(2)]
                kernel=native.InferBatch(spec,generation,len(rows),time.monotonic()+5,3)
                result=kernel(pa.Table.from_pylist(rows)).to_pylist()
                emit('result_ready')
                return dict(status='completed',results=result)
            def close(self):return dict(status='closed',cleanup_errors=[])

        with tempfile.TemporaryDirectory() as directory,patch('src.serving.backends.vllm_engine.VllmEngine',Kernel):
            report=run_sequence([dict(query_id=name,task='arbitrary-task-'+name,phase='measured')
                for name in ('alpha','beta')],Driver,Path(directory)/'run',backend='ray-data',lifecycle='isolated')
        self.assertEqual(report['status'],'completed');self.assertEqual(report['owner_start_count'],2)
        self.assertEqual(len(instances),2);self.assertEqual([k.closed for k in instances],[1,1])
        self.assertEqual(len({k.uid for k in instances}),2)
        first,second=[query['backend_report']['results'] for query in report['queries']]
        self.assertEqual({r['query_id'] for r in first},{'alpha'})
        self.assertEqual({r['query_id'] for r in second},{'beta'})
        self.assertFalse({r['call_id'] for r in first}&{r['call_id'] for r in second})
        for rows in (first,second):
            self.assertTrue(json.loads(rows[-1]['__engine_close_report_json'])['engine_api_shutdown_confirmed'])
        self.assertIsNone(report['model_load_count_observed'])


class MatrixRuntimeReuseTests(unittest.TestCase):
    def fixture(self,root,*,runtime_changes=False):
        from tests.experiments import test_movie_records_matrix as matrix_fixture
        from copy import deepcopy
        f=matrix_fixture.WorkerFixture(root)
        packet=root/'packet';original_read=f.ns['read']
        plan=original_read(packet/'session.json')
        plan.update(backend='ray-data',lifecycle='runtime_reuse_only')
        second=dict(plan['queries'][0],query_id='q1')
        plan['queries'].append(second)
        queries=[deepcopy(f.query),deepcopy(f.query)]
        queries[1]['query_id']='q1'
        for i,q in enumerate(queries):
            q['backend_report']['calls']['records'][0].update(call_id='call-'+str(i),engine_uid='model-'+str(i))
        ready_reads=[]
        def read(path):
            if path==packet/'session.json':return deepcopy(plan)
            if path==f.output/'session/owner-ready-0.json':
                ready_reads.append(path)
                node='changed' if runtime_changes and len(ready_reads)>1 else 'same-runtime'
                return dict(ready=dict(no_session_model=True,runtime_identity=dict(driver_pid=123,node_id=node)))
            return original_read(path)
        f.ns['read']=read
        f.ns['read_report'].side_effect=queries
        f.ns['check_plan'].return_value=dict(queries=plan['queries'])
        f.ns['bound'].return_value[2][0]['max_calls']=2
        def execute(_plan,_context,_output,*,observer,checkpoint):
            for i in range(2):checkpoint(dict(query_index=i))
            return dict(status='completed',owners=[dict(status='closed')],queries=queries)
        f.ns['run_movie_session'].side_effect=execute
        return f

    def test_maintained_worker_accepts_one_runtime_owner_and_two_distinct_model_queries(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(Path(directory));f.run()
            result=json.loads((f.output/'functional-validation.json').read_text())
            self.assertEqual(result['actual_calls'],2)
            self.assertEqual([c['status'] for c in result['checkpoints']],
                ['passed_complete_input_actual_prompt_and_same_runtime']*2)
            self.assertTrue(all(c['owner']==dict(runtime_identity=dict(driver_pid=123,node_id='same-runtime'))
                for c in result['checkpoints']))
            self.assertIsNone(result['model_load_count'])

    def test_maintained_worker_rejects_changed_runtime_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(Path(directory),runtime_changes=True)
            with self.assertRaises(AssertionError):f.run()
            self.assertFalse((f.output/'functional-validation.json').exists())


if __name__=='__main__':unittest.main(verbosity=2)
