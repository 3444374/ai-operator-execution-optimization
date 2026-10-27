"""Same maintained session/CLI and native finite batches; all models are CPU fixtures."""
from copy import deepcopy
import hashlib
import json
import os
import unittest
from unittest.mock import patch

from src.experiments import movie_records_session as session
from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
from src.serving.backends import vllm_engine
from tests.experiments import test_movie_calibration_cli as cli_fixture
from tests.execution_provider import test_lotus_records_async_core as async_fixture
from tests.execution_provider import test_lotus_records_core as core_fixture


class StaticSessionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import duckdb,lotus
        cli_fixture.CLIIntegrationTests.setUpClass()

    def fixture(self):
        f=cli_fixture.CLIIntegrationTests();f.setUp();self.addCleanup(f.doCleanups)
        f.group.pop('calibration');f.group.update(backend='semloom-static-reference',profile='sync-finite-batch')
        f.plan.update(backend='semloom-static-reference',profile=dict(mode='sync'))
        f.plan['resources'].update(prepare_batch_rows=4,model_batch_rows=4,max_active_batches=1)
        for q in f.plan['queries']:q['phase']='measured'
        f.write(f.packet/'session.json',f.plan)
        return f

    def test_maintained_cli_first_reuse_share_one_sync_model_and_native_batches(self):
        f=self.fixture();engines=[]
        class Model:
            def __init__(self,spec,generation):
                engines.append(self);self.batches=[];self.closes=0
            def ready(self):return dict(engine_uid='CPU-static-model',pid=os.getpid())
            def run_prepared(self,rows,**kw):
                self.batches.append([r['call_id'] for r in rows])
                return [dict({k:v for k,v in r.items() if k!='messages_json'},status='succeeded',
                    text='fixed fixture text',prompt_tokens=11,completion_tokens=1,total_tokens=12,
                    prompt_token_ids_json=json.dumps(list(range(11))),output_token_ids_json='[1]',
                    messages_sha256=hashlib.sha256(json.dumps(json.loads(r['messages_json']),
                        ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
                    finish_reason='stop',engine_uid='CPU-static-model',engine_request_id=r['call_id'],
                    submission_confirmed=True,logprobs_json='null') for r in rows]
            def close(self,timeout):
                self.closes+=1
                return dict(status='closed',engine_api_shutdown_confirmed=True,gpu_exit_confirmed=None)
        class Runtime(async_fixture.ThreadedRuntime):
            def __init__(self,*args,**kwargs):
                super().__init__(*args,**kwargs)
                self.stack.enter_context(patch.object(vllm_engine,'VllmEngine',Model))
        with patch.object(cli_fixture,'ThreadedRuntime',Runtime):
            report,out,async_capacities=f.run_cli('static-first-reuse')
        self.assertEqual(report['status'],'completed')
        self.assertEqual((report['scan_succeeded_calls'],report['target_succeeded_calls']),(0,8))
        self.assertEqual(async_capacities,[])
        self.assertEqual(len(engines),1);self.assertEqual(engines[0].closes,1)
        self.assertEqual([len(rows) for rows in engines[0].batches],[4,4])
        self.assertFalse(set(engines[0].batches[0])&set(engines[0].batches[1]))
        results=json.loads((out/'units/g000/query.json').read_text())
        self.assertEqual(len(results['owners']),1)
        self.assertEqual([q['backend_report']['calls']['counts']['succeeded'] for q in results['queries']],[4,4])
        self.assertEqual(results['owners'][0]['status'],'closed')

    def test_static_rejects_method_switch_and_core_does_not_gain_batch_support(self):
        f=self.fixture();plan=deepcopy(f.plan)
        plan['queries'][1].update(task='movie-q2')
        # Materialize the matching task binding before asking about owner semantics.
        from src.baselines.text.movie_records import prepare_movie_source
        source=prepare_movie_source(f.root/'Reviews.csv',f.root/'q2',
            expected_csv_sha256=cli_fixture.entry.sha(f.root/'Reviews.csv'),max_rows=4,task='movie-q2')
        plan['queries'][1]['manifest']=str(source)
        with self.assertRaisesRegex(ValueError,'retains one method'):session.check_plan(plan)
        plan=deepcopy(f.plan);plan.update(backend='semloom-core',profile=dict(mode='sync'))
        with self.assertRaisesRegex(ValueError,'single-row'):session.check_plan(plan)

    def test_direct_static_start_is_idempotent_and_does_not_close_at_query_eof(self):
        f=core_fixture.CoreRecordsTests();f.setUp()
        from src.semantic_methods.lotus.records import FilterMethod,RecordsResources
        with core_fixture.Runtime(f.rows) as runtime:
            owner=SemLoomRecordsStaticReference(FilterMethod('fixture','Keep {reviewText}'),f.spec,
                vllm_engine.GenerationSpec(),RecordsResources(cpu_workers=2,prepare_batch_rows=4,
                    model_batch_rows=4,max_active_batches=1,startup_timeout_s=5,query_timeout_s=5))
            try:
                self.assertEqual(owner.start(),owner.start())
                for q in ('first','reuse'):
                    source=core_fixture.Source(f.rows)
                    self.assertEqual(len(list(owner.evaluate(source,query_id=q))),len(source.expected(q)))
                    self.assertEqual(runtime.engines[0].closes,0)
                self.assertEqual(len(runtime.engines),1)
            finally:self.assertEqual(owner.close()['status'],'closed')
            self.assertEqual(runtime.engines[0].closes,1)
            with self.assertRaisesRegex(RuntimeError,'closing'):owner.start()


if __name__=='__main__':unittest.main()
