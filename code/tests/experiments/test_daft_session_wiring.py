"""CPU wiring checks; native Daft/pipe behavior is tested separately."""
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.experiments import movie_records_session as session
from src.experiments import lotus_records_movie as movie
from src.baselines.text.frameworks import daft_lotus_session as native
from integrations.imlane_records import chain_runtime
from tests.experiments import test_movie_records_session as fixtures
from tests.experiments import test_movie_records_matrix as matrix_fixture


class DaftSessionWiringTests(unittest.TestCase):
    def fixture(self):
        f=fixtures.PlanTests();f.setUp();self.addCleanup(f.doCleanups)
        f.plan.update(backend='daft',lifecycle='native_single_thread_session',profile=dict(session.DAFT_SESSION_PROFILE))
        return f

    def test_explicit_new_profile_is_required_and_old_resident_rejection_stays(self):
        f=self.fixture();checked=session.check_plan(f.plan)
        self.assertTrue(checked['native_single_thread_session'])
        self.assertEqual([q['expected_calls'] for q in checked['queries']],[2,1])
        f.plan['profile']={}
        with self.assertRaisesRegex(ValueError,'explicit different'):session.check_plan(f.plan)
        f.plan['lifecycle']='resident'
        with self.assertRaisesRegex(ValueError,'no reusable'):session.check_plan(f.plan)

    def test_driver_uses_one_native_session_but_new_task_and_source_for_each_query(self):
        f=self.fixture();f.plan['queries'][1].update(task='movie-q3',manifest=f.manifests['movie-q3'])
        checked=session.check_plan(f.plan)
        context=dict(owner_context={},claims={q['query_id']:object() for q in checked['queries']},
            checked_queries={q['query_id']:q for q in checked['queries']},sembench_checkout='CPU-fixture')
        owners=[]
        class Owner:
            def __init__(self,engine,generation,resources,**kwargs):
                owners.append(self);self.calls=[];self.closes=0
                f.assertEqual(asdict(engine),f.plan['engine'])
                f.assertEqual(asdict(generation),f.plan['generation'])
                f.assertEqual(asdict(resources),f.plan['resources'])
            def start(self):return dict(no_session_model=True,model_startup_scope='lazy first query')
            def evaluate(self,source,method,*,query_id):
                self.calls.append((query_id,method.instruction))
                return [dict(row,status='succeeded',engine_uid='one-native-model') for row in source.expected(query_id)]
            def close(self):self.closes+=1;return dict(status='closed',cleanup_errors=[])
        def run(source,checkout,method,generation,execute,*,query_id,observer):
            rows=list(execute());observer('result_ready')
            return dict(status='completed',calls=dict(records=rows))
        with patch.object(native,'DaftNativeSession',Owner),patch.object(chain_runtime,'check_external_lease',return_value={}),patch.object(movie,'run_movie_query',side_effect=run):
            report=session.run_sequence(f.plan['queries'],lambda d,q:session._MovieDriver(f.plan,context,d,q),
                Path(f.manifests['movie-q1']).parent.parent/'session',backend='daft',lifecycle=f.plan['lifecycle'])
        self.assertEqual((len(owners),owners[0].closes),(1,1))
        from src.baselines.text.movie_tasks import MovieTask
        self.assertEqual(owners[0].calls,[(q['query_id'],MovieTask.from_config(q['task']).instruction) for q in f.plan['queries']])
        calls=[r for q in report['queries'] for r in q['backend_report']['calls']['records']]
        self.assertEqual(len(calls),3);self.assertEqual(len({r['call_id'] for r in calls}),3)

    def test_isolated_daft_start_does_not_create_new_session_or_change_runner(self):
        f=self.fixture();f.plan.update(lifecycle='isolated',profile={})
        with patch.object(native,'DaftNativeSession') as constructor:
            d=session._MovieDriver(f.plan,{},Path(f.manifests['movie-q1']).parent.parent/'old',f.plan['queries'][0])
            self.assertTrue(d.start()['no_session_model'])
            self.assertEqual(d.close()['status'],'closed');constructor.assert_not_called()

    def test_other_backend_cannot_accidentally_select_daft_thread_profile(self):
        f=self.fixture();f.plan['backend']='ray-data'
        with self.assertRaisesRegex(ValueError,'requires daft'):session.check_plan(f.plan)


class MatrixDaftSessionTests(unittest.TestCase):
    def fixture(self,root,change_uid=False):
        f=matrix_fixture.WorkerFixture(root);packet=root/'packet';read=f.ns['read']
        plan=read(packet/'session.json');plan.update(backend='daft',lifecycle='native_single_thread_session')
        plan['queries'].append(dict(plan['queries'][0],query_id='q1'))
        queries=[deepcopy(f.query),deepcopy(f.query)];queries[1]['query_id']='q1'
        for i,q in enumerate(queries):
            q['backend_report']['calls']['records'][0].update(call_id='call-'+str(i),
                engine_uid='changed' if change_uid and i else 'same-model')
        def updated(path):
            if path==packet/'session.json':return deepcopy(plan)
            if path==f.output/'session/owner-ready-0.json':return dict(ready={})
            return read(path)
        f.ns['read']=updated;f.ns['read_report'].side_effect=queries
        f.ns['check_plan'].return_value=dict(queries=plan['queries'])
        f.ns['bound'].return_value[2][0]['max_calls']=2
        def execute(_plan,_context,_output,*,observer,checkpoint):
            for i in range(2):checkpoint(dict(query_index=i))
            return dict(status='completed',owners=[dict(status='closed')],queries=queries)
        f.ns['run_movie_session'].side_effect=execute
        return f

    def test_maintained_entry_expects_same_model_and_one_owner_for_two_queries(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(Path(directory));f.run()
            result=json.loads((f.output/'functional-validation.json').read_text())
            self.assertEqual(result['actual_calls'],2)
            self.assertEqual(result['checkpoints'][1]['owner']['engine_uids'],['same-model'])

    def test_maintained_entry_rejects_a_changed_model_uid(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(Path(directory),change_uid=True)
            with self.assertRaises(AssertionError):f.run()
            self.assertFalse((f.output/'functional-validation.json').exists())

if __name__=='__main__':unittest.main(verbosity=2)
