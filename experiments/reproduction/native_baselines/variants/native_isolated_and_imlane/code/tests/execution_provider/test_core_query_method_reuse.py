"""Query methods change; the real Core/CPU IPC owner stays resident.

Ray/Daft scheduling is the existing explicit fixture. LOTUS formatting, Core,
bounded async transport, actor logic and private CPU-child IPC are real. The
child returns fixed synthetic results, never imports a model or uses a GPU.
"""
from contextlib import ExitStack
from dataclasses import asdict, replace
import csv
import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.baselines.text.movie_tasks import MovieTask
from src.execution_provider.adapters import lotus_records_actor_owner as actors
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.experiments import movie_records_session as session
from src.experiments.cell_budget import ClaimedUnit, Reservation
from src.semantic_methods.lotus import engine as methods
from src.semantic_methods.lotus.records import CallLedger, RecordsResources
from src.scheduling.core.session_contract import TaskKey
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider.test_lotus_records_core import Source


MODEL = 'openai/cpu-fixture'


class QueryMethodReuseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Warm the real formatter before the fixture's short child deadline.
        cls.formatter = methods.LotusFilterMethod
        cls.bind = staticmethod(methods.bind_filter_method)
        cls.bind(MovieTask(1).method(MODEL))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        path = self.root/'Reviews.csv'
        with path.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(['reviewId','id','reviewText','scoreSentiment'])
            writer.writerows([['r0','taken_3','good film','POSITIVE'],
                              ['r1','other','good acting','POSITIVE'],
                              ['r2','taken_3','good story','POSITIVE']])
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self.sources = {q: MovieRecordSource.from_manifest(prepare_movie_source(
            path,self.root/('source-'+str(q)),expected_csv_sha256=digest,max_rows=3,
            task='movie-q'+str(q))) for q in (1,2,3)}
        self.rows = self.sources[1].rows()
        self.spec = EngineSpec('/tmp/fixture-model','/tmp/model-python','/tmp/driver-python',max_model_calls=32)
        self.generation = GenerationSpec()
        self.resources = RecordsResources(cpu_workers=2,prepare_batch_rows=1,model_batch_rows=1,
            max_active_batches=4,startup_timeout_s=5,query_timeout_s=8)

    def real_formatting(self, runtime):
        runtime.stack.enter_context(patch.object(methods,'LotusFilterMethod',self.formatter))
        runtime.stack.enter_context(patch.object(methods,'bind_filter_method',side_effect=self.bind))

    def owner(self):
        return core.create_records_core(MovieTask(1).method(MODEL),self.spec,self.generation,
            self.resources,profile=dict(mode='async',max_inflight=4))

    def assert_prompts(self, runtime, queries):
        observed = {}
        for event in runtime.events:
            if event[0]=='submit' and event[1]=='AsyncCoreModelActor.run_core':
                for row in event[3][0].value.result(2):
                    observed.setdefault(row['query_id'],[]).append(row)
        for query_id, task in queries:
            rows = self.sources[task].rows()
            raw = [dict(row,**occurrence) for row,occurrence in zip(
                [row for row in rows if MovieTask(task).accepts(row)],
                self.sources[task].expected(query_id),strict=True)]
            expected = self.formatter(self.bind(MovieTask(task).method(MODEL))).prepare_rows(raw)
            actual = {row['call_id']:json.loads(row['messages_json']) for row in observed[query_id]}
            self.assertEqual(actual,{row['call_id']:json.loads(row['messages_json']) for row in expected})

    def test_q1_q3_q2_q3_keep_one_model_and_each_original_prompt(self):
        order = [('first-q1',1),('first-q3',3),('next-q2',2),('next-q3',3)]
        with ThreadedRuntime(self.rows) as runtime:
            self.real_formatting(runtime)
            owner = self.owner()
            ready = owner.start()
            results = []
            try:
                for query_id, task in order:
                    result = list(owner.evaluate(self.sources[task],query_id=query_id,
                        method=MovieTask(task).method(MODEL)))
                    self.assertEqual({row['query_id'] for row in result},{query_id})
                    self.assertEqual({row['call_id'] for row in result},
                        {row['call_id'] for row in self.sources[task].expected(query_id)})
                    self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                    self.assertEqual(owner.transport.retained_refs(),0)
                    results.extend(result)
                self.assertEqual(len(runtime.async_actors),1)
                self.assertEqual({row['engine_uid'] for row in results},{ready['engine_uid']})
                self.assertEqual(runtime.async_actors[0].instance.engine._sequence,9)
                self.assert_prompts(runtime,order)
                self.assertEqual(len([e for e in runtime.events if e[:2]==('actor','_PrepareActor')]),2)
            finally:
                report=owner.close(timeout=3)
            self.assertEqual(report['status'],'closed')
            self.assertEqual(owner.close(timeout=1)['status'],'closed')

    def test_prepare_override_never_changes_the_legacy_default_formatter(self):
        default=self.bind(MovieTask(1).method(MODEL))
        actor=actors._PrepareActor(asdict(default))
        raw=dict(self.rows[0],**self.sources[1].expected('same-query')[0])
        initial=actor.prepare([raw],time.monotonic()+2)
        other=actor.prepare([raw],time.monotonic()+2,asdict(self.bind(MovieTask(3).method(MODEL))))
        restored=actor.prepare([raw],time.monotonic()+2)
        repeated=actor.prepare([raw],time.monotonic()+2,asdict(self.bind(MovieTask(3).method(MODEL))))
        self.assertNotEqual(initial[0]['messages_json'],other[0]['messages_json'])
        self.assertEqual(initial[0]['messages_json'],restored[0]['messages_json'])
        self.assertEqual(other[0]['messages_json'],repeated[0]['messages_json'])

    def test_active_query_or_other_model_cannot_rebind_the_resident_owner(self):
        with ThreadedRuntime(self.rows) as runtime:
            self.real_formatting(runtime)
            owner=self.owner()
            try:
                list(owner.evaluate(self.sources[1],query_id='settled',method=MovieTask(1).method(MODEL)))
                with self.assertRaisesRegex(ValueError,'model'):
                    owner.evaluate(self.sources[3],query_id='wrong-model',method=MovieTask(3).method('openai/other'))
                stream=owner.evaluate(self.sources[3],query_id='active',method=MovieTask(3).method(MODEL))
                with self.assertRaisesRegex(RuntimeError,'active query'):
                    owner.evaluate(self.sources[2],query_id='too-soon',method=MovieTask(2).method(MODEL))
                list(stream)
                self.assertEqual(len(runtime.async_actors),1)
            finally:
                self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_same_query_id_new_method_uses_new_task_session_and_rejects_old_submission(self):
        from types import SimpleNamespace
        with ThreadedRuntime(self.rows) as runtime:
            self.real_formatting(runtime)
            owner=self.owner()
            try:
                first=owner.evaluate(self.sources[1],query_id='repeat',method=MovieTask(1).method(MODEL))
                original=list(first)
                second=owner.evaluate(self.sources[3],query_id='repeat',method=MovieTask(3).method(MODEL))
                self.assertNotEqual(first.session.session_id,second.session.session_id)
                old=SimpleNamespace(key=TaskKey(first.session.session_id,0))
                with self.assertRaisesRegex(ValueError,'another query method binding'):
                    asyncio.run(owner.transport._execute(old,'local-model'))
                result=list(second)
                self.assertEqual(len(original),3)
                self.assertEqual(len(result),2)
                self.assertEqual(runtime.async_actors[0].instance.engine._sequence,5)
                expected=self.formatter(self.bind(MovieTask(3).method(MODEL))).prepare_rows([
                    dict(row,**occurrence) for row,occurrence in zip(
                        [row for row in self.rows if row['id']=='taken_3'],self.sources[3].expected('repeat'),strict=True)])
                prepared=[]
                for event in runtime.events:
                    if (event[0]=='submit' and event[1]=='AsyncCoreModelActor.run_core'
                            and event[3][3][0]==second.session.session_id):
                        prepared.extend(event[3][0].value.result(2))
                self.assertEqual({row['call_id']:row['messages_json'] for row in prepared},
                                 {row['call_id']:row['messages_json'] for row in expected})
                self.assertEqual(len(runtime.async_actors),1)
            finally:
                self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_public_session_passes_each_task_method_to_one_resident_core(self):
        from integrations.imlane_records import chain_runtime
        from src.experiments import lotus_records_movie
        packet=Path(__file__).resolve().parents[4]/'packet/sembench'
        queries=[dict(query_id='public-'+str(i),task='movie-q'+str(q),phase='measured',
                      manifest=str(self.sources[q].manifest_path)) for i,q in enumerate((1,3,2,3))]
        plan=dict(schema='semloom.movie_session_plan.v1',backend='semloom-core',lifecycle='resident',
            model_name=MODEL,generation=asdict(self.generation),engine=asdict(self.spec),
            resources=asdict(self.resources),profile=dict(mode='async',max_inflight=4),queries=queries)
        claims={q['query_id']:ClaimedUnit(Reservation(q['query_id'],1,3 if q['task']=='movie-q1' else 2),None)
                for q in queries}
        context=dict(owner_context={},claims=claims,sembench_checkout=str(packet),runtime_binding=dict(
            driver_python=self.spec.driver_python,model_python=self.spec.model_python,
            model_path=self.spec.model_path,cuda_visible_devices='0'))
        with ThreadedRuntime(self.rows) as runtime, ExitStack() as stack:
            self.real_formatting(runtime)
            original_ready = AsyncCoreModelActor._ready_report
            def CPU_readiness(actor):
                return dict(original_ready(actor), pid=actor.engine.process.pid)
            stack.enter_context(patch.object(AsyncCoreModelActor,'_ready_report',CPU_readiness))
            state=[False]
            runtime.ray.is_initialized=lambda:state[0]
            runtime.ray.init=lambda **kw:state.__setitem__(0,True)
            runtime.ray.shutdown=lambda:state.__setitem__(0,False)
            stack.enter_context(patch.object(chain_runtime,'check_external_lease',return_value={}))
            stack.enter_context(patch.object(lotus_records_movie,'model_preflight',return_value={}))
            report=session.run_movie_session(plan,context,self.root/'public-session')
            self.assertEqual(report['status'],'completed')
            self.assertEqual(len(runtime.async_actors),1)
            self.assertEqual(len(report['owners']),1)
            self.assertEqual([q['task'] for q in report['queries']],[q['task'] for q in queries])
            self.assert_prompts(runtime,[(q['query_id'],int(q['task'][-1])) for q in queries])
            self.assertFalse(state[0])

    def test_async4_refills_after_fast_results_while_first_call_still_active(self):
        rows=[dict(row_id='flow-'+str(i),source_position=i,reviewId='same',id='taken_3',
                   reviewText='hold' if i==0 else 'slow') for i in range(7)]
        with ThreadedRuntime(rows) as runtime:
            owner=self.owner()
            try:
                stream=owner.evaluate(Source(rows),query_id='async4-flow',method=MovieTask(3).method(MODEL))
                fast=[next(stream) for _ in range(5)]
                self.assertNotIn(0,{row['source_position'] for row in fast})
                actor=runtime.async_actors[0].instance
                self.assertGreaterEqual(actor.engine._sequence,6)
                self.assertGreaterEqual(actor.engine.snapshot()['active_requests'],1)
                self.assertEqual(stream.peak_usage['held_tasks'],4)
                self.assertEqual(owner.transport.backend._maximum,4)
                owner._actors.cancel_core(TaskKey(stream.session.session_id,0)).future().result(2)
                rest=list(stream)
                self.assertEqual({row['source_position'] for row in fast+rest},set(range(7)))
                self.assertEqual(owner.core.capacity.usage().held_tasks,0)
            finally:
                self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_cancel_after_method_switch_preserves_unknown_and_retires_only_closed_producer(self):
        rows=[dict(row_id='cancelled',source_position=0,reviewId='r',id='taken_3',reviewText='hold-unknown')]
        cancelled=[False]
        with ThreadedRuntime(rows) as runtime:
            owner=self.owner()
            try:
                stream=owner.evaluate(Source(rows),query_id='cancel-q3',method=MovieTask(3).method(MODEL),
                    cancelled=lambda:cancelled[0])
                stream._fill()
                actor=runtime.async_actors[0].instance
                deadline=time.monotonic()+3
                while actor.engine.snapshot()['accepted_requests']!=1:
                    stream.session.advance(1)
                    if time.monotonic()>=deadline:self.fail('CPU request was not accepted')
                    owner.core.wake.wait(owner.core.wake.generation,0.005)
                cancelled[0]=True
                with self.assertRaises(InterruptedError):next(stream)
                with self.assertRaises(RuntimeError):
                    owner.evaluate(Source(rows),query_id='too-soon',method=MovieTask(2).method(MODEL))
                closed=owner.close(timeout=3)
                self.assertEqual(closed['status'],'closed')
                self.assertTrue(closed['producer_retirement']['confirmed'])
                self.assertEqual(closed['core_usage']['held_tasks'],0)
                self.assertEqual(closed['retained_remote_inputs'],0)
                ledger=CallLedger(Source(rows).expected('cancel-q3'),1)
                for value in owner.last_query_report['unconsumed_call_results']:ledger.record(value)
                result=ledger.report()
                self.assertEqual(result['counts']['unknown'],1)
                self.assertEqual(result['usage']['known_prompt_tokens'],11)
                self.assertEqual(owner.close(timeout=1)['producer_retirement'],closed['producer_retirement'])
                self.assertEqual(len(runtime.async_actors),1)
            finally:
                owner.close(timeout=2)


if __name__=='__main__':
    unittest.main()
