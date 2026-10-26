"""One Core model owner, query-scoped limits, public increasing search; CPU only."""
import csv
import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource,prepare_movie_source
from src.baselines.text.movie_tasks import MovieTask
from src.execution_provider.adapters import lotus_records_core as core
from src.semantic_methods.lotus import engine as methods
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec
from src.scheduling.runtime import calibration as runtime
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime


class CoreQueryCapacityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.formatter=methods.LotusFilterMethod;cls.bind=staticmethod(methods.bind_filter_method)
        cls.bind(MovieTask(1).method('openai/cpu-fixture'))

    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.root=Path(temp.name)
        self.method=MovieTask(1).method('openai/cpu-fixture')
        self.spec=EngineSpec('/tmp/CPU-fixture','/tmp/model-python','/tmp/driver-python',
            max_num_seqs=8,max_model_calls=512)
        self.resources=RecordsResources(cpu_workers=2,prepare_batch_rows=1,model_batch_rows=1,
            max_active_batches=8,startup_timeout_s=5,query_timeout_s=8)

    def source(self,name='fast',n=12):
        path=self.root/(name+'.csv')
        with path.open('w',newline='') as stream:
            writer=csv.writer(stream);writer.writerow(['reviewId','id','reviewText','scoreSentiment'])
            writer.writerows([[str(i),'taken_3',name+' '+str(i),'POSITIVE'] for i in range(n)])
        manifest=prepare_movie_source(path,self.root/(name+'-input'),
            expected_csv_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),max_rows=n,task='movie-q1')
        return MovieRecordSource.from_manifest(manifest)

    def real_formatting(self,fake):
        fake.stack.enter_context(patch.object(methods,'LotusFilterMethod',self.formatter))
        fake.stack.enter_context(patch.object(methods,'bind_filter_method',side_effect=self.bind))

    def owner(self):
        return core.create_records_core(self.method,self.spec,GenerationSpec(),self.resources,
            profile=dict(mode='async',max_inflight=8))

    def test_public_search_and_cache_target_reuse_one_Core_and_model_with_actual_query_limits(self):
        source=self.source();queries=[];profiles=[];results=[];targets=[]
        identity=dict(hardware=dict(fixture=True),model=dict(fixed=True),generation=dict(tokens=8),
            execution=dict(model_max_num_seqs=8,model_owner_capacity=8,prepare=2),
            workload=dict(manifest=hashlib.sha256(source.manifest_path.read_bytes()).hexdigest()),
            measurement=dict(work_unit='rows',objective='synthetic complete-query latency'))
        with ThreadedRuntime(source.rows()) as fake:
            self.real_formatting(fake);owner=self.owner();ready=owner.start()
            def query(config,label):
                query_id='CPU-probe-'+label;queries.append(query_id)
                stream=owner.evaluate(source,query_id=query_id,max_inflight=config['max_inflight'])
                rows=list(stream);profiles.append(stream.execution_profile);results.extend(rows)
                self.assertEqual(stream.session.limits.active_requests,config['max_inflight'])
                self.assertLessEqual(stream.peak_usage['held_tasks'],config['max_inflight'])
                self.assertEqual({r['call_id'] for r in rows},{r['call_id'] for r in source.expected(query_id)})
                self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                self.assertEqual(owner.transport.retained_refs(),0)
                return rows
            def measure(config,*,repeat_index,deadline_monotonic,work_limit):
                query(config,str(config['max_inflight'])+'-r'+str(repeat_index))
                return runtime.Measurement(12/config['max_inflight'],'ok',True,12)
            def execute(config):
                targets.append(config);return query(config,'target'+str(len(targets)))
            options=dict(cache_dir=self.root/'cache',search=runtime.IncreasingSearch('max_inflight',
                dict(model_max_num_seqs=8,owner_record_slots=8)),measure=measure,
                budget=runtime.ScanBudget(30,200),work_per_sample=12)
            try:
                first=runtime.run_calibrated(identity,[dict(max_inflight=1,cpu_workers=2)],execute,**options)
                hit=runtime.run_calibrated(identity,[dict(max_inflight=1,cpu_workers=2)],execute,**options)
                self.assertEqual(first['calibration']['search']['stop_reason'],'capacity_limit')
                self.assertFalse(first['calibration']['search']['platform_confirmed'])
                self.assertTrue(hit['calibration']['cache_hit']);self.assertEqual(len(queries),14)
                self.assertEqual(targets,[dict(max_inflight=8,cpu_workers=2)]*2)
                self.assertEqual({p['owner_max_inflight'] for p in profiles},{8})
                self.assertEqual(len(fake.async_actors),1)
                self.assertEqual(len([e for e in fake.events if e[:2]==('actor','_PrepareActor')]),2)
                self.assertEqual({row['engine_uid'] for row in results},{ready['engine_uid']})
                messages={}
                for e in fake.events:
                    if e[:2]==('submit','AsyncCoreModelActor.run_core'):
                        for row in e[3][0].value.result(2):messages.setdefault(row['row_id'],set()).add(row['messages_json'])
                self.assertTrue(all(len(values)==1 for values in messages.values()))
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,168)
            finally:closed=owner.close(timeout=3)
            self.assertEqual(closed['status'],'closed')
            self.assertEqual(fake.async_actors[0].instance.engine.process.returncode,0)

    def test_lower_query_limit_reaches_real_CPU_engine_and_cancel_keeps_known_usage(self):
        source=self.source('hold',4)
        with ThreadedRuntime(source.rows()) as fake:
            # This CPU child gates on the first message starting with "hold";
            # use its explicit formatter fixture here, not LOTUS's system turn.
            owner=self.owner();owner.start()
            try:
                stream=owner.evaluate(source,query_id='CPU-held',max_inflight=2);stream._fill()
                until=time.monotonic()+3;engine=fake.async_actors[0].instance.engine
                while engine.snapshot()['accepted_requests']<2:
                    stream.session.advance(1)
                    self.assertLess(time.monotonic(),until)
                    owner.core.wake.wait(owner.core.wake.generation,.005)
                self.assertEqual(engine.snapshot()['active_requests'],2)
                self.assertEqual(owner.core.capacity.usage().held_tasks,2)
                with self.assertRaises(RuntimeError):  # Existing borrowed-work guard may reject first.
                    owner.evaluate(source,query_id='cannot-change-active',max_inflight=4)
                stream.close()
                while owner.core.capacity.usage().held_tasks:
                    owner.core.reap(owner.capacity.step_actions)
                    self.assertLess(time.monotonic(),until)
                    owner.core.wake.wait(owner.core.wake.generation,.005)
                stream.collect_receipts()
                self.assertEqual(len(stream.unconsumed_call_results),2)
                self.assertEqual(sum(r['prompt_tokens'] for r in stream.unconsumed_call_results),22)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_invalid_query_cap_rejects_before_model_and_default_query_contract_is_unchanged(self):
        source=self.source()
        with ThreadedRuntime(source.rows()) as fake:
            self.real_formatting(fake);owner=self.owner();original=dict(owner.execution_profile)
            for cap in (0,9,True):
                with self.assertRaises(ValueError):owner.evaluate(source,query_id='invalid',max_inflight=cap)
            self.assertEqual(fake.async_actors,[])
            try:
                stream=owner.evaluate(source,query_id='old-default');self.assertEqual(len(list(stream)),12)
                self.assertEqual(stream.execution_profile,original)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_ordinary_policy_cache_default_offline_and_online_hit_apply_to_one_real_CPU_model(self):
        source=self.source(n=6);queries=[];measurements=[];results=[]
        identity=dict(hardware=dict(fixture=True),model=dict(revision='CPU-fixture'),
            generation=dict(fixed=True),execution=dict(model_max_num_seqs=8,owner_capacity=8,prepare=2),
            workload=dict(manifest=hashlib.sha256(source.manifest_path.read_bytes()).hexdigest(),
                method=self.method.describe()),
            measurement=dict(objective='full_query_seconds fixture',work_unit='rows'))
        candidates=[dict(max_inflight=1,cpu_workers=2),dict(max_inflight=4,cpu_workers=2)]
        common=dict(default=candidates[0],cache_dir=self.root/'policy-cache',work_per_sample=6)
        with ThreadedRuntime(source.rows()) as fake:
            self.real_formatting(fake);owner=self.owner();request_start=time.monotonic();ready=owner.start()
            def execute(configuration,*,deadline_monotonic=None,work_limit=None):
                if work_limit is not None:self.assertEqual(work_limit,6)
                queries.append(configuration['max_inflight']);qid='CPU-policy-'+str(len(queries))
                cancelled=None if deadline_monotonic is None else lambda:time.monotonic()>=deadline_monotonic
                stream=owner.evaluate(source,query_id=qid,max_inflight=configuration['max_inflight'],cancelled=cancelled)
                rows=list(stream);results.extend(rows)
                self.assertEqual(stream.session.limits.active_requests,configuration['max_inflight'])
                self.assertEqual({r['call_id'] for r in rows},{r['call_id'] for r in source.expected(qid)})
                self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                self.assertEqual(owner.transport.retained_refs(),0)
                return rows
            def measure(configuration,*,repeat_index,deadline_monotonic,work_limit):
                measurements.append(configuration['max_inflight'])
                execute(configuration,deadline_monotonic=deadline_monotonic,work_limit=work_limit)
                # Deterministic fixture objective, never a measured performance claim.
                return runtime.Measurement(20/configuration['max_inflight'],'ok',True,6)
            try:
                first=runtime.run_query(identity,candidates,execute,measure=measure,**common)
                self.assertEqual(first['configuration_source'],'explicit_default')
                self.assertEqual(measurements,[]);self.assertEqual(queries,[1])
                offline=runtime.run_query(identity,candidates,execute,mode='offline',measure=measure,
                    budget=runtime.ScanBudget(30,36),**common)
                self.assertEqual(offline['configuration']['max_inflight'],4)
                self.assertEqual(len(queries),7)  # Six calibration queries, no offline target.
                hit=runtime.run_query(identity,candidates,execute,**common)
                self.assertEqual(hit['configuration_source'],'calibration_cache')
                online_hit=runtime.run_query(identity,candidates,execute,mode='online',measure=measure,
                    budget=runtime.ScanBudget(30,6),request_started_monotonic=request_start,**common)
                self.assertEqual(online_hit['configuration_source'],'calibration_cache')
                self.assertEqual(online_hit['scan_work_budget'],0)
                self.assertGreaterEqual(online_hit['request_wait_s'],online_hit['total_wall_s'])
                self.assertEqual(queries,[1,1,4,1,4,1,4,4,4])
                self.assertEqual(len(measurements),6)
                self.assertEqual(len(fake.async_actors),1)
                self.assertEqual({row['engine_uid'] for row in results},{ready['engine_uid']})
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,54)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')
            self.assertEqual(fake.async_actors[0].instance.engine.process.returncode,0)


if __name__=='__main__':unittest.main()
