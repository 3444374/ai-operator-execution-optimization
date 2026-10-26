"""Per-call native work survives CPU IPC and aligns with Core delivered IDs."""
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.scheduling.runtime.calibration import run_admission_query
from src.semantic_methods.lotus.records import identity
from src.serving.backends.vllm_async_engine import VllmAsyncEngine
from src.serving.backends.vllm_engine import GenerationSpec
from tests.execution_provider import test_core_resource_feedback as fixtures
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider.test_lotus_records_core import Source
from tests.serving.test_native_resource_feedback import CPUFeedbackProcess


class WorkProcess(CPUFeedbackProcess):
    @staticmethod
    def _command(spec,descriptors):
        fixture=Path(__file__).resolve().parents[1]/'serving/work_aware_fixture.py'
        return [sys.executable,str(fixture),*map(str,descriptors)]


class NativeWorkTests(unittest.TestCase):
    def converted(self,cached,*,associated=True):
        converter=object.__new__(VllmAsyncEngine);converter.engine_uid='CPU-native'
        converter._llm=SimpleNamespace(get_tokenizer=lambda:None)
        output=SimpleNamespace(request_id='request',finished=True,prompt_token_ids=list(range(10)),
            num_cached_tokens=cached,outputs=[SimpleNamespace(index=0,text='True',token_ids=[3],finish_reason='stop')])
        return converter._convert(identity('cpu','one',0),output,'request' if associated else 'foreign',
            list(range(10)),GenerationSpec(),time.monotonic(),time.monotonic()+2)

    def test_actual_cache_count_is_preserved_and_usage_remains_logical(self):
        result=self.converted(6)
        self.assertEqual((result['prompt_tokens'],result['prompt_cached_tokens'],result['prompt_computed_tokens']),(10,6,4))
        self.assertEqual(result['total_tokens'],11);self.assertEqual(result['status'],'succeeded')

    def test_missing_or_bad_measurement_does_not_fail_business_or_fabricate_zero(self):
        for cached in (None,-1,11,True,1.5):
            value=self.converted(cached)
            self.assertEqual(value['status'],'succeeded');self.assertIsNone(value['prompt_computed_tokens'])
        self.assertEqual(self.converted(0)['prompt_computed_tokens'],10)

    def stream(self):
        stream=object.__new__(core._CoreRows)
        stream.expected={r['call_id']:r for r in [identity('cpu','r0',0),identity('cpu','r1',1)]}
        stream.received=set();stream.completed_work=(0,0,0);stream.completed_effective_work=(0,0,0)
        stream.owner=SimpleNamespace(_feedback=SimpleNamespace(options=SimpleNamespace(work_basis='computed'),
            observe=lambda value:None))
        return stream

    def test_delivery_alignment_out_of_order_once_and_unknown_do_not_add_false_work(self):
        stream=self.stream()
        value=dict(identity('cpu','r1',1),status='succeeded',prompt_tokens=10,completion_tokens=1,
            prompt_computed_tokens=4,prompt_cached_tokens=6)
        stream._value(json.dumps(value));self.assertEqual(stream.completed_effective_work,(1,4,6))
        with self.assertRaises(ValueError):stream._value(json.dumps(value))
        self.assertEqual(stream.completed_effective_work,(1,4,6))
        value.update(identity('cpu','r0',0));value['status']='unknown'
        stream._value(json.dumps(value));self.assertEqual(stream.completed_effective_work,(2,None,None))

    def test_foreign_association_and_missing_cache_cannot_enter_effective_work(self):
        self.assertEqual(self.converted(6,associated=False)['status'],'unknown')
        stream=self.stream();value=dict(identity('cpu','r0',0),status='succeeded',prompt_tokens=10,completion_tokens=1)
        stream._value(json.dumps(value));self.assertEqual(stream.completed_effective_work,(1,None,None))

    def test_sampler_uses_same_delivered_prefix_and_does_not_need_mfu(self):
        from tests.execution_provider.test_core_feedback_cadence import CadenceTests
        f=CadenceTests().make();f.progress=(10,90,0,False);f.completed_work=(10,100,10)
        f.completed_effective_work=(10,40,60)
        snapshot=f.sample()
        self.assertEqual((snapshot.completed_computed_input_work_total,snapshot.completed_cached_input_work_total),(40,60))
        f.completed_effective_work=(11,44,66)
        self.assertIsNone(f.sample().completed_computed_input_work_total)


class CoreWorkTests(unittest.TestCase):
    def test_one_resident_model_new_ids_real_cpu_ipc_and_feature_hint_reuse(self):
        f=fixtures.CoreFeedbackTests();rows=f.rows(19)
        features=dict(work_unit='tokens',input_work=dict(mean=11),output_work=dict(limit=8),
            cache=dict(enabled=True,scope='model_owner'))
        policy_identity={k:{'CPU':'ordinary'} for k in ('hardware','model','generation','execution','workload','measurement')}
        policy_identity['measurement']['work_unit']='model_calls'
        with tempfile.TemporaryDirectory() as folder,ThreadedRuntime(rows) as fake, \
                patch.object(AsyncCoreModelActor,'_process_type',return_value=WorkProcess):
            owner=f.owner(initial=2,maximum=4,options=dict(search_mode='geometric',work_basis='computed',min_remaining=32))
            all_ids=set();queries=[]
            def execute(window,*,allow_exploration,search_hint):
                query='renamed-'+str(len(queries));queries.append(query)
                stream=owner.evaluate(Source(rows),query_id=query,max_inflight=window,
                    allow_exploration=allow_exploration,search_hint=search_hint)
                values=list(stream)
                self.assertEqual(len(values),19)
                ids={v['call_id'] for v in values};self.assertEqual(len(ids),19);self.assertFalse(ids & all_ids);all_ids.update(ids)
                self.assertTrue(all(v['prompt_computed_tokens']==7 and v['prompt_cached_tokens']==4 for v in values))
                self.assertEqual(stream.completed_effective_work,(19,133,76))
                self.assertEqual(owner.core.capacity.usage().held_tasks,0);self.assertEqual(owner.transport.retained_refs(),0)
                return dict(status='completed',reuse_eligible=stream.last_close_report['status']=='closed',
                    admission_feedback=stream.last_close_report['admission_feedback'])
            try:
                reports=[run_admission_query(policy_identity,execute,initial_window=2,maximum=4,
                    cache_dir=folder,options=owner._feedback.base_options,workload_features=features) for _ in range(2)]
                self.assertEqual([r['cache_hit'] for r in reports],[False,True])
                self.assertEqual([r['result']['admission_feedback']['changes'] for r in reports],[0,0])
                self.assertEqual(len(fake.async_actors),1);self.assertEqual(fake.async_actors[0].instance.engine._sequence,38)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_computed_mode_keeps_existing_cancel_and_producer_retirement(self):
        fixture=fixtures.CoreFeedbackTests();original=fixture.owner
        fixture.owner=lambda **kw:original(options=dict(search_mode='geometric',work_basis='computed'),**kw)
        fixture.test_cancel_stops_sampler_keeps_known_usage_and_original_cleanup()


if __name__=='__main__':unittest.main()
