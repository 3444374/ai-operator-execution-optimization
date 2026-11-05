from src.baselines.text.movie_tasks import MovieTask
"""Production Core + real CPU child; Ray/Daft and model are explicit fixtures."""
import json
import unittest
from unittest.mock import patch
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.semantic_methods.lotus.records import RecordsResources, FilterMethod
from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider.test_lotus_records_core import Source
from tests.serving.test_native_resource_feedback import CPUFeedbackProcess


class CompletedWorkTests(unittest.TestCase):

    def stream(self):
        from types import SimpleNamespace
        from src.semantic_methods.lotus.records import identity
        value=identity('work-query','row',0)
        stream=object.__new__(core._CoreRows)
        stream.expected={value['call_id']:value};stream.received=set()
        stream.owner=SimpleNamespace(_feedback=None);stream.completed_work=(0,0,0)
        return stream,dict(value,status='succeeded')

    def test_existing_optional_usage_can_remain_missing_without_a_business_error(self):
        stream,value=self.stream()
        self.assertEqual(stream._value(json.dumps(value))['status'],'succeeded')
        self.assertEqual(stream.completed_work,(1,None,None))

    def test_actual_work_is_counted_once_after_call_identity_validation(self):
        stream,value=self.stream();value.update(prompt_tokens=12,completion_tokens=4)
        stream._value(json.dumps(value));self.assertEqual(stream.completed_work,(1,12,4))
        with self.assertRaises(ValueError):stream._value(json.dumps(value))
        self.assertEqual(stream.completed_work,(1,12,4))



class CoreMethodReuseTests(unittest.TestCase):

    def rows(self,n,text='fast'):
        return [dict(row_id='r'+str(i),source_position=i,reviewId='review-'+str(i),
            id='taken_3',reviewText=text) for i in range(n)]

    def owner(self, *, method):
        return core.create_records_core(method,
            EngineSpec('/tmp/CPU-fixture','/tmp/model-python','/tmp/driver-python',max_num_seqs=8),
            GenerationSpec(),RecordsResources(cpu_workers=2,prepare_batch_rows=1,model_batch_rows=1,
                max_active_batches=4,startup_timeout_s=5,query_timeout_s=10),
            profile=dict(mode='async',max_inflight=1))

    def test_actual_new_method_prompts_Q1_Q3_Q2_Q3_reuse_one_model(self):
        from src.baselines.text.movie_tasks import MovieTask
        from src.semantic_methods.lotus import engine as methods
        from src.semantic_methods.lotus.records import identity
        formatter,bind=methods.LotusFilterMethod,methods.bind_filter_method
        tasks=[MovieTask(n).method('fixture') for n in (1,3,2,3)]
        rows=self.rows(4);captured=[]
        with ThreadedRuntime(rows) as fake,patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess), \
                patch.object(methods,'LotusFilterMethod',formatter),patch.object(methods,'bind_filter_method',side_effect=bind):
            owner=self.owner(method=tasks[0])
            try:
                for index,method in enumerate(tasks):
                    qid='method-'+str(index)
                    stream=owner.evaluate(Source(rows),query_id=qid,method=method)
                    values=list(stream)
                    prepared=[row for event in fake.events if event[:2]==('submit','AsyncCoreModelActor.try_submit_core')
                              for row in event[3][0].value.result(2) if row['query_id']==qid]
                    expected=formatter(bind(method)).prepare_rows([dict(row,**identity(qid,row['row_id'],row['source_position'])) for row in rows])
                    actual={row['row_id']:row['messages_json'] for row in prepared}
                    self.assertEqual(set(actual), {row['row_id'] for row in rows})
                    self.assertEqual(actual,{row['row_id']:row['messages_json'] for row in expected})
                    captured.append(actual['r0']);self.assertEqual(len(values),4)
                self.assertNotEqual(captured[0],captured[1]);self.assertNotEqual(captured[1],captured[2])
                self.assertEqual(captured[1],captured[3]);self.assertEqual(len(fake.async_actors),1)
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,16)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')
