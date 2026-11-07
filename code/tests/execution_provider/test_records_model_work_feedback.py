"""Model receipt work progresses independently of ordinary result consumption."""
import json
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_core import _CoreRows, create_records_core
from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
from src.scheduling.core.models import AdmissionObservation
from src.scheduling.core.session_contract import TaskKey
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from tests.execution_provider import test_lotus_async_token_batches as base
from tests.execution_provider.test_lotus_records_core import Source
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider.test_records_request_feedback import CPUResourceProcess


class ModelWorkFeedbackTests(unittest.TestCase):
    setUp = base.AsyncTokenBatchTests.setUp

    def test_retained_success_is_counted_before_consumption_even_without_native_sample(self):
        rows = [dict(row_id='row-'+str(i),source_position=i,reviewId=str(i),
                     id='taken_3',reviewText='fast') for i in range(2)]
        self.helper.config.write_text(json.dumps(dict(responses={
            'filter|'+row['row_id']:dict(text='True') for row in rows})))
        engine = EngineSpec(str(self.helper.config),sys.executable,sys.executable,max_model_len=4096)
        resources = RecordsResources(prepare_batch_rows=1,model_batch_rows=1,max_active_batches=3,
                                     query_timeout_s=10,startup_timeout_s=8)
        profile = dict(mode='async',admission=dict(kind='resource_feedback',poll_interval_s=.01,
            window_s=.04,max_sample_age_s=.3,min_samples=1,measurement_windows=2))
        with ThreadedRuntime(rows), patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUResourceProcess):
            owner = create_records_core(FilterMethod('fixture','Keep {reviewText}'),engine,
                                        GenerationSpec(),resources,profile=profile)
            stream = owner.evaluate(Source(rows),query_id='unconsumed-work')
            deliveries = []
            try:
                deadline = time.monotonic()+4
                stream._fill()
                while not deliveries:
                    progress = stream.session.advance(1)
                    deliveries.extend(progress.deliveries)
                    self.assertLess(time.monotonic(),deadline)
                    owner.core.wake.wait(progress.generation,.005)
                self.assertEqual(len(stream.received),0)
                first = json.loads(deliveries[0].result)
                before = owner.core.capacity.usage()
                feedback = owner._feedback
                missing = AdmissionObservation(time.monotonic(),False,before.active_requests,None,None,None)
                with patch.object(feedback.provider,'latest_with_source',return_value=(missing,None)), \
                        patch.object(feedback.controller,'update_supply',wraps=feedback.controller.update_supply) as update:
                    feedback.next_poll = 0
                    feedback.observe(stream)
                observation = update.call_args.args[0]
                self.assertEqual(observation.completed_items_total,1)
                self.assertEqual(observation.completed_input_work_total,first['prompt_tokens'])
                self.assertEqual(observation.completed_output_work_total,first['completion_tokens'])
                self.assertFalse(observation.fresh)
                self.assertEqual(owner.core.capacity.usage(),before)
                self.assertEqual(len(stream.received),0)
                for delivery in deliveries:
                    stream._value(delivery.result)
                    owner.transport.acknowledge(delivery.key)
                    stream.session.release((delivery.lease_id,))
                self.assertEqual(len(list(stream)),1)
                self.assertEqual(owner.core.capacity.usage().held_tasks,0)
            finally:
                stream.close()
                owner.close(timeout=3)

    def synthetic(self, retained):
        stream = _CoreRows.__new__(_CoreRows)
        stream.session = SimpleNamespace(session_id=7)
        stream.received = {'delivered'}
        stream.completed_work = (1,10,2)
        stream.completed_effective_work = (1,8,2)
        stream.owner = SimpleNamespace(transport=SimpleNamespace(failure_receipts=lambda:tuple(retained)))
        return stream

    def test_consumed_and_foreign_receipts_do_not_double_count_or_cross_queries(self):
        value = dict(call_id='delivered',status='succeeded',prompt_tokens=10,completion_tokens=2,
                     prompt_computed_tokens=8,prompt_cached_tokens=2)
        retained = [(TaskKey(7,0),json.dumps(value).encode()),
                    (TaskKey(6,1),json.dumps(dict(value,call_id='old-query')).encode()),
                    (TaskKey(7,1),json.dumps(dict(value,call_id='pending')).encode())]
        stream = self.synthetic(retained)
        expected = dict(completed_items_total=2,completed_input_work_total=20,
            completed_output_work_total=4,completed_computed_input_work_total=16,
            completed_cached_input_work_total=4)
        self.assertEqual(stream.completed_model_work(),expected)
        self.assertEqual(stream.completed_model_work(),expected)
        self.assertEqual(stream.completed_work,(1,10,2))
        self.assertEqual(stream.received,{'delivered'})
        self.assertEqual(len(retained),3)

    def test_unknown_failed_or_missing_costs_do_not_become_zero_work(self):
        for status in ('unknown','failed','succeeded'):
            with self.subTest(status=status):
                row = dict(call_id='pending',status=status,prompt_tokens=5,completion_tokens=1)
                stream = self.synthetic([(TaskKey(7,1),json.dumps(row).encode())])
                work = stream.completed_model_work()
                self.assertEqual(work['completed_items_total'],2 if status=='succeeded' else 1)
                self.assertIsNone(work['completed_computed_input_work_total'])
                self.assertIsNone(work['completed_cached_input_work_total'])
                if status!='succeeded':
                    self.assertIsNone(work['completed_input_work_total'])
                    self.assertIsNone(work['completed_output_work_total'])


if __name__=='__main__':
    unittest.main()
