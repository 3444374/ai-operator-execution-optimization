"""New search mode through the real Core/CPU IPC, with library fixtures explicit."""
import tempfile
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.scheduling.runtime.calibration import run_admission_query
from tests.execution_provider import test_core_resource_feedback as fixtures
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider.test_lotus_records_core import Source
from tests.serving.test_native_resource_feedback import CPUFeedbackProcess


class CoreGeometricTests(unittest.TestCase):
    def test_public_callback_and_exact_hint_reuse_one_model_without_duplicate_rows(self):
        f=fixtures.CoreFeedbackTests();rows=f.rows(24)
        identity={k:{'CPU':'ordinary-query'} for k in ('hardware','model','generation','execution','workload','measurement')}
        identity['measurement']['work_unit']='model_calls'
        with tempfile.TemporaryDirectory() as folder,ThreadedRuntime(rows) as fake, \
                patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess):
            owner=f.owner(initial=2,maximum=4,options=dict(search_mode='geometric',min_remaining=32))
            options=owner._feedback.base_options;calls=[]
            def execute(window,*,allow_exploration,search_hint):
                qid='ordinary-'+str(len(calls));calls.append((window,allow_exploration,search_hint))
                stream=owner.evaluate(Source(rows),query_id=qid,max_inflight=window,
                    allow_exploration=allow_exploration,search_hint=search_hint)
                values=list(stream);report=stream.last_close_report
                self.assertEqual(len(values),24);self.assertEqual(len({r['call_id'] for r in values}),24)
                self.assertEqual({r['query_id'] for r in values},{qid})
                self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                self.assertEqual(owner.transport.retained_refs(),0)
                self.assertEqual(report['admission_feedback']['strategy'],'onepass_geometric_v1')
                self.assertEqual(report['admission_feedback']['search_state']['origin'],2)
                return dict(status='completed',reuse_eligible=report['status']=='closed',
                    admission_feedback=report['admission_feedback'])
            try:
                reports=[run_admission_query(identity,execute,initial_window=2,maximum=4,
                    cache_dir=folder,options=options) for _ in range(3)]
                self.assertEqual([r['cache_hit'] for r in reports],[False,True,True])
                self.assertEqual([c[1] for c in calls],[True,False,False])
                self.assertEqual(len(fake.async_actors),1)
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,72)
                self.assertEqual(owner.engine.max_num_seqs,8)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_new_mode_reaches_existing_model_submission_and_keeps_original_prompts(self):
        f=fixtures.CoreFeedbackTests();rows=f.rows(128)
        with ThreadedRuntime(rows) as fake,patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess):
            owner=f.owner(options=dict(search_mode='geometric'))
            try:
                stream=owner.evaluate(Source(rows),query_id='geometric');values=list(stream)
                report=stream.last_close_report['admission_feedback']
                self.assertGreater(report['changes'],0,report)
                self.assertGreater(report['peak_native_running'],1,report)
                self.assertLessEqual(report['peak_native_running'],4)
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,128)
                self.assertEqual(len({r['call_id'] for r in values}),128)
                self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                self.assertEqual(owner.transport.retained_refs(),0)
                self.assertEqual([p['window'] for p in report['search_state']['points']],[2,4])
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')

    def test_new_mode_cancellation_keeps_usage_and_existing_producer_retirement(self):
        f=fixtures.CoreFeedbackTests();original=f.owner
        f.owner=lambda **kw:original(options=dict(search_mode='geometric'),**kw)
        f.test_cancel_stops_sampler_keeps_known_usage_and_original_cleanup()


if __name__=='__main__':unittest.main()
