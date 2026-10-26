"""One-pass admission safety and progress, without model or hardware calls."""
from dataclasses import replace
import time
import unittest
from src.scheduling.core.models import AdmissionObservation
from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider, ServiceMetricsSnapshot
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig, ResourceFeedbackController


class FeedbackControlTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.config = ResourceFeedbackConfig(window_s=1,min_samples=2,min_remaining=4,exploration_s=100,allow_exploration=True)
        self.c = ResourceFeedbackController(8,2,self.config,clock=lambda:self.now)
        self.c.reset(2,100)

    def tick(self, *, dt=1, completed=0, pending=90, kv=.1, waiting=0, preemptions=0, fresh=True, transition=0, memory=None, input_work=None, hol=None):
        self.now += dt
        return self.c.update(AdmissionObservation(self.now,fresh,2,2,waiting,kv,
            completed_items_total=completed,pending_items=pending,preemptions_total=preemptions,
            transition_pending=transition,memory_pressure=memory,hol_age_s=hol,
            completed_input_work_total=completed*100 if input_work is None else input_work,
            completed_output_work_total=completed*4))

    def test_missing_stale_and_zero_progress_do_not_grow(self):
        for _ in range(20):self.tick(fresh=False,completed=30)
        self.assertEqual(self.c.current_window,2)
        self.c.update(AdmissionObservation(self.now+1,True,2,None,None,None))
        for _ in range(8):self.tick()
        self.assertEqual(self.c.current_window,2)

    def test_persistent_pressure_shrinks_and_does_not_claim_to_cancel_inflight(self):
        self.c.reset(8,100)
        self.assertEqual(self.tick(kv=.95).window,8)
        value=self.tick(kv=.95)
        self.assertEqual((value.window,value.reason),(4,'resource_pressure'))
        self.assertEqual(self.tick(kv=.95).window,2)
        self.assertLessEqual(len(self.c.report()['history']),64)

    def test_native_preemption_and_memory_pressure_reduce_without_completed_samples(self):
        self.tick()
        self.assertEqual(self.tick(preemptions=1).window,1)
        self.c.reset(8,100)
        self.assertEqual(self.tick(memory=True).window,4)

    def test_growth_only_after_progress_and_old_generation_settles(self):
        self.tick(completed=0)
        self.tick(completed=4)
        self.tick(completed=8)
        grown=self.tick(completed=12)
        self.assertEqual(grown.window,4)
        for n in range(3):
            self.assertEqual(self.tick(completed=20+n*8,transition=1).window,4)
        self.assertEqual(self.tick(completed=50).reason,'transition_settled')
        self.assertEqual(self.tick(completed=60).reason,'cooldown')
        self.assertEqual(self.c.current_window,4)

    def test_short_tail_and_disabled_exploration_never_add_work_or_wait_for_samples(self):
        for total,pending,enabled in ((3,3,True),(100,2,True),(100,99,False)):
            self.c=ResourceFeedbackController(8,2,replace(self.config,allow_exploration=enabled),clock=lambda:self.now)
            self.c.reset(2,total)
            for n in range(10):self.tick(completed=n*3,pending=pending)
            self.assertEqual(self.c.current_window,2)
            self.assertEqual(self.c.report()['changes'],0)

    def test_expired_budget_blocks_growth_but_not_pressure_reduction(self):
        self.c=ResourceFeedbackController(8,4,replace(self.config,exploration_s=.5),clock=lambda:self.now)
        self.c.reset(4,100)
        self.tick(completed=2);self.assertEqual(self.tick(completed=20).reason,'exploration_budget')
        self.tick(kv=.99);self.assertEqual(self.tick(kv=.99).window,2)

    def test_no_gain_stops_or_reverts_instead_of_climbing_to_capacity(self):
        self.c.reset(2,1000)
        self.tick(completed=0);self.tick(completed=20);self.tick(completed=40)
        self.assertEqual(self.tick(completed=60).window,4)
        self.tick(completed=80);self.tick(completed=100)
        self.tick(completed=101);self.tick(completed=102);decision=self.tick(completed=104)
        self.assertEqual(decision.reason,'performance_rollback')
        self.assertEqual(decision.window,2)
        for n in range(10):self.tick(completed=104+n*10)
        self.assertEqual(self.c.current_window,2)

    def test_repeated_native_sequence_is_not_refreshed_by_polling(self):
        sample=ServiceMetricsSnapshot(1,0,.1,source_sequence=1,source_observed_at_s=time.monotonic())
        provider=NonBlockingMetricsObservationProvider(lambda:sample,poll_interval_s=.01,stale_after_s=.1,close_timeout_s=1)
        try:
            self.assertTrue(provider.wait_until_sampled(1))
            self.assertTrue(provider.latest(1).fresh)
            time.sleep(.04)
            self.assertFalse(provider.latest(1).fresh)
        finally:provider.close()
        self.assertFalse(provider.is_running)

    def test_old_source_timestamp_and_future_source_timestamp_are_not_fresh(self):
        for timestamp in (time.monotonic()-10,time.monotonic()+10):
            provider=NonBlockingMetricsObservationProvider(lambda:ServiceMetricsSnapshot(1,0,.1,
                source_sequence=1,source_observed_at_s=timestamp),poll_interval_s=.01,close_timeout_s=1)
            try:
                provider.wait_until_sampled(1);self.assertFalse(provider.latest(1).fresh)
            finally:provider.close()

    def test_requested_memory_feedback_missing_does_not_mean_pressure_free(self):
        self.c=ResourceFeedbackController(8,2,replace(self.config,memory_floor_bytes=4096),clock=lambda:self.now)
        self.c.reset(2,100)
        for i in range(10):self.tick(completed=i*8)
        self.assertEqual(self.c.current_window,2)
        self.assertIn('missing_memory_feedback',self.c.report()['reasons'])

    def test_consumer_backpressure_never_triggers_growth(self):
        for i in range(8):
            self.now+=1
            o=AdmissionObservation(self.now,True,2,2,0,.1,completed_items_total=i*10,
                pending_items=90,consumer_backpressure=True)
            self.c.update(o)
        self.assertEqual(self.c.current_window,2)
        self.assertIn('consumer_backpressure',self.c.report()['reasons'])

    def test_token_mix_and_head_of_line_changes_are_not_counted_as_concurrency_gain(self):
        for use_token in (True,False):
            self.c.reset(2,1000)
            for count in (0,4,8,12):self.tick(completed=count,hol=1)
            self.assertEqual(self.c.current_window,4)
            self.tick(completed=16,hol=1);self.tick(completed=20,hol=1)
            for count in (24,28):
                result=self.tick(completed=count,hol=1 if use_token else 4,
                    input_work=2000+(count-20)*(200 if use_token else 100))
            self.assertEqual(result.reason,'work_mix_changed' if use_token else 'waiting_regression')
            self.assertEqual(result.window,2)
