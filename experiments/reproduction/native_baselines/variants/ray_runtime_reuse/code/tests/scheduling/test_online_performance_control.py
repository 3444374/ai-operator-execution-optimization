"""Disjoint delivered-work traces; no hardware performance claims."""
from dataclasses import replace
import unittest
from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig,ResourceFeedbackController


class OnlinePerformanceTests(unittest.TestCase):
    def setUp(self):
        self.now=0.;self.count=self.inputs=self.outputs=0
        self.config=ResourceFeedbackConfig(window_s=1,min_samples=2,min_remaining=4,
            exploration_s=100,allow_exploration=True)
        self.c=ResourceFeedbackController(16,4,self.config,clock=lambda:self.now)
        self.c.reset(4,10000)

    def tick(self,n=10,*,iw=100,ow=4,pending=9000,transition=0,kv=.1,backpressure=False,known=True):
        self.now+=1;self.count+=n;self.inputs+=n*iw;self.outputs+=n*ow
        return self.c.update(AdmissionObservation(self.now,True,self.c.current_window,
            self.c.current_window,0,kv,completed_items_total=self.count,pending_items=pending,
            transition_pending=transition,consumer_backpressure=backpressure,
            completed_input_work_total=self.inputs if known else None,
            completed_output_work_total=self.outputs if known else None))

    def probe(self):
        for _ in range(4):result=self.tick()
        self.assertEqual((result.window,result.reason),(6,'performance_probe'))
        self.assertEqual(self.tick(transition=2).reason,'transition')
        self.assertEqual(self.tick().reason,'transition_settled')
        self.assertEqual(self.tick().reason,'cooldown')

    def test_progress_and_remaining_cost_required_before_first_probe(self):
        self.tick();self.tick()
        self.assertEqual(self.c.current_window,4)
        self.tick();self.assertEqual(self.tick().window,6)
        self.assertEqual(self.c.report()['accepted_steps'],0)

    def test_confirmed_gain_continues_but_does_not_claim_full_query_cache_measurement(self):
        self.probe();self.tick(20);decision=self.tick(20)
        self.assertEqual(decision.window,8)
        report=self.c.report();self.assertEqual(report['accepted_steps'],1)
        self.assertEqual(report['recommended_window'],6)
        self.assertIn('not a measured fixed full-query',report['recommendation_scope'])

    def test_flat_plateau_reverts_once_then_stops(self):
        self.probe();self.tick();d=self.tick()
        self.assertEqual((d.window,d.reason),(4,'local_plateau'))
        for _ in range(10):self.tick(50)
        self.assertEqual(self.c.report()['changes'],2)

    def test_regression_rolls_back_without_retrying_any_work(self):
        self.probe();self.tick(4);d=self.tick(4)
        self.assertEqual((d.window,d.reason),(4,'performance_rollback'))
        self.assertGreater(self.c.report()['estimated_stable_trial_loss_s'],0)

    def test_overlapping_noise_is_not_accepted_as_a_gain(self):
        self.probe()
        for n in (8,12,9,11):d=self.tick(n)
        self.assertEqual((d.window,d.reason),(4,'inconclusive_noise'))
        self.assertEqual(self.c.report()['accepted_steps'],0)

    def test_input_or_output_mix_change_does_not_prove_concurrency_benefit(self):
        for key in ('iw','ow'):
            with self.subTest(key=key):
                self.setUp();self.probe()
                self.tick(20,**{key:200});d=self.tick(20,**{key:200})
                self.assertEqual((d.window,d.reason),(4,'work_mix_changed'))

    def test_warm_progress_transition_and_backpressure_are_not_gain_samples(self):
        self.tick(1000);self.tick(1000);self.tick();self.tick()
        self.assertEqual(self.c.report()['measurements'][0]['rate'],10)
        self.assertEqual(self.tick(500,transition=2).reason,'transition')
        self.tick();self.tick()
        for _ in range(4):self.tick(200,backpressure=True)
        self.assertEqual(self.c.report()['accepted_steps'],0)

    def test_short_query_tail_and_fast_work_do_not_wait_for_extra_samples(self):
        self.c.reset(4,3)
        for _ in range(3):self.tick(1,pending=2)
        self.assertEqual(self.c.current_window,4)
        self.setUp();self.c.reset(4,75)
        for _ in range(4):d=self.tick()
        self.assertEqual(d.reason,'insufficient_remaining_benefit')
        self.assertEqual(self.c.report()['changes'],0)
        self.setUp();self.probe();d=self.tick(pending=2)
        self.assertEqual((d.window,d.reason),(4,'short_tail_or_fixed'))

    def test_missing_work_stops_exploration_but_pressure_still_shrinks(self):
        for _ in range(5):self.tick(known=False)
        self.assertEqual(self.c.current_window,4)
        self.tick(known=False,kv=.99);self.assertEqual(self.tick(known=False,kv=.99).window,2)

    def test_budget_and_reset_do_not_leak_a_prior_trial(self):
        self.probe();self.now=101;d=self.tick()
        self.assertEqual((d.window,d.reason),(4,'exploration_budget'))
        self.c.reset(2,1000);r=self.c.report()
        self.assertEqual((r['changes'],r['accepted_steps'],r['exploration_wall_s']),(0,0,0))
        self.assertEqual(r['window'],2)

    def test_growth_at_capacity_is_not_a_hardware_plateau(self):
        self.c.reset(16,1000);self.assertEqual(self.tick().reason,'capacity_limit')
        self.assertFalse(self.c.report()['learned_global_optimum'])

    def test_compatible_pressure_only_profile_does_not_start_performance_trials(self):
        c=ResourceFeedbackController(16,8,replace(self.config,allow_exploration=False),clock=lambda:self.now)
        self.c=c;c.reset(8,10000)
        for _ in range(8):self.tick(100)
        self.assertEqual(c.report()['changes'],0)
        self.assertEqual(c.report()['measurements'],[])

    def test_stale_or_transition_frames_do_not_extend_a_trial_budget(self):
        for stale in (True,False):
            with self.subTest(stale=stale):
                self.setUp();self.probe();self.now=101
                o=AdmissionObservation(self.now,not stale,6,6,0,.1,
                    completed_items_total=self.count,pending_items=9000,transition_pending=6)
                d=self.c.update(o)
                self.assertEqual((d.window,d.reason),(4,'exploration_budget'))
                self.assertFalse(self.c.report()['unconfirmed_trial'])

    def test_a_trial_requires_enough_time_for_its_minimum_completed_samples(self):
        self.config=replace(self.config,window_s=.1,min_samples=16)
        self.c=ResourceFeedbackController(16,4,self.config,clock=lambda:self.now);self.c.reset(4,85)
        for _ in range(4):d=self.tick(16)
        self.assertEqual(d.reason,'insufficient_remaining_benefit')
        self.assertEqual(self.c.current_window,4)
        self.setUp();self.c=ResourceFeedbackController(16,4,replace(self.config,exploration_s=6),clock=lambda:self.now)
        self.c.reset(4,10000)
        for _ in range(4):d=self.tick()
        self.assertEqual((d.window,d.reason),(4,'exploration_budget'))

    def test_one_changed_work_window_cannot_be_hidden_by_range_overlap(self):
        self.probe();self.tick(20,iw=100);d=self.tick(20,iw=1000)
        self.assertEqual((d.window,d.reason),(4,'work_mix_changed'))
        self.assertEqual(self.c.report()['accepted_steps'],0)
