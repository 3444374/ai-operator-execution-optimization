"""Measured incomparable work plus bounded synthetic continuation; no models."""
from dataclasses import replace
import json,tempfile,unittest
from pathlib import Path
from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig,ResourceFeedbackController
from src.scheduling.runtime.calibration import run_admission_query
from tests.scheduling import test_online_performance_control as traces


class WorkMixRecoveryTests(unittest.TestCase):
    def measured(self):
        # The two actual f0 GPU points; future synthetic windows are separate.
        previous=dict(window=4,rate=32.967491205214046,rate_min=28.47567475419207,
            rate_max=37.45930765623602,input_min=124.93103448275862,
            input_max=126.85416666666667,output_min=4.,output_max=4.,
            hol_max=.10586802614852786,samples=2)
        point=dict(window=6,rate=41.725707951522125,rate_min=40.655761158627314,
            rate_max=42.79565474441694,input_min=128.4390243902439,
            input_max=133.47727272727272,output_min=4.,output_max=4.,
            hol_max=.09267096873372793,samples=2)
        now=[0.];c=ResourceFeedbackController(64,4,ResourceFeedbackConfig(
            allow_exploration=True,exploration_s=30),clock=lambda:now[0]);c.reset(4,2000)
        c._reference=previous;c.current_window=6;c._changes=1;c._probe_at=4.4378935769200325
        now[0]=8.510733495932072
        decision=c._compare(point,AdmissionObservation(now[0],True,6,6,0,.006567425569176888,
            completed_items_total=300,pending_items=1700))
        return c,decision

    def case(self,*,seconds=30,max_changes=8):
        t=traces.OnlinePerformanceTests();t.setUp()
        t.config=replace(t.config,exploration_s=seconds,max_changes=max_changes)
        t.c=ResourceFeedbackController(64,4,t.config,clock=lambda:t.now);t.c.reset(4,10000)
        return t

    def invalid(self,t):
        t.probe();t.tick(20,iw=200);d=t.tick(20,iw=200)
        self.assertEqual((d.window,d.reason),(4,'work_mix_changed'))

    def settle(self,t,iw):
        self.assertEqual(t.tick(iw=iw,transition=2).reason,'transition')
        self.assertEqual(t.tick(iw=iw).reason,'transition_settled')
        self.assertEqual(t.tick(iw=iw).reason,'cooldown')

    def test_actual_two_points_are_invalidated_not_accepted_or_finished(self):
        c,d=self.measured();r=c.report()
        self.assertEqual((d.window,d.reason),(4,'work_mix_changed'))
        self.assertEqual((r['recommended_window'],r['accepted_steps'],r['invalidated_comparisons']),(4,0,1))
        self.assertIsNone(r['stop_reason']);self.assertTrue(r['exploration_censored'])
        self.assertEqual(r['incomplete_reason'],'work_mix_changed')
        self.assertFalse(c._growth_stopped);self.assertIsNone(c._reference)
        self.assertIsNone(c._anchor);self.assertEqual(c._windows,[])
        self.assertFalse(any(e['kind']=='observed_gain_band' for e in r['evaluations']))
        self.assertEqual(c.options.min_gain,.03)

    def test_fresh_stable_safe_windows_required_before_retry_and_gain(self):
        t=self.case();self.invalid(t);started=t.c.started;changes=t.c.report()['changes']
        self.settle(t,200)
        self.assertIsNone(t.c._reference)
        self.assertEqual(t.tick(iw=200).reason,'confirming_performance')
        self.assertEqual(t.c.report()['changes'],changes)
        self.assertEqual((t.tick(iw=200).window,t.c._reference['input_min']),(6,200))
        self.assertEqual(t.c.report()['accepted_steps'],0)
        self.settle(t,200);t.tick(20,iw=200);t.tick(20,iw=200)
        self.assertEqual((t.c.report()['accepted_steps'],t.c.report()['recommended_window']),(1,6))
        self.assertEqual(t.c.started,started)

    def test_oscillating_rebuild_work_never_becomes_reference(self):
        t=self.case(seconds=30);self.invalid(t);self.settle(t,200)
        for i in range(40):t.tick(iw=100 if i%2 else 200)
        r=t.c.report()
        self.assertEqual((r['window'],r['accepted_steps'],r['changes']),(4,0,2))
        self.assertGreater(r['reasons'].get('unstable_reference_work',0),0)
        self.assertEqual(r['stop_reason'],'exploration_budget');self.assertTrue(r['exploration_censored'])
        self.assertEqual(r['incomplete_reason'],'work_mix_changed')

    def test_repeated_incomparability_is_bounded_by_original_change_budget(self):
        t=self.case(seconds=100,max_changes=4);self.invalid(t)
        for _ in range(50):t.tick(iw=200 if t.c.current_window==4 else 400)
        r=t.c.report()
        self.assertLessEqual(r['changes'],4);self.assertEqual(r['invalidated_comparisons'],2)
        self.assertEqual(r['accepted_steps'],0);self.assertEqual(r['stop_reason'],'exploration_budget')
        self.assertTrue(r['exploration_censored']);self.assertEqual(r['recommended_window'],4)

    def test_rebuild_and_trials_share_original_elapsed_budget(self):
        t=self.case(seconds=10);self.invalid(t);started=t.c.started
        for _ in range(10):t.tick(iw=200)
        self.assertEqual(t.c.started,started)
        self.assertEqual(t.c.report()['changes'],2)
        self.assertEqual(t.c.report()['stop_reason'],'exploration_budget')

    def test_tail_does_not_rebuild_or_wait_for_extra_work(self):
        t=self.case();self.invalid(t);before=t.c.report()['changes']
        # Existing settle/cooldown may run; no new request is created for samples.
        for _ in range(5):t.tick(iw=200,pending=2)
        r=t.c.report();self.assertEqual(r['changes'],before)
        self.assertEqual(r['stop_reason'],'short_tail_or_fixed');self.assertTrue(r['exploration_censored'])
        self.assertEqual(t.c.sampling_interval_s(2),t.config.window_s)

    def test_pressure_after_invalid_comparison_still_shrinks_admission(self):
        t=self.case();self.invalid(t)
        t.tick(iw=200,kv=.99);d=t.tick(iw=200,kv=.99)
        self.assertEqual((d.window,d.reason),(2,'resource_pressure'))
        self.assertEqual(t.c.report()['recommended_window'],2)

    def test_noise_is_not_relabelled_as_work_incomparability(self):
        t=self.case();t.probe()
        for n in (8,12,9,11):d=t.tick(n)
        self.assertEqual(d.reason,'inconclusive_noise')
        self.assertEqual(t.c.report()['invalidated_comparisons'],0)
        self.assertFalse(t.c.report()['exploration_censored'])

    def test_initial_unstable_work_also_needs_fresh_reference(self):
        t=self.case(seconds=12)
        for i in range(20):t.tick(iw=100 if i%2 else 200)
        r=t.c.report();self.assertEqual((r['changes'],r['recommended_window']),(0,4))
        self.assertGreater(r['reasons'].get('unstable_reference_work',0),0)
        self.assertEqual(r['stop_reason'],'exploration_budget');self.assertTrue(r['exploration_censored'])

    def identity(self):
        identity={k:{'CPU':'mix-recovery'} for k in ('hardware','model','generation','execution','workload','measurement')}
        identity['measurement']['work_unit']='model_calls';return identity

    def test_cache_remembers_incomplete_cause_then_replaces_with_measured_stop(self):
        c,_=self.measured();first=dict(status='completed',reuse_eligible=True,admission_feedback=c.report())
        t=self.case();t.probe();t.tick();t.tick()
        stopped=dict(status='completed',reuse_eligible=True,admission_feedback=t.c.report())
        calls=[];values=iter([first,stopped,stopped])
        def execute(window,*,allow_exploration):
            calls.append((window,allow_exploration));return next(values)
        with tempfile.TemporaryDirectory() as folder:
            common=dict(initial_window=4,maximum=64,cache_dir=folder)
            r=run_admission_query(self.identity(),execute,**common)
            old=json.loads(Path(r['saved_hint']['path']).read_text())
            self.assertEqual(old['evidence']['incomplete_reason'],'work_mix_changed')
            self.assertEqual(old['evidence']['invalidated_comparisons'],1)
            r=run_admission_query(self.identity(),execute,**common)
            current=json.loads(Path(r['saved_hint']['path']).read_text())
            self.assertEqual(current['evidence']['stop_reason'],'local_plateau')
            self.assertFalse(current['evidence']['exploration_censored'])
            run_admission_query(self.identity(),execute,**common)
        self.assertEqual(calls,[(4,True),(4,True),(4,False)])

    def test_later_invalid_probe_keeps_latest_confirmed_value_not_initial(self):
        t=self.case();t.probe();t.tick(20);t.tick(20)
        self.assertEqual(t.c.report()['recommended_window'],6)
        self.settle(t,100);t.tick(30,iw=200);t.tick(30,iw=200)
        r=t.c.report();self.assertEqual((r['window'],r['recommended_window']),(6,6))
        self.assertEqual(r['accepted_steps'],1);self.assertTrue(r['exploration_censored'])
        calls=[]
        def execute(window,*,allow_exploration):
            calls.append((window,allow_exploration));return dict(status='completed',reuse_eligible=True,admission_feedback=r)
        with tempfile.TemporaryDirectory() as folder:
            for _ in range(2):run_admission_query(self.identity(),execute,initial_window=4,maximum=64,cache_dir=folder)
        self.assertEqual(calls,[(4,True),(6,True)])

    def test_incomparable_probe_discards_only_its_conditional_loss(self):
        t=self.case();t.c.estimated_trial_loss_s=.75
        t.probe();t.tick(4,iw=200);t.tick(4,iw=200)
        r=t.c.report()
        self.assertEqual(r['accepted_steps'],0)
        self.assertAlmostEqual(r['estimated_stable_trial_loss_s'],.75)
        self.assertGreater(r['exploration_wall_s'],0)

    def test_same_value_continuation_saves_new_incomparable_cause(self):
        budget=self.case(seconds=2)
        for _ in range(3):budget.tick()
        c,_=self.measured();missing=self.case()
        values=iter([budget.c.report(),c.report(),missing.c.report()])
        def execute(window,*,allow_exploration):
            return dict(status='completed',reuse_eligible=True,admission_feedback=next(values))
        with tempfile.TemporaryDirectory() as folder:
            common=dict(initial_window=4,maximum=64,cache_dir=folder)
            first=run_admission_query(self.identity(),execute,**common)
            path=Path(first['saved_hint']['path'])
            self.assertEqual(json.loads(path.read_text())['evidence']['incomplete_reason'],'exploration_budget')
            run_admission_query(self.identity(),execute,**common)
            evidence=json.loads(path.read_text())['evidence']
            self.assertEqual(evidence['incomplete_reason'],'work_mix_changed')
            self.assertEqual(evidence['invalidated_comparisons'],1)
            self.assertTrue(evidence['exploration_censored'])
            original=path.read_bytes();run_admission_query(self.identity(),execute,**common)
            self.assertEqual(path.read_bytes(),original)

