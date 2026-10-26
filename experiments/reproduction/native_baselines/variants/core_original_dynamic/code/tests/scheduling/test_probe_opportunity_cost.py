"""Measured-stop replay and partial local hint continuation; zero inference."""
import json,tempfile,unittest
from pathlib import Path
from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig,ResourceFeedbackController
from src.scheduling.runtime.calibration import run_admission_query
from tests.scheduling import test_online_performance_control as traces


class ProbeOpportunityCostTests(unittest.TestCase):
    def controller(self,now):
        c=ResourceFeedbackController(64,10,ResourceFeedbackConfig(allow_exploration=True,
            exploration_s=30),clock=lambda:now[0])
        c.reset(10,2000);c.started=c._window_at=0
        c._reference=dict(window=10,rate=61.80684517377239,rate_min=60.7513025611689,
            rate_max=62.86238778637587,input_min=125.921875,input_max=131.0483870967742,
            output_min=4.,output_max=4.,hol_max=.1189319402910769,samples=2)
        c._changes=c._accepted_steps=3
        return c

    def observation(self,now):
        return AdmissionObservation(now,True,10,10,0,.1,completed_items_total=767,
            pending_items=1233,hol_age_s=.1189319402910769)

    def test_measured_C10_does_not_charge_useful_trial_work_as_extra_cost(self):
        now=[16.928384801838547];c=self.controller(now)
        decision=c._next_trial(self.observation(now[0]))
        self.assertEqual((decision.window,decision.reason),(12,'performance_probe'))
        estimate=c.report()['evaluations'][-1]
        self.assertEqual(estimate['trial_allowance_s'],4.)
        self.assertAlmostEqual(estimate['optimistic_saved_after_trial_s'],2.6582077776158166)

    def test_control_cost_estimate_can_still_reject_a_probe(self):
        now=[16.928384801838547];c=self.controller(now)
        c.control_seconds=.4;c._decisions=1
        decision=c._next_trial(self.observation(now[0]))
        self.assertEqual(decision.reason,'insufficient_remaining_benefit')
        self.assertEqual(c.current_window,10)

    def test_tail_cannot_fit_measurement_even_when_trial_loss_is_zero(self):
        now=[16.];c=self.controller(now);c.total_items=800
        self.assertEqual(c._next_trial(self.observation(now[0])).reason,'insufficient_remaining_benefit')
        self.assertEqual(c.current_window,10)

    def test_change_budget_is_censored_not_a_measured_platform(self):
        now=[16.];c=self.controller(now);c._changes=7
        c._next_trial(self.observation(now[0]));report=c.report()
        self.assertEqual(report['stop_reason'],'exploration_budget')
        self.assertTrue(report['exploration_censored'])
        self.assertFalse(report['learned_global_optimum'])

    def identity(self):
        identity={k:{'CPU':'partial-hint'} for k in ('hardware','model','generation','execution','workload','measurement')}
        identity['measurement']['work_unit']='model_calls'
        return identity

    def receipt(self,stop):
        now=[31.];c=self.controller(now)
        c._stop(stop,self.observation(now[0]))
        return dict(status='completed',reuse_eligible=True,admission_feedback=c.report())

    def test_budget_hint_continues_from_confirmed_value_not_low_initial(self):
        calls=[];value=self.receipt('exploration_budget')
        def execute(window,*,allow_exploration):
            calls.append((window,allow_exploration));return value
        with tempfile.TemporaryDirectory() as folder:
            common=dict(initial_window=4,maximum=64,cache_dir=folder)
            run_admission_query(self.identity(),execute,**common)
            reuse=run_admission_query(self.identity(),execute,**common)
        self.assertEqual(calls,[(4,True),(10,True)])
        self.assertTrue(reuse['cache_hit']);self.assertTrue(reuse['bounded_continuation'])

    def test_measured_plateau_replaces_partial_evidence_then_disables_exploration(self):
        calls=[];values=iter([self.receipt('exploration_budget'),self.receipt('local_plateau'),self.receipt('local_plateau')])
        def execute(window,*,allow_exploration):
            calls.append((window,allow_exploration));return next(values)
        with tempfile.TemporaryDirectory() as folder:
            common=dict(initial_window=4,maximum=64,cache_dir=folder)
            reports=[run_admission_query(self.identity(),execute,**common) for _ in range(3)]
            stored=json.loads(Path(reports[1]['saved_hint']['path']).read_text())
        self.assertEqual(calls,[(4,True),(10,True),(10,False)])
        self.assertFalse(stored['evidence']['exploration_censored'])
        self.assertEqual(stored['evidence']['stop_reason'],'local_plateau')

    def test_missing_feedback_does_not_certify_a_partial_search_completed(self):
        calls=[];partial=self.receipt('exploration_budget')
        now=[0.];missing=self.controller(now);missing.reset(10,2000)
        values=iter([partial,dict(status='completed',reuse_eligible=True,admission_feedback=missing.report()),partial])
        def execute(window,*,allow_exploration):
            calls.append((window,allow_exploration));return next(values)
        with tempfile.TemporaryDirectory() as folder:
            common=dict(initial_window=4,maximum=64,cache_dir=folder)
            first=run_admission_query(self.identity(),execute,**common)
            path=Path(first['saved_hint']['path']);original=path.read_bytes()
            second=run_admission_query(self.identity(),execute,**common)
            self.assertEqual(path.read_bytes(),original)
            run_admission_query(self.identity(),execute,**common)
        self.assertTrue(second['reused_hint_unmodified'])
        self.assertEqual(calls,[(4,True),(10,True),(10,True)])

    def test_later_budget_time_does_not_erase_a_measured_plateau(self):
        case=traces.OnlinePerformanceTests();case.setUp();case.probe();case.tick();case.tick()
        self.assertEqual(case.c.report()['stop_reason'],'local_plateau')
        case.now=101;case.tick()
        self.assertEqual(case.c.report()['stop_reason'],'local_plateau')
        self.assertFalse(case.c.report()['exploration_censored'])

    def test_first_query_without_feedback_is_not_cached_as_a_finished_search(self):
        calls=[];now=[0.];c=self.controller(now);c.reset(4,2000)
        def execute(window,*,allow_exploration):
            calls.append((window,allow_exploration))
            return dict(status='completed',reuse_eligible=True,admission_feedback=c.report())
        with tempfile.TemporaryDirectory() as folder:
            for _ in range(2):run_admission_query(self.identity(),execute,initial_window=4,maximum=64,cache_dir=folder)
        self.assertEqual(calls,[(4,True),(4,True)])

    def test_short_or_capacity_initial_value_can_skip_exploration_without_feedback(self):
        for total,initial in ((16,4),(2000,64)):
            with self.subTest(total=total,initial=initial):
                calls=[];now=[0.];c=self.controller(now);c.reset(initial,total)
                def execute(window,*,allow_exploration):
                    calls.append((window,allow_exploration))
                    return dict(status='completed',reuse_eligible=True,admission_feedback=c.report())
                with tempfile.TemporaryDirectory() as folder:
                    for _ in range(2):run_admission_query(self.identity(),execute,initial_window=initial,maximum=64,cache_dir=folder)
                self.assertEqual(calls,[(initial,True),(initial,False)])
