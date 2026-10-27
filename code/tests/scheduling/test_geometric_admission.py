"""Finite one-pass search on delivered-work traces; no model performance claim."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.runtime.calibration import run_admission_query
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig, ResourceFeedbackController


class Trace:
    def __init__(self, *, maximum=16, initial=4, options=None, hint=None, total=100000):
        self.now = self.count = self.inputs = self.outputs = 0
        self.options = options or ResourceFeedbackConfig(search_mode='geometric',
            window_s=1,min_samples=2,min_remaining=4,exploration_s=100,allow_exploration=True)
        self.c = ResourceFeedbackController(maximum,initial,self.options,clock=lambda:self.now)
        self.c.reset(initial,total,search_hint=hint)

    def tick(self,n=10,*,iw=100,ow=4,pending=90000,kv=.1,fresh=True,transition=0):
        self.now += 1
        self.count += n;self.inputs += n*iw;self.outputs += n*ow
        return self.c.update(AdmissionObservation(self.now,fresh,self.c.current_window,
            self.c.current_window,0,kv,completed_items_total=self.count,pending_items=pending,
            transition_pending=transition,completed_input_work_total=self.inputs,
            completed_output_work_total=self.outputs))

    def run(self,rates,*,works=None):
        sequences={}
        for _ in range(200):
            window=self.c.current_window
            values=rates.get(window,10)
            index=sequences.get(window,0);sequences[window]=index+1
            n=values[index%len(values)] if isinstance(values,list) else values
            self.tick(n,iw=100 if works is None else works.get(window,100))
            if self.c._growth_stopped:
                return self.c.report()
        raise AssertionError('finite trace did not stop')


class GeometricAdmissionTests(unittest.TestCase):
    def test_declared_capacity_reachable_with_original_eight_changes(self):
        trace=Trace(maximum=64)
        report=trace.run({c:c*5 for c in (4,8,16,32,64)})
        self.assertEqual([r['window'] for r in report['history'] if r['reason']=='performance_probe'],[8,16,32,64])
        self.assertEqual((report['recommended_window'],report['stop_reason'],report['changes']),
                         (64,'sampled_range_complete',4))
        self.assertFalse(report['learned_global_optimum'])

    def test_regression_does_not_exclude_better_unmeasured_upper_point(self):
        report=Trace().run({4:40,8:32,16:80})
        self.assertEqual(report['recommended_window'],16)
        self.assertEqual([p['outcome'] for p in report['search_state']['points']],['regression','gain'])

    def test_flat_neighbor_does_not_prove_global_plateau(self):
        report=Trace().run({4:40,8:40,16:70})
        self.assertEqual(report['recommended_window'],16)
        self.assertEqual(report['search_state']['points'][0]['outcome'],'no_gain')

    def test_aggregate_delivery_cannot_be_replaced_by_median_mad(self):
        t=Trace(maximum=8)
        t.tick(100);t.tick(100)
        for n in (100,100,200):t.tick(n)
        self.assertEqual(t.c.current_window,8)
        t.tick(104);t.tick(104)  # settle and cooldown are not scored
        for n in (104,104,20):t.tick(n)
        report=t.c.report()
        self.assertAlmostEqual(report['measurements'][0]['rate'],400/3)
        self.assertAlmostEqual(report['measurements'][-1]['rate'],228/3)
        self.assertEqual(report['accepted_steps'],0)
        t.tick(20)
        self.assertEqual(t.c.report()['recommended_window'],4)

    def test_incomparable_point_is_unresolved_and_next_unvisited_point_runs(self):
        report=Trace().run({4:10,8:20,16:30},works={8:140})
        self.assertEqual(report['search_state']['points'][0]['outcome'],'incomparable')
        self.assertEqual(report['search_state']['points'][1]['outcome'],'gain')
        self.assertTrue(report['can_continue']);self.assertTrue(report['exploration_censored'])
        self.assertEqual(report['recommended_window'],16)

    def test_mean_work_alone_does_not_authorize_a_comparison(self):
        t=Trace()
        for i in range(30):t.tick(10,iw=16 if i%2 else 144)
        self.assertEqual(t.c.current_window,4)
        self.assertEqual(t.c.report()['accepted_steps'],0)
        self.assertGreater(t.c.report()['reasons']['unstable_reference_work'],0)

    def test_original_budget_is_not_reset_by_work_invalidation(self):
        t=Trace(options=replace(Trace().options,exploration_s=12))
        report=t.run({4:10,8:20},works={8:140})
        self.assertGreater(report['invalidated_comparisons'],0)
        self.assertEqual(report['stop_reason'],'exploration_budget')
        self.assertLessEqual(t.now,13)

    def test_short_queries_do_not_probe_or_request_a_continuation(self):
        t=Trace(total=3)
        for _ in range(3):t.tick(1,pending=2)
        report=t.c.report()
        self.assertEqual(report['changes'],0);self.assertFalse(report['can_continue'])

    def test_tail_returns_confirmed_setting_without_waiting_for_samples(self):
        t=Trace()
        for _ in range(5):t.tick()
        self.assertEqual(t.c.current_window,8)
        d=t.tick(pending=1)
        self.assertEqual((d.window,d.reason),(4,'short_tail_or_fixed'))
        self.assertTrue(t.c.report()['can_continue'])

    def test_pressure_reduces_admission_without_cancelling_or_probing_higher(self):
        t=Trace()
        for _ in range(5):t.tick()
        t.tick(kv=.99);d=t.tick(kv=.99)
        self.assertEqual((d.window,d.reason),(4,'resource_pressure'))
        for _ in range(10):t.tick()
        report=t.c.report()
        self.assertEqual(report['window'],4);self.assertFalse(report['can_continue'])
        self.assertEqual(report['search_state']['ceiling'],7)
        self.assertTrue(report['exploration_censored'])

    def test_effective_work_tolerance_is_independent_of_gain_threshold(self):
        t=Trace(maximum=8,options=replace(Trace().options,min_gain=.5,work_tolerance=.03))
        report=t.run({4:10,8:30},works={8:110})
        self.assertEqual(report['accepted_steps'],0)
        self.assertEqual(report['search_state']['points'][0]['outcome'],'incomparable')

    def test_unresolved_lower_point_uses_its_own_minimum_sample_allowance(self):
        hint=dict(schema='semloom.admission_search.v1',origin=4,maximum=16,attempt_limit=2,
            ceiling=16,points=[dict(window=8,attempts=1,outcome='noise'),dict(window=16,attempts=1,outcome='gain')])
        options=replace(Trace().options,poll_interval_s=.01,window_s=.1,min_samples=100)
        t=Trace(initial=16,options=options,hint=hint)
        self.assertEqual(t.c.sampling_interval_s(90000),.01)
        for _ in range(5):t.tick(1600)
        self.assertEqual(t.c.current_window,8)
        estimate=next(r for r in t.c.report()['evaluations'] if r['kind']=='probe_cost_estimate')
        self.assertAlmostEqual(estimate['trial_allowance_s'],3*(100/800)+.2)


class GeometricReuseTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.identity={k:{'CPU':'v1'} for k in ('hardware','model','generation','execution','workload','measurement')}
        self.identity['measurement']['work_unit']='model_calls'
        self.options=Trace().options;self.calls=[]

    def invoke(self,execute,**kwargs):
        return run_admission_query(self.identity,execute,initial_window=4,maximum=8,
            cache_dir=self.tmp.name,options=kwargs.pop('options',self.options),**kwargs)

    def execute_noise(self,window,*,allow_exploration,search_hint):
        self.calls.append((window,allow_exploration,deepcopy(search_hint)))
        t=Trace(maximum=8,initial=window,options=replace(self.options,allow_exploration=allow_exploration),hint=search_hint)
        if allow_exploration:t.run({4:10,8:[8,12,9,11]})
        return dict(status='completed',reuse_eligible=True,admission_feedback=t.c.report())

    def test_noise_is_unresolved_but_retries_do_not_restart_forever(self):
        first=self.invoke(self.execute_noise);second=self.invoke(self.execute_noise);third=self.invoke(self.execute_noise)
        self.assertEqual([r[1] for r in self.calls],[True,True,False])
        self.assertEqual(first['result']['admission_feedback']['stop_reason'],'continuation_required')
        report=second['result']['admission_feedback']
        self.assertEqual(report['stop_reason'],'search_exhausted')
        self.assertTrue(report['exploration_censored']);self.assertFalse(report['can_continue'])
        self.assertEqual(self.calls[2][2]['points'][0]['attempts'],2)
        self.assertTrue(third['cache_hit']);self.assertTrue(third['reused_hint_unmodified'])

    def test_cached_progress_has_no_old_rates_or_work_reference(self):
        first=self.invoke(self.execute_noise)
        state=first['result']['admission_feedback']['search_state']
        self.assertEqual(set(state['points'][0]),{'window','attempts','outcome'})
        self.assertNotIn('reference',state)
        self.assertNotIn('rate',state)

    def test_each_effective_policy_and_performance_identity_change_misses(self):
        self.invoke(self.execute_noise)
        for changed in (replace(self.options,window_s=.5),replace(self.options,max_changes=10),
                replace(self.options,work_tolerance=.04),replace(self.options,max_candidate_attempts=3)):
            self.assertFalse(self.invoke(self.execute_noise,options=changed)['cache_hit'])
        self.identity['execution']['source']='changed-source'
        self.assertFalse(self.invoke(self.execute_noise)['cache_hit'])

    def test_malformed_progress_does_not_repeat_business_execution(self):
        first=self.invoke(self.execute_noise);path=Path(first['saved_hint']['path'])
        import json
        value=json.loads(path.read_text());value['evidence']['search_state']['points'][0]['attempts']=999
        path.write_text(json.dumps(value));count=len(self.calls)
        second=self.invoke(self.execute_noise)
        self.assertEqual(len(self.calls),count+1);self.assertFalse(second['cache_hit'])
        self.assertIsNotNone(second['lookup_error'])

    def test_ineligible_and_exception_cannot_publish_a_completed_selection(self):
        def failed(window,**kwargs):return dict(status='failed',reuse_eligible=False)
        self.assertNotIn('saved_hint',self.invoke(failed))
        calls=[]
        def raises(window,**kwargs):calls.append(window);raise RuntimeError('original')
        with self.assertRaisesRegex(RuntimeError,'original'):self.invoke(raises)
        self.assertEqual(calls,[4]);self.assertEqual(list(Path(self.tmp.name).iterdir()),[])

    def test_full_return_includes_probe_and_resume_without_double_counting(self):
        now=[10.]
        def execute(window,**kwargs):
            now[0]+=7
            return dict(status='completed',reuse_eligible=False,
                        admission_feedback=dict(exploration_wall_s=3))
        value=self.invoke(execute,clock=lambda:now[0],request_started_monotonic=8.)
        self.assertEqual((value['total_wall_s'],value['request_wait_s'],value['online_exploration_s']),(7,9,3))
        self.assertTrue(value['online_exploration_already_in_total'])

    def test_explicit_pressure_only_options_never_enable_exploration_on_miss(self):
        self.options=replace(self.options,allow_exploration=False)
        self.invoke(self.execute_noise);self.invoke(self.execute_noise)
        self.assertEqual([r[1] for r in self.calls],[False,False])


if __name__=='__main__':unittest.main()
