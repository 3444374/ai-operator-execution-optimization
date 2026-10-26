"""Work/cache-aligned trials and task-label-independent profile reuse."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.runtime.calibration import admission_identity, identity_signature, run_admission_query
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig, ResourceFeedbackController


class WorkTrace:
    def __init__(self, *, initial=3, maximum=15, total=100000, hint=None, options=None):
        self.now = self.count = self.inputs = self.outputs = self.computed = self.cached = 0
        self.options = options or ResourceFeedbackConfig(search_mode='geometric', work_basis='computed',
            window_s=1, min_samples=2, min_remaining=4, exploration_s=100, allow_exploration=True)
        self.c = ResourceFeedbackController(maximum, initial, self.options, clock=lambda:self.now)
        self.c.reset(initial, total, search_hint=hint)

    def tick(self, n=10, *, iw=100, ow=4, cached=40, pending=90000, kv=.1, fresh=True,
             transition=0, waiting=0, pressure=None):
        self.now += 1
        self.count += n; self.inputs += n*iw; self.outputs += n*ow
        if cached is not None:
            self.cached += n*cached; self.computed += n*(iw-cached)
        return self.c.update(AdmissionObservation(self.now,fresh,self.c.current_window,
            self.c.current_window,waiting,kv,completed_items_total=self.count,pending_items=pending,
            memory_pressure=pressure,
            transition_pending=transition,completed_input_work_total=self.inputs,
            completed_output_work_total=self.outputs,
            completed_computed_input_work_total=None if cached is None else self.computed,
            completed_cached_input_work_total=None if cached is None else self.cached))

    def run(self, rates, *, cache_by_window=None):
        seq={}
        for _ in range(150):
            window=self.c.current_window;value=rates.get(window,10);index=seq.get(window,0);seq[window]=index+1
            n=value[index%len(value)] if isinstance(value,list) else value
            self.tick(n,cached=(cache_by_window or {}).get(window,40))
            if self.c._growth_stopped:return self.c.report()
        raise AssertionError('finite exploration did not stop')


class WorkAwareTests(unittest.TestCase):
    def test_generic_non_power_of_two_capacity_and_measured_gain(self):
        report=WorkTrace().run({3:30,6:60,12:90,15:90})
        self.assertEqual([r['window'] for r in report['history'] if r['reason']=='performance_probe'],[6,12,15])
        self.assertEqual(report['recommended_window'],12)
        self.assertEqual(report['search_state']['points'][-1]['outcome'],'no_gain')
        self.assertFalse(report['learned_global_optimum'])

    def test_faster_rows_with_different_cache_are_not_accepted(self):
        report=WorkTrace(maximum=6).run({3:10,6:30},cache_by_window={6:90})
        self.assertEqual(report['accepted_steps'],0)
        self.assertEqual(report['recommended_window'],3)
        self.assertEqual(report['search_state']['points'][0]['outcome'],'incomparable')
        self.assertTrue(report['can_continue'])
        self.assertFalse(any(r['kind']=='observed_gain_band' for r in report['evaluations']))

    def test_measured_regression_is_distinct_from_work_invalidation(self):
        report=WorkTrace(maximum=6).run({3:30,6:10})
        self.assertEqual(report['search_state']['points'][0]['outcome'],'regression')
        self.assertEqual(report['invalidated_comparisons'],0)
        self.assertEqual(report['recommended_window'],3)

    def test_cache_drift_after_reuse_returns_to_safe_initial_and_keeps_budget(self):
        first=WorkTrace(maximum=6).run({3:10,6:30})
        trace=WorkTrace(initial=6,maximum=6,hint=first['search_state'],
            options=replace(WorkTrace().options,exploration_s=15))
        report=trace.run({3:10,6:30},cache_by_window={3:80,6:80})
        self.assertIn('cached_work_changed',[r['reason'] for r in report['history']])
        self.assertLessEqual(trace.now,16)
        self.assertEqual(report['accepted_steps'],0)
        self.assertEqual(report['recommended_window'],3)
        self.assertTrue(report['can_continue'])

    def test_reference_rebuild_does_not_respend_original_elapsed_budget(self):
        trace=WorkTrace(options=replace(WorkTrace().options,exploration_s=12))
        report=trace.run({3:10,6:30},cache_by_window={6:90})
        self.assertEqual(report['stop_reason'],'exploration_budget')
        self.assertLessEqual(trace.now,13)

    def test_missing_or_stale_cache_work_never_becomes_zero_work_or_gain(self):
        for mode in ('missing','stale'):
            trace=WorkTrace()
            for _ in range(10):trace.tick(cached=None if mode=='missing' else 40,fresh=mode!='stale')
            report=trace.c.report()
            self.assertEqual(report['changes'],0);self.assertEqual(report['accepted_steps'],0)

    def test_changed_context_does_not_keep_a_former_gain_label(self):
        trace=WorkTrace(maximum=12)
        drift=False
        for _ in range(60):
            window=trace.c.current_window
            drift=drift or window==12
            trace.tick({3:10,6:30,12:50}[window],cached=80 if drift else 40)
            if trace.c._growth_stopped:break
        report=trace.c.report()
        self.assertEqual(report['accepted_steps'],1)
        self.assertFalse(report['comparable_selection'])
        self.assertNotIn('verified_window',report['search_state'])
        self.assertTrue(report['can_continue'])

    def test_pressure_shrinks_but_waiting_plus_changed_work_is_not_pressure(self):
        trace=WorkTrace()
        for _ in range(5):trace.tick()
        trace.tick(1,waiting=1,cached=80);trace.tick(1,waiting=1,cached=80)
        self.assertNotIn('resource_pressure',trace.c.report()['reasons'])
        trace.tick(kv=.99);decision=trace.tick(kv=.99)
        self.assertEqual((decision.window,decision.reason),(3,'resource_pressure'))
        self.assertFalse(trace.c.report()['can_continue'])

    def test_tail_and_backpressure_never_wait_for_extra_records(self):
        for total in (0,3,19):
            trace=WorkTrace(total=total,options=replace(WorkTrace().options,min_remaining=32))
            for _ in range(2):trace.tick(1,pending=max(0,total-2))
            self.assertEqual(trace.c.report()['changes'],0)

    def test_noise_retains_safe_reference_and_no_old_rates_are_cached(self):
        report=WorkTrace(maximum=6).run({3:10,6:[8,12,9,11]})
        self.assertEqual(report['recommended_window'],3)
        self.assertEqual(report['search_state']['points'][0]['outcome'],'noise')
        self.assertNotIn('rate',report['search_state']['work_context'])


class GenericReuseTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.identity={key:{'CPU':'v1'} for key in ('hardware','model','generation','execution','workload','measurement')}
        self.identity['measurement']['work_unit']='model_calls'
        self.features=dict(work_unit='tokens',input_work=dict(mean=100,p95=100),
            output_work=dict(limit=4),cache=dict(enabled=True,scope='model_owner'))
        self.options=WorkTrace().options;self.calls=[]

    def invoke(self,*,features=None,total=100000,rates=None,cached=40,status='completed',options=None):
        def execute(window,*,allow_exploration,search_hint):
            self.calls.append((window,allow_exploration,deepcopy(search_hint)))
            trace=WorkTrace(initial=window,maximum=6,total=total,hint=search_hint,
                options=replace(options or self.options,allow_exploration=allow_exploration))
            if total>32:trace.run(rates or {3:10,6:30},cache_by_window={3:cached,6:cached})
            return dict(status=status,reuse_eligible=status=='completed',admission_feedback=trace.c.report())
        return run_admission_query(self.identity,execute,initial_window=3,maximum=6,options=options or self.options,
            cache_dir=self.tmp.name,workload_features=features or self.features)

    def test_rename_query_path_dataset_and_exact_record_count_do_not_change_feature_key(self):
        self.identity['workload']={'query_id':'name-a','dataset':'path-a','rows':137}
        first=self.invoke()
        self.identity['workload']={'query_id':'renamed','dataset':'other-path','rows':19}
        second=self.invoke(total=19)
        self.assertTrue(second['cache_hit']);self.assertEqual(self.calls[-1][0],6)
        self.assertEqual(second['result']['admission_feedback']['changes'],0)
        self.assertEqual(first['saved_hint']['window'],6)
        self.assertFalse(second.get('performance_tuned',False))

    def test_same_name_changed_features_and_each_environment_dimension_miss(self):
        self.invoke();changed=deepcopy(self.features);changed['input_work']['mean']=200
        self.assertFalse(self.invoke(features=changed)['cache_hit'])
        for group in ('hardware','model','generation','execution','measurement'):
            old=deepcopy(self.identity);self.identity[group]['version']='different'
            self.assertFalse(self.invoke()['cache_hit']);self.identity=old

    def test_complete_hint_matches_work_and_makes_no_new_trials(self):
        self.invoke();second=self.invoke()
        self.assertTrue(second['cache_hit'])
        report=second['result']['admission_feedback']
        self.assertEqual(report['changes'],0);self.assertEqual(report['exploration_wall_s'],0)

    def test_failed_or_noise_execution_cannot_publish_unconfirmed_candidate(self):
        self.invoke(status='failed')
        self.assertEqual(list(Path(self.tmp.name).iterdir()),[])
        report=self.invoke(rates={3:10,6:[8,12,9,11]})
        self.assertEqual(report['saved_hint']['window'],3)
        self.assertTrue(report['result']['admission_feedback']['can_continue'])

    def test_same_work_noise_retries_are_finite_and_never_called_optimal(self):
        results=[self.invoke(rates={3:10,6:[8,12,9,11]}) for _ in range(3)]
        self.assertEqual([r['result']['admission_feedback']['changes'] for r in results],[2,2,0])
        self.assertEqual(results[-1]['result']['admission_feedback']['stop_reason'],'search_exhausted')
        self.assertEqual(self.calls[-1][2]['points'][0]['attempts'],2)
        self.assertFalse(results[-1]['result']['admission_feedback']['learned_global_optimum'])

    def test_pressure_limited_hint_requires_new_verification_not_permanent_plateau(self):
        def execute(window,*,allow_exploration,search_hint):
            trace=WorkTrace(initial=window,maximum=6,hint=search_hint)
            for _ in range(5):trace.tick()
            trace.tick(kv=.99);trace.tick(kv=.99)
            return dict(status='completed',reuse_eligible=True,admission_feedback=trace.c.report())
        value=run_admission_query(self.identity,execute,initial_window=3,maximum=6,cache_dir=self.tmp.name,
            options=self.options,workload_features=self.features)
        def inspect(window,*,allow_exploration,search_hint):
            self.assertLessEqual(window,3);self.assertEqual(search_hint['ceiling'],6)
            self.assertEqual(search_hint['points'][0]['outcome'],'pending')
            return dict(status='completed',reuse_eligible=False)
        second=run_admission_query(self.identity,inspect,initial_window=3,maximum=6,cache_dir=self.tmp.name,
            options=self.options,workload_features=self.features)
        self.assertTrue(second['cache_hit'])

    def test_full_cost_is_measured_once_and_explicit_fixed_mode_is_preserved(self):
        now=[10.]
        def execute(window,**kw):
            self.assertFalse(kw['allow_exploration']);now[0]+=7
            return dict(status='completed',reuse_eligible=False,admission_feedback={'exploration_wall_s':3})
        result=run_admission_query(self.identity,execute,initial_window=3,maximum=6,cache_dir=self.tmp.name,
            options=replace(self.options,allow_exploration=False),workload_features=self.features,
            clock=lambda:now[0],request_started_monotonic=8.)
        self.assertEqual((result['total_wall_s'],result['request_wait_s'],result['online_exploration_s']),(7,9,3))
        self.assertTrue(result['online_exploration_already_in_total'])

    def test_feature_api_does_not_weaken_other_offline_calibration_identity(self):
        changed=deepcopy(self.identity);changed['workload']={'renamed':True}
        self.assertNotEqual(identity_signature(self.identity),identity_signature(changed))
        self.assertEqual(admission_identity(self.identity,workload_features=self.features),
                         admission_identity(changed,workload_features=self.features))
        self.assertEqual(self.identity['workload'],{'CPU':'v1'})


class PressureRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.identity={key:{'CPU':'recovery'} for key in ('hardware','model','generation','execution','workload','measurement')}
        self.identity['measurement']['work_unit']='model_calls'
        self.features=dict(work_unit='tokens',input_work=dict(mean=100),output_work=dict(limit=4),
            cache=dict(enabled=True,scope='model_owner'))

    def query(self,rates,*,pressure=False):
        def execute(window,*,allow_exploration,search_hint):
            trace=WorkTrace(initial=window,maximum=15,hint=search_hint)
            if pressure:
                trace.tick(0,kv=.99);trace.tick(0,kv=.99)
                report=trace.c.report()
            else:report=trace.run(rates)
            return dict(status='completed',reuse_eligible=True,admission_feedback=report)
        return run_admission_query(self.identity,execute,initial_window=3,maximum=15,
            cache_dir=self.tmp.name,options=WorkTrace().options,workload_features=self.features)

    def test_completed_reused_profile_12_to_6_reopens_affected_candidates(self):
        rates={3:30,6:60,12:90,15:90}
        first=self.query(rates);self.assertEqual(first['saved_hint']['window'],12)
        second=self.query(rates,pressure=True)
        pressure=second['result']['admission_feedback']
        self.assertEqual(pressure['window'],6);self.assertFalse(pressure['can_continue'])
        third=self.query(rates);report=third['result']['admission_feedback']
        self.assertTrue(any(row['reason']=='performance_probe' and row['window']==12 for row in report['history']))
        self.assertEqual(report['recommended_window'],12)
        self.assertEqual(third['historical_evidence']['search_state'],pressure['search_state'])
        self.assertLessEqual(report['changes'],WorkTrace().options.max_changes)

    def test_completed_origin_3_to_1_has_a_bounded_path_below_original_grid(self):
        rates={1:10,2:20,3:30,6:30,12:30,15:30}
        self.assertEqual(self.query(rates)['saved_hint']['window'],3)
        self.assertEqual(self.query(rates,pressure=True)['saved_hint']['window'],1)
        report=self.query(rates)['result']['admission_feedback']
        points=[row['window'] for row in report['history'] if row['reason']=='performance_probe']
        self.assertEqual(points[:2],[2,3])
        self.assertEqual(len(points),len(set(points)))
        self.assertLessEqual(report['changes'],WorkTrace().options.max_changes)
        self.assertGreaterEqual(report['recommended_window'],3)

    def test_same_query_pressure_clear_remeasures_without_resetting_budget(self):
        first=WorkTrace().run({3:30,6:60,12:90,15:90})
        trace=WorkTrace(initial=12,hint=first['search_state'])
        started=trace.c.started
        trace.tick(0,kv=.99);trace.tick(0,kv=.99)
        self.assertEqual(trace.c.current_window,6)
        self.assertEqual(trace.c._changes,1)
        for _ in range(80):
            trace.tick({3:30,6:60,12:90,15:90}[trace.c.current_window])
            if trace.c._growth_stopped and trace.c._stop_reason!='resource_pressure':break
        report=trace.c.report()
        self.assertEqual(trace.c.started,started)
        self.assertEqual(report['recommended_window'],12)
        self.assertTrue(any(row['reason']=='performance_probe' and row['window']==12 for row in report['history']))
        self.assertLessEqual(report['changes'],trace.options.max_changes)

    def test_expired_budget_and_disabled_exploration_do_not_reopen_on_clear(self):
        for options in (replace(WorkTrace().options,exploration_s=2),
                        replace(WorkTrace().options,allow_exploration=False)):
            trace=WorkTrace(initial=12,options=options)
            trace.tick(0,kv=.99);trace.tick(0,kv=.99)
            for _ in range(10):trace.tick()
            report=trace.c.report()
            self.assertEqual(report['window'],6)
            self.assertEqual(report['stop_reason'],'resource_pressure')
            self.assertFalse(any(row['reason']=='performance_probe' for row in report['history']))

    def test_missing_feedback_does_not_clear_pressure_and_short_tail_does_not_wait(self):
        trace=WorkTrace(initial=3)
        trace.tick(0,kv=.99);trace.tick(0,kv=.99)
        for _ in range(3):trace.tick(1,fresh=False)
        self.assertEqual(trace.c.current_window,1)
        self.assertIsNotNone(trace.c._search.pressure_recovery)
        trace.tick(1,pending=1)
        self.assertEqual(trace.c.current_window,1)
        self.assertFalse(any(row['reason']=='performance_probe' for row in trace.c.history))

    def test_required_memory_feedback_must_explicitly_clear_before_recovery(self):
        trace=WorkTrace(initial=12,options=replace(WorkTrace().options,memory_floor_bytes=1))
        trace.tick(0,pressure=True)
        for _ in range(3):trace.tick(1,pressure=None)
        self.assertEqual(trace.c.current_window,6)
        self.assertIsNotNone(trace.c._search.pressure_recovery)
        trace.tick(1,pressure=False)
        self.assertEqual(trace.c.current_window,6)
        self.assertIsNone(trace.c._search.pressure_recovery)

    def test_pressure_recovery_never_retries_an_already_tried_point_in_same_query(self):
        trace=WorkTrace()
        for _ in range(30):
            trace.tick({3:30,6:60,12:90,15:90}[trace.c.current_window])
            if trace.c.current_window==12:break
        trace.tick(0,kv=.99);trace.tick(0,kv=.99)
        for _ in range(60):
            trace.tick({3:30,6:60,12:90,15:90}[trace.c.current_window])
            if trace.c._growth_stopped and trace.c._stop_reason!='resource_pressure':break
        points=[row['window'] for row in trace.c.history if row['reason']=='performance_probe']
        self.assertEqual(len(points),len(set(points)))


if __name__=='__main__':unittest.main()
