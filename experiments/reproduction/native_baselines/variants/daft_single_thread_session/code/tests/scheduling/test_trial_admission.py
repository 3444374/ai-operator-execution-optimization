"""Unstable references can admit bounded work, never certify a winner."""
from dataclasses import replace
import unittest

from tests.scheduling.test_work_aware_admission import WorkTrace
from tests.scheduling import test_work_aware_admission as fixtures


class TrialAdmissionTests(unittest.TestCase):
    def trace(self, **kwargs):
        return WorkTrace(total=4093, **kwargs)

    def varying(self, trace, *, ticks=150, stable_candidate=False, recheck_cache=40):
        for _ in range(ticks):
            c=trace.c
            recheck=c._probe_candidate is not None or c._reference_recheck_used
            cache=(recheck_cache if recheck and c.current_window==3 else 40
                   if stable_candidate and c.current_window==6 else 20 if trace.now%2 else 60)
            trace.tick(n=30 if c.current_window>3 else 10, cached=cache)
            if c._growth_stopped:break
        return trace.c.report()

    def test_continuously_changing_work_still_gets_one_adjacent_trial(self):
        trace=self.trace()
        report=self.varying(trace)
        self.assertEqual([r['window'] for r in report['history'] if r['reason']=='performance_probe'],[6])
        self.assertEqual(report['accepted_steps'],0)
        self.assertEqual(report['recommended_window'],3)
        self.assertEqual(report['provisional_trials'],1)
        self.assertTrue(report['exploration_censored'])
        self.assertNotEqual(report['stop_reason'],'sampled_range_complete')
        self.assertEqual(trace.c._search.here,{6})

    def test_trial_fits_original_ten_second_budget_without_reserving_a_second_experiment(self):
        trace=self.trace(options=replace(WorkTrace().options,exploration_s=10))
        report=self.varying(trace)
        self.assertEqual(report['provisional_trials'],1)
        self.assertEqual(report['stop_reason'],'exploration_budget')
        self.assertLessEqual(trace.now,10)
        self.assertEqual((report['recommended_window'],report['window']),(3,3))

    def test_fresh_comparable_reference_can_accept_the_real_trial(self):
        trace=self.trace(maximum=6)
        report=self.varying(trace,stable_candidate=True)
        self.assertEqual(report['accepted_steps'],1)
        self.assertEqual(report['recommended_window'],6)
        self.assertEqual(report['reference_rechecks'],1)
        self.assertTrue(report['comparable_selection'])
        self.assertEqual(report['search_state']['work_context']['cached_min'],40)
        points=report['measurements']
        self.assertEqual([p['control_phase'] for p in points[:3]],['reference','probe','reference_recheck'])
        self.assertEqual([p['control_phase_id'] for p in points[:3]],[0,1,2])
        self.assertTrue(all(p['comparison_eligible'] for p in points[1:3]))
        self.assertEqual([r['reason'] for r in report['history'][:3]],
                         ['performance_probe','reference_recheck','performance_accept'])

    def test_irreversible_cache_warming_is_not_fixed_by_alternating_settings(self):
        trace=self.trace(maximum=6)
        report=self.varying(trace,stable_candidate=True,recheck_cache=80)
        self.assertEqual(report['accepted_steps'],0)
        self.assertEqual(report['recommended_window'],3)
        self.assertGreater(report['invalidated_comparisons'],0)
        self.assertFalse(report['comparable_selection'])
        self.assertNotIn('verified_window',report['search_state'])

    def test_heterogeneous_fast_candidate_is_incomparable_not_regression(self):
        report=self.varying(self.trace(maximum=6))
        self.assertEqual(report['search_state']['points'][0]['outcome'],'incomparable')
        self.assertFalse(any(r['kind']=='observed_gain_band' for r in report['evaluations']))

    def test_valid_slow_candidate_after_fresh_recheck_is_a_measured_regression(self):
        trace=self.trace(maximum=6)
        for _ in range(100):
            c=trace.c
            initial=c._changes==0
            trace.tick(5 if c.current_window==6 else 20,
                       cached=(20 if trace.now%2 else 60) if initial else 40)
            if c._growth_stopped:break
        report=trace.c.report()
        self.assertEqual(report['search_state']['points'][0]['outcome'],'regression')
        self.assertEqual(report['invalidated_comparisons'],0)
        self.assertEqual(report['recommended_window'],3)

    def test_recheck_noise_never_compares_safe_setting_against_itself(self):
        trace=self.trace(maximum=6)
        for _ in range(100):
            c=trace.c
            initial=c._changes==0
            trace.tick([8,12,10][trace.now%3] if c.current_window==6 else 10,
                       cached=(20 if trace.now%2 else 60) if initial else 40)
            if c._growth_stopped:break
        report=trace.c.report()
        self.assertEqual(report['search_state']['points'][0]['outcome'],'noise')
        self.assertEqual(report['accepted_steps'],0)
        self.assertEqual(report['window'],3)
        self.assertEqual([r['window'] for r in report['search_state']['points']],[6])
        self.assertTrue(all(r['window']==6 for r in report['evaluations'] if r['kind']=='observed_gain_band'))

    def test_old_inflight_completions_are_discarded_without_queue_drain(self):
        trace=self.trace(maximum=6)
        while trace.c._changes==0:
            trace.tick(cached=20 if trace.now%2 else 60)
        probe=trace.c.report()['history'][0]
        for n in (1,5,2):trace.tick(n,cached=99,transition=1)
        old_end=trace.count
        report=self.varying(trace,stable_candidate=True)
        candidate=next(p for p in report['measurements'] if p['control_phase']=='probe')
        self.assertGreater(candidate['completed_begin'],probe['completed_items_total'])
        self.assertGreaterEqual(candidate['completed_begin'],old_end)
        self.assertEqual(candidate['cached_min'],40)
        self.assertEqual(candidate['cached_max'],40)
        self.assertEqual(report['reasons']['transition'],3)

    def test_budget_expiry_during_recheck_keeps_safe_window_and_no_winner(self):
        trace=self.trace(maximum=6,options=replace(WorkTrace().options,exploration_s=12))
        report=self.varying(trace,stable_candidate=True)
        self.assertEqual(report['reference_rechecks'],1)
        self.assertEqual((report['window'],report['recommended_window']),(3,3))
        self.assertEqual(report['accepted_steps'],0)
        self.assertEqual(report['stop_reason'],'exploration_budget')
        self.assertEqual(trace.c.started,0)
        self.assertEqual(trace.c._search.here,{6})

    def test_changes_and_retries_are_not_reset_by_recheck(self):
        trace=self.trace(maximum=6,options=replace(WorkTrace().options,max_changes=2))
        report=self.varying(trace,stable_candidate=True)
        self.assertLessEqual(report['changes'],2)
        self.assertEqual(report['accepted_steps'],0)
        self.assertEqual(report['window'],3)
        self.assertEqual(trace.c._search.here,{6})
        self.assertEqual(report['search_state']['points'][0]['attempts'],1)

    def test_pressure_during_probe_returns_to_safe_and_requires_fresh_validation(self):
        trace=self.trace(maximum=6)
        while trace.c._changes==0:trace.tick(cached=20 if trace.now%2 else 60)
        trace.tick(kv=.99);trace.tick(kv=.99)
        report=trace.c.report()
        self.assertEqual(report['window'],3)
        self.assertEqual(report['accepted_steps'],0)
        self.assertIsNone(trace.c._probe_candidate)
        self.assertFalse(report['can_continue'])
        trace.tick(fresh=False)
        self.assertEqual(trace.c.current_window,3)
        self.assertIsNotNone(trace.c._search.pressure_recovery)
        report=trace.run({3:10,6:30})
        self.assertIn('performance_probe',[r['reason'] for r in report['history']])
        self.assertLessEqual(report['changes'],trace.options.max_changes)

    def test_missing_resource_or_native_work_does_not_admit_a_trial(self):
        for mode in ('stale','missing_work','missing_memory'):
            trace=self.trace(options=replace(WorkTrace().options,memory_floor_bytes=int(mode=='missing_memory')))
            for _ in range(20):trace.tick(fresh=mode!='stale',cached=None if mode=='missing_work' else 40)
            self.assertEqual(trace.c.report()['changes'],0)

    def test_short_tail_and_fixed_queries_never_request_more_work(self):
        for pending,fixed in ((3,False),(4000,True)):
            trace=self.trace(options=replace(WorkTrace().options,allow_exploration=not fixed))
            for _ in range(20):trace.tick(cached=20 if trace.now%2 else 60,pending=pending)
            self.assertEqual(trace.c.report()['changes'],0)

    def test_observation_and_trial_are_subsets_of_total_not_added_twice(self):
        trace=self.trace(maximum=6)
        report=self.varying(trace,stable_candidate=True)
        self.assertTrue(report['exploration_already_in_query_wall'])
        self.assertLessEqual(report['exploration_wall_s'],trace.now-trace.c.started)
        self.assertEqual(report['estimated_stable_trial_loss_s'],0)


class ProvisionalReuseTests(unittest.TestCase):
    setUp=fixtures.GenericReuseTests.setUp

    def test_incomparable_trial_saves_only_safe_incomplete_progress(self):
        def execute(window,*,allow_exploration,search_hint):
            trace=WorkTrace(initial=window,maximum=6,hint=search_hint)
            TrialAdmissionTests().varying(trace)
            return dict(status='completed',reuse_eligible=True,admission_feedback=trace.c.report())
        from src.scheduling.runtime.calibration import run_admission_query
        result=run_admission_query(self.identity,execute,initial_window=3,maximum=6,
            options=self.options,cache_dir=self.tmp.name,workload_features=self.features)
        self.assertEqual(result['saved_hint']['window'],3)
        feedback=result['result']['admission_feedback']
        self.assertTrue(feedback['exploration_censored'])
        self.assertEqual(feedback['search_state']['points'][0]['outcome'],'incomparable')
        self.assertNotIn('verified_window',feedback['search_state'])


class CoreTrialDeliveryTests(unittest.TestCase):
    def test_original_requests_continue_once_through_cpu_ipc_and_same_resident_owner(self):
        from unittest.mock import patch
        from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
        from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
        from tests.execution_provider.test_lotus_records_core import Source
        from tests.execution_provider.test_core_resource_feedback import CoreFeedbackTests
        from tests.execution_provider.test_work_aware_core import WorkProcess
        fixture=CoreFeedbackTests();rows=fixture.rows(80);outputs=[];prompts=[];ids=set()
        with ThreadedRuntime(rows) as fake,patch.object(AsyncCoreModelActor,'_process_type',return_value=WorkProcess):
            owner=fixture.owner(initial=1,maximum=4,options=dict(search_mode='geometric',work_basis='computed'))
            try:
                for enabled in (False,True):
                    start=len(fake.events)
                    stream=owner.evaluate(Source(rows),query_id='query-'+str(enabled),allow_exploration=enabled)
                    values=list(stream)
                    outputs.append([(v['source_position'],v['text'],v['total_tokens'])
                                    for v in sorted(values,key=lambda v:v['source_position'])])
                    call_ids={v['call_id'] for v in values}
                    self.assertEqual(len(call_ids),80);self.assertFalse(ids & call_ids);ids.update(call_ids)
                    prepared=[row for e in fake.events[start:] if e[:2]==('submit','AsyncCoreModelActor.run_core')
                              for row in e[3][0].value.result(2)]
                    prompts.append([v['messages_json'] for v in sorted(prepared,key=lambda v:v['source_position'])])
                    report=stream.last_close_report['admission_feedback']
                    self.assertEqual(report['total_items'],80)
                    self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                    self.assertEqual(owner.transport.retained_refs(),0)
                    if enabled:
                        self.assertGreater(report['changes'],0)
                        self.assertGreater(report['peak_native_running'],1)
                        self.assertTrue(all(p['completed_end']>p['completed_begin'] for p in report['measurements']))
                    else:self.assertEqual(report['changes'],0)
                self.assertEqual(len(fake.async_actors),1)
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,160)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')
        self.assertEqual(outputs[0],outputs[1]);self.assertEqual(prompts[0],prompts[1])


if __name__=='__main__':unittest.main()
