"""Local evidence without a fixed percentage equality rule for workload."""
from copy import deepcopy
from dataclasses import replace
from unittest.mock import patch
import unittest

from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig, _local_pair_evidence
from src.scheduling.core.models import AdmissionObservation
from tests.scheduling.test_work_aware_admission import WorkTrace
from tests.scheduling import test_work_aware_admission as fixtures


def point(work, rates, phase=0):
    return dict(control_phase_id=phase,windows=[dict(input=i,computed=c,output=o,rate=r)
                for (i,c,o),r in zip(work,rates)])


class PairEvidenceTests(unittest.TestCase):
    def test_same_heterogeneous_composition_known_gain_and_phase_permutation(self):
        a=point([(100,60,4),(140,80,4),(120,70,4)],[20,14,16])
        b=point([(140,80,4),(120,70,4),(100,60,4)],[28,32,40],phase=1)
        e=_local_pair_evidence(a,b,0)
        self.assertEqual(e['outcome'],'gain')
        self.assertEqual([(p['reference'],p['candidate']) for p in e['not_lighter_pairs']],[(1,0),(2,1),(0,2)])

    def test_pairing_does_not_select_on_rate_or_reuse_a_window(self):
        a=point([(100,60,4)]*3,[10,20,30]);b=point([(100,60,4)]*3,[11,22,33],1)
        first=_local_pair_evidence(a,b,0)
        b['windows'][0]['rate']=1000
        second=_local_pair_evidence(a,b,0)
        keys=lambda x:[(r['reference'],r['candidate']) for r in x['not_lighter_pairs']]
        self.assertEqual(keys(first),keys(second))
        self.assertEqual(len({p[0] for p in keys(first)}),len(keys(first)))
        self.assertEqual(len({p[1] for p in keys(first)}),len(keys(first)))

    def test_cache_only_lighter_work_cannot_certify_concurrency_gain(self):
        a=point([(100,80,4)]*3,[10]*3);b=point([(100,20,4)]*3,[40]*3,1)
        e=_local_pair_evidence(a,b,0)
        self.assertIsNone(e['outcome']);self.assertEqual(e['not_lighter_pairs'],[])

    def test_one_heavy_fast_pair_does_not_justify_a_winner(self):
        a=point([(100,60,4)]*3,[10]*3)
        b=point([(100,70,4),(100,20,4),(100,30,4)],[20,20,20],1)
        self.assertIsNone(_local_pair_evidence(a,b,0)['outcome'])

    def test_opposing_local_evidence_is_uncertain(self):
        a=point([(100,60,4)]*3,[10]*3)
        b=point([(100,70,4),(100,70,4),(100,20,4)],[20,20,5],1)
        self.assertIsNone(_local_pair_evidence(a,b,0)['outcome'])

    def test_slower_candidate_despite_lighter_work_is_observed_regression(self):
        a=point([(100,80,4)]*3,[10]*3);b=point([(100,20,4)]*3,[5]*3,1)
        self.assertEqual(_local_pair_evidence(a,b,0)['outcome'],'regression')

    def test_gain_margin_is_independent_from_workload_matching(self):
        a=point([(100,60,4)]*3,[10]*3);b=point([(100,60,4)]*3,[12]*3,1)
        low=_local_pair_evidence(a,b,0);high=_local_pair_evidence(a,b,.25)
        self.assertEqual(low['outcome'],'gain');self.assertEqual(high['outcome'],'no_gain')
        self.assertEqual(low['not_lighter_pairs'],high['not_lighter_pairs'])

    def test_window_totals_never_replace_per_completed_request_work(self):
        a=point([(100,80,4)]*3,[20]*3)
        b=point([(100,40,4)]*3,[60]*3,1)
        for w in a['windows']:w.update(completed=20,computed_total=1600)
        for w in b['windows']:w.update(completed=60,computed_total=2400)
        self.assertIsNone(_local_pair_evidence(a,b,0)['outcome'])

    def test_alternating_rates_cannot_pick_only_faster_windows(self):
        a=point([(100,60,4)]*4,[10]*4)
        b=point([(100,60,4)]*4,[8,12,8,12],1)
        e=_local_pair_evidence(a,b,0)
        self.assertEqual(len(e['not_lighter_pairs']),4)
        self.assertIsNone(e['outcome'])

    def test_overlapping_duplicate_prefixes_cannot_multiply_support(self):
        a=point([(100,60,4)]*3,[10]*3)
        b=point([(100,60,4)]*3,[20]*3,1)
        for w in a['windows']:w.update(completed_begin=0,completed_end=20)
        for w in b['windows']:w.update(completed_begin=100,completed_end=120)
        e=_local_pair_evidence(a,b,0)
        self.assertEqual(e['support_pairs'],1);self.assertIsNone(e['outcome'])

    def test_same_completed_prefix_cannot_be_its_own_evidence(self):
        a=point([(100,60,4)]*3,[10]*3)
        b=point([(100,60,4)]*3,[20]*3,1)
        for i,w in enumerate(a['windows']):w.update(completed_begin=i*20,completed_end=(i+1)*20)
        for i,w in enumerate(b['windows']):w.update(completed_begin=i*20,completed_end=(i+1)*20)
        # Different indices must not permit crossing/reusing a complete phase.
        e=_local_pair_evidence(a,b,0)
        self.assertEqual(e['support_pairs'],0);self.assertIsNone(e['outcome'])


class LocalPairControlTests(unittest.TestCase):
    def make(self, **kw):
        options=replace(WorkTrace().options,work_comparison='local_pairs',**kw)
        return WorkTrace(total=4093,maximum=6,options=options)

    def run_trace(self,trace,mode='gain'):
        for _ in range(200):
            c=trace.c
            if mode=='gain':
                iw,cached,n=[(100,40,20),(140,60,14),(120,50,16)][trace.now%3]
                if c.current_window==6:n*=2
            elif mode=='cache_only':
                iw=100;cached=80 if c._changes else 20;n=40 if cached==80 else 10
            elif mode=='order_conflict':
                iw,cached=100,40;n=30 if c._probe_candidate is not None else 20 if c.current_window==6 else 10
            elif mode=='no_support':
                iw=100;cached=80 if c.current_window==6 else 20;n=30 if c.current_window==6 else 10
            else:raise AssertionError(mode)
            trace.tick(n,cached=cached,iw=iw)
            if c._growth_stopped:return c.report()
        raise AssertionError('finite original budget did not stop')

    def test_natural_work_variation_can_be_accepted_after_fresh_safe_reference(self):
        trace=self.make();r=self.run_trace(trace)
        self.assertEqual(r['recommended_window'],6)
        self.assertEqual(r['accepted_steps'],1)
        self.assertEqual(r['reference_rechecks'],1)
        self.assertIn('performance_accept',[h['reason'] for h in r['history']])
        self.assertTrue(any(p['input_max']>p['input_min']*1.03 for p in r['measurements']))
        self.assertTrue(r['comparable_selection'])
        self.assertTrue(all(len(p['windows'])<=6 for p in r['measurements']))
        for point in r['measurements']:
            self.assertTrue(all(a['completed_end']<=b['completed_begin']
                                for a,b in zip(point['windows'],point['windows'][1:])))

    def test_warming_reference_cancels_apparent_cache_only_gain(self):
        r=self.run_trace(self.make(),'cache_only')
        self.assertEqual(r['accepted_steps'],0);self.assertEqual(r['recommended_window'],3)
        self.assertEqual(r['reference_rechecks'],1)

    def test_changing_reference_enough_to_reverse_ranking_is_not_accepted(self):
        r=self.run_trace(self.make(),'order_conflict')
        self.assertEqual(r['accepted_steps'],0)
        self.assertTrue(any(e['kind']=='reference_order_conflict' for e in r['evaluations']))
        self.assertEqual(r['search_state']['points'][0]['outcome'],'noise')

    def partial_reference(self, old_matching_count):
        trace=self.make(poll_interval_s=.1,window_s=.25,min_samples=16,min_remaining=64,exploration_s=10)
        for i in range(100):
            c=trace.c
            if c._changes==0:
                iw,computed,n=((128,64,old_matching_count),(160,32,80),(96,80,80))[i%3]
            elif c.current_window==6:iw,computed,n=128,64,100
            else:iw,computed,n=128,64,50
            trace.now+=.25;trace.count+=n;trace.inputs+=n*iw;trace.outputs+=n*4
            trace.computed+=n*computed;trace.cached+=n*(iw-computed)
            c.update(AdmissionObservation(trace.now,True,c.current_window,c.current_window,0,.11,
                completed_items_total=trace.count,pending_items=4093-trace.count,
                completed_input_work_total=trace.inputs,completed_output_work_total=trace.outputs,
                completed_computed_input_work_total=trace.computed,completed_cached_input_work_total=trace.cached))
            if c._growth_stopped:break
        return trace,c.report()

    def test_one_earlier_adverse_pair_cannot_disappear_in_unresolved_summary(self):
        trace,r=self.partial_reference(200)
        evidence=[e for e in r['evaluations'] if e['kind']=='local_pair_evidence'
                  and e['earlier_reference_evidence'] is not None and e['evidence']['outcome']=='gain']
        self.assertTrue(evidence)
        before=evidence[-1]['earlier_reference_evidence']
        self.assertIsNone(before['outcome']);self.assertEqual(before['support_pairs'],1)
        self.assertEqual(before['not_heavier_pairs'][0]['gain'],-.5)
        self.assertEqual(r['accepted_steps'],0);self.assertEqual(r['recommended_window'],3)
        self.assertTrue(any(e['kind']=='reference_order_conflict' for e in r['evaluations']))
        self.assertEqual(r['search_state']['points'][0]['outcome'],'noise')
        self.assertNotIn('verified_window',r['search_state'])
        self.assertEqual(trace.c.started,0);self.assertLessEqual(trace.now,10)

    def test_unresolved_earlier_nonadverse_pair_does_not_block_fresh_support(self):
        _,r=self.partial_reference(50)
        self.assertEqual(r['accepted_steps'],1);self.assertEqual(r['recommended_window'],6)
        self.assertFalse(any(e['kind']=='reference_order_conflict' for e in r['evaluations']))

    def test_missing_support_is_unresolved_not_an_optimum(self):
        r=self.run_trace(self.make(),'no_support')
        self.assertEqual(r['accepted_steps'],0)
        self.assertNotIn('verified_window',r['search_state'])
        self.assertNotEqual(r['stop_reason'],'sampled_range_complete')
        self.assertTrue(r['exploration_censored'])

    def test_work_equality_tolerance_does_not_control_pair_acceptance(self):
        reports=[self.run_trace(self.make(work_tolerance=value)) for value in (0,.5)]
        self.assertEqual([r['recommended_window'] for r in reports],[6,6])
        self.assertEqual([r['accepted_steps'] for r in reports],[1,1])

    def test_original_budget_and_change_limit_do_not_reset_on_recheck(self):
        trace=self.make(exploration_s=12);r=self.run_trace(trace)
        self.assertEqual(trace.c.started,0);self.assertLessEqual(trace.now,12)
        self.assertLessEqual(r['changes'],trace.options.max_changes)
        self.assertEqual(r['accepted_steps'],0);self.assertEqual(r['window'],3)
        self.assertEqual(trace.c._search.here,{6})

    def test_short_fixed_unknown_and_pressure_controls_remain(self):
        for mode in ('short','fixed','stale','memory'):
            trace=self.make(allow_exploration=mode!='fixed',memory_floor_bytes=int(mode=='memory'))
            for _ in range(20):trace.tick(pending=1 if mode=='short' else 4000,fresh=mode!='stale')
            self.assertEqual(trace.c.report()['changes'],0)
        trace=self.make()
        while not trace.c._changes:trace.tick(cached=40 if trace.now%2 else 60)
        trace.tick(kv=.99);trace.tick(kv=.99)
        self.assertEqual(trace.c.current_window,3)
        self.assertIsNone(trace.c._probe_candidate)
        self.assertFalse(trace.c.report()['can_continue'])

    def test_transition_prefix_is_not_used_in_local_pair_windows(self):
        trace=self.make()
        while not trace.c._changes:trace.tick(cached=40 if trace.now%2 else 60)
        for n in (2,7,1):trace.tick(n,cached=99,transition=1)
        end=trace.count;r=self.run_trace(trace)
        candidates=[p for p in r['measurements'] if p['control_phase']=='probe']
        self.assertTrue(candidates)
        self.assertTrue(all(p['completed_begin']>=end for p in candidates))
        self.assertTrue(all(p['cached']!=99 for point in candidates for p in point['windows']))

    def test_configuration_requires_existing_measured_work_path(self):
        with self.assertRaises(ValueError):ResourceFeedbackConfig(work_comparison='local_pairs')
        with self.assertRaises(ValueError):replace(self.make().options,work_comparison='invented')


class PairReuseTests(unittest.TestCase):
    setUp=fixtures.GenericReuseTests.setUp
    invoke=fixtures.GenericReuseTests.invoke

    def test_same_features_rename_reuse_and_changed_identity_revalidation(self):
        self.options=replace(self.options,work_comparison='local_pairs')
        first=self.invoke()
        self.identity['workload']={'query_id':'unrelated-name','path':'other','rows':113}
        second=self.invoke()
        self.assertTrue(second['cache_hit']);self.assertEqual(second['configuration']['max_inflight'],6)
        self.assertEqual(second['result']['admission_feedback']['changes'],0)
        self.assertEqual(first['configuration']['max_inflight'],3)
        features=deepcopy(self.features);features['input_work']['mean']=300
        self.assertFalse(self.invoke(features=features)['cache_hit'])

    def test_failed_execution_cannot_publish_pair_progress(self):
        self.options=replace(self.options,work_comparison='local_pairs')
        self.invoke(status='failed')
        from pathlib import Path
        self.assertEqual(list(Path(self.tmp.name).iterdir()),[])


class PairCoreTests(unittest.TestCase):
    def test_option_reaches_same_production_owner_and_real_cpu_ipc(self):
        from tests.execution_provider.test_core_resource_feedback import CoreFeedbackTests
        from tests.scheduling.test_trial_admission import CoreTrialDeliveryTests
        original=CoreFeedbackTests.owner;made=[]
        def owner(fixture,**kw):
            kw['options']=dict(kw.get('options',{}),work_comparison='local_pairs')
            result=original(fixture,**kw);made.append(result);return result
        with patch.object(CoreFeedbackTests,'owner',owner):
            CoreTrialDeliveryTests().test_original_requests_continue_once_through_cpu_ipc_and_same_resident_owner()
        self.assertEqual(len(made),1)
        self.assertEqual(made[0]._feedback.base_options.work_comparison,'local_pairs')


if __name__=='__main__':unittest.main()
