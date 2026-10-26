"""Increasing public search: synthetic traces, explicit bounds and real consumers."""
import json
from pathlib import Path
import tempfile
import unittest

from src.scheduling.runtime import calibration as runtime
from tests.scheduling.test_runtime_calibration import Clock


class PlateauSearchTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.root=Path(temp.name)
        self.clock=Clock();self.calls=[];self.executed=[]
        self.identity=dict(hardware=dict(cpu=8),model=dict(revision='CPU-fixture'),
            generation=dict(fixed=True),execution=dict(max_num_seqs=64),
            workload=dict(rows=3),measurement=dict(work_unit='rows',objective='complete workload'))
        self.seed=[dict(workers=1,chunk=1)]

    def search(self,cap=64,**kwargs):
        return runtime.IncreasingSearch('workers',dict(declared_capacity=cap),**kwargs)

    def options(self,trace,*,search=None,max_work=1000,timeout=1000,fail=None):
        def measure(config,*,repeat_index,deadline_monotonic,work_limit):
            self.calls.append((config['workers'],repeat_index));self.clock.advance(1)
            if fail is not None and config['workers']==4:return fail()
            value=trace[config['workers']]
            return runtime.Measurement(value[repeat_index-1] if isinstance(value,list) else value,'ok',True,3)
        return dict(cache_dir=self.root,measure=measure,search=search or self.search(),
            budget=runtime.ScanBudget(timeout,max_work),work_per_sample=3,clock=self.clock)

    def test_continuous_improvement_stops_at_capacity_not_plateau_and_applies_choice(self):
        from concurrent.futures import ThreadPoolExecutor
        def execute(candidate):
            with ThreadPoolExecutor(max_workers=candidate['workers']) as pool:
                self.executed.append(pool._max_workers);return list(pool.map(lambda n:n*n,[1,2,3]))
        result=runtime.run_calibrated(self.identity,self.seed,execute,
            **self.options({1:100,2:50,4:25,8:12},search=self.search(8)))
        resolution=result['calibration']
        self.assertEqual(result['result'],[1,4,9]);self.assertEqual(self.executed,[8])
        self.assertEqual(resolution['search']['stop_reason'],'capacity_limit')
        self.assertFalse(resolution['search']['platform_confirmed'])
        self.assertEqual(len(self.calls),12)

    def test_two_low_gain_growth_steps_confirm_local_plateau_before_cap(self):
        result=runtime.calibrate(self.identity,self.seed,**self.options({1:100,2:50,4:49.8,8:49.7}))
        self.assertEqual(result['search']['stop_reason'],'plateau')
        self.assertTrue(result['search']['platform_confirmed'])
        self.assertEqual(result['candidate']['workers'],2)  # Existing near-peak rule.
        self.assertEqual({c for c,_ in self.calls},{1,2,4,8})

    def test_wide_noise_ranges_do_not_turn_flat_medians_into_a_platform(self):
        result=runtime.calibrate(self.identity,self.seed,**self.options(
            {1:100,2:50,4:[40,50,60],8:[39,50,61],16:[39,49,62]},search=self.search(16)))
        self.assertEqual(result['search']['stop_reason'],'capacity_limit')
        self.assertFalse(result['search']['platform_confirmed'])

    def test_confirmed_slowdown_stops_and_returns_smaller_healthy_configuration(self):
        result=runtime.calibrate(self.identity,self.seed,**self.options({1:100,2:50,4:70}))
        self.assertEqual(result['search']['stop_reason'],'regression')
        self.assertFalse(result['search']['platform_confirmed'])
        self.assertEqual(result['candidate']['workers'],2)
        self.assertEqual({c for c,_ in self.calls},{1,2,4})

    def test_named_resource_ceiling_is_not_engine_capacity_or_plateau(self):
        result=runtime.calibrate(self.identity,self.seed,**self.options({1:100,2:50,4:25},
            search=self.search(resource_limits=dict(safe_resource_cap=4))))
        self.assertEqual(result['search']['stop_reason'],'resource_limit')
        self.assertEqual(result['search']['limits_hit'],[dict(kind='resource_limit',name='safe_resource_cap',value=4)])

    def test_non_power_of_two_boundary_is_probed_and_never_called_platform(self):
        result=runtime.calibrate(self.identity,self.seed,**self.options(
            {1:100,2:50,4:25,8:12,10:10},search=self.search(10)))
        self.assertEqual(result['candidate']['workers'],10)
        self.assertEqual(result['search']['stop_reason'],'capacity_limit')

    def test_work_budget_stops_before_partial_next_point_no_target_or_cached_success(self):
        with self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.run_calibrated(self.identity,self.seed,lambda c:self.executed.append(c),
                **self.options({1:100},max_work=12))
        report=caught.exception.report
        self.assertEqual(report['search']['stop_reason'],'work_budget')
        self.assertEqual(report['search']['lowest_median_candidate']['workers'],1)
        self.assertEqual(report['scan_cost']['reported_work'],9)
        self.assertEqual(self.executed,[]);self.assertEqual(list(self.root.iterdir()),[])

    def test_time_budget_has_distinct_reason_and_retains_completed_samples(self):
        with self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.calibrate(self.identity,self.seed,**self.options({1:100,2:50},timeout=4))
        self.assertEqual(caught.exception.report['search']['stop_reason'],'time_budget')
        self.assertFalse(caught.exception.report['search']['platform_confirmed'])
        self.assertEqual(len(self.calls),4)

    def test_failed_probe_preserves_cost_and_never_calls_larger_or_target(self):
        def fail():return runtime.Measurement(1,'failed',True,3)
        with self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.run_calibrated(self.identity,self.seed,lambda c:self.executed.append(c),
                **self.options({1:100,2:50},fail=fail))
        self.assertEqual(caught.exception.report['search']['stop_reason'],'measurement_failed')
        self.assertEqual(caught.exception.report['scan_cost']['reported_work'],21)
        self.assertEqual(self.executed,[]);self.assertEqual(self.calls[-1],(4,1))

    def test_resource_failure_stops_even_if_prior_point_was_valid(self):
        def fail():return runtime.Measurement(1,'ok',False,3)
        with self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.calibrate(self.identity,self.seed,**self.options({1:100,2:50},fail=fail))
        self.assertEqual(caught.exception.report['search']['stop_reason'],'resource_limit')
        self.assertEqual(list(self.root.iterdir()),[])

    def test_unknown_callback_never_retries_or_publishes_and_keeps_reservation_unknown(self):
        def fail():raise RuntimeError('CPU fixture unconfirmed cleanup')
        with self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.calibrate(self.identity,self.seed,**self.options({1:100,2:50},fail=fail))
        self.assertEqual(caught.exception.report['search']['stop_reason'],'unconfirmed_callback')
        self.assertEqual(caught.exception.report['scan_cost']['unreported_reserved_work'],3)
        self.assertEqual(self.calls[-1],(4,1));self.assertEqual(list(self.root.iterdir()),[])

    def test_cached_search_has_zero_probes_and_keeps_original_cost_and_stopping_reason(self):
        options=self.options({1:100,2:50,4:49.8,8:49.7})
        first=runtime.calibrate(self.identity,self.seed,**options);count=len(self.calls)
        hit=runtime.calibrate(self.identity,self.seed,**options)
        self.assertTrue(hit['cache_hit']);self.assertEqual(len(self.calls),count)
        self.assertEqual(hit['scan_cost']['reported_work'],0)
        self.assertEqual(hit['calibration_cost'],first['calibration_cost'])
        self.assertEqual(hit['search'],first['search'])

    def test_capacity_search_scope_and_model_max_num_seqs_change_isolate_cache(self):
        runtime.calibrate(self.identity,self.seed,**self.options({1:100,2:50,4:25,8:12},search=self.search(8)))
        for identity,search in [(self.identity,self.search(16)),
                (dict(self.identity,execution=dict(max_num_seqs=128)),self.search(8))]:
            miss=runtime.calibrate(identity,self.seed,cache_dir=self.root,search=search)
            self.assertEqual(miss['status'],'needs_scan');self.assertFalse(miss['cache_hit'])

    def test_search_policy_isolation_and_forged_platform_flag_rejection(self):
        result=runtime.calibrate(self.identity,self.seed,**self.options({1:100,2:50,4:25,8:12},search=self.search(8)))
        miss=runtime.calibrate(self.identity,self.seed,cache_dir=self.root,search=self.search(8,min_improvement=.05))
        self.assertEqual(miss['status'],'needs_scan')
        path=Path(result['selection_path']);payload=json.loads(path.read_text());payload['search_evidence']['platform_confirmed']=True
        path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(ValueError,'evidence differs'):
            runtime.calibrate(self.identity,self.seed,cache_dir=self.root,search=self.search(8))

    def test_fixed_grid_and_progressive_search_do_not_share_cache(self):
        runtime.calibrate(self.identity,self.seed,**self.options({1:100,2:50,4:25,8:12},search=self.search(8)))
        fixed=runtime.calibrate(self.identity,[dict(workers=n,chunk=1) for n in (1,2,4,8)],cache_dir=self.root)
        self.assertEqual(fixed['status'],'needs_scan')

    def test_search_validation_does_not_treat_eight_as_a_universal_limit(self):
        values=runtime.search_candidates(dict(workers=1),self.search(128))
        self.assertEqual([x['workers'] for x in values],[1,2,4,8,16,32,64,128])
        with self.assertRaises(ValueError):runtime.IncreasingSearch('workers',{})
        with self.assertRaises(ValueError):runtime.calibrate(self.identity,self.seed,
            cache_dir=self.root,search=self.search(),repetitions=1)


if __name__=='__main__':unittest.main()
