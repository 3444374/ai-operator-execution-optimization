"""Variable tentative search positions; synthetic resources and useful work only."""
from dataclasses import asdict, replace
import unittest

from src.planning.work import StageWork
from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.request_scan import AscendingRequestController
from src.scheduling.submission_control.resource_probe import ResourceProbeView, resource_probe_opportunity
from src.scheduling.submission_control.stage_work import SupplyState, default_request_feedback_options
from tests.scheduling.test_ascending_request_scan import ScanTrace


class ResourceProbeVariableStepTests(unittest.TestCase):
    def opportunity(self, *, current=1, maximum=64, ready=100, gpu=20., host=25., kv=.1,
                    now=20., index=0, observation_changes=None, supply_changes=None,
                    preemption_increased=False):
        observation = AdmissionObservation(now, True, current, current, 0, kv,
                                           sample_age_s=0, preemptions_total=0)
        supply = SupplyState(now, ready, 200, 100, 1, True, False, active_requests=current)
        pair = tuple(ResourceProbeView('CPU-variable-step', 7, index*2+i+1,
            now-.4+i*.1, now-.3+i*.1, .05,
            ('GPU-00000000-0000-0000-0000-000000000001',),
            gpu[i] if isinstance(gpu, tuple) else gpu,
            host[i] if isinstance(host, tuple) else host) for i in (0, 1))
        return resource_probe_opportunity(replace(observation, **(observation_changes or {})),
            replace(supply, **(supply_changes or {})), current_window=current, hard_limit=maximum,
            max_age_s=1., expected_engine_uid='CPU-variable-step', expected_session_id=7,
            physical_samples=pair, preemption_increased=preemption_increased)

    def drive(self, trace, *, ticks, gpu=20., host=25., ready=100, **changes):
        for _ in range(ticks):
            window, now = trace.controller.current_window, trace.now+1
            opportunity = self.opportunity(current=window, maximum=trace.controller.config.max_window,
                ready=ready, gpu=gpu, host=host, now=now, index=int(now), **changes)
            trace.tick(40, ready_items=ready, resource_opportunity=opportunity)
        return trace.controller.report()

    def test_more_joint_idle_share_selects_further_different_probe_positions(self):
        cases = ((5., 5., .05), (20., 25., .1), (49., 49., .49))
        outcomes = [self.opportunity(gpu=gpu, host=host, kv=kv) for gpu, host, kv in cases]
        self.assertEqual([result.probe_target_window for result in outcomes], [60, 48, 33])
        self.assertTrue(all(result.probe_target_window > 4 for result in outcomes))
        self.assertTrue(all(not result.evidence['capacity_estimate'] for result in outcomes))
        self.assertTrue(all(result.evidence['tentative'] for result in outcomes))

    def test_each_sample_peak_and_native_KV_limit_the_joint_signal(self):
        mixed = self.opportunity(gpu=(5., 45.), host=(5., 10.))
        self.assertEqual(mixed.probe_target_window, 35, 'one low read cannot hide a second busier device read')
        kv_bound = self.opportunity(gpu=10., host=10., kv=.4)
        self.assertEqual(kv_bound.probe_target_window, 38)

    def test_ready_work_and_non_power_of_two_R_bound_the_trial_interval(self):
        supply_bound = self.opportunity(current=2, maximum=37, ready=3, gpu=10., host=20.)
        capacity_bound = self.opportunity(current=32, maximum=37)
        single_ready = self.opportunity(current=6, maximum=37, ready=1)
        self.assertEqual(supply_bound.probe_target_window, 4)
        self.assertEqual(capacity_bound.probe_target_window, 35)
        self.assertEqual(single_ready.probe_target_window, 7)
        self.assertIsNone(self.opportunity(current=37, maximum=37).probe_target_window)

    def test_missing_or_hot_physical_metrics_do_not_choose_an_enlarged_target(self):
        for gpu, host in ((None, 25.), (20., None), (80., 25.), (20., 80.)):
            with self.subTest(gpu=gpu, host=host):
                opportunity = self.opportunity(gpu=gpu, host=host)
                self.assertEqual(opportunity.kind, 'native_headroom')
                self.assertIsNone(opportunity.probe_target_window)

    def test_low_supply_queued_pressure_and_byte_backpressure_win_over_idle_metrics(self):
        cases = (
            dict(ready=0),
            dict(observation_changes=dict(inflight=0)),
            dict(supply_changes=dict(active_requests=None)),
            dict(observation_changes=dict(waiting=1)),
            dict(observation_changes=dict(memory_pressure=True)),
            dict(kv=.9),
            dict(preemption_increased=True),
            dict(supply_changes=dict(capacity_blocked='result_bytes')),
        )
        for changes in cases:
            with self.subTest(changes=changes):
                opportunity = self.opportunity(gpu=0., host=0., **changes)
                self.assertEqual(opportunity.kind, 'none')
                self.assertIsNone(opportunity.probe_target_window)

    def test_large_no_gain_trial_rolls_back_and_unchanged_unknown_coverage_does_not_repeat_it(self):
        reports = []
        for periodic in (False, True):
            trace = ScanTrace(maximum=64)
            for index in range(200):
                unknown = periodic and (index//8) % 2 == 1
                self.drive(trace, ticks=1, gpu=None if unknown else 20., host=None if unknown else 25.)
            report = trace.controller.report()
            reports.append(report)
            self.assertGreater(report['history'][0]['window'], 4)
            self.assertEqual((report['window'], report['recommended_window'], report['changes']), (1, 1, 2))
            self.assertEqual(report['stop_reason'], 'local_plateau')
        self.assertEqual(reports[0]['changes'], reports[1]['changes'])

    def test_verified_hot_then_idle_transition_can_reopen_a_completed_flat_scan(self):
        trace = ScanTrace(maximum=64)
        baseline = self.drive(trace, ticks=100)
        self.assertEqual(baseline['changes'], 2)
        hot = self.drive(trace, ticks=6, host=90.)
        self.assertEqual(hot['changes'], baseline['changes'])
        reopened = self.drive(trace, ticks=40)
        self.assertGreater(reopened['changes'], baseline['changes'])
        self.assertTrue(any(event['kind'] == 'new_resource_opportunity' for event in reopened['evaluations']))

    def test_gradual_stronger_idle_signal_accumulates_against_the_last_tested_condition(self):
        trace = ScanTrace(maximum=64)
        baseline = self.drive(trace, ticks=100)
        for host in (24., 23.):
            self.assertEqual(self.drive(trace, ticks=6, host=host)['changes'], baseline['changes'])
            self.drive(trace, ticks=2, gpu=None, host=None)
        reopened = self.drive(trace, ticks=40, host=22.)
        self.assertGreater(reopened['changes'], baseline['changes'],
            'several sub-tolerance idle increases must not keep shifting the measured high-water mark')

    def test_explicit_change_budget_bounds_the_large_trial_and_its_required_return(self):
        trace = ScanTrace(maximum=64)
        options = replace(default_request_feedback_options(), max_changes=2)
        trace.controller = AscendingRequestController(64, options, clock=lambda: trace.now)
        trace.controller.reset(1, 100000)
        report = self.drive(trace, ticks=100)
        self.assertEqual(report['changes'], 2)
        self.assertEqual(report['recommended_window'], 1)
        self.assertTrue(all(1 <= event['window'] <= 64 for event in report['history']))

    def test_native_pressure_shrinks_a_large_unconfirmed_trial_even_with_idle_physical_metrics(self):
        trace = ScanTrace(maximum=64)
        for _ in range(20):
            self.drive(trace, ticks=1)
            if trace.controller.current_window > 4:
                break
        before = trace.controller.current_window
        self.assertGreater(before, 4)
        now = trace.now+1
        opportunity = self.opportunity(current=before, now=now, index=int(now),
                                       observation_changes=dict(memory_pressure=True))
        self.assertIsNone(opportunity.probe_target_window)
        decision = trace.tick(40, memory_pressure=True, resource_opportunity=opportunity)
        self.assertEqual(decision.reason, 'resource_pressure')
        self.assertLess(decision.window, before)
        self.assertGreaterEqual(decision.window, 1)

    def test_stage_units_and_unknown_capacity_are_audited_without_inventing_heterogeneous_actions(self):
        work = (StageWork('prepare_bytes', 2048, 'bytes'), StageWork('model_tokens', 4096, 'tokens'))
        capacity = dict(cpu_workers=None, host_memory_bytes=None,
                        transport=dict(input_bytes_limit=8192, input_bytes_used=1024),
                        io_bandwidth_bytes_s=None)
        supply_changes = dict(ready_work=work, local_capacity=capacity, source_exhausted=True)
        result = self.opportunity(gpu=10., host=20., supply_changes=supply_changes)
        audit = result.evidence['stage_supply']
        self.assertEqual(audit['ready_work'], [asdict(stage) for stage in work])
        self.assertEqual(audit['local_capacity'], capacity)
        self.assertTrue(audit['source_exhausted'])
        self.assertIsNone(audit['local_capacity']['cpu_workers'])
        self.assertIsNone(audit['local_capacity']['io_bandwidth_bytes_s'])
        self.assertIn('request permits', result.evidence['action_scope'])
        self.assertEqual(result.probe_target_window, self.opportunity(gpu=10., host=20.).probe_target_window,
            'bytes and tokens remain different declared units, not a summed physical capacity estimate')
        capacity['transport']['input_bytes_used'] = 2048
        self.assertEqual(audit['local_capacity']['transport']['input_bytes_used'], 1024)
        audit['local_capacity']['transport']['input_bytes_used'] = 0
        audit['ready_work'][0]['units'] = 0
        self.assertEqual(capacity['transport']['input_bytes_used'], 2048)
        self.assertEqual(work[0].units, 2048)
        transport_blocked = self.opportunity(gpu=10., host=20.,
            supply_changes=dict(supply_changes, capacity_blocked='input_bytes'))
        self.assertEqual(transport_blocked.kind, 'none')
        self.assertIsNone(transport_blocked.probe_target_window)
        self.assertEqual(transport_blocked.evidence['stage_supply']['capacity_blocked'], 'input_bytes')
        cpu_hot = self.opportunity(gpu=10., host=95., supply_changes=supply_changes)
        self.assertEqual(cpu_hot.condition['kind'], 'physical_busy')
        self.assertIsNone(cpu_hot.probe_target_window, 'an idle GPU cannot override observed host CPU pressure')


if __name__ == '__main__':
    unittest.main()
