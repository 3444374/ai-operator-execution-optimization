"""CPU counterexamples for disabling only the physical probe shortcut."""
from dataclasses import replace
import unittest

from src.scheduling.submission_control.resource_probe import ResourceProbeView
from src.scheduling.submission_control.stage_work import default_request_feedback_options
from tests.scheduling.test_ascending_request_scan import ScanTrace
from tests.scheduling import test_resource_probe as probe_fixture


class ResourceProbePolicyTests(unittest.TestCase):
    setUp = probe_fixture.ResourceProbeTests.setUp
    view = probe_fixture.ResourceProbeTests.view
    opportunity = probe_fixture.ResourceProbeTests.opportunity

    def test_explicit_default_keeps_the_existing_double_fifty_rule(self):
        samples = (self.view(1), self.view(2))
        self.assertEqual(self.opportunity(samples),
                         self.opportunity(samples, physical_probe_policy='idle_majority'))
        for change in (dict(gpu_busy_pct=50.), dict(host_peak_cpu_pct=50.)):
            result = self.opportunity((self.view(1), self.view(2, **change)))
            self.assertEqual((result.kind, result.probe_target_window), ('native_headroom', None))

    def test_native_only_disables_the_shortcut_and_keeps_the_regular_opportunity(self):
        result = self.opportunity((self.view(1), self.view(2)), physical_probe_policy='native_only')
        self.assertEqual((result.kind, result.probe_target_window), ('native_headroom', None))
        self.assertEqual(result.reason, 'physical_acceleration_disabled')
        self.assertEqual(result.evidence['physical_probe_policy'], 'native_only')
        self.assertFalse(result.evidence['capacity_estimate'])

    def test_native_only_still_rejects_pressure_unknown_feedback_and_missing_supply(self):
        observations = (dict(memory_pressure=True), dict(waiting=1), dict(kv_usage=.99),
            dict(fresh=False), dict(sample_age_s=2.), dict(running=None),
            dict(transition_pending=1), dict(inflight=1), dict(consumer_backpressure=True))
        for change in observations:
            with self.subTest(change=change):
                result = self.opportunity(physical_probe_policy='native_only',
                    observation=replace(self.observation, **change))
                self.assertEqual((result.kind, result.probe_target_window), ('none', None))
        for change in (dict(ready_items=0), dict(exhausted=True),
                       dict(capacity_blocked='result_bytes'), dict(active_requests=1)):
            with self.subTest(change=change):
                result = self.opportunity(physical_probe_policy='native_only',
                    supply=replace(self.supply, **change))
                self.assertEqual((result.kind, result.probe_target_window), ('none', None))
        for change in (dict(preemption_increased=True), dict(hard_limit=2)):
            with self.subTest(change=change):
                self.assertEqual(self.opportunity(physical_probe_policy='native_only', **change).kind, 'none')

    def test_unknown_physical_reads_do_not_become_resource_capacity(self):
        result = self.opportunity((self.view(1), self.view(2, host_peak_cpu_pct=None)),
                                  physical_probe_policy='native_only')
        self.assertEqual((result.kind, result.probe_target_window), ('native_headroom', None))
        self.assertNotIn('probe_position', result.evidence)

    def test_invalid_or_misplaced_policy_is_rejected(self):
        options = default_request_feedback_options()
        for value in ('unchecked', 50, True, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.opportunity(physical_probe_policy=value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                replace(options, physical_probe_policy=value)
        with self.assertRaisesRegex(ValueError, 'ascending'):
            replace(options, search_policy='reference', physical_probe_policy='native_only')

    def test_no_measured_reference_means_no_probe_even_with_ready_work(self):
        trace = ScanTrace()
        trace.controller.options = replace(trace.controller.options, physical_probe_policy='native_only')
        for _ in range(10):
            opportunity = self.opportunity(current_window=1,
                observation=replace(self.observation, inflight=1, running=1),
                supply=replace(self.supply, active_requests=1), physical_probe_policy='native_only')
            trace.tick(0, resource_opportunity=opportunity)
        self.assertEqual(trace.controller.report()['changes'], 0)
        self.assertIsNone(trace.controller._reference)

    def test_changed_policy_cannot_reuse_an_old_query_hint(self):
        trace = ScanTrace()
        trace.run_until(lambda report, _: report['recommended_window'] >= 2)
        hint = trace.controller.export_hint()
        trace.controller.options = replace(trace.controller.options, physical_probe_policy='native_only')
        with self.assertRaisesRegex(ValueError, 'incompatible'):
            trace.controller.reset(hint['selected_window'], 1000, search_hint=hint)

    def test_adapter_passes_the_policy_to_the_actual_opportunity_function(self):
        from tests.execution_provider.test_lotus_ascending_resources import ResourceWiringTests
        fixture = ResourceWiringTests()
        fixture.feedback()
        fixture.f.config = replace(fixture.f.config, physical_probe_policy='native_only')
        fixture.f.controller.options = fixture.f.config
        for _ in range(5):
            now = fixture.now+1
            fixture.f.resource_samples = [ResourceProbeView('fixture-engine', 1, index,
                now-.2, now-.1, .05, ('GPU-fixture',), 10., 10.) for index in (1, 2)]
            fixture.tick()
        report = fixture.f.controller.report()
        self.assertEqual(report['window'], 2)
        step, = report['step_decisions']
        self.assertEqual(step['resource_observation']['physical_probe_policy'], 'native_only')
        self.assertIsNone(step['requested_probe_target'])
