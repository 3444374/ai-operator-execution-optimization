"""Permission, identity and physical-position checks with CPU fixtures only."""
from dataclasses import replace
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.resource_probe import (
    ResourceProbeView, ResourceRequestPermit, resource_request_opportunity,
)
from src.scheduling.submission_control.resource_step import NativeKVFrame, RequestKVWork, propose_kv_window
from src.scheduling.submission_control.stage_work import SupplyState


class ResourceRequestProbeTests(unittest.TestCase):
    def setUp(self):
        self.observation = AdmissionObservation(20., True, 2, 2, 0, .99,
            sample_age_s=.1, sample_sequence=5, preemptions_total=0, pending_items=2000)
        self.supply = SupplyState(20., 2000, 2100, 100, 1, True, False, active_requests=2)
        self.permit = ResourceRequestPermit('CPU-engine', 7, 5, 13, 20., 2000,
            local_limits_enforced=True, backend_owns_memory=True)

    def view(self, index, **changes):
        return replace(ResourceProbeView('CPU-engine', 7, index, 19.5+index*.1,
            19.6+index*.1, .02, ('GPU-cpu-fixture',), 20., 20.), **changes)

    def opportunity(self, samples=(), *, observation=None, supply=None, permit=None, **kwargs):
        return resource_request_opportunity(observation or self.observation, supply or self.supply,
            current_window=2, hard_limit=2000, max_age_s=1.,
            expected_engine_uid='CPU-engine', expected_session_id=7,
            permit=self.permit if permit is None else permit, physical_samples=samples, **kwargs)

    def test_continuous_busy_values_do_not_have_a_fifty_percent_cutoff(self):
        targets = []
        for busy in (49., 50., 51., 90.):
            result = self.opportunity(tuple(self.view(i, gpu_busy_pct=busy) for i in (1, 2)))
            self.assertEqual(result.kind, 'idle_device_headroom')
            self.assertFalse(result.evidence['physical_capacity_known'])
            targets.append(result.probe_target_window)
        self.assertEqual(targets, sorted(targets, reverse=True))
        self.assertTrue(all(target > 128 for target in targets))

    def test_latest_busy_read_is_not_replaced_with_a_more_favourable_older_read(self):
        pair = self.opportunity((self.view(1), self.view(2)))
        latest = self.opportunity((self.view(1), self.view(2), self.view(3, gpu_busy_pct=95.)))
        self.assertLess(latest.probe_target_window, pair.probe_target_window)
        self.assertEqual([v['request_index'] for v in latest.evidence['physical_samples']], [2, 3])

    def test_physical_busy_does_not_shrink_a_qualified_native_geometric_trial(self):
        native = self.opportunity()
        for busy in (99., 99.99, 100.):
            with self.subTest(busy=busy):
                physical = self.opportunity(tuple(self.view(i, gpu_busy_pct=busy) for i in (1, 2)))
                self.assertGreaterEqual(physical.probe_target_window, native.probe_target_window)
                self.assertFalse(physical.evidence['physical_capacity_known'])

    def test_prefix_metadata_separates_available_work_from_geometric_spacing(self):
        opportunity = self.opportunity(supply=replace(self.supply, ready_items=1))
        self.assertEqual(opportunity.probe_target_window, 3)
        self.assertEqual(opportunity.evidence['requested_native_target'], 4)
        self.assertEqual(opportunity.evidence['native_trial_target'], 3)
        self.assertEqual(opportunity.evidence['measurable_end'], 3)
        self.assertEqual(opportunity.evidence['clip_causes'], ['ready'])

    def test_unknown_or_foreign_physical_data_only_keeps_the_native_trial(self):
        changes = ({'engine_uid': 'foreign'}, {'session_id': 8}, {'gpu_uuids': ()},
                   {'requested_at_s': 1.}, {'host_peak_cpu_pct': None}, {'gpu_busy_pct': float('nan')},
                   {'source_elapsed_s': 100.}, {'gpu_busy_pct': 101.})
        for change in changes:
            with self.subTest(change=change):
                result = self.opportunity((self.view(1), self.view(2, **change)))
                self.assertEqual((result.kind, result.probe_target_window), ('native_headroom', 4))
                self.assertNotIn('physical_samples', result.evidence)

    def test_duplicate_or_conflicting_RPC_identity_does_not_make_two_samples(self):
        for samples in ((self.view(1), self.view(1)),
                        (self.view(1), self.view(2), self.view(2, gpu_busy_pct=0.))):
            with self.subTest(samples=samples):
                result = self.opportunity(samples)
                self.assertEqual((result.kind, result.probe_target_window), ('native_headroom', 4))

    def test_actual_pending_and_ready_work_bound_any_large_position(self):
        result = self.opportunity((self.view(1), self.view(2)),
            observation=replace(self.observation, pending_items=3))
        self.assertLessEqual(result.probe_target_window, 5)
        result = self.opportunity((self.view(1), self.view(2)),
            supply=replace(self.supply, ready_items=1))
        self.assertEqual(result.probe_target_window, 3)

    def test_stale_native_or_permit_data_does_not_claim_protected_headroom(self):
        for observation, permit in ((replace(self.observation, sample_age_s=1.01), self.permit),
                                    (self.observation, replace(self.permit, observed_at_s=18.))):
            result = self.opportunity((self.view(1), self.view(2)), observation=observation, permit=permit)
            self.assertEqual(result.kind, 'none')

    def step(self):
        return propose_kv_window(NativeKVFrame('CPU-engine', 7, 13, 19.9, 20.,
            2000, 16, .50, cost_model='single_pool_token_blocks'), now_s=20., max_age_s=1.,
            expected_engine_uid='CPU-engine', expected_session_id=7,
            current_window=2, hard_limit=2000,
            committed=tuple(RequestKVWork(('active', i), 16, 16) for i in range(2)),
            ready=tuple(RequestKVWork(('ready', i), 16, 16) for i in range(1000)))

    def test_costed_step_uses_actual_prepared_work_and_the_same_native_sample(self):
        step = self.step()
        result = self.opportunity(resource_step=step)
        self.assertEqual(result.reason, 'costed_ready_prefix_trial')
        self.assertEqual(result.probe_target_window, step.target_window)
        self.assertFalse(result.evidence['physical_capacity_known'])

    def test_costed_step_cannot_bypass_operational_permission_or_source_checks(self):
        step = self.step()
        result = self.opportunity(resource_step=step, permit=replace(self.permit, request_limit=10))
        self.assertEqual(result.probe_target_window, 10)
        for name, value in (('engine_uid', 'foreign'), ('sequence', 14), ('cost_model', None)):
            changed = replace(step, evidence=dict(step.evidence, **{name: value}))
            result = self.opportunity(resource_step=changed)
            self.assertEqual(result.probe_target_window, 4)
            self.assertNotIn('costed_resource_step', result.evidence)


if __name__ == '__main__':
    unittest.main()
