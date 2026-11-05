"""Capacity-policy cache isolation; CPU configuration fixtures, no model/Ray."""
from copy import deepcopy
from dataclasses import asdict, replace
import unittest

from src.execution_provider.adapters.lotus_records_core import (
    CoreRecordsCapacity, resolve_records_async_limits,
)
from src.experiments.calibration import core
from src.experiments.movie_records_config import records_runtime as movie_runtime
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from tests.experiments import test_core_calibration as fixtures


class CoreCapacityIdentityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CoreCalibrationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.plan = deepcopy(self.fixture.plan)

    def identity(self, plan=None):
        return core.build_core_identity(plan or self.plan, self.fixture.checked,
            hardware=self.fixture.hardware, model=self.fixture.model, software=self.fixture.software)

    def signature(self, plan=None):
        return core.core_identity_signature(self.identity(plan))

    def candidate(self, maximum, plan=None):
        plan = plan or self.plan
        return core.core_candidate_plan(plan, self.fixture.checked,
            dict(max_inflight=maximum, cpu_workers=1), identity=self.identity(plan))

    def test_omitted_empty_and_explicit_independent_defaults_keep_legacy_identity(self):
        expected = self.signature()
        for changes in (dict(core_capacity={}), dict(async_limits={}),
                dict(core_capacity=asdict(CoreRecordsCapacity())),
                dict(core_capacity={}, async_limits=dict(item_input_bytes=2*1024**2,
                    item_result_bytes=2*1024**2, control_frame_bytes=65536,
                    feedback_interval_s=0, control_slots=16))):
            with self.subTest(changes=changes):
                self.assertEqual(self.signature(dict(self.plan, **changes)), expected)

    def test_each_Core_capacity_field_is_bound(self):
        for name, value in dict(item_input_bytes=4*1024**2, item_result_bytes=4*1024**2,
                metadata_bytes=4096, step_actions=3, poll_interval_s=.02).items():
            with self.subTest(field=name):
                self.assertNotEqual(self.signature(dict(self.plan, core_capacity={name:value})), self.signature())

    def test_each_nondefault_IPC_field_is_bound(self):
        values = dict(max_inflight=4, input_bytes=32*1024**2, item_input_bytes=1024**2,
            result_slots=5, result_bytes=32*1024**2, item_result_bytes=65536,
            command_slots=1, control_slots=3, control_frame_bytes=4096, feedback_interval_s=.1)
        for name, value in values.items():
            with self.subTest(field=name):
                self.assertNotEqual(self.signature(dict(self.plan, async_limits={name:value})), self.signature())

    def test_partial_custom_capacity_and_equivalent_defaults_share_identity(self):
        self.plan['core_capacity'] = dict(item_input_bytes=4*1024**2, poll_interval_s=1)
        explicit = deepcopy(self.plan)
        explicit['core_capacity'] = asdict(CoreRecordsCapacity(**self.plan['core_capacity']))
        explicit['core_capacity']['poll_interval_s'] = 1.0
        explicit['async_limits'] = dict(item_input_bytes=4*1024**2,
            item_result_bytes=2*1024**2, feedback_interval_s=0.0)
        self.assertEqual(self.signature(explicit), self.signature())

    def test_explicit_total_is_independent_of_result_slot_count(self):
        self.plan['async_limits'] = dict(result_slots=5, item_result_bytes=65536)
        explicit = deepcopy(self.plan)
        explicit['async_limits']['result_bytes'] = 5*65536
        self.assertNotEqual(self.signature(explicit), self.signature())

    def test_feedback_interval_integer_and_float_have_one_identity(self):
        self.assertEqual(self.signature(dict(self.plan, async_limits=dict(feedback_interval_s=1))),
                         self.signature(dict(self.plan, async_limits=dict(feedback_interval_s=1.0))))

    def test_small_Core_result_with_explicit_IPC_minimum_remains_valid(self):
        self.plan.update(core_capacity=dict(item_result_bytes=1024),
                         async_limits=dict(item_result_bytes=4096))
        identity = self.identity()
        self.assertNotEqual(identity, self.fixture.identity)
        for maximum in (1, 4, 8):
            with self.subTest(maximum=maximum):
                plan = self.candidate(maximum)
                self.assertEqual(self.identity(plan), identity)
                resources = RecordsResources(**plan['resources'])
                self.assertEqual(movie_runtime(plan, resources=resources).async_limits.item_result_bytes, 4096)

    def test_derived_capacity_does_not_bind_one_scanned_candidate(self):
        self.plan['engine']['max_num_seqs'] = None
        self.plan.update(core_capacity=dict(item_input_bytes=4*1024**2),
                         async_limits=dict(item_result_bytes=65536))
        identity = self.identity()
        for maximum in (1, 2, 8):
            with self.subTest(maximum=maximum):
                plan = self.candidate(maximum)
                self.assertEqual(self.identity(plan), identity)
                resources = RecordsResources(**plan['resources'])
                limits = movie_runtime(plan, resources=resources).async_limits
                self.assertEqual(limits.max_inflight,resources.max_active_batches)
                self.assertEqual(limits.input_bytes,AsyncEngineLimits().input_bytes)
                self.assertEqual(limits.result_bytes,AsyncEngineLimits().result_bytes)

    def test_explicit_count_matching_current_C_is_not_a_derived_policy(self):
        self.plan['async_limits'] = dict(max_inflight=4)
        self.assertNotEqual(self.signature(), self.signature(self.fixture.plan))
        self.assertEqual(self.candidate(4)['async_limits'], dict(max_inflight=4))
        self.assertEqual(self.candidate(2)['async_limits'],dict(max_inflight=4))

    def test_explicit_result_slots_remain_independent_of_soft_candidates(self):
        self.plan['async_limits'] = dict(result_slots=4)
        self.candidate(2)
        self.assertEqual(self.candidate(8)['async_limits'],dict(result_slots=4))

    def test_explicit_totals_matching_current_C_remain_distinct(self):
        limits = asdict(resolve_records_async_limits(
            replace(RecordsResources(**self.plan['resources']), max_active_batches=4), CoreRecordsCapacity()))
        self.assertNotEqual(self.signature(dict(self.plan, async_limits=limits)), self.signature())
        self.plan['async_limits'] = dict(input_bytes=8*1024**2, result_bytes=8*1024**2)
        self.assertEqual(self.candidate(8)['async_limits'], self.plan['async_limits'])

    def test_single_candidate_capacity_defaults_can_fully_normalize(self):
        self.plan['resources']['max_active_batches'] = self.plan['profile']['max_inflight'] = 1
        configured = movie_runtime(self.plan)
        limits = asdict(configured.async_limits)
        self.assertEqual(self.signature(dict(self.plan, async_limits=limits)), self.signature())

    def test_new_default_identity_binds_actual_resources_and_allows_more_than_native_iteration(self):
        self.plan['engine']['max_num_seqs']=2
        identity=self.identity()
        policy=identity['execution']['records_capacity_policy_v2']
        configured=movie_runtime(self.plan)
        self.assertEqual(policy['core']['budget'],asdict(configured.capacity.budget))
        self.assertEqual(policy['IPC_overrides'],asdict(configured.async_limits))
        before=deepcopy(self.plan)
        self.assertEqual(self.candidate(8)['profile']['max_inflight'],8)
        self.assertEqual(self.plan,before)
        self.plan['profile']['admission']=dict(kind='legacy_resource_feedback')
        with self.assertRaises(ValueError):
            self.identity()
        with self.assertRaises(ValueError):
            self.candidate(8)


    def test_cache_hits_equivalent_defaults_and_misses_changed_capacity(self):
        path = self.fixture.save_selection(core.build_core_selection(**self.fixture.scan()))
        equivalent = dict(self.plan, core_capacity=asdict(CoreRecordsCapacity()), async_limits={})
        self.assertEqual(core.resolve_core_selection(path, self.identity(equivalent))['status'], 'ready')
        for changes in (dict(core_capacity=dict(item_input_bytes=4*1024**2)),
                dict(async_limits=dict(command_slots=1))):
            with self.subTest(changes=changes):
                result = core.resolve_core_selection(path, self.identity(dict(self.plan, **changes)))
                self.assertEqual((result['status'], result['reason'], result['performance_tuned']),
                    ('needs_scan', 'identity_or_policy_mismatch', False))

    def test_capacity_change_rejects_old_cache_identity_without_mutating_plan(self):
        original = deepcopy(self.plan)
        self.plan['core_capacity'] = dict(metadata_bytes=4096)
        before = deepcopy(self.plan)
        with self.assertRaisesRegex(ValueError, 'differs from'):
            core.core_candidate_plan(self.plan, self.fixture.checked,
                dict(max_inflight=4, cpu_workers=1), identity=self.identity(original))
        self.assertEqual(self.plan, before)

    def test_invalid_capacity_is_rejected_before_a_measurement_or_cache_hit(self):
        for changes in (dict(core_capacity=None), dict(async_limits=None),
                dict(core_capacity=dict(metadata_bytes=0)),
                dict(async_limits=dict(max_inflight=0)),
                dict(async_limits=dict(input_bytes=1)), dict(async_limits=dict(unknown=1))):
            with self.subTest(changes=changes), self.assertRaises((ValueError, TypeError)):
                self.identity(dict(self.plan, **changes))


if __name__ == '__main__':
    unittest.main()
