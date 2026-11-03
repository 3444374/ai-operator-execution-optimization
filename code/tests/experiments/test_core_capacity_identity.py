"""Capacity-policy cache isolation; CPU configuration fixtures, no model/Ray."""
from copy import deepcopy
from dataclasses import asdict, replace
import unittest

from src.execution_provider.adapters.lotus_records_core import (
    CoreRecordsCapacity, resolve_core_profile, resolve_records_async_limits,
)
from src.experiments.calibration import core
from src.experiments.lotus_records_movie import _async_limits, _record_admission_cache
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
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
        values = dict(max_inflight=4, input_bytes=16*1024**2, item_input_bytes=1024**2,
            result_slots=5, result_bytes=16*1024**2, item_result_bytes=65536,
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

    def test_explicit_total_derived_from_fixed_result_slots_is_redundant(self):
        self.plan['async_limits'] = dict(result_slots=5, item_result_bytes=65536)
        explicit = deepcopy(self.plan)
        explicit['async_limits']['result_bytes'] = 5*65536
        self.assertEqual(self.signature(explicit), self.signature())

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
                resolved = resolve_core_profile(EngineSpec.from_dict(plan['engine']), resources, plan['profile'])
                self.assertEqual(_async_limits(plan, resources=resources, resolved=resolved).item_result_bytes, 4096)

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
                resolved = resolve_core_profile(EngineSpec.from_dict(plan['engine']), resources, plan['profile'])
                limits = _async_limits(plan, resources=resources, resolved=resolved)
                self.assertEqual(limits.max_inflight, maximum)
                self.assertEqual(limits.input_bytes, maximum*4*1024**2)
                self.assertEqual(limits.result_bytes, maximum*65536)

    def test_explicit_count_matching_current_C_is_not_a_derived_policy(self):
        self.plan['async_limits'] = dict(max_inflight=4)
        self.assertNotEqual(self.signature(), self.signature(self.fixture.plan))
        self.assertEqual(self.candidate(4)['async_limits'], dict(max_inflight=4))
        with self.assertRaisesRegex(ValueError, 'owner request limit'):
            self.candidate(2)
        with self.assertRaisesRegex(ValueError, 'owner request limit'):
            core.build_core_selection(self.identity(), [dict(max_inflight=2, cpu_workers=1)], [], scan_wall_s=0)

    def test_explicit_result_slots_remain_a_candidate_constraint(self):
        self.plan['async_limits'] = dict(result_slots=4)
        self.candidate(2)
        with self.assertRaisesRegex(ValueError, 'result slot'):
            self.candidate(8)

    def test_explicit_totals_matching_current_C_remain_distinct(self):
        limits = asdict(resolve_records_async_limits(
            replace(RecordsResources(**self.plan['resources']), max_active_batches=4), CoreRecordsCapacity()))
        self.assertNotEqual(self.signature(dict(self.plan, async_limits=limits)), self.signature())
        self.plan['async_limits'] = dict(input_bytes=8*1024**2, result_bytes=8*1024**2)
        self.assertEqual(self.candidate(8)['async_limits'], self.plan['async_limits'])

    def test_single_candidate_capacity_defaults_can_fully_normalize(self):
        self.plan['resources']['max_active_batches'] = self.plan['profile']['max_inflight'] = 1
        resources = RecordsResources(**self.plan['resources'])
        limits = asdict(resolve_records_async_limits(resources, CoreRecordsCapacity()))
        self.assertEqual(self.signature(dict(self.plan, async_limits=limits)), self.signature())

    def test_fixed_owner_feedback_defaults_do_not_bind_initial_C(self):
        self.plan['profile']['admission'] = dict(kind='resource_feedback')
        resources = RecordsResources(**self.plan['resources'])
        explicit = dict(self.plan, async_limits=asdict(resolve_records_async_limits(resources, CoreRecordsCapacity())))
        self.assertEqual(self.signature(explicit), self.signature())
        explicit = deepcopy(explicit)
        explicit['profile']['max_inflight'] = 2
        self.assertEqual(self.signature(explicit), self.signature())

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
                dict(async_limits=dict(max_inflight=3)),
                dict(async_limits=dict(input_bytes=1)), dict(async_limits=dict(unknown=1))):
            with self.subTest(changes=changes), self.assertRaises((ValueError, TypeError)):
                self.identity(dict(self.plan, **changes))

    def online_cache(self, plan):
        resources = RecordsResources(**plan['resources'])
        resolved = resolve_core_profile(EngineSpec.from_dict(plan['engine']), resources, plan['profile'])
        return _record_admission_cache(plan, resources=resources, resolved=resolved)

    def test_online_admission_omitted_and_full_equivalent_defaults_share_identity(self):
        self.plan['profile']['admission'] = dict(kind='resource_feedback')
        self.plan['admission_cache'] = dict(identity=dict(execution=dict(fixture=True)))
        cache = self.online_cache(self.plan)
        resources = RecordsResources(**self.plan['resources'])
        limits = asdict(resolve_records_async_limits(resources, CoreRecordsCapacity()))
        for changes in (dict(core_capacity={}, async_limits={}),
                dict(core_capacity=asdict(CoreRecordsCapacity()), async_limits=limits)):
            with self.subTest(changes=changes):
                self.assertEqual(self.online_cache(dict(self.plan, **changes)), cache)

    def test_online_custom_capacity_is_canonical_and_does_not_mutate_input(self):
        self.plan.update(core_capacity=dict(poll_interval_s=1), async_limits=dict(feedback_interval_s=1),
            admission_cache=dict(identity=dict(execution=dict(fixture=True))))
        original = deepcopy(self.plan)
        explicit = deepcopy(self.plan)
        explicit['core_capacity']['poll_interval_s'] = 1.0
        explicit['async_limits']['feedback_interval_s'] = 1.0
        left, right = self.identity(), self.identity(explicit)
        left['execution'] = self.online_cache(self.plan)['identity']['execution']
        right['execution'] = self.online_cache(explicit)['identity']['execution']
        self.assertEqual(core.core_identity_signature(left), core.core_identity_signature(right))
        self.assertEqual(self.plan, original)
        explicit['async_limits']['feedback_interval_s'] = 2
        self.assertNotEqual(self.online_cache(self.plan), self.online_cache(explicit))


if __name__ == '__main__':
    unittest.main()
