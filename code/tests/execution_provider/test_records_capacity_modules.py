"""Public import and independent capacity roles; CPU declarations only."""
from dataclasses import asdict, replace
import pickle
import unittest

from src.execution_provider.adapters import lotus_records_core as public
from src.execution_provider.adapters import records_capacity as capacity
from src.execution_provider.adapters import records_profile as profile
from src.semantic_methods.lotus.records import RecordsResources
from tests.experiments.test_records_resource_budget import capacity as declared, ipc


class CapacityModuleTests(unittest.TestCase):
    def test_original_imports_refer_to_the_same_types_and_resolvers(self):
        for module, names in ((capacity, ('RecordsCapacityBudget', 'CoreRecordsCapacity',
                'resolve_records_async_limits')), (profile, ('expand_core_profile',
                'resolve_core_profile', 'resolve_record_batching'))):
            for name in names:
                with self.subTest(name=name):
                    self.assertIs(getattr(public, name), getattr(module, name))

    def test_serialized_capacity_keeps_the_original_public_location(self):
        old = b'csrc.execution_provider.adapters.lotus_records_core\nCoreRecordsCapacity\n.'
        self.assertIs(pickle.loads(old), capacity.CoreRecordsCapacity)
        for value in (declared(), declared().budget):
            payload = pickle.dumps(value, protocol=0)
            self.assertIn(b'src.execution_provider.adapters.lotus_records_core', payload)
            self.assertNotIn(b'\nrecords_capacity\n', payload)
            self.assertEqual(pickle.loads(payload), value)
            self.assertIs(type(pickle.loads(payload)), type(value))

    def test_explicit_budget_fields_only_resize_their_own_resource_owner(self):
        resources = RecordsResources(max_active_batches=4)
        value = declared()
        method = value.method_limits(1)

        def roles(current):
            return (asdict(current.limits(resources)),
                    asdict(current.method_capacity(resources, method)),
                    asdict(capacity.resolve_records_async_limits(resources, current, ipc())))

        original = roles(value)
        for name in asdict(value.budget):
            changed = replace(value, budget=replace(value.budget, **{name:getattr(value.budget, name)+1}))
            expected = (name not in ('method_runs', 'method_bytes'),
                        name in ('method_runs', 'method_bytes'), False)
            with self.subTest(field=name):
                self.assertEqual(tuple(a != b for a, b in zip(original, roles(changed))), expected)

    def test_legacy_R_does_not_resize_any_explicit_capacity_role(self):
        value = declared()
        method = value.method_limits(1)
        outputs = []
        for R in (1, 4, 29):
            resources = RecordsResources(max_active_batches=R)
            outputs.append((value.limits(resources), value.method_capacity(resources, method),
                capacity.resolve_records_async_limits(resources, value, ipc())))
        self.assertEqual(outputs, [outputs[0]]*3)


if __name__ == '__main__':
    unittest.main()
