"""Public import and independent capacity roles; CPU declarations only."""
from dataclasses import asdict, replace
import pickle
import unittest

from src.execution_provider.adapters import lotus_records_core as public
from src.execution_provider.adapters import lotus_record_chain_core as chain
from src.execution_provider.adapters import records_capacity as capacity
from src.execution_provider.adapters import records_profile as profile
from src.semantic_methods.lotus.records import RecordsResources
from tests.experiments.test_records_resource_budget import capacity as declared, ipc


class CapacityModuleTests(unittest.TestCase):
    def test_original_imports_refer_to_the_same_types_and_resolvers(self):
        self.assertIs(chain.CoreChainCapacity, capacity.CoreChainCapacity)
        for module, names in ((capacity, ('RecordsCapacityBudget', 'CoreRecordsCapacity',
                'resolve_records_async_limits')), (profile, ('resolve_core_profile', 'resolve_record_batching'))):
            for name in names:
                with self.subTest(name=name):
                    self.assertIs(getattr(public, name), getattr(module, name))

    def test_capacity_pickle_round_trip_preserves_types_and_values(self):
        old = b'csrc.execution_provider.adapters.lotus_records_core\nCoreRecordsCapacity\n.'
        self.assertIs(pickle.loads(old), capacity.CoreRecordsCapacity)
        old_chain = b'csrc.execution_provider.adapters.lotus_record_chain_core\nCoreChainCapacity\n.'
        self.assertIs(pickle.loads(old_chain), capacity.CoreChainCapacity)
        for value in (declared(), declared().budget, capacity.CoreChainCapacity()):
            payload = pickle.dumps(value, protocol=0)
            self.assertEqual(pickle.loads(payload), value)
            self.assertIs(type(pickle.loads(payload)), type(value))

    def test_fused_capacity_keeps_single_task_and_complete_stage_work(self):
        value = capacity.CoreChainCapacity(item_input_bytes=128, item_result_bytes=256)
        limits = value.limits(RecordsResources(model_batch_rows=4, max_active_batches=1), 3)
        self.assertEqual((limits.held_tasks, limits.active_requests, limits.active_work), (1, 1, 12))
        self.assertEqual((limits.input_bytes, limits.result_bytes), (128, 256))
        for kind in (capacity.CoreChainCapacity, capacity.CoreRecordsCapacity):
            for options in (dict(item_input_bytes=False), dict(metadata_bytes=0),
                            dict(poll_interval_s=float('nan'))):
                with self.subTest(kind=kind.__name__, options=options), self.assertRaises(ValueError):
                    kind(**options)

    def test_explicit_budget_fields_only_affect_their_own_resource_owner(self):
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
                        name == 'method_bytes', False)
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
