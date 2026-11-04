"""Independent declared budgets provide finite IPC defaults, never hardware facts."""
from copy import deepcopy
from dataclasses import asdict, replace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import records_capacity as capacity
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from tests.experiments import test_records_budget_query_identity as identity_fixture
from tests.experiments import test_records_resource_budget as budget_fixture
from tests.experiments.test_records_resource_budget import capacity as declared, ipc, ITEM
from tests.execution_provider import test_lotus_resource_budget_runtime as runtime


class IndependentIPCDefaultTests(unittest.TestCase):
    def test_old_R_and_query_C_do_not_resize_derived_IPC_grants(self):
        value = declared()
        results = [capacity.resolve_records_async_limits(RecordsResources(max_active_batches=R), value)
                   for R in (1, 4, 29)]
        self.assertEqual(results, [results[0]]*3)
        limits = results[0]
        self.assertEqual((limits.max_inflight, limits.command_slots), (4, 4))
        self.assertEqual(limits.result_slots, value.budget.held_tasks)
        self.assertEqual((limits.input_bytes, limits.result_bytes),
                         (value.budget.input_bytes, value.budget.result_bytes))
        case = budget_fixture.ResourceBudgetTests()
        case.setUp()
        for C in (1, 2, 4):
            owner = case.owner(C, async_limits=None)
            self.assertEqual(owner._actors.limits, limits)
            self.assertIsNone(owner._actors.model_actor)

    def test_partial_IPC_values_override_only_their_own_fields(self):
        value = declared()
        resources = RecordsResources(max_active_batches=29)
        original = capacity.resolve_records_async_limits(resources, value)
        options = dict(result_slots=1, feedback_interval_s=.1)
        actual = capacity.resolve_records_async_limits(resources, value, options)
        self.assertEqual(actual, replace(original, **options))
        self.assertEqual(options, dict(result_slots=1, feedback_interval_s=.1))
        self.assertEqual(actual.result_bytes, 2*ITEM)

    def test_complete_explicit_limits_and_typed_object_keep_their_values(self):
        value = declared()
        resources = RecordsResources(max_active_batches=29)
        options = ipc()
        explicit = AsyncEngineLimits(**dict(item_input_bytes=ITEM, item_result_bytes=ITEM, **options))
        self.assertEqual(capacity.resolve_records_async_limits(resources, value, options), explicit)
        self.assertIs(capacity.resolve_records_async_limits(resources, value, explicit), explicit)

    def test_unknown_or_too_small_explicit_bytes_reject_before_actor_allocation(self):
        case = budget_fixture.ResourceBudgetTests()
        case.setUp()
        for options in (dict(input_bytes=None), dict(result_bytes=ITEM-1),
                        dict(item_input_bytes=4*ITEM), dict(result_slots=True)):
            with self.subTest(options=options), patch(
                    'src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as allocate:
                with self.assertRaises((ValueError, TypeError)):
                    case.owner(2, async_limits=options)
                allocate.assert_not_called()

    def test_omitted_and_equivalent_explicit_IPC_have_the_same_cache_identity(self):
        case = identity_fixture.CanonicalOrganizationTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        plan = deepcopy(case.plan)
        plan.pop('async_limits')
        explicit = deepcopy(plan)
        explicit['async_limits'] = asdict(capacity.resolve_records_async_limits(
            RecordsResources(**plan['resources']), declared()))
        self.assertEqual(case.identity(plan), case.identity(explicit))
        changed = deepcopy(explicit)
        changed['async_limits']['result_bytes'] += ITEM
        self.assertNotEqual(case.identity(plan), case.identity(changed))
        self.assertNotIn('async_limits', plan)


class DerivedIPCRuntimeTests(runtime.ResourceBudgetRuntimeTests):
    """Reuse full ownership/unknown/cancel scenarios with omitted IPC allocations."""
    def owner(self, plan, *, C=2, result_slots=1):
        resolve = capacity.resolve_records_async_limits
        with patch('src.execution_provider.adapters.lotus_records_core.resolve_records_async_limits',
                   side_effect=lambda resources, value, overrides: resolve(
                       resources, value, dict(result_slots=result_slots))):
            return super().owner(plan, C=C, result_slots=result_slots)


if __name__ == '__main__':
    unittest.main()
