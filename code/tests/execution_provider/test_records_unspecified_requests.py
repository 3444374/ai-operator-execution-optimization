"""Unspecified legacy request count uses complete grants; CPU fixtures only."""
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.movie_tasks import MovieTask
from src.execution_provider.adapters import lotus_records_core as core
from src.experiments import movie_records_config as config
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_protocol import GenerationSpec
from tests.experiments import test_records_resource_budget as budget_fixture
from tests.experiments import test_records_budget_query_identity as identity_fixture
from tests.experiments.test_records_resource_budget import capacity, ipc
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime, Source


class UnspecifiedRequestTests(unittest.TestCase):
    def fixture(self):
        case = budget_fixture.ResourceBudgetTests()
        case.setUp()
        case.resources = replace(case.resources, max_active_batches=None)
        return case

    def test_None_round_trips_without_turning_into_an_integer_or_infinity(self):
        value = RecordsResources(max_active_batches=None)
        self.assertIsNone(value.describe()['max_active_batches'])
        self.assertEqual(RecordsResources(**asdict(value)), value)
        self.assertEqual(RecordsResources().max_active_batches, 2)
        for bad in (False, True, 0, -1, 'auto', float('inf'), 2.5):
            with self.subTest(value=bad), self.assertRaises(ValueError):
                RecordsResources(max_active_batches=bad)

    def test_valid_profile_namespace_does_not_require_new_resource_methods(self):
        case = self.fixture()
        resources = replace(case.resources, max_active_batches=7)
        profile = dict(mode='async', max_inflight=3)
        typed = core.resolve_core_profile(case.engine, resources, profile)
        plain = core.resolve_core_profile(case.engine, SimpleNamespace(**asdict(resources)), profile)
        self.assertEqual(plain, typed)
        with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
            core.resolve_core_profile(case.engine, SimpleNamespace(max_active_batches=None), profile)

    def test_ordinary_factory_uses_independent_grants_for_every_initial_window(self):
        case = self.fixture()
        before = asdict(case.resources)
        for C in (1, 2, 4):
            with self.subTest(C=C):
                owner = case.owner(C, async_limits=None)
                self.assertEqual(owner.resources.max_active_batches, capacity().budget.active_requests)
                self.assertIsNone(owner.execution_profile['declared_legacy_request_budget'])
                self.assertEqual(owner.execution_profile['max_inflight'], C)
                self.assertEqual(owner._actors.limits.max_inflight, 4)
                self.assertEqual(owner._actors.limits.result_slots, 9)
                self.assertEqual(owner.capacity, capacity())
                self.assertIsNone(owner._actors.model_actor)
        self.assertEqual(asdict(case.resources), before)

    def test_explicit_IPC_values_still_apply_independently(self):
        case = self.fixture()
        owner = case.owner(3)
        self.assertEqual({name: getattr(owner._actors.limits, name) for name in ipc()}, ipc())
        self.assertEqual(owner.resources.max_active_batches, 4)
        self.assertEqual(owner.capacity.budget.method_runs, 7)

    def test_rolling_factory_carries_None_intent_and_finite_owner_allocations(self):
        owner = self.fixture().owner(2,
            prepared_batching=PreparedBatching('token_budget', None, 10000), rolling_refill=True)
        chain = owner._new_owner(None)
        self.assertIsNone(owner.execution_profile['declared_legacy_request_budget'])
        self.assertEqual((chain.resources.max_active_batches, chain.request_window), (4, 2))
        self.assertEqual(chain.capacity.budget, capacity().budget)
        self.assertEqual(chain._actors.limits.max_inflight, 2)

    def test_modern_feedback_starts_at_one_with_the_complete_owner_grant(self):
        owner = self.fixture().owner(1,
            profile=dict(mode='async', admission=dict(kind='resource_feedback')))
        self.assertEqual(owner.execution_profile['max_inflight'], 1)
        self.assertEqual(owner.execution_profile['owner_max_inflight'], 4)
        self.assertIsNone(owner.execution_profile['declared_legacy_request_budget'])
        self.assertIsNone(owner._actors.model_actor)

    def test_missing_grant_rejects_before_any_model_or_actor_allocation(self):
        case = self.fixture()
        for profile in (None, dict(mode='sync'), dict(mode='async'),
                        dict(mode='async', max_inflight=1), dict(mode='async', max_inflight='auto'),
                        dict(mode='async', admission=dict(kind='resource_feedback'))):
            with self.subTest(profile=profile), patch(
                    'src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as actor:
                with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                    case.owner(1, profile=profile, capacity=None, async_limits=None)
                actor.assert_not_called()
        value = core.CoreRecordsCapacity()
        with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
            value.limits(case.resources)
        with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
            core.resolve_records_async_limits(case.resources, value)

    def test_low_level_actor_rejects_unresolved_count_before_startup(self):
        case = self.fixture()
        with patch.object(core._ActorOwner, 'start') as start:
            with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                core._ActorOwner(MovieTask(1).method('cpu-fixture'), case.engine, GenerationSpec(), case.resources)
            start.assert_not_called()

    def test_complete_budget_still_rejects_a_window_larger_than_its_request_grant(self):
        case = self.fixture()
        with patch('src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as actor:
            with self.assertRaisesRegex(ValueError, 'in-flight limit'):
                case.owner(5)
            actor.assert_not_called()

    def test_real_CPU_IPC_query_reuse_keeps_results_ACK_and_counting(self):
        rows = [dict(row_id='r'+str(i), source_position=i, reviewId='repeat', id='taken_3',
                     reviewText='fast') for i in range(5)]
        with ThreadedRuntime(rows):
            owner = self.fixture().owner(3)
            try:
                for index, maximum in enumerate((1, 3, 1)):
                    stream = owner.evaluate(Source(rows), query_id='q'+str(index),
                        max_inflight=maximum)
                    values = list(stream)
                    self.assertEqual(len(values), len(rows))
                    self.assertEqual(len({v['call_id'] for v in values}), len(rows))
                    self.assertLessEqual(stream.peak_usage['active_requests'], maximum)
                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                    self.assertEqual(owner.transport.retained_refs(), 0)
                    self.assertEqual(owner.core.capacity.limits, capacity().limits(owner.resources))
            finally:
                self.assertEqual(owner.close(timeout=3)['status'], 'closed')


class UnspecifiedConfigIdentityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = identity_fixture.CanonicalOrganizationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.plan = deepcopy(self.fixture.plan)
        self.plan['resources']['max_active_batches'] = None

    def test_config_and_offline_identity_use_the_same_complete_budget(self):
        checked = config.records_runtime(self.plan)
        self.assertIsNone(checked.profile['declared_legacy_request_budget'])
        self.assertEqual(checked.profile['owner_max_inflight'], 4)
        identity = self.fixture.identity(self.plan)
        changed = deepcopy(self.plan)
        changed['core_capacity']['budget']['active_requests'] = 5
        self.assertNotEqual(identity, self.fixture.identity(changed))
        self.assertIsNone(self.plan['resources']['max_active_batches'])


class UnspecifiedEntryTests(budget_fixture.ResourceBudgetEntryTests):
    """Exercise ordinary CLI, resident/isolated session and rolling checks."""
    def setUp(self):
        super().setUp()
        self.plan['resources']['max_active_batches'] = None


if __name__ == '__main__':
    unittest.main()
