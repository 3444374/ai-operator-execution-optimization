"""Unspecified legacy request count uses complete grants; CPU fixtures only."""
from copy import deepcopy
from contextlib import redirect_stdout
from dataclasses import asdict, replace
import io
import json
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.movie_tasks import MovieTask
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.records_profile import resolve_records_runtime
from src.experiments import movie_records_config as config
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_protocol import GenerationSpec
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from tests.experiments import test_records_resource_budget as budget_fixture
from tests.experiments import test_records_budget_query_identity as identity_fixture
from tests.experiments.test_records_resource_budget import capacity, ipc, ITEM
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime, Source


def typed_ipc():
    return AsyncEngineLimits(max_inflight=5, result_slots=1, input_bytes=2*ITEM,
        item_input_bytes=ITEM, result_bytes=ITEM, item_result_bytes=ITEM,
        command_slots=3, control_slots=2)


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

    def test_complete_typed_IPC_resolves_None_in_ordinary_and_rolling_CPU_preflight(self):
        case, limits = self.fixture(), typed_ipc()
        shape = core.CoreRecordsCapacity(item_input_bytes=ITEM, item_result_bytes=ITEM)
        before = asdict(case.resources)
        ray = SimpleNamespace(init=Mock(side_effect=AssertionError('Ray must not initialize in preflight')))
        with patch.dict(sys.modules, {'ray': ray}), patch(
                'src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as actor:
            for rolling in (False, True):
                with self.subTest(rolling=rolling):
                    resolved = resolve_records_runtime(case.engine, case.resources,
                        dict(mode='async', max_inflight='auto'), capacity=shape,
                        async_limits=limits, rolling_refill=rolling)
                    self.assertIs(resolved.async_limits, limits)
                    self.assertEqual(resolved.capacity.budget.active_requests, 5)
                    self.assertEqual((resolved.capacity.budget.input_bytes,
                                      resolved.capacity.budget.result_bytes), (2*ITEM, ITEM))
                    self.assertIsNone(resolved.profile['declared_legacy_request_budget'])
                    self.assertEqual(resolved.profile['owner_max_inflight'], 5)
            actor.assert_not_called()
            ray.init.assert_not_called()
        self.assertEqual(asdict(case.resources), before)

    def test_complete_JSON_IPC_matches_typed_capacity_without_startup(self):
        case, limits = self.fixture(), typed_ipc()
        shape = core.CoreRecordsCapacity(item_input_bytes=ITEM, item_result_bytes=ITEM)
        ray = SimpleNamespace(init=Mock())
        with patch.dict(sys.modules, {'ray': ray}), patch(
                'src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as actor:
            for rolling in (False, True):
                for feedback in (False, True):
                    profile = (dict(mode='async', admission=dict(kind='resource_feedback'))
                               if feedback else dict(mode='async', max_inflight='auto'))
                    expected = resolve_records_runtime(case.engine, case.resources, profile,
                        capacity=shape, async_limits=limits, rolling_refill=rolling)
                    for include_feedback_interval in (False, True):
                        raw = asdict(limits)
                        if not include_feedback_interval:
                            raw.pop('feedback_interval_s')
                        with self.subTest(rolling=rolling, feedback=feedback,
                                          feedback_interval=include_feedback_interval):
                            actual = resolve_records_runtime(case.engine, case.resources, profile,
                                capacity=shape, async_limits=raw, rolling_refill=rolling)
                            self.assertEqual(actual, expected)
                            self.assertIsNone(case.resources.max_active_batches)
            actor.assert_not_called()
            ray.init.assert_not_called()

    def test_invalid_complete_JSON_IPC_rejects_before_owner_or_runtime(self):
        case = self.fixture()
        variants = [dict(asdict(typed_ipc()), **change) for change in
                    ({'max_inflight': True}, {'result_bytes': ITEM-1},
                     {'control_slots': 0}, {'feedback_interval_s': float('nan')})]
        ray = SimpleNamespace(init=Mock())
        with patch.dict(sys.modules, {'ray': ray}), patch(
                'src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as actor:
            for raw in variants:
                with self.subTest(limits=raw), self.assertRaises(ValueError):
                    case.owner(1, capacity=None, async_limits=raw)
            actor.assert_not_called()
            ray.init.assert_not_called()

    def test_public_factory_uses_the_typed_IPC_grant_without_rewriting_explicit_storage(self):
        case, limits = self.fixture(), typed_ipc()
        shape = core.CoreRecordsCapacity(item_input_bytes=ITEM, item_result_bytes=ITEM)
        for C in (1, 3, 'auto'):
            with self.subTest(C=C):
                owner = case.owner(C, capacity=shape, async_limits=limits)
                try:
                    self.assertEqual(owner.resources.max_active_batches, 5)
                    self.assertEqual(owner.execution_profile['max_inflight'], 5 if C == 'auto' else C)
                    self.assertIs(owner._actors.limits, limits)
                    self.assertEqual(owner.capacity.budget.held_tasks, 2)
                    self.assertEqual(owner.capacity.budget.method_runs, 2)
                    self.assertEqual(owner.capacity.budget.result_bytes, ITEM)
                    self.assertEqual(owner.engine, case.engine)
                    self.assertIsNone(owner.engine.max_num_seqs)
                    self.assertIsNone(owner.engine.max_num_batched_tokens)
                    self.assertIsNone(owner.engine.max_model_len)
                    self.assertIsNone(owner._actors.model_actor)
                finally:
                    owner.close(timeout=1)

    def test_rolling_factory_uses_the_same_typed_grant_with_or_without_token_packing(self):
        case, limits = self.fixture(), typed_ipc()
        shape = core.CoreRecordsCapacity(item_input_bytes=ITEM, item_result_bytes=ITEM)
        for batching in (None, PreparedBatching('token_budget', None, 10000)):
            for feedback in (False, True):
                profile = (dict(mode='async', admission=dict(kind='resource_feedback'))
                           if feedback else dict(mode='async', max_inflight='auto'))
                with self.subTest(batching=batching, feedback=feedback):
                    owner = case.owner(1, capacity=shape, async_limits=limits, profile=profile,
                        prepared_batching=batching, rolling_refill=True,
                        interleaved_preparation=True, event_driven_preparation=True)
                    chain = owner._new_owner(None)
                    try:
                        self.assertEqual((chain.resources.max_active_batches, chain.request_window),
                                         (5, 1 if feedback else 5))
                        self.assertIs(chain._actors.limits, limits)
                        self.assertEqual(chain.capacity, owner.capacity)
                        self.assertIsNone(owner.execution_profile['declared_legacy_request_budget'])
                        self.assertIsNone(chain._actors.model_actor)
                    finally:
                        chain.close(timeout=1)
                        owner.close(timeout=1)

    def test_missing_or_partial_dictionary_IPC_is_not_a_finite_None_grant(self):
        case = self.fixture()
        partial = asdict(typed_ipc())
        partial.pop('control_frame_bytes')
        ray = SimpleNamespace(init=Mock(side_effect=AssertionError('Ray must not initialize on rejection')))
        with patch.dict(sys.modules, {'ray': ray}), patch(
                'src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as actor:
            for limits in (None, {}, dict(max_inflight=5), partial):
                for rolling in (False, True):
                    with self.subTest(limits=limits, rolling=rolling), self.assertRaisesRegex(
                            ValueError, 'request capacity.*unresolved'):
                        case.owner(1, capacity=None, async_limits=limits, rolling_refill=rolling)
            actor.assert_not_called()
            ray.init.assert_not_called()

    def test_explicit_R_and_complete_budget_take_precedence_over_the_IPC_request_count(self):
        case, limits = self.fixture(), typed_ipc()
        case.resources = replace(case.resources, max_active_batches=3)
        for declared in (None, capacity()):
            with self.subTest(capacity=declared):
                owner = case.owner(1, capacity=declared, async_limits=limits)
                try:
                    self.assertEqual(owner.resources.max_active_batches, 3 if declared is None else 4)
                    self.assertIs(owner._actors.limits, limits)
                    self.assertEqual(owner.capacity.budget.active_requests, 3 if declared is None else 4)
                    if declared is not None:
                        self.assertEqual(owner.capacity, declared)
                finally:
                    owner.close(timeout=1)

    def test_complete_IPC_does_not_allow_sync_or_a_window_above_its_request_grant(self):
        case, limits = self.fixture(), typed_ipc()
        ray = SimpleNamespace(init=Mock(side_effect=AssertionError('Ray must not initialize on rejection')))
        with patch.dict(sys.modules, {'ray': ray}), patch(
                'src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as actor:
            for grant in (limits, asdict(limits)):
                for profile in (None, dict(mode='sync', max_inflight=1), dict(mode='async', max_inflight=6)):
                    with self.subTest(profile=profile, grant=grant), self.assertRaises(ValueError):
                        case.owner(1, profile=profile, capacity=None, async_limits=grant)
            with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                case.owner(1, capacity=None, async_limits=limits,
                           prepared_batching=PreparedBatching('token_budget', None, 10000))
            actor.assert_not_called()
            ray.init.assert_not_called()

    def test_typed_IPC_None_query_reuse_keeps_one_result_slot_and_ACK_cleanup(self):
        rows = [dict(row_id='typed-'+str(i), source_position=i, reviewId='repeat', id='taken_3',
                     reviewText='fast') for i in range(5)]
        case, limits = self.fixture(), typed_ipc()
        shape = core.CoreRecordsCapacity(item_input_bytes=ITEM, item_result_bytes=ITEM)
        with ThreadedRuntime(rows) as runtime:
            owner = case.owner('auto', capacity=shape, async_limits=limits)
            try:
                engine_uids = set()
                for index in range(2):
                    stream = owner.evaluate(Source(rows), query_id='typed-'+str(index))
                    values = list(stream)
                    self.assertEqual(len(values), len(rows))
                    self.assertEqual(len({v['call_id'] for v in values}), len(rows))
                    engine_uids.update(v['engine_uid'] for v in values)
                    self.assertLessEqual(stream.peak_usage['active_requests'], owner.capacity.budget.active_requests)
                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                    self.assertEqual(owner.transport.retained_refs(), 0)
                    self.assertEqual(runtime.async_actors[0].instance.engine.snapshot()['held_requests'], 0)
                self.assertEqual(len(engine_uids), 1)
                self.assertEqual(len(runtime.async_actors), 1)
            finally:
                self.assertEqual(owner.close(timeout=3)['status'], 'closed')

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

    def test_omitted_R_and_None_share_the_complete_IPC_identity(self):
        plan = deepcopy(self.plan)
        plan.update(profile=dict(mode='async', max_inflight='auto'),
            core_capacity=dict(item_input_bytes=ITEM, item_result_bytes=ITEM),
            async_limits=asdict(typed_ipc()))
        explicit = self.fixture.identity(plan)
        plan['resources'].pop('max_active_batches')
        self.assertEqual(self.fixture.identity(plan), explicit)

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

    def test_omitted_R_with_complete_JSON_passes_Movie_and_both_session_checks(self):
        self.plan['resources'].pop('max_active_batches')
        self.plan.update(profile=dict(mode='async', max_inflight='auto'),
            core_capacity=dict(item_input_bytes=ITEM, item_result_bytes=ITEM),
            async_limits=asdict(typed_ipc()))
        with patch.object(core, 'create_records_core') as allocate:
            for lifecycle in ('isolated', 'resident'):
                with self.subTest(lifecycle=lifecycle):
                    checked = session.check_plan(dict(self.plan, lifecycle=lifecycle))
                    self.assertEqual(checked['core_execution']['owner_max_inflight'], 5)
                    self.assertIsNone(checked['core_execution']['declared_legacy_request_budget'])
                    self.assertEqual(checked['async_limits']['result_slots'], 1)
            value = dict(schema='semloom.movie_records_run.v1', backend=config.ASYNC_CORE_BACKEND,
                query_id='q1', task='movie-q1',
                method=asdict(MovieTask(1).method(self.plan['model_name'])),
                engine=self.plan['engine'], generation=self.plan['generation'],
                resources=self.plan['resources'], core_profile=self.plan['profile'],
                core_capacity=self.plan['core_capacity'], async_limits=self.plan['async_limits'])
            path = self.root/'complete-ipc-config.json'
            path.write_text(json.dumps(value))
            output = io.StringIO()
            with patch.object(movie, 'model_preflight', return_value=dict(inference_calls=0)), \
                    redirect_stdout(output):
                self.assertEqual(movie.main(['check', '--config', str(path),
                    '--sembench-checkout', str(self.root),
                    '--source-manifest', self.plan['queries'][0]['manifest']]), 0)
            checked = json.loads(output.getvalue())
            self.assertEqual(checked['core_execution']['owner_max_inflight'], 5)
            self.assertEqual(checked['resolved_async_limits_before_feedback'], asdict(typed_ipc()))
            for batching in (None, dict(policy='token_budget', max_rows=None, token_budget=10000)):
                with self.subTest(rolling_batching=batching):
                    checked = session.check_plan(dict(self.plan, rolling_refill=True,
                        prepared_batching=batching, lifecycle='resident'))
                    self.assertEqual(checked['record_batching']['R'], 5)
            allocate.assert_not_called()

    def test_omitted_R_reuses_complete_Core_or_service_grants(self):
        for service in (False, True):
            with self.subTest(service=service):
                plan = deepcopy(self.plan)
                plan['resources'].pop('max_active_batches')
                plan['profile'] = dict(mode='async', max_inflight='auto')
                if service:
                    plan.pop('async_limits')
                    plan['core_capacity'] = dict(item_input_bytes=8192, item_result_bytes=8192)
                    plan['service_payload_bytes'] = 8*1024**2
                checked = session.check_plan(plan)
                execution = checked['core_execution']
                self.assertIsNone(execution['declared_legacy_request_budget'])
                if service:
                    self.assertGreater(execution['owner_max_inflight'], 2)
                    self.assertFalse(execution['service_allocation_acquired'])
                    with self.assertRaisesRegex(ValueError, 'delivery declarations'):
                        session.check_plan(dict(plan, rolling_refill=True))
                else:
                    self.assertEqual(execution['owner_max_inflight'], 4)

    def test_omitted_R_without_a_grant_rejects_actual_entries_before_startup(self):
        plan = deepcopy(self.plan)
        plan['resources'].pop('max_active_batches')
        plan.pop('core_capacity')
        plan.pop('async_limits')
        ray = SimpleNamespace(init=Mock(), is_initialized=Mock())
        with patch.dict(sys.modules, {'ray': ray}), patch.object(core, 'create_records_core') as allocate:
            with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                session.check_plan(plan)
            directory = self.root/'unstarted-driver'
            driver = session._MovieDriver(plan, {}, directory, plan['queries'][0])
            with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                driver.start()
            self.assertIsNone(driver.owner)
            self.assertFalse(directory.exists())
            value = dict(schema='semloom.movie_records_run.v1', backend=config.ASYNC_CORE_BACKEND,
                query_id='q1', task='movie-q1',
                method=asdict(MovieTask(1).method(plan['model_name'])),
                engine=plan['engine'], generation=plan['generation'],
                resources=plan['resources'], core_profile=plan['profile'])
            path = self.root/'missing-ipc-config.json'
            path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                movie.read_config(path)
            allocate.assert_not_called()
            ray.is_initialized.assert_not_called()
            ray.init.assert_not_called()

    def test_explicit_R_and_other_entry_defaults_keep_their_existing_meaning(self):
        plan = deepcopy(self.plan)
        plan['resources']['max_active_batches'] = 2
        plan['profile'] = dict(mode='async', max_inflight='auto')
        self.assertEqual(session.check_plan(plan)['core_execution']['owner_max_inflight'], 4)
        plan.update(core_capacity=dict(item_input_bytes=ITEM, item_result_bytes=ITEM),
                    async_limits=asdict(typed_ipc()))
        checked = session.check_plan(plan)
        self.assertEqual(checked['core_execution']['owner_max_inflight'], 2)
        self.assertEqual(checked['core_execution']['declared_legacy_request_budget'], 2)
        plan['resources'].pop('max_active_batches')
        plan.pop('core_capacity')
        plan.pop('async_limits')
        for backend, profile in (('semloom-static-reference', dict(mode='sync')),
                                 ('lotus-native', dict(native_lm_max_batch_size=3))):
            with self.subTest(backend=backend):
                candidate = dict(plan, backend=backend, profile=profile)
                session.check_plan(candidate)
                self.assertEqual(config.records_resources(candidate).max_active_batches, 2)
        plan.update(lifecycle='isolated', profile=dict(mode='async', max_inflight=2),
                    prepared_batching=dict(policy='token_budget', max_rows=None, token_budget=10000))
        checked = session.check_plan(plan)
        self.assertEqual(checked['record_batching']['R'], 2)


if __name__ == '__main__':
    unittest.main()
