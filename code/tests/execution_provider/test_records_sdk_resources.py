"""An unspecified SDK request count is resolved or rejected by its real consumer."""
import builtins
from dataclasses import asdict, replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from integrations.imlane_records.cascade_host import IMLaneCascadeBackend
from integrations.imlane_records.chain_backend import IMLaneChainBackend
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.lotus_cascade_core import CoreCascadeBackend
from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
from src.experiments import movie_records_worker as worker
from src.experiments import lotus_records_movie as movie
from src.experiments import record_chain_validation_run as validation
from src.experiments.movie_matrix_support import validate_core_scan
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_protocol import GenerationSpec
from tests.experiments import test_records_resource_budget as budget_fixture
from tests.execution_provider import test_records_unspecified_requests as fixtures


class SDKResourcesTests(unittest.TestCase):
    def setUp(self):
        self.case = budget_fixture.ResourceBudgetTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def test_partial_JSON_can_declare_requests_without_repeating_default_fields(self):
        self.case.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1)
        item = budget_fixture.ITEM
        shape = core.CoreRecordsCapacity(item_input_bytes=item, item_result_bytes=item)
        declaration = dict(max_inflight=5, result_slots=3,
                           input_bytes=2*item, result_bytes=2*item)
        for capacity, expected_requests in ((shape, 5), (budget_fixture.capacity(), 4)):
            with self.subTest(expected_requests=expected_requests):
                owner = self.case.owner(1, capacity=capacity, async_limits=declaration)
                try:
                    self.assertEqual(owner.resources.request_capacity(), expected_requests)
                    self.assertEqual(owner._actors.limits.max_inflight, 5)
                    self.assertEqual(owner._actors.limits.result_slots, 3)
                    self.assertEqual(owner._actors.limits.command_slots, 5)
                    self.assertEqual((owner._actors.limits.input_bytes, owner._actors.limits.result_bytes),
                                     (2*item, 2*item))
                    self.assertIsNone(owner._actors.model_actor)
                finally:
                    owner.close(timeout=1)
        self.assertEqual(declaration, dict(max_inflight=5, result_slots=3,
                                          input_bytes=2*item, result_bytes=2*item))
        owner = self.case.owner(1, capacity=shape, async_limits=dict(max_inflight=5))
        try:
            self.assertEqual(owner.resources.request_capacity(), 5)
            self.assertEqual((owner._actors.limits.result_slots, owner._actors.limits.command_slots), (5, 5))
            self.assertEqual((owner._actors.limits.input_bytes, owner._actors.limits.result_bytes),
                             (5*item, 5*item))
            self.assertIsNone(owner._actors.model_actor)
        finally:
            owner.close(timeout=1)
        with patch('src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as actor:
            for invalid in (None, True, 0, -1, '5'):
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    self.case.owner(1, capacity=shape, async_limits=dict(max_inflight=invalid))
            actor.assert_not_called()

    def test_default_None_uses_complete_Core_or_typed_IPC_without_an_actor(self):
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1)
        self.assertIsNone(resources.max_active_batches)
        self.assertIsNone(resources.describe()['max_active_batches'])
        self.case.resources = resources
        item = budget_fixture.ITEM
        shape = core.CoreRecordsCapacity(item_input_bytes=item, item_result_bytes=item)
        for capacity, limits, expected in ((budget_fixture.capacity(), None, 4),
                                           (shape, fixtures.typed_ipc(), 5)):
            with self.subTest(expected=expected):
                owner = self.case.owner(1, capacity=capacity, async_limits=limits)
                try:
                    self.assertEqual(owner.resources.max_active_batches, expected)
                    self.assertIsNone(owner.execution_profile['declared_legacy_request_budget'])
                    self.assertIsNone(owner._actors.model_actor)
                finally:
                    owner.close(timeout=1)
        self.assertEqual(RecordsResources(max_active_batches=2).request_capacity(), 2)

    def test_finite_consumers_reject_None_before_model_work(self):
        resources = replace(self.case.resources, max_active_batches=None)
        engine = self.case.engine
        engines = dict(main=engine, helper=replace(engine, model_path='/tmp/helper-model'))
        factories = (
            lambda: SemLoomRecordsStaticReference(budget_fixture.MovieTask(1).method('fixture'),
                engine, GenerationSpec(), resources),
            lambda: SemLoomRecordChainCore(engine, resources),
            lambda: SemLoomRecordChainCore(engine, resources, execution='record_local_sections',
                prepared_batching=PreparedBatching('token_budget', None, 10000)),
            lambda: CoreCascadeBackend(None, engines=engines, resources=resources),
            lambda: IMLaneChainBackend(engine=engine, resources=resources,
                author_args=None, model_environment=None),
            lambda: IMLaneCascadeBackend(None, engines=engines, resources=resources,
                author_args=None, model_environment=None),
            lambda: validation.make_resources(dict(suite='progression', progression_batch_rows=1,
                resources=asdict(resources))),
        )
        for index, construct in enumerate(factories):
            with self.subTest(consumer=index), self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                construct()

    def test_direct_workers_reject_missing_finite_R_before_framework_imports(self):
        resources = replace(self.case.resources, max_active_batches=None)
        source = SimpleNamespace(task=SimpleNamespace(name='movie-q1'))
        original = builtins.__import__

        def guarded(name, *args, **kwargs):
            if name.split('.')[0] in ('ray', 'lotus', 'vllm'):
                raise AssertionError('framework imported before finite request validation: '+name)
            return original(name, *args, **kwargs)

        with patch.object(builtins, '__import__', guarded):
            with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                worker.run_model_worker(dict(task='movie-q1', backend='semloom-local-model-static-reference'),
                    source, None, None, GenerationSpec(), resources, self.case.engine, None)
            with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                movie._cascade_model_worker(dict(backend='semloom-local-model-core-cascade'),
                    None, None, None, None, resources, None, None, None)

    def test_Ray_native_strategy_accepts_the_unspecified_request_setting(self):
        from ray.data._internal.compute import ActorPoolStrategy

        resources = RecordsResources()
        self.assertIsNone(resources.max_active_batches)
        strategy = ActorPoolStrategy(size=1, max_tasks_in_flight_per_actor=resources.max_active_batches)
        self.assertIsNone(strategy.max_tasks_in_flight_per_actor)

    def test_matrix_scan_reuses_the_complete_budget_with_unspecified_R(self):
        plan = dict(backend='semloom-core', engine=asdict(self.case.engine),
            resources=asdict(replace(self.case.resources, max_active_batches=None)),
            profile=dict(mode='async', max_inflight=1), core_capacity=asdict(budget_fixture.capacity()))
        group = dict(backend='semloom-core', calibration=dict(cache_dir='/tmp/scan-cache',
            candidates=[dict(max_inflight=4, cpu_workers=1)], repetitions=1,
            near_peak_ratio=.9, timeout_s=1))
        validate_core_scan(plan, group, 0, 8)
        group['calibration']['candidates'][0]['max_inflight'] = 5
        with self.assertRaisesRegex(ValueError, 'in-flight limit'):
            validate_core_scan(plan, group, 0, 8)
