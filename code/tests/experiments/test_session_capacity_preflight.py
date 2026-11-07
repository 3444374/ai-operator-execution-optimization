"""Finite request validation precedes inert Ray and model startup; CPU only."""
import builtins
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

from src.experiments import movie_records_session as session
from src.execution_provider.adapters import lotus_records_core as core
from tests.experiments import test_movie_records_session as fixtures
from tests.experiments.test_records_resource_budget import capacity, ipc


class SessionCapacityPreflightTests(unittest.TestCase):
    def setUp(self):
        case = fixtures.PlanTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.plan = deepcopy(case.plan)
        self.plan['queries'] = self.plan['queries'][:1]
        self.plan['lifecycle'] = 'isolated'
        self.plan['resources']['max_active_batches'] = None

    def static(self):
        return dict(deepcopy(self.plan), backend='semloom-static-reference', profile=dict(mode='sync'))

    def asynchronous(self):
        return dict(deepcopy(self.plan), profile=dict(mode='async', max_inflight=2),
            core_capacity=asdict(capacity()), async_limits=ipc())

    def checked_without_frameworks(self, plan):
        original = builtins.__import__
        def guard(name, *args, **options):
            if name.split('.')[0] in ('ray', 'daft', 'vllm', 'torch'):
                raise AssertionError('CPU plan validation imported '+name)
            return original(name, *args, **options)
        with patch.object(builtins, '__import__', guard):
            return session.check_plan(plan)

    def inert_ray(self):
        ray = ModuleType('ray')
        ray.__version__ = '2.56.1'
        ray.is_initialized = Mock(return_value=False)
        ray.init = Mock()
        ray.shutdown = Mock()
        return ray

    def reject_before_runtime(self, plan, *, reason='request capacity.*unresolved'):
        ray = self.inert_ray()
        with tempfile.TemporaryDirectory(prefix='preflight-no-runtime-') as directory:
            path = Path(directory)/'driver'
            owner = session._MovieDriver(plan, {}, path, plan['queries'][0])
            with patch.dict(sys.modules, {'ray': ray}):
                with self.assertRaisesRegex(ValueError, reason):
                    owner.start()
            ray.is_initialized.assert_not_called()
            ray.init.assert_not_called()
            self.assertFalse(owner.created_ray)
            self.assertIsNone(owner.owner)
            self.assertFalse(path.exists())

    def test_static_None_is_rejected_by_CPU_check_without_framework_imports(self):
        for lifecycle in ('isolated', 'resident'):
            with self.subTest(lifecycle=lifecycle), self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                self.checked_without_frameworks(dict(self.static(), lifecycle=lifecycle))

    def test_unchecked_static_None_is_rejected_before_any_runtime_or_owner(self):
        for lifecycle in ('isolated', 'resident'):
            with self.subTest(lifecycle=lifecycle):
                self.reject_before_runtime(dict(self.static(), lifecycle=lifecycle))

    def test_unresolved_Core_or_invalid_budget_also_reject_before_runtime(self):
        for profile in (dict(mode='sync'), dict(mode='async'), dict(mode='async', max_inflight=1)):
            plan = dict(deepcopy(self.plan), profile=profile)
            with self.subTest(profile=profile):
                with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                    self.checked_without_frameworks(plan)
                self.reject_before_runtime(plan)
        plan = self.asynchronous()
        plan['core_capacity']['budget']['active_requests'] = None
        with self.assertRaises(ValueError):
            self.checked_without_frameworks(plan)
        self.reject_before_runtime(plan, reason='active_requests')

    def test_static_explicit_capacity_preserves_the_current_owner(self):
        from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
        plan = self.static()
        plan['resources']['max_active_batches'] = 2
        checked = self.checked_without_frameworks(plan)
        self.assertEqual(checked['status'], 'checked_cpu_only')
        ray, ready = self.inert_ray(), dict(engine_uid='CPU-static-start', closed=False)
        with tempfile.TemporaryDirectory(prefix='preflight-static-') as directory:
            owner = session._MovieDriver(plan, {}, Path(directory)/'driver', plan['queries'][0])
            with patch.dict(sys.modules, {'ray': ray}), patch.object(
                    SemLoomRecordsStaticReference, 'start', return_value=ready) as start:
                try:
                    self.assertEqual(owner.start(), ready)
                    self.assertEqual(owner.resources.max_active_batches, 2)
                    self.assertEqual(owner.owner.resources.max_active_batches, 2)
                    ray.init.assert_called_once()
                    start.assert_called_once()
                finally:
                    if owner.owner is not None:
                        owner.owner.close()

    def test_complete_async_None_budget_passes_check_and_the_real_public_factory(self):
        plan = self.asynchronous()
        checked = self.checked_without_frameworks(plan)
        self.assertEqual(checked['core_execution']['owner_max_inflight'], 4)
        self.assertIsNone(checked['core_execution']['declared_legacy_request_budget'])
        ray, ready = self.inert_ray(), dict(engine_uid='CPU-async-start', closed=False)
        with tempfile.TemporaryDirectory(prefix='preflight-async-') as directory:
            owner = session._MovieDriver(plan, {}, Path(directory)/'driver', plan['queries'][0])
            with patch.dict(sys.modules, {'ray': ray}), patch.object(core.SemLoomRecordsCore,
                    'start', return_value=ready):
                try:
                    self.assertEqual(owner.start(), ready)
                    self.assertIsNone(owner.resources.max_active_batches)
                    self.assertEqual(owner.owner.resources.max_active_batches, 4)
                    self.assertEqual(owner.owner.capacity, capacity())
                    self.assertEqual(owner.core_execution, checked['core_execution'])
                    self.assertEqual({name: getattr(owner.owner._actors.limits, name) for name in ipc()}, ipc())
                    ray.init.assert_called_once()
                finally:
                    if owner.owner is not None:
                        owner.owner.close(timeout=1)
        self.assertIsNone(plan['resources']['max_active_batches'])

    def test_rolling_None_budget_uses_its_existing_deferred_model_owner(self):
        plan = self.asynchronous()
        plan.update(rolling_refill=True,
            prepared_batching=dict(policy='token_budget', max_rows=None, token_budget=10000))
        checked = self.checked_without_frameworks(plan)
        ray = self.inert_ray()
        with tempfile.TemporaryDirectory(prefix='preflight-rolling-') as directory:
            owner = session._MovieDriver(plan, {}, Path(directory)/'driver', plan['queries'][0])
            with patch.dict(sys.modules, {'ray': ray}):
                try:
                    ready = owner.start()
                    self.assertTrue(ready['no_session_model'])
                    self.assertIsNone(owner.owner.owner)
                    self.assertEqual(owner.owner.resources.max_active_batches, 4)
                    self.assertEqual(owner.owner.capacity, capacity())
                    self.assertEqual(owner.core_execution, checked['core_execution'])
                    ray.init.assert_called_once()
                finally:
                    if owner.owner is not None:
                        owner.owner.close(timeout=1)


if __name__ == '__main__':
    unittest.main()
