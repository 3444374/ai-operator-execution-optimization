"""Session batching validation precedes runtime allocation, including direct start."""
from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.experiments import movie_records_session as session
from tests.experiments import test_session_capacity_preflight as fixtures


class SessionBatchingPreflightTests(unittest.TestCase):
    setUp = fixtures.SessionCapacityPreflightTests.setUp
    checked_without_frameworks = fixtures.SessionCapacityPreflightTests.checked_without_frameworks
    inert_ray = fixtures.SessionCapacityPreflightTests.inert_ray
    reject_before_runtime = fixtures.SessionCapacityPreflightTests.reject_before_runtime

    def batched(self):
        plan = deepcopy(self.plan)
        plan['resources']['max_active_batches'] = 2
        plan.update(profile=dict(mode='async', max_inflight=2),
            prepared_batching=dict(policy='token_budget', max_rows=None, token_budget=10000))
        return plan

    def test_invalid_concurrency_rejects_before_runtime_without_check_plan(self):
        for concurrency in (0, True, 3):
            for lifecycle in ('isolated', 'resident'):
                plan = dict(self.batched(), batch_concurrency=concurrency, lifecycle=lifecycle)
                with self.subTest(concurrency=concurrency, lifecycle=lifecycle):
                    with self.assertRaisesRegex(ValueError, 'batch_concurrency must fit'):
                        self.checked_without_frameworks(plan)
                    self.reject_before_runtime(plan, reason='batch_concurrency must fit')

    def test_incompatible_or_missing_batch_policy_rejects_before_runtime(self):
        cases = []
        rolling = dict(self.batched(), rolling_refill=True, batch_concurrency=1)
        cases.append((rolling, 'rolling_refill uses request R'))
        plan = self.batched()
        del plan['prepared_batching']
        plan['batch_concurrency'] = 1
        cases.append((plan, 'batch controls require prepared_batching'))
        for plan, reason in cases:
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(ValueError, reason):
                    self.checked_without_frameworks(plan)
                self.reject_before_runtime(plan, reason=reason)

    def test_valid_finite_batching_keeps_deferred_model_and_actual_configuration(self):
        plan = dict(self.batched(), batch_concurrency=2)
        checked = self.checked_without_frameworks(plan)
        ray = self.inert_ray()
        with tempfile.TemporaryDirectory(prefix='batch-preflight-valid-') as directory:
            driver = session._MovieDriver(plan, {}, Path(directory)/'driver', plan['queries'][0])
            with patch.dict(sys.modules, {'ray': ray}):
                try:
                    ready = driver.start()
                    self.assertTrue(ready['no_session_model'])
                    self.assertIsNone(driver.owner.owner)
                    self.assertEqual(driver.core_execution, checked['core_execution'])
                    self.assertEqual(driver.owner.execution_profile['record_batching'], checked['record_batching'])
                    ray.init.assert_called_once()
                finally:
                    if driver.owner is not None:
                        driver.owner.close(timeout=1)


if __name__ == '__main__':
    unittest.main()
