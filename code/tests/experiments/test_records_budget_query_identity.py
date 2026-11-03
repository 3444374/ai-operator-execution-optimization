"""Independent-review regressions; CPU IPC fixtures, no model or real Ray."""
from copy import deepcopy
from dataclasses import asdict, replace
import sys
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core
from src.experiments import lotus_records_movie as movie
from src.experiments.calibration import core as calibration
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.experiments import test_core_calibration as fixtures
from tests.experiments.test_records_resource_budget import capacity, ipc, ITEM
from tests.execution_provider.test_lotus_records_async_core import Source, ThreadedRuntime


class QueryWindowReuseTests(unittest.TestCase):
    def run_queries(self, *, independent, slow_consumer=False):
        rows = [dict(row_id='r'+str(i), source_position=i, reviewId='repeat',
                     id='taken_3', reviewText='slow') for i in range(3)]
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=3,
            query_timeout_s=5, startup_timeout_s=5)
        options = {}
        if independent:
            value = capacity()
            value = replace(value, budget=replace(value.budget, result_bytes=4*ITEM))
            options = dict(capacity=value, async_limits=dict(ipc(result_slots=4), max_inflight=4))
        with ThreadedRuntime(rows):
            owner = core.create_records_core(FilterMethod('fixture', instruction='Keep {reviewText}'),
                EngineSpec('/tmp/fixture-model', sys.executable, sys.executable), GenerationSpec(), resources,
                profile=dict(mode='async', max_inflight=3), **options)
            samples = []
            try:
                for index, maximum in enumerate((1, 3, 1)):
                    stream = owner.evaluate(Source(rows), query_id='query-'+str(index), max_inflight=maximum)
                    if slow_consumer and index == 0:
                        first = next(stream)
                        with self.assertRaises(RuntimeError):
                            owner.evaluate(Source(rows), query_id='blocked-next', max_inflight=3)
                        values = [first, *stream]
                    else:
                        values = list(stream)
                    samples.append((owner.core.policies.admission.limit, stream.peak_usage['active_requests']))
                    self.assertEqual(len(values), 3)
                    self.assertEqual({v['query_id'] for v in values}, {'query-'+str(index)})
                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                    self.assertEqual(owner.transport.retained_refs(), 0)
                    self.assertEqual(owner.core.capacity.limits, owner.capacity.limits(owner.resources))
                self.assertEqual([peak for _,peak in samples], [1, 3, 1])
                self.assertEqual([gate for gate,_ in samples], [1, 3, 1] if independent else [3, 3, 3])
            finally:
                self.assertEqual(owner.close(timeout=3)['status'], 'closed')

    def test_first_low_C_does_not_lock_legacy_or_explicit_owner(self):
        for independent in (False, True):
            with self.subTest(independent=independent):
                self.run_queries(independent=independent)

    def test_slow_consumer_must_finish_before_larger_next_query(self):
        for independent in (False, True):
            with self.subTest(independent=independent):
                self.run_queries(independent=independent, slow_consumer=True)

    def test_direct_budget_owner_rejects_implicit_sync_before_actor_allocation(self):
        method = FilterMethod('fixture', instruction='Keep {reviewText}')
        engine = EngineSpec('/tmp/fixture-model', sys.executable, sys.executable)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=4)
        with patch.object(core, '_ActorOwner') as allocate:
            with self.assertRaisesRegex(ValueError, 'async actor'):
                core.SemLoomRecordsCore(method, engine, GenerationSpec(), resources, capacity=capacity())
            allocate.assert_not_called()

    def test_direct_budget_owner_rejects_a_supplied_sync_actor(self):
        method = FilterMethod('fixture', instruction='Keep {reviewText}')
        engine, generation = EngineSpec('/tmp/fixture-model', sys.executable, sys.executable), GenerationSpec()
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=4)
        actor = core._ActorOwner(method, engine, generation, resources)
        with self.assertRaisesRegex(ValueError, 'async actor'):
            core.SemLoomRecordsCore(method, engine, generation, resources,
                capacity=capacity(), actor_owner=actor)
        self.assertIsNone(actor.model_actor)


class CanonicalOrganizationTests(unittest.TestCase):
    def setUp(self):
        self.helper = fixtures.CoreCalibrationTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.plan = deepcopy(self.helper.plan)
        self.plan.update(core_capacity=asdict(capacity()), async_limits=ipc(), rolling_refill=True,
            profile=dict(mode='async', max_inflight=2),
            prepared_batching=dict(policy='token_budget', token_budget=10000))

    def identity(self, plan):
        return calibration.build_core_identity(plan, self.helper.checked,
            hardware=self.helper.hardware, model=self.helper.model, software=self.helper.software)

    def test_implicit_and_explicit_token_defaults_have_one_effective_identity(self):
        explicit = deepcopy(self.plan)
        explicit['prepared_batching']['max_rows'] = None
        explicit['experimental_skip_token_packing'] = False
        resources = RecordsResources(**self.plan['resources'])
        resolved = core.resolve_core_profile(EngineSpec.from_dict(self.plan['engine']), resources,
            self.plan['profile'], rolling_refill=True, capacity=capacity())
        self.assertEqual(core.resolve_record_batching(resolved, **movie._record_batching_options(self.plan)),
                         core.resolve_record_batching(resolved, **movie._record_batching_options(explicit)))
        self.assertEqual(self.identity(self.plan), self.identity(explicit))
        self.assertEqual(self.plan['prepared_batching'], dict(policy='token_budget', token_budget=10000))

    def test_equivalent_defaults_hit_selection_but_other_tokens_and_budgets_miss(self):
        self.helper.plan = deepcopy(self.plan)
        self.helper.identity = self.identity(self.plan)
        path = self.helper.save_selection(calibration.build_core_selection(**self.helper.scan()))
        explicit = deepcopy(self.plan)
        explicit['prepared_batching']['max_rows'] = None
        self.assertEqual(calibration.resolve_core_selection(path, self.identity(explicit))['status'], 'ready')
        variants = []
        larger_B = deepcopy(explicit)
        larger_B['prepared_batching']['token_budget'] += 1
        variants.append(larger_B)
        larger_budget = deepcopy(explicit)
        larger_budget['core_capacity']['budget']['input_bytes'] += 1
        variants.append(larger_budget)
        bypass = dict(explicit, experimental_skip_token_packing=True)
        variants.append(bypass)
        for variant in variants:
            with self.subTest(variant=variant):
                selection = calibration.resolve_core_selection(path, self.identity(variant))
                self.assertEqual((selection['status'], selection['reason']),
                    ('needs_scan', 'identity_or_policy_mismatch'))


if __name__ == '__main__':
    unittest.main()
