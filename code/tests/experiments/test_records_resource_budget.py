"""Explicit logical resource envelopes; CPU fixtures, no hardware-capacity claims."""
from copy import deepcopy
from contextlib import redirect_stdout
from dataclasses import asdict
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from src.baselines.text.movie_tasks import MovieTask
from src.execution_provider.adapters import lotus_records_core as core
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.experiments.calibration import core as calibration
from src.semantic_methods.budget import row_reservation
from src.semantic_methods.continuation import MethodLimits
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.experiments import test_core_calibration as calibration_fixture
from tests.experiments import test_movie_records_session as session_fixture
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime, Source
from tests.scheduling.test_incremental_session import setup, task

ITEM = 65536


def capacity(stages=1, *, state_rows=2):
    size = ITEM + stages*ITEM
    method = MethodLimits(ITEM, 2*size, size, stages)
    return core.CoreRecordsCapacity(item_input_bytes=ITEM, item_result_bytes=ITEM,
        budget=dict(held_tasks=9, input_bytes=3*ITEM, result_bytes=2*ITEM,
            active_requests=4, method_runs=7, method_bytes=state_rows*row_reservation(method)))


def ipc(*, result_slots=1):
    return dict(max_inflight=2, result_slots=result_slots, command_slots=3, control_slots=2,
        input_bytes=3*ITEM, result_bytes=result_slots*ITEM)


class ResourceBudgetTests(unittest.TestCase):
    def setUp(self):
        self.engine = EngineSpec('/tmp/fixture-model','/tmp/fixture-model-python','/tmp/fixture-driver-python')
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=1)

    def owner(self, C, **changes):
        options = dict(capacity=capacity(), async_limits=ipc(), profile=dict(mode='async', max_inflight=C))
        options.update(changes)
        return core.create_records_core(MovieTask(1).method('cpu-fixture'), self.engine,
            GenerationSpec(), self.resources, **options)

    def test_complete_budget_round_trips_and_rejects_unknown_or_nonpositive(self):
        value = capacity()
        self.assertEqual(core.CoreRecordsCapacity(**asdict(value)), value)
        for name in asdict(value.budget):
            for bad in (None, 0, -1, True, float('inf')):
                raw = asdict(value)
                raw['budget'][name] = bad
                with self.subTest(name=name, bad=bad), self.assertRaises(ValueError):
                    core.CoreRecordsCapacity(**raw)
        raw = asdict(value)
        raw['budget'].pop('method_bytes')
        with self.assertRaises(TypeError):
            core.CoreRecordsCapacity(**raw)

    def test_declared_Core_items_and_complete_method_envelope_must_fit(self):
        for name in ('input_bytes', 'result_bytes'):
            raw = asdict(capacity())
            raw['budget'][name] = ITEM-1
            with self.subTest(name=name), self.assertRaises(ValueError):
                core.CoreRecordsCapacity(**raw)
        raw = asdict(capacity())
        raw['budget']['method_bytes'] = 1
        value = core.CoreRecordsCapacity(**raw)
        with self.assertRaisesRegex(ValueError, 'continuation envelope'):
            value.method_capacity(self.resources, MethodLimits(ITEM, ITEM, ITEM, 1))

    def test_C_and_legacy_R_do_not_resize_explicit_storage(self):
        expected = capacity().limits(self.resources)
        for C in (1, 2, 4):
            owner = self.owner(C)
            self.assertEqual(owner.resources.max_active_batches, 4)
            self.assertEqual(owner.capacity.limits(owner.resources), expected)
            self.assertEqual(owner.execution_profile['max_inflight'], C)
            self.assertEqual(owner.execution_profile['owner_max_inflight'], 4)
            self.assertIsNone(owner._actors.model_actor)

    def test_IPC_slots_and_totals_are_independent_finite_declarations(self):
        owner = self.owner(3)
        limits = owner._actors.limits
        self.assertEqual((limits.max_inflight, limits.result_slots), (2, 1))
        self.assertEqual((limits.input_bytes, limits.result_bytes), (3*ITEM, ITEM))
        self.assertEqual(owner.capacity.limits(owner.resources).held_tasks, 9)
        self.assertEqual(owner.capacity.limits(owner.resources).result_bytes, 2*ITEM)

    def test_missing_IPC_budget_is_not_derived_from_C_or_infinity(self):
        for limits in (None, {}, dict(feedback_interval_s=.1), dict(ipc(), input_bytes=None)):
            with self.subTest(limits=limits), self.assertRaises((ValueError, TypeError)):
                self.owner(1, async_limits=limits)

    def test_explicit_C_and_supported_async_mode_are_required(self):
        for profile in (dict(mode='sync', max_inflight=1), dict(mode='async'),
                        dict(mode='async', max_inflight=5)):
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                self.owner(1, profile=profile)
        with self.assertRaisesRegex(ValueError, 'rolling'):
            self.owner(1, prepared_batching=PreparedBatching('token_budget', None, 10000))

    def test_default_path_retains_R_derived_limits_and_strict_IPC_count_guard(self):
        owner = self.owner(1, capacity=None, async_limits=None)
        self.assertEqual((owner.resources.max_active_batches, owner._actors.limits.result_slots), (1, 1))
        self.assertEqual(owner.capacity.limits(owner.resources).input_bytes, 2*1024**2)
        with self.assertRaises(ValueError):
            self.owner(1, capacity=None, async_limits=ipc())

    def test_method_bytes_bind_independently_before_declared_row_slots(self):
        value = capacity()
        method = MethodLimits(ITEM, 4*ITEM, 2*ITEM, 1)
        actual = value.method_capacity(self.resources, method)
        self.assertEqual(actual.runs, 7)
        self.assertEqual(actual.bytes//row_reservation(method), 2)

    def test_short_large_and_oversize_inputs_use_declared_byte_limits(self):
        limits = capacity().limits(self.resources)
        for size, accepted in ((1024, 9), (ITEM, 3)):
            engine, session, _, _ = setup(**vars(limits))
            count = 0
            for index in range(9):
                offered = session.offer([task(index, payload=b'x'*size, max_result_bytes=16)])
                if not offered.accepted_prefix_count:
                    break
                count += 1
            self.assertEqual(count, accepted)
            self.assertLessEqual(engine.capacity.usage().input_bytes, limits.input_bytes)
        engine, session, _, _ = setup(**vars(limits))
        self.assertEqual(session.offer([task(0, payload=b'x'*(ITEM+1), max_result_bytes=16)]).status, 'REJECTED')
        self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_infinite_producer_stops_at_result_reservations_with_one_pending(self):
        limits = capacity().limits(self.resources)
        engine, session, _, _ = setup(**vars(limits))
        pulled = []
        def source():
            index = 0
            while True:
                pulled.append(index)
                yield task(index, payload=b'x', max_result_bytes=ITEM)
                index += 1
        values, pending = source(), None
        for _ in range(50):
            pending = next(values) if pending is None else pending
            if session.offer([pending]).accepted_prefix_count:
                pending = None
        self.assertEqual(engine.capacity.usage().held_tasks, 2)
        self.assertEqual(pulled, [0, 1, 2])
        self.assertEqual(pending.sequence, 2)

    def test_rolling_factory_keeps_C_separate_from_actor_and_method_budgets(self):
        owner = self.owner(2, prepared_batching=PreparedBatching('token_budget', None, 10000), rolling_refill=True)
        chain = owner._new_owner(None)
        self.assertEqual(chain.request_window, 2)
        self.assertEqual(chain.resources.max_active_batches, 4)
        self.assertEqual(chain.capacity.budget.method_runs, 7)
        self.assertEqual(chain._actors.limits.result_slots, 1)

    def test_ordinary_query_C_changes_only_dispatch_not_storage_or_ownership(self):
        rows = [dict(row_id='r'+str(i), source_position=i, reviewId='repeat', id='taken_3',
                     reviewText='fast') for i in range(5)]
        with ThreadedRuntime(rows):
            owner = self.owner(3)
            try:
                for maximum in (1, 3):
                    stream = owner.evaluate(Source(rows), query_id='q'+str(maximum), max_inflight=maximum)
                    values = list(stream)
                    self.assertEqual(len(values), 5)
                    self.assertLessEqual(stream.peak_usage['active_requests'], maximum)
                    self.assertEqual(owner.core.capacity.limits, capacity().limits(owner.resources))
                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                    self.assertEqual(owner.transport.retained_refs(), 0)
            finally:
                self.assertEqual(owner.close(timeout=3)['status'], 'closed')


class ResourceBudgetEntryTests(unittest.TestCase):
    def setUp(self):
        helper = session_fixture.PlanTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        self.plan = deepcopy(helper.plan)
        self.plan.update(profile=dict(mode='async', max_inflight=2),
            core_capacity=asdict(capacity()), async_limits=ipc())
        self.plan['resources']['max_active_batches'] = 1
        self.root = Path(helper.manifests['movie-q1']).parent.parent

    def test_normal_session_check_reports_declared_budget_with_old_R_independent(self):
        for lifecycle in ('resident', 'isolated'):
            with self.subTest(lifecycle=lifecycle):
                checked = session.check_plan(dict(self.plan, lifecycle=lifecycle))
                self.assertEqual(checked['core_execution']['max_inflight'], 2)
                self.assertEqual(checked['core_execution']['owner_max_inflight'], 4)
                self.assertEqual(checked['core_capacity']['budget']['held_tasks'], 9)
                self.assertEqual(checked['async_limits']['result_slots'], 1)

    def test_normal_rolling_and_dynamic_check_use_same_budget(self):
        self.plan.update(prepared_batching=dict(policy='token_budget', max_rows=None, token_budget=10000),
                         rolling_refill=True, lifecycle='resident')
        checked = session.check_plan(self.plan)
        self.assertEqual(checked['record_batching']['C_request'], 2)
        self.plan['profile'] = dict(mode='async', admission=dict(kind='resource_feedback'))
        checked = session.check_plan(self.plan)
        self.assertEqual(checked['core_execution']['max_inflight'], 1)
        self.assertEqual(checked['core_execution']['owner_max_inflight'], 4)

    def test_other_baselines_do_not_accept_resource_budget(self):
        for backend in ('lotus-native', 'ray-data', 'daft', 'semloom-static-reference'):
            with self.subTest(backend=backend), self.assertRaisesRegex(ValueError, 'semloom-core'):
                session.check_plan(dict(self.plan, backend=backend, lifecycle='isolated'))

    def test_normal_Movie_check_reports_budgets_without_allocating_owner(self):
        config = dict(schema='semloom.movie_records_run.v1', backend='semloom-local-model-core-async',
            query_id='q1', task='movie-q1', method=asdict(MovieTask(1).method(self.plan['model_name'])),
            engine=self.plan['engine'], generation=self.plan['generation'], resources=self.plan['resources'],
            core_profile=self.plan['profile'], core_capacity=self.plan['core_capacity'],
            async_limits=self.plan['async_limits'])
        path = self.root/'budget-config.json'
        path.write_text(json.dumps(config))
        output = io.StringIO()
        with patch.object(movie, 'model_preflight', return_value=dict(inference_calls=0)), \
                patch.object(core, 'create_records_core') as allocate, redirect_stdout(output):
            self.assertEqual(movie.main(['check', '--config', str(path), '--sembench-checkout', str(self.root),
                '--source-manifest', self.plan['queries'][0]['manifest']]), 0)
        allocate.assert_not_called()
        report = json.loads(output.getvalue())
        self.assertEqual(report['core_execution']['max_inflight'], 2)
        self.assertEqual(report['core_execution']['owner_max_inflight'], 4)
        self.assertEqual(report['requested_async_limits']['result_slots'], 1)


class ResourceBudgetIdentityTests(unittest.TestCase):
    def setUp(self):
        self.helper = calibration_fixture.CoreCalibrationTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.plan = deepcopy(self.helper.plan)
        self.plan.update(core_capacity=asdict(capacity()), async_limits=ipc())
        self.plan['resources']['max_active_batches'] = 1
        self.plan['profile']['max_inflight'] = 2

    def identity(self, plan=None):
        return calibration.build_core_identity(plan or self.plan, self.helper.checked,
            hardware=self.helper.hardware, model=self.helper.model, software=self.helper.software)

    def test_capacity_policy_stays_constant_across_C_and_ignores_legacy_R(self):
        identity = self.identity()
        for C in (1, 3, 4):
            candidate = calibration.core_candidate_plan(self.plan, self.helper.checked,
                dict(max_inflight=C, cpu_workers=1), identity=identity)
            self.assertEqual(self.identity(candidate), identity)
        plan = deepcopy(self.plan)
        plan['resources']['max_active_batches'] = 17
        self.assertEqual(self.identity(plan), identity)

    def test_each_distinct_budget_invalidates_identity(self):
        initial = calibration.core_identity_signature(self.identity())
        for name in self.plan['core_capacity']['budget']:
            plan = deepcopy(self.plan)
            plan['core_capacity']['budget'][name] += 1
            with self.subTest(name=name):
                self.assertNotEqual(calibration.core_identity_signature(self.identity(plan)), initial)
        plan = deepcopy(self.plan)
        plan['async_limits']['result_slots'] = 2
        self.assertNotEqual(calibration.core_identity_signature(self.identity(plan)), initial)

    def test_packing_and_rolling_paths_do_not_share_a_capacity_selection(self):
        ordinary = self.identity()
        plan = dict(self.plan, rolling_refill=True,
            prepared_batching=dict(policy='token_budget', max_rows=None, token_budget=10000))
        initial = self.identity(plan)
        self.assertNotEqual(initial, ordinary)
        larger = deepcopy(plan)
        larger['prepared_batching']['token_budget'] = 20000
        self.assertNotEqual(self.identity(larger), initial)
        self.assertNotEqual(self.identity(dict(plan, experimental_skip_token_packing=True)), initial)


if __name__ == '__main__':
    unittest.main()
