"""Ordinary records capacity wiring; CPU fixtures, no Ray/model/GPU runtime."""
from collections import deque
from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import asdict
import io
import json
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.movie_tasks import MovieTask
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters import lotus_records_async_actor as actors
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.async_engine_process import AsyncEngineProcess, _frame
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from src.serving.backends.model_context import model_context_record
from tests.experiments import test_movie_records_session as fixtures
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime, Source
from tests.serving.test_async_engine_process import row

MIB = 1024**2


def queued_process(spec, limits):
    """Exercise real submit/encoding/charging without allocating a child."""
    value = object.__new__(AsyncEngineProcess)
    value.spec, value.generation, value.limits = spec, GenerationSpec(), limits
    value.ready_report = dict(engine_uid='cpu-queued-fixture',
        model_context=model_context_record(spec, 'cpu-queued-fixture', spec.max_model_len or 32768))
    value._condition, value._io_done = threading.Condition(), threading.Event()
    value.process = SimpleNamespace(poll=lambda: None)
    value._broken, value._closing, value._query_id = None, False, None
    value._sequence, value._owner_uid = 0, 'f'*32
    value._slots, value._ready, value._data_commands, value._controls = {}, deque(), deque(), deque()
    return value


class CapacityFactoryTests(unittest.TestCase):
    def setUp(self):
        self.method = MovieTask(1).method('openai/cpu-fixture')
        self.engine = EngineSpec('/tmp/cpu-model', '/tmp/model-python', '/tmp/driver-python')
        self.generation = GenerationSpec()
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=11, startup_timeout_s=5, query_timeout_s=8)

    def owner(self, *, overrides=None, capacity=None, maximum=3, **options):
        return core.create_records_core(self.method, self.engine, self.generation, self.resources,
            profile=dict(mode='async', max_inflight=maximum), async_limits=overrides,
            capacity=capacity, **options)

    def test_omitted_empty_and_feedback_only_limits_use_resolved_owner_not_eight(self):
        for maximum in (1, 3, 11):
            for overrides in (None, {}, dict(feedback_interval_s=.1)):
                with self.subTest(maximum=maximum, overrides=overrides):
                    owner = self.owner(maximum=maximum, overrides=overrides)
                    limits = owner._actors.limits
                    self.assertEqual((limits.max_inflight, limits.result_slots, limits.command_slots),
                                     (maximum, maximum, maximum))
                    self.assertEqual((limits.input_bytes, limits.result_bytes), (maximum*2*MIB,)*2)
                    self.assertEqual(limits.feedback_interval_s, .1 if overrides else 0.)
                    self.assertIsNone(owner._actors.model_actor)

    def test_partial_items_totals_and_result_slots_keep_distinct_units(self):
        capacity = core.CoreRecordsCapacity(item_input_bytes=6*MIB, item_result_bytes=7*MIB)
        owner = self.owner(capacity=capacity, overrides=dict(item_input_bytes=MIB,
            item_result_bytes=65536, result_slots=5))
        self.assertIs(owner.capacity, capacity)
        grants = capacity.limits(owner.resources)
        self.assertEqual((grants.input_bytes, grants.result_bytes), (18*MIB, 21*MIB))
        limits = owner._actors.limits
        self.assertEqual((limits.item_input_bytes, limits.input_bytes), (MIB, 3*MIB))
        self.assertEqual((limits.item_result_bytes, limits.result_bytes), (65536, 5*65536))
        self.assertEqual((limits.max_inflight, limits.result_slots), (3, 5))

    def test_explicit_small_totals_are_not_expanded_to_R_times_item(self):
        capacity = core.CoreRecordsCapacity(item_input_bytes=4*MIB, item_result_bytes=4*MIB)
        options = dict(item_input_bytes=MIB, input_bytes=MIB, item_result_bytes=4096,
                       result_bytes=4096)
        limits = self.owner(capacity=capacity, overrides=options)._actors.limits
        self.assertEqual({k:getattr(limits, k) for k in options}, options)
        self.assertEqual(limits.max_inflight, 3)

    def test_small_Core_result_and_IPC_minimum_remain_independent(self):
        owner = self.owner(capacity=core.CoreRecordsCapacity(item_result_bytes=1024),
            overrides=dict(item_result_bytes=4096))
        self.assertEqual(owner.capacity.limits(owner.resources).result_bytes, 3072)
        self.assertEqual(owner._actors.limits.item_result_bytes, 4096)
        self.assertEqual(owner._actors.limits.result_bytes, 12288)

    def test_explicit_counts_and_invalid_totals_reject_before_actor_construction(self):
        for overrides in (dict(max_inflight=8), dict(result_slots=2), dict(input_bytes=MIB),
                          dict(result_bytes=1024), dict(item_result_bytes=1024),
                          dict(item_input_bytes='4096'), dict(result_slots='three')):
            with self.subTest(overrides=overrides), patch.object(actors, 'AsyncRecordsActorOwner') as allocate:
                with self.assertRaises(ValueError):
                    self.owner(overrides=overrides)
                allocate.assert_not_called()

    def test_explicit_typed_limits_are_preserved(self):
        explicit = AsyncEngineLimits(max_inflight=3, result_slots=3, command_slots=1,
            input_bytes=MIB, item_input_bytes=MIB, result_bytes=4096, item_result_bytes=4096)
        self.assertIs(self.owner(overrides=explicit)._actors.limits, explicit)

    def test_token_records_forward_capacity_to_existing_chain_and_owner(self):
        capacity = core.CoreRecordsCapacity(item_input_bytes=4*MIB, item_result_bytes=65536)
        owner = self.owner(capacity=capacity, overrides={},
            prepared_batching=PreparedBatching('token_budget', None, 100000), rolling_refill=True,
            resident=True)
        chain = owner._new_owner(SimpleNamespace())
        self.assertIs(chain.capacity, capacity)
        self.assertEqual(chain._actors.limits.item_input_bytes, 4*MIB)
        self.assertEqual(chain._actors.limits.input_bytes, 12*MIB)
        self.assertIsNone(chain._actors.model_actor)

    def test_three_MiB_requires_both_declared_input_allowances(self):
        request = row(text='x'*(3*MIB))
        for core_bytes, ipc_bytes, rejected_at in ((4*MIB, 4*MIB, None),
                (2*MIB, 4*MIB, 'Core'), (4*MIB, 2*MIB, 'IPC')):
            with self.subTest(Core=core_bytes, IPC=ipc_bytes):
                owner = self.owner(capacity=core.CoreRecordsCapacity(item_input_bytes=core_bytes),
                    overrides=dict(item_input_bytes=ipc_bytes))
                process = queued_process(self.engine, owner._actors.limits)
                if rejected_at == 'Core':
                    with self.assertRaisesRegex(ValueError, 'records bytes'):
                        core._encode(request, owner.capacity.item_input_bytes)
                else:
                    core._encode(request, owner.capacity.item_input_bytes)
                    if rejected_at == 'IPC':
                        with self.assertRaisesRegex(ValueError, 'IPC item'):
                            process.submit(request, deadline_monotonic=time.monotonic()+10)
                    else:
                        outcome = process.submit(request, deadline_monotonic=time.monotonic()+10)
                        self.assertEqual(outcome.status, 'queued')
                        self.assertGreater(process._usage()['input_bytes'], 3*MIB)
                self.assertEqual(len(process._slots), int(rejected_at is None))
                self.assertEqual(process._sequence, int(rejected_at is None))

    def test_small_IPC_total_keeps_unknown_input_and_result_charged(self):
        owner = self.owner(overrides=dict(item_input_bytes=4096, input_bytes=4096,
            item_result_bytes=4096, result_bytes=4096))
        process = queued_process(self.engine, owner._actors.limits)
        first = process.submit(row(), deadline_monotonic=time.monotonic()+10)
        self.assertEqual(first.status, 'queued')
        slot = process._slots[first.request_id]
        slot.accepted, slot.terminal = True, dict(confirmed=False)
        usage = process._usage()
        self.assertEqual(usage['unknown_requests'], 1)
        self.assertGreater(usage['input_bytes'], 0)
        self.assertEqual(usage['reserved_result_bytes'], 4096)
        second = process.submit(row(1), deadline_monotonic=time.monotonic()+10)
        self.assertEqual(second.status, 'backpressure')
        self.assertEqual(process._usage(), usage)

    def test_three_MiB_result_has_independent_IPC_and_Core_limits(self):
        result = dict(text='x'*(3*MIB))
        for core_bytes, ipc_bytes in ((4*MIB, 4*MIB), (2*MIB, 4*MIB), (4*MIB, 2*MIB)):
            owner = self.owner(capacity=core.CoreRecordsCapacity(item_result_bytes=core_bytes),
                overrides=dict(item_result_bytes=ipc_bytes))
            if ipc_bytes < 3*MIB:
                with self.assertRaisesRegex(ValueError, 'IPC item'):
                    _frame(dict(op='terminal', result=result), owner._actors.limits.item_result_bytes)
            else:
                _frame(dict(op='terminal', result=result), owner._actors.limits.item_result_bytes)
            if core_bytes < 3*MIB:
                with self.assertRaisesRegex(ValueError, 'records bytes'):
                    core._encode(result, owner.capacity.item_result_bytes)
            else:
                core._encode(result, owner.capacity.item_result_bytes)

    def test_consecutive_queries_get_fresh_limits_and_ledgers_with_one_CPU_owner(self):
        rows = [dict(row_id='r'+str(i), source_position=i, reviewId='repeat', id='taken_3',
                     reviewText='fast') for i in range(2)]
        capacity = core.CoreRecordsCapacity(item_input_bytes=4*MIB, item_result_bytes=65536)
        with ThreadedRuntime(rows) as runtime:
            owner = self.owner(capacity=capacity, overrides=dict(input_bytes=4*MIB,
                item_result_bytes=65536, result_bytes=65536))
            sessions = []
            try:
                for index, maximum in enumerate((1, 2)):
                    stream = owner.evaluate(Source(rows), query_id='query-'+str(index), max_inflight=maximum)
                    results = list(stream)
                    sessions.append(stream.session.session_id)
                    self.assertEqual(len(results), 2)
                    self.assertEqual({r['query_id'] for r in results}, {'query-'+str(index)})
                    self.assertEqual(stream.session.limits.input_bytes, maximum*4*MIB)
                    self.assertEqual(stream.session.limits.result_bytes, maximum*65536)
                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                    self.assertEqual(owner.transport.retained_refs(), 0)
                self.assertNotEqual(*sessions)
                self.assertEqual(len(runtime.async_actors), 1)
                self.assertEqual(owner._actors.limits.result_bytes, 65536)
            finally:
                self.assertEqual(owner.close(timeout=3)['status'], 'closed')


class CapacityMovieEntryTests(unittest.TestCase):
    def setUp(self):
        helper = fixtures.PlanTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        self.plan = deepcopy(helper.plan)
        self.root = Path(helper.manifests['movie-q1']).parent.parent
        self.plan['resources']['max_active_batches'] = 11
        self.plan['profile'] = dict(mode='async', max_inflight=3)
        self.plan['core_capacity'] = dict(item_input_bytes=4*MIB, item_result_bytes=65536)
        self.plan['async_limits'] = dict(feedback_interval_s=.1)

    def config(self):
        return dict(schema='semloom.movie_records_run.v1', backend='semloom-local-model-core-async',
            query_id='q1', task='movie-q1', method=asdict(MovieTask(1).method(self.plan['model_name'])),
            engine=self.plan['engine'], generation=self.plan['generation'], resources=self.plan['resources'],
            core_profile=self.plan['profile'], core_capacity=self.plan['core_capacity'],
            async_limits=self.plan['async_limits'])

    def save(self, value):
        path = self.root/'config.json'
        path.write_text(json.dumps(value))
        return path

    def test_ordinary_config_and_parser_keep_explicit_capacity_and_partial_limits(self):
        config = self.config()
        parsed, _, _, resources, _ = movie.read_config(self.save(config))
        self.assertEqual(parsed, json.loads(json.dumps(config)))
        limits = movie._async_limits(parsed)
        self.assertEqual((limits.max_inflight, limits.result_slots), (3, 3))
        self.assertEqual((limits.item_input_bytes, limits.input_bytes), (4*MIB, 12*MIB))
        self.assertEqual((limits.item_result_bytes, limits.result_bytes), (65536, 3*65536))
        self.assertEqual(resources.max_active_batches, 11)

    def test_session_accepts_config_for_resident_and_isolated_lifetimes(self):
        for lifecycle in ('resident', 'isolated'):
            with self.subTest(lifecycle=lifecycle):
                self.plan['lifecycle'] = lifecycle
                checked = session.check_plan(self.plan)
                self.assertEqual(checked['core_capacity']['item_input_bytes'], 4*MIB)
                self.assertEqual(checked['async_limits']['max_inflight'], 3)
                self.assertEqual(checked['async_limits']['feedback_interval_s'], .1)

    def test_ordinary_check_reports_capacity_without_allocating_an_owner(self):
        output = io.StringIO()
        with patch.object(movie, 'model_preflight', return_value=dict(inference_calls=0)), \
                patch.object(core, 'create_records_core') as allocate, redirect_stdout(output):
            status = movie.main(['check', '--source-manifest', self.plan['queries'][0]['manifest'],
                '--sembench-checkout', str(self.root), '--config', str(self.save(self.config()))])
        self.assertEqual(status, 0)
        allocate.assert_not_called()
        checked = json.loads(output.getvalue())
        self.assertEqual(checked['core_capacity']['item_input_bytes'], 4*MIB)
        self.assertEqual(checked['requested_async_limits']['max_inflight'], 3)
        self.assertEqual(checked['requested_async_limits']['input_bytes'], 12*MIB)

    def test_rolling_feedback_uses_hard_owner_R_instead_of_initial_C(self):
        self.plan.update(profile=dict(mode='async', admission=dict(kind='resource_feedback')),
            prepared_batching=dict(policy='token_budget', max_rows=None, token_budget=100000),
            rolling_refill=True, async_limits={})
        checked = session.check_plan(self.plan)
        self.assertEqual(checked['core_execution']['initial_window'], 1)
        self.assertEqual(checked['async_limits']['max_inflight'], 11)
        self.assertEqual(checked['async_limits']['input_bytes'], 44*MIB)

    def test_other_baselines_and_sync_limits_reject_new_async_fields(self):
        for backend in ('lotus-native', 'semloom-static-reference', 'ray-data', 'daft'):
            with self.subTest(backend=backend), self.assertRaisesRegex(ValueError, 'semloom-core'):
                session.check_plan(dict(self.plan, backend=backend, lifecycle='isolated'))
        config = self.config()
        config.update(backend='semloom-local-model-core', core_profile=dict(mode='sync'))
        with self.assertRaisesRegex(ValueError, 'async'):
            movie.read_config(self.save(config))

    def test_small_budget_and_bad_capacity_reject_during_preflight(self):
        for changes in (dict(async_limits=dict(input_bytes=MIB)),
                        dict(async_limits=dict(max_inflight=8)),
                        dict(core_capacity=dict(item_input_bytes=0)),
                        dict(core_capacity=dict(input_bytes=4*MIB))):
            with self.subTest(changes=changes), self.assertRaises((ValueError, TypeError)):
                session.check_plan(dict(self.plan, **changes))
        plan = dict(self.plan, core_capacity=dict(item_result_bytes=1024))
        plan.pop('async_limits')
        with self.assertRaisesRegex(ValueError, 'result and control frames'):
            session.check_plan(plan)

    def test_driver_forwards_the_same_capacity_and_limits_to_factory(self):
        for lifecycle in ('resident', 'isolated'):
            with self.subTest(lifecycle=lifecycle):
                plan = dict(self.plan, lifecycle=lifecycle)
                fake_ray = SimpleNamespace(is_initialized=lambda: False, init=Mock())
                with patch.dict(sys.modules, {'ray':fake_ray}), patch.object(core, 'SemLoomRecordsCore') as allocate, \
                        patch.object(actors, 'AsyncRecordsActorOwner') as model, \
                        patch('psutil.Process', return_value=SimpleNamespace(pid=41, create_time=lambda: 10.)):
                    allocate.return_value.start.return_value = dict(pid=41, engine_uid='CPU-fixture')
                    driver = session._MovieDriver(plan, {}, self.root/lifecycle, plan['queries'][0])
                    driver.start()
                self.assertEqual(allocate.call_args.kwargs['capacity'].item_input_bytes, 4*MIB)
                limits = model.call_args.kwargs['limits']
                self.assertEqual((limits.max_inflight, limits.input_bytes), (3, 12*MIB))
                self.assertEqual(limits.result_bytes, 3*65536)

    def test_explicit_capacities_change_existing_admission_identity_without_mutating_config(self):
        from src.experiments.movie_matrix_support import run_record_admission
        from src.scheduling.runtime import calibration
        plan = deepcopy(self.plan)
        plan['profile'] = dict(mode='async', max_inflight=3, admission=dict(kind='resource_feedback'))
        plan['admission_cache'] = dict(cache_dir=str(self.root/'cache'), identity={
            name:dict(fixture=True) for name in ('hardware', 'model', 'generation', 'execution', 'workload', 'measurement')})
        plan['admission_cache']['identity']['measurement']['work_unit'] = 'rows'
        original = deepcopy(plan)
        resources = RecordsResources(**plan['resources'])
        engine = EngineSpec.from_dict(plan['engine'])
        resolved = core.resolve_core_profile(engine, resources, plan['profile'])
        identities = []
        for total in (65536, 131072):
            plan['async_limits'] = dict(item_result_bytes=65536, result_bytes=total)
            cache = movie._record_admission_cache(plan, resources=resources, resolved=resolved)
            with patch.object(calibration, 'run_admission_query',
                    return_value=dict(result=dict(query_report=dict(status='CPU-fixture')))) as select:
                run_record_admission(cache, resolved,
                    SimpleNamespace(manifest_path=Path(plan['queries'][0]['manifest'])),
                    MovieTask(1).method(plan['model_name']), GenerationSpec(), engine, resources,
                    Mock(side_effect=AssertionError('no query execution')),
                    model_ready=dict(engine_uid='CPU-capacity-cache', model_context=model_context_record(
                        engine,'CPU-capacity-cache',32768)))
            identities.append(select.call_args.args[0])
        self.assertNotEqual(calibration.identity_signature(identities[0]),
                            calibration.identity_signature(identities[1]))
        self.assertEqual(plan['admission_cache'], original['admission_cache'])
        self.assertEqual(identities[0]['execution']['declared']['records_capacity_v1']['IPC']['result_bytes'], 65536)

    def test_default_admission_identity_is_unchanged_when_capacity_fields_are_absent(self):
        plan = dict(self.plan, admission_cache=dict(identity=dict(execution=dict(fixture=True))))
        plan.pop('core_capacity')
        plan.pop('async_limits')
        self.assertIs(movie._record_admission_cache(plan, resources=None, resolved=None), plan['admission_cache'])


if __name__ == '__main__':
    unittest.main()
