"""Raw IPC JSON and typed grants share slot semantics without starting services."""
from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import asdict, replace
import io
import json
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.records_profile import resolve_records_runtime
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from tests.experiments import test_records_resource_budget as budget_fixture
from tests.execution_provider import test_records_unspecified_requests as fixtures


class RecordsIPCNullSlotTests(unittest.TestCase):
    def setUp(self):
        self.case = budget_fixture.ResourceBudgetTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def test_complete_JSON_null_slots_match_typed_grants_with_or_without_Core_budget(self):
        raw = dict(asdict(fixtures.typed_ipc()), result_slots=None, command_slots=None,
                   input_bytes=None, result_bytes=None)
        typed = AsyncEngineLimits(**raw)
        shape = core.CoreRecordsCapacity(item_input_bytes=budget_fixture.ITEM,
                                         item_result_bytes=budget_fixture.ITEM)
        for capacity in (shape, budget_fixture.capacity()):
            for request_count in (None, 3):
                with self.subTest(budget=capacity.budget, R=request_count):
                    self.case.resources = replace(self.case.resources, max_active_batches=request_count)
                    expected = resolve_records_runtime(self.case.engine, self.case.resources,
                        dict(mode='async', max_inflight=1), capacity=capacity, async_limits=typed)
                    actual = resolve_records_runtime(self.case.engine, self.case.resources,
                        dict(mode='async', max_inflight=1), capacity=capacity, async_limits=raw)
                    self.assertEqual(actual, expected)
                    owner = self.case.owner(1, capacity=capacity, async_limits=raw)
                    try:
                        self.assertEqual(owner._actors.limits, typed)
                        self.assertIsNone(owner._actors.model_actor)
                    finally:
                        owner.close(timeout=1)

    def test_partial_null_slots_are_omitted_overrides_and_do_not_supply_a_grant(self):
        self.case.resources = replace(self.case.resources, max_active_batches=4)
        shape = core.CoreRecordsCapacity(item_input_bytes=budget_fixture.ITEM,
                                         item_result_bytes=budget_fixture.ITEM)
        for capacity in (shape, budget_fixture.capacity()):
            with self.subTest(budget=capacity.budget):
                expected = resolve_records_runtime(self.case.engine, self.case.resources,
                    dict(mode='async', max_inflight=1), capacity=capacity)
                actual = resolve_records_runtime(self.case.engine, self.case.resources,
                    dict(mode='async', max_inflight=1), capacity=capacity,
                    async_limits=dict(result_slots=None, command_slots=None, input_bytes=None, result_bytes=None))
                self.assertEqual(actual, expected)
        with patch('src.execution_provider.adapters.lotus_records_async_actor.AsyncRecordsActorOwner') as actor:
            for overrides in (dict(input_bytes=0), dict(control_slots=None),
                              dict(feedback_interval_s=None), dict(result_slots=0), dict(command_slots=False)):
                with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                    self.case.owner(1, capacity=shape, async_limits=overrides)
            self.case.resources = replace(self.case.resources, max_active_batches=None)
            with self.assertRaisesRegex(ValueError, 'request capacity.*unresolved'):
                self.case.owner(1, capacity=shape,
                    async_limits=dict(result_slots=None, command_slots=None, input_bytes=None, result_bytes=None))
            actor.assert_not_called()

    def test_Movie_and_Session_preflight_accept_complete_JSON_null_slots_without_allocation(self):
        entry = fixtures.UnspecifiedEntryTests()
        entry.setUp()
        self.addCleanup(entry.doCleanups)
        raw = dict(asdict(fixtures.typed_ipc()), result_slots=None, command_slots=None,
                   input_bytes=None, result_bytes=None)
        typed = AsyncEngineLimits(**raw)
        with patch.object(core, 'create_records_core') as allocate:
            for omitted in (False, True):
                with self.subTest(omitted_R=omitted):
                    plan = deepcopy(entry.plan)
                    if omitted:
                        plan['resources'].pop('max_active_batches')
                    plan.update(async_limits=raw, profile=dict(mode='async', max_inflight='auto'))
                    checked = session.check_plan(plan)
                    self.assertIsNone(checked['core_execution']['declared_legacy_request_budget'])
                    self.assertEqual(checked['core_execution']['owner_max_inflight'], 4)
                    self.assertEqual(checked['async_limits'], asdict(typed))
                    value = dict(schema='semloom.movie_records_run.v1',
                        backend='semloom-local-model-core-async', query_id='q1', task='movie-q1',
                        method=asdict(budget_fixture.MovieTask(1).method(plan['model_name'])),
                        engine=plan['engine'], generation=plan['generation'], resources=plan['resources'],
                        core_profile=plan['profile'], core_capacity=plan['core_capacity'], async_limits=raw)
                    path = entry.root/'null-slot-config.json'
                    path.write_text(json.dumps(value))
                    output = io.StringIO()
                    with patch.object(movie, 'model_preflight', return_value=dict(inference_calls=0)), \
                            redirect_stdout(output):
                        self.assertEqual(movie.main(['check', '--config', str(path),
                            '--sembench-checkout', str(entry.root),
                            '--source-manifest', plan['queries'][0]['manifest']]), 0)
                    checked = json.loads(output.getvalue())
                    self.assertEqual(checked['resolved_async_limits_before_feedback'], asdict(typed))
                    self.assertEqual(checked['core_execution']['owner_max_inflight'], 4)
            allocate.assert_not_called()
