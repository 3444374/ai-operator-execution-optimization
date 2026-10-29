"""Same production async actor/IPC with controlled CPU native completions."""
import json
import sys
import time
import unittest
from dataclasses import replace
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.execution_provider.adapters.lotus_records_core import CoreRecordsCapacity
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.execution_provider import test_lotus_record_local_query as query_fixture
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.semantic_methods import test_lotus_chain_composition as fixture
from tests.semantic_methods import test_lotus_chain_relation_handoff as relation


class ProgressionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = query_fixture.QueryOwnerTests(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.event_offset = 0

    def owner(self, *, progression='record', group=None, C=2, **options):
        engine = EngineSpec(str(self.fixture.config), sys.executable, sys.executable, max_model_len=4096)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=C, startup_timeout_s=8, query_timeout_s=12)
        return SemLoomRecordChainCore(engine, resources, execution='record_local_sections',
            progression=progression, progression_batch_rows=group,
            capacity=CoreRecordsCapacity(item_input_bytes=65536, item_result_bytes=65536), **options)

    def events(self):
        return self.fixture.events()[self.event_offset:]

    def until(self, stream, predicate):
        end = time.monotonic() + 5
        while not predicate():
            if time.monotonic() >= end:
                self.fail('controlled CPU query did not reach the requested boundary')
            stream._tick()

    def test_only_successor_submission_changes_and_Core_credit_is_released(self):
        chain, rows = fixture.chain_for('FM'), fixture.source_rows(size=2)
        normalized = []
        with ThreadedRuntime([]) as runtime, patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            for policy in ('record', 'batch_stage'):
                self.event_offset = len(self.fixture.events())
                gate = self.fixture.config.parent / (policy + '-release')
                config = self.fixture.config_for(chain, rows)
                config['responses']['step_0|occ-1']['wait_file'] = str(gate)
                self.fixture.config.write_text(json.dumps(config))
                owner = self.owner(progression=policy, group=2 if policy == 'batch_stage' else None)
                stream = owner.evaluate(iter(rows), chain, expected=rows, query_id=chain.query_id)
                self.addCleanup(owner.close, timeout=3)
                try:
                    self.until(stream, lambda: any(e.get('event') == 'end' and e.get('token') == 'step_0|occ-0' for e in self.events())
                               and any(e.get('event') == 'start' and e.get('token') == 'step_0|occ-1' for e in self.events()))
                    if policy == 'record':
                        self.until(stream, lambda: any(e.get('event') == 'start' and e.get('token') == 'step_1|occ-0' for e in self.events()))
                    else:
                        for _ in range(8): stream._tick()
                        self.assertFalse(any(e.get('token') == 'step_1|occ-0' for e in self.events()))
                        self.assertEqual(owner.core.capacity.usage().active_requests, 1)
                        self.assertEqual(owner.core.capacity.usage().held_tasks, 1)
                        self.assertEqual(stream.pool.used[0], 2)
                    gate.touch()
                    records = list(stream)
                    report = owner.close(timeout=3)
                    normalized.append(fixture.normalized(dict(records=records, calls=owner.calls, columns=stream.columns)))
                    self.assertEqual(report['status'], 'closed')
                    self.assertTrue(report['query_completed'])
                    self.assertFalse(any(report['core_usage'].values()))
                    self.assertEqual(stream.pool.used, (0, 0))
                    self.assertEqual(len({c['engine_uid'] for c in owner.calls}), 1)
                    self.assertEqual(report['progression']['policy'], policy)
                finally:
                    gate.touch(); owner.close(timeout=3)
            self.assertEqual(len(runtime.async_actors), 2)
        self.assertEqual(normalized[0], normalized[1])
        self.assertEqual(sum(e['event'] == 'construct' for e in self.fixture.events()), 2)

    def test_singleton_C1_degenerates_and_empty_source_allocates_no_model(self):
        chain, rows = fixture.chain_for('MM'), fixture.source_rows(size=2)
        self.fixture.config_for(chain, rows)
        normalized = []
        with ThreadedRuntime([]) as runtime, patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            for policy in ('record', 'batch_stage'):
                owner = self.owner(progression=policy, group=1 if policy == 'batch_stage' else None, C=1)
                try:
                    stream = owner.evaluate(iter(rows), chain, expected=rows, query_id=chain.query_id)
                    records = list(stream)
                    normalized.append(fixture.normalized(dict(records=records, calls=owner.calls, columns=stream.columns)))
                    self.assertLessEqual(owner.last_close_report['peak_core_usage']['active_requests'], 1)
                finally: owner.close(timeout=3)
            owner = self.owner(progression='batch_stage', group=2)
            try:
                stream = owner.evaluate(iter([]), chain, expected=[], query_id=chain.query_id)
                self.assertEqual(list(stream), [])
                self.assertTrue(owner.last_close_report['query_completed'])
                self.assertEqual(len(runtime.async_actors), 2)
            finally: owner.close(timeout=3)
        self.assertEqual(normalized[0], normalized[1])

    def test_later_stages_have_their_own_completion_barrier(self):
        chain, rows = fixture.chain_for('MMM'), fixture.source_rows(size=2)
        gate = self.fixture.config.parent / 'second-stage-release'
        config = self.fixture.config_for(chain, rows)
        config['responses']['step_1|occ-1']['wait_file'] = str(gate)
        self.fixture.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.owner(progression='batch_stage', group=2)
            try:
                stream = owner.evaluate(iter(rows), chain, expected=rows, query_id=chain.query_id)
                self.until(stream, lambda: any(e.get('event') == 'end' and e.get('token') == 'step_1|occ-0' for e in self.events())
                           and any(e.get('event') == 'start' and e.get('token') == 'step_1|occ-1' for e in self.events()))
                for _ in range(8): stream._tick()
                self.assertFalse(any(e.get('token', '').startswith('step_2|') for e in self.events()))
                gate.touch()
                self.assertEqual(len(list(stream)), 2)
                self.assertEqual(len(owner.calls), 6)
                self.assertTrue(owner.last_close_report['query_completed'])
                self.assertLessEqual(owner.last_close_report['peak_core_usage']['active_requests'], 2)
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_cancel_while_successor_waits_preserves_unknown_and_retires_compute(self):
        chain, rows = fixture.chain_for('MM'), fixture.source_rows(size=2)
        gate = self.fixture.config.parent / 'cancel-release'
        config = self.fixture.config_for(chain, rows)
        config['responses']['step_0|occ-1'].update(wait_file=str(gate), status='unknown')
        self.fixture.config.write_text(json.dumps(config)); cancelled = [False]
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.owner(progression='batch_stage', group=2)
            stream = owner.evaluate(iter(rows), chain, expected=rows, query_id=chain.query_id,
                                    cancelled=lambda: cancelled[0])
            try:
                self.until(stream, lambda: any(e.get('event') == 'end' and e.get('token') == 'step_0|occ-0' for e in self.events())
                           and any(e.get('event') == 'start' and e.get('token') == 'step_0|occ-1' for e in self.events()))
                for _ in range(8): stream._tick()
                self.assertFalse(any(e.get('token', '').startswith('step_1|') for e in self.events()))
                cancelled[0] = True
                with self.assertRaises(InterruptedError): next(stream)
                report = owner.close(timeout=3)
                self.assertEqual({c['algorithm_stage'] for c in owner.calls}, {'step_0'})
                self.assertTrue(any(c['status'] == 'unknown' for c in owner.calls))
                self.assertEqual(report['core_usage']['active_work'], 0)
                self.assertTrue(report['producer_retirement']['confirmed'])
                self.assertFalse(report['query_completed'])
                self.assertEqual(stream.pool.used, (0, 0))
            finally: gate.touch(); owner.close(timeout=3)

    def test_failure_stops_group_successors_and_late_cleanup_does_not_replay(self):
        chain, rows = fixture.chain_for('MM'), fixture.source_rows(size=2)
        config = self.fixture.config_for(chain, rows)
        config['responses']['step_0|occ-0']['status'] = 'failed'
        self.fixture.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.owner(progression='batch_stage', group=2)
            try:
                stream = owner.evaluate(iter(rows), chain, expected=rows, query_id=chain.query_id)
                with self.assertRaises(RuntimeError): list(stream)
                report = owner.close(timeout=3)
                self.assertEqual({c['algorithm_stage'] for c in owner.calls}, {'step_0'})
                before = [dict(c) for c in owner.calls]
                owner.close(timeout=3)
                self.assertEqual(owner.calls, before)
                self.assertFalse(report['query_completed'])
                self.assertFalse(any(report['core_usage'].values()))
                self.assertEqual(stream.pool.used, (0, 0))
            finally: owner.close(timeout=3)

    def test_tail_and_filtered_members_do_not_hold_a_nonexistent_successor(self):
        for kinds, count, all_false in [('FM', 3, False), ('FM', 3, True), ('MFM', 3, False)]:
            with self.subTest(kinds=kinds, all_false=all_false):
                chain, rows = fixture.chain_for(kinds), fixture.source_rows(size=count)
                config = self.fixture.config_for(chain, rows)
                if all_false:
                    for key, response in config['responses'].items():
                        if key.startswith('step_0|'): response['text'] = 'False'
                    self.fixture.config.write_text(json.dumps(config))
                with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
                    owner = self.owner(progression='batch_stage', group=2)
                    try:
                        stream = owner.evaluate(iter(rows), chain, expected=rows, query_id=chain.query_id)
                        result = list(stream)
                        self.assertEqual(len(result), 0 if all_false else 2)
                        self.assertEqual(len(owner.calls), count if all_false else (5 if kinds == 'FM' else 8))
                        self.assertEqual(owner.last_close_report['status'], 'closed')
                        self.assertEqual(stream.pool.used, (0, 0))
                    finally: owner.close(timeout=3)

    def test_incompatible_scope_rejects_before_source_or_model(self):
        for group in (0, True, 3):
            with self.subTest(group=group), self.assertRaises(ValueError):
                self.owner(progression='batch_stage', group=group)
        with self.assertRaises(ValueError): self.owner(group=2)
        with self.assertRaises(ValueError): self.owner(progression='typo')
        with self.assertRaisesRegex(ValueError, 'count admission'):
            self.owner(progression='batch_stage', group=2, token_budget=object())
        with self.assertRaisesRegex(ValueError, 'FIFO'):
            self.owner(progression='batch_stage', group=2, ready_order='round_robin')
        first = fixture.chain_for('FM')
        mixed_role = type(first)((first.stages[0], replace(first.stages[1], model_role='other')),
                                 query_id=first.query_id)
        for chain in (relation.relation_chain('FEM'), mixed_role):
            self.fixture.config_for(chain, [])
            with self.subTest(chain=chain), ThreadedRuntime([]) as runtime:
                owner = self.owner(progression='batch_stage', group=2)
                class Source:
                    def __iter__(self): raise AssertionError('unsupported source must not be entered')
                with self.assertRaisesRegex(ValueError, 'same-role Filter/Map'):
                    owner.evaluate(Source(), chain, expected=[], query_id=chain.query_id)
                self.assertEqual(runtime.async_actors, [])
                owner.close(timeout=3)

        with self.assertRaisesRegex(ValueError, 'requires record_local_sections'):
            SemLoomRecordChainCore(EngineSpec(str(self.fixture.config), sys.executable, sys.executable),
                RecordsResources(max_active_batches=1), progression='batch_stage', progression_batch_rows=1)

    def test_batch_must_fit_byte_capacity_as_well_as_row_count(self):
        from src.execution_provider.adapters.lotus_record_chain_fifo import LotusChainFIFO
        from src.scheduling.core.session_contract import SessionSpec, TaskProfile
        from src.semantic_methods.budget import MethodBudgetPool, MethodCapacity, row_reservation
        from src.semantic_methods.continuation import MethodLimits
        from tests.scheduling.test_incremental_session import setup

        core, old, backend, _ = setup(held_tasks=2, active_requests=2, active_work=2)
        old.close(); chain = fixture.chain_for('MM')
        session = core.open(SessionSpec('budget-check', 'group', 'fixture',
            task_profiles=tuple(TaskProfile(s.stage_id, 'fixture') for s in chain.stages)))
        limits = MethodLimits(16, 16, 16, 2)
        pool = MethodBudgetPool(MethodCapacity(2, row_reservation(limits)))
        budget = pool.allocate(pool.capacity)
        try:
            with self.assertRaisesRegex(ValueError, 'byte grant'):
                LotusChainFIFO(session, chain, limits, budget, completion_batch_rows=2)
            self.assertEqual(pool.used, (0, 0))
        finally:
            budget.close(); session.close()
