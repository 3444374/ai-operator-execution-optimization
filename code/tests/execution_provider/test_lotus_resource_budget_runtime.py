"""Existing Core/method/actor with CPU IPC fixture; no real Ray/model/GPU."""
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.experiments.test_records_resource_budget import capacity, ipc
from tests.execution_provider import test_lotus_async_token_batches as base
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime


class ResourceBudgetRuntimeTests(unittest.TestCase):
    setUp = base.AsyncTokenBatchTests.setUp

    def owner(self, plan, *, C=2, result_slots=1):
        value = capacity(len(plan.stages))
        engine = EngineSpec(str(self.helper.config), sys.executable, sys.executable, max_model_len=4096)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=value.budget.active_requests, query_timeout_s=12, startup_timeout_s=8)
        from src.execution_provider.adapters.lotus_records_core import resolve_records_async_limits
        limits = resolve_records_async_limits(resources, value, ipc(result_slots=result_slots))
        actors = AsyncRecordsActorOwner.for_prepared(engine, plan.stages[0].generation,
            resources, limits=limits, capacity=value)
        return SemLoomRecordChainCore(engine, resources, capacity=value, actor_owner=actors,
            execution='record_local_sections', rolling_refill=True, request_window=C,
            prepared_batching=PreparedBatching('token_budget', None, 100000),
            batching_tokenizer=base.CountingTokenizer())

    def tick_until(self, stream, predicate):
        deadline = time.monotonic()+5
        while not predicate():
            stream._tick()
            self.assertLess(time.monotonic(), deadline)
            usage = stream.owner.core.capacity.usage()
            budget = stream.owner.capacity.budget
            self.assertLessEqual(usage.held_tasks, budget.held_tasks)
            self.assertLessEqual(usage.input_bytes, budget.input_bytes)
            self.assertLessEqual(usage.result_bytes, budget.result_bytes)
            self.assertLessEqual(usage.active_requests, budget.active_requests)
            self.assertLessEqual(stream.pool.used[0], budget.method_runs)
            self.assertLessEqual(stream.pool.used[1], budget.method_bytes)

    def test_IPC_single_result_slot_and_state_bytes_below_row_slots_progress_to_EOF(self):
        plan, rows = base.fixture.chain_for('FM'), base.fixture.source_rows(size=6)
        config = self.helper.config_for(plan, rows)
        class Replies(base.fixture.ResponseFixture):
            def one(self, row, generation):
                value = super().one(row, generation)
                value['text'] = config['responses'][row['algorithm_stage']+'|'+row['row_id']]['text']
                return value
        expected = plan.run_batch(rows, Replies(plan))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: len(owner.calls) >= 3)
                values = list(stream)
                self.assertEqual(base.fixture.normalized(dict(records=values, calls=owner.calls, columns=stream.columns)),
                                 base.fixture.normalized(expected))
                self.assertTrue(owner.last_close_report['query_completed'])
                self.assertEqual(owner.last_close_report['status'], 'closed')
                self.assertEqual(len({c['call_id'] for c in owner.calls}), len(owner.calls))
                self.assertEqual(stream.pool.used, (0, 0))
                self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                self.assertLessEqual(owner.last_close_report['async_token_batches']['peak_requests'], 2)
                self.assertEqual(owner.last_close_report['resource_budget']['method_runs'], 7)
            finally:
                owner.close(timeout=3)

    def test_shrink_keeps_old_requests_and_stops_new_admission_until_below_C(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=4)
        config = self.helper.config_for(plan, rows)
        gates = [Path(str(self.helper.config)+'.gate'+str(i)) for i in (0, 1)]
        for index, gate in enumerate(gates):
            config['responses']['step_0|occ-'+str(index)]['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, result_slots=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: len([e for e in self.helper.events() if e['event']=='start']) == 2)
                owner._token_organizer.request_window = 1
                gates[1].touch()
                self.tick_until(stream, lambda: len(owner.calls) == 1)
                for _ in range(3):
                    stream._tick()
                starts = [e['token'] for e in self.helper.events() if e['event']=='start']
                self.assertEqual(set(starts), {'step_0|occ-0', 'step_0|occ-1'})
                self.assertEqual(owner._token_organizer.usage()[1], 1)
                gates[0].touch()
                self.assertEqual(len(list(stream)), 4)
                self.assertEqual(owner.last_close_report['status'], 'closed')
                self.assertEqual(owner.capacity.budget.active_requests, 4)
            finally:
                for gate in gates:
                    gate.touch()
                owner.close(timeout=3)

    def test_cancel_preserves_known_late_terminal_without_replaying_successor(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=3)
        self.helper.config_for(plan, rows, hold=['step_0|occ-0'])
        cancelled = [False]
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, result_slots=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id,
                cancelled=lambda:cancelled[0])
            try:
                self.tick_until(stream, lambda: any(c['row_id']=='occ-1' for c in owner.calls))
                cancelled[0] = True
                with self.assertRaises(InterruptedError):
                    next(stream)
                report = owner.close(timeout=3)
                self.assertFalse(report['query_completed'])
                self.assertTrue(any(c['row_id']=='occ-0' and c['status']=='succeeded' for c in owner.calls))
                self.assertFalse(any(c['row_id']=='occ-0' and c['algorithm_stage']=='step_1' for c in owner.calls))
            finally:
                owner.close(timeout=3)

    def test_explicit_unknown_does_not_invent_success_or_free_unconfirmed_work(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=2)
        config = self.helper.config_for(plan, rows)
        gate = Path(str(self.helper.config)+'.unknown')
        config['responses']['step_0|occ-0'].update(status='unknown', wait_file=str(gate))
        self.helper.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, result_slots=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: any(c['row_id']=='occ-1' and c['algorithm_stage']=='step_1' for c in owner.calls))
                gate.touch()
                with self.assertRaises(RuntimeError):
                    list(stream)
                report = owner.close(timeout=3)
                self.assertEqual(sum(c['row_id']=='occ-0' and c['status']=='unknown' for c in owner.calls), 1)
                self.assertFalse(any(c['row_id']=='occ-0' and c['algorithm_stage']=='step_1' for c in owner.calls))
                if report['core_usage']['held_tasks'] == 0:
                    self.assertTrue(report['producer_retirement']['confirmed'])
            finally:
                gate.touch()
                owner.close(timeout=3)

    def test_Extract_relation_barrier_preserves_semantics_with_independent_state_bytes(self):
        plan, rows = base.relation.relation_chain('MFEM'), base.fixture.source_rows(size=3)
        config = self.helper.config_for(plan, rows)
        class Replies(base.fixture.ResponseFixture):
            def one(self, row, generation):
                value = super().one(row, generation)
                value['text'] = config['responses'][row['algorithm_stage']+'|'+row['row_id']]['text']
                return value
        expected = base.relation.reference(plan, rows, Replies(plan))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan)
            try:
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                actual = dict(records=list(stream), calls=owner.calls, columns=stream.columns)
                self.assertEqual(base.fixture.normalized(actual), base.fixture.normalized(expected))
                self.assertTrue(all(p['source_exhausted'] for p in owner.last_close_report['phase_history']))
                self.assertEqual(owner.last_close_report['core_usage']['held_tasks'], 0)
            finally:
                owner.close(timeout=3)

    def test_empty_source_closes_without_starting_an_IPC_model_fixture(self):
        plan, rows = base.fixture.chain_for('M'), []
        self.helper.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type') as allocate:
            owner = self.owner(plan)
            try:
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                self.assertEqual(list(stream), [])
                self.assertTrue(owner.last_close_report['query_completed'])
                allocate.assert_not_called()
            finally:
                owner.close(timeout=3)


if __name__ == '__main__':
    unittest.main()
