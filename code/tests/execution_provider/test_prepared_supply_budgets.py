"""Prepared handoffs with independent declared grants and CPU child replies."""
from dataclasses import asdict, replace
from itertools import product
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.execution_provider.adapters.lotus_records_core import create_records_core
from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity, RecordsCapacityBudget
from src.semantic_methods.budget import row_reservation
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.record_chain import LotusRecordChain, StageSpec
from src.semantic_methods.lotus.records import RecordsResources, RecordScan, identity
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_protocol import EngineSpec
from tests.execution_provider import test_lotus_async_token_batches as base
from tests.execution_provider.test_event_state_preparation import first_actor_ack_gate


ITEM = 65536


class IndependentSupplyTests(unittest.TestCase):
    setUp = base.AsyncTokenBatchTests.setUp

    def declarations(self, plan, *, held=9, requests=4, runs=5, state_rows=None):
        capacity = CoreRecordsCapacity(item_input_bytes=ITEM, item_result_bytes=ITEM)
        size = row_reservation(capacity.method_limits(len(plan.stages)))
        capacity = replace(capacity, budget=RecordsCapacityBudget(held, held*ITEM,
            held*ITEM, requests, runs, (runs if state_rows is None else state_rows)*size))
        limits = AsyncEngineLimits(max_inflight=requests, input_bytes=held*ITEM,
            item_input_bytes=ITEM, result_slots=held, result_bytes=held*ITEM,
            item_result_bytes=ITEM)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=requests, query_timeout_s=12, startup_timeout_s=8,
            model_rpc_workers=1)
        engine = EngineSpec(str(self.helper.config), sys.executable, sys.executable,
            max_model_len=4096)
        return engine, resources, capacity, limits

    def owner(self, plan, *, window=2, request_feedback=None, **options):
        engine, resources, capacity, limits = self.declarations(plan, **options)
        actors = AsyncRecordsActorOwner.for_prepared(engine, plan.stages[0].generation,
            resources, limits=limits, capacity=capacity)
        owner = SemLoomRecordChainCore(engine, resources, capacity=capacity,
            execution='record_local_sections', actor_owner=actors,
            prepared_batching=PreparedBatching('token_budget', None, 1000000),
            batching_tokenizer=base.CountingTokenizer(), rolling_refill=True,
            request_window=window, request_feedback=request_feedback)
        # Only already-declared fixtures use the direct-chain coordination seam.
        owner._interleaved_preparation = owner._event_driven_preparation = True
        return owner

    def until(self, stream, predicate):
        end = time.monotonic()+4
        while not predicate():
            stream._tick()
            self.assertLess(time.monotonic(), end)
            owner, usage = stream.owner, stream.owner.core.capacity.usage()
            limits = owner.core.capacity.limits
            for name in ('held_tasks', 'input_bytes', 'result_bytes', 'active_requests'):
                self.assertLessEqual(getattr(usage, name), getattr(limits, name))
            self.assertLessEqual(stream.pool.used[0], stream.pool.capacity.runs)
            self.assertLessEqual(stream.pool.used[1], stream.pool.capacity.bytes)

    def held_prefix(self, *, requests, runs, state_rows=None):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=12)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        expected = runs if state_rows is None else state_rows
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process), first_actor_ack_gate() as (entered, ack):
            owner = self.owner(plan, requests=requests, runs=runs, state_rows=state_rows)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.until(stream, lambda: owner.core.capacity.usage().active_requests == 1)
                self.assertTrue(entered.wait(2))
                self.until(stream, lambda: stream._phase_count == expected
                    and (state_rows is None or stream.pending is not None))
                self.assertEqual(stream.driver.active_rows, expected)
                self.assertEqual(stream.pool.used[0], expected)
                self.assertEqual(stream.can_prepare, state_rows is not None)
                self.assertEqual(owner._token_organizer.request_window, 2)
                self.assertEqual(owner._actors._data_rpc_progress().workers, 1)
                ack.set(); gate.touch()
                self.assertEqual(len(list(stream)), len(rows))
                self.assertEqual(len({v['call_id'] for v in owner.calls}), len(rows))
                self.assertFalse(any(asdict(owner.core.capacity.usage()).values()))
                self.assertEqual(owner.transport.retained_refs(), 0)
                self.assertEqual(stream.pool.used, (0, 0))
            finally:
                ack.set(); gate.touch(); owner.close(timeout=3)

    def test_preparation_can_use_more_Method_rows_than_requests(self):
        self.held_prefix(requests=2, runs=5)

    def test_preparation_stops_at_Method_rows_before_request_capacity(self):
        self.held_prefix(requests=8, runs=2)

    def test_Method_bytes_can_stop_preparation_before_its_row_count(self):
        self.held_prefix(requests=4, runs=7, state_rows=2)

    def test_single_held_task_keeps_RPC_observation_and_completes(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=5)
        self.helper.config_for(plan, rows)
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process):
            owner = self.owner(plan, held=1, requests=4, runs=3)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.assertTrue(owner.transport._observe_handoff)
                self.assertIsNotNone(owner._actors._rpc_subscription)
                self.assertEqual(owner.core.capacity.limits.held_tasks, 1)
                self.assertEqual(owner.core.capacity.limits.active_requests, 4)
                self.assertEqual(len(list(stream)), len(rows))
                self.assertEqual(owner.last_close_report['status'], 'closed')
                self.assertLessEqual(owner.last_close_report['peak_core_usage']['held_tasks'], 1)
                self.assertIsNone(owner._actors._rpc_subscription)
            finally:
                owner.close(timeout=3)

    def test_public_factory_accepts_None_with_complete_budget_or_typed_IPC(self):
        original = base.fixture.chain_for('F').stages[0]
        plan = LotusRecordChain((StageSpec('filter', original.method, original.generation),),
            query_id='ordinary-independent-supply')
        records = base.fixture.source_rows(size=7)
        self.helper.config_for(plan, records)
        engine, resources, capacity, limits = self.declarations(plan)
        rows = [dict(row_id=row.row_id, source_position=row.source_position,
            passage=row.values['passage']) for row in records]
        source = SimpleNamespace(preflight=lambda method: None,
            scan_spec=lambda method: RecordScan((('row_id','string'), ('source_position','int64'),
                ('passage','string')), ('passage',)),
            expected=lambda query: [identity(query, row['row_id'], row['source_position']) for row in rows],
            rows=lambda: iter(rows))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process):
            for shape, batching in product((capacity, replace(capacity, budget=None)),
                    (None, PreparedBatching('token_budget', None, 1000000))):
                with self.subTest(complete_budget=shape.budget is not None, token_packing=batching is not None):
                    ordinary = create_records_core(plan.stages[0].method, engine, plan.stages[0].generation,
                        replace(resources, max_active_batches=None), capacity=shape, async_limits=limits,
                        profile=dict(mode='async', max_inflight=2),
                        prepared_batching=batching, rolling_refill=True,
                        interleaved_preparation=True, event_driven_preparation=True)
                    try:
                        with patch.object(ordinary, '_tokenizer', return_value=base.CountingTokenizer()):
                            receipts = list(ordinary.evaluate(source, query_id=plan.query_id))
                        self.assertEqual(len(receipts), len(records))
                        self.assertEqual(len({row['call_id'] for row in receipts}), len(records))
                        self.assertIsNone(ordinary.execution_profile['declared_legacy_request_budget'])
                        report = ordinary.last_close_report
                        self.assertTrue(report['query_completed'])
                        self.assertTrue(report['async_token_batches']['event_driven_preparation'])
                        self.assertEqual(report['async_token_batches']['C_request'], 2)
                        self.assertEqual(report['async_token_batches']['request_limit'], 4)
                        self.assertFalse(any(report['core_usage'].values()))
                    finally:
                        ordinary.close(timeout=3)

    def test_public_factory_rejects_unusable_candidate_before_allocating_an_actor(self):
        plan = base.fixture.chain_for('F')
        engine, resources, capacity, limits = self.declarations(plan)
        for options, error in [
                (dict(interleaved_preparation=1), TypeError),
                (dict(event_driven_preparation=None), TypeError),
                (dict(interleaved_preparation=True), ValueError)]:
            with self.subTest(options=options), patch.object(AsyncRecordsActorOwner, '__init__') as allocate:
                with self.assertRaises(error):
                    create_records_core(plan.stages[0].method, engine, plan.stages[0].generation,
                        resources, **options)
                allocate.assert_not_called()

    def test_public_feedback_interleaves_preparation_progresses_results_and_reacts_to_memory(self):
        from src.observability.metrics import memory_pressure
        from src.semantic_methods.lotus.record_continuation import LotusChainProgram
        from src.execution_provider.adapters.lotus_chain_actor_backend import _SectionTransport
        from tests.execution_provider.test_records_request_feedback import CPUResourceProcess
        from tests.observability.memory_pressure_fixture import KernelMemoryFixture
        for event_driven, batching in product((False, True),
                (None, PreparedBatching('token_budget', None, 1000000))):
            with self.subTest(event_driven=event_driven, batching=batching):
                original = base.fixture.chain_for('F').stages[0]
                plan = LotusRecordChain((StageSpec('filter', original.method, original.generation),),
                    query_id='prepared-feedback-'+str(event_driven))
                records = base.fixture.source_rows(size=12)
                config = self.helper.config_for(plan, records)
                gate = Path(str(self.helper.config)+'.feedback-release')
                if gate.exists():gate.unlink()
                config['responses']['filter|occ-0']['wait_file'] = str(gate)
                self.helper.config.write_text(json.dumps(config))
                source = self.ordinary_source(records)
                engine, resources, capacity, limits = self.declarations(plan)
                prepared, submitted = [], []
                start, submit = LotusChainProgram.start, _SectionTransport.try_submit_batch
                def prepare(program, value):
                    result = start(program, value)
                    prepared.append(json.loads(value)['record']['source_position'])
                    return result
                def dispatch(transport, tasks, endpoint):
                    submitted.append((len(prepared), len(tasks)))
                    return submit(transport, tasks, endpoint)
                with KernelMemoryFixture() as kernel, base.ThreadedRuntime([]), \
                        patch.object(base.AsyncCoreModelActor, '_process_type', return_value=CPUResourceProcess), \
                        patch.object(memory_pressure, 'CgroupMemoryPressureSource', return_value=kernel.source), \
                        patch.object(LotusChainProgram, 'start', prepare), \
                        patch.object(_SectionTransport, 'try_submit_batch', dispatch):
                    kernel.source.clock = time.monotonic
                    owner = create_records_core(original.method, engine, original.generation, resources,
                        capacity=capacity, async_limits=limits,
                        profile=dict(mode='async', max_inflight='auto', admission=dict(kind='resource_feedback',
                            require_memory_feedback=True, poll_interval_s=.01, window_s=.04,
                            max_sample_age_s=.3, min_samples=1, measurement_windows=2)),
                        prepared_batching=batching, rolling_refill=True,
                        interleaved_preparation=True, event_driven_preparation=event_driven)
                    try:
                        with patch.object(owner, '_tokenizer', return_value=base.CountingTokenizer()):
                            result = owner.evaluate(source, query_id=plan.query_id)
                        stream = result.stream
                        self.until(stream, lambda: owner.owner._token_organizer.request_window > 1)
                        self.assertEqual(submitted[0], (1, 1))
                        self.until(stream, lambda: any(c['row_id'] != 'occ-0' for c in owner.owner.calls))
                        self.assertFalse(any(c['row_id'] == 'occ-0' for c in owner.owner.calls))
                        feedback = owner.owner._supply_feedback
                        kernel.write(kernel.leaf, high=1)
                        self.until(stream, lambda: feedback.controller.report()['reasons'].get('resource_pressure', 0))
                        self.assertEqual(owner.owner._token_organizer.request_window, 1)
                        self.assertGreater(owner.owner.core.capacity.usage().active_requests, 0)
                        gate.touch()
                        receipts = list(result)
                        self.assertEqual(len(receipts), len(records))
                        self.assertEqual(len({row['call_id'] for row in receipts}), len(records))
                        self.assertEqual(prepared, list(range(len(records))))
                        report = owner.last_close_report
                        self.assertTrue(report['query_completed'])
                        self.assertEqual(report['async_token_batches']['policy'],
                            None if batching is None else asdict(batching))
                        self.assertTrue(report['request_supply_feedback']['sampler_stopped'])
                        self.assertFalse(any(report['core_usage'].values()))
                        self.assertEqual(stream.pool.used, (0, 0))
                    finally:
                        gate.touch(); owner.close(timeout=3)

    def test_event_feedback_cancel_preserves_unknown_and_stops_the_sampler(self):
        from src.scheduling.submission_control.feedback_options import ResourceRequestConfig
        from tests.execution_provider.test_records_request_feedback import CPUResourceProcess
        plan, records = base.fixture.chain_for('MM'), base.fixture.source_rows(size=2)
        config = self.helper.config_for(plan, records, hold=['step_0|occ-0'])
        config['responses']['step_0|occ-0']['status'] = 'unknown'
        self.helper.config.write_text(json.dumps(config))
        cancelled = [False]
        options = ResourceRequestConfig(poll_interval_s=.01, window_s=.04,
            max_sample_age_s=.3, min_samples=1, measurement_windows=2)
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=CPUResourceProcess):
            owner = self.owner(plan, window=1, request_feedback=options)
            stream = owner.evaluate(iter(records), plan, expected=records,
                query_id=plan.query_id, cancelled=lambda: cancelled[0])
            try:
                self.until(stream, lambda: any(e['event'] == 'start'
                    and e.get('token') == 'step_0|occ-0' for e in self.helper.events()))
                cancelled[0] = True
                with self.assertRaises(InterruptedError):next(stream)
                report = owner.close(timeout=3)
                self.assertEqual(report['status'], 'closed')
                self.assertFalse(report['query_completed'])
                self.assertEqual(sum(c['status'] == 'unknown' for c in owner.calls), 1)
                self.assertFalse(any(c['algorithm_stage'] == 'step_1' and c['row_id'] == 'occ-0'
                    for c in owner.calls))
                self.assertTrue(report['request_supply_feedback']['sampler_stopped'])
                self.assertFalse(any(report['core_usage'].values()))
                self.assertEqual(stream.pool.used, (0, 0))
            finally:
                owner.close(timeout=3)

    @staticmethod
    def ordinary_source(records):
        rows = [dict(row_id=row.row_id, source_position=row.source_position,
            passage=row.values['passage']) for row in records]
        return SimpleNamespace(preflight=lambda method: None,
            scan_spec=lambda method: RecordScan((('row_id', 'string'), ('source_position', 'int64'),
                ('passage', 'string')), ('passage',)),
            expected=lambda query: [identity(query, row['row_id'], row['source_position']) for row in rows],
            rows=lambda: iter(rows))
