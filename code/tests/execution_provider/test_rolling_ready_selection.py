"""Real Core and private CPU child, original methods; no model/Ray/GPU claims."""
from dataclasses import replace
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.execution_provider.adapters.records_capacity import (
    CoreRecordsCapacity, RecordsCapacityBudget, resolve_records_async_limits,
)
from src.semantic_methods.budget import row_reservation
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_protocol import EngineSpec
from src.scheduling.organization.ready_work import ReadyCost, WorkProgressPriority
from tests.execution_provider import test_lotus_async_token_batches as base


class ObservedSelector:
    def __init__(self, estimate):
        self.policy = WorkProgressPriority(estimate, mode='shortest_work', max_wait_s=30)
        self.choices = []

    def __call__(self, candidates, now_s):
        selected = self.policy(candidates, now_s)
        self.choices.append((candidates, selected))
        return selected


class RollingReadySelectionTests(unittest.TestCase):
    setUp = base.AsyncTokenBatchTests.setUp

    def owner(self, plan, *, selector=None, retained=4, C=1, B=None, supply_actor=True, **options):
        shape = CoreRecordsCapacity(item_input_bytes=65536, item_result_bytes=65536)
        budget = RecordsCapacityBudget(held_tasks=C, input_bytes=C*shape.item_input_bytes,
            result_bytes=C*shape.item_result_bytes, active_requests=C, method_runs=retained,
            method_bytes=retained*row_reservation(shape.method_limits(len(plan.stages))))
        capacity = replace(shape, budget=budget)
        engine = EngineSpec(str(self.helper.config), sys.executable, sys.executable, max_model_len=4096)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=C, query_timeout_s=12, startup_timeout_s=8)
        actors = (AsyncRecordsActorOwner.for_prepared(engine, plan.stages[0].generation,
            resources, capacity=capacity, limits=resolve_records_async_limits(resources, capacity))
            if supply_actor else None)
        return SemLoomRecordChainCore(engine, resources, capacity=capacity, actor_owner=actors,
            execution='record_local_sections', rolling_refill=True, request_window=C,
            prepared_batching=None if B is None else PreparedBatching('token_budget', None, B),
            batching_tokenizer=base.CountingTokenizer(), ready_selector=selector, **options)

    def tick_until(self, stream, predicate):
        deadline = time.monotonic() + 5
        while not predicate():
            stream._tick()
            self.assertLess(time.monotonic(), deadline)
            usage, budget = stream.owner.core.capacity.usage(), stream.owner.capacity.budget
            self.assertLessEqual(usage.active_requests, stream.owner.request_window)
            self.assertLessEqual(usage.held_tasks, budget.held_tasks)
            self.assertLessEqual(usage.input_bytes, budget.input_bytes)
            self.assertLessEqual(usage.result_bytes, budget.result_bytes)
            self.assertLessEqual(stream.pool.used[0], budget.method_runs)
            self.assertLessEqual(stream.pool.used[1], budget.method_bytes)

    def expected(self, plan, rows, config):
        class Replies(base.fixture.ResponseFixture):
            def one(self, row, generation):
                value = super().one(row, generation)
                value['text'] = config['responses'][row['algorithm_stage']+'|'+row['row_id']]['text']
                return value
        return base.fixture.normalized(plan.run_batch(rows, Replies(plan)))

    def test_real_preparation_window_selects_consumes_and_refills_without_reordering_results(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=7)
        config = self.helper.config_for(plan, rows)
        selector = ObservedSelector(lambda c: ReadyCost(100-c.sequence, 0, 'CPU declared row cost'))
        events = []
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process):
            owner = self.owner(plan, selector=selector, on_ready_event=events.append)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                actual = dict(records=list(stream), calls=owner.calls, columns=stream.columns)
                self.assertEqual(base.fixture.normalized(actual), self.expected(plan, rows, config))
                candidates, (selected, _) = selector.choices[0]
                self.assertEqual([c.sequence for c in candidates], [0, 1, 2, 3])
                self.assertEqual(selected, 3)
                offered = [e.row.sequence for e in events if e.action == 'Core_offer_accepted']
                self.assertEqual(offered[0], 3)
                self.assertEqual(sorted(offered), list(range(7)))
                self.assertTrue(any(4 in [c.sequence for c in candidates] and len(candidates) > 1
                                    for candidates, _ in selector.choices))
                self.assertLess(offered.index(4), offered.index(0))
                for candidates, (selected, _) in selector.choices:
                    self.assertLessEqual(len(candidates), 4)
                    self.assertEqual(selected, max(c.sequence for c in candidates))
                started = [e['token'] for e in self.helper.events() if e['event'] == 'start']
                self.assertEqual(started, ['step_0|occ-'+str(i) for i in offered])
                self.assertEqual([r.source_position for r in actual['records']], list(range(7)))
                self.assertTrue(owner.last_close_report['query_completed'])
                self.assertEqual(owner.last_close_report['async_token_batches']['peak_requests'], 1)
                self.assertEqual(stream.pool.used, (0, 0))
                self.assertFalse(any(owner.last_close_report['core_usage'].values()))
            finally:
                owner.close(timeout=3)

    def test_default_actor_uses_existing_IPC_resolution_for_the_declared_window(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=4)
        config = self.helper.config_for(plan, rows)
        selector = ObservedSelector(lambda c: ReadyCost(100-c.sequence, 0, 'CPU declared row cost'))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process):
            owner = self.owner(plan, selector=selector, supply_actor=False)
            try:
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                actual = dict(records=list(stream), calls=owner.calls, columns=stream.columns)
                self.assertEqual(base.fixture.normalized(actual), self.expected(plan, rows, config))
                self.assertEqual(owner._actors.limits, resolve_records_async_limits(owner.resources, owner.capacity))
                self.assertEqual(len(selector.choices[0][0]), 4)
                self.assertTrue(owner.last_close_report['query_completed'])
                self.assertFalse(any(owner.last_close_report['core_usage'].values()))
            finally:
                owner.close(timeout=3)

    def test_missing_cost_falls_back_to_default_FIFO_with_the_same_window_and_messages(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=6)
        config = self.helper.config_for(plan, rows)
        reference = None
        for selector in (None, ObservedSelector(lambda c: None)):
            with self.subTest(selector=selector), base.ThreadedRuntime([]), \
                    patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
                owner = self.owner(plan, selector=selector)
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                try:
                    actual = dict(records=list(stream), calls=owner.calls, columns=stream.columns)
                    self.assertEqual(base.fixture.normalized(actual), self.expected(plan, rows, config))
                    observed = [(c['call_id'], c['fixture_messages'], c['fixture_generation']) for c in owner.calls]
                    if reference is None:
                        reference = observed
                        self.assertNotIn('ready_selection', owner.last_close_report)
                    else:
                        self.assertEqual(observed, reference)
                        self.assertTrue(any(len(candidates) > 1 for candidates, _ in selector.choices))
                        self.assertTrue(all(reason == 'costs_unavailable_fifo' for _, (_, reason) in selector.choices))
                    self.assertFalse(any(owner.last_close_report['core_usage'].values()))
                finally:
                    owner.close(timeout=3)

    def test_token_packing_preserves_whole_requests_and_priority_with_multiple_candidates(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=4)
        config = self.helper.config_for(plan, rows)
        for B in (None, 1):
            selector = ObservedSelector(lambda c: ReadyCost(100-c.sequence, 0, 'CPU declared row cost'))
            events = []
            with self.subTest(B=B), base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor,
                    '_process_type', return_value=base.query_fixture.Process):
                owner = self.owner(plan, selector=selector, C=2, B=B, on_ready_event=events.append)
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                try:
                    actual = dict(records=list(stream), calls=owner.calls, columns=stream.columns)
                    self.assertEqual(base.fixture.normalized(actual), self.expected(plan, rows, config))
                    offered = [e.row.sequence for e in events if e.action == 'Core_offer_accepted']
                    self.assertEqual(offered[:2], [3, 2])
                    batches = owner.last_close_report['async_token_batches']['recent_batches']
                    if B is None:
                        self.assertGreater(max(b['members'] for b in batches), 1)
                    else:
                        self.assertTrue(all(b['members'] == 1 for b in batches))
                    self.assertEqual(len({c['call_id'] for c in owner.calls}), 4)
                finally:
                    owner.close(timeout=3)

    def test_interleaved_and_event_refill_rank_real_waiting_rows_and_legal_successors(self):
        plan, rows = base.fixture.chain_for('MFM'), base.fixture.source_rows(size=8)
        for mode in ('serial', 'interleaved', 'event'):
            config = self.helper.config_for(plan, rows)
            gate = Path(str(self.helper.config)+'.'+mode)
            config['responses']['step_0|occ-0']['wait_file'] = str(gate)
            self.helper.config.write_text(json.dumps(config))
            selector = ObservedSelector(lambda c: ReadyCost(1 if c.sequence == 0 else 100-c.sequence,
                                                           0, 'CPU declared row cost'))
            offset = len(self.helper.events())
            with self.subTest(mode=mode), base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor,
                    '_process_type', return_value=base.query_fixture.Process):
                owner = self.owner(plan, selector=selector, C=2, retained=5)
                # Exercise the existing coordinator modes; no new preparation loop.
                owner._interleaved_preparation = mode != 'serial'
                owner._event_driven_preparation = mode == 'event'
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                try:
                    self.tick_until(stream, lambda: len(owner.calls) == 20 and stream.source_eof)
                    self.assertFalse(any(e['event'] == 'end' and e['token'] == 'step_0|occ-0'
                                         for e in self.helper.events()[offset:]))
                    self.assertTrue(any(len(candidates) > 1 and selected != candidates[0].sequence
                                        for candidates, (selected, _) in selector.choices))
                    self.assertTrue(any(len({c.stage_index for c in candidates}) > 1
                                        for candidates, _ in selector.choices))
                    self.assertFalse(any(c['row_id'] == 'occ-1' and c['algorithm_stage'] == 'step_2'
                                         for c in owner.calls))
                    gate.touch()
                    actual = dict(records=list(stream), calls=owner.calls, columns=stream.columns)
                    self.assertEqual(base.fixture.normalized(actual), self.expected(plan, rows, config))
                    self.assertEqual(len({c['call_id'] for c in owner.calls}), 23)
                    self.assertEqual(stream.pool.used, (0, 0))
                    self.assertFalse(any(owner.last_close_report['core_usage'].values()))
                finally:
                    gate.touch()
                    owner.close(timeout=3)

    def test_wait_age_survives_Core_backpressure_until_the_request_can_be_accepted(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=4)
        config = self.helper.config_for(plan, rows)
        gate = Path(str(self.helper.config)+'.age')
        config['responses']['step_0|occ-3']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        clock, events = [0.0], []
        selector = ObservedSelector(lambda c: ReadyCost(100-c.sequence, 0, 'CPU declared row cost'))
        selector.policy.max_wait_s = 1
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process):
            owner = self.owner(plan, selector=selector, ready_clock=lambda: clock[0], on_ready_event=events.append)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: any(e['event'] == 'start' for e in self.helper.events()))
                clock[0] = 2
                stream._tick()
                self.assertTrue(any(e.action == 'Core_backpressure' and e.reason == 'aged_fifo'
                                    and e.row.sequence == 0 and e.ready_at == 0 for e in events))
                gate.touch()
                self.assertEqual(len(list(stream)), 4)
                offered = [e for e in events if e.action == 'Core_offer_accepted']
                self.assertEqual([e.row.sequence for e in offered[:2]], [3, 0])
                self.assertEqual(offered[1].ready_at, 0)
            finally:
                gate.touch()
                owner.close(timeout=3)

    def test_selector_error_is_not_swallowed_and_releases_pending_continuations(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=4)
        self.helper.config_for(plan, rows)
        def cost(candidate):
            if candidate.stage_index:
                raise ValueError('CPU cost provider failed')
            return ReadyCost(candidate.sequence+1, 0, 'CPU declared row cost')
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process):
            owner = self.owner(plan, selector=ObservedSelector(cost))
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                with self.assertRaisesRegex(ValueError, 'CPU cost provider failed'):
                    list(stream)
                self.assertEqual([(c['row_id'], c['algorithm_stage']) for c in owner.calls], [('occ-0', 'step_0')])
                report = owner.close(timeout=3)
                self.assertFalse(report['query_completed'])
                self.assertEqual(report['status'], 'closed')
                self.assertEqual(stream.pool.used, (0, 0))
                self.assertFalse(any(report['core_usage'].values()))
            finally:
                owner.close(timeout=3)

    def test_failed_or_unknown_member_never_runs_a_successor_or_replays_selected_work(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=4)
        for status in ('failed', 'unknown'):
            config = self.helper.config_for(plan, rows)
            config['responses']['step_0|occ-3']['status'] = status
            self.helper.config.write_text(json.dumps(config))
            selector = ObservedSelector(lambda c: ReadyCost(100-c.sequence, 0, 'CPU declared row cost'))
            with self.subTest(status=status), base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor,
                    '_process_type', return_value=base.query_fixture.Process):
                owner = self.owner(plan, selector=selector)
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                try:
                    with self.assertRaises(RuntimeError):
                        list(stream)
                    self.assertEqual([(c['row_id'], c['algorithm_stage'], c['status']) for c in owner.calls],
                                     [('occ-3', 'step_0', status)])
                    report = owner.close(timeout=3)
                    self.assertFalse(report['query_completed'])
                    self.assertEqual(stream.pool.used, (0, 0))
                    self.assertFalse(any(report['core_usage'].values()))
                finally:
                    owner.close(timeout=3)

    def test_cancel_retains_known_prefix_and_unknown_peer_without_consuming_waiting_rows(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=6)
        config = self.helper.config_for(plan, rows, hold=['step_0|occ-0'])
        config['responses']['step_0|occ-0']['status'] = 'unknown'
        self.helper.config.write_text(json.dumps(config))
        selector = ObservedSelector(lambda c: ReadyCost(1 if c.sequence == 0 else 100-c.sequence,
                                                       0, 'CPU declared row cost'))
        cancelled = [False]
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process):
            owner = self.owner(plan, selector=selector, C=2, retained=5)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id,
                                    cancelled=lambda: cancelled[0])
            try:
                self.tick_until(stream, lambda: any(c['algorithm_stage'] == 'step_1' for c in owner.calls))
                known = {c['call_id'] for c in owner.calls}
                self.assertTrue(any(len(candidates) > 1 and selected != candidates[0].sequence
                                    for candidates, (selected, _) in selector.choices))
                cancelled[0] = True
                with self.assertRaises(InterruptedError):
                    next(stream)
                report = owner.close(timeout=3)
                self.assertTrue(known.issubset({c['call_id'] for c in owner.calls}))
                self.assertEqual(sum(c['status'] == 'unknown' for c in owner.calls), 1)
                self.assertFalse(any(c['row_id'] == 'occ-0' and c['algorithm_stage'] == 'step_1'
                                     for c in owner.calls))
                self.assertEqual(len({c['call_id'] for c in owner.calls}), len(owner.calls))
                self.assertFalse(report['query_completed'])
                self.assertEqual(stream.pool.used, (0, 0))
                self.assertFalse(any(report['core_usage'].values()))
            finally:
                owner.close(timeout=3)
