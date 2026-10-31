"""Request-bounded token transport with CPU replies; no model or GPU execution."""
import json
from pathlib import Path
import time
import unittest
from unittest.mock import patch

from tests.execution_provider import test_lotus_async_token_batches as base


class RollingRefillTests(unittest.TestCase):
    setUp = base.AsyncTokenBatchTests.setUp

    def owner(self, plan, **options):
        return base.AsyncTokenBatchTests.owner(self, plan, C=None, rolling_refill=True, **options)

    def tick_until(self, stream, predicate):
        deadline = time.monotonic()+5
        while not predicate():
            stream._tick()
            self.assertLess(time.monotonic(), deadline)
            owner = stream.owner
            usage = owner.core.capacity.usage()
            self.assertLessEqual(usage.active_requests, owner.resources.max_active_batches)
            self.assertLessEqual(usage.held_tasks, owner.resources.max_active_batches)
            self.assertLessEqual(stream.driver.active_rows, owner.resources.max_active_batches)
            self.assertLessEqual(usage.input_bytes, owner.core.capacity.limits.input_bytes)
            self.assertLessEqual(usage.result_bytes, owner.core.capacity.limits.result_bytes)
            self.assertLessEqual(len(owner.transport._bulk), owner.resources.max_active_batches)

    def test_fast_final_refills_new_source_rows_while_slow_peer_is_unfinished(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=6)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: len(owner.calls)==5)
                self.assertFalse(any(e['event']=='end' and e['token']=='step_0|occ-0' for e in self.helper.events()))
                self.assertEqual(len(owner.calls), 5)
                self.assertEqual(owner._token_organizer.peak_requests, 2)
                self.assertEqual(owner._token_organizer.peak_batches, 2)
                gate.touch()
                self.assertEqual(len(list(stream)), 6)
                report = owner.last_close_report
                self.assertTrue(report['query_completed'])
                self.assertEqual(report['core_usage']['held_tasks'], 0)
                self.assertEqual(stream.pool.used, (0, 0))
                self.assertEqual(report['async_token_batches']['C_request'], 2)
                self.assertIsNone(report['async_token_batches']['concurrency'])
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_fast_legal_successor_runs_before_slow_same_batch_peer(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=2)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: any(c['algorithm_stage']=='step_1' and c['row_id']=='occ-1'
                    and c['status']=='succeeded' for c in owner.calls) and owner.core.capacity.usage().active_requests==1)
                self.assertFalse(any(e['event']=='end' and e['token']=='step_0|occ-0' for e in self.helper.events()))
                self.assertEqual(owner._token_organizer.usage(), (1, 1))
                gate.touch()
                self.assertEqual(len(list(stream)), 2)
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_Filter_Map_and_Extract_barriers_preserve_original_relations(self):
        plan, rows = base.relation.relation_chain('MFEM'), base.fixture.source_rows(size=3)
        config = self.helper.config_for(plan, rows)
        class Replies(base.fixture.ResponseFixture):
            def one(self, row, generation):
                value = super().one(row, generation)
                value['text'] = config['responses'][row['algorithm_stage']+'|'+row['row_id']]['text']
                return value
        expected = base.relation.reference(plan, rows, Replies(plan))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                actual = dict(records=list(stream), calls=owner.calls, columns=stream.columns)
                self.assertEqual(base.fixture.normalized(actual), base.fixture.normalized(expected))
                self.assertTrue(all(p['source_exhausted'] and p['verified_success'] for p in owner.last_close_report['phase_history']))
                self.assertEqual(owner.last_close_report['core_usage']['held_tasks'], 0)
            finally:
                owner.close(timeout=3)

    def test_cancel_preserves_known_prefix_unknown_peer_and_no_false_successor(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=2)
        config = self.helper.config_for(plan, rows, hold=['step_0|occ-0'])
        config['responses']['step_0|occ-0']['status'] = 'unknown'
        self.helper.config.write_text(json.dumps(config))
        cancelled = [False]
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id, cancelled=lambda: cancelled[0])
            try:
                self.tick_until(stream, lambda: any(e['event']=='end' and e['token']=='step_1|occ-1' for e in self.helper.events()))
                cancelled[0] = True
                with self.assertRaises(InterruptedError): next(stream)
                report = owner.close(timeout=3)
                self.assertEqual(report['status'], 'closed')
                self.assertEqual(sum(c['status']=='unknown' for c in owner.calls), 1)
                self.assertFalse(any(c['algorithm_stage']=='step_1' and c['row_id']=='occ-0' for c in owner.calls))
                self.assertEqual(report['core_usage']['held_tasks'], 0)
                self.assertEqual(stream.pool.used, (0, 0))
            finally:
                owner.close(timeout=3)

    def test_unknown_member_failure_does_not_replay_or_invent_success(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=2)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0'].update(status='unknown', wait_file=str(gate))
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: any(e['event']=='end' and e['token']=='step_1|occ-1' for e in self.helper.events()))
                gate.touch()
                with self.assertRaises(RuntimeError): list(stream)
                self.assertEqual(sum(c['status']=='unknown' for c in owner.calls), 1)
                self.assertEqual(sum(c['status']=='succeeded' for c in owner.calls), 2)
                self.assertEqual(owner.close(timeout=3)['core_usage']['held_tasks'], 0)
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_oversize_singletons_and_finite_tail_do_not_wait_to_fill_B(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=5)
        self.helper.config_for(plan, rows)
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=2, B=1)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.assertEqual(len(list(stream)), 5)
                stats = owner.last_close_report['async_token_batches']
                self.assertEqual(stats['selected_batches'], 5)
                self.assertTrue(all(b['members']==1 for b in stats['recent_batches']))
                self.assertLessEqual(stats['peak_requests'], 2)
            finally:
                owner.close(timeout=3)

    def test_empty_query_never_allocates_model(self):
        plan = base.relation.relation_chain('EM')
        self.helper.config_for(plan, [])
        with base.ThreadedRuntime([]) as runtime:
            owner = self.owner(plan)
            stream = owner.evaluate(iter([]), plan, expected=[], query_id=plan.query_id)
            self.assertEqual(list(stream), [])
            self.assertEqual(runtime.async_actors, [])

    def test_confirmed_member_failure_keeps_failure_and_blocks_its_successor(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=2)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        config['responses']['step_0|occ-1']['status'] = 'failed'
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                with self.assertRaises(RuntimeError): list(stream)
                failed = [c for c in owner.calls if c['row_id']=='occ-1']
                self.assertEqual(len(failed), 1)
                self.assertEqual(failed[0]['status'], 'failed')
                self.assertTrue(failed[0]['engine_terminal_confirmed'])
                self.assertFalse(any(c['algorithm_stage']=='step_1' and c['row_id']=='occ-1' for c in owner.calls))
                self.assertEqual(owner.close(timeout=3)['core_usage']['held_tasks'], 0)
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_request_window_one_blocks_new_rows_until_the_owned_member_finishes(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=3)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=1)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: any(e['event']=='start' and e['token']=='step_0|occ-0' for e in self.helper.events()))
                for _ in range(4): stream._tick()
                self.assertFalse(any(e.get('token')=='step_0|occ-1' for e in self.helper.events()))
                self.assertEqual(stream.driver.active_rows, 1)
                gate.touch()
                self.assertEqual(len(list(stream)), 3)
                self.assertEqual(owner.last_close_report['async_token_batches']['peak_requests'], 1)
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_rolling_mode_rejects_batch_window_and_non_boolean_flags(self):
        plan = base.fixture.chain_for('M')
        for options in (dict(C=1, rolling_refill=True), dict(C=None, rolling_refill=1),
                        dict(C=None, rolling_refill=None)):
            with self.subTest(options=options), self.assertRaises(ValueError):
                base.AsyncTokenBatchTests.owner(self, plan, **options)
