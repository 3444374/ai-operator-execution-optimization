"""CPU-only owner-turn progression; inference/Ray use existing fixtures.

The inherited rolling cases qualify the new opt-in path against the existing
per-member, cancellation and relation contracts. No model weights are loaded.
"""

from dataclasses import asdict, replace
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.execution_provider.adapters import lotus_record_chain_core as core
from src.semantic_methods.driver import MethodDriver
from src.semantic_methods.lotus.record_continuation import LotusChainProgram
from tests.execution_provider import test_lotus_async_token_batches as base
from tests.execution_provider import test_lotus_token_rolling_refill as rolling


class StatePreparationTests(rolling.RollingRefillTests):
    def owner(self, plan, **options):
        # Valid, already-declared CPU fixtures exercise the coordination seam.
        # Public direct-chain callers cannot enable the ordinary-only candidate.
        enabled = options.pop('interleaved_preparation', True)
        owner = super().owner(plan, **options)
        owner._interleaved_preparation = enabled
        return owner

    def measure_first_submit(self, enabled, *, prefilled=False, actions=3):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=22)
        self.helper.config_for(plan, rows)
        prepared, submitted = [], []
        start = LotusChainProgram.start
        dispatch = core._SectionTransport.try_submit_batch

        def record_prepare(program, value):
            result = start(program, value)
            prepared.append(json.loads(value)['record']['source_position'])
            return result

        def record_dispatch(transport, tasks, endpoint):
            submitted.append((len(prepared), tuple(t.key for t in tasks)))
            return dispatch(transport, tasks, endpoint)

        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process), \
                patch.object(LotusChainProgram, 'start', record_prepare), \
                patch.object(core._SectionTransport, 'try_submit_batch', record_dispatch):
            owner = self.owner(plan, R=11, interleaved_preparation=enabled)
            owner.capacity = replace(owner.capacity, step_actions=actions)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                if prefilled:
                    stream._tick()  # Open the normal phase/session.
                    for _ in range(11):
                        stream._fill()
                    self.assertEqual(stream.driver._submit(11), 11)
                self.assertEqual(len(list(stream)), len(rows))
                self.assertEqual(prepared, list(range(len(rows))))
                self.assertEqual(owner.last_close_report['status'], 'closed')
                return submitted
            finally:
                owner.close(timeout=3)

    def test_default_keeps_R_frontier_and_opt_in_does_not_use_Core_action_count(self):
        old = self.measure_first_submit(False)
        new = self.measure_first_submit(True)
        other_budget = self.measure_first_submit(True, actions=31)
        self.assertEqual(old[0][0], 11)
        self.assertEqual(new[0][0], 1)
        self.assertEqual(other_budget[0][0], 1)
        self.assertTrue(any(len(keys)>1 for _,keys in new), 'later packets must retain batching opportunities')
        self.assertEqual([k.sequence for _, keys in old for k in keys], list(range(22)))
        self.assertEqual([k.sequence for _, keys in new for k in keys], list(range(22)))

    def test_work_quantum_does_not_cap_an_already_ready_transport_batch(self):
        submitted = self.measure_first_submit(True, prefilled=True)
        self.assertEqual(len(submitted[0][1]), 11)
        self.assertGreater(len(submitted[0][1]), 3)

    def test_live_compute_accumulates_partial_ready_until_real_resident_capacity(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=22)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        packets = []
        submit = core._SectionTransport.try_submit_batch
        def observed(transport, tasks, endpoint):
            packets.append(tuple(task.key.sequence for task in tasks))
            return submit(transport, tasks, endpoint)
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process), \
                patch.object(core._SectionTransport, 'try_submit_batch', observed):
            owner = self.owner(plan, R=11)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: len(owner.calls)==21)
                self.assertEqual(len(packets[0]), 1)
                self.assertEqual(len(packets[1]), 10)
                self.assertLess(len(packets), len(rows))
                self.assertFalse(any(e['event']=='end' and e.get('token')=='step_0|occ-0'
                                     for e in self.helper.events()))
                gate.touch()
                self.assertEqual(len(list(stream)),22)
            finally:
                gate.touch()
                owner.close(timeout=3)

    def test_fast_Filter_successor_uses_original_comment_and_false_member_has_no_Map(self):
        plan, rows = base.fixture.chain_for('FM'), base.fixture.source_rows(size=3)
        rows = [replace(row, values=dict(row.values, passage='ORIGINAL_COMMENT_'+str(i)))
                for i,row in enumerate(rows)]
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=3)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: any(c['algorithm_stage']=='step_1'
                    and c['row_id']=='occ-2' and c['status']=='succeeded' for c in owner.calls))
                self.assertFalse(any(e['event']=='end' and e.get('token')=='step_0|occ-0'
                                     for e in self.helper.events()))
                mapped = next(c for c in owner.calls if c['algorithm_stage']=='step_1')
                self.assertIn('ORIGINAL_COMMENT_2', mapped['fixture_messages'])
                self.assertNotEqual(json.loads(mapped['fixture_messages'])[-1]['content'], 'True')
                gate.touch()
                self.assertEqual(len(list(stream)), 2)
                self.assertEqual(len(owner.calls), 5)
                self.assertFalse(any(c['algorithm_stage']=='step_1' and c['row_id']=='occ-1'
                                     for c in owner.calls))
            finally:
                gate.touch()
                owner.close(timeout=3)

    def test_failed_Filter_does_not_create_its_Map(self):
        plan, rows = base.fixture.chain_for('FM'), base.fixture.source_rows(size=2)
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-1']['status'] = 'failed'
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process):
            owner = self.owner(plan, R=2)
            try:
                with self.assertRaises(RuntimeError):
                    list(owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id))
                self.assertTrue(any(c['row_id']=='occ-1' and c['status']=='failed' for c in owner.calls))
                self.assertFalse(any(c['row_id']=='occ-1' and c['algorithm_stage']=='step_1'
                                     for c in owner.calls))
            finally:
                owner.close(timeout=3)

    def test_slow_method_consumer_retains_R_and_each_tick_still_advances_Core(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=7)
        self.helper.config_for(plan, rows)
        results = MethodDriver.results
        advance = MethodDriver.advance
        hold, calls = [True], []

        def delayed(driver, maximum=1):
            return () if hold[0] else results(driver, maximum)

        def observed(driver, maximum=1, **kwargs):
            calls.append(driver.active_rows)
            return advance(driver, maximum, **kwargs)

        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process), patch.object(MethodDriver,'results',delayed), \
                patch.object(MethodDriver,'advance',observed):
            owner = self.owner(plan,R=3)
            stream = owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: len(owner.calls)==3
                    and owner.core.capacity.usage().active_requests==0)
                before = len(calls)
                for _ in range(4):
                    stream._tick()
                self.assertEqual(len(calls)-before,4)
                self.assertEqual(stream.driver.active_rows,3)
                self.assertEqual(len(owner.calls),3)
                self.assertEqual(stream._phase_count,3)
                hold[0]=False
                self.assertEqual(len(list(stream)),7)
                self.assertEqual(stream.pool.used,(0,0))
            finally:
                hold[0]=False
                owner.close(timeout=3)

    def test_existing_full_relation_identity_preflight_still_rejects_before_actor_start(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=2)
        rows[1] = replace(rows[1], row_id=rows[0].row_id)
        with base.ThreadedRuntime([]) as runtime:
            owner = self.owner(plan,R=2)
            try:
                with self.assertRaisesRegex(ValueError,'globally unique'):
                    owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
                self.assertFalse(runtime.async_actors)
                self.assertEqual(owner.calls,[])
            finally:
                owner.close(timeout=3)

    def test_illegal_first_method_input_has_no_backend_submission(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=3)
        rows[0] = replace(rows[0], values=dict(rows[0].values, passage=42))
        self.helper.config_for(plan,rows)
        submitted=[]
        original=core._SectionTransport.try_submit_batch

        def observed(transport,tasks,endpoint):
            submitted.extend(t.key for t in tasks)
            return original(transport,tasks,endpoint)

        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor,'_process_type',
                return_value=base.query_fixture.Process), patch.object(core._SectionTransport,'try_submit_batch',observed):
            owner=self.owner(plan,R=3)
            try:
                with self.assertRaisesRegex(ValueError,'present text input'):
                    list(owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id))
                self.assertFalse(submitted)
                self.assertFalse(any(e['event']=='start' for e in self.helper.events()))
            finally:
                owner.close(timeout=3)

    def test_public_direct_chain_late_input_error_keeps_original_accepted_prefix(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=7)
        rows[4] = replace(rows[4], values=dict(rows[4].values, passage=42))
        self.helper.config_for(plan, rows)
        submitted = []
        original = core._SectionTransport.try_submit_batch
        def observed(transport, tasks, endpoint):
            submitted.extend(t.key.sequence for t in tasks)
            return original(transport, tasks, endpoint)
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor, '_process_type',
                return_value=base.query_fixture.Process), \
                patch.object(core._SectionTransport, 'try_submit_batch', observed):
            owner = rolling.RollingRefillTests.owner(self, plan, R=7)
            owner.capacity = replace(owner.capacity, step_actions=2)
            try:
                with self.assertRaisesRegex(ValueError, 'present text input'):
                    list(owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id))
                self.assertFalse(submitted)
                self.assertFalse(owner._interleaved_preparation)
            finally:
                owner.close(timeout=3)

    def test_ordinary_source_still_checks_late_illegal_payload_before_creating_owner(self):
        from src.execution_provider.adapters.lotus_records_batches import PreparedRecordsCore
        from src.semantic_methods.lotus.records import RecordScan, identity

        plan = base.fixture.chain_for('F')
        rows = [dict(row_id='occ-'+str(i), source_position=i, passage='comment') for i in range(7)]
        rows[-1]['passage'] = 42
        source = SimpleNamespace(preflight=lambda method: None,
            scan_spec=lambda method: RecordScan((('row_id', 'string'), ('source_position', 'int64'),
                                                ('passage', 'string')), ('passage',)),
            expected=lambda query: [identity(query, row['row_id'], row['source_position']) for row in rows],
            rows=lambda: iter(rows))
        ordinary = PreparedRecordsCore(plan.stages[0].method, None, plan.stages[0].generation,
            None, {}, dict(rolling_refill=True), None, interleaved_preparation=True)
        with patch.object(ordinary, '_new_owner') as create:
            with self.assertRaisesRegex(ValueError, 'payload columns must retain text'):
                ordinary.evaluate(source, query_id=plan.query_id)
            create.assert_not_called()
            self.assertIsNone(ordinary.owner)

    def test_prevalidated_ordinary_entry_enables_candidate_and_delivers_all_receipts(self):
        from src.execution_provider.adapters.lotus_records_batches import PreparedRecordsCore
        from src.execution_provider.adapters.lotus_records_core import resolve_core_profile, resolve_record_batching
        from src.semantic_methods.lotus.record_chain import LotusRecordChain, StageSpec
        from src.semantic_methods.lotus.records import RecordScan, identity
        method = base.fixture.chain_for('F').stages[0]
        plan = LotusRecordChain((StageSpec('filter',method.method,method.generation),),query_id='ordinary-state')
        records = base.fixture.source_rows(size=7)
        self.helper.config_for(plan, records)
        rows = [dict(row_id=row.row_id,source_position=row.source_position,passage=row.values['passage'])
                for row in records]
        source = SimpleNamespace(preflight=lambda method: None,
            scan_spec=lambda method: RecordScan((('row_id','string'),('source_position','int64'),
                                                ('passage','string')),('passage',)),
            expected=lambda query: [identity(query,row['row_id'],row['source_position']) for row in rows],
            rows=lambda: iter(rows))
        with base.ThreadedRuntime([]), patch.object(base.AsyncCoreModelActor,'_process_type',
                return_value=base.query_fixture.Process):
            template = self.owner(plan,R=4,interleaved_preparation=False)
            profile = resolve_core_profile(template.engine,template.resources,dict(mode='async'))
            batching = resolve_record_batching(profile,base.PreparedBatching('token_budget',None,512),
                                              rolling_refill=True)
            ordinary = PreparedRecordsCore(method.method,template.engine,method.generation,
                template.resources,profile,batching,None,
                capacity=template.capacity,interleaved_preparation=True)
            try:
                with patch.object(ordinary,'_tokenizer',return_value=base.CountingTokenizer()):
                    receipts = list(ordinary.evaluate(source,query_id=plan.query_id))
                self.assertEqual(len(receipts),len(records))
                self.assertTrue(ordinary.owner._interleaved_preparation)
                self.assertTrue(ordinary.owner.last_close_report['async_token_batches']['interleaved_preparation'])
                self.assertFalse(any(asdict(ordinary.owner.core.capacity.usage()).values()))
            finally:
                ordinary.close(timeout=3)
                template.close(timeout=3)

    def test_opt_in_rejects_non_boolean_and_non_rolling_modes(self):
        from src.execution_provider.adapters.lotus_records_batches import PreparedRecordsCore
        plan=base.fixture.chain_for('M')
        method=base.fixture.chain_for('F').stages[0].method
        for enabled in (1,None,'true'):
            with self.subTest(enabled=enabled),self.assertRaises(TypeError):
                PreparedRecordsCore(method,None,plan.stages[0].generation,None,{},
                    dict(rolling_refill=True),None,interleaved_preparation=enabled)
        with self.assertRaisesRegex(ValueError,'fixed rolling'):
            PreparedRecordsCore(method,None,plan.stages[0].generation,None,{},
                {},None,interleaved_preparation=True)
        with self.assertRaises(TypeError):
            base.AsyncTokenBatchTests.owner(self,plan,interleaved_preparation=True)


if __name__=='__main__':
    import unittest
    unittest.main()
