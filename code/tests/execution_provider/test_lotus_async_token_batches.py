"""Real Core, original methods, one actor/CPU child and bulk sockets; no GPU claim."""
from dataclasses import replace
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_core import CoreRecordsCapacity
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.execution_provider import test_lotus_record_local_query as query_fixture
from tests.execution_provider.chain_tokenizer_fixture import tokenizer
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.semantic_methods import test_lotus_chain_composition as fixture
from tests.semantic_methods import test_lotus_chain_relation_handoff as relation


class CountingTokenizer:
    def __init__(self):
        self.inner, self.calls = tokenizer(), 0
    def apply_chat_template(self, *args, **kwargs):
        self.calls += 1
        return self.inner.apply_chat_template(*args, **kwargs)


class AsyncTokenBatchTests(unittest.TestCase):
    def setUp(self):
        self.helper = query_fixture.QueryOwnerTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)

    def owner(self, plan, *, R=4, C=2, B=1000000, S=2, **options):
        engine = EngineSpec(str(self.helper.config), sys.executable, sys.executable,
            max_model_len=4096, max_num_seqs=S)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=R, query_timeout_s=12, startup_timeout_s=8)
        actors = AsyncRecordsActorOwner.for_prepared(engine, plan.stages[0].generation, resources)
        self.tokens = CountingTokenizer()
        return SemLoomRecordChainCore(engine, resources, capacity=CoreRecordsCapacity(
            item_input_bytes=65536, item_result_bytes=65536), execution='record_local_sections',
            actor_owner=actors, prepared_batching=PreparedBatching('token_budget', None, B),
            batch_concurrency=C, batching_tokenizer=self.tokens, **options)

    def test_one_bulk_batch_can_exceed_engine_sequence_capacity_and_read_chunks_do_not_seal(self):
        for chunk in (1, 2, 7, 64):
            with self.subTest(chunk=chunk):
                plan, rows = fixture.chain_for('M'), fixture.source_rows(size=80)
                self.helper.config_for(plan, rows)
                def source():
                    for start in range(0, len(rows), chunk):
                        yield from rows[start:start+chunk]
                with ThreadedRuntime([]) as runtime, patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
                    owner = self.owner(plan, R=80, C=2, S=4)
                    stream = owner.evaluate(source(), plan, expected=rows, query_id=plan.query_id)
                    try:
                        values = list(stream)
                        report = owner.last_close_report
                        self.assertEqual(len(values), 80)
                        stats = report['async_token_batches']
                        self.assertEqual(stats['recent_batches'][0]['members'], 80)
                        self.assertEqual(stats['selected_batches'], 1)
                        self.assertEqual(stats['data_batch_rpcs'], 1)
                        self.assertEqual(report['model_owner']['engine_close_report']['ipc_close']['submitted_batch_frames'], 1)
                        self.assertEqual(self.tokens.calls, 80)
                        self.assertEqual(len(runtime.async_actors), 1)
                        self.assertEqual(len({v['engine_uid'] for v in owner.calls}), 1)
                        self.assertEqual(report['status'], 'closed')
                        self.assertEqual(report['core_usage']['held_tasks'], 0)
                    finally:
                        owner.close(timeout=3)

    def test_fast_member_successor_runs_before_its_slow_batch_peer(self):
        plan, rows = fixture.chain_for('MM'), fixture.source_rows(size=2)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.owner(plan, R=2, C=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                deadline = time.monotonic()+5
                while not any(e['event']=='end' and e['token']=='step_1|occ-1' for e in self.helper.events()):
                    stream._tick()
                    self.assertLess(time.monotonic(), deadline, owner.calls)
                self.assertFalse(any(e['event']=='end' and e['token']=='step_0|occ-0' for e in self.helper.events()))
                self.assertEqual(owner._token_organizer.peak_batches, 2)
                while owner.core.capacity.usage().active_requests > 1:
                    stream._tick()
                    self.assertLess(time.monotonic(), deadline)
                self.assertEqual(owner.core.capacity.usage().active_requests, 1)
                for context in tuple(owner.transport._bulk.values()):
                    self.assertFalse(context['tasks'], 'handed-off task payloads must leave the batch context')
                    self.assertFalse(context['values'], 'decoded payloads must leave the batch context after data handoff')
                    self.assertFalse(any(f.done() for f in context['futures'].values()),
                                     'transferred member results must not wait for their slow peer')
                gate.touch()
                self.assertEqual(len(list(stream)), 2)
                self.assertEqual(self.tokens.calls, 4)
                self.assertTrue(owner.last_close_report['query_completed'])
            finally:
                gate.touch()
                owner.close(timeout=3)

    def test_mixed_chain_outputs_match_original_reference_and_preserve_Extract_barriers(self):
        plan, rows = relation.relation_chain('MFEM'), fixture.source_rows(size=3)
        config = self.helper.config_for(plan, rows)
        class Replies(fixture.ResponseFixture):
            def one(self, row, generation):
                value = super().one(row, generation)
                value['text'] = config['responses'][row['algorithm_stage']+'|'+row['row_id']]['text']
                return value
        replay = Replies(plan)
        expected = relation.reference(plan, rows, replay)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.owner(plan, R=4, C=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                actual = dict(records=list(stream), calls=owner.calls, columns=stream.columns)
                self.assertEqual(fixture.normalized(actual), fixture.normalized(expected))
                by_id = {v['call_id']: v for v in owner.calls}
                for call_id, (messages, generation) in replay.requests.items():
                    self.assertEqual(by_id[call_id]['fixture_messages'], messages)
                    self.assertEqual(by_id[call_id]['fixture_generation']['max_tokens'], generation['max_tokens'])
                self.assertTrue(all(p['source_exhausted'] and p['verified_success']
                                    for p in owner.last_close_report['phase_history']))
            finally:
                owner.close(timeout=3)

    def test_batch_capacity_one_keeps_fast_receipt_but_waits_for_a_supply_slot(self):
        plan, rows = fixture.chain_for('MM'), fixture.source_rows(size=2)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.owner(plan, R=2, C=1)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                deadline = time.monotonic()+5
                while not any(r.phase=='QUEUED' and r.task.profile_name=='step_1'
                              for r in owner.core.capacity.records.values()):
                    stream._tick()
                    self.assertLess(time.monotonic(), deadline)
                self.assertTrue(any(v['algorithm_stage']=='step_0' and v['row_id']=='occ-1'
                                    and v['status']=='succeeded' for v in owner.calls))
                self.assertFalse(any(e['event']=='start' and e['token']=='step_1|occ-1' for e in self.helper.events()))
                self.assertEqual(owner._token_organizer.usage()[0], 1)
                gate.touch()
                self.assertEqual(len(list(stream)), 2)
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_oversize_whole_requests_form_many_concurrent_batches(self):
        plan, rows = fixture.chain_for('M'), fixture.source_rows(size=4)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.owner(plan, R=4, C=2, B=1, S=1)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                deadline = time.monotonic()+5
                while not any(e['event']=='end' and e['token']=='step_0|occ-3' for e in self.helper.events()):
                    stream._tick()
                    self.assertLess(time.monotonic(), deadline)
                self.assertEqual(owner._token_organizer.peak_batches, 2)
                self.assertFalse(any(e['event']=='end' and e['token']=='step_0|occ-0' for e in self.helper.events()))
                gate.touch()
                self.assertEqual(len(list(stream)), 4)
                stats = owner.last_close_report['async_token_batches']
                self.assertEqual(stats['data_batch_rpcs'], 4)
                self.assertTrue(all(b['members']==1 and b['tokens']>1 for b in stats['recent_batches']))
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_cancel_keeps_known_prefix_unknown_peer_and_unsubmitted_successor_distinct(self):
        plan, rows = fixture.chain_for('MM'), fixture.source_rows(size=2)
        config = self.helper.config_for(plan, rows, hold=['step_0|occ-0'])
        config['responses']['step_0|occ-0']['status'] = 'unknown'
        self.helper.config.write_text(json.dumps(config))
        cancelled = [False]
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.owner(plan, R=2, C=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id, cancelled=lambda: cancelled[0])
            try:
                deadline = time.monotonic()+5
                while not any(e['event']=='end' and e['token']=='step_1|occ-1' for e in self.helper.events()):
                    stream._tick()
                    self.assertLess(time.monotonic(), deadline)
                cancelled[0] = True
                with self.assertRaises(InterruptedError): next(stream)
                report = owner.close(timeout=3)
                self.assertEqual(report['status'], 'closed', {key: report[key] for key in (
                    'model_owner', 'transport_closed', 'core_usage', 'cleanup_errors')})
                self.assertFalse(report['query_completed'])
                self.assertEqual(sum(v['status']=='unknown' for v in owner.calls), 1)
                self.assertFalse(any(v['algorithm_stage']=='step_1' and v['row_id']=='occ-0' for v in owner.calls))
                self.assertEqual(report['core_usage']['held_tasks'], 0)
                self.assertEqual(stream.pool.used, (0, 0))
                self.assertEqual(owner.transport.retained_refs(), 0)
            finally:
                owner.close(timeout=3)

    def test_empty_query_has_no_batch_or_model_and_invalid_combinations_reject(self):
        plan = relation.relation_chain('EM')
        self.helper.config_for(plan, [])
        with ThreadedRuntime([]) as runtime:
            owner = self.owner(plan)
            stream = owner.evaluate(iter([]), plan, expected=[], query_id=plan.query_id)
            self.assertEqual(list(stream), [])
            self.assertEqual(runtime.async_actors, [])
            self.assertEqual(owner.last_close_report['async_token_batches']['selected_batches'], 0)
            self.assertEqual(self.tokens.calls, 0)
        for kwargs in (dict(C=0), dict(C=True), dict(progression='batch_stage', progression_batch_rows=1)):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.owner(plan, **kwargs)

    def test_unknown_member_keeps_known_prefix_and_never_replays_unsubmitted_successor(self):
        plan, rows = fixture.chain_for('MM'), fixture.source_rows(size=2)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0'].update(status='unknown', wait_file=str(gate))
        self.helper.config.write_text(json.dumps(config))
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
            owner = self.owner(plan, R=2, C=2)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                deadline = time.monotonic()+5
                while not any(e['event']=='end' and e['token']=='step_1|occ-1' for e in self.helper.events()):
                    stream._tick()
                    self.assertLess(time.monotonic(), deadline)
                gate.touch()
                with self.assertRaises(RuntimeError): list(stream)
                report = owner.close(timeout=3)
                self.assertEqual(report['status'], 'closed')
                self.assertFalse(report['query_completed'])
                self.assertEqual(sum(v['status']=='unknown' for v in owner.calls), 1)
                self.assertEqual(sum(v['status']=='succeeded' for v in owner.calls), 2)
                self.assertFalse(any(v['algorithm_stage']=='step_1' and v['row_id']=='occ-0' for v in owner.calls))
                self.assertEqual(report['core_usage']['held_tasks'], 0)
                self.assertEqual(stream.pool.used, (0, 0))
            finally:
                gate.touch(); owner.close(timeout=3)
