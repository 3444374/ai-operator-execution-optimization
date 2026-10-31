"""RF/RD session reuse contract; resident cases are intentionally red on e737.

Exercise existing check_plan/run_movie_session and real CPU IPC. These tests do
not implement reuse, bypass its preflight, or turn the resident gate into a skip.
"""
import asyncio
from copy import deepcopy
import json
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource
from src.baselines.text.movie_tasks import MovieTask
from src.experiments import movie_records_session as session
from src.execution_provider.adapters.lotus_records_core import create_records_core
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

from tests.experiments.movie_token_resident_fixture import (
    MovieTokenResidentFixture, query_calls, query_close,
    with_actual_cpu_pid,
)


class MovieTokenResidentTests(unittest.TestCase):
    def assert_query_released(self, query, count):
        close = query_close(query)
        self.assertTrue(close['query_completed'])
        self.assertTrue(close['model_retained'])
        self.assertEqual(close['model_owner']['status'], 'retained')
        self.assertEqual(len(query_calls(query)), count)
        self.assertEqual(close['unconsumed_call_results'], [])
        for field in ('held_tasks', 'input_bytes', 'result_bytes', 'active_requests', 'active_work'):
            self.assertEqual(close['core_usage'][field], 0, field)

    def test_isolated_mode_keeps_fresh_models_and_query_ledgers(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                report = fixture.run(fixture.plan(mode, lifecycle='isolated'))
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(report['owner_start_count'], 2)
                self.assertEqual([q['backend_report']['result'] for q in report['queries']],
                                 [{'reviewId': ['1']}, {'reviewId': ['1']}])
                calls = [query_calls(q) for q in report['queries']]
                self.assertEqual([len(c) for c in calls], [2, 2])
                self.assertEqual(len({c['engine_uid'] for group in calls for c in group}), 2)
                self.assertEqual(sum(e['event'] == 'construct' for e in fixture.events()), 2)
                self.assertEqual(sum(e['event'] == 'close' for e in fixture.events()), 2)
                self.assertTrue(all(query_close(q)['core_usage']['held_tasks'] == 0
                                    for q in report['queries']))

    def test_resident_plan_uses_existing_schema_without_constructing_owner(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                checked = session.check_plan(fixture.plan(mode))
                self.assertEqual(checked['status'], 'checked_cpu_only')
                self.assertEqual(checked['expected_calls'], 4)
                self.assertEqual(checked['record_batching']['C_request'], 1 if mode == 'RD' else 2)
                self.assertEqual(len(fixture.runtime.async_actors), 0)

    def test_same_configuration_reuses_one_actual_model_through_session_close(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                snapshots = []

                def checkpoint(info):
                    events = fixture.events()
                    self.assertEqual(sum(e['event'] == 'construct' for e in events), 1)
                    self.assertEqual(sum(e['event'] == 'close' for e in events), 0)
                    path = fixture.outputs[-1]/('query-'+str(info['query_index'])+'.json')
                    snapshots.append(json.loads(path.read_text()))

                report = fixture.run(fixture.plan(mode), checkpoint=checkpoint)
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(report['owner_start_count'], 1)
                self.assertEqual(len(fixture.runtime.async_actors), 1)
                self.assertEqual(len({c['engine_uid'] for q in report['queries'] for c in query_calls(q)}), 1)
                self.assertEqual(sum(e['event'] == 'close' for e in fixture.events()), 1)
                self.assertEqual(len({q['owner_id'] for q in report['queries']}), 1)
                self.assertEqual([q['backend_report']['result'] for q in report['queries']],
                                 [{'reviewId': ['1']}, {'reviewId': ['1']}])
                for before, after in zip(snapshots, report['queries']):
                    self.assertEqual(query_calls(before), query_calls(after))
                    self.assert_query_released(after, 2)

    def test_each_query_has_fresh_expected_rows_calls_and_native_request_ids(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                report = fixture.run(fixture.plan(mode, tasks=('movie-q1', 'movie-q2')))
                first, second = report['queries']
                self.assert_query_released(first, 2)
                self.assert_query_released(second, 1)
                a, b = query_calls(first), query_calls(second)
                self.assertEqual({c['query_id'] for c in a}, {first['query_id']})
                self.assertEqual({c['query_id'] for c in b}, {second['query_id']})
                self.assertTrue({c['call_id'] for c in a}.isdisjoint(c['call_id'] for c in b))
                self.assertTrue({c['engine_request_id'] for c in a}.isdisjoint(c['engine_request_id'] for c in b))
                self.assertEqual({c['source_position'] for c in b}, {0})
                self.assertLess(max(c['core_task_key']['session_id'] for c in a),
                                min(c['core_task_key']['session_id'] for c in b))

    def test_public_start_and_old_iterator_snapshots_remain_stable_across_reuse(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                owner = fixture.owner(mode, resident=True)
                try:
                    ready = owner.start()
                    self.assertEqual(owner.start(), ready)
                    self.assertIsInstance(ready['pid'], int)
                    self.assertGreater(ready['pid'], 0)
                    self.assertTrue(ready['engine_uid'])
                    self.assertEqual(sum(e['event'] == 'construct' for e in fixture.events()), 1)
                    self.assertFalse(any(e['event'] == 'start' for e in fixture.events()))
                    source = MovieRecordSource.from_manifest(fixture.manifests['movie-q1'])
                    first = owner.evaluate(source, query_id='snapshot-first')
                    self.assertEqual(len(list(first)), 2)
                    snapshot = deepcopy(first.last_close_report)
                    self.assertEqual(len(list(owner.evaluate(source, query_id='snapshot-next'))), 2)
                    self.assertEqual(first.last_close_report, snapshot)
                    self.assertEqual({c['query_id'] for c in snapshot['calls']}, {'snapshot-first'})
                    self.assertTrue(all(c['engine_uid'] == ready['engine_uid'] for c in snapshot['calls']))
                finally:
                    self.assertEqual(owner.close()['status'], 'closed')
                self.assertEqual(first.last_close_report, snapshot)

    def test_inflight_cancel_keeps_known_prefix_and_unknown_peer_then_bars_reuse(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                held = next(key for key, value in fixture.replies.items() if value['text'] == 'False')
                fixture.replies[held].update(status='unknown', wait_file=str(fixture.root/'release'))
                fixture.write_replies()
                owner = fixture.owner(mode, resident=True)

                def cancelled():
                    return any(e['event'] == 'start' and e['token'] == held for e in fixture.events())

                source = MovieRecordSource.from_manifest(fixture.manifests['movie-q1'])
                try:
                    owner.start()
                    with self.assertRaises((RuntimeError, InterruptedError)):
                        list(owner.evaluate(source, query_id='cancel-pending', cancelled=cancelled))
                    close = owner.close(timeout=3)
                    self.assertEqual(sum(c['status'] == 'succeeded' for c in close['calls']), 1)
                    self.assertEqual(sum(c['status'] == 'unknown' for c in close['calls']), 1)
                    self.assertFalse(close['query_completed'])
                    with self.assertRaises(ValueError):
                        owner.evaluate(source, query_id='after-cancel')
                finally:
                    (fixture.root/'release').touch()
                    owner.close(timeout=3)

    def test_empty_query_between_queries_has_no_calls_or_previous_results(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                fixture.manifests['movie-q3'] = fixture.source_manifest('empty', [], task='movie-q3')
                report = fixture.run(fixture.plan(mode, tasks=('movie-q1', 'movie-q3', 'movie-q1')))
                self.assertEqual([len(query_calls(q)) for q in report['queries']], [2, 0, 2])
                self.assertEqual(report['queries'][1]['backend_report']['result'], {'positive_review_cnt': 0})
                self.assertEqual(len({c['engine_uid'] for q in report['queries'] for c in query_calls(q)}), 1)
                self.assertEqual(sum(e['event'] == 'construct' for e in fixture.events()), 1)
                for query, count in zip(report['queries'], (2, 0, 2)):
                    self.assert_query_released(query, count)

    def test_final_close_is_idempotent_and_closed_owner_rejects_more_work(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                fixture.run(fixture.plan(mode))
                owner, = fixture.owners
                before = deepcopy(fixture.events())
                for _ in range(2):
                    self.assertEqual(owner.close()['status'], 'closed')
                source = MovieRecordSource.from_manifest(fixture.manifests['movie-q1'])
                with self.assertRaises(ValueError):
                    list(owner.evaluate(source, query_id='after-final-close'))
                self.assertEqual(fixture.events(), before)

    def test_rd_starts_each_query_at_one_while_rf_keeps_declared_request_window(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                report = fixture.run(fixture.plan(mode))
                for query in report['queries']:
                    close = query_close(query)
                    batching = close['async_token_batches']
                    self.assertEqual(batching['request_limit'], 2)
                    if mode == 'RD':
                        self.assertEqual(batching['recent_batches'][0]['C_request'], 1)
                        self.assertEqual(close['request_supply_feedback']['initial_requests'], 1)
                    else:
                        self.assertNotIn('request_supply_feedback', close)
                        self.assertEqual(batching['C_request'], 2)
                        self.assertEqual(batching['peak_requests'], 2)
                        self.assertEqual(batching['recent_batches'][0]['members'], 2)

    def test_rd_resets_actual_first_submission_after_prior_cpu_feedback_trial(self):
        # Existing logical observation clock, real CPU IPC and actual controller;
        # this is a reset/ownership test, never a real-time or GPU benefit claim.
        from src.execution_provider.adapters import lotus_chain_token_batches as batches
        from src.scheduling.submission_control.stage_work import RequestSupplyFeedbackController
        from tests.execution_provider.test_lotus_batch_supply_feedback import logical_feedback_frontier
        from tests.execution_provider.test_lotus_request_supply_feedback import CPUWorkProcess
        fixture = MovieTokenResidentFixture(self)
        fixture.manifests['movie-q1'] = fixture.source_manifest('many', [
            [str(i+1), 'taken_3', 'good' if i % 2 == 0 else 'bad',
             'POSITIVE' if i % 2 == 0 else 'NEGATIVE'] for i in range(48)], task='movie-q1')
        plan = fixture.plan('RD')
        plan['resources']['max_active_batches'] = 8
        plan['profile']['admission'].update(poll_interval_s=.002, min_samples=1)
        with logical_feedback_frontier(), \
                patch('src.scheduling.submission_control.stage_work.RequestSupplyFeedbackController',
                      lambda maximum, config: RequestSupplyFeedbackController(
                          maximum, config, clock=lambda: batches.time.monotonic())), \
                patch.object(AsyncCoreModelActor, '_process_type', return_value=with_actual_cpu_pid(CPUWorkProcess)):
            report = fixture.run(plan)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(len(fixture.runtime.async_actors), 1)
        self.assertEqual(len({c['engine_uid'] for q in report['queries'] for c in query_calls(q)}), 1)
        first, second = [query_close(q) for q in report['queries']]
        self.assertTrue(any(b['C_request'] > 1 for b in first['async_token_batches']['recent_batches']))
        self.assertGreater(first['request_supply_feedback']['phases'][0]['changes'], 0)
        for close in (first, second):
            self.assertEqual(close['async_token_batches']['recent_batches'][0]['C_request'], 1)
            self.assertEqual(close['request_supply_feedback']['initial_requests'], 1)
            self.assertEqual(len(close['calls']), 48)
            self.assertEqual(close['core_usage']['held_tasks'], 0)

    def test_failure_preserves_completed_query_and_unknown_receipt_without_running_next(self):
        for mode in ('RF', 'RD'):
            for outcome in ('failed', 'unknown'):
                with self.subTest(mode=mode, outcome=outcome):
                    fixture = MovieTokenResidentFixture(self)
                    for response in fixture.replies.values():
                        if response['text'] == 'False':
                            response['status'] = outcome
                    fixture.write_replies()
                    plan = fixture.plan(mode, tasks=('movie-q2', 'movie-q1', 'movie-q1'))
                    with self.assertRaises(RuntimeError) as caught:
                        fixture.run(plan)
                    report = caught.exception.session_report
                    self.assertEqual([q['status'] for q in report['queries']], ['completed', 'failed'])
                    self.assertEqual(report['skipped_queries'], [plan['queries'][2]['query_id']])
                    self.assert_query_released(report['queries'][0], 1)
                    calls = query_calls(report['queries'][1])
                    self.assertEqual(sum(c['status'] == outcome for c in calls), 1)
                    self.assertTrue(all(c['query_id'] == plan['queries'][1]['query_id'] for c in calls))
                    self.assertEqual(sum(e['event'] == 'construct' for e in fixture.events()), 1)
                    source = MovieRecordSource.from_manifest(fixture.manifests['movie-q1'])
                    with self.assertRaises(ValueError):
                        list(fixture.owners[0].evaluate(source, query_id='after-failed-session'))

    def test_checkpoint_cancellation_preserves_primary_and_closes_resident_owner(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                plan = fixture.plan(mode)
                cancelled = asyncio.CancelledError('CPU checkpoint cancellation')

                def checkpoint(info):
                    raise cancelled

                with self.assertRaises(asyncio.CancelledError) as caught:
                    fixture.run(plan, checkpoint=checkpoint)
                self.assertIs(caught.exception, cancelled)
                report = cancelled.session_report
                self.assertEqual(len(report['queries']), 1)
                self.assert_query_released(report['queries'][0], 2)
                self.assertEqual(report['skipped_queries'], [plan['queries'][1]['query_id']])
                self.assertEqual(sum(e['event'] == 'close' for e in fixture.events()), 1)
                self.assertEqual(report['owners'][0]['status'], 'closed')

    def test_runtime_model_or_tokenizer_change_is_rejected_before_factory(self):
        for mode in ('RF', 'RD'):
            for field in ('model_path', 'tokenizer_path'):
                with self.subTest(mode=mode, field=field):
                    fixture = MovieTokenResidentFixture(self)
                    plan = fixture.plan(mode, lifecycle='isolated')
                    context = fixture.context(plan)
                    context['runtime_binding'][field] += '-different'
                    with self.assertRaisesRegex(ValueError, 'runtime binding'):
                        fixture.run(plan, context=context)
                    self.assertEqual(fixture.owners, [])
                    self.assertEqual(fixture.events(), [])

    def test_resident_owner_rejects_different_model_before_any_request(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                owner = fixture.owner(mode, resident=True)
                try:
                    owner.start()
                    source = MovieRecordSource.from_manifest(fixture.manifests['movie-q1'])
                    changed = MovieTask(1).method('openai/different-cpu-fixture')
                    with self.assertRaisesRegex(ValueError, 'model'):
                        list(owner.evaluate(source, query_id='incompatible-model', method=changed))
                    self.assertFalse(any(e['event'] == 'start' for e in fixture.events()))
                    self.assertEqual(sum(e['event'] == 'construct' for e in fixture.events()), 1)
                finally:
                    self.assertEqual(owner.close()['status'], 'closed')

    def test_default_factory_remains_single_query_without_resident_opt_in(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                fixture.runtime.ray.init()
                plan = fixture.plan(mode, lifecycle='isolated')
                source = MovieRecordSource.from_manifest(fixture.manifests['movie-q1'])
                owner = create_records_core(MovieTask(1).method(plan['model_name']),
                    EngineSpec.from_dict(plan['engine']), GenerationSpec.from_dict(plan['generation']),
                    RecordsResources(**plan['resources']), profile=plan['profile'], rolling_refill=True,
                    prepared_batching=PreparedBatching(**plan['prepared_batching']))
                try:
                    self.assertEqual(len(list(owner.evaluate(source, query_id='default-first'))), 2)
                    with self.assertRaises(ValueError):
                        list(owner.evaluate(source, query_id='default-second'))
                finally:
                    self.assertEqual(owner.close()['status'], 'closed')
                self.assertEqual(sum(e['event'] == 'construct' for e in fixture.events()), 1)


if __name__ == '__main__':
    unittest.main()
