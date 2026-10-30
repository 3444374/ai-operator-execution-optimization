"""Normal Movie config/CLI/factory with original methods and real CPU IPC.

Ray and upstream checkout/evaluation are explicit fixtures; this is no GPU result.
"""
from contextlib import ExitStack, redirect_stdout
from copy import deepcopy
from dataclasses import asdict
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.movie_records import MovieRecordSource
from src.baselines.text.movie_tasks import MovieTask
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.execution_provider.adapters.lotus_records_core import create_records_core
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus import engine as methods
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.experiments import test_movie_records_session as fixtures
from tests.execution_provider.chain_tokenizer_fixture import tokenizer
from tests.execution_provider.test_lotus_record_local_query import Process
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig


class AsyncTokenEntryTests(unittest.TestCase):
    def setUp(self):
        helper = fixtures.PlanTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        self.helper = helper
        self.root = Path(helper.manifests['movie-q1']).parent.parent
        tokenizer().save_pretrained(self.root / 'tokenizer')
        self.model = self.root / 'native.json'
        self.engine = EngineSpec(str(self.model), sys.executable, sys.executable,
            tokenizer_path=str(self.root / 'tokenizer'), max_model_len=4096, max_num_seqs=1, max_model_calls=4)
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=2, startup_timeout_s=8, query_timeout_s=15)

    def config(self, task='movie-q1'):
        return dict(schema='semloom.movie_records_run.v1', backend='semloom-local-model-core-async',
            query_id=task+'-batch', task=task, method=asdict(MovieTask.from_config(task).method('openai/cpu-fixture')),
            generation=asdict(GenerationSpec(max_tokens=32)), engine=asdict(self.engine), resources=asdict(self.resources),
            core_profile=dict(mode='async', max_inflight=2),
            prepared_batching=dict(policy='token_budget', max_rows=None, token_budget=100000), batch_concurrency=2)

    def save(self, config):
        path = self.root / 'config.json'; path.write_text(json.dumps(config)); return path

    def runtime(self):
        stack = ExitStack(); self.addCleanup(stack.close)
        formatter, binder = methods.LotusFilterMethod, methods.bind_filter_method
        runtime = stack.enter_context(ThreadedRuntime([]))
        initialized = [False]
        runtime.ray.is_initialized = lambda: initialized[0]
        runtime.ray.init = Mock(side_effect=lambda **kw: initialized.__setitem__(0, True))
        runtime.ray.shutdown = Mock(side_effect=lambda: initialized.__setitem__(0, False))
        stack.enter_context(patch.object(methods, 'LotusFilterMethod', formatter))
        stack.enter_context(patch.object(methods, 'bind_filter_method', binder))
        stack.enter_context(patch.object(AsyncCoreModelActor, '_process_type', return_value=Process))
        stack.enter_context(patch.object(movie, 'verify_checkout', return_value=self.root))
        stack.enter_context(patch.object(movie, 'evaluate_original', return_value={'original_metric':'CPU-fixture'}))
        return runtime

    def test_check_CLI_reports_B_C_batch_R_without_constructing_owner(self):
        config = self.config(); path = self.save(config)
        out = io.StringIO()
        with patch.object(movie, 'model_preflight', return_value=dict(status='preflight_only', inference_calls=0)), \
                patch('src.execution_provider.adapters.lotus_records_core.create_records_core') as allocate, redirect_stdout(out):
            self.assertEqual(movie.main(['check', '--source-manifest', self.helper.manifests['movie-q1'],
                '--sembench-checkout', str(self.root), '--config', str(path)]), 0)
        allocate.assert_not_called()
        report = json.loads(out.getvalue())
        self.assertEqual((report['record_batching']['B'], report['record_batching']['C_batch'],
                          report['record_batching']['R']), (100000, 2, 2))
        self.assertIsNone(report['record_batching']['row_cap'])
        self.assertEqual(report['inference_calls'], 0)

    def feedback_config(self):
        return dict(self.config(), batch_concurrency=1,
            batch_feedback=dict(max_batches=2, options=dict(poll_interval_s=.002, window_s=.05)))

    def test_feedback_check_CLI_uses_shared_defaults_without_allocating_owner(self):
        config = self.feedback_config(); path = self.save(config)
        out = io.StringIO()
        with patch.object(movie, 'model_preflight', return_value=dict(status='preflight_only', inference_calls=0)), \
                patch('src.execution_provider.adapters.lotus_records_core.create_records_core') as allocate, redirect_stdout(out):
            self.assertEqual(movie.main(['check', '--source-manifest', self.helper.manifests['movie-q1'],
                '--sembench-checkout', str(self.root), '--config', str(path)]), 0)
        allocate.assert_not_called()
        batching = json.loads(out.getvalue())['record_batching']
        self.assertEqual((batching['B'], batching['C_batch'], batching['R']), (100000, 1, 2))
        self.assertIsNone(batching['row_cap'])
        self.assertEqual(batching['feedback']['max_batches'], 2)
        options = batching['feedback']['options']
        self.assertEqual((options['work_basis'], options['work_comparison'], options['search_mode']),
                         ('computed', 'local_pairs', 'geometric'))
        self.assertTrue(options['allow_exploration'])

    def test_feedback_CLI_rejects_incompatible_or_malformed_config_before_factory(self):
        config = self.feedback_config()
        changes = [dict(batch_concurrency=2), dict(batch_feedback=dict(max_batches=3)),
            dict(batch_feedback=[]), dict(batch_feedback=dict(max_batches=True)),
            dict(batch_feedback=dict(max_batches=2, options=dict(rows=64))),
            dict(prepared_batching=None, batch_concurrency=None),
            dict(core_profile=dict(mode='sync')), dict(core_profile=dict(mode='async', stage_timing=True)),
            dict(core_profile=dict(mode='async', admission=dict(kind='resource_feedback', allow_exploration=True))),
            dict(backend='ray-data-local-model')]
        with patch('src.execution_provider.adapters.lotus_records_core.create_records_core') as allocate:
            for change in changes:
                with self.subTest(change=change), self.assertRaises(ValueError):
                    movie.read_config(self.save(dict(deepcopy(config), **change)))
        allocate.assert_not_called()

    def test_absent_or_null_feedback_retains_fixed_config_and_legacy_default(self):
        for config in (self.config(), dict(self.config(), batch_feedback=None)):
            value = movie.read_config(self.save(config))[0]
            options = movie._record_batching_options(value)
            self.assertIsNone(options.get('batch_feedback'))
            self.assertEqual(options['batch_concurrency'], 2)
        legacy = self.config()
        legacy.pop('prepared_batching'); legacy.pop('batch_concurrency')
        self.assertEqual(movie._record_batching_options(legacy), {})

    def test_feedback_normal_worker_CLI_forwards_typed_config_and_preserves_results(self):
        self.runtime()
        source = MovieRecordSource.from_manifest(self.helper.manifests['movie-q1'])
        self.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text=str(r['source_position']==0)) for r in source.rows()})))
        results = []
        for enabled in (False, True):
            config = self.feedback_config()
            if not enabled:config.pop('batch_feedback')
            path = self.save(config)
            output = self.root/('feedback-on' if enabled else 'feedback-off'); output.mkdir()
            with patch('src.execution_provider.adapters.lotus_records_core.create_records_core',
                    wraps=create_records_core) as allocate, redirect_stdout(io.StringIO()):
                self.assertEqual(movie.main(['run', '--source-manifest', str(source.manifest_path),
                    '--sembench-checkout', str(self.root), '--config', str(path),
                    '--output-dir', str(output), '--execute-model', '--worker']), 0)
            report = json.loads((output/'query.json').read_text())
            self.assertEqual(report['status'], 'completed', report)
            self.assertEqual(report['generation'], json.loads(json.dumps(config['generation'])))
            close = report['owner_close']
            self.assertEqual(close['status'], 'closed')
            self.assertEqual(close['core_usage']['held_tasks'], 0)
            self.assertEqual(sum(len(b['receipt_members']) for b in close['transport_batches']), 2)
            results.append(report['result'])
            if enabled:
                self.assertIsInstance(allocate.call_args.kwargs['batch_feedback'], SupplyFeedbackConfig)
                self.assertEqual(close['record_batching']['feedback']['max_batches'], 2)
                feedback = close['batch_supply_feedback']
                self.assertEqual((feedback['initial_batches'], feedback['fixed_B']), (1, 100000))
                self.assertTrue(feedback['sampler_stopped'])
            else:self.assertNotIn('batch_supply_feedback', close)
        self.assertEqual(results, [{'reviewId':['1']}, {'reviewId':['1']}])

    def test_feedback_isolated_session_uses_same_factory_and_rejects_resident(self):
        self.runtime(); config = self.feedback_config()
        plan = dict(schema='semloom.movie_session_plan.v1', backend='semloom-core', lifecycle='isolated',
            model_name='openai/cpu-fixture', generation=config['generation'], engine=config['engine'],
            resources=config['resources'], profile=config['core_profile'], prepared_batching=config['prepared_batching'],
            batch_concurrency=1, batch_feedback=config['batch_feedback'], queries=[dict(query_id='session-feedback',
                task='movie-q1', manifest=self.helper.manifests['movie-q1'], phase='measured')])
        checked = session.check_plan(plan)
        self.assertEqual(checked['record_batching']['feedback']['max_batches'], 2)
        with self.assertRaisesRegex(ValueError, 'isolated'):
            session.check_plan(dict(plan, lifecycle='resident'))
        query = plan['queries'][0]
        source = MovieRecordSource.from_manifest(query['manifest'])
        self.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text=str(r['source_position']==0)) for r in source.rows()})))
        context = dict(owner_context={}, claims={'session-feedback':None}, sembench_checkout=self.root,
            checked_queries={q['query_id']:q for q in checked['queries']})
        driver = session._MovieDriver(plan, context, self.root/'feedback-driver', query)
        with patch('integrations.imlane_records.chain_runtime.check_external_lease'):
            self.assertTrue(driver.start()['no_session_model'])
            try:
                report = driver.query(query, lambda event: None)
                self.assertEqual(report['status'], 'completed')
                close = report['backend_close']
                self.assertEqual(close['record_batching']['feedback']['max_batches'], 2)
                self.assertTrue(close['batch_supply_feedback']['sampler_stopped'])
                self.assertEqual(report['result'], {'reviewId':['1']})
            finally:self.assertEqual(driver.close()['status'], 'closed')

    def test_normal_worker_CLI_runs_each_frozen_task_through_actual_factory_and_bulk_IPC(self):
        runtime = self.runtime()
        for task, count, result in [('movie-q1', 2, {'reviewId':['1']}),
                                    ('movie-q2', 1, {'reviewId':['1']}),
                                    ('movie-q3', 1, {'positive_review_cnt':1})]:
            with self.subTest(task=task):
                source = MovieRecordSource.from_manifest(self.helper.manifests[task])
                self.model.write_text(json.dumps(dict(responses={
                    'filter|'+r['row_id']:dict(text=str(r['source_position']==0)) for r in source.rows()})))
                config = self.config(task); path = self.save(config)
                output = self.root / ('run-'+task); output.mkdir()
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(movie.main(['run', '--source-manifest', str(source.manifest_path),
                        '--sembench-checkout', str(self.root), '--config', str(path),
                        '--output-dir', str(output), '--execute-model', '--worker']), 0)
                report = json.loads((output/'query.json').read_text())
                self.assertEqual(report['status'], 'completed', report)
                self.assertEqual(report['result'], result)
                self.assertEqual(report['calls']['counts']['succeeded'], count)
                self.assertEqual(report['generation'], json.loads(json.dumps(config['generation'])))
                self.assertTrue(report['production_core_used'])
                close = report['owner_close']
                self.assertEqual(close['status'], 'closed')
                self.assertEqual(close['core_usage']['held_tasks'], 0)
                self.assertEqual(close['record_batching']['R'], 2)
                self.assertEqual(close['async_token_batches']['data_batch_rpcs'], 1)
                members = close['transport_batches'][0]['receipt_members']
                self.assertEqual(len(members), count)
                expected = {v['call_id'] for v in source.expected(config['query_id'])}
                self.assertEqual({v['call_id'] for v in members}, expected)
                self.assertTrue(all(v['submission_confirmed'] and v['status']=='succeeded'
                                    and isinstance(v['model_end_monotonic'], float) for v in members))
                self.assertTrue(all(v['chain_call_id'] != v['call_id'] for v in members))
        self.assertEqual(runtime.ray.init.call_count, 3)
        self.assertEqual(len(runtime.async_actors), 3)

    def test_unsupported_combinations_and_task_changes_fail_before_factory(self):
        config = self.config()
        mutations = [dict(backend='semloom-local-model-core', core_profile={}), dict(batch_concurrency=3),
            dict(prepared_batching=dict(policy='fixed_rows', max_rows=1, token_budget=0)),
            dict(prepared_batching=dict(policy='token_budget', max_rows=1, token_budget=10)),
            dict(core_profile=dict(mode='async', stage_timing=True)),
            dict(core_profile=dict(mode='async', admission=dict(kind='resource_feedback', allow_exploration=True))),
            dict(backend='ray-data-local-model'), dict(task='movie-q3')]
        for changes in mutations:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                movie.read_config(self.save(dict(deepcopy(config), **changes)))
        plan = dict(self.helper.plan, profile=dict(mode='async'), resources=asdict(self.resources),
            prepared_batching=config['prepared_batching'], batch_concurrency=2)
        with self.assertRaisesRegex(ValueError, 'isolated'):
            session.check_plan(plan)
        plan['lifecycle'] = 'isolated'
        self.assertEqual(session.check_plan(plan)['record_batching']['C_batch'], 2)
        plan['queries'][0]['task'] = 'movie-q3'
        with self.assertRaisesRegex(ValueError, 'source manifest'):
            session.check_plan(plan)

    def test_unknown_call_keeps_its_receipt_and_usage_through_normal_worker_failure(self):
        self.runtime()
        source = MovieRecordSource.from_manifest(self.helper.manifests['movie-q1'])
        self.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text='True', status='unknown' if r['source_position']==0 else 'succeeded')
            for r in source.rows()})))
        path = self.save(self.config()); output = self.root/'failed'; output.mkdir()
        with redirect_stdout(io.StringIO()), self.assertRaises(Exception):
            movie.main(['run', '--source-manifest', str(source.manifest_path), '--sembench-checkout', str(self.root),
                '--config', str(path), '--output-dir', str(output), '--execute-model', '--worker'])
        report = json.loads((output/'query.json').read_text())
        self.assertEqual(report['status'], 'failed')
        counts = report['calls']['counts']
        self.assertEqual((counts['succeeded'], counts['unknown'], counts['recorded']), (1,1,2))
        self.assertEqual(report['calls']['usage']['known_prompt_tokens'], 22)
        self.assertEqual(report['owner_close']['core_usage']['held_tasks'], 0)
        native = [json.loads(line) for line in Path(str(self.model)+'.events').read_text().splitlines()]
        self.assertEqual(sum(v['event']=='start' for v in native), 2)

    def test_empty_finite_source_and_unused_factory_close_allocate_no_model(self):
        runtime = self.runtime(); runtime.ray.init()
        original = MovieRecordSource.from_manifest(self.helper.manifests['movie-q1'])
        from types import SimpleNamespace
        source = SimpleNamespace(preflight=lambda method: None, scan_spec=original.scan_spec,
            expected=lambda query: [], rows=lambda: [])
        owner = create_records_core(MovieTask(1).method('openai/cpu-fixture'), self.engine,
            GenerationSpec(max_tokens=32), self.resources, profile=dict(mode='async'),
            prepared_batching=PreparedBatching('token_budget', None, 100), batch_concurrency=2)
        with patch('transformers.AutoTokenizer.from_pretrained') as load:
            self.assertEqual(list(owner.evaluate(source, query_id='empty')), [])
        load.assert_not_called()
        self.assertEqual(len(runtime.async_actors), 0)
        self.assertEqual(owner.close()['status'], 'closed')
        unused = create_records_core(MovieTask(1).method('openai/cpu-fixture'), self.engine,
            GenerationSpec(max_tokens=32), self.resources, profile=dict(mode='async'),
            prepared_batching=PreparedBatching('token_budget', None, 100))
        self.assertFalse(unused.close()['model_allocated'])

    def test_existing_isolated_session_driver_forwards_same_batch_options_and_task_binding(self):
        runtime = self.runtime()
        config = self.config()
        plan = dict(schema='semloom.movie_session_plan.v1', backend='semloom-core', lifecycle='isolated',
            model_name='openai/cpu-fixture', generation=config['generation'], engine=config['engine'],
            resources=config['resources'], profile=config['core_profile'], prepared_batching=config['prepared_batching'],
            batch_concurrency=2, queries=[dict(query_id='session-q1', task='movie-q1',
                manifest=self.helper.manifests['movie-q1'], phase='measured')])
        checked = session.check_plan(plan)
        query = plan['queries'][0]
        source = MovieRecordSource.from_manifest(query['manifest'])
        self.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text=str(r['source_position']==0)) for r in source.rows()})))
        context = dict(owner_context={}, claims={'session-q1':None}, sembench_checkout=self.root,
            checked_queries={q['query_id']:q for q in checked['queries']})
        driver = session._MovieDriver(plan, context, self.root/'session-driver', query)
        with patch('integrations.imlane_records.chain_runtime.check_external_lease'):
            self.assertTrue(driver.start()['no_session_model'])
            try:
                report = driver.query(query, lambda event: None)
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(report['backend_close']['record_batching']['C_batch'], 2)
                self.assertEqual(report['result'], {'reviewId':['1']})
                self.assertEqual(len(runtime.async_actors), 1)
            finally:
                self.assertEqual(driver.close()['status'], 'closed')

    def test_consumer_close_preserves_undelivered_receipts_and_stops_iteration(self):
        runtime = self.runtime(); runtime.ray.init()
        source = MovieRecordSource.from_manifest(self.helper.manifests['movie-q1'])
        self.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text='True') for r in source.rows()})))
        owner = create_records_core(MovieTask(1).method('openai/cpu-fixture'), self.engine,
            GenerationSpec(max_tokens=32), self.resources, profile=dict(mode='async'),
            prepared_batching=PreparedBatching('token_budget', None, 10000))
        stream = owner.evaluate(source, query_id='early-consumer')
        first = next(stream)
        closed = stream.close()
        self.assertEqual(len(closed['unconsumed_call_results']), 1)
        self.assertNotEqual(closed['unconsumed_call_results'][0]['call_id'], first['call_id'])
        with self.assertRaises(StopIteration): next(stream)
        self.assertEqual(owner.last_close_report['unconsumed_call_results'], closed['unconsumed_call_results'])

    def test_cancel_before_chain_initialization_has_no_model_and_closes_cleanly(self):
        runtime = self.runtime(); runtime.ray.init()
        source = MovieRecordSource.from_manifest(self.helper.manifests['movie-q1'])
        owner = create_records_core(MovieTask(1).method('openai/cpu-fixture'), self.engine,
            GenerationSpec(max_tokens=32), self.resources, profile=dict(mode='async'),
            prepared_batching=PreparedBatching('token_budget', None, 10000))
        with self.assertRaises(InterruptedError): owner.evaluate(source, query_id='cancelled', cancelled=lambda: True)
        self.assertEqual(owner.close()['status'], 'closed')
        self.assertEqual(len(runtime.async_actors), 0)
