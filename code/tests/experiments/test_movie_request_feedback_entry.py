"""Ordinary Movie/isolated-session request feedback; CPU-only model fixture."""
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.execution_provider.adapters.lotus_records_core import create_records_core
from tests.experiments import test_movie_async_token_entry as entry


class RequestEntryTests(unittest.TestCase):
    setUp = entry.AsyncTokenEntryTests.setUp
    config = entry.AsyncTokenEntryTests.config
    save = entry.AsyncTokenEntryTests.save
    runtime = entry.AsyncTokenEntryTests.runtime
    rolling_config = entry.AsyncTokenEntryTests.rolling_config

    def dynamic_config(self):
        config = self.rolling_config()
        config['core_profile'] = dict(mode='async', admission=dict(kind='resource_feedback'))
        return config

    def test_check_reports_initial_request_one_hard_R_and_unbudgeted_default_scan(self):
        config = self.dynamic_config()
        out = io.StringIO()
        with patch.object(movie, 'model_preflight', return_value=dict(inference_calls=0)), \
                patch('src.execution_provider.adapters.lotus_records_core.create_records_core') as allocate, redirect_stdout(out):
            self.assertEqual(movie.main(['check', '--source-manifest', self.helper.manifests['movie-q1'],
                '--sembench-checkout', str(self.root), '--config', str(self.save(config))]), 0)
        allocate.assert_not_called()
        report = json.loads(out.getvalue())
        selected = report['record_batching']
        self.assertEqual((selected['B'], selected['C_request'], selected['C_batch'], selected['R']), (100000, 1, None, 2))
        options = selected['request_feedback']
        self.assertEqual((options['search_mode'], options['work_basis'], options['work_comparison']),
                         ('geometric', 'computed', 'local_pairs'))
        self.assertTrue(options['allow_exploration'])
        for field in ('exploration_s', 'max_changes', 'max_candidate_attempts'):self.assertIsNone(options[field])
        self.assertEqual(options['min_remaining'], 0)
        self.assertEqual(report['core_execution']['owner_max_inflight'], 2)
        self.assertEqual(report['core_execution']['initial_window'], 1)
        self.assertEqual(report['inference_calls'], 0)

    def test_request_feedback_rejects_incompatible_modes_and_start_before_allocation(self):
        config = self.dynamic_config()
        changes = [dict(rolling_refill=False), dict(batch_concurrency=1),
            dict(batch_feedback=dict(max_batches=2)), dict(prepared_batching=None),
            dict(core_profile=dict(mode='sync', admission=dict(kind='resource_feedback'))),
            dict(core_profile=dict(mode='async', max_inflight=2, admission=dict(kind='resource_feedback'))),
            dict(core_profile=dict(mode='async', max_inflight='auto', admission=dict(kind='resource_feedback'))),
            dict(core_profile=dict(mode='async', admission=dict(kind='resource_feedback', gpu_ids=['GPU-fixture']))),
            dict(core_profile=dict(mode='async', stage_timing=True, admission=dict(kind='resource_feedback'))),
            dict(core_profile=dict(mode='async', admission=dict(kind='resource_feedback', poll_interval_s=2, window_s=1)))]
        with patch('src.execution_provider.adapters.lotus_records_core.create_records_core') as allocate:
            for change in changes:
                with self.subTest(change=change), self.assertRaises(ValueError):
                    movie.read_config(self.save(dict(deepcopy(config), **change)))
        allocate.assert_not_called()

    def test_normal_Movie_worker_uses_dynamic_request_factory_without_changing_inputs_or_results(self):
        self.runtime()
        source = MovieRecordSource.from_manifest(self.helper.manifests['movie-q1'])
        self.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text=str(r['source_position']==0)) for r in source.rows()})))
        results, messages = [], []
        for enabled in (False, True):
            config = self.dynamic_config() if enabled else self.rolling_config()
            path = self.save(config); output = self.root/('request-on' if enabled else 'request-off'); output.mkdir()
            with patch('src.execution_provider.adapters.lotus_records_core.create_records_core', wraps=create_records_core), \
                    redirect_stdout(io.StringIO()):
                self.assertEqual(movie.main(['run', '--source-manifest', str(source.manifest_path),
                    '--sembench-checkout', str(self.root), '--config', str(path),
                    '--output-dir', str(output), '--execute-model', '--worker']), 0)
            report = json.loads((output/'query.json').read_text())
            self.assertEqual(report['status'], 'completed')
            self.assertEqual(report['generation'], json.loads(json.dumps(config['generation'])))
            close = report['owner_close']
            results.append(report['result'])
            messages.append(sorted((c['source_position'], c['messages_sha256']) for c in close['calls']))
            self.assertEqual(close['status'], 'closed')
            self.assertEqual(close['core_usage']['held_tasks'], 0)
            if enabled:
                self.assertEqual(close['record_batching']['C_request'], 1)
                self.assertEqual(close['async_token_batches']['C_request'], 1)
                self.assertEqual(close['async_token_batches']['request_limit'], 2)
                self.assertTrue(close['request_supply_feedback']['sampler_stopped'])
                self.assertNotIn('batch_supply_feedback', close)
            else:self.assertNotIn('request_supply_feedback', close)
        self.assertEqual(results, [{'reviewId':['1']}, {'reviewId':['1']}])
        self.assertEqual(messages[0], messages[1])

    def test_isolated_session_uses_same_request_feedback_and_keeps_resident_unsupported(self):
        self.runtime(); config = self.dynamic_config()
        plan = dict(schema='semloom.movie_session_plan.v1', backend='semloom-core', lifecycle='isolated',
            model_name='openai/cpu-fixture', generation=config['generation'], engine=config['engine'],
            resources=config['resources'], profile=config['core_profile'], prepared_batching=config['prepared_batching'],
            rolling_refill=True, queries=[dict(query_id='request-session', task='movie-q1',
                manifest=self.helper.manifests['movie-q1'], phase='measured')])
        checked = session.check_plan(plan); query = plan['queries'][0]
        self.assertEqual((checked['record_batching']['C_request'], checked['record_batching']['R']), (1, 2))
        with self.assertRaises(ValueError):session.check_plan(dict(plan, lifecycle='resident'))
        source = MovieRecordSource.from_manifest(query['manifest'])
        self.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text=str(r['source_position']==0)) for r in source.rows()})))
        context = dict(owner_context={}, claims={'request-session':None}, sembench_checkout=self.root,
            checked_queries={q['query_id']:q for q in checked['queries']})
        driver = session._MovieDriver(plan, context, self.root/'request-session', query)
        with patch('integrations.imlane_records.chain_runtime.check_external_lease'):
            driver.start()
            try:
                report = driver.query(query, lambda event: None)
                self.assertEqual(report['result'], {'reviewId':['1']})
                close = report['backend_close']
                self.assertEqual(close['record_batching']['C_request'], 1)
                self.assertTrue(close['request_supply_feedback']['sampler_stopped'])
            finally:self.assertEqual(driver.close()['status'], 'closed')
