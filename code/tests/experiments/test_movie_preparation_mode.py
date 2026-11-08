"""Opt-in preparation through the maintained Movie entries; CPU fixtures only."""
from contextlib import contextmanager, redirect_stdout
from copy import deepcopy
from dataclasses import asdict
import io
import json
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.experiments.calibration import core as calibration
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.records_profile import resolve_record_batching
from tests.experiments import test_movie_async_token_entry as entry
from tests.experiments import test_core_calibration as calibration_fixture
from tests.experiments import test_session_batching_preflight as preflight
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture, query_close, query_calls
from tests.experiments import test_records_resource_budget as budgets


@contextmanager
def fixture():
    case = entry.AsyncTokenEntryTests()
    case.setUp()
    try:
        yield case
    finally:
        case.doCleanups()


class PreparationModeEntryTests(unittest.TestCase):
    def options(self, mode):
        return dict(interleaved_preparation=mode != 'serial',
                    event_driven_preparation=mode == 'event_driven')

    def test_absent_and_explicit_serial_keep_the_existing_canonical_metadata(self):
        with fixture() as case:
            config = case.rolling_config()
            resolved = movie._records_runtime(config).profile
            original = resolve_record_batching(resolved, **movie._record_batching_options(config))
            explicit = resolve_record_batching(resolved,
                **movie._record_batching_options(dict(config, preparation_mode='serial')))
            self.assertEqual(explicit, original)
            self.assertNotIn('interleaved_preparation', explicit)
            self.assertNotIn('event_driven_preparation', explicit)

    def test_unknown_or_nonstring_mode_is_rejected_before_owner_allocation(self):
        with fixture() as case:
            for mode in ('unknown', '', None, True, 1, []):
                with self.subTest(mode=mode), patch.object(core, 'create_records_core') as allocate:
                    with self.assertRaisesRegex(ValueError, 'preparation_mode'):
                        movie.read_config(case.save(dict(case.rolling_config(), preparation_mode=mode)))
                    allocate.assert_not_called()

    def test_check_CLI_reports_each_mode_without_creating_an_owner(self):
        with fixture() as case:
            for mode in ('serial', 'interleaved', 'event_driven'):
                out = io.StringIO()
                path = case.save(dict(case.rolling_config(), preparation_mode=mode))
                with self.subTest(mode=mode), patch.object(movie, 'model_preflight',
                        return_value=dict(status='CPU-only', inference_calls=0)), \
                        patch.object(core, 'create_records_core') as allocate, redirect_stdout(out):
                    self.assertEqual(movie.main(['check', '--source-manifest', case.helper.manifests['movie-q1'],
                        '--sembench-checkout', str(case.root), '--config', str(path)]), 0)
                allocate.assert_not_called()
                batching = json.loads(out.getvalue())['record_batching']
                for name, expected in self.options(mode).items():
                    self.assertIs(batching.get(name, False), expected)
                self.assertEqual(batching['B'], case.rolling_config()['prepared_batching']['token_budget'])

    def test_rolling_required_and_no_legacy_feedback_or_other_backends(self):
        with fixture() as case:
            config = dict(case.rolling_config(), preparation_mode='event_driven')
            variants = [dict(config, rolling_refill=False),
                dict(config, core_profile=dict(mode='async', admission=dict(kind='legacy_resource_feedback'))),
                dict(config, batch_feedback=dict(max_batches=2)),
                dict(config, backend='daft-native-local-model')]
            for value in variants:
                with self.subTest(value=value), self.assertRaises(ValueError):
                    movie.read_config(case.save(value))

    def test_check_CLI_combines_existing_preparation_modes_with_current_feedback(self):
        with fixture() as case:
            for mode in ('interleaved', 'event_driven'):
                config = dict(case.rolling_config(), preparation_mode=mode,
                    core_profile=dict(mode='async', admission=dict(kind='resource_feedback')))
                path = case.save(config)
                output = io.StringIO()
                with self.subTest(mode=mode), patch.object(movie, 'model_preflight',
                        return_value=dict(status='CPU-only', inference_calls=0)), \
                        patch.object(core, 'create_records_core') as allocate, redirect_stdout(output):
                    self.assertEqual(movie.main(['check', '--source-manifest', case.helper.manifests['movie-q1'],
                        '--sembench-checkout', str(case.root), '--config', str(path)]), 0)
                allocate.assert_not_called()
                batching = json.loads(output.getvalue())['record_batching']
                self.assertEqual(batching['request_feedback_kind'], 'resource_feedback')
                self.assertEqual(batching['C_request'], 1)
                for name, expected in self.options(mode).items():
                    self.assertIs(batching.get(name, False), expected)

    def test_run_worker_CLI_reaches_existing_factory_and_preserves_results(self):
        results = {}
        for mode in ('serial', 'interleaved', 'event_driven'):
            with self.subTest(mode=mode), fixture() as case:
                case.runtime()
                config = dict(case.rolling_config(), preparation_mode=mode)
                source = MovieRecordSource.from_manifest(case.helper.manifests['movie-q1'])
                case.model.write_text(json.dumps(dict(responses={
                    'filter|'+row['row_id']:dict(text=str(row['source_position']==0)) for row in source.rows()})))
                output = case.root/'preparation-run';output.mkdir()
                with patch.object(core, 'create_records_core', wraps=core.create_records_core) as allocate, \
                        redirect_stdout(io.StringIO()):
                    self.assertEqual(movie.main(['run', '--source-manifest', str(source.manifest_path),
                        '--sembench-checkout', str(case.root), '--config', str(case.save(config)),
                        '--output-dir', str(output), '--execute-model', '--worker']), 0)
                report = json.loads((output/'query.json').read_text())
                for name, expected in self.options(mode).items():
                    self.assertIs(allocate.call_args.kwargs.get(name, False), expected)
                    self.assertIs(report['owner_close']['record_batching'].get(name, False), expected)
                self.assertEqual(report['owner_close']['status'], 'closed')
                self.assertEqual(report['owner_close']['core_usage']['held_tasks'], 0)
                results[mode] = dict(result=report['result'], generation=report['generation'])
        self.assertEqual(set(results), {'serial', 'interleaved', 'event_driven'})
        self.assertEqual(results['serial'], results['interleaved'])
        self.assertEqual(results['serial'], results['event_driven'])

    def test_resident_queries_reuse_one_owner_with_original_prompts_and_generations(self):
        results = {}
        for mode in ('serial', 'interleaved', 'event_driven'):
            with self.subTest(mode=mode):
                f = MovieTokenResidentFixture(self)
                plan = dict(f.plan('RF'), preparation_mode=mode)
                checked = session.check_plan(plan)
                for name, expected in self.options(mode).items():
                    self.assertIs(checked['record_batching'].get(name, False), expected)
                report = f.run(plan)
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(sum(x['event']=='construct' for x in f.events()), 1)
                self.assertEqual(sum(x['event']=='close' for x in f.events()), 1)
                for query in report['queries']:
                    self.assertEqual(query_close(query)['core_usage']['held_tasks'], 0)
                results[mode] = [dict(result=q['backend_report']['result'],
                    calls=[{k:c[k] for k in ('call_id','row_id','fixture_messages','fixture_generation','status','text')}
                           for c in query_calls(q)]) for q in report['queries']]
        self.assertEqual(set(results), {'serial', 'interleaved', 'event_driven'})
        self.assertEqual(results['serial'], results['interleaved'])
        self.assertEqual(results['serial'], results['event_driven'])

    def test_direct_start_rejects_invalid_mode_before_directory_or_Ray(self):
        case = preflight.SessionBatchingPreflightTests();case.setUp();self.addCleanup(case.doCleanups)
        plan = dict(case.batched(), preparation_mode='event_driven', rolling_refill=False)
        for lifecycle in ('resident', 'isolated'):
            with self.subTest(lifecycle=lifecycle):
                case.reject_before_runtime(dict(plan, lifecycle=lifecycle), reason='rolling')

    def test_direct_start_rejects_other_backends_before_directory_or_Ray(self):
        case = preflight.SessionBatchingPreflightTests();case.setUp();self.addCleanup(case.doCleanups)
        for backend in ('semloom-static-reference', 'daft', 'ray-data', 'imlane'):
            plan = dict(case.batched(), backend=backend, preparation_mode='event_driven')
            with self.subTest(backend=backend):
                case.reject_before_runtime(plan, reason='preparation_mode requires semloom-core')

    def test_calibration_identity_separates_preparation_and_normalizes_serial(self):
        case = calibration_fixture.CoreCalibrationTests();case.setUp();self.addCleanup(case.doCleanups)
        plan = dict(deepcopy(case.plan), profile=dict(mode='async', max_inflight=2),
            prepared_batching=dict(policy='token_budget', token_budget=10000), rolling_refill=True)
        def identity(value):
            return calibration.build_core_identity(value,case.checked,
                hardware=case.hardware,model=case.model,software=case.software)
        original = identity(plan)
        self.assertEqual(original, identity(dict(plan, preparation_mode='serial')))
        interleaved = identity(dict(plan, preparation_mode='interleaved'))
        events = identity(dict(plan, preparation_mode='event_driven'))
        self.assertNotEqual(original, interleaved)
        self.assertNotEqual(interleaved, events)
        self.assertEqual(case.plan.get('preparation_mode'), None)

    def test_run_worker_supports_None_with_complete_independent_budgets(self):
        with fixture() as case:
            case.runtime()
            config = dict(case.rolling_config(), preparation_mode='event_driven',
                core_capacity=asdict(budgets.capacity()), async_limits=budgets.ipc())
            config['resources']['max_active_batches'] = None
            source = MovieRecordSource.from_manifest(case.helper.manifests['movie-q1'])
            case.model.write_text(json.dumps(dict(responses={
                'filter|'+row['row_id']:dict(text='True') for row in source.rows()})))
            output = case.root/'independent-run';output.mkdir()
            with redirect_stdout(io.StringIO()):
                self.assertEqual(movie.main(['run', '--source-manifest', str(source.manifest_path),
                    '--sembench-checkout', str(case.root), '--config', str(case.save(config)),
                    '--output-dir', str(output), '--execute-model', '--worker']), 0)
            report = json.loads((output/'query.json').read_text())
            self.assertEqual(report['status'], 'completed')
            self.assertEqual(report['owner_close']['record_batching']['request_limit'], 4)
            self.assertEqual(report['owner_close']['record_batching']['C_request'], 2)
            self.assertEqual(report['owner_close']['core_usage']['held_tasks'], 0)
            self.assertIsNone(config['resources']['max_active_batches'])

    def test_late_context_error_keeps_legal_prefix_receipts_and_never_completes_query(self):
        prefix_counts = {}
        for mode in ('serial', 'event_driven'):
            with self.subTest(mode=mode):
                f = MovieTokenResidentFixture(self)
                manifest = f.source_manifest('late-context-'+mode,
                    [(str(i), 'taken_3', 'x '*10000 if i == 4 else 'valid', 'POSITIVE') for i in range(7)],
                    task='movie-q1')
                source = MovieRecordSource.from_manifest(manifest)
                f.replies = {'filter|'+row['row_id']:dict(text='True') for row in source.rows()}
                f.write_replies()
                config = dict(f.helper.rolling_config(), preparation_mode=mode)
                config['engine']['max_model_calls'] = 7
                output = f.root/('late-context-output-'+mode);output.mkdir()
                with self.assertRaises(ValueError), redirect_stdout(io.StringIO()):
                    movie.main(['run', '--source-manifest', str(source.manifest_path),
                        '--sembench-checkout', str(f.root), '--config', str(f.helper.save(config)),
                        '--output-dir', str(output), '--execute-model', '--worker'])
                report = json.loads((output/'query.json').read_text())
                self.assertEqual(report['status'], 'failed')
                close = report['owner_close']
                self.assertFalse(close['query_completed'])
                self.assertEqual(close['core_usage']['held_tasks'], 0)
                succeeded = [call for call in close['calls'] if call['status']=='succeeded']
                self.assertTrue(all(call['prompt_tokens'] > 0 and call['completion_tokens'] > 0
                                    for call in succeeded))
                self.assertTrue(all(call['source_position'] < 4 for call in succeeded))
                native = [event for event in f.events()
                          if event['event']=='end' and event['status']=='succeeded']
                self.assertEqual({event['token'] for event in native},
                                 {'filter|'+call['row_id'] for call in succeeded})
                prefix_counts[mode] = len(succeeded)
        self.assertEqual(set(prefix_counts), {'serial', 'event_driven'})
        self.assertTrue(all(0 <= count <= 4 for count in prefix_counts.values()))
        self.assertGreater(prefix_counts['event_driven'], 0)


if __name__ == '__main__':
    unittest.main()
