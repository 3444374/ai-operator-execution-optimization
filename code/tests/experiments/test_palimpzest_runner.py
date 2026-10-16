"""Public PZ configuration, query timing boundary and caller-owned cleanup fixtures."""
from io import StringIO
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.experiments.palimpzest import native, runner
from src.experiments.palimpzest.model import LocalModelBinding
from src.experiments.postgresql.sembench_queries import MOVIE_MANIFEST_DIGEST


def fixed_config(arm='original-native', options=None):
    return dict(schema='semloom.sembench_palimpzest_fixed.v1', query_id=3, max_attempts=4,
        query_timeout_s=120, native_options={'max_workers': 2, 'join_parallelism': 2},
        dataset=dict(checkout_env='BENCH', manifest_env='MANIFEST', csv_env='CSV',
                     manifest_content_digest=MOVIE_MANIFEST_DIGEST),
        palimpzest_checkout_env='PZ', model_config_env='MODEL', output_env='OUTPUT', host_scope_env='SCOPE',
        fixed_execution=dict(arm=arm, run_id='public-query',
            environment=dict(service_config_sha256='a'*64, service_cache_sha256='b'*64, dependency_lock_sha256='c'*64),
            backend_profile=dict(deployment_budget_sha256='d'*64, mode='fixed'),
            limits=None if arm == 'original-native' else dict(pz_workers=2, max_pending_calls=3, max_batch_rows=2),
            backend_options=options or {}))


class PublicRunnerTests(unittest.TestCase):
    def test_legacy_config_and_native_dispatch_remain_supported(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'config.json'
            value = fixed_config()
            path.write_text(json.dumps(value))
            self.assertEqual(native.load_config(path), value)
            value.pop('fixed_execution')
            value['schema'] = 'semloom.sembench_palimpzest.v1'
            path.write_text(json.dumps(value))
            self.assertEqual(native.load_config(path), value)
        with patch.object(native, 'load_config', return_value=value), \
                patch.object(native, 'run', return_value={'status': 'passed'}) as run, \
                patch.object(runner, 'run_fixed') as fixed, patch('sys.stdout', StringIO()):
            self.assertEqual(runner.main(['--config', 'fixture.json', '--execute']), 0)
        run.assert_called_once_with(value)
        fixed.assert_not_called()

    def test_fixed_entry_dispatches_execution_preflight_and_invalid_status(self):
        config = fixed_config()
        with patch.object(native, 'load_config', return_value=config), \
                patch.object(runner, 'run_fixed', return_value={'status': 'completed_with_invalid_predictions'}) as run, \
                patch.object(runner, 'preflight', return_value=(None, {'status': 'preflight'})) as preflight, \
                patch('sys.stdout', StringIO()):
            self.assertEqual(runner.main(['--config', 'fixture.json', '--execute']), 2)
            self.assertEqual(runner.main(['--config', 'fixture.json', '--data-preflight']), 0)
        run.assert_called_once_with(config)
        preflight.assert_called_once_with(config, libraries=False)

    def test_fixed_profile_rejects_limit_unknown_arm_and_implicit_runtime(self):
        for mutate in (lambda c: c.update(query_id=2),
                       lambda c: c['fixed_execution'].update(arm='unknown'),
                       lambda c: c['fixed_execution'].update(run_id=''),
                       lambda c: c['fixed_execution'].update(arm='ray-data', limits=dict(pz_workers=2, max_pending_calls=2, max_batch_rows=2),
                            backend_options=dict(graph_options={}, ray_address_env='ABSENT_PZ_RAY'))):
            config = fixed_config()
            mutate(config)
            with self.subTest(config=config), self.assertRaises(ValueError):
                runner.fixed_settings(config)

    def test_existing_native_executors_keep_backend_specific_concurrency(self):
        fresh = SimpleNamespace(run_id='query', semantic_operator_ids=('filter',))
        binding = LocalModelBinding(FixedModelConfig('http://127.0.0.1:8000/v1/chat/completions', 'fixture', 1000))
        for arm, options in (('native-adapted', dict(max_workers=7)),
                             ('daft', dict(graph_options=dict(concurrency=8))),
                             ('ray-data', dict(graph_options=dict(ray_actors=3, concurrency=8), ray_address_env='PZ_RAY'))):
            config = fixed_config(arm, options)
            with patch.dict(os.environ, {'PZ_RAY': '127.0.0.1:12345'}):
                settings = runner.fixed_settings(config)
            with patch.object(runner, 'NativeAdaptedExecutor') as adapted, patch.object(runner, 'NativeCallExecutor') as vendor:
                runner._executor(fresh, binding, config['fixed_execution'], settings, None, None)
                if arm == 'native-adapted':
                    self.assertEqual(adapted.call_args.kwargs['max_workers'], 7)
                    vendor.assert_not_called()
                else:
                    self.assertEqual(vendor.call_args.args[0], arm)
                    self.assertIs(vendor.call_args.args[1], settings['graph'])
                    adapted.assert_not_called()

    def test_imlane_modes_and_semloom_use_their_existing_typed_executors(self):
        fresh = SimpleNamespace(run_id='query', semantic_operator_ids=('filter',))
        binding = LocalModelBinding(FixedModelConfig('http://127.0.0.1:8000/v1/chat/completions', 'fixture', 1000))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for runtime in ('cpython', 'ray'):
                path = root/(runtime+'.json')
                path.write_text(json.dumps(dict(runtime=runtime, bridge_config_path=str(root/'bridge.json'),
                    coordinator_pid=123, coordinator_start_ticks=1, author_build_identity=str(root/'identity.json'),
                    n_executors=6)))
                config = fixed_config('imlane-'+runtime, dict(graph_options={}, call_config_env='PZ_IMLANE'))
                with patch.dict(os.environ, {'PZ_IMLANE': str(path)}):
                    settings = runner.fixed_settings(config)
                with patch.object(runner, 'IMLaneCallExecutor') as author:
                    runner._executor(fresh, binding, config['fixed_execution'], settings, None, None)
                self.assertEqual(author.call_args.args[0].runtime, runtime)
                self.assertEqual(author.call_args.args[0].n_executors, 6)
            path = root/'physical.json'
            path.write_text(json.dumps(dict(address='127.0.0.1:12345', workers=1, batch_rows=1,
                window_bytes=1048576, object_bytes=2097152, response_mode='native-call-full')))
            config = fixed_config('semloom', dict(max_active_requests=8, physical_config_env='PZ_PHYSICAL'))
            with patch.dict(os.environ, {'PZ_PHYSICAL': str(path)}):
                settings = runner.fixed_settings(config)
            route = SimpleNamespace(effective_endpoint='http://127.0.0.1:9000/v1/chat/completions')
            with patch('src.semantic_methods.palimpzest.semloom.SemLoomProgress') as core:
                runner._executor(fresh, binding, config['fixed_execution'], settings, route, None)
            self.assertEqual(core.call_args.args[0].endpoint_url, route.effective_endpoint)
            self.assertEqual(core.call_args.kwargs['max_active_requests'], 8)
            self.assertEqual(core.call_args.kwargs['operator_ids'], fresh.semantic_operator_ids)
            self.assertIs(core.call_args.kwargs['physical'], settings['physical'])


@unittest.skipUnless(os.environ.get('SEMLOOM_PZ_LIBRARY_TEST') == '1', 'requires pinned PZ record container; no HTTP/model')
class OwnedQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get('SEMLOOM_TEST_PALIMPZEST_ROOT'):
            from src.experiments.palimpzest.source import load_palimpzest
            load_palimpzest(Path(os.environ['SEMLOOM_TEST_PALIMPZEST_ROOT']))

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.binding = LocalModelBinding(FixedModelConfig('http://127.0.0.1:8000/v1/chat/completions', 'fixture', 1000))
        self.clock = [100_000_000_000]
        self.fixed = SimpleNamespace(describe=lambda: {}, selection_accounting={}, fresh=self.fresh)
        self.fresh_query = SimpleNamespace(run_id='public-query', semantic_operator_ids=('filter',))

    def fresh(self, run_id):
        self.clock[0] += 11_000_000_000
        return self.fresh_query

    def run_owned_fixture(self, config, execute, *, executor=None, gateway=None):
        prepared = (None, [], self.root/'pz', self.binding, self.root/'output', self.root/'scope', {})
        def make_executor(*args):
            self.clock[0] += 13_000_000_000
            return executor
        with patch.object(runner, 'preflight', return_value=prepared), \
                patch.object(runner, 'fetch_model_metadata', return_value=object()), \
                patch.object(runner, 'select_fixed_movie', return_value=self.fixed), \
                patch.object(runner, '_executor', side_effect=make_executor), \
                patch.object(runner, 'execute_fixed_movie', side_effect=execute), \
                patch.object(runner, 'ObservationGateway', return_value=gateway), \
                patch.object(runner.PreparedRequestRoute, 'from_gateway', return_value=SimpleNamespace(effective_endpoint=self.binding.model.endpoint_url)), \
                patch.object(runner.time, 'monotonic_ns', side_effect=lambda: self.clock[0]), \
                patch('sys.stdout', StringIO()):
            return runner.run_fixed(config)

    def test_outer_query_clock_covers_fresh_state_and_semloom_progress_construction(self):
        config = fixed_config('semloom', dict(max_active_requests=8, physical_config_env='PZ_PHYSICAL'))
        physical = self.root/'physical.json'
        physical.write_text(json.dumps(dict(address='127.0.0.1:12345', workers=1, batch_rows=1,
            window_bytes=1048576, object_bytes=2097152, response_mode='native-call-full')))
        ray = SimpleNamespace(is_initialized=lambda: True,
            get_runtime_context=lambda: SimpleNamespace(gcs_address='127.0.0.1:12345'))
        gateway, executor = Mock(), Mock()
        executor.close.return_value = True
        executor.last_close_report = None
        def execute(fresh, **kwargs):
            self.assertIs(fresh, self.fresh_query)
            self.assertEqual(kwargs['query_started_ns'], 100_000_000_000)
            self.assertEqual(self.clock[0], 124_000_000_000)
            kwargs['evidence'].update(status='completed', full_query_seconds=24, result_ready_ns=self.clock[0])
            self.clock[0] += 23_000_000_000  # evaluation follows result readiness
            return SimpleNamespace(records=[], evidence=kwargs['evidence'])
        with patch.dict(os.environ, {'PZ_PHYSICAL': str(physical)}), patch.dict('sys.modules', {'ray': ray}):
            summary = self.run_owned_fixture(config, execute, executor=executor, gateway=gateway)
        self.assertEqual(summary['execution']['full_query_seconds'], 24)
        self.assertFalse(summary['owner_cleanup_pending'])
        self.assertIsNone(summary['service_startup_seconds'])
        executor.close.assert_called_once()
        gateway.stop.assert_called_once()
        self.assertFalse((self.root/'scope/.runner-lease.json').exists())

    def test_active_strategy_failure_retains_sdk_dependencies_and_owner_lease(self):
        config = fixed_config('native-adapted', dict(max_workers=1))
        primary = ValueError('query fixture')
        strategy, executor, gateway = Mock(), Mock(), Mock()
        executor.last_close_report = None
        strategy.snapshot.return_value = dict(closed=False, unconfirmed_work=True)
        strategy.close.side_effect = RuntimeError('finish fixture')
        def execute(fresh, **kwargs):
            kwargs['owner']['strategy'] = strategy
            raise primary
        try:
            with self.assertRaises(ValueError) as caught:
                self.run_owned_fixture(config, execute, executor=executor, gateway=gateway)
            self.assertIs(caught.exception, primary)
            owner = primary.pz_execution_owner
            self.assertIs(owner['strategy'], strategy)
            executor.close.assert_not_called()
            gateway.stop.assert_not_called()
            self.assertTrue((self.root/'scope/.runner-lease.json').exists())
            summary = json.loads((self.root/'output/summary.json').read_text())
            self.assertEqual(summary['status'], 'failed')
            self.assertTrue(summary['owner_cleanup_pending'])
        finally:
            if hasattr(primary, 'pz_execution_owner'):
                primary.pz_execution_owner['events'].close()
                primary.pz_execution_owner['lease'].__exit__(None, None, None)

    def test_executor_unknown_report_before_strategy_creation_retains_owner(self):
        primary, executor, gateway = ValueError('before strategy fixture'), Mock(), Mock()
        executor.last_close_report = None
        executor.close.return_value = dict(closed=False, unconfirmed_work=True)
        def execute(fresh, **kwargs):
            raise primary
        try:
            with self.assertRaises(ValueError) as caught:
                self.run_owned_fixture(fixed_config('native-adapted', dict(max_workers=1)), execute,
                                       executor=executor, gateway=gateway)
            self.assertIs(caught.exception, primary)
            self.assertIs(primary.pz_execution_owner['executor'], executor)
            gateway.stop.assert_not_called()
            self.assertTrue((self.root/'scope/.runner-lease.json').exists())
        finally:
            if hasattr(primary, 'pz_execution_owner'):
                primary.pz_execution_owner['events'].close()
                primary.pz_execution_owner['lease'].__exit__(None, None, None)

    def test_gateway_cleanup_failure_retains_owner_and_lease_after_other_safe_cleanup(self):
        primary, executor, gateway = ValueError('original query fixture'), Mock(), Mock()
        executor.last_close_report = None
        executor.close.return_value = True
        gateway.stop.side_effect = RuntimeError('gateway stop fixture')
        def execute(fresh, **kwargs):
            raise primary
        try:
            with self.assertRaises(ValueError) as caught:
                self.run_owned_fixture(fixed_config('native-adapted', dict(max_workers=1)), execute,
                                       executor=executor, gateway=gateway)
            self.assertIs(caught.exception, primary)
            self.assertIs(primary.pz_execution_owner['gateway'], gateway)
            self.assertIn('events', primary.pz_execution_owner['closed_resources'])
            self.assertTrue((self.root/'scope/.runner-lease.json').exists())
            summary = json.loads((self.root/'output/summary.json').read_text())
            self.assertTrue(summary['owner_cleanup_pending'])
            self.assertEqual(summary['error']['type'], 'ValueError')
        finally:
            if hasattr(primary, 'pz_execution_owner'):
                primary.pz_execution_owner['lease'].__exit__(None, None, None)

    def test_imlane_existing_external_cleanup_marker_retains_owner_on_none_close_return(self):
        primary, executor, gateway = ValueError('IMLane driver fixture'), Mock(), Mock()
        executor.close.return_value = None
        executor.last_close_report = dict(clean=False, external_coordinator_requires_cleanup=True)
        def execute(fresh, **kwargs):
            raise primary
        try:
            with self.assertRaises(ValueError):
                self.run_owned_fixture(fixed_config('native-adapted', dict(max_workers=1)), execute,
                                       executor=executor, gateway=gateway)
            self.assertIs(primary.pz_execution_owner['executor'], executor)
            gateway.stop.assert_not_called()
            self.assertTrue((self.root/'scope/.runner-lease.json').exists())
            self.assertTrue(json.loads((self.root/'output/summary.json').read_text())['owner_cleanup_pending'])
        finally:
            if hasattr(primary, 'pz_execution_owner'):
                primary.pz_execution_owner['events'].close()
                primary.pz_execution_owner['lease'].__exit__(None, None, None)

    def test_call_evidence_write_failure_is_saved_as_failed_summary(self):
        first = OSError('call evidence fixture')
        original_write = runner.write_private_json
        def write(path, value):
            if path.name == 'generator-calls.json':
                raise first
            return original_write(path, value)
        def execute(fresh, **kwargs):
            kwargs['evidence']['status'] = 'completed'
            return SimpleNamespace(records=[], evidence=kwargs['evidence'])
        with patch.object(runner, 'write_private_json', side_effect=write), self.assertRaises(OSError) as caught:
            self.run_owned_fixture(fixed_config(), execute)
        self.assertIs(caught.exception, first)
        summary = json.loads((self.root/'output/summary.json').read_text())
        self.assertEqual(summary['status'], 'failed')
        self.assertEqual(summary['error']['type'], 'OSError')
        self.assertFalse(summary['owner_cleanup_pending'])
        self.assertFalse((self.root/'scope/.runner-lease.json').exists())


if __name__ == '__main__':
    unittest.main()
