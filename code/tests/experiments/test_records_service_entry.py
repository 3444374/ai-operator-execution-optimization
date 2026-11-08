"""Declared service payload bytes through normal Movie CLI and session entries."""
from contextlib import redirect_stdout
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource
from src.experiments import movie_records_worker as worker, movie_records_query as query
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.experiments import movie_records_service as service_results
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.records_service_budget import RecordsPayloadPool
from tests.experiments import test_movie_async_token_entry as entry
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture


def runtime_identity(runtime):
    identity = dict(job_id='service-cpu-job', node_id='service-cpu-node')
    runtime.ray.get_runtime_context = lambda:SimpleNamespace(
        get_job_id=lambda:identity['job_id'], get_node_id=lambda:identity['node_id'])
    return identity


class ServiceEntryTests(unittest.TestCase):
    def setUp(self):
        self.case = entry.AsyncTokenEntryTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def config(self):
        config = self.case.config()
        config.pop('prepared_batching')
        config.pop('batch_concurrency')
        config.pop('core_profile')
        config.update(service_payload_bytes=8*1024**2, resources={},
            core_capacity=dict(item_input_bytes=8192, item_result_bytes=8192))
        return config

    def test_config_and_check_preview_allocate_no_owner_and_do_not_inherit_R(self):
        config = self.config()
        output = io.StringIO()
        with patch.object(RecordsPayloadPool, 'allocate') as reserve, \
                patch.object(core, 'create_records_core') as create, \
                patch.object(movie, 'model_preflight', return_value=dict(inference_calls=0)), \
                redirect_stdout(output):
            _, _, _, resources, _ = movie.read_config(self.case.save(config))
            self.assertIsNone(resources.max_active_batches)
            self.assertEqual((resources.prepare_batch_rows, resources.model_batch_rows), (1, 1))
            self.assertEqual(movie.main(['check', '--source-manifest', self.case.helper.manifests['movie-q1'],
                '--sembench-checkout', str(self.case.root), '--config', str(self.case.save(config))]), 0)
        reserve.assert_not_called()
        create.assert_not_called()
        checked = json.loads(output.getvalue())['core_execution']
        self.assertGreater(checked['owner_max_inflight'], 8)
        self.assertEqual(checked['declared_legacy_request_budget'], None)
        self.assertEqual(checked['service_payload_bytes'], config['service_payload_bytes'])
        self.assertFalse(checked['service_allocation_acquired'])

    def test_invalid_service_bytes_and_conflicts_fail_in_CPU_preflight(self):
        config = self.config()
        variants = [dict(config, service_payload_bytes=value) for value in (None, True, 0, -1, 2.5)]
        variants += [dict(config, resources=dict(max_active_batches=2)),
            dict(config, core_profile=dict(mode='sync')), dict(config, async_limits={}),
            dict(config, admission_cache={}), dict(config, backend='daft-native-local-model')]
        with patch.object(core, 'create_records_core') as create:
            for value in variants:
                with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                    movie.read_config(self.case.save(value))
        create.assert_not_called()

    def test_normal_worker_acquires_before_Ray_and_preserves_query_and_pool_report(self):
        config = self.config()
        runtime = self.case.runtime()
        runtime_identity(runtime)
        source = MovieRecordSource.from_manifest(self.case.helper.manifests['movie-q1'])
        runtime.rows = source.rows()
        self.case.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text=str(r['source_position']==0)) for r in source.rows()})))
        output = self.case.root/'service-worker'
        output.mkdir()
        owners = []
        factory = core.create_records_core
        def create(*args, **kwargs):
            self.assertFalse(runtime.ray.is_initialized(), 'service grant must precede Ray initialization')
            owner = factory(*args, **kwargs)
            if all(item is not owner for item in owners):
                owners.append(owner)
            return owner
        with patch.object(core, 'create_records_core', side_effect=create), redirect_stdout(io.StringIO()):
            self.assertEqual(movie.main(['run', '--source-manifest', str(source.manifest_path),
                '--sembench-checkout', str(self.case.root), '--config', str(self.case.save(config)),
                '--output-dir', str(output), '--execute-model', '--worker']), 0)
        self.assertEqual(len(owners), 1)
        report = json.loads((output/'query.json').read_text())
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(report['result'], {'reviewId':['1']})
        self.assertEqual(report['generation'], json.loads(json.dumps(config['generation'])))
        self.assertGreater(owners[0].resources.max_active_batches, 8)
        self.assertEqual(report['service_payload_pool']['allocated_bytes'], 0)
        self.assertEqual(report['service_payload_pool']['status'], 'closed')
        self.assertEqual(report['owner_close']['service_allocation']['status'], 'released')

    def test_direct_session_start_rejects_bad_bytes_before_directory_or_Ray(self):
        runtime = self.case.runtime()
        config = self.config()
        plan = dict(schema='semloom.movie_session_plan.v1', backend='semloom-core', lifecycle='isolated',
            engine=config['engine'], generation=config['generation'], resources={}, profile={},
            model_name='openai/cpu-fixture', service_payload_bytes=True, queries=[])
        output = self.case.root/'invalid-service-session'
        driver = session._MovieDriver(plan, {}, output, dict(task='movie-q1'))
        with self.assertRaises((ValueError, TypeError)):
            driver.start()
        runtime.ray.init.assert_not_called()
        self.assertFalse(output.exists())

    def test_worker_checks_runtime_before_first_model_cleanup_RPC(self):
        runtime = self.case.runtime()
        identity = runtime_identity(runtime)
        source = MovieRecordSource.from_manifest(self.case.helper.manifests['movie-q1'])
        runtime.rows = source.rows()
        self.case.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text=str(r['source_position']==0)) for r in source.rows()})))
        config = self.config()
        output = self.case.root/'service-worker-runtime-change'
        output.mkdir()
        original = query.run_movie_query
        def replace_runtime(*args, **kwargs):
            result = original(*args, **kwargs)
            identity['job_id'] = 'replacement-service-job'
            return result
        try:
            with patch.object(query, 'run_movie_query', side_effect=replace_runtime), \
                    patch.object(core.SemLoomRecordsCore, 'close', autospec=True) as close, \
                    redirect_stdout(io.StringIO()):
                with self.assertRaises(RuntimeError) as caught:
                    movie.main(['run', '--source-manifest', str(source.manifest_path),
                        '--sembench-checkout', str(self.case.root), '--config', str(self.case.save(config)),
                        '--output-dir', str(output), '--execute-model', '--worker'])
            close.assert_not_called()
            runtime.ray.shutdown.assert_not_called()
            self.assertEqual(caught.exception.records_payload_pool.snapshot()['allocations'], 1)
            self.assertEqual(caught.exception.query_report['status'], 'failed')
        finally:
            identity['job_id'] = 'service-cpu-job'
            if 'caught' in locals():
                caught.exception.records_owner.close(timeout=3)
            runtime.ray.shutdown()

    def test_worker_keeps_an_explicit_runtime_finisher_after_shutdown_failure(self):
        runtime = self.case.runtime()
        runtime_identity(runtime)
        source = MovieRecordSource.from_manifest(self.case.helper.manifests['movie-q1'])
        runtime.rows = source.rows()
        self.case.model.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text=str(r['source_position']==0)) for r in source.rows()})))
        config, method, generation, resources, engine = movie.read_config(self.case.save(self.config()))
        directory = self.case.root/'worker-runtime-finisher'
        directory.mkdir()
        shutdown = runtime.ray.shutdown.side_effect
        runtime.ray.shutdown.side_effect = OSError('CPU first shutdown failed')
        events = []
        try:
            with self.assertRaises(OSError) as caught:
                worker.run_model_worker(config, source, self.case.root, method, generation, resources, engine,
                                    directory, observer=events.append)
            error = caught.exception
            self.assertEqual(error.query_report['status'], 'failed')
            self.assertEqual(error.records_payload_pool.snapshot()['allocated_bytes'], 0)
            self.assertIs(error.records_runtime[0], runtime.ray)
            self.assertIn('runtime_close_unconfirmed', events)
            self.assertNotIn('runtime_closed', events)
            self.assertTrue(runtime.ray.is_initialized())
            runtime.ray.shutdown.side_effect = shutdown
            self.assertEqual(error.records_finish_runtime()['status'], 'closed')
            self.assertFalse(runtime.ray.is_initialized())
        finally:
            runtime.ray.shutdown.side_effect = shutdown
            runtime.ray.shutdown()


class ServiceSessionTests(unittest.TestCase):
    def fixture_plan(self):
        fixture = MovieTokenResidentFixture(self)
        runtime_identity(fixture.runtime)
        plan = fixture.plan('RF')
        plan.pop('prepared_batching')
        plan.pop('rolling_refill')
        plan.update(service_payload_bytes=8*1024**2, resources={}, profile={},
            core_capacity=dict(item_input_bytes=8192, item_result_bytes=8192))
        fixture.runtime.rows = MovieRecordSource.from_manifest(fixture.manifests['movie-q1']).rows()
        return fixture, plan

    def test_resident_and_isolated_queries_use_one_process_pool_with_original_results(self):
        for lifecycle in ('resident', 'isolated'):
            with self.subTest(lifecycle=lifecycle):
                fixture = MovieTokenResidentFixture(self)
                runtime_identity(fixture.runtime)
                plan = fixture.plan('RF', lifecycle=lifecycle)
                plan.pop('prepared_batching')
                plan.pop('rolling_refill')
                plan.update(service_payload_bytes=8*1024**2, resources={}, profile={},
                    core_capacity=dict(item_input_bytes=8192, item_result_bytes=8192))
                fixture.runtime.rows = MovieRecordSource.from_manifest(fixture.manifests['movie-q1']).rows()
                checked = session.check_plan(plan)
                self.assertGreater(checked['core_execution']['owner_max_inflight'], 8)
                self.assertFalse(checked['core_execution']['service_allocation_acquired'])
                report = fixture.run(plan)
                self.assertEqual(report['status'], 'completed')
                self.assertEqual([q['backend_report']['result'] for q in report['queries']],
                                 [{'reviewId':['1']}, {'reviewId':['1']}])
                owners = list({id(owner):owner for owner in fixture.owners}.values())
                self.assertEqual(len(owners), 1 if lifecycle=='resident' else 2)
                self.assertEqual(len(fixture.runtime.async_actors), len(owners))
                pools = {id(owner._service_allocation._pool) for owner in owners}
                self.assertEqual(len(pools), 1)
                for owner in owners:
                    self.assertEqual(owner._service_allocation.describe()['status'], 'released')
                self.assertEqual(report['service_payload_pool']['status'], 'closed')
                self.assertEqual(report['service_payload_pool']['allocated_bytes'], 0)

    def test_explicit_empty_context_slot_still_creates_an_owned_pool(self):
        fixture, plan = self.fixture_plan()
        context = fixture.context(plan)
        context['_records_payload_pool'] = None
        report = fixture.run(plan, context=context)
        self.assertEqual(report['service_payload_pool']['status'], 'closed')

    def test_external_pool_remains_open_after_all_session_queries(self):
        fixture, plan = self.fixture_plan()
        pool = RecordsPayloadPool(plan['service_payload_bytes'])
        context = dict(fixture.context(plan), _records_payload_pool=pool)
        report = fixture.run(plan, context=context)
        self.assertEqual(report['service_payload_pool']['status'], 'open')
        self.assertEqual(pool.snapshot()['allocations'], 0)
        self.assertEqual(report['service_payload_pool']['allocated_bytes'], 0)

    def test_partial_pool_cannot_silently_shrink_the_checked_owner_before_Ray(self):
        from src.experiments.movie_records_config import core_capacity
        fixture, plan = self.fixture_plan()
        pool = RecordsPayloadPool(plan['service_payload_bytes'])
        other = pool.allocate(core_capacity(plan), max_bytes=plan['service_payload_bytes']//2)
        self.addCleanup(other.cancel)
        context = dict(fixture.context(plan), _records_payload_pool=pool)
        directory = fixture.root/'partially-held-service'
        driver = session._MovieDriver(plan, context, directory, plan['queries'][0])
        with self.assertRaisesRegex(ValueError, 'complete checked payload plan'):
            driver.start()
        fixture.runtime.ray.init.assert_not_called()
        self.assertFalse(directory.exists())
        self.assertEqual(pool.snapshot()['allocations'], 1)
        self.assertEqual(pool.snapshot()['allocated_bytes'], other.plan.bytes)

    def test_replacement_runtime_is_not_shutdown_on_a_repeated_service_close(self):
        fixture, plan = self.fixture_plan()
        identity = runtime_identity(fixture.runtime)
        checked = session.check_plan(plan)
        context = dict(fixture.context(plan), checked_queries={q['query_id']:q for q in checked['queries']})
        driver = session._MovieDriver(plan, context, fixture.root/'changed-service-runtime', plan['queries'][0])
        driver.start()
        try:
            self.assertEqual(driver.close()['status'], 'closed')
            fixture.runtime.ray.init()
            fixture.runtime.ray.shutdown.reset_mock()
            identity['job_id'] = 'replacement-service-job'
            report = driver.close()
            self.assertEqual(report['status'], 'unknown')
            self.assertIn('owned Ray shutdown failed', report['cleanup_errors'])
            fixture.runtime.ray.shutdown.assert_not_called()
        finally:
            identity['job_id'] = 'service-cpu-job'
            fixture.runtime.ray.shutdown()

    def test_first_close_checks_original_runtime_before_any_owner_RPC(self):
        for changed in ('stopped', 'replacement'):
            with self.subTest(runtime=changed):
                fixture, plan = self.fixture_plan()
                identity = runtime_identity(fixture.runtime)
                driver = session._MovieDriver(plan, {}, fixture.root/('service-close-'+changed),
                                              plan['queries'][0])
                driver.start()
                try:
                    if changed == 'stopped':
                        fixture.runtime.ray.shutdown()
                    else:
                        identity['job_id'] = 'replacement-service-job'
                    fixture.runtime.ray.init.reset_mock()
                    fixture.runtime.ray.shutdown.reset_mock()
                    with patch.object(driver.owner, 'close', wraps=driver.owner.close) as close:
                        report = driver.close()
                    self.assertEqual(report['status'], 'unknown')
                    close.assert_not_called()
                    fixture.runtime.ray.init.assert_not_called()
                    fixture.runtime.ray.shutdown.assert_not_called()
                    self.assertEqual(driver.payload_pool.snapshot()['allocations'], 1)
                finally:
                    identity['job_id'] = 'service-cpu-job'
                    if not fixture.runtime.ray.is_initialized():
                        fixture.runtime.ray.init()
                    driver.owner.close(timeout=3)
                    fixture.runtime.ray.shutdown()

    def test_runtime_replaced_between_entry_binding_and_start_cannot_create_an_actor(self):
        fixture, plan = self.fixture_plan()
        identity = runtime_identity(fixture.runtime)
        driver = session._MovieDriver(plan, {}, fixture.root/'changed-before-first-start', plan['queries'][0])
        original = core.SemLoomRecordsCore.start
        def start(owner):
            identity['job_id'] = 'replacement-service-job'
            return original(owner)
        try:
            with patch.object(core.SemLoomRecordsCore, 'start', start):
                with self.assertRaises(RuntimeError):
                    driver.start()
            self.assertEqual(len(fixture.runtime.async_actors), 0)
            fixture.runtime.ray.shutdown.reset_mock()
            self.assertEqual(driver.close()['status'], 'unknown')
            fixture.runtime.ray.shutdown.assert_not_called()
            self.assertEqual(driver.payload_pool.snapshot()['allocations'], 0)
        finally:
            identity['job_id'] = 'service-cpu-job'
            fixture.runtime.ray.shutdown()

    def test_final_pool_recording_failure_preserves_complete_report_and_recovery_handles(self):
        fixture, plan = self.fixture_plan()
        failure = OSError('CPU final pool evidence failed')
        with patch.object(service_results, 'finish_service_pool', side_effect=failure):
            with self.assertRaises(OSError) as caught:
                fixture.run(plan)
        self.assertIs(caught.exception, failure)
        self.assertEqual(failure.session_report['status'], 'failed')
        self.assertEqual([q['status'] for q in failure.session_report['queries']], ['completed','completed'])
        self.assertEqual(failure.records_payload_pool.snapshot()['allocated_bytes'], 0)
        self.assertEqual(failure.records_payload_pool.snapshot()['status'], 'closed')
        self.assertEqual(failure.records_payload_drivers, {})

    def test_runtime_shutdown_failure_keeps_the_same_driver_after_its_bytes_return(self):
        fixture, plan = self.fixture_plan()
        shutdown = fixture.runtime.ray.shutdown.side_effect
        fixture.runtime.ray.shutdown.side_effect = OSError('CPU first session shutdown failed')
        try:
            with self.assertRaises(RuntimeError) as caught:
                fixture.run(plan)
            error = caught.exception
            self.assertEqual(error.records_payload_pool.snapshot()['allocated_bytes'], 0)
            driver, = error.records_payload_drivers.values()
            self.assertTrue(fixture.runtime.ray.is_initialized())
            self.assertEqual(driver.owner._service_allocation.describe()['status'], 'released')
            fixture.runtime.ray.shutdown.side_effect = shutdown
            self.assertEqual(driver.close()['status'], 'closed')
            self.assertFalse(fixture.runtime.ray.is_initialized())
            self.assertEqual(driver.context['_records_payload_drivers'], {})
        finally:
            fixture.runtime.ray.shutdown.side_effect = shutdown
            fixture.runtime.ray.shutdown()


if __name__ == '__main__':
    unittest.main()
