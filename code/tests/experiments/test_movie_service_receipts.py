"""Private accounting must be durable before a service accepts retained results."""
from copy import deepcopy
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from unittest.mock import Mock
import sys

from src.infrastructure.private_artifacts import open_private_text
from src.experiments import movie_records_service as service_results
from src.experiments import movie_records_session as session
from src.semantic_methods.lotus.records import CallLedger
from tests.execution_provider import test_records_service_owner as fixture
from tests.execution_provider import test_service_feedback_rpc_retention as snapshots


class ServiceReceiptTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.ServiceOwnerTests()
        self.case.setUp()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)

    def _closed_result(self, text, check):
        rows = self.case.rows(text)
        source = fixture.source_fixture.Source(rows)
        expected = source.expected('persist-receipts')
        ledger = CallLedger(expected, len(expected))
        ledger.mark_unconfirmed('CPU query cancellation')
        report = dict(status='failed', calls=ledger.report(), generation=dict(stop=('\n',)),
            runner_unconfirmed_call_ids=[v['call_id'] for v in expected])
        with fixture.fixtures.ThreadedRuntime(rows) as runtime:
            owner = self.case.owner()
            try:
                stream = owner.evaluate(source, query_id='persist-receipts')
                stream._fill()
                def admitted():
                    stream.session.advance(1)
                    return runtime.async_actors[0].instance.engine.snapshot()['accepted_requests'] == 1
                self.case.until(admitted)
                stream.close()
                self.assertEqual(owner.close(timeout=3)['status'], 'closed')
                self.assertEqual(len(owner.last_query_report['unconsumed_call_results']), 1)
                self.assertEqual(self.case.pool.snapshot()['allocations'], 1)
                check(owner, report, expected)
            finally:
                owner.close(timeout=3)

    def test_failed_persistence_does_not_transfer_or_return_the_grant(self):
        def check(owner, report, expected):
            with patch.object(service_results, 'write_private_json', side_effect=OSError('CPU disk full')), \
                    patch.object(owner, 'take_unconsumed_results', wraps=owner.take_unconsumed_results) as take:
                with self.assertRaises(OSError):
                    service_results.accept_service_results(owner, report, expected, self.path)
                take.assert_not_called()
            self.assertEqual(self.case.pool.snapshot()['allocations'], 1)
            accepted = service_results.accept_service_results(owner, report, expected, self.path)
            saved = json.loads((self.path/accepted['path']).read_text())
            self.assertEqual(saved['retained'][0]['prompt_tokens'], 11)
            self.assertEqual(saved['query']['generation']['stop'], ['\n'])
            self.assertEqual(self.case.pool.snapshot()['allocations'], 0)
            self.assertIsNone(service_results.accept_service_results(owner, report, expected, self.path))
        self._closed_result('hold', check)

    def test_receipt_snapshot_mismatch_preserves_original_rows_and_previous_report(self):
        def check(owner, report, expected):
            closed = deepcopy(owner.last_close_report)
            retained = deepcopy(owner.last_query_report['unconsumed_call_results'])
            with self.assertRaisesRegex(ValueError, 'changed'):
                owner.take_unconsumed_results(expected=[])
            self.assertEqual(owner.last_close_report, closed)
            self.assertEqual(self.case.pool.snapshot()['allocations'], 1)
            self.assertEqual(owner.take_unconsumed_results(expected=retained), retained)
            self.assertEqual(self.case.pool.snapshot()['allocations'], 0)
        self._closed_result('hold', check)

    def test_reuse_of_complete_saved_accounting_does_not_overwrite_or_repeat_results(self):
        def check(owner, report, expected):
            original = owner.take_unconsumed_results
            with patch.object(owner, 'take_unconsumed_results', side_effect=ValueError('CPU acknowledgement failed')):
                with self.assertRaises(ValueError):
                    service_results.accept_service_results(owner, report, expected, self.path)
            artifact, = self.path.iterdir()
            before = artifact.read_bytes()
            with patch.object(service_results, 'write_private_json') as write, \
                    patch.object(owner, 'take_unconsumed_results', original):
                receipt = service_results.accept_service_results(owner, report, expected, self.path)
            write.assert_not_called()
            self.assertEqual(receipt['path'], artifact.name)
            self.assertEqual(artifact.read_bytes(), before)
            self.assertEqual(self.case.pool.snapshot()['allocations'], 0)
        self._closed_result('hold-unknown', check)

    def test_partial_failed_file_is_preserved_and_retry_uses_a_new_complete_artifact(self):
        def check(owner, report, expected):
            def partial(path, value):
                with open_private_text(path) as stream:
                    stream.write('{"incomplete":')
                raise OSError('CPU partial write')
            with patch.object(service_results, 'write_private_json', side_effect=partial):
                with self.assertRaises(OSError):
                    service_results.accept_service_results(owner, report, expected, self.path)
            partial_file, = self.path.iterdir()
            original = partial_file.read_bytes()
            receipt = service_results.accept_service_results(owner, report, expected, self.path)
            self.assertNotEqual(receipt['path'], partial_file.name)
            self.assertEqual(partial_file.read_bytes(), original)
            self.assertEqual(json.loads((self.path/receipt['path']).read_text())['retained'][0]['status'], 'succeeded')
            self.assertEqual(self.case.pool.snapshot()['allocations'], 0)
        self._closed_result('hold', check)

    def test_directory_sync_failure_keeps_results_until_a_durable_retry(self):
        def check(owner, report, expected):
            original = os.fsync
            def fsync(descriptor):
                if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                    raise OSError('CPU directory sync failure')
                return original(descriptor)
            with patch.object(service_results.os, 'fsync', side_effect=fsync), \
                    patch.object(owner, 'take_unconsumed_results', wraps=owner.take_unconsumed_results) as take:
                with self.assertRaises(OSError):
                    service_results.accept_service_results(owner, report, expected, self.path)
                take.assert_not_called()
            self.assertEqual(self.case.pool.snapshot()['allocations'], 1)
            self.assertEqual(service_results.accept_service_results(owner, report, expected, self.path)['records'], 1)
            self.assertEqual(self.case.pool.snapshot()['allocations'], 0)
        self._closed_result('hold', check)

    def test_driver_can_retry_only_local_acceptance_after_its_Ray_runtime_stops(self):
        def check(owner, report, expected):
            driver = session._MovieDriver(dict(backend='semloom-core'), {}, self.path, {})
            driver.owner, driver.payload_pool = owner, self.case.pool
            driver.core_execution = dict(mode='async')
            driver.last_query_report, driver.last_expected = report, expected
            driver.last_query_directory = self.path
            driver.context['_records_payload_drivers'] = {owner._service_allocation.allocation_id:driver}
            driver.ray, driver.created_ray = owner._actors.ray, True
            driver.ray.get_runtime_context = lambda:SimpleNamespace(
                get_job_id=lambda:'receipt-job', get_node_id=lambda:'receipt-node')
            initialized = [True]
            driver.ray.is_initialized = lambda:initialized[0]
            driver.service_runtime = service_results.service_runtime_identity(driver.ray)
            driver.ray.shutdown = lambda:initialized.__setitem__(0, False)
            with patch.object(service_results, 'write_private_json', side_effect=OSError('CPU disk full')):
                self.assertEqual(driver.close()['status'], 'unknown')
            self.assertFalse(initialized[0])
            self.assertEqual(self.case.pool.snapshot()['allocations'], 1)
            with patch.object(self.case.pool, 'retry_cleanup', side_effect=AssertionError('no remote retry')) as retry:
                final = driver.close()
            retry.assert_not_called()
            self.assertEqual(final['status'], 'closed', final)
            self.assertEqual(final['receipts']['service_result_transfer']['records'], 1)
            self.assertEqual(self.case.pool.snapshot()['allocations'], 0)
            self.assertEqual(driver.context['_records_payload_drivers'], {})
        self._closed_result('hold', check)

    def test_driver_keeps_the_original_runtime_until_its_snapshot_RPC_returns(self):
        case = snapshots.ServiceSnapshotRetentionTests()
        case.setUp()
        def check():
            ray = sys.modules['ray']
            initialized = [True]
            ray.is_initialized = lambda:initialized[0]
            ray.get_runtime_context = lambda:SimpleNamespace(
                get_job_id=lambda:'snapshot-driver-job', get_node_id=lambda:'snapshot-driver-node')
            ray.shutdown = Mock(side_effect=lambda:initialized.__setitem__(0,False))
            owner = case._owner_and_query()
            original = owner._feedback.future
            driver = session._MovieDriver(dict(backend='semloom-core'), {}, self.path, {})
            driver.owner, driver.payload_pool = owner, case.pool
            driver.core_execution = dict(mode='async')
            driver.ray, driver.created_ray = ray, True
            driver.service_runtime = service_results.service_runtime_identity(ray)
            allocation = owner._service_allocation.allocation_id
            driver.context['_records_payload_drivers'] = {allocation:driver}
            first = driver.close()
            self.assertEqual(first['status'], 'unknown', first)
            self.assertEqual(first['receipts']['ray']['status'], 'retained')
            ray.shutdown.assert_not_called()
            self.assertTrue(initialized[0])
            self.assertEqual(case.pool.snapshot()['allocations'], 1)
            self.assertIs(driver.context['_records_payload_drivers'][allocation], driver)
            case._release.set()
            original.result(3)
            closed = driver.close()
            self.assertEqual(closed['status'], 'closed', closed)
            self.assertEqual(case.pool.snapshot()['allocations'], 0)
            self.assertFalse(initialized[0])
            ray.shutdown.assert_called_once()
            self.assertEqual(driver.context['_records_payload_drivers'], {})
        case._run(check)


if __name__ == '__main__':
    unittest.main()
