"""Synthetic child states on the real adapters; no author, process or HTTP run."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig, IMLaneCallExecutor
from src.baselines.text.frameworks.native_calls import NativeCall, NativeCallOutcome, _outcome_wire
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.full_response import FullModelResponse


class Child:
    def __init__(self, call, returncode):
        read, write = os.pipe()
        self.stdout = os.fdopen(read, 'rb', buffering=0)
        self.returncode = returncode
        metadata = json.dumps(dict(native_executors=2, native_task_threads=2))
        result = _outcome_wire(NativeCallOutcome(call.call_id, call.row_id, FullModelResponse(200, (), b'{}')))
        os.write(write, ('SEMA_IMLANE_META ' + metadata + '\nSEMA_IMLANE_RESULT ' + result + '\n').encode())
        os.close(write)

    def poll(self):
        return self.returncode


class IMLaneBridgeOwnershipTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        bridge = root / 'bridge.json'
        bridge.write_text(json.dumps(dict(author_commit='072f8257db80569e42c9f5727d839587df529652',
            bridge_binary=str(root/'unused'), bridge_sha256='0'*64, gateway_port=1,
            udf_path=str(root/'unused.py'), udf_sha256='0'*64, work_root=str(root),
            namespace='synthetic-owner', native_task_threads=2)))
        self.config = IMLaneCallConfig('cpython', str(bridge), 123, 456, str(root/'build.json'))
        self.call = NativeCall('synthetic', 0, b'{}', 'http://localhost/unused', (), 1.)
        self.verifying = patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator',
                              return_value=dict(runtime='cpython', evidence='synthetic only'))
        self.verifying.start()
        self.addCleanup(self.verifying.stop)

    def executor(self):
        value = IMLaneCallExecutor(self.config, NativeGraphOptions(concurrency=4))
        value.delegate._started = True
        return value

    def assert_new_owner_blocked(self, executor, launching):
        self.assertTrue(executor._uncertain.is_file())
        before = launching.call_count
        with self.assertRaisesRegex(RuntimeError, 'unsettled'):
            list(self.executor().iter_batch((self.call,)))
        self.assertEqual(launching.call_count, before)

    def test_failure_before_child_creation_preserves_cause_and_no_author_responsibility(self):
        executor = self.executor()
        primary = OSError('synthetic creation failure')
        with patch('src.execution_provider.adapters.sema_imlane.subprocess.Popen', side_effect=primary):
            with self.assertRaises(OSError) as observed:
                list(executor.iter_batch((self.call,)))
        self.assertIs(observed.exception, primary)
        self.assertFalse(executor.last_close_report['bridge_started'])
        self.assertFalse(executor.last_close_report['author_submission_possible'])
        self.assertFalse(executor.last_close_report['external_coordinator_requires_cleanup'])
        self.assertFalse(executor._uncertain.exists())
        self.assertFalse(self.executor()._uncertain.exists())

    def test_normal_exit_drains_work_without_uncertain_marker(self):
        executor, child = self.executor(), Child(self.call, 0)
        self.addCleanup(child.stdout.close)
        with patch('src.execution_provider.adapters.sema_imlane.subprocess.Popen', return_value=child):
            results = list(executor.iter_batch((self.call,)))
        self.assertEqual(len(results), 1)
        self.assertTrue(executor.last_close_report['bridge_started'])
        self.assertTrue(executor.last_close_report['bridge_stopped'])
        self.assertTrue(executor.last_close_report['clean'])
        self.assertFalse(executor._uncertain.exists())

    def test_nonzero_exit_keeps_original_error_and_blocks_another_owner(self):
        executor, child = self.executor(), Child(self.call, 7)
        self.addCleanup(child.stdout.close)
        with patch('src.execution_provider.adapters.sema_imlane.subprocess.Popen', return_value=child) as launching:
            with self.assertRaisesRegex(RuntimeError, 'did not complete every submitted occurrence'):
                list(executor.iter_batch((self.call,)))
            self.assertTrue(executor.last_close_report['bridge_started'])
            self.assertEqual(executor.last_close_report['returncode'], 7)
            self.assert_new_owner_blocked(executor, launching)

    def test_started_child_with_unconfirmed_stop_keeps_primary_and_blocks_another_owner(self):
        executor, child = self.executor(), Child(self.call, None)
        self.addCleanup(child.stdout.close)
        primary = ValueError('synthetic read failure')
        with patch('src.execution_provider.adapters.sema_imlane.subprocess.Popen', return_value=child) as launching, \
             patch('src.execution_provider.adapters.sema_imlane.os.set_blocking', side_effect=primary), \
             patch.object(executor.delegate, '_stop', side_effect=OSError('synthetic stop unconfirmed')):
            with self.assertRaises(ValueError) as observed:
                list(executor.iter_batch((self.call,)))
            self.assertIs(observed.exception, primary)
            self.assertIn('Sema IMLane child cleanup error: OSError', primary.__notes__)
            report = executor.last_close_report
            self.assertIsNone(report['returncode'])
            self.assertTrue(report['bridge_started'])
            self.assertFalse(report['bridge_stopped'])
            self.assertFalse(report['clean'])
            self.assertTrue(report['author_submission_possible'])
            self.assertTrue(report['external_coordinator_requires_cleanup'])
            self.assert_new_owner_blocked(executor, launching)


if __name__ == '__main__':
    unittest.main()
