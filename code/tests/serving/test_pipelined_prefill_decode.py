"""Cross-request progress, bounded ownership and failure preservation over real IPC."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from src.serving.backends import pipelined_prefill_decode as pipeline
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_protocol import AsyncEngineLimits, EngineSpec, GenerationSpec
from tests.serving.test_async_engine_process import row


class CPUProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable, str(Path(__file__).with_name('async_stage_fixture.py')),
                *map(str, descriptors)]


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.patch = patch.object(EngineSpec, 'validate_driver_environment', return_value={})
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.transport = patch.object(pipeline, 'AsyncEngineProcess', CPUProcess)
        self.transport.start()
        self.addCleanup(self.transport.stop)

    def owner(self, *, result_bytes=8192):
        owner = pipeline.PipelinedPrefillDecodeProcess(
            EngineSpec('/tmp/CPU-phase-model', sys.executable, '/tmp/CPU-driver', max_model_len=2048),
            GenerationSpec(max_tokens=12, stop=()), self.temp.name, time.monotonic()+10, 5,
            prefill=pipeline.NixlPlacement('0', 15600), decode=pipeline.NixlPlacement('1', 15601),
            limits=AsyncEngineLimits(max_inflight=3, item_input_bytes=4096, item_result_bytes=result_bytes),
            batch_size=2, prefill_window=2, close_timeout_s=3)
        self.addCleanup(owner.close)
        return owner

    def test_fast_prefill_enters_decode_while_slow_batch_peer_is_running(self):
        owner = self.owner()
        records = [row(0, 'slow-prefill'), *[row(i) for i in range(1, 6)]]
        results = owner.run_prepared(records)
        self.assertEqual([r['call_id'] for r in results], [r['call_id'] for r in records])
        self.assertTrue(all(r['status'] == 'succeeded' for r in results), results)
        slow = results[0]['stage_receipts']['prefill'][0]
        fast = results[1]['stage_receipts']['decode'][0]
        self.assertLess(fast['model_end_monotonic'], slow['model_end_monotonic'])
        self.assertEqual(fast['observed_max_tokens'], 12)
        self.assertTrue(owner.last_run_report['simultaneous_stage_ownership'])
        self.assertLessEqual(owner.last_run_report['peak_logical_inflight'], 3)
        self.assertLessEqual(owner.last_run_report['peak_pending']['prefill'], 2)
        self.assertGreater(owner.last_run_report['batch_frames']['prefill'][2], 0)
        self.assertEqual(sum(len(v) for r in results for v in r['stage_receipts'].values()), 12)
        for stage in owner.stages.values():
            self.assertEqual(stage.snapshot()['held_requests'], 0)
        following = owner.run_prepared([row(0, query='next-query')])
        self.assertEqual(following[0]['status'], 'succeeded')

    def test_unknown_prefill_stops_admission_and_keeps_its_received_peer(self):
        owner = self.owner()
        results = owner.run_prepared([row(0, 'unknown-prefill'), row(1), row(2)])
        self.assertEqual(results[0]['status'], 'unknown')
        self.assertIn('prefill', results[0]['stage_receipts'])
        self.assertIn('prefill', results[1]['stage_receipts'])
        self.assertEqual(owner.last_run_report['submitted_logical_requests'], 2)
        self.assertEqual(owner.last_run_report['status'], 'failed')
        self.assertEqual(owner.last_close_report['status'], 'closed')

    def test_oversized_native_descriptor_has_compact_failure_and_no_decode(self):
        owner = self.owner(result_bytes=2048)
        result = owner.run_prepared([row(0, 'large-kv')])[0]
        receipt = result['stage_receipts']['prefill'][0]
        self.assertEqual(receipt['error_type'], 'ResultCapacityExceeded')
        self.assertIsNone(receipt['kv_transfer_params'])
        self.assertEqual(receipt['total_tokens'], 3)
        self.assertEqual(owner.last_run_report['batch_frames']['decode'], {})
        self.assertEqual(owner.last_close_report['status'], 'closed')

    def test_cancel_wakes_pipeline_and_cleans_both_owners(self):
        owner = self.owner()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(owner.run_prepared, [row(i, 'hold') for i in range(3)])
            deadline = time.monotonic()+3
            while owner.stages['prefill'].snapshot()['accepted_requests'] < 2:
                self.assertLess(time.monotonic(), deadline)
                owner.stages['prefill'].wait(.01)
            owner.cancel()
            results = future.result(timeout=5)
        self.assertEqual(len(results), 3)
        self.assertEqual(owner.last_run_report['status'], 'failed')
        self.assertEqual(owner.last_close_report['status'], 'closed')
        self.assertTrue(all(s.process.poll() is not None for s in owner.stages.values()))


if __name__ == '__main__':
    unittest.main()
