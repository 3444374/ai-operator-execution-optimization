"""Cross-request progress, bounded ownership and failure preservation over real IPC."""
from concurrent.futures import ThreadPoolExecutor
import json
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

    def owner(self, *, result_bytes=8192, deadline=None, command_slots=None, input_bytes=None,
              item_input_bytes=4096, control_dir=None,
              batch_size=2, prefill_window=2):
        owner = pipeline.PipelinedPrefillDecodeProcess(
            EngineSpec('/tmp/CPU-phase-model', sys.executable, '/tmp/CPU-driver', max_model_len=2048),
            GenerationSpec(max_tokens=12, stop=()), control_dir or self.temp.name,
            time.monotonic()+10 if deadline is None else deadline, 5,
            prefill=pipeline.NixlPlacement('0', 15600), decode=pipeline.NixlPlacement('1', 15601),
            limits=AsyncEngineLimits(max_inflight=3, item_input_bytes=item_input_bytes,
                                     input_bytes=input_bytes, item_result_bytes=result_bytes,
                                     command_slots=command_slots),
            batch_size=batch_size, prefill_window=prefill_window, close_timeout_s=3)
        self.addCleanup(owner.close)
        return owner

    def test_byte_limited_prefill_and_decode_batches_keep_complete_requests(self):
        for width, phase in [(700, 'prefill'), (500, 'decode')]:
            with self.subTest(phase=phase):
                owner = self.owner(input_bytes=2048, item_input_bytes=2048,
                                   control_dir=Path(self.temp.name)/phase)
                producer = owner.stages['prefill']
                original_poll = producer.poll
                # Hold both P receipts together only for the decode-byte case.
                def poll(max_events):
                    if phase == 'decode' and producer.snapshot()['ready_results'] < 2:
                        return ()
                    return original_poll(max_events)
                records = [row(i, 'x' * width) for i in range(2)]
                with patch.object(producer, 'poll', side_effect=poll):
                    results = owner.run_prepared(records)
                self.assertEqual([r['call_id'] for r in results], [r['call_id'] for r in records])
                self.assertTrue(all(r['status'] == 'succeeded' for r in results), owner.last_run_report)
                self.assertEqual(owner.last_run_report['batch_frames'][phase], {1: 2})
                for stage in owner.stages.values():
                    self.assertEqual(stage.snapshot()['issued_submissions'], 2)
                    self.assertEqual(stage.snapshot()['held_requests'], 0)
                owner.close()

    def test_registered_cleanup_uses_close_deadline_and_keeps_retry_identities(self):
        import psutil
        import subprocess
        import threading
        from types import SimpleNamespace
        marker = Path(self.temp.name) / 'ignores-term'
        child = subprocess.Popen([sys.executable, '-c',
            'import signal,sys,time; from pathlib import Path; '
            'signal.signal(signal.SIGTERM,signal.SIG_IGN); '
            'Path(sys.argv[1]).touch(); time.sleep(20)', str(marker)], start_new_session=True)
        try:
            until = time.monotonic() + 2
            while not marker.exists():
                self.assertLess(time.monotonic(), until)
                time.sleep(.005)
            owner = pipeline.PipelinedPrefillDecodeProcess.__new__(pipeline.PipelinedPrefillDecodeProcess)
            owner._cancelled, owner._wakeup = threading.Event(), threading.Event()
            owner._close_lock = threading.Lock()
            owner._closed = False
            owner.control = Path(self.temp.name)
            owner.last_close_report = None
            owner._process_records = {}
            stage = SimpleNamespace(process=child, process_created=psutil.Process(child.pid).create_time(),
                ready_report={'engine_uid': 'cpu'},
                close=lambda timeout: dict(status='unknown', process_exit_confirmed=False))
            owner.stages = {'prefill': stage}
            owner._register('prefill', stage)
            started = time.monotonic()
            report = owner.close(timeout=.02)
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, .2, 'registered cleanup must not open another fixed wait')
            self.assertEqual(report['status'], 'unknown')
            recovered = report['registered_process_cleanup']['reports'][0]
            self.assertEqual(owner._process_records['prefill']['owned_processes'], recovered['owned_processes'])
            child.wait(timeout=2)
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=2)

    def test_expired_total_deadline_starts_neither_stage(self):
        with patch('subprocess.Popen') as spawn:
            with self.assertRaises(TimeoutError):
                self.owner(deadline=time.monotonic()-1)
        spawn.assert_not_called()

    def test_total_deadline_bounds_a_stage_that_never_becomes_ready(self):
        def command(spec, descriptors):
            return [sys.executable, '-c', 'import time; time.sleep(10)']
        with patch.object(CPUProcess, '_command', side_effect=command):
            started = time.monotonic()
            with self.assertRaises(TimeoutError) as failure:
                self.owner(deadline=started+.1)
            self.assertLess(time.monotonic()-started, 3)
        child = failure.exception.async_engine_process
        self.assertIsNotNone(child.process.poll())
        self.assertEqual(child.last_close_report['process_exit_confirmed'], True)

    def test_command_capacity_release_admits_peers_before_a_held_request_finishes(self):
        owner = self.owner(command_slots=1, batch_size=1, prefill_window=3)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(owner.run_prepared, [row(0, 'hold'), row(1), row(2)])
            deadline = time.monotonic()+3
            try:
                while owner.stages['decode'].snapshot()['issued_submissions'] < 2:
                    self.assertLess(time.monotonic(), deadline)
                    owner.stages['decode'].wait(.01)
                self.assertFalse(future.done())
            finally:
                owner.cancel()
            results = future.result(timeout=5)
        self.assertEqual(owner.last_run_report['submitted_logical_requests'], 3)
        self.assertEqual(results[0]['status'], 'unknown')
        self.assertEqual(owner.last_close_report['status'], 'closed')

    def test_failed_startup_reaps_native_child_after_its_ipc_parent_exits(self):
        import psutil
        record = Path(self.temp.name)/'spawned-child.json'
        script = ('import subprocess,sys,time,json,psutil\n'
                  'from pathlib import Path\n'
                  'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(20)"],close_fds=True)\n'
                  'Path(sys.argv[1]).write_text(json.dumps([p.pid,psutil.Process(p.pid).create_time()]))\n'
                  'time.sleep(20)\n')
        def command(spec, descriptors):
            return [sys.executable, '-c', script, str(record)]
        try:
            with patch.object(CPUProcess, '_command', side_effect=command):
                with self.assertRaises(TimeoutError) as failure:
                    self.owner(deadline=time.monotonic()+.4)
            self.assertTrue(record.exists(), 'fixture must spawn its native child before timeout')
            pid, born = json.loads(record.read_text())
            try:
                process = psutil.Process(pid)
                alive = process.create_time() == born and process.status() not in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD)
            except psutil.NoSuchProcess:
                alive = False
            self.assertFalse(alive, 'unknown IPC close must also stop its registered native process group')
            report = failure.exception.pipeline_close_report
            self.assertEqual(report['status'], 'unknown')
            self.assertTrue(report['registered_process_cleanup']['process_exit_confirmed'])
        finally:
            if record.exists():
                pid, born = json.loads(record.read_text())
                try:
                    process = psutil.Process(pid)
                    if process.create_time() == born and process.status() not in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD):
                        process.kill()
                        process.wait(timeout=2)
                except psutil.NoSuchProcess:
                    pass

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
