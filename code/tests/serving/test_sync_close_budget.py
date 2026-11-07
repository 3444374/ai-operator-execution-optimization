"""Sync close timing through actual parent/worker code and memory pipes."""

from dataclasses import asdict
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.infrastructure import owned_processes
from src.serving.backends import staged_generation_process as staged
from src.serving.backends import vllm_sync_process as sync
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec


class CPUEngine:
    def __init__(self, spec, generation):
        self.spec = spec

    def ready(self):
        return dict(engine_uid="CPU-close-budget")

    def close(self, timeout=10.):
        return dict(status="closed", engine_api_shutdown_confirmed=True,
                    gpu_exit_confirmed=None, close_timeout_s=timeout)


class MemoryPipe(io.BytesIO):
    def fileno(self):
        return 1


class SyncCloseBudgetTests(unittest.TestCase):
    def test_parent_encodes_existing_api_deadline_and_exit_alone_keeps_unknown(self):
        for timeout in (30., .25):
            with self.subTest(timeout=timeout):
                owner = object.__new__(sync.EngineProcess)
                owner._lock, owner._reaper_stop = threading.RLock(), threading.Event()
                owner._closed = owner._broken = False
                owner.created, owner.owned = 1., {(123, 1.)}
                owner.engine_close, owner.ready_report = None, dict(engine_uid="CPU-close-budget")
                owner._result_failure_write_errors = []
                owner.last_result_failure = dict(known_prompt_tokens=7)
                owner.pending, owner.reader = bytearray(), MemoryPipe()
                owner.report_path = Path("/tmp/unused-memory-close-report")
                owner.ipc_seconds = owner.startup_seconds = 0.
                owner.ipc_encode_seconds = owner.ipc_decode_seconds = 0.
                owner.ipc_request_bytes = owner.ipc_response_bytes = 0

                class Process:
                    pid, returncode = 123, None
                    stdin = MemoryPipe()

                    def poll(self):
                        return self.returncode

                    def wait(self, timeout):
                        self.returncode = 0

                owner.process = Process()
                sent = bytearray()
                reply = sync._encode(dict(kind="closed", report=dict(
                    status="unknown", engine_api_shutdown_confirmed=False)))
                memory_os = SimpleNamespace(
                    write=lambda fd, value: (sent.extend(value) or len(value)),
                    read=lambda fd, maximum: reply, getpid=lambda: 1)
                memory_select = SimpleNamespace(select=lambda reads, writes, errors, timeout:
                                                (reads, writes, []))
                with patch("time.monotonic", return_value=100.), \
                        patch.object(sync, "os", memory_os), patch.object(sync, "select", memory_select), \
                        patch.object(sync, "_publish"), patch.object(owned_processes, "_observe"), \
                        patch.object(owned_processes, "_wait"), patch.object(owned_processes, "_live", return_value=set()):
                    report = owner.close(timeout)
                self.assertEqual(json.loads(sent), dict(op="close", deadline=100. + timeout - min(4., timeout / 3)))
                self.assertEqual(report["status"], "unknown")
                self.assertTrue(report["process_exit_confirmed"])
                self.assertFalse(report["engine_api"]["engine_api_shutdown_confirmed"])
                self.assertIsNone(report["gpu_exit_confirmed"])
                self.assertEqual(report["result_failure"], dict(known_prompt_tokens=7))
                self.assertEqual(report["process_cleanup_reserved_s"], min(4., timeout / 3))

    def test_both_actual_workers_use_caller_time_and_keep_emergency_cleanup_separate(self):
        cases = (
            ("long", 130., 15., None, [30.], "closed", True),
            ("short", 100.25, 1., None, [.25], "closed", False),
            ("expired", 99., 0., None, [], "closed", False),
            ("peer_error", None, 0., None, [10.], "error", None),
            ("close_error", 120., 0., 0., [20., 20.], "error", None),
            ("close_error_expired", 120., 0., 30., [20.], "error", None),
        )
        for module in (sync, staged):
            for name, deadline, required, failure_elapsed, expected, kind, confirmed in cases:
                with self.subTest(worker=module.__name__, case=name):
                    clock, calls = [100.], []

                    class ObservedEngine(CPUEngine):
                        def close(self, timeout=10.):
                            calls.append(timeout)
                            if failure_elapsed is not None and len(calls) == 1:
                                clock[0] += failure_elapsed
                                raise RuntimeError("CPU native close error")
                            return dict(super().close(timeout),
                                status="closed" if timeout >= required else "unknown",
                                engine_api_shutdown_confirmed=timeout >= required)

                    opened = dict(op="open", engine=asdict(EngineSpec(
                        "/tmp/cpu-fixture", sys.executable, sys.executable)), generation=asdict(GenerationSpec()))
                    tail = b"not-json\n" if deadline is None else sync._encode(dict(op="close", deadline=deadline))
                    source = io.BytesIO(sync._encode(opened) + tail)
                    sink = io.BytesIO()

                    class Output:
                        def __enter__(self):
                            return sink

                        def __exit__(self, *unused):
                            pass

                    with patch("time.monotonic", side_effect=lambda: clock[0]), \
                            patch.object(module, "sys", SimpleNamespace(stdin=SimpleNamespace(buffer=source))), \
                            patch.object(module, "os", SimpleNamespace(fdopen=lambda *args, **kwargs: Output())):
                        module.serve(1, engine_factory=ObservedEngine)
                    self.assertEqual(calls, expected)
                    replies = [json.loads(line) for line in sink.getvalue().splitlines()]
                    self.assertEqual([value["kind"] for value in replies], ["ready", kind])
                    if confirmed is not None:
                        report = replies[-1]["report"]
                        self.assertIs(report["engine_api_shutdown_confirmed"], confirmed)
                        self.assertEqual(report["status"], "closed" if confirmed else "unknown")
                        self.assertIsNone(report["gpu_exit_confirmed"])


class SoftwareSyncCloseBudgetTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux software-pipe check")
    def test_sync_and_staged_pipe_return_actual_close_allowance(self):
        for kind, base in (("sync", sync.EngineProcess), ("staged", staged.ChainEngineProcess)):
            with self.subTest(worker=kind), tempfile.TemporaryDirectory() as directory:
                class CPUProcess(base):
                    @staticmethod
                    def _command(engine, result_fd):
                        return [sys.executable, "-B", "-m", __name__, "--serve", kind, str(result_fd)]

                owner = CPUProcess(EngineSpec("/tmp/cpu-fixture", sys.executable, sys.executable),
                    GenerationSpec(), Path(directory), None, 5.)
                try:
                    report = owner.close(3.)
                    self.assertEqual(report["status"], "closed", report)
                    self.assertTrue(report["process_exit_confirmed"])
                    self.assertTrue(report["engine_api"]["engine_api_shutdown_confirmed"])
                    self.assertGreater(report["engine_api"]["close_timeout_s"], 0.)
                    self.assertLessEqual(report["engine_api"]["close_timeout_s"], report["engine_api_rpc_budget_s"])
                    self.assertIsNone(report["gpu_exit_confirmed"])
                finally:
                    owner.close(2.)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--serve":
        module = {"sync": sync, "staged": staged}[sys.argv[2]]
        module.serve(int(sys.argv[3]), engine_factory=CPUEngine)
    else:
        unittest.main()
