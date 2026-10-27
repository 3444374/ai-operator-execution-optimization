"""Owned-process identity, cleanup and legacy callers; no model is constructed."""

import importlib
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import psutil

from src.infrastructure import owned_processes as owned


CODE = Path(__file__).resolve().parents[2]


class OwnedProcessTests(unittest.TestCase):
    def test_live_requires_the_registered_birth_and_excludes_dead_processes(self):
        processes = {
            101: Mock(create_time=Mock(return_value=1.0), status=Mock(return_value="running")),
            102: Mock(create_time=Mock(return_value=9.0), status=Mock(return_value="running")),
            103: Mock(create_time=Mock(return_value=3.0), status=Mock(return_value=psutil.STATUS_ZOMBIE)),
            104: Mock(create_time=Mock(return_value=4.0), status=Mock(return_value=psutil.STATUS_DEAD)),
        }
        def process(pid):
            if pid == 105:
                raise psutil.NoSuchProcess(pid)
            return processes[pid]
        with patch.object(owned.psutil, "Process", side_effect=process):
            self.assertEqual(owned._live({(pid, float(pid - 100)) for pid in range(101, 106)}),
                             [processes[101]])
        processes[102].status.assert_not_called()

    def test_observe_finds_surviving_group_members_after_the_leader_exits(self):
        child = SimpleNamespace(pid=102, info={"create_time": 2.0})
        state = {(101, 1.0)}
        with patch.object(owned.psutil, "Process", side_effect=psutil.NoSuchProcess(101)), \
             patch.object(owned.psutil, "process_iter", return_value=[child]), \
             patch.object(owned.os, "getpgid", return_value=101):
            owned._observe(state, 101, 1.0)
        self.assertEqual(state, {(101, 1.0), (102, 2.0)})

    def test_reused_leader_rejects_observation_before_scanning_other_processes(self):
        with patch.object(owned.psutil, "Process", return_value=Mock(
                create_time=Mock(return_value=2.0))), \
             patch.object(owned.psutil, "process_iter") as scan:
            with self.assertRaisesRegex(RuntimeError, "identity was reused"):
                owned._observe({(101, 1.0)}, 101, 1.0)
        scan.assert_not_called()

    def test_signal_is_birth_matched_and_keeps_redacted_failure_receipts(self):
        process = Mock(pid=101, create_time=Mock(return_value=1.0),
                       status=Mock(return_value="running"),
                       cmdline=Mock(return_value=["worker", "--api-key", "fixture-value"]))
        process.name.return_value = "fixture-worker"
        process.send_signal.side_effect = PermissionError("fixture denied signal")
        with patch.object(owned.psutil, "Process", return_value=process), \
             patch.object(owned.os, "killpg") as group_signal:
            with self.assertRaises(PermissionError) as caught:
                owned._signal({(101, 1.0), (101, 0.5)}, 101, 1.0, signal.SIGTERM)
        process.send_signal.assert_called_once_with(signal.SIGTERM)
        group_signal.assert_not_called()
        event, = caught.exception.signal_events
        self.assertEqual(event["outcome"], "unconfirmed")
        self.assertEqual(event["command"], ["worker", "--api-key", "***"])
        self.assertNotIn("fixture-value", str(event))
        self.assertEqual(event["create_time"], 1.0)

    def test_optional_command_metadata_failure_does_not_prevent_a_matched_signal(self):
        process = Mock(pid=101, create_time=Mock(return_value=1.0),
                       status=Mock(return_value="running"),
                       cmdline=Mock(side_effect=PermissionError("fixture metadata denied")))
        with patch.object(owned.psutil, "Process", return_value=process):
            event, = owned._signal({(101, 1.0)}, 101, 1.0, signal.SIGTERM)
        process.send_signal.assert_called_once_with(signal.SIGTERM)
        self.assertEqual(event["outcome"], "sent")
        self.assertIn("metadata_error", event)

    def test_wait_preserves_deadline_and_reports_the_first_observed_exit(self):
        process = Mock(pid=101, create_time=Mock(return_value=1.0))
        state, exits, output = {(101, 1.0)}, [], Mock()
        with patch.object(owned, "_live", side_effect=[[process], []]), \
             patch.object(owned, "_observe") as observe, \
             patch.object(owned.time, "monotonic", side_effect=[1.0, 1.1, 1.2]), \
             patch.object(owned.time, "sleep") as sleep:
            owned._wait(state, 101, 1.0, 2.0, output,
                        exit_events=exits, phase="fixture", watch=state)
        output.pump.assert_called_once_with(2.0)
        observe.assert_called_once_with(state, 101, 1.0)
        sleep.assert_called_once_with(0.02)
        self.assertEqual([(e["pid"], e["create_time"], e["phase"]) for e in exits],
                         [(101, 1.0, "fixture")])

    def test_expired_wait_does_not_pump_observe_or_sleep(self):
        process = Mock(pid=101, create_time=Mock(return_value=1.0))
        with patch.object(owned, "_live", return_value=[process]), \
             patch.object(owned.time, "monotonic", return_value=2.0), \
             patch.object(owned, "_observe") as observe, \
             patch.object(owned.time, "sleep") as sleep:
            output = Mock()
            owned._wait({(101, 1.0)}, 101, 1.0, 2.0, output)
        output.pump.assert_not_called()
        observe.assert_not_called()
        sleep.assert_not_called()

    def test_positive_timeout_validation_keeps_bool_nonfinite_and_zero_rejected(self):
        for value in (True, False, 0, -1, float("nan"), float("inf"), "1", None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "finite positive"):
                owned._seconds(value, "timeout")
        self.assertIsNone(owned._seconds(0.25, "timeout"))


class ProcessWiringTests(unittest.TestCase):
    def test_legacy_helper_imports_are_direct_same_object_reexports(self):
        legacy = importlib.import_module("src.experiments.lotus_records_supervisor")
        for name in ("_seconds", "_live", "_observe", "_signal", "_exit_wait_identity", "_wait"):
            with self.subTest(name=name):
                self.assertIs(getattr(legacy, name), getattr(owned, name))

    def test_cold_import_has_no_experiment_or_model_dependency_and_observes_nothing(self):
        program = '''
import importlib, importlib.abc, sys
from unittest.mock import patch
import psutil
sys.path.insert(0, sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        forbidden = ('src.experiments', 'src.execution_provider', 'src.scheduling',
                     'ray', 'daft', 'vllm', 'torch', 'lotus')
        if any(fullname == p or fullname.startswith(p + '.') for p in forbidden):
            raise AssertionError('unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Guard())
with patch.object(psutil, 'Process', side_effect=AssertionError('eager process read')), \
     patch.object(psutil, 'process_iter', side_effect=AssertionError('eager process scan')):
    importlib.import_module('src.infrastructure.owned_processes')
'''
        result = subprocess.run([sys.executable, "-I", "-B", "-c", program, str(CODE)],
                                text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_existing_worker_commands_and_code_root_start_and_exit_on_empty_input(self):
        from src.baselines.text.frameworks.daft_engine_process import CODE_ROOT, EngineProcess
        from src.baselines.text.frameworks.lotus_chain_engine_process import ChainEngineProcess
        self.assertEqual(CODE_ROOT, CODE)
        environment = dict(os.environ, PYTHONPATH=str(CODE), CUDA_VISIBLE_DEVICES="",
                           HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                           PYTHONDONTWRITEBYTECODE="1")
        for process_type, module in (
            (EngineProcess, "src.serving.backends.vllm_sync_process"),
            (ChainEngineProcess, "src.baselines.text.frameworks.lotus_chain_engine_process"),
        ):
            with self.subTest(module=module):
                read_fd, write_fd = os.pipe()
                try:
                    command = process_type._command(SimpleNamespace(model_python=sys.executable), write_fd)
                    self.assertEqual(command, [sys.executable, "-m", module, "--serve", str(write_fd)])
                    with subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE, pass_fds=(write_fd,), env=environment) as child:
                        os.close(write_fd)
                        write_fd = None
                        try:
                            _, error = child.communicate(timeout=15)
                        except BaseException:
                            child.kill()
                            child.communicate()
                            raise
                    self.assertEqual(child.returncode, 0, error.decode())
                    self.assertEqual(os.read(read_fd, 1), b"")
                finally:
                    os.close(read_fd)
                    if write_fd is not None:
                        os.close(write_fd)


class SupervisorProcessTests(unittest.TestCase):
    def test_existing_private_pipe_transport_closes_its_cpu_child_once(self):
        from src.baselines.text.frameworks.daft_engine_process import EngineProcess
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        class CPUProcess(EngineProcess):
            @staticmethod
            def _command(engine, result_fd):
                return [engine.model_python, "-m", "tests.baselines.text.daft_session_cpu_engine",
                        str(result_fd)]
        with tempfile.TemporaryDirectory() as directory:
            process = CPUProcess(EngineSpec("/tmp/CPU-fixture-model", sys.executable, sys.executable),
                                 GenerationSpec(), Path(directory), time.monotonic() + 5, 3)
            try:
                report = process.close(2)
                self.assertEqual(report["status"], "closed")
                self.assertTrue(report["engine_api"]["engine_api_shutdown_confirmed"])
                self.assertTrue(report["process_exit_confirmed"])
                self.assertIsNotNone(process.process.poll())
                self.assertIs(process.close(2), report)
                self.assertIsNone(report["gpu_exit_confirmed"])
            finally:
                process.close(2)
                process._reaper_stop.set()

    def test_normal_real_child_exits_without_cleanup_signals(self):
        from src.experiments.lotus_records_supervisor import supervise
        with tempfile.TemporaryDirectory() as directory:
            result = supervise([sys.executable, "-B", "-c", "pass"], Path(directory),
                               timeout_s=3, grace_s=0.3)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["worker_exit"], 0)
        self.assertTrue(result["observed_owned_exit_confirmed"])
        self.assertEqual(result["signals"], [])
        self.assertIsNone(result["gpu_exit_confirmed"])

    def test_timeout_reaps_real_child_and_preserves_failure_status(self):
        from src.experiments.lotus_records_supervisor import supervise
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(TimeoutError) as caught:
                supervise([sys.executable, "-B", "-c", "import time; time.sleep(30)"],
                          Path(directory), timeout_s=0.8, grace_s=0.3)
        result = caught.exception.report
        self.assertEqual(result["status"], "failed")
        self.assertIsNotNone(result["worker_exit"])
        self.assertTrue(result["observed_owned_exit_confirmed"])
        self.assertEqual(result["remaining_owned_pids"], [])
        self.assertIn("SIGTERM", result["signals"])
        self.assertIsNone(result["gpu_exit_confirmed"])
