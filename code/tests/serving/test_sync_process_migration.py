"""Shared sync transport and stage cleanup migration; CPU fixtures only."""

import importlib
import json
import os
from pathlib import Path
import pickle
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import psutil

from src.infrastructure import owned_processes
from src.serving.backends import stage_process_cleanup, vllm_sync_process


CODE = Path(__file__).resolve().parents[2]
LEGACY_SYNC = "src.baselines.text.frameworks.daft_engine_process"
SYNC = "src.serving.backends.vllm_sync_process"
LEGACY_CLEANUP = "src.baselines.text.frameworks.lotus_cascade_process"
CLEANUP = "src.serving.backends.stage_process_cleanup"


class MigrationTests(unittest.TestCase):
    def test_existing_imports_reexport_the_same_classes_functions_and_constants(self):
        old = importlib.import_module(LEGACY_SYNC)
        for name in ("EngineProcess", "EngineSpec", "GenerationSpec", "IDENTITY_FIELDS", "CODE_ROOT",
                     "MAX_MESSAGE_BYTES", "DEFAULT_CLOSE_TIMEOUT_S", "CLOSE_CLEANUP_RESERVE_S",
                     "RESULT_PROTOCOL_ERROR", "_encode", "_publish", "_verified_terminal", "serve"):
            with self.subTest(name=name):
                self.assertIs(getattr(old, name), getattr(vllm_sync_process, name))
        self.assertIs(importlib.import_module(LEGACY_CLEANUP).StageEngineCleanup,
                      stage_process_cleanup.StageEngineCleanup)

    def test_legacy_pickle_globals_and_new_class_pickles_resolve_to_same_objects(self):
        for old, new, name in (
            (LEGACY_SYNC, SYNC, "EngineProcess"),
            (LEGACY_CLEANUP, CLEANUP, "StageEngineCleanup"),
        ):
            with self.subTest(name=name):
                expected = getattr(importlib.import_module(new), name)
                self.assertIs(pickle.loads(("c" + old + "\n" + name + "\n.").encode()), expected)
                self.assertIs(pickle.loads(pickle.dumps(expected)), expected)

    def test_cold_import_orders_preserve_identity(self):
        program = '''
import importlib, sys
sys.path.insert(0, sys.argv[1])
first, second = [importlib.import_module(name) for name in sys.argv[2:4]]
assert getattr(first, sys.argv[4]) is getattr(second, sys.argv[4])
'''
        for old, new, name in ((LEGACY_SYNC, SYNC, "EngineProcess"),
                               (LEGACY_CLEANUP, CLEANUP, "StageEngineCleanup")):
            for order in ((old, new), (new, old)):
                with self.subTest(order=order):
                    run = subprocess.run([sys.executable, "-I", "-B", "-c", program,
                                          str(CODE), *order, name], text=True, capture_output=True, timeout=15)
                    self.assertEqual(run.returncode, 0, run.stderr)

    def test_canonical_imports_and_empty_cleanup_do_not_depend_on_legacy_workers_or_vendor_runtime(self):
        program = '''
import importlib, importlib.abc, pathlib, sys, tempfile
sys.path.insert(0, sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        forbidden = ('src.experiments', 'src.baselines.text.frameworks.daft_engine_process',
                     'src.baselines.text.frameworks.lotus_cascade_process',
                     'src.baselines.text.frameworks.lotus_chain_batch', 'src.execution_provider',
                     'src.scheduling', 'ray', 'daft', 'vllm', 'torch', 'lotus')
        if any(fullname == p or fullname.startswith(p + '.') for p in forbidden):
            raise AssertionError('unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Guard())
sync = importlib.import_module('src.serving.backends.vllm_sync_process')
cleanup = importlib.import_module('src.serving.backends.stage_process_cleanup')
assert sync.CODE_ROOT == pathlib.Path(sys.argv[1])
with tempfile.TemporaryDirectory() as directory:
    assert cleanup.StageEngineCleanup(pathlib.Path(directory)).stop(started=False)['process_exit_confirmed']
'''
        run = subprocess.run([sys.executable, "-I", "-B", "-c", program, str(CODE)],
                             text=True, capture_output=True, timeout=15)
        self.assertEqual(run.returncode, 0, run.stderr)

    def test_old_and_new_cli_validate_descriptors_and_exit_on_empty_input(self):
        environment = dict(os.environ, PYTHONPATH=str(CODE), PYTHONDONTWRITEBYTECODE="1",
                           CUDA_VISIBLE_DEVICES="", HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1")
        for module in (LEGACY_SYNC, SYNC):
            with self.subTest(module=module):
                invalid = subprocess.run([sys.executable, "-B", "-m", module], env=environment,
                                         text=True, capture_output=True, timeout=15)
                self.assertEqual(invalid.returncode, 1)
                self.assertEqual(invalid.stderr.strip(),
                                 "private engine worker requires an inherited result descriptor")
                read_fd, write_fd = os.pipe()
                try:
                    with subprocess.Popen([sys.executable, "-B", "-m", module, "--serve", str(write_fd)],
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            pass_fds=(write_fd,), env=environment) as child:
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


class StageCleanupTests(unittest.TestCase):
    def test_no_started_model_needs_no_process_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = stage_process_cleanup.StageEngineCleanup(root).stop(started=False)
            self.assertTrue(result["process_exit_confirmed"])
            self.assertEqual(result["errors"], [])
            self.assertTrue((root / "stop").exists())
            self.assertTrue((root / "driver-cleanup.json").is_file())

    def test_started_but_unregistered_model_does_not_report_confirmed_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            result = stage_process_cleanup.StageEngineCleanup(Path(directory)).stop(grace_s=0)
        self.assertFalse(result["process_exit_confirmed"])
        self.assertIn("stage_model_process_registration_unconfirmed", result["errors"])

    def test_corrupt_report_cannot_be_hidden_by_a_separate_confirmed_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "engine-bad.json").write_text("{")
            (root / "engine-good.json").write_text(json.dumps(dict(
                model_pid=101, process_created=1.0, process_exit_confirmed=True, cleanup_errors=[])))
            result = stage_process_cleanup.StageEngineCleanup(root).stop(grace_s=0)
        self.assertFalse(result["process_exit_confirmed"])
        self.assertEqual(len(result["reports"]), 2)
        self.assertTrue(any("engine_report_read" in error for error in result["errors"]))

    def test_cached_exit_proof_is_not_extended_to_a_newly_registered_process(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owner = stage_process_cleanup.StageEngineCleanup(root)
            owner._driver_reports[(101, 1.0)] = dict(process_exit_confirmed=True, cleanup_errors=[],
                                                   owned_processes=[[101, 1.0]])
            (root / "engine-new.json").write_text(json.dumps(dict(
                model_pid=101, process_created=1.0, process_exit_confirmed=False,
                owned_processes=[[101, 1.0], [102, 2.0]], cleanup_errors=[], usage_receipt="new")))
            report, = owner._read()
        self.assertFalse(report["process_exit_confirmed"])
        self.assertEqual(report["usage_receipt"], "new")

    def test_reused_registered_pid_is_rejected_before_any_signal(self):
        with patch.object(psutil, "Process", return_value=Mock(create_time=Mock(return_value=2.0))), \
             patch.object(owned_processes, "_signal") as send:
            with self.assertRaisesRegex(RuntimeError, "identity was reused"):
                stage_process_cleanup.StageEngineCleanup._stop_registered(
                    dict(model_pid=101, process_created=1.0))
        send.assert_not_called()

    def test_os_exit_after_observation_errors_keeps_unknown_model_cleanup(self):
        with patch.object(psutil, "Process", return_value=Mock(create_time=Mock(return_value=1.0))), \
             patch.object(owned_processes, "_observe", side_effect=PermissionError("fixture observation")), \
             patch.object(owned_processes, "_live", side_effect=[[Mock()], [], [], []]), \
             patch.object(owned_processes, "_signal", return_value=[]) as send, \
             patch.object(owned_processes, "_wait", side_effect=PermissionError("fixture wait")):
            report = stage_process_cleanup.StageEngineCleanup._stop_registered(
                dict(model_pid=101, process_created=1.0))
        self.assertTrue(report["process_exit_confirmed"])
        self.assertEqual(report["status"], "unknown")
        self.assertIsNone(report["gpu_exit_confirmed"])
        self.assertIn("engine_API_cleanup_unconfirmed_driver_stopped_registered_processes",
                      report["cleanup_errors"])
        self.assertIn("process_tree_observation: PermissionError", report["cleanup_errors"])
        self.assertIn("process_tree_wait: PermissionError", report["cleanup_errors"])
        send.assert_called_once()


class RealProcessTests(unittest.TestCase):
    def test_idle_stop_and_deadline_confirm_API_but_broken_channel_keeps_unknown(self):
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

        class CPUProcess(vllm_sync_process.EngineProcess):
            @staticmethod
            def _command(engine, result_fd):
                return [engine.model_python, '-m', 'tests.baselines.text.daft_session_cpu_engine',
                        str(result_fd)]

        for trigger in ('stop', 'deadline', 'broken'):
            with self.subTest(trigger=trigger), tempfile.TemporaryDirectory() as directory:
                control = Path(directory)
                process = CPUProcess(EngineSpec('/tmp/CPU-fixture-model', sys.executable, sys.executable),
                                     GenerationSpec(), control, time.monotonic() + 5, 3)
                try:
                    with process._lock:
                        if trigger == 'deadline':
                            process.deadline = time.monotonic() - 1
                        else:
                            process._broken = trigger == 'broken'
                            (control / 'stop').touch()
                    process._reaper.join(timeout=3)
                    self.assertFalse(process._reaper.is_alive())
                    report = process.close(2)
                    self.assertTrue(report['process_exit_confirmed'], report)
                    self.assertTrue(process._broken)
                    self.assertIs(process.close(2), report)
                    if trigger == 'broken':
                        self.assertEqual(report['status'], 'unknown')
                        self.assertIsNone(report['engine_api'])
                        self.assertIn('engine_api_shutdown_unconfirmed', report['cleanup_errors'])
                    else:
                        self.assertEqual(report['status'], 'closed', report)
                        self.assertTrue(report['engine_api']['engine_api_shutdown_confirmed'])
                finally:
                    process.close(2)

    def test_migrated_transport_preserves_call_identity_usage_and_idempotent_close(self):
        from src.semantic_methods.lotus.records import identity
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        class CPUProcess(vllm_sync_process.EngineProcess):
            @staticmethod
            def _command(engine, result_fd):
                return [engine.model_python, "-m", "tests.baselines.text.daft_session_cpu_engine",
                        str(result_fd)]
        with tempfile.TemporaryDirectory() as directory:
            process = CPUProcess(EngineSpec("/tmp/CPU-fixture-model", sys.executable, sys.executable),
                                 GenerationSpec(), Path(directory), time.monotonic() + 5, 3)
            try:
                request = dict(identity("fixture-query", "row-0", 0),
                               messages_json=json.dumps([dict(role="user", content="complete fixture")]))
                result, = process.run_prepared([request])
                self.assertEqual(result["call_id"], request["call_id"])
                self.assertEqual(result["total_tokens"], 4)
                report = process.close(2)
                self.assertEqual(report["status"], "closed")
                self.assertTrue(report["process_exit_confirmed"])
                self.assertTrue(report["engine_api"]["engine_api_shutdown_confirmed"])
                self.assertIs(process.close(2), report)
            finally:
                process.close(2)

    def test_cleanup_stops_only_its_registered_cpu_child_and_keeps_model_exit_unknown(self):
        child = subprocess.Popen([sys.executable, "-B", "-c", "import time;time.sleep(30)"],
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            created = psutil.Process(child.pid).create_time()
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "engine-fixture.json").write_text(json.dumps(dict(
                    model_pid=child.pid, process_created=created, process_exit_confirmed=False,
                    owned_processes=[[child.pid, created]], cleanup_errors=[])))
                result = stage_process_cleanup.StageEngineCleanup(root).stop(grace_s=0)
                child.wait(timeout=3)
                self.assertTrue(result["process_exit_confirmed"])
                report, = result["reports"]
                self.assertEqual(report["status"], "unknown")
                self.assertIsNone(report["gpu_exit_confirmed"])
                self.assertEqual(report["process_exit_scope"], "observed_registered_processes")
                self.assertFalse(report["unobserved_descendants_confirmed"])
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=3)
