"""CPU-only supervision checks using short, self-authored Python workers."""

import json
import os
from contextlib import contextmanager
from pathlib import Path
import signal
import stat
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch

import psutil

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents
                 if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments import lotus_records_supervisor as supervisor


try:
    psutil.pids()
    INVENTORY_ERROR = None
except (PermissionError, psutil.AccessDenied) as error:
    INVENTORY_ERROR = str(error)
needs_inventory = unittest.skipIf(
    INVENTORY_ERROR is not None, "process inventory unavailable: " + str(INVENTORY_ERROR),
)


class SupervisorFixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def run_worker(self, source, **options):
        return supervisor.supervise(
            [sys.executable, "-c", source], self.root,
            timeout_s=options.pop("timeout_s", 2), grace_s=options.pop("grace_s", 0.3), **options,
        )

    def read_report(self):
        return json.loads((self.root / "supervisor.json").read_text())


class SupervisorTests(SupervisorFixture, unittest.TestCase):
    @needs_inventory
    def test_success_keeps_private_streams_and_reports_only_observed_process_scope(self):
        report = self.run_worker("import sys;print('out');print('err',file=sys.stderr)")
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["worker_exit"], 0)
        self.assertEqual(report["remaining_owned_pids"], [])
        self.assertTrue(report["observed_owned_exit_confirmed"])
        self.assertFalse(report["physical_gpu_exit_checked"])
        self.assertIsNone(report["gpu_exit_confirmed"])
        self.assertFalse(report["unobserved_descendants_confirmed"])
        self.assertEqual(report["process_exit_scope"], "observed_owned_processes")
        self.assertIn("out", (self.root / "worker.log").read_text())
        self.assertIn("err", (self.root / "worker.log").read_text())
        self.assertEqual(stat.S_IMODE((self.root / "worker.log").stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE((self.root / "supervisor.json").stat().st_mode), 0o600)
        self.assertEqual(report, self.read_report())
        self.assertLess(report["worker_deadline_monotonic"], report["overall_deadline_monotonic"])
        self.assertEqual(report["signals"], [])
        self.assertTrue(report["output_capture"]["redacted_before_write"])
        self.assertTrue(report["output_capture"]["complete"])

    @needs_inventory
    def test_nonzero_exit_raises_with_preserved_report(self):
        with self.assertRaisesRegex(RuntimeError, "worker failed") as caught:
            self.run_worker("raise SystemExit(7)")
        self.assertEqual(caught.exception.report, self.read_report())
        self.assertEqual(caught.exception.report["worker_exit"], 7)
        self.assertEqual(caught.exception.report["status"], "failed")

    @needs_inventory
    def test_successfully_exited_leader_does_not_hide_live_group_child(self):
        source = """import os,subprocess,sys
child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])
print(child.pid,flush=True)
os._exit(0)
"""
        with self.assertRaisesRegex(RuntimeError, "left live owned processes") as caught:
            self.run_worker(source)
        report = caught.exception.report
        child_pid = int((self.root / "worker.log").read_text().strip())
        self.assertIn(child_pid, [item["pid"] for item in report["observed_owned"]])
        self.assertEqual(report["worker_exit"], 0)
        self.assertEqual(report["remaining_owned_pids"], [])
        self.assertTrue(report["observed_owned_exit_confirmed"], report)
        self.assertIn("SIGTERM", report["signals"])

    @needs_inventory
    def test_term_ignoring_worker_is_killed_within_single_total_budget(self):
        source = """import signal,time
signal.signal(signal.SIGTERM,signal.SIG_IGN)
print('ready',flush=True)
time.sleep(30)
"""
        started = time.monotonic()
        with self.assertRaises(TimeoutError) as caught:
            self.run_worker(source, timeout_s=0.6, grace_s=0.15)
        report = caught.exception.report
        self.assertEqual(report["signals"], ["SIGTERM", "SIGKILL"])
        self.assertEqual(report["worker_exit"], -signal.SIGKILL)
        self.assertEqual(report["remaining_owned_pids"], [])
        self.assertLess(time.monotonic() - started, 1.5)
        self.assertLess(report["elapsed_s"], 1.0)

    @needs_inventory
    def test_unrelated_owned_by_test_process_is_not_signalled(self):
        control = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"],
                                   start_new_session=True)
        try:
            with self.assertRaises(TimeoutError) as caught:
                self.run_worker("import time;time.sleep(30)", timeout_s=0.4, grace_s=0.1)
            self.assertIsNone(control.poll())
            observed = [item["pid"] for item in caught.exception.report["observed_owned"]]
            self.assertNotIn(control.pid, observed)
        finally:
            control.kill()
            control.wait(timeout=3)

    def test_invalid_seconds_and_commands_have_no_process_or_artifacts(self):
        for name in ("timeout_s", "grace_s"):
            for value in (True, False, 0, -1, float("nan"), float("inf"), "1"):
                with self.subTest(name=name, value=value):
                    options = dict(timeout_s=1, grace_s=0.1)
                    options[name] = value
                    with patch.object(supervisor.subprocess, "Popen") as spawn:
                        with self.assertRaises(ValueError):
                            supervisor.supervise([sys.executable, "-c", "pass"], self.root, **options)
                    spawn.assert_not_called()
        for command in ("echo hi", [], [True], [sys.executable, "a\0b"]):
            with self.subTest(command=command), self.assertRaises(ValueError):
                supervisor.supervise(command, self.root, timeout_s=1)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_popen_error_closes_log_and_reports_redacted_failure(self):
        handles = []
        original_open = supervisor.open_private_text

        @contextmanager
        def tracked_log(path):
            with original_open(path) as stream:
                handles.append(stream)
                yield stream

        def fail(*args, **kwargs):
            self.assertEqual(kwargs["stdout"], subprocess.PIPE)
            self.assertIs(kwargs["shell"], False)
            self.assertIs(kwargs["start_new_session"], True)
            raise OSError("auth_token=private-fixture-value")

        with patch.object(supervisor, "open_private_text", tracked_log), patch.object(
            supervisor.subprocess, "Popen", side_effect=fail,
        ):
            with self.assertRaises(OSError) as caught:
                self.run_worker("pass")
        self.assertTrue(handles[0].closed)
        self.assertEqual(caught.exception.report["status"], "failed")
        self.assertNotIn("private-fixture-value", (self.root / "supervisor.json").read_text())
        self.assertIn("***", caught.exception.report["error"])

    def test_observer_failure_still_attempts_term_and_kill_and_keeps_unknown(self):
        # Fail observation after the real leader identity is already captured.
        with patch.object(supervisor, "_observe", side_effect=OSError("injected observer failure")):
            with self.assertRaises(OSError) as caught:
                self.run_worker("import time;time.sleep(30)", timeout_s=1, grace_s=0.1)
        report = caught.exception.report
        self.assertEqual(report["status"], "unknown")
        self.assertTrue(report["cleanup_errors"])
        self.assertEqual(report["remaining_owned_pids"], [])
        self.assertIsNotNone(report["worker_exit"])
        self.assertTrue(report["output_capture"]["pipe_closed"])
        self.assertEqual(report, self.read_report())

    @needs_inventory
    def test_report_write_error_is_attached_without_replacing_worker_failure(self):
        with patch.object(supervisor, "write_private_json", side_effect=OSError("disk fixture failure")):
            with self.assertRaisesRegex(RuntimeError, "worker failed") as caught:
                self.run_worker("raise SystemExit(3)")
        self.assertEqual(caught.exception.report["status"], "unknown")
        self.assertIn("report_write_error", caught.exception.report)

    def test_reused_leader_identity_never_signals_numeric_process_group(self):
        process = types.SimpleNamespace(create_time=lambda: 2.0)
        with patch.object(supervisor.psutil, "Process", return_value=process):
            with patch.object(supervisor, "_live", return_value=[]):
                with patch.object(supervisor.os, "killpg") as killpg:
                    with self.assertRaisesRegex(RuntimeError, "reused"):
                        supervisor._signal(set(), 12345, 1.0, signal.SIGKILL)
        killpg.assert_not_called()

    def test_process_identity_reuse_is_not_counted_as_live_owned(self):
        process = types.SimpleNamespace(create_time=lambda: 2.0)
        with patch.object(supervisor.psutil, "Process", return_value=process):
            self.assertEqual(supervisor._live({(12345, 1.0)}), [])

    def test_process_exit_between_live_check_and_children_keeps_group_discovery(self):
        leader = types.SimpleNamespace(create_time=lambda: 1.0)

        def disappeared(recursive):
            raise psutil.NoSuchProcess(12345)

        snapshot = types.SimpleNamespace(children=disappeared)
        survivor = types.SimpleNamespace(pid=12346, info=dict(create_time=2.0))
        owned = {(12345, 1.0)}
        with patch.object(supervisor.psutil, "Process", return_value=leader):
            with patch.object(supervisor, "_live", return_value=[snapshot]):
                with patch.object(supervisor.psutil, "process_iter", return_value=[survivor]):
                    with patch.object(supervisor.os, "getpgid", return_value=12345):
                        supervisor._observe(owned, 12345, 1.0)
        self.assertIn((12346, 2.0), owned)

    def test_cleanup_residue_is_unknown_and_raised_report_is_saved(self):
        residue = types.SimpleNamespace(pid=98765)
        with patch.object(supervisor, "_live", return_value=[residue]):
            with patch.object(supervisor, "_observe"), patch.object(supervisor, "_signal"):
                with self.assertRaises(RuntimeError) as caught:
                    self.run_worker("pass", timeout_s=1, grace_s=0.05)
        self.assertEqual(caught.exception.report["status"], "unknown")
        self.assertEqual(caught.exception.report["remaining_owned_pids"], [98765])
        self.assertFalse(caught.exception.report["observed_owned_exit_confirmed"])
        self.assertEqual(caught.exception.report, self.read_report())

    @needs_inventory
    def test_worker_stdout_stderr_split_writes_and_traceback_are_redacted_before_disk(self):
        source = """import os,time
os.write(1,b'stdout auth_to')
time.sleep(.03)
os.write(1,b'ken=fixture-out-secret\\n')
os.write(2,b'stderr password=fixture-err-secret\\n')
raise RuntimeError('api_key=fixture-trace-secret')
"""
        with self.assertRaises(RuntimeError) as caught:
            self.run_worker(source)
        value = (self.root / "worker.log").read_text()
        for secret in ("fixture-out-secret", "fixture-err-secret", "fixture-trace-secret"):
            self.assertNotIn(secret, value)
        self.assertIn("stdout auth_token=***", value)
        self.assertIn("stderr password=***", value)
        self.assertIn("Traceback", value)
        self.assertIn("RuntimeError: api_key=***", value)
        self.assertTrue(caught.exception.report["output_capture"]["complete"])

    @needs_inventory
    def test_worker_overlong_line_is_omitted_and_no_newline_tail_is_redacted(self):
        source = """import os
os.write(1,b'api_key=' + b'X'*40000 + b'\\n')
os.write(2,b'auth_token=fixture-tail-secret')
"""
        report = self.run_worker(source)
        value = (self.root / "worker.log").read_text()
        self.assertNotIn("XXXX", value)
        self.assertNotIn("api_key=", value)
        self.assertNotIn("fixture-tail-secret", value)
        self.assertIn("line exceeding 16384 bytes omitted", value)
        self.assertIn("auth_token=***", value)
        capture = report["output_capture"]
        self.assertEqual(capture["omitted_long_lines"], 1)
        self.assertEqual(capture["omitted_bytes"], 40009)
        self.assertLessEqual(capture["max_buffer_bytes"], 16384)
        self.assertTrue(capture["eof_observed"])
        self.assertFalse(capture["complete"])


class FakeProcessTests(SupervisorFixture, unittest.TestCase):
    """Exercise the same group discovery/signalling code with an explicit process table."""

    def fake_processes(self, stack, *, leader_exited=False, child=False, ignore_term=False,
                       exitcode=0):
        leader_pid, child_pid = 400001, 400002
        state = dict(leader_alive=not leader_exited, child_alive=child, returncode=(
            exitcode if leader_exited else None), signals=[], handles=[])

        class Process:
            def __init__(self, pid):
                self.pid = pid
                self.info = dict(pid=pid, create_time=1.0 if pid == leader_pid else 2.0)

            def create_time(self):
                return self.info["create_time"]

            def status(self):
                alive = state["leader_alive" if self.pid == leader_pid else "child_alive"]
                return psutil.STATUS_RUNNING if alive else psutil.STATUS_ZOMBIE

            def children(self, recursive=False):
                return [Process(child_pid)] if self.pid == leader_pid and child else []

            def send_signal(self, value):
                group_signal(self.pid, value)

        class Popen:
            pid = leader_pid

            @property
            def returncode(self):
                return state["returncode"]

            def poll(self):
                return self.returncode

            def kill(self):
                group_signal(leader_pid, signal.SIGKILL)

            def wait(self, timeout):
                return self.returncode

        def group_signal(pgid, value):
            self.assertEqual(pgid, leader_pid)
            state["signals"].append(value)
            if value == signal.SIGKILL or not ignore_term:
                if state["leader_alive"]:
                    state["returncode"] = -value
                state.update(leader_alive=False, child_alive=False)

        def spawn(*args, **kwargs):
            self.assertEqual(kwargs["stdout"], subprocess.PIPE)
            read_fd, write_fd = os.pipe()
            process = Popen()
            process.stdout = os.fdopen(read_fd, "rb", buffering=0)
            os.write(write_fd, b"fake worker stdout\n")
            os.close(write_fd)
            state["handles"].append(process.stdout)
            self.assertIs(kwargs["shell"], False)
            self.assertIs(kwargs["start_new_session"], True)
            return process

        stack.enter_context(patch.object(supervisor.subprocess, "Popen", side_effect=spawn))
        stack.enter_context(patch.object(supervisor.psutil, "Process", side_effect=Process))
        stack.enter_context(patch.object(supervisor.psutil, "process_iter", side_effect=(
            lambda attrs: [Process(child_pid)] if child else [])))
        stack.enter_context(patch.object(supervisor.os, "getpgid", return_value=leader_pid))
        stack.enter_context(patch.object(supervisor.os, "killpg", side_effect=group_signal))
        return state

    def test_fake_leader_exited_before_child_discovery_still_cleans_group(self):
        from contextlib import ExitStack

        with ExitStack() as stack:
            state = self.fake_processes(stack, leader_exited=True, child=True, ignore_term=True)
            with self.assertRaisesRegex(RuntimeError, "left live owned processes") as caught:
                self.run_worker("fake-only", timeout_s=1, grace_s=0.05)
        report = caught.exception.report
        self.assertEqual(report["signals"], ["SIGTERM", "SIGKILL"])
        self.assertIn(400002, [item["pid"] for item in report["observed_owned"]])
        self.assertEqual(report["remaining_owned_pids"], [])
        self.assertTrue(report["observed_owned_exit_confirmed"])
        self.assertTrue(state["handles"][0].closed)

    def test_fake_success_and_nonzero_exit_keep_reports(self):
        from contextlib import ExitStack

        with ExitStack() as stack:
            state = self.fake_processes(stack, leader_exited=True)
            result = self.run_worker("fake-only")
        self.assertEqual(result["status"], "passed")
        self.assertFalse(result["physical_gpu_exit_checked"])
        self.assertTrue(state["handles"][0].closed)
        with tempfile.TemporaryDirectory() as folder, ExitStack() as stack:
            self.root = Path(folder)
            self.fake_processes(stack, leader_exited=True, exitcode=4)
            with self.assertRaisesRegex(RuntimeError, "worker failed") as caught:
                self.run_worker("fake-only")
            self.assertEqual(caught.exception.report["worker_exit"], 4)
            self.assertEqual(caught.exception.report, self.read_report())

    def test_fake_cleanup_signal_failure_continues_to_kill_and_stays_unknown(self):
        from contextlib import ExitStack

        with ExitStack() as stack:
            self.fake_processes(stack, leader_exited=True, child=True)
            actual_signal = supervisor._signal

            def fail_term(owned, pgid, created, value):
                if value == signal.SIGTERM:
                    raise OSError("fake TERM failure")
                actual_signal(owned, pgid, created, value)

            stack.enter_context(patch.object(supervisor, "_signal", side_effect=fail_term))
            with self.assertRaises(RuntimeError) as caught:
                self.run_worker("fake-only", timeout_s=1, grace_s=0.05)
        self.assertEqual(caught.exception.report["signals"], ["SIGTERM", "SIGKILL"])
        self.assertEqual(caught.exception.report["status"], "unknown")
        self.assertEqual(caught.exception.report["remaining_owned_pids"], [])


class OutputCaptureTests(SupervisorFixture, unittest.TestCase):
    def pipe(self):
        read_fd, write_fd = os.pipe()
        reader = os.fdopen(read_fd, "rb", buffering=0)
        self.addCleanup(lambda: reader.close() if not reader.closed else None)
        self.addCleanup(lambda: os.close(write_fd))
        return reader, write_fd

    def test_partial_line_is_not_written_until_redaction_has_the_whole_line(self):
        reader, writer = self.pipe()
        with supervisor.open_private_text(self.root / "capture.log") as log:
            capture = supervisor._RedactedOutput(reader, log)
            os.write(writer, b"auth_to")
            capture.pump(time.monotonic() + 1)
            self.assertEqual(log.tell(), 0)
            os.write(writer, b"ken=fixture-segment-secret\n")
            capture.pump(time.monotonic() + 1)
            capture.finish()
        value = (self.root / "capture.log").read_text()
        self.assertEqual(value, "auth_token=***\n")

    def test_open_inherited_writer_cannot_hold_capture_finish_or_reader_thread(self):
        reader, writer = self.pipe()
        with supervisor.open_private_text(self.root / "capture.log") as log:
            capture = supervisor._RedactedOutput(reader, log)
            os.write(writer, b"password=fixture-no-newline")
            started = time.monotonic()
            capture.pump(time.monotonic() + 0.1)
            capture.finish()
            self.assertLess(time.monotonic() - started, 0.2)
        self.assertEqual((self.root / "capture.log").read_text(), "password=***")
        self.assertTrue(capture.report()["pipe_closed"])
        self.assertFalse(capture.report()["eof_observed"])
        self.assertFalse(capture.report()["complete"])

    def test_continuous_writer_has_bounded_reads_and_line_memory(self):
        reader, _ = self.pipe()
        with supervisor.open_private_text(self.root / "capture.log") as log:
            capture = supervisor._RedactedOutput(reader, log)
            with patch.object(supervisor.os, "read", return_value=b"X" * 8192) as read:
                capture.pump(time.monotonic() + 1)
            capture.finish()
        self.assertEqual(read.call_count, 8)
        self.assertEqual(capture.report()["bytes_read"], 65536)
        self.assertLessEqual(capture.report()["max_buffer_bytes"], 16384)
        self.assertEqual(capture.report()["omitted_long_lines"], 1)
        self.assertNotIn("XXXX", (self.root / "capture.log").read_text())


if __name__ == "__main__":
    unittest.main()
