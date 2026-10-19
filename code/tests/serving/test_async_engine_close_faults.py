"""Execute Async close failure paths with owned local sockets and a fake child."""

from collections import deque
import socket
import subprocess
import threading
import unittest
from unittest.mock import Mock, patch

from src.serving.backends import async_engine_process as transport
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, _prepared
from tests.serving.test_async_engine_process import row


class StubbornChild:
    """No OS process: every wait really raises the standard TimeoutExpired."""

    def __init__(self, fault=None):
        self.fault, self.waits, self.signals = fault, [], []

    def poll(self):
        if self.fault == "poll": raise PermissionError("CPU poll fault")
        return None

    def wait(self, timeout):
        self.waits.append(timeout)
        if self.fault == "wait": raise OSError("CPU wait fault")
        raise subprocess.TimeoutExpired("self-written CPU child", timeout)

    def terminate(self):
        self.signals.append("terminate")
        if self.fault == "terminate": raise PermissionError("CPU terminate fault")

    def kill(self):
        self.signals.append("kill")
        if self.fault == "kill": raise OSError("CPU kill fault")


class StartupFailure(RuntimeError):
    pass


class SocketWrapper:
    def __init__(self, sock, *, blocking_fault=False, close_fault=False):
        self.sock, self.blocking_fault, self.close_fault = sock, blocking_fault, close_fault

    def fileno(self):
        return self.sock.fileno()

    def setblocking(self, value):
        if self.blocking_fault: raise StartupFailure("CPU setblocking fault")
        self.sock.setblocking(value)

    def close(self):
        self.sock.close()
        if self.close_fault: raise OSError("CPU socket close fault")


class AsyncCloseFaultTests(unittest.TestCase):
    def setUp(self):
        self.driver = patch.object(EngineSpec, "validate_driver_environment", return_value={})
        self.driver.start()
        self.addCleanup(self.driver.stop)
        self.no_process = patch.object(transport.subprocess, "Popen", side_effect=AssertionError("Popen forbidden"))
        self.launch = self.no_process.start()
        self.addCleanup(self.no_process.stop)

    def owner(self, fault=None):
        owner = transport.AsyncEngineProcess.__new__(transport.AsyncEngineProcess)
        owner.limits = transport.AsyncEngineLimits()
        owner._condition = threading.Condition()
        owner._stop, owner._io_done = threading.Event(), threading.Event()
        owner._io_done.set()
        owner._closing, owner._broken = False, None
        owner.last_close_report = None
        owner.engine_close_report = dict(engine_api_shutdown_confirmed=True)
        owner.process = StubbornChild(fault)
        owner._sockets = [sock for _ in range(3) for sock in socket.socketpair()]
        for sock in owner._sockets: self.addCleanup(sock.close)
        owner._data, owner._control, owner._results = owner._sockets[::2]
        owner._thread = Mock()
        owner._thread.is_alive.return_value = False
        owner._data_commands, owner._controls, owner._ready = deque(), deque(), deque(["known"])
        identities, _ = _prepared([row(0), row(1)])
        known = dict(identities[0], status="succeeded", prompt_tokens=11, completion_tokens=1, total_tokens=12)
        owner._slots = {
            "known": transport._Slot(identities[0], 0, accepted=True,
                                    terminal=dict(confirmed=True, result=known), result_bytes=128),
            "pending": transport._Slot(identities[1], 200, accepted=True),
        }
        return owner, known

    def construct(self):
        return transport.AsyncEngineProcess(EngineSpec("/tmp/cpu-fixture", "/tmp/model-python", "/tmp/driver-python"),
                                             GenerationSpec(), startup_timeout_s=0.01)

    def tracked_pairs(self, *, fail_at=None, blocking_fault=False):
        made, count = [], 0
        original = socket.socketpair
        def pair():
            nonlocal count
            count += 1
            if count == fail_at: raise StartupFailure("CPU socketpair fault")
            parent, child = original()
            made.extend((parent, child))
            return SocketWrapper(parent, blocking_fault=blocking_fault), child
        return made, pair

    def test_final_wait_timeout_runs_fd_and_thread_cleanup_and_retains_receipts(self):
        owner, known = self.owner()
        child = owner.process
        report = owner.close(timeout=0.05)
        self.assertEqual(owner.process.signals, ["terminate", "kill"])
        self.assertEqual(len(owner.process.waits), 3)
        self.assertTrue(all(0 <= budget <= 0.05 for budget in owner.process.waits))
        self.assertTrue(owner._stop.is_set())
        self.assertTrue(all(sock.fileno() == -1 for sock in owner._sockets))
        owner._thread.join.assert_called_once()
        self.assertLessEqual(owner._thread.join.call_args.args[0], 0.05)
        self.assertEqual(report["status"], "unknown")
        self.assertFalse(report["process_exit_confirmed"])
        self.assertIsNone(report["gpu_exit_confirmed"])
        self.assertIsNone(report["descendants_exit_confirmed"])
        self.assertIn("direct_child_cleanup: TimeoutExpired", report["cleanup_errors"])
        events = owner.poll(2)
        self.assertIs(events[0].result, known)
        self.assertTrue(events[0].engine_terminal_confirmed)
        self.assertIsNone(events[1].result)
        self.assertFalse(events[1].engine_terminal_confirmed)
        self.assertEqual(owner._usage()["held_requests"], 1)
        self.assertIs(owner.process, child)
        owner.process.wait = Mock(return_value=0)
        owner.process.poll = Mock(return_value=0)
        self.assertTrue(owner.close(timeout=0.05)["process_exit_confirmed"])
        self.assertEqual(owner._usage()["unknown_requests"], 1)

    def test_wait_terminate_kill_and_poll_errors_still_finish_local_io(self):
        for fault in ("wait", "terminate", "kill", "poll"):
            with self.subTest(fault=fault):
                owner, _ = self.owner(fault)
                report = owner.close(timeout=0.05)
                self.assertEqual(report["status"], "unknown")
                self.assertFalse(report["process_exit_confirmed"])
                self.assertTrue(report["cleanup_errors"])
                self.assertTrue(owner._stop.is_set())
                self.assertTrue(all(sock.fileno() == -1 for sock in owner._sockets))
                owner._thread.join.assert_called_once()

    def test_local_socket_or_join_error_keeps_primary_cleanup_error_and_unknown_scope(self):
        owner, known = self.owner()
        owner._sockets[0] = SocketWrapper(owner._sockets[0], close_fault=True)
        owner._thread.join.side_effect = RuntimeError("CPU join fault")
        owner._thread.is_alive.return_value = True
        report = owner.close(timeout=0.05)
        self.assertEqual(report["cleanup_errors"], ["direct_child_cleanup: TimeoutExpired",
                                                    "socket_close: OSError", "io_thread_join: RuntimeError"])
        self.assertFalse(report["io_thread_closed"])
        self.assertEqual(owner.poll(1)[0].result, known)
        self.assertTrue(all(sock.fileno() == -1 for sock in owner._sockets))

    def test_constructor_start_failure_is_not_replaced_by_final_wait_timeout(self):
        made, pair = self.tracked_pairs()
        child = StubbornChild()
        self.launch.side_effect = None
        self.launch.return_value = child
        with patch.object(transport.socket, "socketpair", side_effect=pair), \
                patch.object(transport.threading.Thread, "start", side_effect=StartupFailure("CPU thread start")):
            with self.assertRaisesRegex(StartupFailure, "CPU thread start") as caught:
                self.construct()
        self.assertEqual(len(child.waits), 3)
        self.assertTrue(all(sock.fileno() == -1 for sock in made))
        error = caught.exception
        self.assertIs(error.async_engine_process.process, child)
        self.assertIs(error.async_close_report, error.async_engine_process.last_close_report)
        self.assertFalse(error.async_close_report["process_exit_confirmed"])
        self.assertIn("direct_child_cleanup: TimeoutExpired", error.async_close_report["cleanup_errors"])

    def test_constructor_ready_failure_preserves_primary_and_closes_local_sockets(self):
        class BrokenReady(transport.AsyncEngineProcess):
            def _io(self):
                with self._condition: self._uncertain("CPUReadyFailure")
                self._io_done.set()
        made, pair = self.tracked_pairs()
        self.launch.side_effect = None
        self.launch.return_value = StubbornChild()
        with patch.object(transport.socket, "socketpair", side_effect=pair):
            with self.assertRaisesRegex(RuntimeError, "CPUReadyFailure") as caught:
                BrokenReady(EngineSpec("/tmp/cpu-fixture", "/tmp/model-python", "/tmp/driver-python"),
                            GenerationSpec(), startup_timeout_s=0.1)
        self.assertTrue(all(sock.fileno() == -1 for sock in made))
        self.assertTrue(caught.exception.async_close_report["io_thread_closed"])
        self.assertFalse(caught.exception.async_close_report["process_exit_confirmed"])

    def test_unexpected_startup_close_exception_cannot_mask_launch_error_or_keep_fds(self):
        made, pair = self.tracked_pairs()
        self.launch.side_effect = StartupFailure("CPU launch primary")
        with patch.object(transport.socket, "socketpair", side_effect=pair), \
                patch.object(transport.AsyncEngineProcess, "close", side_effect=LookupError("CPU cleanup secondary")):
            with self.assertRaisesRegex(StartupFailure, "CPU launch primary") as caught:
                self.construct()
        self.assertTrue(all(sock.fileno() == -1 for sock in made))
        self.assertEqual(caught.exception.async_close_report["status"], "unknown")
        self.assertEqual(caught.exception.async_close_report["cleanup_errors"], ["startup_cleanup: LookupError"])

    def test_socketpair_and_setblocking_faults_close_every_created_fd_before_launch(self):
        for fail_at, blocking_fault in ((2, False), (3, False), (None, True)):
            with self.subTest(fail_at=fail_at, blocking_fault=blocking_fault):
                made, pair = self.tracked_pairs(fail_at=fail_at, blocking_fault=blocking_fault)
                with patch.object(transport.socket, "socketpair", side_effect=pair):
                    with self.assertRaises(StartupFailure) as caught:
                        self.construct()
                self.assertTrue(all(sock.fileno() == -1 for sock in made))
                self.assertEqual(caught.exception.async_close_report["status"], "unknown")
        self.launch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
