"""Actual IPC close paths over memory only: no sockets, process or model."""

import asyncio
from collections import deque
from dataclasses import asdict
import json
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.serving.backends import async_engine_process as parent
from src.serving.backends import async_engine_worker as child
from src.serving.backends.async_engine_wire import _frame, _HEADER
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from tests.serving.async_engine_fixture import FixtureEngine


class MemorySocket:
    def __init__(self, incoming=b""):
        self.incoming = bytearray(incoming)
        self.sent = bytearray()

    def setblocking(self, value):
        pass

    def set_inheritable(self, value):
        pass

    def close(self):
        pass

    def send(self, value):
        self.sent.extend(value)
        return len(value)

    def recv(self, maximum):
        return b""

    async def read(self, maximum):
        if not self.incoming:
            await asyncio.Event().wait()
        value = bytes(self.incoming[:maximum])
        del self.incoming[:maximum]
        return value


class MemoryLoop(asyncio.SelectorEventLoop):
    # Fixture work stays on one thread; no OS wakeup socket is needed.
    def _make_self_pipe(self):
        pass

    def _close_self_pipe(self):
        pass


def limits():
    return AsyncEngineLimits(max_inflight=1, result_slots=1, input_bytes=4096,
        item_input_bytes=4096, result_bytes=4096, item_result_bytes=4096)


def messages(raw):
    values = []
    while raw:
        size = _HEADER.unpack(raw[:4])[0]
        values.append(json.loads(raw[4:4 + size]))
        raw = raw[4 + size:]
    return values


class CloseBudgetTests(unittest.TestCase):
    def test_parent_sends_remaining_api_time_and_exit_alone_keeps_unknown(self):
        for timeout in (30., .25):
            with self.subTest(timeout=timeout):
                owner = object.__new__(parent.AsyncEngineProcess)
                owner.limits = limits()
                owner._condition = threading.Condition()
                owner._stop, owner._io_done = threading.Event(), threading.Event()
                owner._closing = owner._close_sent = owner._drain_requested = False
                owner._data, owner._control, owner._results = (MemorySocket() for _ in range(3))
                owner._sockets = [owner._data, owner._control, owner._results]
                owner._data_commands, owner._controls, owner._ready = deque(), deque(), deque()
                owner._slots = {"owner:0": parent._Slot({}, 9, terminal={"confirmed": False})}
                owner._owner_uid, owner._sequence = "owner", 1
                owner.ready_report, owner.engine_close_report = {"engine_uid": "memory"}, None
                owner._producer_report = owner._producer_quiescence = None
                owner._retired_unknown, owner._submitted_batches = set(), 0
                owner._broken, owner._thread = "ModelOutcomeUnknown", None

                class Process:
                    pid = 123
                    returncode = None

                    def poll(self):
                        if owner._io_done.is_set():
                            self.returncode = 0
                        return self.returncode

                    def wait(self, timeout):
                        self.returncode = 0

                owner.process = owner._owned_process = Process()
                memory_select = SimpleNamespace(select=lambda reads, writes, errors, timeout:
                                                (reads, writes, []))
                with patch.object(parent, "time", SimpleNamespace(monotonic=lambda: 100.)), \
                        patch.object(parent, "select", memory_select), \
                        patch.object(owner._condition, "wait", side_effect=lambda timeout: owner._io()):
                    report = owner.close(timeout=timeout)
                packet, = messages(owner._control.sent)
                self.assertEqual(packet.get("op"), "close")
                self.assertEqual(packet.get("deadline"), 100. + timeout - min(1., timeout / 3))
                self.assertEqual(report["status"], "unknown")
                self.assertTrue(report["process_exit_confirmed"])
                self.assertIsNone(report["producer_quiescence"])
                self.assertEqual(report["unknown_requests"], 1)
                self.assertEqual(report["input_bytes"], 9)

    def test_actual_worker_uses_close_time_and_keeps_failure_cleanup_separate(self):
        cases = (
            ("long", 125., 0., None, [25.], True),
            ("short", 100.25, 1., None, [.25], False),
            ("expired", 99., 0., None, [], False),
            ("control_error", None, 0., None, [1.], None),
            ("close_error", 120., 0., 0., [20., 20.], None),
            ("close_error_expired", 120., 0., 30., [20.], None),
        )
        for name, deadline, required, failure_elapsed, expected, confirmed in cases:
            with self.subTest(case=name):
                calls, frames = self.worker_case(deadline, required, failure_elapsed)
                self.assertEqual(calls, expected)
                closed = [value for value in frames if value["op"] == "closed"]
                if confirmed is None:
                    self.assertFalse(closed)
                    self.assertEqual([value["op"] for value in frames], ["ready", "error"])
                else:
                    self.assertEqual(len(closed), 1)
                    self.assertIs(closed[0]["report"]["engine_api_shutdown_confirmed"], confirmed)
                    self.assertEqual(closed[0]["report"]["status"], "closed" if confirmed else "unknown")

    def test_small_control_frames_cover_close_and_limit_error_receipts(self):
        capacity = AsyncEngineLimits(max_inflight=1, item_result_bytes=2048,
                                     control_frame_bytes=2048)
        calls, frames = self.worker_case(120., 0., None, capacity=asdict(capacity))
        self.assertEqual(calls, [20.])
        self.assertEqual([value['op'] for value in frames], ['ready', 'closed'])
        self.assertTrue(frames[-1]['report']['engine_api_shutdown_confirmed'])
        packet = _frame(frames[-1], capacity.control_frame_bytes)
        size, = _HEADER.unpack(packet[:4])
        self.assertEqual(size, len(packet)-4)
        self.assertLess(size, 4096)
        self.assertEqual(_frame(frames[-1], size), packet)
        with self.assertRaisesRegex(ValueError, 'byte capacity'):
            _frame(frames[-1], size-1)

        calls, frames = self.worker_case(None, 0., None, capacity=asdict(capacity))
        self.assertEqual(calls, [1.])
        self.assertEqual([value['op'] for value in frames], ['ready', 'error'])
        self.assertEqual(frames[-1]['error_type'], 'ValueError')
        _frame(frames[-1], capacity.control_frame_bytes)

        oversized_error = type('E'*capacity.control_frame_bytes, (RuntimeError,), {})
        calls, frames = self.worker_case(120., 0., 0., capacity=asdict(capacity),
                                        close_error=oversized_error)
        self.assertEqual(calls, [20., 20.])
        self.assertEqual([value['op'] for value in frames], ['ready'])

        calls, frames = self.worker_case(None, 0., None,
            capacity=asdict(capacity) | dict(control_slots=0))
        self.assertFalse(calls)
        self.assertEqual([value['op'] for value in frames], ['error'])
        self.assertEqual(frames[0]['error_type'], 'ValueError')

    def worker_case(self, deadline, required, failure_elapsed, *, capacity=None,
                    close_error=RuntimeError):
        clock, calls = [100.], []
        capacity = asdict(limits()) if capacity is None else capacity
        opening = dict(op="open", owner_uid="memory-owner", engine=asdict(EngineSpec(
            "/tmp/cpu-fixture", sys.executable, sys.executable, max_model_len=1024)),
            generation=asdict(GenerationSpec()), limits=capacity)
        command = dict(op="invalid") if deadline is None else dict(op="close", deadline=deadline)
        control = MemorySocket(_frame(opening, 65536) + _frame(command, 65536))
        channels = {1: MemorySocket(), 2: control, 3: MemorySocket()}

        class ObservedEngine(FixtureEngine):
            async def close(self, timeout=10):
                calls.append(timeout)
                if len(calls) == 1 and failure_elapsed is not None:
                    clock[0] += failure_elapsed
                    raise close_error("fixture close error")
                if timeout < required:
                    return dict(status="unknown", engine_api_shutdown_confirmed=False,
                                gpu_exit_confirmed=None)
                return await super().close(timeout)

        async def run():
            loop = asyncio.get_running_loop()

            async def receive(sock, maximum):
                return await sock.read(maximum)

            async def send(sock, value):
                sock.sent.extend(value)

            with patch.object(loop, "sock_recv", side_effect=receive), \
                    patch.object(loop, "sock_sendall", side_effect=send), \
                    patch.object(child, "socket", SimpleNamespace(socket=lambda fileno: channels[fileno])), \
                    patch.object(child, "time", SimpleNamespace(monotonic=lambda: clock[0])):
                await child.serve(1, 2, 3, engine_factory=ObservedEngine)

        with patch("socket.socketpair", side_effect=AssertionError("memory fixture must not open sockets")), \
                patch("socket.socket", side_effect=AssertionError("memory fixture must not open sockets")):
            loop = MemoryLoop()
            try:
                loop.run_until_complete(run())
            finally:
                loop.close()
        return calls, messages(control.sent)


if __name__ == "__main__":
    unittest.main()
