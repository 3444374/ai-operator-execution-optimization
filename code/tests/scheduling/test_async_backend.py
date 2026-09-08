"""Controlled async I/O validates bounded admission, order, and uncertain outcomes."""

import asyncio
import threading
import time
import unittest

from src.scheduling.core.session import WakeSignal
from src.scheduling.core.session_contract import (
    Acceptance,
    BackendTask,
    OfferedTask,
    SessionSpec,
    TaskKey,
)
from src.scheduling.runtime.async_backend import BoundedAsyncBackend


def task(sequence):
    return BackendTask(
        TaskKey(0, sequence),
        SessionSpec("job", "flow", "fixture"),
        OfferedTask(sequence, b"input", 1, 8),
    )


class AsyncBackendTests(unittest.TestCase):
    def wait_for(self, predicate):
        deadline = time.monotonic() + 3
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("asynchronous fixture did not progress")
            time.sleep(0.001)

    def test_slots_and_reverse_completion(self):
        entered = threading.Event()
        gates = {}

        async def execute(request, endpoint):
            gate = asyncio.Event()
            gates[request.key.sequence] = gate
            if len(gates) == 2:
                entered.set()
            await gate.wait()
            return str(request.key.sequence).encode()

        wake = WakeSignal()
        backend = BoundedAsyncBackend(execute, max_tasks=2, notify=wake.notify)
        try:
            first = backend.try_submit(task(0), "e")
            second = backend.try_submit(task(1), "e")
            self.assertEqual(backend.try_submit(task(2), "e").acceptance, Acceptance.NOT_ACCEPTED)
            self.assertTrue(entered.wait(3))
            handles = ((TaskKey(0, 0), first.handle), (TaskKey(0, 1), second.handle))
            self.assertFalse(backend.close(0.01))
            backend.request_cancel(TaskKey(0, 1), second.handle)
            self.assertEqual(backend.poll(handles, 2), ())
            backend._loop.call_soon_threadsafe(gates[1].set)
            self.wait_for(lambda: backend._slots[TaskKey(0, 1)].completed_order is not None)
            backend._loop.call_soon_threadsafe(gates[0].set)
            self.wait_for(lambda: backend._slots[TaskKey(0, 0)].completed_order is not None)
            # Completed responses still occupy backend slots until poll transfers them.
            self.assertEqual(backend.try_submit(task(2), "e").acceptance, Acceptance.NOT_ACCEPTED)
            completed = backend.poll(handles, 2)
            self.assertEqual([e.key.sequence for e in completed], [1, 0])
            self.assertEqual([e.result for e in completed], [b"1", b"0"])
        finally:
            for gate in gates.values():
                backend._loop.call_soon_threadsafe(gate.set)
            self.wait_for(lambda: all(s.future.done() for s in backend._slots.values()))
            self.assertTrue(backend.close())

    def test_transport_exception_retains_slot_and_reports_once(self):
        async def execute(request, endpoint):
            raise OSError("private diagnostic")

        backend = BoundedAsyncBackend(execute, max_tasks=1, notify=lambda: None)
        submitted = backend.try_submit(task(0), "e")
        try:
            self.wait_for(lambda: backend._slots[TaskKey(0, 0)].completed_order is not None)
            with self.assertRaisesRegex(RuntimeError, "^asynchronous remote outcome unknown$"):
                backend.poll(((TaskKey(0, 0), submitted.handle),), 1)
            self.assertEqual(backend.poll(((TaskKey(0, 0), submitted.handle),), 1), ())
            self.assertEqual(backend.try_submit(task(1), "e").acceptance, Acceptance.NOT_ACCEPTED)
        finally:
            self.assertTrue(backend.close())

    def test_finalizer_error_does_not_leave_io_thread(self):
        async def execute(request, endpoint):
            return b"ok"

        async def finalize():
            raise RuntimeError("private finalizer error")

        backend = BoundedAsyncBackend(execute, max_tasks=1, notify=lambda: None, finalize=finalize)
        with self.assertRaisesRegex(RuntimeError, "^asynchronous transport finalization failed$"):
            backend.close()
        self.assertFalse(backend._thread.is_alive())

    def test_oversized_result_is_not_delivered_as_a_safe_terminal(self):
        async def execute(request, endpoint):
            return b"x" * 9

        backend = BoundedAsyncBackend(execute, max_tasks=1, notify=lambda: None)
        submitted = backend.try_submit(task(0), "e")
        try:
            self.wait_for(lambda: backend._slots[TaskKey(0, 0)].completed_order is not None)
            with self.assertRaisesRegex(RuntimeError, "reservation"):
                backend.poll(((TaskKey(0, 0), submitted.handle),), 1)
            self.assertEqual(len(backend._slots), 1)
        finally:
            self.assertTrue(backend.close())


if __name__ == "__main__":
    unittest.main()
