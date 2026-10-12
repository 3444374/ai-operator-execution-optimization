"""Controlled async I/O validates bounded admission, order, and uncertain outcomes."""

import asyncio
import threading
import time
import unittest

from src.scheduling.core.session import SessionEngine, WakeSignal
from src.scheduling.core.session_contract import (
    Acceptance,
    BackendTask,
    OfferedTask,
    SessionSpec,
    TaskKey,
    Terminal,
    Uncertain,
    Usage,
)
from src.scheduling.core.session_jobs import JobBudget
from src.scheduling.runtime.async_backend import BoundedAsyncBackend, ConfirmedTaskFailure
from tests.scheduling.test_incremental_session import setup, task as offered_task


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

    def test_confirmed_failure_emits_one_terminal_and_reuses_only_its_slot(self):
        for isolate in (False, True):
            with self.subTest(isolate=isolate):
                reason = {'exception_type': 'builtins.ValueError', 'cause_types': []}
                failure = ConfirmedTaskFailure(task(0).key, reason)
                async def execute(request, endpoint):
                    if request.key.sequence == 0:
                        raise failure
                    return b'ok'
                backend = BoundedAsyncBackend(execute, max_tasks=1, notify=lambda: None,
                                              isolate_failures=isolate)
                submitted = backend.try_submit(task(0), 'e')
                try:
                    self.wait_for(lambda: backend._slots[task(0).key].completed_order is not None)
                    handles = ((task(0).key, submitted.handle),)
                    self.assertEqual(backend.poll(handles, 1),
                                     (Terminal(task(0).key, submitted.handle, 'failed'),))
                    self.assertEqual(backend.poll(handles, 1), ())
                    self.assertIs(failure.reason, reason)
                    second = backend.try_submit(task(1), 'e')
                    self.assertEqual(second.acceptance, Acceptance.ACCEPTED)
                    self.wait_for(lambda: backend._slots[task(1).key].completed_order is not None)
                    self.assertEqual(backend.poll(((task(1).key, second.handle),), 1),
                                     (Terminal(task(1).key, second.handle, 'completed', b'ok'),))
                    self.assertFalse(backend._slots)
                finally:
                    self.assertTrue(backend.close())

    def test_wrong_confirmed_identity_and_cancelled_future_remain_uncertain(self):
        for cancelled in (False, True):
            with self.subTest(cancelled=cancelled):
                async def execute(request, endpoint):
                    if cancelled:
                        raise asyncio.CancelledError()
                    raise ConfirmedTaskFailure(TaskKey(99, 0), {'exception_type': 'builtins.ValueError'})
                backend = BoundedAsyncBackend(execute, max_tasks=1, notify=lambda: None,
                                              isolate_failures=True)
                submitted = backend.try_submit(task(0), 'e')
                try:
                    self.wait_for(lambda: backend._slots[task(0).key].completed_order is not None)
                    handles = ((task(0).key, submitted.handle),)
                    self.assertEqual(backend.poll(handles, 1),
                                     (Uncertain(task(0).key, submitted.handle),))
                    self.assertEqual(backend.poll(handles, 1), ())
                    self.assertEqual(backend.try_submit(task(1), 'e').acceptance, Acceptance.NOT_ACCEPTED)
                finally:
                    self.assertTrue(backend.close())

    def test_confirmed_failure_preserves_first_error_and_other_job_compute(self):
        template, _, _, clock = setup(held_tasks=4, input_bytes=16, result_bytes=16,
                                      active_requests=2, active_work=2)
        gates, calls = {}, []
        async def execute(request, endpoint):
            calls.append(request.key)
            gate = asyncio.Event()
            gates[request.key] = gate
            await gate.wait()
            if request.key.session_id == failed.session_id:
                raise ConfirmedTaskFailure(request.key, {'exception_type': 'builtins.ValueError'})
            return b'ok'
        backend = BoundedAsyncBackend(execute, max_tasks=2, notify=lambda: engine.wake.notify(),
                                      isolate_failures=True)
        engine = SessionEngine(template.capacity.limits, backend, template.policies,
                               max_jobs=2, clock=clock)
        first = engine.register_job('failed-job', JobBudget(2, 8, 8, 2, 2))
        other = engine.register_job('other-job', JobBudget(2, 8, 8, 2, 2))
        failed = engine.open(SessionSpec('first', 'flow', 'fixture'), job=first)
        peer = engine.open(SessionSpec('other', 'flow', 'fixture'), job=other)
        failed.offer((offered_task(0),))
        peer.offer((offered_task(0),))
        engine.advance()
        failed_key, peer_key = TaskKey(failed.session_id, 0), TaskKey(peer.session_id, 0)
        try:
            self.wait_for(lambda: len(gates) == 2)
            peer_usage = engine.capacity.usage(job_id=other.job_id)
            failed.fail('first consumer error')
            backend._loop.call_soon_threadsafe(gates[failed_key].set)
            self.wait_for(lambda: backend._slots[failed_key].completed_order is not None)
            engine.advance()
            self.assertEqual(failed.error, 'first consumer error')
            self.assertEqual(engine.capacity.usage(job_id=first.job_id), Usage())
            self.assertEqual(engine.capacity.usage(job_id=other.job_id), peer_usage)
            self.assertEqual(engine.capacity.usage().active_requests, 1)
            self.assertEqual(set(backend._slots), {peer_key})
            for _ in range(4): engine.advance()
            self.assertEqual(len(calls), 2)
            self.assertEqual(engine.capacity.usage(job_id=other.job_id), peer_usage)
            backend._loop.call_soon_threadsafe(gates[peer_key].set)
            self.wait_for(lambda: backend._slots[peer_key].completed_order is not None)
            engine.advance()
            delivery = peer.advance(1).deliveries[0]
            self.assertEqual(delivery.result, b'ok')
            peer.release((delivery.lease_id,))
        finally:
            for gate in tuple(gates.values()): backend._loop.call_soon_threadsafe(gate.set)
            self.wait_for(lambda: all(s.future.done() for s in backend._slots.values()))
            failed.close_consumer(clean=True)
            peer.close_consumer(clean=True)
            engine.close_job(first)
            engine.close_job(other)
            engine.advance()
            self.assertTrue(backend.close())
        self.assertEqual(engine.capacity.usage(), Usage())

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
