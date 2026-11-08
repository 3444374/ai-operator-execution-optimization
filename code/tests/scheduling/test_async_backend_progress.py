"""Notification metadata does not change bounded async ownership or polling."""
import asyncio
from concurrent.futures import Future
import threading
import unittest
from unittest.mock import patch

from src.scheduling.runtime import async_backend as runtime
from src.scheduling.core.session_contract import Acceptance, Terminal, Uncertain
from tests.scheduling import test_async_backend as base


class BackendProgressTests(unittest.TestCase):
    wait_for = base.AsyncBackendTests.wait_for

    def backend(self, maximum=3, execute=None):
        gates = {}
        async def controlled(task, endpoint):
            gate = gates[task.key.sequence] = asyncio.Event()
            await gate.wait()
            return str(task.key.sequence).encode()
        backend = runtime.BoundedAsyncBackend(execute or controlled, max_tasks=maximum,
                                             notify=lambda: None, isolate_failures=True)
        self.addCleanup(self.cleanup, backend, gates)
        return backend, gates

    def cleanup(self, backend, gates):
        if getattr(backend,'_test_stuck_submit',False):
            backend._loop.call_soon_threadsafe(backend._loop.stop)
            return  # A daemon watchdog reported the regression; do not hang cleanup.
        if backend._thread.is_alive():
            for gate in gates.values():
                backend._loop.call_soon_threadsafe(gate.set)
            self.wait_for(lambda: all(slot.future.done() for slot in backend._slots.values()))
            self.assertTrue(backend.close())

    def submit(self, backend, gates, count):
        submitted = [backend.try_submit(base.task(i), 'e') for i in range(count)]
        self.wait_for(lambda: len(gates)==count)
        return tuple((base.task(i).key,value.handle) for i,value in enumerate(submitted))

    def test_submit_and_empty_poll_do_not_publish_completion(self):
        backend,gates = self.backend()
        handles = self.submit(backend,gates,1)
        self.assertEqual(backend.progress(),runtime.BackendProgress(0,False,False))
        self.assertEqual(backend.poll(handles,1),())
        self.assertEqual(backend.progress().generation,0)
        backend._loop.call_soon_threadsafe(gates[0].set)
        self.wait_for(lambda: backend.progress().pending)
        self.assertEqual(backend.progress().generation,1)
        self.assertEqual(backend.poll(handles,2)[0].result,b'0')
        self.assertFalse(backend.progress().pending)

    def test_burst_is_not_settled_by_a_bounded_full_poll(self):
        backend,gates = self.backend()
        handles = self.submit(backend,gates,3)
        for gate in gates.values():
            backend._loop.call_soon_threadsafe(gate.set)
        self.wait_for(lambda: backend.progress().generation==3)
        self.assertEqual(len(backend.poll(handles,2)),2)
        self.assertTrue(backend.progress().pending)
        self.assertEqual(len(backend.poll(handles,2)),1)
        self.assertFalse(backend.progress().pending)
        self.assertFalse(backend._slots)

    def test_contended_empty_poll_retains_notification_and_capacity(self):
        backend,gates = self.backend(1)
        handles = self.submit(backend,gates,1)
        backend._loop.call_soon_threadsafe(gates[0].set)
        self.wait_for(lambda: backend.progress().pending)
        with backend._lock:
            self.assertEqual(backend.poll(handles,1),())
        state = backend.progress()
        self.assertTrue(state.pending)
        self.assertTrue(state.contended)
        self.assertEqual(backend.try_submit(base.task(1),'e').acceptance,Acceptance.NOT_ACCEPTED)
        self.assertEqual(len(backend.poll(handles,2)),1)
        self.assertFalse(backend.progress().pending)
        self.assertFalse(backend.progress().contended)

    def test_completion_racing_with_poll_remains_pending(self):
        backend,gates = self.backend(2)
        handles = self.submit(backend,gates,2)
        backend._loop.call_soon_threadsafe(gates[0].set)
        self.wait_for(lambda: backend.progress().generation==1)
        first = backend._slots[base.task(0).key].future
        result = first.result
        second_done = threading.Event()
        backend._notify = lambda: second_done.set() if backend.progress().generation==2 else None
        def publish_second(*args,**kwargs):
            value = result(*args,**kwargs)
            backend._loop.call_soon_threadsafe(gates[1].set)
            self.assertTrue(second_done.wait(3))
            return value
        with patch.object(first,'result',publish_second):
            self.assertEqual([event.key.sequence for event in backend.poll(handles,3)],[0])
        self.assertTrue(backend.progress().pending)
        self.assertEqual([event.key.sequence for event in backend.poll(handles,3)],[1])
        self.assertFalse(backend.progress().pending)

    def test_inline_done_callback_and_notification_reader_do_not_reenter_slot_lock(self):
        backend,gates = self.backend(1)
        received = []
        backend._notify = lambda: received.append(backend.progress())
        future = Future();future.set_result(b'ok')
        def completed(coroutine,loop):
            coroutine.close()
            return future
        submitted,errors = [],[]
        def invoke():
            try:
                submitted.append(backend.try_submit(base.task(0),'e'))
            except BaseException as error:
                errors.append(error)
        with patch.object(runtime.asyncio,'run_coroutine_threadsafe',completed):
            thread = threading.Thread(target=invoke,daemon=True)
            thread.start();thread.join(3)
        backend._test_stuck_submit = thread.is_alive()
        self.assertFalse(thread.is_alive(),'inline callback deadlocked the submit slot lock')
        self.assertFalse(errors)
        self.assertEqual(received,[runtime.BackendProgress(1,True,False)])
        self.assertEqual(backend.poll(((base.task(0).key,submitted[0].handle),),2)[0].result,b'ok')

    def test_unknown_keeps_slot_until_retirement_and_retirement_publishes(self):
        async def unknown(task,endpoint):
            raise OSError('CPU transport outcome unknown')
        backend,gates = self.backend(1,unknown)
        submission = backend.try_submit(base.task(0),'e')
        handles = ((base.task(0).key,submission.handle),)
        self.wait_for(lambda: backend.progress().pending)
        self.assertIsInstance(backend.poll(handles,2)[0],Uncertain)
        self.assertEqual(len(backend._slots),1)
        self.assertEqual(backend.try_submit(base.task(1),'e').acceptance,Acceptance.NOT_ACCEPTED)
        with self.assertRaises(RuntimeError):
            backend.retire_producer((base.task(0).key,))
        self.assertTrue(backend.close())
        before = backend.progress().generation
        self.assertEqual(backend.retire_producer((base.task(0).key,)),1)
        self.assertEqual(backend.progress().generation,before+1)
        event = backend.poll(handles,2)[0]
        self.assertIsInstance(event,Terminal)
        self.assertEqual(event.status,'cancelled')
        self.assertFalse(backend._slots)
