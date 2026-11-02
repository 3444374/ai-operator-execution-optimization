"""RPC permits follow remote terminal evidence, never the caller Task lifetime."""

import asyncio
from concurrent.futures import Future
import threading
import time
from types import SimpleNamespace
import unittest

from tests.execution_provider.test_lotus_rpc_capacity import fixture, Handle


class BlockingRPC:
    def __init__(self):
        self.condition = threading.Condition()
        self.release = threading.Event()
        self.started = 0

    def data(self):
        with self.condition:
            self.started += 1
            self.condition.notify_all()
        if not self.release.wait(5):
            raise TimeoutError('fixture did not release the RPC')
        return dict(status='accepted')

    def entered(self, count):
        with self.condition:
            return self.condition.wait_for(lambda: self.started >= count, 3)

    def snapshot(self):
        return dict(control='returned')


class RPCTerminalTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_cancel_cannot_spend_control_threads_or_issue_extra_RPCs(self):
        owner, original = fixture(3)
        original_handle = owner.model_actor
        remote = BlockingRPC()
        handle = owner.model_actor = Handle(remote, owner._model_actor_options()['max_concurrency'])
        tasks = []
        try:
            for i in range(3):
                task = asyncio.create_task(owner._data_rpc('data'))
                tasks.append(task)
                if i == 0:
                    self.assertTrue(await asyncio.to_thread(remote.entered, 1))
                await asyncio.sleep(0)
                task.cancel()
                await asyncio.sleep(0)
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            control = await asyncio.wait_for(asyncio.wrap_future(handle.snapshot.remote().future()), 3)
            self.assertEqual(control, dict(control='returned'))
            self.assertEqual(remote.started, 1)
            self.assertEqual(sum(name == 'data' for name, _ in handle.calls), 1)
            self.assertEqual(owner._rpc_pending, 1)
            self.assertEqual(owner._rpc_inflight, 1)
            self.assertEqual(owner._rpc_unconfirmed, {})
            remote.release.set()
            await asyncio.wrap_future(next(f for name, f in handle.calls if name == 'data'))
            self.assertEqual(owner._rpc_pending, 0)
            self.assertEqual(owner._rpc_inflight, 0)
        finally:
            remote.release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            handle.pool.shutdown()
            original.close(1)
            original_handle.pool.shutdown()

    async def test_future_access_error_blocks_new_RPCs_and_requires_object_readiness_to_clear(self):
        owner, actor = fixture(2)
        handle = owner.model_actor
        issued, terminal = [], threading.Event()
        def data():
            def unavailable():
                raise RuntimeError('synthetic Future acquisition failure after issuance')
            ref = SimpleNamespace(future=unavailable)
            issued.append(ref)
            return ref
        owner.model_actor = SimpleNamespace(data=SimpleNamespace(remote=data))
        try:
            with self.assertRaisesRegex(RuntimeError, 'Future acquisition'):
                await owner._data_rpc('data')
            with self.assertRaisesRegex(RuntimeError, 'terminal observer'):
                await owner._data_rpc('data')
            self.assertEqual(len(issued), 1)
            self.assertEqual(owner._rpc_pending, 1)
            self.assertEqual(owner._rpc_inflight, 1)
            self.assertEqual(len(owner._rpc_unconfirmed), 1)
            owner.ray = SimpleNamespace(wait=lambda refs, **options: (refs if terminal.is_set() else [], []))
            report = owner._rpc_cleanup_status(dict(status='retained', model_retained=True, cleanup_errors=[]),
                                               deadline=time.monotonic())
            self.assertEqual(report['status'], 'unknown')
            self.assertFalse(report['model_retained'])
            self.assertEqual(report['data_rpc_unconfirmed'], 1)
            terminal.set()
            report = owner._rpc_cleanup_status(dict(status='closed', cleanup_errors=[]), deadline=time.monotonic())
            self.assertEqual(report['status'], 'closed')
            self.assertEqual(report['data_rpc_pending'], 0)
            self.assertEqual(owner._rpc_inflight, 0)
            self.assertEqual(owner._rpc_unconfirmed, {})
        finally:
            actor.close(1)
            handle.pool.shutdown()

    async def test_cancelled_local_Future_is_not_a_remote_terminal_or_a_successful_close(self):
        owner, actor = fixture(1)
        handle = owner.model_actor
        native = Future()
        ref = SimpleNamespace(future=lambda: native)
        owner.model_actor = SimpleNamespace(data=SimpleNamespace(remote=lambda: ref))
        owner.ray = SimpleNamespace(wait=lambda refs, **options: ([], refs))
        task = asyncio.create_task(owner._data_rpc('data'))
        try:
            await asyncio.sleep(0)
            self.assertEqual(owner._rpc_inflight, 1)
            self.assertTrue(native.cancel())
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(owner._rpc_pending, 1)
            self.assertEqual(owner._rpc_inflight, 1)
            report = owner._rpc_cleanup_status(dict(status='closed', cleanup_errors=[]), deadline=time.monotonic())
            self.assertEqual(report['status'], 'unknown')
            with self.assertRaisesRegex(RuntimeError, 'terminal observer'):
                await owner._data_rpc('data')
            owner.ray.wait = lambda refs, **options: (refs, [])
            report = owner._rpc_cleanup_status(dict(status='closed', cleanup_errors=[]), deadline=time.monotonic())
            self.assertEqual(report['status'], 'closed')
            self.assertEqual(owner._rpc_pending, 0)
        finally:
            await asyncio.gather(task, return_exceptions=True)
            actor.close(1)
            handle.pool.shutdown()

    async def test_remote_exception_releases_once_and_does_not_poison_following_RPC(self):
        owner, actor = fixture(1)
        handle = owner.model_actor
        futures = []
        def data():
            future = Future()
            futures.append(future)
            if len(futures) == 1:
                future.set_exception(ValueError('remote data method raised'))
            else:
                future.set_result('second returned')
            return SimpleNamespace(future=lambda: future)
        owner.model_actor = SimpleNamespace(data=SimpleNamespace(remote=data))
        try:
            with self.assertRaisesRegex(ValueError, 'remote data method'):
                await owner._data_rpc('data')
            self.assertEqual(owner._rpc_pending, 0)
            self.assertEqual(owner._rpc_inflight, 0)
            self.assertEqual(await owner._data_rpc('data'), 'second returned')
            self.assertEqual(owner._rpc_pending, 0)
            self.assertEqual(owner._rpc_inflight, 0)
            self.assertEqual(owner._rpc_unconfirmed, {})
        finally:
            actor.close(1)
            handle.pool.shutdown()

    async def test_observer_failure_wakes_already_queued_calls_without_issuing_them(self):
        owner, actor = fixture(4)
        handle = owner.model_actor
        first, issued = Future(), []
        def data():
            def unavailable():
                raise RuntimeError('synthetic Future registration unavailable')
            ref = SimpleNamespace(future=(lambda: first) if not issued else unavailable)
            issued.append(ref)
            return ref
        owner.model_actor = SimpleNamespace(data=SimpleNamespace(remote=data))
        tasks = [asyncio.create_task(owner._data_rpc('data')) for _ in range(4)]
        try:
            await asyncio.sleep(0)
            self.assertEqual(len(issued), 1)
            first.set_result('first returned')
            results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 3)
            self.assertEqual(results[0], 'first returned')
            self.assertTrue(all(isinstance(value, RuntimeError) for value in results[1:]))
            self.assertEqual(len(issued), 2)
            self.assertEqual(owner._rpc_pending, 1, 'only the issued unobserved RPC is retained')
            self.assertEqual(owner._rpc_inflight, 1)
            self.assertEqual(len(owner._rpc_unconfirmed), 1)
        finally:
            first.set_result(None) if not first.done() else None
            await asyncio.gather(*tasks, return_exceptions=True)
            actor.close(1)
            handle.pool.shutdown()

    async def test_partial_callback_registration_and_late_notification_cannot_release_twice(self):
        class BrokenRegistration(Future):
            def add_done_callback(self, callback):
                super().add_done_callback(callback)
                raise RuntimeError('synthetic error after callback registration')
        for already_done in (False, True):
            with self.subTest(already_done=already_done):
                owner, actor = fixture(1)
                handle = owner.model_actor
                native = BrokenRegistration()
                if already_done:
                    native.set_result('returned before registration')
                ref = SimpleNamespace(future=lambda: native)
                owner.model_actor = SimpleNamespace(data=SimpleNamespace(remote=lambda: ref))
                try:
                    with self.assertRaisesRegex(RuntimeError, 'after callback registration'):
                        await owner._data_rpc('data')
                    self.assertEqual(owner._rpc_pending, 0 if already_done else 1)
                    self.assertEqual(owner._rpc_inflight, 0 if already_done else 1)
                    if not already_done:
                        # The object is terminal before Python's local callback
                        # notification; the independent readiness proof wins.
                        owner.ray = SimpleNamespace(wait=lambda refs, **options: (refs, []))
                        report = owner._rpc_cleanup_status(dict(status='closed', cleanup_errors=[]),
                                                           deadline=time.monotonic())
                        self.assertEqual(report['status'], 'closed')
                        native.set_result('late local notification')
                    self.assertEqual(owner._rpc_pending, 0)
                    self.assertEqual(owner._rpc_inflight, 0)
                    self.assertEqual(owner._rpc_unconfirmed, {})
                finally:
                    actor.close(1)
                    handle.pool.shutdown()


class RPCLoopShutdownTests(unittest.TestCase):
    def test_loop_shutdown_cannot_refund_RPC_but_late_terminal_allows_rebinding(self):
        owner, original = fixture(1)
        original_handle = owner.model_actor
        remote = BlockingRPC()
        handle = owner.model_actor = Handle(remote, owner._model_actor_options()['max_concurrency'])
        try:
            async def start():
                asyncio.create_task(owner._data_rpc('data'))
                self.assertTrue(await asyncio.to_thread(remote.entered, 1))
                # asyncio.run cancels the local waiter and closes this loop.
            asyncio.run(start())
            self.assertTrue(owner._rpc_loop.is_closed())
            self.assertEqual(owner._rpc_pending, 1)
            self.assertEqual(owner._rpc_inflight, 1)
            future = next(f for name, f in handle.calls if name == 'data')
            self.assertFalse(future.cancelled())
            with self.assertRaisesRegex(RuntimeError, 'previous transport loop'):
                asyncio.run(owner._data_rpc('data'))
            self.assertEqual(remote.started, 1)
            remote.release.set()
            future.result(3)
            with owner._rpc_state_lock:
                self.assertTrue(owner._rpc_state_lock.wait_for(lambda: owner._rpc_pending == 0, 3))
            self.assertEqual(owner._rpc_inflight, 0)
            self.assertEqual(asyncio.run(owner._data_rpc('data')), dict(status='accepted'))
            self.assertEqual(remote.started, 2)
        finally:
            remote.release.set()
            handle.pool.shutdown()
            original.close(1)
            original_handle.pool.shutdown()
