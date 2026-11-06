"""Same-process cleanup uses the original retained owner and CPU pipe fixtures."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import gc
import os
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import weakref

from tests.execution_provider import test_records_service_owner as service
from tests.execution_provider import test_service_feedback_rpc_retention as snapshots


class RecoveryRuntime(service.fixtures.ThreadedRuntime):
    def __init__(self, rows):
        super().__init__(rows)
        self.identity = dict(job_id='cpu-original-job', node_id='cpu-original-node')
        self.ray.get_runtime_context = lambda: SimpleNamespace(
            get_job_id=lambda: self.identity['job_id'], get_node_id=lambda: self.identity['node_id'])


class ServiceRecoveryTests(unittest.TestCase):
    setUp = service.ServiceOwnerTests.setUp
    owner = service.ServiceOwnerTests.owner
    rows = service.ServiceOwnerTests.rows
    until = service.ServiceOwnerTests.until
    _run = snapshots.ServiceSnapshotRetentionTests._run
    _owner_and_query = snapshots.ServiceSnapshotRetentionTests._owner_and_query

    def test_pre_ready_failure_retries_original_owner_without_returning_unknown_bytes(self):
        with RecoveryRuntime([]) as runtime:
            owner = self.owner()
            grant = owner._service_allocation
            original = dict(runtime.identity)
            before = self.pool.snapshot()
            failure = LookupError('pre-ready remote declaration failure')
            with patch.object(runtime.ray, 'remote', side_effect=failure):
                with self.assertRaises(LookupError) as caught:
                    owner.start()
            self.assertIs(caught.exception, failure)
            self.assertEqual(owner._runtime_binding.original[1], original)
            self.assertIsNone(grant._runtime_identity)
            self.assertIsNone(owner._actors.model_ready)
            self.assertIsNone(owner._actors.model_actor)
            with patch.object(owner, 'close', wraps=owner.close) as close:
                runtime.identity = dict(original, job_id='replacement-job')
                with self.assertRaisesRegex(RuntimeError, 'runtime'):
                    self.pool.retry_cleanup(grant.allocation_id, timeout=1)
                close.assert_not_called()
                runtime.identity = original
                report = self.pool.retry_cleanup(grant.allocation_id, timeout=1)
                close.assert_called_once_with(timeout=1)
            self.assertEqual(report['status'], 'unknown')
            self.assertEqual(grant.describe()['status'], 'bound')
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], before['allocated_bytes'])
            self.assertEqual(self.pool.snapshot()['allocations'], before['allocations'])
            self.assertEqual(runtime.async_actors, [])
            self.assertEqual(runtime.engines, [])

    def test_lost_caller_reference_keeps_the_actual_owner_available_for_cleanup(self):
        with RecoveryRuntime([]) as runtime:
            owner = self.owner()
            ready = owner.start()
            allocation = owner._service_allocation.allocation_id
            original = weakref.ref(owner)
            try:
                del owner
                gc.collect()
                self.assertIsNotNone(original())
                self.assertEqual(self.pool.close()['status'], 'pending')
                report = self.pool.retry_cleanup(allocation, timeout=3)
                self.assertEqual(report['status'], 'closed', report)
                self.assertEqual(report['model_owner']['model_ready']['engine_uid'], ready['engine_uid'])
                self.assertEqual(report['service_allocation']['status'], 'released')
                self.assertEqual(len(runtime.async_actors), 1)
                self.assertEqual(self.pool.close()['status'], 'closed')
            finally:
                if original() is not None:
                    original().close(timeout=3)

    def test_unknown_ids_and_snapshot_objects_never_call_the_owner(self):
        owner = self.owner()
        other = type(self.pool)(self.pool.total_bytes)
        foreign = other.allocate(self.shape)
        before = self.pool.snapshot()
        try:
            with patch.object(owner, 'close', wraps=owner.close) as close, \
                    patch.object(owner, 'take_unconsumed_results', wraps=owner.take_unconsumed_results) as take:
                for identity in (None, True, '', 'not-registered', foreign.allocation_id,
                                 before['grants'][0], before):
                    for action in (self.pool.retry_cleanup, self.pool.take_unconsumed_results):
                        with self.subTest(identity=identity, action=action.__name__), self.assertRaises(ValueError):
                            action(identity)
                close.assert_not_called()
                take.assert_not_called()
            self.assertEqual(self.pool.snapshot(), before)
        finally:
            foreign.cancel()
            owner.close(timeout=1)

    def test_unbound_allocation_requires_its_original_cancel_operation(self):
        grant = self.pool.allocate(self.shape)
        before = self.pool.snapshot()
        try:
            for action in (self.pool.retry_cleanup, self.pool.take_unconsumed_results):
                with self.subTest(action=action.__name__), self.assertRaisesRegex(RuntimeError, 'bound'):
                    action(grant.allocation_id)
            self.assertEqual(self.pool.snapshot(), before)
        finally:
            grant.cancel()

    def test_other_thread_cannot_close_or_transfer_the_owner_results(self):
        owner = self.owner()
        allocation = owner._service_allocation.allocation_id
        before = self.pool.snapshot()
        try:
            with patch.object(owner, 'close', wraps=owner.close) as close, \
                    patch.object(owner, 'take_unconsumed_results', wraps=owner.take_unconsumed_results) as take, \
                    ThreadPoolExecutor(max_workers=1) as worker:
                for action in (self.pool.retry_cleanup, self.pool.take_unconsumed_results):
                    with self.subTest(action=action.__name__), self.assertRaisesRegex(RuntimeError, 'control thread'):
                        worker.submit(action, allocation).result(3)
                close.assert_not_called()
                take.assert_not_called()
            self.assertEqual(self.pool.snapshot(), before)
        finally:
            owner.close(timeout=1)

    def test_other_process_cannot_close_or_transfer_the_owner_results(self):
        owner = self.owner()
        allocation = owner._service_allocation.allocation_id
        before = self.pool.snapshot()
        try:
            with patch.object(owner, 'close', wraps=owner.close) as close, \
                    patch.object(owner, 'take_unconsumed_results', wraps=owner.take_unconsumed_results) as take, \
                    patch('os.getpid', return_value=os.getpid() + 1):
                for action in (self.pool.retry_cleanup, self.pool.take_unconsumed_results):
                    with self.subTest(action=action.__name__), self.assertRaisesRegex(RuntimeError, 'process'):
                        action(allocation)
                close.assert_not_called()
                take.assert_not_called()
            self.assertEqual(self.pool.snapshot(), before)
        finally:
            owner.close(timeout=1)

    def test_invalid_cleanup_timeout_does_not_begin_owner_cleanup(self):
        owner = self.owner()
        allocation = owner._service_allocation.allocation_id
        try:
            with patch.object(owner, 'close', wraps=owner.close) as close:
                for value in (True, 0, -1, '1', float('inf'), float('nan'), 10**1000):
                    with self.subTest(timeout=value), self.assertRaises(ValueError):
                        self.pool.retry_cleanup(allocation, timeout=value)
                close.assert_not_called()
            self.assertFalse(owner._closing)
        finally:
            owner.close(timeout=1)

    def test_shutdown_runtime_never_triggers_close_or_automatic_initialization(self):
        with RecoveryRuntime([]) as runtime:
            owner = self.owner()
            owner.start()
            allocation = owner._service_allocation.allocation_id
            before = self.pool.snapshot()
            runtime.ray.init = Mock(side_effect=AssertionError('replacement runtime was initialized'))
            try:
                with patch.object(runtime.ray, 'is_initialized', return_value=False), \
                        patch.object(runtime.ray, 'get_runtime_context',
                            side_effect=AssertionError('runtime context read while disconnected')) as context, \
                        patch.object(owner, 'close', wraps=owner.close) as close, \
                        patch.object(owner, 'take_unconsumed_results', wraps=owner.take_unconsumed_results) as take:
                    with self.assertRaisesRegex(RuntimeError, 'runtime'):
                        self.pool.retry_cleanup(allocation)
                    self.assertEqual(self.pool.take_unconsumed_results(allocation), [])
                    close.assert_not_called()
                    take.assert_called_once_with()
                    runtime.ray.init.assert_not_called()
                    context.assert_not_called()
                self.assertEqual(self.pool.snapshot(), before)
            finally:
                owner.close(timeout=3)

    def test_replacement_module_or_unknown_runtime_observation_keeps_the_grant(self):
        with RecoveryRuntime([]) as runtime:
            owner = self.owner()
            owner.start()
            allocation = owner._service_allocation.allocation_id
            before = self.pool.snapshot()
            try:
                with patch.object(owner, 'close', wraps=owner.close) as close:
                    with patch.dict(sys.modules, {'ray': object()}):
                        with self.assertRaisesRegex(RuntimeError, 'runtime'):
                            self.pool.retry_cleanup(allocation, timeout=1)
                    for observation in (None, Mock(side_effect=RuntimeError('runtime unavailable'))):
                        with patch.object(runtime.ray, 'is_initialized', observation):
                            with self.assertRaisesRegex(RuntimeError, 'runtime'):
                                self.pool.retry_cleanup(allocation, timeout=1)
                    close.assert_not_called()
                self.assertEqual(self.pool.snapshot(), before)
            finally:
                owner.close(timeout=3)

    def test_missing_initial_runtime_identity_is_not_filled_by_a_later_connection(self):
        with service.fixtures.ThreadedRuntime([]) as runtime:
            owner = self.owner()
            owner.start()
            allocation = owner._service_allocation.allocation_id
            before = self.pool.snapshot()
            try:
                context = SimpleNamespace(get_job_id=lambda: 'later-job', get_node_id=lambda: 'later-node')
                with patch.object(runtime.ray, 'get_runtime_context', return_value=context, create=True):
                    owner.start()
                    with patch.object(owner, 'close', wraps=owner.close) as close:
                        with self.assertRaisesRegex(RuntimeError, 'runtime'):
                            self.pool.retry_cleanup(allocation, timeout=1)
                        close.assert_not_called()
                self.assertIsNone(owner._service_allocation._runtime_identity)
                self.assertEqual(self.pool.snapshot(), before)
            finally:
                owner.close(timeout=3)

    def test_failed_initial_identity_observation_preserves_normal_start_and_close(self):
        bad_node = SimpleNamespace(get_job_id=lambda: 'job', get_node_id=lambda: None)
        missing_node = SimpleNamespace(get_job_id=lambda: 'job')
        for observation in (Mock(side_effect=RuntimeError('identity read failed')),
                            Mock(return_value=bad_node), Mock(return_value=missing_node)):
            with self.subTest(observation=observation), RecoveryRuntime([]) as runtime:
                owner = self.owner()
                allocation = owner._service_allocation.allocation_id
                try:
                    with patch.object(runtime.ray, 'get_runtime_context', observation):
                        self.assertTrue(owner.start()['engine_uid'])
                    self.assertIsNone(owner._service_allocation._runtime_identity)
                    with patch.object(owner, 'close', wraps=owner.close) as close:
                        with self.assertRaisesRegex(RuntimeError, 'runtime'):
                            self.pool.retry_cleanup(allocation, timeout=1)
                        close.assert_not_called()
                finally:
                    self.assertEqual(owner.close(timeout=3)['status'], 'closed')

    def test_same_module_reinitialized_with_other_job_or_node_cannot_close_the_owner(self):
        with RecoveryRuntime([]) as runtime:
            owner = self.owner()
            owner.start()
            allocation = owner._service_allocation.allocation_id
            identity = dict(runtime.identity)
            try:
                with patch.object(owner, 'close', wraps=owner.close) as close:
                    for name in ('job_id', 'node_id'):
                        runtime.identity = dict(identity, **{name: 'replacement-' + name})
                        with self.subTest(identity=name), self.assertRaisesRegex(RuntimeError, 'runtime'):
                            self.pool.retry_cleanup(allocation, timeout=1)
                        with self.assertRaisesRegex(ValueError, 'runtime'):
                            owner.start()
                        self.assertEqual(owner._service_allocation._runtime_identity, identity)
                    close.assert_not_called()
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
            finally:
                runtime.identity = identity
                owner.close(timeout=3)

    def test_pending_snapshot_stays_owned_until_the_original_read_returns(self):
        def check():
            owner = self._owner_and_query()
            allocation = owner._service_allocation.allocation_id
            original = owner._feedback.future
            used = self.pool.snapshot()['allocated_bytes']
            first = self.pool.retry_cleanup(allocation, timeout=3)
            self.assertEqual(first['status'], 'unknown')
            self.assertFalse(original.done())
            ray = owner._actors.ray
            with patch.object(ray, 'is_initialized', return_value=False), \
                    patch.object(ray, 'init', side_effect=AssertionError('new runtime'), create=True) as initialize, \
                    patch.object(ray, 'wait', side_effect=AssertionError('new wait')) as wait, \
                    patch.object(ray, 'get', side_effect=AssertionError('new get')) as get:
                self.assertEqual(self.pool.take_unconsumed_results(allocation), [])
                initialize.assert_not_called()
                wait.assert_not_called()
                get.assert_not_called()
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], used)
            self._release.set()
            original.result(3)
            last = self.pool.retry_cleanup(allocation, timeout=3)
            self.assertEqual(last['status'], 'closed', last)
            self.assertEqual(last['service_allocation']['status'], 'released')
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)
        with patch.object(service.fixtures, 'ThreadedRuntime', RecoveryRuntime):
            self._run(check)

    def test_pending_data_RPC_cannot_be_replaced_by_a_new_owner(self):
        entered, release = threading.Event(), threading.Event()
        original = service.actors.AsyncCoreModelActor.try_submit_core
        def held(actor, *args):
            entered.set()
            if not release.wait(5):
                raise TimeoutError('test did not release the data handoff')
            return original(actor, *args)
        rows = self.rows()
        with RecoveryRuntime(rows), \
                patch.object(service.actors.AsyncCoreModelActor, 'try_submit_core', held):
            owner = self.owner()
            allocation = owner._service_allocation.allocation_id
            stream = owner.evaluate(service.source_fixture.Source(rows), query_id='pending-data')
            try:
                stream._fill()
                def issued():
                    stream.session.advance(1)
                    return entered.is_set()
                self.until(issued)
                used = self.pool.snapshot()['allocated_bytes']
                first = self.pool.retry_cleanup(allocation, timeout=.02)
                self.assertEqual(first['status'], 'unknown')
                self.assertGreater(first['model_owner']['data_rpc_pending'], 0)
                self.assertEqual(self.pool.take_unconsumed_results(allocation), [])
                self.assertEqual(self.pool.snapshot()['allocated_bytes'], used)
                with self.assertRaisesRegex(ValueError, 'payload'):
                    self.owner()
                release.set()
                final = self.pool.retry_cleanup(allocation, timeout=3)
                self.assertEqual(final['status'], 'closed', final)
                taken = self.pool.take_unconsumed_results(allocation)
                self.assertEqual(len(taken), 1)
                self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)
            finally:
                release.set()
                owner.close(timeout=3)

    def _retained_receipt(self, text, status, *, runtime_stopped=False):
        rows = self.rows(text)
        with RecoveryRuntime(rows) as runtime:
            owner = self.owner()
            allocation = owner._service_allocation.allocation_id
            stream = owner.evaluate(service.source_fixture.Source(rows), query_id='late-result')
            try:
                stream._fill()
                def admitted():
                    stream.session.advance(1)
                    return runtime.async_actors[0].instance.engine.snapshot()['accepted_requests'] == 1
                self.until(admitted)
                report = self.pool.retry_cleanup(allocation, timeout=3)
                self.assertEqual(report['status'], 'closed', report)
                self.assertEqual(report['service_allocation']['status'], 'bound')
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
                with ExitStack() as blocked:
                    operations = []
                    if runtime_stopped:
                        operations.append(blocked.enter_context(patch.object(runtime.ray, 'is_initialized',
                            return_value=False)))
                        for name in ('init', 'wait', 'get', 'remote', 'kill', 'cancel', 'get_runtime_context'):
                            operations.append(blocked.enter_context(patch.object(runtime.ray, name,
                                side_effect=AssertionError('local transfer called Ray.' + name), create=True)))
                    before = list(runtime.events)
                    taken = self.pool.take_unconsumed_results(allocation)
                    if runtime_stopped:
                        self.assertEqual(runtime.events, before)
                        for operation in operations:
                            operation.assert_not_called()
                self.assertEqual([(r['status'], r['prompt_tokens']) for r in taken], [(status, 11)])
                self.assertEqual(self.pool.snapshot()['allocations'], 0)
                self.assertEqual(owner.take_unconsumed_results(), [])
                for action in (self.pool.retry_cleanup, self.pool.take_unconsumed_results):
                    with self.assertRaisesRegex(ValueError, 'registered'):
                        action(allocation)
            finally:
                owner.close(timeout=3)

    def test_successful_close_receipt_is_explicitly_transferred_once(self):
        self._retained_receipt('hold', 'succeeded')

    def test_unknown_close_receipt_keeps_its_original_outcome_when_transferred(self):
        self._retained_receipt('hold-unknown', 'unknown')

    def test_confirmed_close_results_can_transfer_after_Ray_shutdown_without_a_new_connection(self):
        self._retained_receipt('hold', 'succeeded', runtime_stopped=True)

    def test_confirmed_producer_stop_preserves_unknown_results_after_Ray_shutdown(self):
        self._retained_receipt('hold-unknown', 'unknown', runtime_stopped=True)

    def test_successful_recovery_does_not_add_a_retired_owner_history(self):
        initial_fields = set(vars(self.pool))
        previous = []
        for _ in range(12):
            owner = self.owner()
            allocation = owner._service_allocation.allocation_id
            previous.append(weakref.ref(owner))
            report = self.pool.retry_cleanup(allocation)
            self.assertEqual(report['service_allocation']['status'], 'released')
            self.assertEqual(self.pool.snapshot()['allocations'], 0)
            del owner
        gc.collect()
        self.assertEqual(set(vars(self.pool)), initial_fields)
        self.assertTrue(all(ref() is None for ref in previous))


if __name__ == '__main__':
    unittest.main()
