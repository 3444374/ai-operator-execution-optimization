"""A stopped sampler cannot return a grant while its actual snapshot RPC is pending."""
from concurrent.futures import Future
import threading
import unittest
from unittest.mock import patch

from tests.execution_provider import test_records_service_owner as service


class ServiceSnapshotRetentionTests(unittest.TestCase):
    setUp = service.ServiceOwnerTests.setUp
    owner = service.ServiceOwnerTests.owner
    rows = service.ServiceOwnerTests.rows

    def _owner_and_query(self, **options):
        owner = self.owner(profile=dict(mode='async', admission=dict(
            kind='resource_feedback', poll_interval_s=.01)), **options)
        self._owners.append(owner)
        ready = owner.start()
        # Virtual-backend eligibility only; this is not a memory observation.
        ready.update(batch_mode='independent_final_results', closed=False,
            resource_owner=dict(schema='semloom.model_resource_owner.v1', engine_uid=ready['engine_uid'],
                request_memory_owner='vllm', native_feedback_clock='private_child_and_model_actor.monotonic'))
        stream = owner.evaluate(service.source_fixture.Source(self.rows()), query_id='pending-control')
        self.assertTrue(self._entered.wait(3), 'the owned CPU snapshot RPC did not enter')
        self.assertEqual(len(list(stream)), 1)
        return owner

    def _run(self, check, *, raised=False):
        self._entered, self._release = threading.Event(), threading.Event()
        self._owners = []
        actual = service.actors.AsyncCoreModelActor.snapshot
        def held(actor, *, observe_resources=False):
            self._entered.set()
            if not self._release.wait(8):
                raise TimeoutError('CPU test did not release the retained snapshot')
            if raised:
                raise RuntimeError('intentional CPU snapshot return error')
            return actual(actor, observe_resources=observe_resources)
        with service.fixtures.ThreadedRuntime(self.rows()), \
                patch.object(service.actors.AsyncCoreModelActor, 'snapshot', held):
            try:
                check()
            finally:
                self._release.set()
                for owner in self._owners:
                    feedback = owner._feedback
                    future = None if feedback is None else feedback.future
                    if future is not None:
                        try:
                            future.result(3)
                        except Exception:
                            pass
                    owner.close(timeout=3)

    def test_snapshot_return_proof_precedes_second_owner_and_uses_live_sampler_state(self):
        def check():
            owner = self._owner_and_query()
            feedback, allocation = owner._feedback, owner._service_allocation
            original = feedback.future
            cached = owner.last_query_report['admission_feedback']
            self.assertFalse(cached['snapshot_rpc_terminal_evidence_complete'])
            used = self.pool.snapshot()['allocated_bytes']
            report = owner.close(timeout=3)
            self.assertFalse(feedback.provider.is_running)
            self.assertFalse(original.done())
            self.assertEqual(owner._actors._data_rpc_progress().pending, 0)
            self.assertEqual(report['status'], 'unknown')
            self.assertEqual(allocation.describe()['status'], 'bound')
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], used)
            with self.assertRaisesRegex(ValueError, 'payload'):
                second = self.owner()
                self._owners.append(second)
            self.assertFalse(allocation.settle(owner))
            self._release.set()
            original.result(3)
            closed = owner.close(timeout=3)
            self.assertEqual(closed['status'], 'closed', closed)
            self.assertTrue(closed['feedback_close']['snapshot_rpc_terminal_evidence_complete'])
            self.assertFalse(cached['snapshot_rpc_terminal_evidence_complete'])
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)
            second = self.owner()
            self._owners.append(second)
            self.assertNotEqual(second._service_allocation.allocation_id, allocation.allocation_id)
        self._run(check)

    def test_known_snapshot_exception_is_terminal_evidence_after_the_original_RPC_returns(self):
        def check():
            owner = self._owner_and_query()
            original = owner._feedback.future
            first = owner.close(timeout=3)
            self.assertEqual(first['status'], 'unknown')
            self.assertEqual(self.pool.snapshot()['allocations'], 1)
            self._release.set()
            with self.assertRaisesRegex(RuntimeError, 'intentional CPU snapshot'):
                original.result(3)
            closed = owner.close(timeout=3)
            self.assertEqual(closed['status'], 'closed', closed)
            proof = closed['feedback_close']
            self.assertTrue(proof['snapshot_rpc_terminal_evidence_complete'])
            self.assertEqual([r['rpc_terminal_status'] for r in proof['snapshot_rpc_receipts']], ['raised'])
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)
        self._run(check, raised=True)

    def test_cancelled_local_future_does_not_return_bytes_or_replace_the_remote_read(self):
        def check():
            owner = self._owner_and_query()
            feedback = owner._feedback
            feedback.stop()
            original, ref = feedback.future, feedback.ref
            cancelled = Future()
            cancelled.cancel()
            with feedback._snapshot_lock:
                feedback.future = cancelled
            try:
                report = owner.close(timeout=3)
                self.assertEqual(report['status'], 'unknown')
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
                self.assertIs(feedback.ref, ref)
                self.assertIs(feedback.future, cancelled)
                self.assertEqual(feedback.requests, 1)
                proof = report['feedback_close']
                self.assertFalse(proof['snapshot_rpc_terminal_evidence_complete'])
                self.assertTrue(proof['snapshot_rpc_pending']['client_future_cancelled'])
                self.assertFalse(original.done())
            finally:
                self._release.set()
                original.result(3)
                # Return the same original retained RPC Future to this CPU
                # fixture; no replacement read or production recovery is added.
                with feedback._snapshot_lock:
                    feedback.future = original
            self.assertEqual(owner.close(timeout=3)['status'], 'closed')
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)
        self._run(check)

    def test_other_owner_can_return_only_its_own_half_while_the_first_RPC_is_pending(self):
        def check():
            first = self._owner_and_query(service_allocation_bytes=self.pool.total_bytes // 2)
            second = self.owner(service_allocation_bytes=self.pool.total_bytes // 2)
            self._owners.append(second)
            retained = first._service_allocation.plan.bytes
            self.assertEqual(first.close(timeout=3)['status'], 'unknown')
            self.assertEqual(self.pool.snapshot()['allocations'], 2)
            self.assertEqual(second.close(timeout=3)['status'], 'closed')
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], retained)
            third = self.owner(service_allocation_bytes=self.pool.total_bytes // 2)
            self._owners.append(third)
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], retained + third._service_allocation.plan.bytes)
            with self.assertRaisesRegex(ValueError, 'payload'):
                fourth = self.owner(service_allocation_bytes=self.pool.total_bytes // 2)
                self._owners.append(fourth)
            self._release.set()
            first._feedback.future.result(3)
            self.assertEqual(first.close(timeout=3)['status'], 'closed')
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], third._service_allocation.plan.bytes)
        self._run(check)


if __name__ == '__main__':
    unittest.main()
