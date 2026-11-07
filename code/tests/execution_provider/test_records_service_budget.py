"""Exclusive service-process grants for declared payload envelopes, not RAM."""
import copy
import os
import pickle
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity


class ServiceBudgetTests(unittest.TestCase):
    def pool(self, total=8 * 1024**2):
        from src.execution_provider.adapters.records_service_budget import RecordsPayloadPool
        return RecordsPayloadPool(total)

    def shape(self):
        return CoreRecordsCapacity(item_input_bytes=8192, item_result_bytes=8192)

    def test_preview_does_not_allocate_and_all_declared_roles_are_charged(self):
        pool = self.pool()
        first, second = pool.preview(self.shape()), pool.preview(self.shape())
        self.assertEqual(first, second)
        self.assertGreater(first.capacity.budget.active_requests, 8)
        parts = dict(first.components)
        self.assertEqual(first.bytes, sum(parts.values()))
        self.assertEqual(parts['ipc_parent_input'], parts['ipc_child_input'])
        self.assertEqual(parts['ipc_parent_result'], parts['ipc_child_result'])
        self.assertEqual(parts['ipc_parent_control'], parts['ipc_child_control'])
        self.assertGreater(first.bytes, first.capacity.budget.input_bytes + first.capacity.budget.result_bytes)
        self.assertEqual(pool.snapshot()['allocated_bytes'], 0)

    def test_full_allocation_cannot_be_spent_twice_and_unbound_cancel_returns_it(self):
        pool = self.pool()
        grant = pool.allocate(self.shape())
        used = grant.plan.bytes
        self.assertEqual(pool.snapshot()['allocated_bytes'], used)
        with self.assertRaisesRegex(ValueError, 'payload'):
            pool.allocate(self.shape())
        self.assertTrue(grant.cancel())
        self.assertFalse(grant.cancel())
        self.assertEqual(pool.snapshot()['allocated_bytes'], 0)
        replacement = pool.allocate(self.shape())
        self.assertNotEqual(replacement.allocation_id, grant.allocation_id)
        self.assertEqual(replacement.plan, grant.plan)
        replacement.cancel()

    def test_independent_instances_receive_disjoint_slices_of_one_pool(self):
        pool = self.pool()
        first = pool.allocate(self.shape(), max_bytes=pool.total_bytes // 2)
        second = pool.allocate(self.shape(), max_bytes=pool.total_bytes // 2)
        self.assertEqual(pool.snapshot()['allocations'], 2)
        self.assertEqual(pool.snapshot()['allocated_bytes'], first.plan.bytes + second.plan.bytes)
        self.assertLessEqual(pool.snapshot()['allocated_bytes'], pool.total_bytes)
        before = second.describe()
        first.cancel()
        self.assertEqual(second.describe(), before)
        self.assertEqual(pool.snapshot()['allocated_bytes'], second.plan.bytes)
        second.cancel()

    def test_concurrent_allocators_cannot_each_receive_the_whole_pool(self):
        pool = self.pool()
        def allocate():
            try:
                return pool.allocate(self.shape())
            except ValueError:
                return None
        with ThreadPoolExecutor(max_workers=8) as workers:
            grants = [g for g in workers.map(lambda _: allocate(), range(16)) if g is not None]
        self.assertEqual(len(grants), 1)
        self.assertEqual(pool.snapshot()['allocations'], 1)
        grants[0].cancel()

    def test_forged_capability_and_serialized_copies_cannot_acquire_ownership(self):
        pool = self.pool()
        grant = pool.allocate(self.shape())
        forged = object.__new__(type(grant))
        forged.__dict__.update(grant.__dict__)
        with self.assertRaisesRegex(ValueError, 'unregistered'):
            forged.cancel()
        for value in (pool, grant):
            for action in (copy.copy, copy.deepcopy, pickle.dumps):
                with self.subTest(value=type(value).__name__, action=action.__name__), self.assertRaises(TypeError):
                    action(value)
        grant.cancel()

    def test_forked_process_identity_cannot_allocate_or_return_a_parent_grant(self):
        pool = self.pool()
        grant = pool.allocate(self.shape())
        parent = os.getpid()
        with patch('os.getpid', return_value=parent + 1):
            with self.assertRaisesRegex(RuntimeError, 'process'):
                pool.allocate(self.shape())
            with self.assertRaisesRegex(RuntimeError, 'process'):
                grant.cancel()
        self.assertEqual(pool.snapshot()['allocations'], 1)
        grant.cancel()

    def test_pool_shutdown_keeps_existing_grants_until_their_owners_return_them(self):
        pool = self.pool()
        grant = pool.allocate(self.shape())
        self.assertEqual(pool.close()['status'], 'pending')
        self.assertEqual(pool.snapshot()['allocations'], 1)
        with self.assertRaisesRegex(RuntimeError, 'closing'):
            pool.allocate(self.shape())
        grant.cancel()
        self.assertEqual(pool.close()['status'], 'closed')

    def test_missing_invalid_or_insufficient_bytes_never_become_unlimited(self):
        for bad in (None, True, 0, -1, 2.5, float('inf')):
            with self.subTest(total=bad), self.assertRaises(ValueError):
                self.pool(bad)
        with self.assertRaisesRegex(ValueError, 'payload'):
            self.pool(1).allocate(self.shape())
        pool = self.pool()
        for bad in (True, 0, -1, 2.5, float('inf')):
            with self.subTest(maximum=bad), self.assertRaises(ValueError):
                pool.allocate(self.shape(), max_bytes=bad)
        self.assertEqual(pool.snapshot()['allocated_bytes'], 0)

    def test_public_plan_and_allocation_identity_cannot_change_live_accounting(self):
        pool = self.pool()
        grant = pool.allocate(self.shape())
        used = pool.snapshot()['allocated_bytes']
        for name, value in (('plan', None), ('allocation_id', 'another-grant')):
            with self.subTest(name=name), self.assertRaises(AttributeError):
                setattr(grant, name, value)
        observed = pool.snapshot()
        observed['grants'][0]['bytes'] = 0
        self.assertEqual(pool.snapshot()['allocated_bytes'], used)
        grant.cancel()
