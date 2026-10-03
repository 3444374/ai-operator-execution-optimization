"""Prepared component only: no model, database server, SDK or network calls."""
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import replace
import multiprocessing
import os
from pathlib import Path
import pickle
import tempfile
import time
import unittest
from unittest.mock import patch

from src.experiments.attempt_ledger import AttemptBudget, BudgetError, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.mapped_request_budget import (
    MappedUnitClient, MappedUnitOwner, _HEADER, _DIRTY,
)


A, B = 'a' * 64, 'b' * 64


def spend(descriptor):
    client = MappedUnitClient(descriptor)
    values = []
    try:
        for _ in range(descriptor.requests):
            try:
                values.append(client.reserve(A))
            except BudgetExhausted:
                break
        return values
    finally:
        client.close()


def crash_call(descriptor, dirty):
    client = MappedUnitClient(descriptor)
    if dirty:
        with client._locked():
            client._write_header(client._header()[4], _DIRTY)
            os._exit(7)
    client.reserve(A)
    os._exit(9)


def owner_process(path, connection):
    ledger = CellBudgetLedger(path / 'ledger', AttemptBudget('mapped.fixture', 8))
    owner = MappedUnitOwner.prepare(ledger, 'cell', [A] * 8, path / 'owned')
    connection.send(owner.descriptor)
    connection.recv()
    os._exit(7)


def inherited_handles(owner, connection):
    connection.send((owner._lease_fd is None, owner.client._mapping is None))
    connection.recv()


class MappedBudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.owner = None
        self.addCleanup(self.close_owner)

    def close_owner(self):
        if self.owner is not None:
            try:
                self.owner.close()
            except BudgetError:
                pass

    def create(self, digests, *, limit=None):
        self.budget = AttemptBudget('mapped.fixture', limit or len(digests))
        self.ledger = CellBudgetLedger.create(self.root / 'ledger', self.budget,
                                             deadline_utc=time.time() + 60)
        self.ledger.reserve_unit('cell', len(digests))
        self.owner = MappedUnitOwner.prepare(self.ledger, 'cell', digests, self.root / 'owned')
        return self.owner.client

    def test_duplicate_multiplicity_and_attempt_order(self):
        client = self.create([A, A, B])
        self.assertEqual([client.reserve(A), client.reserve(A)], [1, 2])
        with self.assertRaises(BudgetExhausted):
            client.reserve(A)
        self.assertEqual(client.reserve(B), 3)
        self.assertEqual(client.remaining, 0)
        self.assertEqual(self.ledger.snapshot()['allocated_requests'], 3)

    def test_thread_and_multiple_client_calls_share_one_prefix(self):
        client = self.create([A] * 37)
        peer = MappedUnitClient(self.owner.descriptor)
        self.addCleanup(peer.close)
        def use(i):
            try:
                return (client if i % 2 else peer).reserve(A)
            except BudgetExhausted:
                return None
        with ThreadPoolExecutor(8) as pool:
            outcomes = list(pool.map(use, range(60)))
        self.assertEqual(sorted(x for x in outcomes if x is not None), list(range(1, 38)))

    def test_spawned_callers_cannot_overspend_or_reclaim(self):
        client = self.create([A] * 37)
        with ProcessPoolExecutor(3, mp_context=multiprocessing.get_context('spawn')) as pool:
            values = sum(list(pool.map(spend, [self.owner.descriptor] * 3)), [])
        self.assertEqual(sorted(values), list(range(1, 38)))
        self.assertEqual(client.attempts, 37)
        for claim in (self.ledger.claim_unit, self.ledger.claim_shared_unit):
            with self.assertRaises(BudgetError):
                claim('cell')

    def test_prepaid_total_remains_charged_after_partial_use_and_close(self):
        client = self.create([A] * 8, limit=10)
        peer = MappedUnitClient(self.owner.descriptor)
        self.addCleanup(peer.close)
        self.assertEqual(client.reserve(A), 1)
        self.owner.close()
        with self.assertRaises(BudgetExhausted):
            peer.reserve(A)
        state = peer.snapshot()
        self.assertEqual((state['attempts'], state['remaining'], state['closed'], state['owner_alive']),
                         (1, 7, True, False))
        self.assertEqual(self.ledger.reserve_unit('next', 2).first_attempt, 9)
        with self.assertRaises(BudgetExhausted):
            self.ledger.reserve_unit('more', 1)

    def test_reservation_has_no_sqlite_or_fsync(self):
        client = self.create([A] * 8)
        with patch('sqlite3.connect', side_effect=AssertionError('send-time SQLite')), \
             patch('os.fsync', side_effect=AssertionError('send-time fsync')):
            self.assertEqual([client.reserve(A) for _ in range(8)], list(range(1, 9)))

    def test_unknown_or_malformed_digest_never_grants(self):
        client = self.create([A] * 2)
        for digest in ('bad', B, None):
            with self.assertRaises(BudgetError):
                client.reserve(digest)
        self.assertEqual(client.attempts, 0)
        self.assertEqual(self.ledger.snapshot()['allocated_requests'], 2)

    def test_deadline_rejects_without_new_grant(self):
        client = self.create([A] * 2)
        with patch('time.monotonic', return_value=self.owner.descriptor.deadline_monotonic + 1):
            with self.assertRaises(BudgetExhausted):
                client.reserve(A)
        self.assertEqual(client.attempts, 0)

    def test_descriptor_identity_rejects_wrong_nonce_plan_and_file(self):
        self.create([A] * 2)
        descriptor = self.owner.descriptor
        for changed in (replace(descriptor, nonce=b'x' * 16),
                        replace(descriptor, plan_sha256='0' * 64),
                        replace(descriptor, state_identity=(0, 0))):
            with self.assertRaises(BudgetError):
                MappedUnitClient(changed)
        self.assertEqual(self.owner.client.attempts, 0)

    def test_missing_and_symlink_files_reject_before_grant(self):
        self.create([A] * 2)
        other = self.root / 'other'; other.mkdir()
        descriptor = replace(self.owner.descriptor, directory=str(other))
        with self.assertRaises(BudgetError):
            MappedUnitClient(descriptor)
        (other / 'state').symlink_to(self.root / 'owned/state')
        with self.assertRaises(BudgetError):
            MappedUnitClient(descriptor)
        self.assertEqual(self.owner.client.attempts, 0)

    def test_live_handles_cannot_be_pickled(self):
        client = self.create([A])
        for value in (client, self.owner):
            with self.assertRaises(TypeError):
                pickle.dumps(value)
        self.assertEqual(pickle.loads(pickle.dumps(self.owner.descriptor)), self.owner.descriptor)

    def test_closed_handle_and_foreign_process_reject(self):
        client = self.create([A])
        with patch('os.getpid', return_value=-1), self.assertRaises(BudgetError):
            client.reserve(A)
        peer = MappedUnitClient(self.owner.descriptor)
        peer.close()
        with self.assertRaises(BudgetError):
            peer.reserve(A)

    def test_wrong_prepared_total_burns_claim_instead_of_rebuilding(self):
        self.budget = AttemptBudget('mapped.fixture', 4)
        self.ledger = CellBudgetLedger.create(self.root / 'ledger', self.budget, deadline_utc=time.time() + 60)
        self.ledger.reserve_unit('cell', 4)
        with self.assertRaises(BudgetError):
            MappedUnitOwner.prepare(self.ledger, 'cell', [A], self.root / 'owned')
        with self.assertRaises(BudgetError):
            self.ledger.claim_unit('cell')
        self.assertEqual(self.ledger.snapshot()['allocated_requests'], 4)

    def test_duplicate_output_directory_never_resets_claim(self):
        self.create([A] * 2)
        with self.assertRaises(FileExistsError):
            MappedUnitOwner.prepare(self.ledger, 'cell', [A] * 2, self.root / 'owned')
        self.assertEqual(self.owner.client.reserve(A), 1)

    def test_owner_process_death_blocks_existing_and_reopened_callers(self):
        self.budget = AttemptBudget('mapped.fixture', 8)
        self.ledger = CellBudgetLedger.create(self.root / 'ledger', self.budget, deadline_utc=time.time() + 60)
        self.ledger.reserve_unit('cell', 8)
        context = multiprocessing.get_context('spawn')
        parent, child = context.Pipe()
        process = context.Process(target=owner_process, args=(self.root, child))
        process.start()
        self.addCleanup(parent.close)
        self.addCleanup(child.close)
        try:
            self.assertTrue(parent.poll(5))
            descriptor = parent.recv()
            client = MappedUnitClient(descriptor)
            self.addCleanup(client.close)
            self.assertEqual(client.reserve(A), 1)
            parent.send('exit')
            process.join(5)
            self.assertEqual(process.exitcode, 7)
            with self.assertRaises(BudgetExhausted):
                client.reserve(A)
            peer = MappedUnitClient(descriptor)
            self.addCleanup(peer.close)
            with self.assertRaises(BudgetExhausted):
                peer.reserve(A)
            self.assertEqual(self.ledger.snapshot()['allocated_requests'], 8)
        finally:
            if process.is_alive():
                process.kill()
            process.join(5)

    def test_caller_death_does_not_refund_a_granted_attempt(self):
        client = self.create([A] * 4)
        process = multiprocessing.get_context('spawn').Process(target=crash_call,
                    args=(self.owner.descriptor, False))
        process.start(); process.join(5)
        self.assertEqual(process.exitcode, 9)
        self.assertEqual(client.reserve(A), 2)
        self.assertEqual(self.ledger.snapshot()['allocated_requests'], 4)

    def test_death_during_registration_stops_other_callers(self):
        client = self.create([A] * 4)
        process = multiprocessing.get_context('spawn').Process(target=crash_call,
                    args=(self.owner.descriptor, True))
        process.start(); process.join(5)
        self.assertEqual(process.exitcode, 7)
        with self.assertRaisesRegex(BudgetError, 'unknown'):
            client.reserve(A)
        with self.assertRaises(BudgetError):
            MappedUnitClient(self.owner.descriptor)
        self.assertEqual(self.ledger.snapshot()['allocated_requests'], 4)

    def test_python_fork_drops_inherited_owner_and_client_handles(self):
        self.create([A] * 4)
        context = multiprocessing.get_context('fork')
        parent, child = context.Pipe()
        process = context.Process(target=inherited_handles, args=(self.owner, child))
        process.start()
        try:
            self.assertTrue(parent.poll(5))
            self.assertEqual(parent.recv(), (True, True))
            peer = MappedUnitClient(self.owner.descriptor)
            self.addCleanup(peer.close)
            self.owner.close()
            self.assertFalse(peer.snapshot()['owner_alive'])
            parent.send('done')
            process.join(5)
            self.assertEqual(process.exitcode, 0)
        finally:
            if process.is_alive():process.kill()
            process.join(5)
            parent.close(); child.close()


if __name__ == '__main__':
    unittest.main()
