"""Durable accounting stays serialized and does not block the transport loop."""
import asyncio
import hashlib
import tempfile
from pathlib import Path
import threading
import time
import unittest

from src.experiments.async_request_guard import ThreadedRequestGuard
from src.experiments.attempt_ledger import AttemptBudget, BudgetError
from src.experiments.cell_budget import CellBudgetLedger
from tests.execution_provider.test_ray_map_transport import task


class ThreadedGuardTests(unittest.IsolatedAsyncioTestCase):
    def ledger(self, path, requests):
        ledger = CellBudgetLedger.create(path, AttemptBudget('fixture.threaded.v1', requests),
                                        deadline_utc=time.time()+20)
        ledger.reserve_unit('unit', requests)
        return ledger.claim_shared_unit('unit')

    async def test_committed_reservations_precede_loop_observation_and_enforce_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = self.ledger(Path(directory)/'budget.sqlite', 3)
            loop_thread = threading.get_ident()
            observed, events, threads = [], [], []
            reserve = ledger.reserve
            class Ledger:
                def reserve(self, digest):
                    threads.append(threading.get_ident())
                    return reserve(digest)
            def observe(attempt, body):
                self.assertEqual(threading.get_ident(), loop_thread)
                self.assertGreaterEqual(ledger.attempts, attempt)
                observed.append((attempt, hashlib.sha256(body).hexdigest()))
            with ThreadedRequestGuard(Ledger(), observe, events.append) as guard:
                await asyncio.gather(*(guard(task(i)) for i in range(3)))
                with self.assertRaises(BudgetError):
                    await guard(task(3))
            self.assertEqual([r[0] for r in observed], [1, 2, 3])
            self.assertEqual(len(set(threads)), 1)
            self.assertNotEqual(threads[0], loop_thread)
            self.assertEqual(ledger.attempts, 3)
            self.assertEqual([e['status'] for e in events], ['completed']*3+['failed'])
            self.assertTrue(all(e['reserve_ns'] >= 0 for e in events))

    async def test_loop_progress_and_cancel_wait_for_commit_without_observing_or_refunding(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = self.ledger(Path(directory)/'budget.sqlite', 1)
            entered, release = threading.Event(), threading.Event()
            observed, events = [], []
            class Ledger:
                def reserve(self, digest):
                    entered.set()
                    if not release.wait(3):
                        raise RuntimeError('fixture accounting was not released')
                    return ledger.reserve(digest)
            with ThreadedRequestGuard(Ledger(), lambda *args: observed.append(args), events.append) as guard:
                operation = asyncio.create_task(guard(task(0)))
                try:
                    await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 2.5)
                    progressed = []
                    asyncio.get_running_loop().call_soon(progressed.append, 'loop is responsive')
                    await asyncio.sleep(0)
                    self.assertEqual(progressed, ['loop is responsive'])
                    operation.cancel()
                    await asyncio.sleep(0)
                    self.assertFalse(operation.done(), 'cancellation abandoned a pending durable transaction')
                    release.set()
                    with self.assertRaises(asyncio.CancelledError):
                        await operation
                finally:
                    release.set()
                    if not operation.done():
                        await asyncio.gather(operation, return_exceptions=True)
            self.assertEqual(ledger.attempts, 1)
            self.assertEqual(observed, [])
            self.assertEqual(events[0]['status'], 'cancelled')
            self.assertEqual(events[0]['attempt'], 1)

    async def test_failed_observation_keeps_committed_allowance(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = self.ledger(Path(directory)/'budget.sqlite', 1)
            def fail(*args):
                raise ValueError('fixture request record failed')
            events = []
            with ThreadedRequestGuard(ledger, fail, events.append) as guard:
                with self.assertRaisesRegex(ValueError, 'record failed'):
                    await guard(task(0))
            self.assertEqual(ledger.attempts, 1)
            self.assertEqual(events[0]['status'], 'failed')
