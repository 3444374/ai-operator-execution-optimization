"""Ready-row accounting preserves durable quota before mocked remote sends."""
import asyncio
from pathlib import Path
import tempfile
import time
import unittest

from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.batched_request_guard import BatchedRequestGuard
from tests.execution_provider.test_ray_map_transport import task


class BatchedGuardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.ledger = CellBudgetLedger.create(Path(self.tmp.name)/'budget.sqlite',
            AttemptBudget('guard.batch.fixture', 40), deadline_utc=time.time()+30)
        self.ledger.reserve_unit('query', 40)
        self.unit = self.ledger.claim_shared_unit('query')
        self.observed, self.events = [], []
        self.guard = BatchedRequestGuard(self.unit, lambda attempt, body:self.observed.append(attempt), self.events.append)

    async def test_ready_rows_commit_in_bounded_batches_before_send(self):
        async def send(i):
            await self.guard(task(i))
            self.assertGreaterEqual(self.unit.attempts, i+1)
            self.assertIn(i+1, self.observed)
        await asyncio.gather(*(send(i) for i in range(33)))
        self.assertEqual(self.observed, list(range(1, 34)))
        self.assertEqual([e['rows'] for e in self.events if e['event']=='remote_request_batch'], [16,16,1])
        self.assertEqual(self.guard.pending, {})

    async def test_single_ready_row_does_not_wait_for_a_full_batch(self):
        await asyncio.wait_for(self.guard(task(0)), timeout=1)
        self.assertEqual(self.unit.attempts, 1)

    async def test_cancel_waits_for_queued_commit_and_never_sends(self):
        sent = []
        async def send():
            await self.guard(task(0))
            sent.append(0)
        operation = asyncio.create_task(send())
        await asyncio.sleep(0)
        operation.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await operation
        self.assertEqual(sent, [])
        self.assertEqual(self.unit.attempts, 1)
        self.assertEqual(self.guard.pending, {})
        guards = [e for e in self.events if e['event']=='remote_request_guard']
        self.assertEqual([(e['attempt'], e['status']) for e in guards], [(1, 'cancelled')])

    async def test_closed_budget_rejects_whole_ready_group(self):
        self.ledger.close_shared_unit('query')
        results = await asyncio.gather(*(self.guard(task(i)) for i in range(3)), return_exceptions=True)
        self.assertTrue(all(isinstance(x, BudgetExhausted) for x in results))
        self.assertEqual(self.unit.attempts, 0)
        self.assertEqual(self.observed, [])
        batch = next(e for e in self.events if e['event']=='remote_request_batch')
        self.assertEqual((batch['status'], batch['stage']), ('failed', 'reserve'))
        self.assertIsNone(batch['first_attempt'])
        self.assertIsNone(batch['last_attempt'])
        self.assertIsNone(batch['reserve_ns'])

    async def test_observation_failure_keeps_charge_and_permits_no_send(self):
        def fail(*args):
            raise RuntimeError('injected recording error')
        self.guard.observe_request = fail
        results = await asyncio.gather(*(self.guard(task(i)) for i in range(3)), return_exceptions=True)
        self.assertTrue(all(isinstance(x, RuntimeError) for x in results))
        self.assertEqual(self.unit.attempts, 3)
        self.assertEqual(self.guard.pending, {})
        batch = next(e for e in self.events if e['event']=='remote_request_batch')
        self.assertEqual((batch['first_attempt'], batch['last_attempt'], batch['rows']), (1,3,3))
        self.assertEqual((batch['status'], batch['stage']), ('failed', 'request_observe'))
        self.assertGreaterEqual(batch['reserve_ns'], 0)
        guards = [e for e in self.events if e['event']=='remote_request_guard']
        self.assertEqual([(e['key']['sequence'], e['attempt'], e['status']) for e in guards],
                         [(0,1,'failed'), (1,2,'failed'), (2,3,'failed')])

    async def test_failed_batch_recording_keeps_charge_and_permits_no_send(self):
        def record(event):
            if event['event']=='remote_request_batch':
                raise RuntimeError('injected batch writer error')
            self.events.append(event)
        self.guard.record = record
        sent = []
        async def send(i):
            await self.guard(task(i))
            sent.append(i)
        results = await asyncio.gather(*(send(i) for i in range(3)), return_exceptions=True)
        self.assertTrue(all(isinstance(x, RuntimeError) for x in results))
        self.assertEqual(sent, [])
        self.assertEqual(self.unit.attempts, 3)
        self.assertEqual([e['attempt'] for e in self.events], [1,2,3])
        self.assertTrue(all(e['status']=='failed' for e in self.events))

    async def test_batch_recording_error_does_not_replace_observation_failure(self):
        def observe(*args):
            raise ValueError('injected observation error')
        def record(event):
            if event['event']=='remote_request_batch':
                raise RuntimeError('injected batch writer error')
            self.events.append(event)
        self.guard.observe_request, self.guard.record = observe, record
        results = await asyncio.gather(*(self.guard(task(i)) for i in range(3)), return_exceptions=True)
        self.assertTrue(all(isinstance(x, ValueError) for x in results))
        self.assertEqual(self.unit.attempts, 3)

    async def test_all_recording_failures_preserve_observation_error(self):
        def observe(*args):
            raise ValueError('injected observation error')
        def record(event):
            raise RuntimeError('injected writer error')
        self.guard.observe_request, self.guard.record = observe, record
        results = await asyncio.gather(*(self.guard(task(i)) for i in range(3)), return_exceptions=True)
        self.assertTrue(all(isinstance(x, ValueError) for x in results))
        self.assertEqual(self.unit.attempts, 3)
        self.assertEqual(self.guard.pending, {})

    async def test_all_recording_failures_preserve_cancellation(self):
        def record(event):
            raise RuntimeError('injected writer error')
        self.guard.record = record
        operation = asyncio.create_task(self.guard(task(0)))
        await asyncio.sleep(0)
        operation.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await operation
        self.assertTrue(operation.cancelled())
        self.assertEqual(self.unit.attempts, 1)
        self.assertEqual(self.guard.pending, {})


class BatchLoopLifetimeTests(unittest.TestCase):
    def test_one_guard_supports_sequential_gateway_event_loops(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = CellBudgetLedger.create(Path(tmp)/'budget.sqlite',
                AttemptBudget('guard.loops', 2), deadline_utc=time.time()+30)
            ledger.reserve_unit('query',2)
            shared = ledger.claim_shared_unit('query')
            guard = BatchedRequestGuard(shared, lambda *args:None, lambda event:None)
            for i in range(2):
                asyncio.run(guard(task(i)))
            self.assertEqual(shared.attempts,2)
            self.assertEqual(guard.pending,{})
