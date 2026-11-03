"""Byte-budget refusal through real driver/Core; deterministic CPU, no model/Ray."""
from dataclasses import asdict
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import _RecordLocalRows
from src.execution_provider.adapters.lotus_records_core import CoreRecordsCapacity
from src.semantic_methods.budget import MethodBudgetPool, MethodCapacity, row_reservation
from src.semantic_methods.continuation import Continue, Final, MethodLimits, Request
from src.semantic_methods.driver import MethodDriver, RowIdentity
from src.scheduling.core.session_contract import SessionSpec, TaskProfile, TaskKey, Uncertain
from tests.scheduling.test_incremental_session import setup


class Program:
    def __init__(self, remote=False):
        self.remote = remote

    def input(self, value, **unused):
        return value

    def start(self, value):
        return Continue(Request('step', value, 1, 128)) if self.remote else Final(value)

    def resume(self, state, result):
        return Final(result)


class RefillWaitTests(unittest.TestCase):
    def fixture(self, *, remote=False):
        engine, session, backend, _ = setup(held_tasks=9, input_bytes=2048, result_bytes=2048,
            active_requests=3, active_work=3, offer_tasks=9, item_input_bytes=128,
            item_result_bytes=128, metadata_bytes=2048)
        session.close()
        session = engine.open(SessionSpec('job', 'flow', 'fixture',
            task_profiles=(TaskProfile('step', 'fixture'),)))
        limits = MethodLimits(128, 128, 128, 1)
        pool = MethodBudgetPool(MethodCapacity(9, row_reservation(limits)))
        driver = MethodDriver(session, Program(remote), limits, pool.allocate(pool.capacity))
        self.assertTrue(driver.offer_row(RowIdentity(0, 'r0'), b'0'))
        stream = object.__new__(_RecordLocalRows)
        stream.owner = SimpleNamespace(deadline=time.monotonic()+30, core=engine,
            resources=SimpleNamespace(max_active_batches=9), capacity=CoreRecordsCapacity(),
            _async_token_batches=True, _token_organizer=SimpleNamespace(usage=lambda: None),
            _supply_feedback=None)
        stream.session, stream.driver, stream.pool = session, driver, pool
        stream.cancelled = stream.section = stream.pending = None
        stream._phase_eof, stream._using_source, stream._phase_count = False, False, 0
        pulls = []
        def source():
            for index in (1, 2, 3):
                pulls.append(index)
                yield SimpleNamespace(source_position=index, row_id='r'+str(index))
        stream._phase_source = source()
        stream._prepared = {'r'+str(i):str(i).encode() for i in (1, 2, 3)}
        stream.run = SimpleNamespace(parents={('r'+str(i), i):() for i in (1, 2, 3)})
        stream._extract_results, stream.peak_core_usage = [], asdict(engine.capacity.usage())
        actual_fill, attempts = stream._fill, []
        def guarded_fill():
            attempts.append(stream._phase_count)
            if len(attempts) > 4:
                raise RuntimeError('bounded probe stopped a repeated refused fill')
            return actual_fill()
        stream._fill = guarded_fill
        return stream, backend, pulls, attempts

    def test_refused_input_waits_once_and_keeps_one_pending_record(self):
        stream, _, pulls, attempts = self.fixture()
        with patch.object(stream.driver, 'results', return_value=()), \
                patch.object(stream.owner.core.wake, 'wait') as wait:
            for index in range(3):
                attempts.clear()
                stream._tick()
                self.assertEqual(len(attempts), 1)
                self.assertEqual(stream.pending.row_id, 'r1')
            self.assertEqual(wait.call_count, 3)
        self.assertEqual(pulls, [1])
        self.assertEqual(stream._phase_count, 0)
        self.assertEqual(stream.driver.active_rows, 1)

    def test_release_result_wakes_waiter_and_next_tick_retries_without_duplicate(self):
        stream, _, pulls, attempts = self.fixture()
        held = stream.driver.results(1)[0]
        with patch.object(stream.owner.core.wake, 'wait'):
            stream._tick()
        generation = stream.owner.core.wake.generation
        entered, returned = threading.Event(), threading.Event()
        def waiter():
            entered.set()
            stream.owner.core.wake.wait(generation, 1)
            returned.set()
        thread = threading.Thread(target=waiter)
        thread.start()
        self.assertTrue(entered.wait(1))
        try:
            stream.driver.release_result(held.row)
            self.assertGreater(stream.owner.core.wake.generation, generation)
            self.assertTrue(returned.wait(.3))
        finally:
            stream.owner.core.wake.notify()
            thread.join(1)
        attempts.clear()
        with patch.object(stream.owner.core.wake, 'wait'):
            stream._tick()
        self.assertEqual(stream._phase_count, 1)
        self.assertEqual(pulls, [1, 2])
        self.assertEqual(stream._extract_results, [1])
        self.assertEqual(stream.pending.row_id, 'r2')

    def test_owner_consumption_releases_budget_before_followup_refill(self):
        stream, _, pulls, attempts = self.fixture()
        for _ in range(2):
            attempts.clear()
            stream._tick()
        self.assertEqual(stream._phase_count, 1)
        self.assertEqual(stream._extract_results, [0, 1])
        self.assertEqual(pulls, [1, 2])
        self.assertEqual(stream.pool.used, (0, 0))

    def test_unknown_terminates_flow_without_releasing_remote_responsibility(self):
        stream, backend, pulls, attempts = self.fixture(remote=True)
        stream.driver.advance(1)
        stream.driver.advance(1)
        key = TaskKey(stream.session.session_id, 0)
        handle, _ = backend.pending[key]
        backend.events.append(Uncertain(key, handle))
        with self.assertRaises(RuntimeError):
            stream._tick()
        self.assertEqual(len(attempts), 1)
        self.assertEqual(pulls, [1])
        usage = stream.owner.core.capacity.usage()
        self.assertEqual(usage.active_requests, 1)
        self.assertGreater(usage.input_bytes, 0)
        self.assertGreater(usage.result_bytes, 0)
        self.assertEqual(stream._phase_count, 0)


if __name__ == '__main__':
    unittest.main()
