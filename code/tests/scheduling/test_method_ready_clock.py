"""Clock failures at public offer/advance calls over the actual Core CPU fixture."""

import unittest

from src.semantic_methods.driver import RowIdentity
from tests.scheduling.test_method_ready_order import finish, make


class FaultClock:
    def __init__(self, clock):
        self.clock = clock
        self.until_failure = None

    def fail_after(self, successful_reads=0):
        self.until_failure = successful_reads

    def __call__(self):
        if self.until_failure is not None:
            if self.until_failure == 0:
                self.until_failure = None
                raise OSError("CPU fixture clock unavailable")
            self.until_failure -= 1
        return self.clock()


class ReadyClockTests(unittest.TestCase):
    def setup_driver(self, *, policy="fifo", rows=1, aging=100):
        events = []
        d, core, backend, clock, pool = make(
            rows=rows, policy=policy, aging=aging, observer=events.append)
        self.addCleanup(d.close)
        fault = FaultClock(clock)
        d._clock = fault
        return d, core, backend, clock, pool, fault, events

    def drain(self, d, backend, core, pool):
        d.end_input()
        output = []
        for _ in range(40):
            finish(d, backend)
            for result in d.results(3):
                output.append((result.row.sequence, result.value))
                d.release_result(result.row)
            if d.finished:
                break
        self.assertTrue(d.finished)
        self.assertEqual(d.error, None)
        self.assertEqual(core.capacity.usage().held_tasks, 0)
        self.assertEqual(pool.used, (0, 0))
        d.close()
        self.assertEqual(pool.allocated, (0, 0))
        return output

    def test_fifo_optional_ready_clock_failure_does_not_leak_or_fail_offer(self):
        d, core, backend, _, pool, fault, events = self.setup_driver()
        fault.fail_after()
        self.assertTrue(d.offer_row(RowIdentity(0, "source"), b"2"))
        self.assertEqual(d.active_rows, 1)
        self.assertEqual(d.ready_observer_error, "OSError")
        self.assertEqual(events, [])
        self.assertEqual(self.drain(d, backend, core, pool), [(0, b"OK")])
        self.assertEqual(len(backend.routes), 2)

    def test_fifo_optional_accept_clock_failure_preserves_accepted_task(self):
        d, core, backend, _, pool, fault, events = self.setup_driver()
        d.offer_row(RowIdentity(0, "source"), b"2")
        fault.fail_after()
        d.advance()
        self.assertEqual(d.ready_observer_error, "OSError")
        self.assertEqual(len(d._tasks), 1)
        self.assertEqual(core.capacity.usage().held_tasks, 1)
        self.assertEqual(self.drain(d, backend, core, pool), [(0, b"OK")])
        self.assertEqual(len(backend.routes), 2)
        self.assertEqual([e.action for e in events], ["method_ready"])

    def test_priority_required_ready_clock_failure_aborts_and_returns_local_budget(self):
        d, core, _, _, pool, fault, _ = self.setup_driver(policy="successor_first")
        fault.fail_after()
        with self.assertRaises(OSError):
            d.offer_row(RowIdentity(0, "source"), b"2")
        self.assertEqual(d.error, "OSError")
        self.assertEqual(d.active_rows, 0)
        self.assertEqual(pool.used, (0, 0))
        self.assertEqual(pool.allocated, (0, 0))
        self.assertEqual(core.capacity.usage().held_tasks, 0)
        d.close()  # Idempotent after the failed offer.

    def test_priority_required_selection_clock_failure_keeps_remote_responsibility(self):
        d, core, backend, _, pool, fault, _ = self.setup_driver(
            policy="successor_first", rows=2)
        for i in range(2):
            d.offer_row(RowIdentity(i, "source-" + str(i)), b"2")
        d.advance(); d.advance()
        (key,) = backend.pending
        fault.fail_after()
        with self.assertRaises(OSError):
            d.advance()
        self.assertEqual(d.error, "OSError")
        self.assertEqual(pool.used, (0, 0))
        self.assertEqual(pool.allocated, (0, 0))
        self.assertEqual(core.capacity.usage().held_tasks, 1)
        self.assertIn(key, backend.cancelled)
        d.close()
        self.assertEqual(core.capacity.usage().held_tasks, 1)
        backend.complete(key)
        core.advance()
        self.assertEqual(core.capacity.usage().held_tasks, 0)

    def test_priority_required_successor_clock_failure_releases_delivery(self):
        d, core, backend, _, pool, fault, _ = self.setup_driver(policy="successor_first")
        d.offer_row(RowIdentity(0, "source"), b"2")
        d.advance(); d.advance()
        backend.complete(next(iter(backend.pending)), b"OK")
        fault.fail_after()
        with self.assertRaises(OSError):
            d.advance()
        self.assertEqual(d.error, "OSError")
        self.assertEqual(d.active_rows, 0)
        self.assertEqual(pool.used, (0, 0))
        self.assertEqual(pool.allocated, (0, 0))
        self.assertEqual(core.capacity.usage().held_tasks, 0)
        self.assertEqual(len(backend.routes), 1)

    def test_priority_optional_event_clock_failure_preserves_policy_age(self):
        for failure_at in ("ready_event", "accepted_event"):
            with self.subTest(failure_at=failure_at):
                d, core, backend, clock, pool, fault, _ = self.setup_driver(
                    policy="successor_first", rows=2, aging=0.5)
                if failure_at == "ready_event":
                    fault.fail_after(1)  # Policy read succeeds, diagnostic read fails.
                d.offer_row(RowIdentity(0, "source-0"), b"2")
                d.offer_row(RowIdentity(1, "source-1"), b"2")
                if failure_at == "accepted_event":
                    fault.fail_after(1)  # Selection read succeeds, event read fails.
                d.advance(); d.advance()
                self.assertEqual(d.ready_observer_error, "OSError")
                self.assertEqual(d._rows[1].ready_at, 0)
                clock.now = 0.6
                backend.complete(next(iter(backend.pending)), b"OK")
                d.advance()
                accepted = next(iter(d._tasks))
                self.assertEqual(d._tasks[accepted], 1)  # Old source wins aging.
                self.assertCountEqual(self.drain(d, backend, core, pool),
                                      [(0, b"OK"), (1, b"OK")])

    def test_fifo_optional_backpressure_clock_failure_allows_explicit_cancel(self):
        d, core, backend, _, pool, fault, _ = self.setup_driver(rows=2)
        for i in range(2):
            d.offer_row(RowIdentity(i, "source-" + str(i)), b"2")
        d.advance(); d.advance()
        (key,) = backend.pending
        fault.fail_after()
        d.advance()
        self.assertIsNone(d.error)
        self.assertEqual(d.ready_observer_error, "OSError")
        d.close(); d.close()
        self.assertEqual(pool.used, (0, 0))
        self.assertEqual(pool.allocated, (0, 0))
        self.assertEqual(core.capacity.usage().held_tasks, 1)
        self.assertIn(key, backend.cancelled)
        backend.complete(key)
        core.advance()
        self.assertEqual(core.capacity.usage().held_tasks, 0)


if __name__ == "__main__":
    unittest.main()
