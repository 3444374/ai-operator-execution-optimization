"""Ready selection over real Core; model completion and time are controlled CPU values."""

from dataclasses import asdict
import unittest

from src.semantic_methods.budget import MethodBudgetPool, MethodCapacity, row_reservation
from src.semantic_methods.continuation import Continue, Final, MethodLimits, Request
from src.semantic_methods.driver import MethodDriver, RowIdentity, SuccessorFirst
from src.scheduling.core.session_contract import SessionSpec, TaskProfile
from tests.scheduling.test_incremental_session import setup


class Stages:
    def start(self, value):
        return Continue(Request("model", value, 1, 8), value)

    def resume(self, state, result):
        remaining = int(state)
        if remaining == 1:
            return Final(result)
        return Continue(Request("model", result, 1, 8), str(remaining - 1).encode())


def make(rows=3, slots=1, *, policy="successor_first", burst=2, aging=100, observer=None):
    core, old, backend, clock = setup(metadata_bytes=4096, held_tasks=slots,
        active_requests=slots, active_work=slots, input_bytes=slots * 8, result_bytes=slots * 8)
    old.close()
    session = core.open(SessionSpec("opaque-job", "flow", "model",
                                   task_profiles=(TaskProfile("model", "model"),)))
    limits = MethodLimits(8, 8, 8, 16)
    pool = MethodBudgetPool(MethodCapacity(rows, rows * row_reservation(limits)))
    driver = MethodDriver(session, Stages(), limits, pool.allocate(pool.capacity),
        ready_order=policy, successor_first=SuccessorFirst(burst, aging) if policy == "successor_first" else None,
        clock=clock, on_ready_event=observer)
    return driver, core, backend, clock, pool


def finish(driver, backend):
    driver.advance(driver.session.limits.held_tasks)
    for key in tuple(backend.pending):
        backend.complete(key, b"OK")


class ReadyOrderTests(unittest.TestCase):
    def run_order(self, policy, stages=4, *, aging=100):
        events = []
        d, core, backend, clock, pool = make(policy=policy, aging=aging, observer=events.append)
        self.addCleanup(d.close)
        for i in range(3):
            self.assertTrue(d.offer_row(RowIdentity(i, "opaque-" + str(i)), str(stages).encode()))
        d.end_input()
        for _ in range(100):
            finish(d, backend)
            for result in d.results(3):
                d.release_result(result.row)
            if d.finished:
                break
        self.assertTrue(d.finished)
        self.assertEqual(core.capacity.usage().held_tasks, 0)
        self.assertEqual(pool.used, (0, 0))
        return events

    def test_successors_precede_sources_but_burst_gives_source_a_turn(self):
        fifo = self.run_order("fifo")
        priority = self.run_order("successor_first")
        pairs = lambda events: [(e.row.sequence, e.stage_index) for e in events if e.action == "Core_offer_accepted"]
        self.assertEqual(pairs(fifo)[:3], [(0, 0), (1, 0), (2, 0)])
        self.assertEqual(pairs(priority)[:4], [(0, 0), (0, 1), (0, 2), (1, 0)])
        self.assertIn("source_turn", [e.reason for e in priority])
        self.assertEqual(len(pairs(priority)), len(set(pairs(priority))))

    def test_aging_chooses_oldest_before_a_fresh_successor(self):
        events = []
        d, core, backend, clock, _ = make(rows=2, aging=0.5, observer=events.append)
        self.addCleanup(d.close)
        for i in range(2): d.offer_row(RowIdentity(i, str(i)), b"3")
        d.advance(); d.advance()
        clock.now = 0.6
        backend.complete(next(iter(backend.pending)))
        d.advance()
        offers = [e for e in events if e.action == "Core_offer_accepted"]
        self.assertEqual([(e.row.sequence, e.stage_index) for e in offers], [(0, 0), (1, 0)])
        self.assertEqual(offers[-1].reason, "aged")
        self.assertEqual(offers[-1].ready_at, 0)
        self.assertEqual(offers[-1].at, 0.6)

    def test_backpressure_preserves_age_and_does_not_consume_burst(self):
        events = []
        d, core, backend, clock, _ = make(rows=2, aging=0.5, observer=events.append)
        self.addCleanup(d.close)
        for i in range(2): d.offer_row(RowIdentity(i, str(i)), b"3")
        d.advance(); d.advance()
        for _ in range(4):
            clock.now += 0.1
            p = d.advance()
            self.assertFalse(p.has_immediate_work)
        waits = [e for e in events if e.action == "Core_backpressure"]
        self.assertTrue(waits)
        self.assertTrue(all(e.ready_at == 0 for e in waits))
        self.assertEqual(d._successor_burst, 0)
        clock.now = 0.6
        backend.complete(next(iter(backend.pending)))
        d.advance()
        self.assertEqual([e.reason for e in events if e.action == "Core_offer_accepted"][-1], "aged")

    def test_long_chain_and_late_source_both_get_bounded_service(self):
        events = []
        d, core, backend, clock, _ = make(rows=3, observer=events.append)
        self.addCleanup(d.close)
        d.offer_row(RowIdentity(0, "long"), b"12")
        d.advance(); d.advance()
        self.assertTrue(d.offer_row(RowIdentity(1, "late"), b"2"))
        self.assertTrue(d.offer_row(RowIdentity(2, "tail"), b"1"))
        d.end_input()
        for _ in range(100):
            for key in tuple(backend.pending): backend.complete(key, b"OK")
            d.advance()
            for result in d.results(3): d.release_result(result.row)
            if d.finished: break
        self.assertTrue(d.finished)
        offers = [(e.row.sequence, e.stage_index) for e in events if e.action == "Core_offer_accepted"]
        self.assertLessEqual(offers.index((1, 0)), 3)
        self.assertLessEqual(offers.index((2, 0)), 7)
        self.assertEqual(sum(row == 0 for row, stage in offers), 12)
        self.assertEqual(len(offers), 15)

    def test_successor_reenqueue_gets_a_new_ready_timestamp(self):
        events = []
        d, _, backend, clock, _ = make(rows=1, observer=events.append)
        self.addCleanup(d.close)
        d.offer_row(RowIdentity(0, "x"), b"2")
        d.advance(); d.advance()
        clock.now = 0.7
        backend.complete(next(iter(backend.pending)))
        d.advance()
        ready = [e for e in events if e.action == "method_ready"]
        self.assertEqual([(e.stage_index, e.ready_at) for e in ready], [(0, 0), (1, 0.7)])

    def test_ready_events_are_actual_local_clocks_and_not_native_acceptance(self):
        events = self.run_order("successor_first", stages=2)
        ready = {(e.row, e.stage_index): e for e in events if e.action == "method_ready"}
        for offered in (e for e in events if e.action == "Core_offer_accepted"):
            self.assertGreaterEqual(offered.at, ready[offered.row, offered.stage_index].at)
            self.assertEqual(offered.ready_at, ready[offered.row, offered.stage_index].ready_at)
            self.assertIsNotNone(offered.key)
        self.assertTrue(all("payload" not in asdict(e) for e in events))

    def test_bad_optional_observer_is_disabled_without_changing_results(self):
        def broken(event): raise OSError("optional observer unavailable")
        d, core, backend, clock, pool = make(rows=1, observer=broken)
        self.addCleanup(d.close)
        d.offer_row(RowIdentity(0, "x"), b"2"); d.end_input()
        output = []
        for _ in range(20):
            finish(d, backend)
            for result in d.results():
                output.append(result.value); d.release_result(result.row)
            if d.finished: break
        self.assertEqual(output, [b"OK"])
        self.assertTrue(d.finished)
        self.assertEqual(d.ready_observer_error, "OSError")

    def test_explicit_configuration_rejects_invalid_values(self):
        for burst, age in [(0, 1), (True, 1), (1, 0), (1, True), (1, float("nan")), (1, float("inf"))]:
            with self.subTest(burst=burst, age=age), self.assertRaises(ValueError):
                SuccessorFirst(burst, age)
        with self.assertRaises(ValueError): make(policy="unknown")

    def test_fifo_with_tracing_off_never_reads_clock(self):
        d, core, backend, _, _ = make(rows=1, policy="fifo")
        self.addCleanup(d.close)
        def forbidden(): raise AssertionError("FIFO should not read ready clock")
        d._clock = forbidden
        d.offer_row(RowIdentity(0, "x"), b"2"); d.end_input()
        for _ in range(20):
            finish(d, backend)
            for result in d.results(): d.release_result(result.row)
            if d.finished: break
        self.assertTrue(d.finished)


if __name__ == "__main__":
    unittest.main()
