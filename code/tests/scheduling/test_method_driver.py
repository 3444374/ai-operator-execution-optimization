"""Adversarial producer/consumer and terminal ordering through the real method driver."""

import unittest

from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import SessionSpec, TaskProfile
from src.scheduling.core.session_jobs import JobBudget
from src.semantic_methods.budget import (
    MethodBudgetPool,
    MethodCapacity,
    row_reservation,
)
from src.semantic_methods.continuation import Continue, Final, MethodLimits, Request
from src.semantic_methods.driver import MethodDriver, RowIdentity

from tests.scheduling.test_incremental_session import setup

LIMITS = MethodLimits(input_bytes=8, state_bytes=8, result_bytes=8, stages=2)
ROW_BYTES = row_reservation(LIMITS)


class Stages:
    def start(self, value):
        if value == b"zero":
            return Final(value)
        return Continue(Request("model", value, 1, 8), value)

    def resume(self, state, result):
        if state == b"two":
            return Continue(Request("model", result, 1, 8))
        return Final(result)


def make_driver(rows=2, slots=1, method=None):
    engine, old, backend, _clock = setup(
        metadata_bytes=1024,
        held_tasks=slots,
        active_requests=slots,
        active_work=slots,
        input_bytes=slots * 8,
        result_bytes=slots * 8,
    )
    old.close()
    session = engine.open(
        SessionSpec(
            "methods", "flow", "model", task_profiles=(TaskProfile("model", "model"),)
        )
    )
    pool = MethodBudgetPool(MethodCapacity(rows, rows * ROW_BYTES))
    driver = MethodDriver(
        session, method or Stages(), LIMITS, pool.allocate(pool.capacity)
    )
    return driver, engine, backend, pool


def tick(driver, backend, reverse=False):
    progress = driver.advance(driver.session.limits.held_tasks)
    for key in sorted(backend.pending, key=lambda k: k.sequence, reverse=reverse):
        backend.complete(key, b"OK")
    return progress


class MethodDriverTests(unittest.TestCase):
    def test_zero_one_two_stages_eof_and_one_slot(self):
        driver, engine, backend, pool = make_driver(rows=3)
        for seq, value in enumerate((b"zero", b"one", b"two")):
            self.assertTrue(driver.offer_row(RowIdentity(seq, "call"), value))
        driver.end_input()
        output = {}
        for _ in range(40):
            tick(driver, backend)
            for result in driver.results(3):
                output[result.row.sequence] = result.value
                driver.release_result(result.row)
            if driver.finished:
                break
        self.assertTrue(driver.finished)
        self.assertEqual(output, {0: b"zero", 1: b"OK", 2: b"OK"})
        self.assertEqual(len(backend.routes), 3)
        self.assertEqual(pool.used, (0, 0))
        self.assertEqual(engine.capacity.usage().held_tasks, 0)
        driver.close()
        self.assertEqual(pool.allocated, (0, 0))

    def test_reordered_identical_rows_keep_call_identity(self):
        driver, _, backend, _ = make_driver(rows=3, slots=3)
        for seq in range(3):
            driver.offer_row(RowIdentity(seq, f"call{seq}"), b"one")
        driver.end_input()
        tick(driver, backend)
        driver.advance(3)
        for key in sorted(backend.pending, key=lambda k: k.sequence, reverse=True):
            backend.complete(key, str(key.sequence).encode())
        driver.advance(3)
        results = driver.results(3)
        self.assertEqual(
            {r.row.call_id: r.value for r in results},
            {"call0": b"0", "call1": b"1", "call2": b"2"},
        )
        driver.close()

    def test_final_lease_backpressure_and_long_stream_no_history(self):
        driver, _, backend, pool = make_driver(rows=1)
        for seq in range(100):
            row = RowIdentity(seq, "call")
            self.assertTrue(driver.offer_row(row, b"zero"))
            (result,) = driver.results()
            self.assertEqual(result.row, row)
            self.assertFalse(driver.offer_row(RowIdentity(seq + 1, "call"), b"zero"))
            self.assertEqual(pool.used, (1, ROW_BYTES))
            self.assertEqual(driver.results(), ())
            driver.release_result(row)
            self.assertEqual(driver.active_rows, 0)
            self.assertEqual(pool.used, (0, 0))
        driver.end_input()
        tick(driver, backend)
        tick(driver, backend)
        self.assertTrue(driver.finished)
        driver.close()

    def test_pool_grants_are_shared_and_not_reissued(self):
        pool = MethodBudgetPool(MethodCapacity(2, ROW_BYTES * 2))
        first = pool.allocate(MethodCapacity(1, ROW_BYTES))
        second = pool.allocate(MethodCapacity(1, ROW_BYTES))
        with self.assertRaises(ValueError):
            pool.allocate(MethodCapacity(1, ROW_BYTES))
        first.claim()
        with self.assertRaises(ValueError):
            first.claim()
        self.assertTrue(first.reserve(ROW_BYTES))
        second.claim()
        self.assertTrue(second.reserve(ROW_BYTES))
        self.assertEqual(pool.used, (2, ROW_BYTES * 2))
        first.release(ROW_BYTES)
        first.close()
        second.release(ROW_BYTES)
        second.close()
        self.assertEqual(pool.allocated, (0, 0))

    def test_callback_failure_releases_whole_delivery_batch(self):
        class Broken(Stages):
            def resume(self, state, result):
                raise ValueError("callback failed")

        driver, engine, backend, pool = make_driver(rows=2, slots=2, method=Broken())
        for seq in range(2):
            driver.offer_row(RowIdentity(seq, "call"), b"one")
        tick(driver, backend)
        tick(driver, backend)
        with self.assertRaisesRegex(ValueError, "callback failed"):
            driver.advance(2)
        self.assertEqual(engine.capacity.usage().held_tasks, 0)
        self.assertEqual(pool.used, (0, 0))
        self.assertEqual(pool.allocated, (0, 0))

    def test_cancel_keeps_remote_credit_until_late_terminal(self):
        driver, engine, backend, pool = make_driver()
        driver.offer_row(RowIdentity(0, "call"), b"two")
        driver.advance()
        driver.advance()
        (key,) = backend.pending
        driver.close()
        driver.close()
        self.assertEqual(pool.allocated, (0, 0))
        self.assertEqual(engine.capacity.usage().held_tasks, 1)
        backend.complete(key)
        engine.advance()
        self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_backend_error_never_reaches_resume(self):
        driver, engine, backend, pool = make_driver()
        driver.offer_row(RowIdentity(0, "call"), b"two")
        driver.advance()
        driver.advance()
        (key,) = backend.pending
        backend.complete(key, status="failed")
        with self.assertRaises(RuntimeError):
            driver.advance()
        self.assertIsNotNone(driver.error)
        self.assertEqual(pool.used, (0, 0))
        self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_invalid_input_and_duplicate_identity(self):
        driver, _, _, pool = make_driver()
        driver.offer_row(RowIdentity(1, "call"), b"zero")
        with self.assertRaises(ValueError):
            driver.offer_row(RowIdentity(1, "different"), b"zero")
        with self.assertRaises(ValueError):
            driver.offer_row(RowIdentity(2, "call"), b"oversized input")
        self.assertEqual(pool.allocated, (0, 0))

    def test_new_submission_requires_another_immediate_step(self):
        driver, _, _, _ = make_driver()
        driver.offer_row(RowIdentity(0, "call"), b"one")
        self.assertTrue(driver.advance().has_immediate_work)
        driver.close()

    def test_slow_consumer_does_not_stop_another_job(self):
        template, old, backend, clock = setup(
            held_tasks=2, input_bytes=16, result_bytes=16, metadata_bytes=1024
        )
        old.close()
        engine = SessionEngine(
            template.capacity.limits,
            backend,
            template.policies,
            clock=clock,
            max_jobs=2,
        )
        pool = MethodBudgetPool(MethodCapacity(2, 2 * ROW_BYTES))
        drivers, jobs = [], []
        for i in range(2):
            job = engine.register_job(str(i), JobBudget(1, 8, 8, 1, 1))
            jobs.append(job)
            session = engine.open(
                SessionSpec(
                    str(i),
                    "flow",
                    "model",
                    task_profiles=(TaskProfile("model", "model"),),
                ),
                job=job,
            )
            drivers.append(
                MethodDriver(
                    session,
                    Stages(),
                    LIMITS,
                    pool.allocate(MethodCapacity(1, ROW_BYTES)),
                )
            )
        slow, fast = drivers
        slow.offer_row(RowIdentity(0, "slow"), b"zero")
        slow.results()  # Held indefinitely; its grant must remain occupied.
        fast.offer_row(RowIdentity(0, "fast"), b"two")
        fast.end_input()
        for _ in range(30):
            engine.advance()
            fast.advance()
            for key in tuple(backend.pending):
                backend.complete(key, b"OK")
            for result in fast.results():
                self.assertEqual(result.value, b"OK")
                fast.release_result(result.row)
            if fast.finished:
                break
        self.assertTrue(fast.finished)
        self.assertEqual(pool.used, (1, ROW_BYTES))
        for driver, job in zip(drivers, jobs):
            driver.close()
            engine.close_job(job)
        engine.advance()
        self.assertEqual(pool.allocated, (0, 0))

    def test_organized_path_has_row_stage_work_metadata(self):
        from dataclasses import replace

        from src.scheduling.organization.session_window import WorkWindowOrganizer

        driver, engine, backend, pool = make_driver(rows=2, slots=2)
        engine.policies = replace(engine.policies, organize=WorkWindowOrganizer(2, 2))
        driver.offer_row(RowIdentity(10, "a"), b"two")
        driver.offer_row(RowIdentity(11, "b"), b"two")
        driver.end_input()
        observed = []
        for _ in range(30):
            driver.advance(2)
            for key, (_, task) in tuple(backend.pending.items()):
                observed.append(
                    (
                        task.task.info.call_id,
                        task.task.info.row_sequence,
                        task.task.info.stage_id,
                    )
                )
                backend.complete(key, b"OK")
            for result in driver.results(2):
                driver.release_result(result.row)
            if driver.finished:
                break
        self.assertTrue(driver.finished)
        self.assertCountEqual(
            observed, [("a", 10, "0"), ("a", 10, "1"), ("b", 11, "0"), ("b", 11, "1")]
        )
        driver.close()
        self.assertEqual(pool.allocated, (0, 0))

    def test_failed_callback_does_not_retain_unbounded_diagnostic(self):
        class Broken(Stages):
            def start(self, value):
                raise ValueError("x" * 100000)

        driver, _, _, pool = make_driver(method=Broken())
        with self.assertRaises(ValueError):
            driver.offer_row(RowIdentity(0, "call"), b"one")
        self.assertEqual(driver.error, "ValueError")
        self.assertEqual(pool.allocated, (0, 0))
