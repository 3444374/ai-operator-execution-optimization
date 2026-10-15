"""Completion-first ownership and bounded ticks; deterministic, no SDK or models."""

from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.scheduling.core.session_contract import SessionSpec, SessionTimeouts, State, Usage
from src.semantic_methods.budget import MethodCapacity, row_reservation
from src.semantic_methods.continuation import Continue, Final, MethodLimits, Request
from src.semantic_methods.driver import MethodDriver
from src.semantic_methods.lotus.driver import iter_two_map_rows
from tests.execution_provider.test_native_tasks import fixture, task
from tests.scheduling.test_input_preparation import Preparation, prepared_setup
from tests.scheduling.test_incremental_session import task as small_task
from tests.scheduling.test_multisession import engine as multi_engine, budget, flow


LIMITS = MethodLimits(128, 128, 256, 2)


class TwoStages:
    def start(self, value):
        return Continue(Request("lotus-map", value, 1, 128), b"first:" + value)

    def resume(self, state, result):
        if state.startswith(b"first:"):
            return Continue(Request("lotus-map", state[6:], 1, 128), b"last:" + state[6:])
        return Final(state[5:] + b":" + result)


class DeferredDispatchTests(unittest.TestCase):
    def test_false_settles_delivery_without_selecting_or_dispatching(self):
        execution, backend, _ = fixture(active_requests=1)
        _, session, _ = execution.open_job("q", SessionSpec("q", "map", "text-completion"))
        session.offer((task(0), task(1)))
        execution.engine.advance()
        backend.complete(next(iter(backend.pending)))
        with patch.object(execution.engine, "_select_job_flow", side_effect=AssertionError("selected")):
            report = execution.engine.advance(dispatch=False)
        self.assertEqual(report.events, 1)
        self.assertEqual(backend.pending, {})
        delivery = session.advance(2).deliveries[0]
        session.release((delivery.lease_id,))
        execution.engine.advance()
        self.assertEqual(len(backend.pending), 1)

    def test_false_preserves_fractional_deadline_and_expires_queued_work(self):
        execution, _, clock = fixture(timeouts=SessionTimeouts(queue_s=.03125))
        _, session, _ = execution.open_job("q", SessionSpec("q", "map", "text-completion"))
        session.offer((task(0),))
        self.assertEqual(execution.engine.advance(dispatch=False).next_deadline, .03125)
        clock.now = .03125
        execution.engine.advance(dispatch=False)
        self.assertEqual(session.state, State.FAILED)
        self.assertEqual(session.error, "capacity wait timed out")
        self.assertEqual(execution.engine.capacity.usage(), Usage())

    def test_false_advances_preparation_and_cancellation_with_small_budgets(self):
        for maximum in (1, 2, 3):
            with self.subTest(maximum=maximum):
                engine, session, backend, preparation = prepared_setup(
                    registered=True, step_actions=maximum)
                session.offer((small_task(0), small_task(1)))
                for _ in range(4):
                    self.assertLessEqual(engine.advance(dispatch=False).events, maximum)
                self.assertEqual(len(preparation.held), 2)
                preparation.ready.update(preparation.held)
                self.assertFalse(backend.pending)
                engine.advance()
                session.request_cancel()
                for _ in range(4):
                    engine.advance(dispatch=False)
                self.assertEqual(session.state, State.CANCELLED)
                self.assertEqual(len(backend.cancelled), 1)
                self.assertEqual(engine.capacity.usage().active_requests, 1)
                for key in tuple(backend.pending):
                    preparation.releasable.add(key)
                    backend.complete(key)
                preparation.releasable.update(preparation.held)
                for _ in range(8):
                    engine.advance(dispatch=False)
                self.assertEqual(engine.capacity.usage(), Usage())

    def test_false_does_not_change_round_robin_history_across_jobs(self):
        engine, backend, _ = multi_engine(requests=2)
        handles = [engine.register_job(label, budget()) for label in ("A", "B")]
        sessions = [flow(engine, handle, handle.label) for handle in handles]
        for session in sessions:
            session.offer((small_task(0),))
        before = (engine._last_job, dict(engine._last_flow))
        engine.advance(dispatch=False)
        self.assertEqual((engine._last_job, engine._last_flow), before)
        self.assertFalse(backend.pending)
        engine.advance()
        self.assertEqual({key.session_id for key in backend.pending}, {s.session_id for s in sessions})

    def test_split_tick_with_multiple_jobs_and_preparation_keeps_one_budget(self):
        for maximum in (1, 2, 3):
            with self.subTest(maximum=maximum):
                engine, backend, _ = multi_engine(requests=2)
                engine.capacity.limits = replace(engine.capacity.limits, step_actions=maximum)
                preparation = engine.preparation = Preparation()
                handles = [engine.register_job(label, budget()) for label in ("A", "B")]
                sessions = [flow(engine, handle, handle.label) for handle in handles]
                for session in sessions:
                    session.offer((small_task(0),))
                    session.seal()
                delivered = set()
                for _ in range(20):
                    first = engine.advance(dispatch=False)
                    preparation.ready.update(preparation.held)
                    preparation.releasable.update(preparation.held)
                    for session in sessions:
                        progress = session.advance(1)
                        for item in progress.deliveries:
                            delivered.add(item.key.session_id)
                        session.release(tuple(d.lease_id for d in progress.deliveries))
                    if first.events < maximum:
                        second = engine.advance(maximum - first.events)
                        self.assertLessEqual(first.events + second.events, maximum)
                    for key in tuple(backend.pending):
                        backend.complete(key)
                self.assertEqual(delivered, {s.session_id for s in sessions})
                for session, handle in zip(sessions, handles):
                    session.close_consumer(clean=True)
                    engine.close_job(handle)
                self.assertEqual(engine.capacity.usage(), Usage())
                self.assertEqual(engine.jobs.jobs, {})

    def test_dispatch_requires_a_keyword_boolean(self):
        execution, _, _ = fixture()
        for value in (None, 0, 1, "false"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                execution.engine.advance(dispatch=value)
        with self.assertRaises(TypeError):
            execution.engine.advance(1, False)


class CompletionFirstTests(unittest.TestCase):
    def run_chain(self, *, maximum=8, enabled=True, method=None, stop=None, runs=2):
        execution, backend, clock = fixture(step_actions=maximum, active_requests=1,
                                            timeouts=SessionTimeouts())
        drivers, calls, waits = [], [], []
        original = execution.engine.advance

        class CaptureDriver(MethodDriver):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                drivers.append(self)

        def advance(actions=None, *, dispatch=True):
            report = original(actions, dispatch=dispatch)
            calls.append((dispatch, actions, report.events))
            return report

        def wait(generation, timeout):
            waits.append((generation, timeout))
            clock.now += timeout
            # Every other wake is deliberately spurious.
            if len(waits) % 2 == 0 and backend.pending:
                backend.complete(next(iter(backend.pending)), b"OK")
            if len(waits) > 200:
                raise AssertionError("owner loop stopped making progress")

        options = {} if enabled is None else dict(completion_first=enabled)
        iterator = iter_two_map_rows(execution, method or TwoStages(), (b"a", b"b", b"c"),
            query_id="q", operator_id="two-map", limits=LIMITS,
            capacity=MethodCapacity(runs, runs * row_reservation(LIMITS)), **options)
        output = []
        with patch("src.semantic_methods.lotus.driver.MethodDriver", CaptureDriver), \
             patch.object(execution.engine, "advance", advance), \
             patch.object(execution.engine.wake, "wait", wait):
            try:
                for result in iterator:
                    output.append(result)
                    if stop:
                        stop(iterator, drivers[0], execution)
                        break
            finally:
                iterator.close()
        for key in tuple(backend.pending):
            backend.complete(key)
        for _ in range(20):
            original()
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(execution.engine.jobs.jobs, {})
        self.assertEqual(drivers[0].budget._pool.used, (0, 0))
        return output, calls, waits

    def test_default_and_false_have_identical_results_actions_and_waits(self):
        self.assertEqual(self.run_chain(enabled=None), self.run_chain(enabled=False))

    def test_split_tick_never_doubles_small_global_budgets(self):
        for maximum in (1, 2, 3, 8):
            with self.subTest(maximum=maximum):
                output, calls, _ = self.run_chain(maximum=maximum)
                self.assertCountEqual([r.value for r in output], [b"a:OK", b"b:OK", b"c:OK"])
                used = 0
                for dispatch, requested, events in calls:
                    if not dispatch:
                        used = events
                        self.assertEqual(requested, maximum)
                    else:
                        self.assertEqual(requested, maximum - used)
                        used += events
                    self.assertLessEqual(used, maximum)

    def test_result_stays_charged_until_consumer_close(self):
        def close(iterator, driver, execution):
            self.assertEqual(driver.budget.runs, 1)
            self.assertEqual(driver.budget.bytes, row_reservation(LIMITS))
            iterator.close()
        self.run_chain(stop=close, runs=1)

    def test_consumer_exception_and_cancel_do_not_erase_remote_ownership(self):
        def fail(iterator, driver, execution):
            primary = LookupError("consumer fixture")
            with self.assertRaises(LookupError) as raised:
                iterator.throw(primary)
            self.assertIs(raised.exception, primary)
        self.run_chain(stop=fail)
        for enabled in (False, True):
            execution, _, _ = fixture()
            iterator = iter_two_map_rows(execution, TwoStages(), (), query_id="q", operator_id="two",
                limits=LIMITS, capacity=MethodCapacity(1, row_reservation(LIMITS)),
                cancelled=lambda: True, completion_first=enabled)
            with self.assertRaisesRegex(RuntimeError, "cancelled"):
                next(iterator)
            self.assertEqual(execution.engine.jobs.jobs, {})

    def test_merge_keeps_both_events_earliest_deadline_error_and_immediate(self):
        from src.scheduling.core.session_contract import CleanupReport
        from src.semantic_methods.lotus import driver as module
        execution, _, _ = fixture()
        reports = [CleanupReport(1, Usage(1), "first", True, .003),
                   CleanupReport(2, Usage(2), "second", False, .007, "WAIT_BACKEND")]
        captured = []
        def merged(value, **changes):
            result = replace(value, **changes)
            captured.append(result)
            return result
        class ImmediateFinal:
            def start(self, value):
                return Final(value)
        iterator = iter_two_map_rows(execution, ImmediateFinal(), (b"a",), query_id="q",
            operator_id="two", limits=LIMITS, capacity=MethodCapacity(1, row_reservation(LIMITS)),
            completion_first=True)
        with patch.object(execution.engine, "advance", side_effect=reports) as advancing, \
             patch.object(module, "replace", merged):
            self.assertEqual(next(iterator).value, b"a")
            iterator.close()
        self.assertEqual(advancing.call_args_list[1].args, (7,))
        self.assertEqual(captured, [CleanupReport(3, Usage(2), "first", True, .003)])

    def test_flag_validation_precedes_job_allocation(self):
        for value in (1, 0, None, "yes"):
            iterator = iter_two_map_rows(SimpleNamespace(), None, (), query_id="q", operator_id="o",
                limits=LIMITS, capacity=MethodCapacity(1, row_reservation(LIMITS)),
                completion_first=value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                next(iterator)
