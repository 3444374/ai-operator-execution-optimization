"""Deterministic ownership/capacity tests; the backend never starts model work."""

from __future__ import annotations

import threading
import unittest
from unittest.mock import patch
from dataclasses import replace

from src.scheduling.core.session import SessionEngine, WakeSignal
from src.scheduling.core.session_contract import (
    Acceptance,
    LeaseId,
    OfferedTask,
    SessionLimits,
    SessionSpec,
    SessionTimeouts,
    State,
    Submission,
    TaskKey,
    Terminal,
)
from src.scheduling.core.session_policy import SessionPolicies
from src.scheduling.core.models import EndpointSnapshot, TopologySnapshot
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.submission_control.admission import StaticAdmissionController


class Clock:
    now = 0.0

    def __call__(self):
        return self.now


class Backend:
    def __init__(self):
        self.pending = {}
        self.events = []
        self.routes = []
        self.cancelled = []
        self.acceptance = Acceptance.ACCEPTED
        self.on_submit = None

    def try_submit(self, task, endpoint):
        self.routes.append(endpoint)
        if self.on_submit:
            self.on_submit()
        if self.acceptance == Acceptance.NOT_ACCEPTED:
            return Submission(self.acceptance)
        handle = f"h{task.key.session_id}:{task.key.sequence}"
        self.pending[task.key] = (handle, task)
        return Submission(self.acceptance, handle)

    def poll(self, handles, max_events):
        events, self.events = self.events[:max_events], self.events[max_events:]
        return tuple(events)

    def request_cancel(self, key, handle):
        self.cancelled.append(key)

    def complete(self, key, result=b"done", status="completed"):
        handle, task = self.pending.pop(key)
        self.events.append(Terminal(key, handle, status, result))


def limits(**changes):
    return replace(SessionLimits(2, 8, 8, 1, 2, 4, 8, 8, 64, 8, 10.0, 0.1), **changes)


def task(sequence, payload=b"data", **changes):
    return replace(OfferedTask(sequence, payload, 1, 4), **changes)


def setup(**changes):
    backend, clock = Backend(), Clock()
    endpoints = tuple(
        EndpointSnapshot(e, f"http://localhost/{e}", "default", "0", True, 0, 0, 0.0, 1.0)
        for e in ("e1", "e2")
    )
    policy = SessionPolicies(
        StaticAdmissionController(8),
        RoundRobinEndpointRouter(),
        TopologySnapshot(endpoints, 0.0),
        "default",
    )
    engine = SessionEngine(limits(**changes), backend, policy, clock=clock)
    session = engine.open(SessionSpec("job", "flow", "fixture"))
    return engine, session, backend, clock


class IncrementalSessionTests(unittest.TestCase):
    def test_queue_does_not_inherit_backend_timeout(self):
        e, s, b, clock = setup(
            held_tasks=3,
            input_bytes=12,
            result_bytes=12,
            wait_timeout_s=11,
            timeouts=SessionTimeouts(backend_s=11),
        )
        self.assertEqual(s.offer([task(i) for i in range(3)]).accepted_prefix_count, 3)
        s.advance(3)
        for sequence, now in enumerate((8, 16, 24)):
            clock.now = now
            b.complete(TaskKey(0, sequence))
            result = s.advance(3)
            self.assertEqual(result.state, State.OPEN)
            self.assertEqual([d.key.sequence for d in result.deliveries], [sequence])
            s.release([result.deliveries[0].lease_id])
            if sequence == 0:
                clock.now = 11
                self.assertEqual(s.advance(3).state, State.OPEN)
        self.assertEqual(e.capacity.usage().held_tasks, 0)

    def test_phase_deadlines_keep_their_own_start_and_resource_ownership(self):
        for phase, policy, reason in (
            ("queue", SessionTimeouts(queue_s=3, backend_s=20), "capacity wait timed out"),
            ("backend", SessionTimeouts(backend_s=3), "backend wait timed out"),
            ("consumer", SessionTimeouts(consumer_s=3), "consumer release timed out"),
        ):
            with self.subTest(phase=phase):
                e, s, b, clock = setup(timeouts=policy)
                s.offer([task(0), task(1)])
                s.advance(2)
                if phase == "consumer":
                    clock.now = 2
                    b.complete(TaskKey(0, 0))
                    delivery = s.advance(2).deliveries[0]
                    clock.now = 4
                    self.assertEqual(s.advance(2).state, State.OPEN)
                    clock.now = 5
                else:
                    clock.now = 3
                result = s.advance(2)
                self.assertEqual(result.state, State.FAILED)
                self.assertEqual(result.error, reason)
                self.assertGreater(e.capacity.usage().held_tasks, 0)
                if phase == "consumer":
                    s.release([delivery.lease_id])

    def test_prefix_transfer_release_backpressure_and_no_eager_dispatch(self):
        e, s, b, _ = setup()
        r = s.offer([task(i) for i in range(3)])
        self.assertEqual((r.status, r.accepted_prefix_count), ("BACKPRESSURE", 2))
        self.assertFalse(b.pending)
        self.assertEqual(s.offer([task(2)]).accepted_prefix_count, 0)
        s.advance(2)
        b.complete(TaskKey(0, 0))
        result = s.advance(2)
        self.assertEqual(len(result.deliveries), 1)
        self.assertEqual(e.capacity.usage().held_tasks, 2)
        self.assertEqual(s.offer([task(2)]).accepted_prefix_count, 0)
        s.release([result.deliveries[0].lease_id])
        s.release([result.deliveries[0].lease_id])
        self.assertEqual(s.offer([task(2)]).accepted_prefix_count, 1)
        self.assertLessEqual(e.capacity.usage().result_bytes, 8)

    def test_whole_batch_permanent_validation(self):
        for bad in (
            task(1, b"x" * 9),
            task(3),
            task(1, bytearray(b"x")),
            task(1, estimated_work=3),
            task(1, max_result_bytes=9),
            task(1, metadata=b"x" * 65),
            task(True),
            task(1, estimated_work=True),
        ):
            with self.subTest(bad=bad):
                e, s, b, _ = setup()
                self.assertEqual(s.offer([task(0), bad]).status, "REJECTED")
                self.assertEqual(e.capacity.usage().held_tasks, 0)
                self.assertEqual(s.offer([task(0)]).accepted_prefix_count, 1)
        self.assertEqual(s.offer(iter([task(1)])).status, "REJECTED")

    def test_unsealed_idle_and_sealed_finished(self):
        e, s, b, clock = setup()
        clock.now = 100
        self.assertEqual(s.advance(1).state, State.OPEN)
        self.assertEqual(s.advance(1).blocked_reason, "NEED_INPUT")
        s.offer([task(0)])
        s.advance(1)
        b.complete(TaskKey(0, 0))
        d = s.advance(1).deliveries[0]
        s.release([d.lease_id])
        self.assertEqual(s.advance(1).state, State.OPEN)
        s.seal()
        s.seal()
        self.assertEqual(s.offer([]).status, "REJECTED")
        self.assertEqual(s.advance(1).state, State.FINISHED)
        report = s.close()
        self.assertEqual(report.status, "CLOSED")
        self.assertEqual(s.close(), report)

    def test_reverse_completions_and_identical_payloads(self):
        e, s, b, _ = setup(active_requests=2)
        s.offer([task(0), task(1)])
        s.advance(2)
        b.complete(TaskKey(0, 1))
        b.complete(TaskKey(0, 0))
        r = s.advance(2)
        self.assertEqual([d.key.sequence for d in r.deliveries], [1, 0])
        self.assertEqual(b.routes, ["e1", "e2"])
        self.assertEqual(s.offer([task(1)]).status, "REJECTED")

    def test_cancel_keeps_leases_and_unknown_compute(self):
        e, s, b, _ = setup(held_tasks=4, input_bytes=16, result_bytes=16, active_requests=2)
        s.offer([task(i) for i in range(4)])
        s.advance(2)
        b.complete(TaskKey(0, 0))
        d = s.advance(1).deliveries[0]
        s.cancel()
        s.cancel()
        s.advance(1)
        report = s.close()
        self.assertEqual(report.status, "WAITING_FOR_RELEASE")
        self.assertEqual(len(set(b.cancelled)), len(b.cancelled))
        self.assertGreater(report.uncertain_requests, 0)
        s.release([d.lease_id])
        report = s.close()
        self.assertEqual(report.status, "CLOSED")
        self.assertGreater(e.capacity.usage().active_requests, 0)

    def test_old_session_terminal_reaps_without_entering_new_session(self):
        e, a, b, _ = setup()
        a.offer([task(0)])
        a.advance(1)
        a.close()
        second = e.open(SessionSpec("other-job", "flow", "fixture"))
        second.offer([task(0)])
        self.assertFalse(second.advance(1).deliveries)
        self.assertEqual(len(b.routes), 1)
        b.complete(TaskKey(0, 0))
        second.advance(1)
        # Capacity denial is retried on the finite deadline.
        e.clock.now = 0.2
        second.advance(1)
        b.complete(TaskKey(1, 0), b"next")
        r = second.advance(1)
        self.assertEqual(r.deliveries[0].key.session_id, 1)
        self.assertEqual(r.deliveries[0].result, b"next")

    def test_unknown_submit_exception_retains_no_handle_reservation(self):
        e, s, b, _ = setup()

        def fail():
            raise RuntimeError("private backend diagnostic")

        b.on_submit = fail
        s.offer([task(0)])
        self.assertEqual(s.advance(1).state, State.FAILED)
        self.assertEqual(s.close().uncertain_requests, 1)
        self.assertEqual(e.capacity.usage().active_work, 1)
        self.assertNotIn("private", s.error)

    def test_not_accepted_does_not_reset_capacity_deadline(self):
        e, s, b, clock = setup()
        b.acceptance = Acceptance.NOT_ACCEPTED
        s.offer([task(0)])
        s.advance(1)
        self.assertEqual(e.capacity.usage().active_requests, 0)
        self.assertFalse(s.advance(1).has_immediate_work)
        clock.now = 9
        s.advance(1)
        clock.now = 10
        self.assertEqual(s.advance(1).error, "capacity wait timed out")

    def test_backend_timeout_retains_compute(self):
        e, s, b, clock = setup()
        s.offer([task(0)])
        s.advance(1)
        clock.now = 10
        self.assertEqual(s.advance(1).error, "backend wait timed out")
        self.assertEqual(e.capacity.usage().active_requests, 1)

    def test_consumer_timeout_does_not_discard_lease(self):
        e, s, b, clock = setup()
        s.offer([task(0)])
        s.advance(1)
        b.complete(TaskKey(0, 0))
        d = s.advance(1).deliveries[0]
        clock.now = 10
        self.assertEqual(s.advance(1).error, "consumer release timed out")
        self.assertEqual(s.close().status, "WAITING_FOR_RELEASE")
        s.release([d.lease_id])
        self.assertEqual(s.close().status, "CLOSED")

    def test_unknown_duplicate_and_conflicting_events_fail_closed(self):
        for kind in ("unknown", "duplicate", "handle"):
            with self.subTest(kind=kind):
                e, s, b, _ = setup()
                s.offer([task(0)])
                s.advance(1)
                key = TaskKey(0, 0)
                b.complete(key)
                if kind == "duplicate":
                    b.events.append(b.events[0])
                elif kind == "unknown":
                    b.events[0] = replace(b.events[0], key=TaskKey(88, 0))
                else:
                    b.events[0] = replace(b.events[0], handle="wrong")
                r = s.advance(1)
                self.assertEqual(r.state, State.FAILED)
                self.assertFalse(r.deliveries)
                self.assertGreaterEqual(e.capacity.usage().active_requests, 0)

    def test_oversized_completion_fails_after_terminal_settlement(self):
        e, s, b, _ = setup()
        s.offer([task(0)])
        s.advance(1)
        b.complete(TaskKey(0, 0), b"x" * 5)
        self.assertEqual(s.advance(1).state, State.FAILED)
        self.assertEqual(e.capacity.usage().active_requests, 0)

    def test_bad_release_is_atomic_and_cross_session_rejected(self):
        e, s, b, _ = setup()
        s.offer([task(0)])
        s.advance(1)
        b.complete(TaskKey(0, 0))
        d = s.advance(1).deliveries[0]
        for lease in (LeaseId(0, 1), LeaseId(1, 0)):
            with self.assertRaises(ValueError):
                s.release([d.lease_id, lease])
            self.assertEqual(e.capacity.usage().held_tasks, 1)

    def test_sink_failure_publishes_no_unobservable_lease(self):
        e, s, b, _ = setup()
        s.offer([task(0)])
        s.advance(1)
        b.complete(TaskKey(0, 0))

        def sink(*args):
            raise RuntimeError("private sink detail")

        e.sink = sink
        r = s.advance(1)
        self.assertEqual(r.state, State.FAILED)
        self.assertFalse(r.deliveries)
        self.assertEqual(s.close().status, "CLOSED")

    def test_reentrant_backend_is_rejected_before_nested_offer(self):
        e, s, b, _ = setup()
        b.on_submit = lambda: s.offer([task(1)])
        s.offer([task(0)])
        self.assertEqual(s.advance(1).state, State.FAILED)
        self.assertEqual(e.capacity.usage().held_tasks, 1)

    def test_other_thread_only_requests_cancel(self):
        e, s, b, _ = setup()
        s.offer([task(0)])
        thread = threading.Thread(target=s.request_cancel)
        thread.start()
        thread.join()
        self.assertEqual(s.advance(1).state, State.CANCELLED)
        self.assertFalse(b.routes)

    def test_generation_prevents_lost_wakeup(self):
        wake = WakeSignal()
        generation = wake.generation
        wake.notify()
        self.assertNotEqual(wake.wait(generation, 0), generation)

    def test_one_action_steps_eventually_deliver_and_finish(self):
        e, s, b, _ = setup(step_actions=1)
        s.offer([task(0)])
        s.seal()
        s.advance(1)
        b.complete(TaskKey(0, 0))
        first = s.advance(1)
        self.assertTrue(first.has_immediate_work)
        second = s.advance(1)
        self.assertEqual(len(second.deliveries), 1)
        s.release([second.deliveries[0].lease_id])
        self.assertEqual(s.advance(1).state, State.FINISHED)

    def test_long_stream_has_no_history_table(self):
        e, s, b, _ = setup()
        for sequence in range(1000):
            self.assertEqual(s.offer([task(sequence)]).accepted_prefix_count, 1)
            s.advance(1)
            b.complete(TaskKey(0, sequence))
            delivery = s.advance(1).deliveries[0]
            s.release([delivery.lease_id])
            self.assertEqual(len(e.capacity.records), 0)
        self.assertEqual(s.advance(1).state, State.OPEN)

    def test_delivery_construction_failure_preserves_only_returned_leases(self):
        e, s, b, _ = setup(active_requests=2)
        s.offer([task(0), task(1)])
        s.advance(2)
        b.complete(TaskKey(0, 0))
        old = s.advance(1).deliveries[0]
        b.complete(TaskKey(0, 1))
        with patch("src.scheduling.core.session.Delivery", side_effect=MemoryError):
            with self.assertRaises(MemoryError):
                s.advance(1)
        self.assertEqual(s.close().status, "WAITING_FOR_RELEASE")
        s.release([old.lease_id])
        self.assertEqual(s.close().status, "CLOSED")

    def test_existing_local_credit_waits_for_remote_terminal_before_finish(self):
        from src.scheduling.submission_control.shared_credit import FairEndpointCreditCoordinator

        e, s, b, _ = setup()
        credit = FairEndpointCreditCoordinator(
            {"e1": (1, 2), "e2": (1, 2)}, quantum=1, policy="fifo"
        )
        e.credit = credit
        s.offer([task(0)])
        s.advance(1)
        s.close()
        self.assertEqual(credit.snapshot("e1").active_requests, 1)
        self.assertIn("job", e._retired_jobs)
        b.complete(TaskKey(0, 0), status="cancelled")
        self.assertEqual(e.reap(1).usage.active_requests, 0)
        self.assertEqual(credit.snapshot("e1").active_requests, 0)
        self.assertNotIn("job", e._retired_jobs)

    def test_not_accepted_returns_existing_credit_and_preserves_task(self):
        from src.scheduling.submission_control.shared_credit import FairEndpointCreditCoordinator

        e, s, b, _ = setup()
        e.credit = FairEndpointCreditCoordinator(
            {"e1": (1, 2), "e2": (1, 2)}, quantum=1, policy="fifo"
        )
        b.acceptance = Acceptance.NOT_ACCEPTED
        s.offer([task(0)])
        s.advance(1)
        self.assertEqual(e.credit.snapshot("e1").active_requests, 0)
        self.assertEqual(e.capacity.usage().held_tasks, 1)
        self.assertEqual(s.close().status, "CLOSED")

    def test_legacy_and_incremental_share_routing_decisions(self):
        from tests.scheduling.test_scheduler import FakeSubmissionAdapter, envelope, topology
        from src.scheduling.core.scheduler import SynchronousScheduler

        adapter = FakeSubmissionAdapter()
        legacy = SynchronousScheduler(
            StaticAdmissionController(1), RoundRobinEndpointRouter(), adapter, "default"
        ).run([envelope(i) for i in range(4)], topology())
        e, s, b, _ = setup()
        results = []
        for i in range(4):
            s.offer([task(i)])
            s.advance(1)
            b.complete(TaskKey(0, i))
            d = s.advance(1).deliveries[0]
            results.append(d.key.sequence)
            s.release([d.lease_id])
        self.assertEqual(b.routes, [endpoint for _, endpoint in adapter.submitted])
        self.assertEqual(results, [0, 1, 2, 3])
        self.assertEqual([c.request_id for c in legacy.completions], ["r0", "r1", "r2", "r3"])
        self.assertEqual(legacy.operator_invocations, 4)
        self.assertEqual(legacy.max_inflight_seen, 1)

    def test_duplicate_after_last_release_is_detected_by_reap(self):
        e, s, b, _ = setup()
        s.offer([task(0)])
        s.advance(1)
        b.complete(TaskKey(0, 0))
        duplicate = b.events[0]
        d = s.advance(1).deliveries[0]
        s.release([d.lease_id])
        b.events.append(duplicate)
        self.assertEqual(e.reap(1).error, "backend terminal identity conflict")
        self.assertEqual(s.state, State.FAILED)

    def test_poll_only_backend_supplies_finite_next_deadline(self):
        e, s, b, clock = setup()
        s.offer([task(0)])
        s.advance(1)
        r = s.advance(1)
        self.assertFalse(r.has_immediate_work)
        self.assertEqual(r.blocked_reason, "WAIT_BACKEND")
        self.assertEqual(r.next_deadline, clock.now + 0.1)

    def test_retired_local_fifo_credit_does_not_accumulate_job_history(self):
        from src.scheduling.submission_control.shared_credit import FairEndpointCreditCoordinator

        e, s, b, _ = setup()
        e.credit = FairEndpointCreditCoordinator(
            {"e1": (1, 2), "e2": (1, 2)}, quantum=1, policy="fifo"
        )
        for i in range(100):
            if i:
                s = e.open(SessionSpec(f"job-{i}", "flow", "fixture"))
            s.offer([task(0)])
            s.advance(1)
            b.complete(TaskKey(i, 0))
            d = s.advance(1).deliveries[0]
            s.release([d.lease_id])
            s.close()
            self.assertFalse(e.credit._finished_jobs)
            self.assertFalse(e.credit._weights)
            self.assertTrue(all(not jobs for jobs in e.credit._granted_requests.values()))

    def test_retirement_cannot_erase_live_credit_or_legacy_history(self):
        from src.scheduling.submission_control.shared_credit import FairEndpointCreditCoordinator

        credit = FairEndpointCreditCoordinator({"e1": (1, 2)}, quantum=1, policy="fifo")
        credit.try_acquire(request_id="r", job_id="j", endpoint_id="e1", estimated_work=1)
        with self.assertRaises(ValueError):
            credit.forget_finished_job("j")
        credit.release("r", job_id="j")
        credit.finish_job("j")
        self.assertIn("j", credit._finished_jobs)
        credit.forget_finished_job("j")
        self.assertNotIn("j", credit._finished_jobs)

    def test_boolean_terminal_identity_cannot_alias_integer_key(self):
        e, s, b, _ = setup()
        s.offer([task(0)])
        s.advance(1)
        b.events.append(Terminal(TaskKey(False, 0), "h0:0", "completed", b"done"))
        self.assertEqual(s.advance(1).state, State.FAILED)
        self.assertEqual(e.capacity.usage().active_requests, 1)

    def test_smaller_session_limits_and_single_session_ownership(self):
        e, s, b, _ = setup()
        with self.assertRaises(RuntimeError):
            e.open(SessionSpec("other", "flow", "fixture"))
        s.close()
        with self.assertRaises(ValueError):
            e.open(SessionSpec("other", "flow", "fixture"), limits(held_tasks=3))
        smaller = e.open(SessionSpec("other", "flow", "fixture"), limits(held_tasks=1))
        self.assertEqual(smaller.offer([task(0), task(1)]).accepted_prefix_count, 1)

    def test_binary_execution_preserves_payload_and_neutral_work_identity(self):
        e, s, b, _ = setup()
        s.close()
        observed = []
        delegate = e.policies.router

        class Router:
            def route(self, request, topology, pool_id):
                observed.append(request)
                return delegate.route(request, topology, pool_id)

        e.policies = replace(e.policies, router=Router())
        s = e.open(SessionSpec("image-job", "flow", "fixture", "ai_embed", "frames"))
        payload = b"\x00\xff\x80\x01"
        s.offer([task(0, payload)])
        s.advance(1)
        submitted = b.pending[TaskKey(1, 0)][1]
        self.assertIs(submitted.task.payload, payload)
        self.assertEqual((observed[0].operator, observed[0].work_unit), ("ai_embed", "frames"))
        self.assertEqual(observed[0].estimated_work_units, 1)
        b.complete(TaskKey(1, 0), payload)
        self.assertEqual(s.advance(1).deliveries[0].result, payload)

    def test_partial_delivery_commit_rolls_back_unreturned_leases(self):
        from src.scheduling.core.session_capacity import TaskRecord

        e, s, b, _ = setup(active_requests=2)
        s.offer([task(0), task(1)])
        s.advance(2)
        b.complete(TaskKey(0, 0))
        b.complete(TaskKey(0, 1))
        original = TaskRecord.__setattr__
        count = 0

        def fail_second(record, name, value):
            nonlocal count
            if name == "phase" and value == "LEASED":
                count += 1
                if count == 2:
                    raise MemoryError("synthetic commit failure")
            original(record, name, value)

        with patch.object(TaskRecord, "__setattr__", fail_second):
            with self.assertRaises(MemoryError):
                s.advance(2)
        self.assertEqual(s._next_lease, 0)
        self.assertEqual(s.close().status, "CLOSED")

    def test_task_selection_changes_order_without_changing_ownership(self):
        e, s, b, _ = setup(active_requests=2)
        e.policies = replace(e.policies, choose_task=lambda items: items[-1].key)
        s.offer([task(0), task(1)])
        s.advance(2)
        self.assertEqual(list(b.pending), [TaskKey(0, 1), TaskKey(0, 0)])
        self.assertEqual(e.capacity.usage().active_requests, 2)
        self.assertEqual(e.capacity.usage().held_tasks, 2)

    def test_selector_cannot_dispatch_unaccepted_task(self):
        e, s, b, _ = setup()
        e.policies = replace(e.policies, choose_task=lambda items: TaskKey(0, 999))
        s.offer([task(0)])
        with self.assertRaises(ValueError):
            s.advance(1)
        self.assertFalse(b.routes)
        self.assertEqual(s.state, State.FAILED)
        self.assertEqual(s.close().usage.held_tasks, 0)


if __name__ == "__main__":
    unittest.main()
