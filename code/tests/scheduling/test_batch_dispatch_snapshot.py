"""CPU differential replay of atomic reservations; no Ray or model requests.

Disabling the call-local snapshot exercises the original per-member scan path.
All comparisons include rejected-member position, routing state and ownership.
"""

from tests.scheduling.test_multisession import choose_first_flow

from dataclasses import asdict, replace
import random
import threading
import unittest
from unittest.mock import patch

from src.scheduling.core import session_dispatch as dispatch_module
from src.scheduling.core.session_dispatch import _dispatch_batch, _next_record
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_capacity import SessionCapacity
from src.scheduling.core.session_contract import Acceptance, BatchSubmission, SessionSpec, State, Terminal
from src.scheduling.core.session_jobs import JobBudget
from src.scheduling.core.session_policy import SessionPolicies
from src.scheduling.submission_control.admission import StaticAdmissionController
from tests.scheduling.test_incremental_session import setup, task


def fixture(*, requests=64, work=256, admission=64):
    engine, session, backend, clock = setup(
        held_tasks=64, input_bytes=512, result_bytes=512, active_requests=requests,
        active_work=work, offer_tasks=64, metadata_bytes=1024, step_actions=64)
    engine.policies = replace(engine.policies,
        topology=replace(engine.policies.topology, endpoints=(replace(
            engine.policies.topology.endpoints[0], running=3, estimated_active_work=7),)),
        admission=StaticAdmissionController(admission),
        organize=lambda rows: tuple(r.key for r in rows))
    backend.supports_batch_submit = True
    backend.groups = []
    backend.batch_outcome = Acceptance.ACCEPTED
    backend.on_batch = None

    def submit(tasks, endpoint):
        backend.groups.append((tasks, endpoint))
        if backend.on_batch:
            backend.on_batch()
        if isinstance(backend.batch_outcome, Exception):
            raise backend.batch_outcome
        if type(backend.batch_outcome) is not Acceptance:
            return backend.batch_outcome
        if backend.batch_outcome != Acceptance.ACCEPTED:
            return BatchSubmission(backend.batch_outcome)
        replies = tuple(backend.try_submit(t, endpoint).handle for t in tasks)
        return BatchSubmission(Acceptance.ACCEPTED, replies)

    backend.try_submit_batch = submit
    return engine, session, backend, clock


def dispatch(session, now=1.0):
    with session.engine._operation():
        if not session._pending_members:
            _next_record(session, tuple(r for r in session._records() if r.phase == "QUEUED"))
        return _dispatch_batch(session, now)


def state(engine, session, backend):
    return (
        tuple(asdict(r) for r in engine.capacity.records.values()),
        engine.capacity.usage(), session.state, session.error, session._pending_members,
        session._capacity_waiting, session._next_batch,
        dict(engine.policies.router._next_index_by_pool),
        tuple(backend.groups), dict(backend.pending), tuple(backend.cancelled),
        engine.wake.generation,
    )


class CountingRecords(dict):
    def __init__(self, records):
        super().__init__(records)
        self.scans = self.visits = 0

    def values(self):
        self.scans += 1
        for record in super().values():
            self.visits += 1
            yield record


class BatchDispatchSnapshotTests(unittest.TestCase):
    def paired(self, replay):
        with patch.object(dispatch_module, "_batch_dispatch_snapshot", return_value=None) as oracle:
            old = replay()
            self.assertGreater(oracle.call_count, 0)
        new = replay()
        self.assertEqual(new, old)
        return new

    def test_randomized_members_capacity_and_admission_keep_rejection_position(self):
        for seed in range(120):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                works = tuple(rng.randint(1, 8) for _ in range(rng.randint(1, 12)))
                requests, work, admission = rng.randint(1, 16), rng.randint(8, 64), rng.randint(1, 16)
                order = list(range(len(works)))
                rng.shuffle(order)

                def replay():
                    e, s, b, _ = fixture(requests=requests, work=work, admission=admission)
                    s.limits = replace(s.limits, active_requests=max(1, requests - 1),
                                       active_work=max(8, work - 3))
                    e.policies = replace(e.policies, organize=lambda rows: tuple(rows[i].key for i in order))
                    self.assertEqual(s.offer(tuple(task(i, estimated_work=w) for i, w in enumerate(works)))
                                     .accepted_prefix_count, len(works))
                    result = dispatch(s)
                    return result, state(e, s, b)

                self.paired(replay)

    def test_global_session_and_job_scopes_include_other_flows_and_uncertain_work(self):
        for scope in ("global_requests", "session_requests", "job_requests",
                      "global_work", "session_work", "job_work", "fits"):
            with self.subTest(scope=scope):
                def replay():
                    template, previous, backend, clock = fixture()
                    previous.close()
                    e = SessionEngine(template.capacity.limits, backend, template.policies,
                                      clock=clock, max_jobs=2, choose_flow=choose_first_flow)
                    budget = JobBudget(32, 256, 256, 64, 256, 2)
                    a = e.register_job("a", replace(budget,
                        active_requests=3 if scope == "job_requests" else 64,
                        active_work=7 if scope == "job_work" else 256))
                    b = e.register_job("b", budget)
                    s = e.open(SessionSpec("ignored", "first", "fixture"), job=a)
                    sibling = e.open(SessionSpec("ignored", "second", "fixture"), job=a)
                    foreign = e.open(SessionSpec("ignored", "foreign", "fixture"), job=b)
                    for owner in (s, sibling, foreign):
                        owner.offer((task(0, estimated_work=2),))
                        record = owner._records()[0]
                        record.compute, record.endpoint, record.phase = True, "e1", "UNCERTAIN"
                        record.since = -2.0
                    s.offer((task(1, estimated_work=2), task(2, estimated_work=2)))
                    if scope == "global_requests":
                        e.capacity.limits = replace(e.capacity.limits, active_requests=4)
                    elif scope == "session_requests":
                        s.limits = replace(s.limits, active_requests=2)
                    elif scope == "global_work":
                        e.capacity.limits = replace(e.capacity.limits, active_work=9)
                    elif scope == "session_work":
                        s.limits = replace(s.limits, active_work=5)
                    result = dispatch(s)
                    self.assertEqual(result, scope == "fits")
                    if not result:
                        self.assertEqual(s._capacity_waiting.sequence, 2)
                    return result, state(e, s, backend)

                self.paired(replay)

    def test_backend_rejection_retry_unknown_exception_and_malformed_outcomes(self):
        outcomes = (Acceptance.NOT_ACCEPTED, Acceptance.UNKNOWN, RuntimeError("fixture"),
                    BatchSubmission(Acceptance.ACCEPTED, ("missing-members",)),
                    BatchSubmission(Acceptance.NOT_ACCEPTED, ("unexpected",)), object())
        for outcome in outcomes:
            with self.subTest(outcome=type(outcome).__name__):
                def replay():
                    e, s, b, clock = fixture()
                    s.offer(tuple(task(i) for i in range(3)))
                    clock.now = 1.0
                    b.batch_outcome = outcome
                    result = dispatch(s)
                    before = state(e, s, b)
                    if outcome == Acceptance.NOT_ACCEPTED:
                        self.assertFalse(result)
                        self.assertEqual(e.capacity.usage().active_requests, 0)
                        self.assertTrue(all(r.since == 0 for r in s._records()))
                        b.batch_outcome = Acceptance.ACCEPTED
                        self.assertTrue(dispatch(s, 2.0))
                    else:
                        self.assertTrue(result)
                        self.assertEqual(s.state, State.FAILED)
                        self.assertEqual(e.capacity.usage().active_requests, 3)
                        s.close()
                        self.assertEqual(e.capacity.usage().active_requests, 3)
                    return before, state(e, s, b)

                self.paired(replay)

    def test_completions_deliver_out_of_order_and_next_call_rebuilds_snapshot(self):
        def replay():
            e, s, b, clock = fixture(requests=3)
            s.offer(tuple(task(i) for i in range(3)))
            self.assertTrue(dispatch(s))
            observed = [state(e, s, b)]
            for sequence in (2, 0, 1):
                key = next(key for key in b.pending if key.sequence == sequence)
                b.complete(key, bytes([sequence]))
                clock.now += .1
                result = s.advance(3)
                self.assertEqual(tuple(d.key for d in result.deliveries), (key,))
                s.release(tuple(d.lease_id for d in result.deliveries))
                observed.append((result, state(e, s, b)))
            s.offer((task(3, estimated_work=3), task(4, estimated_work=2)))
            self.assertTrue(dispatch(s))
            observed.append(state(e, s, b))
            return observed

        self.paired(replay)

    def test_pending_completion_cannot_reenter_owner_during_batch_handoff(self):
        def replay():
            e, s, b, _ = fixture()
            s.offer((task(0), task(1)))
            errors = []

            def callback():
                for operation in (lambda: s.offer((task(2),)), lambda: s.advance(1)):
                    try:
                        operation()
                    except RuntimeError as error:
                        errors.append(str(error))
                # Completion is only queued here; the owner settles it on the next tick.
                key = s._pending_members[0]
                b.events.append(Terminal(key, f"h{key.session_id}:{key.sequence}", "completed", b"ok"))
                self.assertEqual(e.capacity.usage().active_requests, 2)

            b.on_batch = callback
            self.assertTrue(dispatch(s))
            self.assertEqual(len(errors), 2)
            b.pending.pop(b.events[0].key)
            before = state(e, s, b)
            result = s.advance(2)
            self.assertEqual(len(result.deliveries), 1)
            self.assertEqual(e.capacity.usage().active_requests, 1)
            return errors, before, result, state(e, s, b)

        self.paired(replay)

    def test_cancellation_flag_from_another_thread_rolls_back_unsent_reservations(self):
        def replay():
            e, s, b, _ = fixture()
            s.offer((task(0), task(1)))
            worker = threading.Thread(target=s.request_cancel)
            worker.start()
            worker.join()
            self.assertFalse(dispatch(s))
            self.assertEqual(e.capacity.usage().active_requests, 0)
            self.assertFalse(b.groups)
            return state(e, s, b)

        self.paired(replay)

    def test_custom_policies_and_general_routes_use_original_path(self):
        for kind in ("admission", "router", "pool", "multi_endpoint", "policy", "capacity"):
            with self.subTest(kind=kind):
                def replay():
                    e, s, b, _ = fixture()
                    calls = []
                    policy = e.policies
                    if kind == "admission":
                        class CustomAdmission:
                            limit = 2
                            def decide(self, inflight, *, hol_age_s=None):
                                calls.append((inflight, hol_age_s))
                                return StaticAdmissionController(self.limit).decide(inflight)
                        e.policies = replace(policy, admission=CustomAdmission())
                    elif kind == "router":
                        class CustomRouter:
                            def route(self, request, topology, pool):
                                calls.append((request, topology))
                                return policy.router.route(request, topology, pool)
                        e.policies = replace(policy, router=CustomRouter())
                    elif kind == "pool":
                        from src.scheduling.core.models import PoolRoutingDecision
                        class CustomPool:
                            def route(self, request, topology):
                                calls.append((request, topology))
                                return PoolRoutingDecision("default", "fixture")
                        e.policies = replace(policy, pool_router=CustomPool())
                    elif kind == "multi_endpoint":
                        endpoint = replace(policy.topology.endpoints[0], endpoint_id="e2")
                        e.policies = replace(policy, topology=replace(policy.topology,
                            endpoints=policy.topology.endpoints + (endpoint,)))
                    elif kind == "policy":
                        class CustomPolicy(SessionPolicies):
                            def select(self, record, active, now):
                                calls.append(tuple(r.key for r in active))
                                return super().select(record, active, now)
                        e.policies = CustomPolicy(**vars(policy))
                    else:
                        class CustomCapacity(SessionCapacity):
                            def can_dispatch(self, record, limits):
                                calls.append(record.key)
                                return super().can_dispatch(record, limits)
                        e.capacity = CustomCapacity(e.capacity.limits)
                    s.offer(tuple(task(i) for i in range(3)))
                    _next_record(s, s._records())
                    self.assertIsNone(dispatch_module._batch_dispatch_snapshot(
                        e.capacity, e.policies, session_id=s.session_id, limits=s.limits,
                        job_id=s.spec.job_id, member_count=len(s._pending_members), now=1.0))
                    try:
                        result = dispatch(s)
                    except ValueError as error:
                        result = str(error)
                    # Compare the wrapped router's real state in the standard state projection.
                    e.policies = replace(e.policies, router=policy.router)
                    return result, calls, state(e, s, b)

                self.paired(replay)

    def test_unavailable_or_unhealthy_endpoint_preserves_rejection_and_error(self):
        for change in ({"available": False}, {"healthy": False}, {"pool_id": "other"}):
            with self.subTest(change=change):
                def replay():
                    e, s, b, _ = fixture()
                    topology = e.policies.topology
                    e.policies = replace(e.policies, topology=replace(topology,
                        endpoints=(replace(topology.endpoints[0], **change),)))
                    s.offer((task(0), task(1)))
                    try:
                        result = dispatch(s)
                    except RuntimeError as error:
                        result = str(error)
                    self.assertFalse(b.groups)
                    self.assertEqual(e.capacity.usage().active_requests, 0)
                    return result, state(e, s, b)

                self.paired(replay)

    def test_capacity_table_scans_are_constant_per_batch(self):
        scans = {}
        snapshot = dispatch_module._batch_dispatch_snapshot
        for fast in (False, True):
            e, s, _, _ = fixture()
            s.offer(tuple(task(i) for i in range(32)))
            _next_record(s, s._records())
            e.capacity.records = CountingRecords(e.capacity.records)
            with patch.object(dispatch_module, "_batch_dispatch_snapshot",
                              side_effect=snapshot if fast else None,
                              return_value=None) as projection:
                self.assertTrue(dispatch(s))
                self.assertEqual(projection.call_count, 1)
            scans[fast] = e.capacity.records.scans
        self.assertEqual(scans[False], 32 * 3)
        self.assertEqual(scans[True], 3)


if __name__ == "__main__":
    unittest.main()
