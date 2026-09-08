"""Finite independent producer through existing work grouping and session ownership."""

from dataclasses import replace
import unittest

from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.session_contract import Acceptance, State, TaskInfo, TaskKey
from src.scheduling.organization.session_window import WorkWindowOrganizer
from src.semantic_methods.continuation import Continue, Final, MethodLimits, MethodRun, Request
from tests.scheduling.test_incremental_session import setup, task


def described(sequence, work=1, locality="a", stage="generate"):
    return task(
        sequence,
        str(sequence).encode(),
        estimated_work=work,
        info=TaskInfo(
            "map",
            sequence,
            stage,
            WorkDescriptor((StageWork("model", work, "work_units"),), "model", "fixture", locality),
        ),
    )


def organized(**changes):
    options = dict(
        held_tasks=4,
        input_bytes=64,
        result_bytes=16,
        active_requests=4,
        active_work=16,
        metadata_bytes=1024,
        step_actions=16,
    )
    options.update(changes)
    engine, session, backend, clock = setup(**options)
    engine.policies = replace(engine.policies, organize=WorkWindowOrganizer(3, 5, True))
    return engine, session, backend, clock


class OrganizedSessionTests(unittest.TestCase):
    def test_partial_intake_grouping_multiple_requests_and_reverse_row_correlation(self):
        engine, session, backend, _ = organized(held_tasks=3)
        inputs = [described(0, 3), described(1, 1, "b"), described(2, 1), described(3)]
        accepted = session.offer(inputs)
        self.assertEqual(accepted.accepted_prefix_count, 3)
        self.assertFalse(backend.pending)
        session.advance(3)
        keys = tuple(backend.pending)
        self.assertEqual([k.sequence for k in keys], [2, 0, 1])
        submitted = [backend.pending[key][1] for key in keys]
        self.assertEqual(submitted[0].member.batch_key, submitted[1].member.batch_key)
        self.assertEqual([r.member.index for r in submitted[:2]], [0, 1])
        self.assertEqual(submitted[0].member.size, 2)
        self.assertNotEqual(submitted[1].member.batch_key, submitted[2].member.batch_key)
        # An organization batch of two is two requests with this backend, never one credit.
        self.assertEqual(engine.capacity.usage().active_requests, 3)
        self.assertEqual(engine.capacity.usage().active_work, 5)
        for key in reversed(keys):
            backend.complete(key, str(key.sequence).encode())
        deliveries = session.advance(3).deliveries
        self.assertEqual([d.info.row_sequence for d in deliveries], [1, 0, 2])
        self.assertEqual(
            {d.info.row_sequence: d.result for d in deliveries}, {0: b"0", 1: b"1", 2: b"2"}
        )
        self.assertEqual(session.offer(inputs[3:]).accepted_prefix_count, 0)
        session.release(tuple(d.lease_id for d in deliveries))
        self.assertEqual(session.offer(inputs[3:]).accepted_prefix_count, 1)
        session.advance(3)
        backend.complete(TaskKey(session.session_id, 3))
        (delivery,) = session.advance(3).deliveries
        session.release((delivery.lease_id,))
        session.seal()
        self.assertEqual(session.advance(3).state, State.FINISHED)
        self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_group_membership_survives_capacity_and_backend_rejection(self):
        engine, session, backend, clock = organized(active_requests=1)
        session.offer([described(0), described(1)])
        backend.acceptance = Acceptance.NOT_ACCEPTED
        session.advance(4)
        group = engine.capacity.records[TaskKey(0, 0)].member.batch_key
        backend.acceptance = Acceptance.ACCEPTED
        clock.now += 0.2
        session.advance(4)
        backend.complete(TaskKey(0, 0))
        clock.now += 0.2
        (delivered,) = session.advance(4).deliveries
        session.release((delivered.lease_id,))
        self.assertEqual(backend.pending[TaskKey(0, 1)][1].member.batch_key, group)
        self.assertEqual(engine.capacity.usage().active_requests, 1)

    def test_invalid_organization_cannot_dispatch_or_transfer_extra_tasks(self):
        for bad in (lambda w: (w[0].key, w[0].key), lambda w: (TaskKey(99, 0),), lambda w: list(w)):
            engine, session, backend, _ = organized()
            engine.policies = replace(engine.policies, organize=bad)
            session.offer([described(0)])
            with self.assertRaises(ValueError):
                session.advance(4)
            self.assertEqual(session.state, State.FAILED)
            self.assertFalse(backend.pending)
            self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_invalid_metadata_rejects_whole_input_before_transfer(self):
        for change in (
            lambda info: replace(info, row_sequence=True),
            lambda info: replace(info, work=replace(info.work, locality_key="x" * 257)),
            lambda info: replace(
                info, work=replace(info.work, stages=(StageWork("model", 2, "work_units"),))
            ),
            lambda info: replace(
                info, work=replace(info.work, stages=(StageWork("model", 1, "tokens"),))
            ),
        ):
            engine, session, backend, _ = organized()
            second = described(1)
            second = replace(second, info=change(second.info))
            self.assertEqual(session.offer([described(0), second]).status, "REJECTED")
            self.assertEqual(engine.capacity.usage().held_tasks, 0)
            self.assertFalse(backend.pending)
        engine, session, _, _ = organized(metadata_bytes=32)
        self.assertEqual(session.offer([described(0)]).status, "REJECTED")

    def test_member_failure_cancels_remaining_requests_without_early_credit_return(self):
        engine, session, backend, _ = organized()
        session.offer([described(0), described(1)])
        session.advance(4)
        backend.complete(TaskKey(0, 0), status="failed")
        result = session.advance(4)
        self.assertEqual(result.state, State.FAILED)
        self.assertFalse(result.deliveries)
        self.assertEqual(engine.capacity.usage().active_requests, 1)
        session.close()
        backend.complete(TaskKey(0, 1))
        engine.reap(4)
        self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_cancelled_group_late_response_cannot_reach_new_session(self):
        engine, session, backend, _ = organized(active_requests=1)
        session.offer([described(0), described(1)])
        session.advance(4)
        session.cancel()
        self.assertEqual(session.close().uncertain_requests, 1)
        replacement = engine.open(replace(session.spec, job_id="next"))
        backend.complete(TaskKey(0, 0))
        self.assertFalse(replacement.advance(4).deliveries)
        self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_reordered_small_member_reports_immediate_progress(self):
        engine, session, backend, _ = organized(active_work=3, step_actions=1)
        session.offer([described(0, 3), described(1), described(2)])
        result = session.advance(4)
        self.assertEqual([key.sequence for key in backend.pending], [1])
        self.assertTrue(result.has_immediate_work)
        session.advance(4)
        self.assertEqual([key.sequence for key in backend.pending], [1, 2])

    def test_work_target_preserves_complete_oversized_tasks(self):
        engine, session, backend, _ = organized()
        engine.policies = replace(engine.policies, organize=WorkWindowOrganizer(3, 2))
        session.offer([described(0, 3), described(1, 1), described(2, 1)])
        session.advance(4)
        requests = [item[1] for item in backend.pending.values()]
        self.assertEqual([r.task.payload for r in requests], [b"0", b"1", b"2"])
        self.assertEqual([r.member.size for r in requests], [1, 2, 2])

    def test_router_receives_typed_locality_and_work(self):
        engine, session, backend, _ = organized()
        original = engine.policies.router
        seen = []

        class RecordingRouter:
            def route(self, request, topology, pool):
                seen.append(request)
                return original.route(request, topology, pool)

        engine.policies = replace(engine.policies, router=RecordingRouter())
        session.offer([described(0, 3, "prefix")])
        session.advance(4)
        self.assertEqual(seen[0].prefix_key, "prefix")
        self.assertEqual(seen[0].work_descriptor.primary.units, 3)
        self.assertEqual(seen[0].row_count, 1)

    def test_methods_produce_stages_in_the_same_organized_window(self):
        class TwoStage:
            def start(self, value):
                return Continue(Request("model", value, 1, 4), b"again")

            def resume(self, state, result):
                return Continue(Request("model", result, 1, 4)) if state else Final(result)

        from src.scheduling.core.session_contract import TaskProfile

        engine, old, backend, _ = organized()
        old.close()
        session = engine.open(replace(old.spec, task_profiles=(TaskProfile("model", "fixture"),)))
        runs = [MethodRun(TwoStage(), str(i).encode(), MethodLimits(8, 8, 8, 2)) for i in range(2)]
        sequence = 0
        for stage in ("first", "second"):
            offered = []
            for row, run in enumerate(runs):
                info = replace(described(row, stage=stage).info, row_sequence=row)
                offered.append(replace(run.pending.offered(sequence), info=info))
                sequence += 1
            self.assertEqual(session.offer(offered).accepted_prefix_count, 2)
            for run, request in zip(runs, offered):
                run.accepted(TaskKey(session.session_id, request.sequence))
            session.advance(4)
            for key in reversed(tuple(backend.pending)):
                backend.complete(key, b"yes")
            for delivery in session.advance(4).deliveries:
                try:
                    runs[delivery.info.row_sequence].complete(delivery.key, delivery.result)
                finally:
                    session.release((delivery.lease_id,))
        self.assertEqual([run.final.value for run in runs], [b"yes", b"yes"])
        session.seal()
        self.assertEqual(session.advance(4).state, State.FINISHED)
