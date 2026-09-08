"""Method continuations use the same session accounting across capability changes."""

from dataclasses import replace
import unittest

from src.semantic_methods.continuation import (
    Continue,
    Final,
    MethodLimits,
    MethodRun,
    Request,
)
from src.scheduling.core.session_contract import SessionSpec, State, TaskKey, TaskProfile
from tests.scheduling.test_incremental_session import setup, task


class TwoStage:
    def start(self, value):
        return Continue(Request("proxy", value, 1, 4), b"next")

    def resume(self, state, result):
        if state == b"next":
            return Continue(Request("judge", result, 1, 4))
        return Final(result)


def method_run(method=None, **changes):
    return MethodRun(method or TwoStage(), b"row", replace(MethodLimits(8, 8, 8, 2), **changes))


class MethodContinuationTests(unittest.TestCase):
    def test_two_stages_share_one_slot_and_preserve_effective_capability(self):
        engine, old, backend, _ = setup(held_tasks=1)
        old.close()
        session = engine.open(
            SessionSpec(
                "job",
                "flow",
                "default",
                task_profiles=(
                    TaskProfile("proxy", "embedding-model", "ai_embed"),
                    TaskProfile("judge", "generation-model"),
                ),
            )
        )
        run = method_run()
        for seq, capability in enumerate(("embedding-model", "generation-model")):
            self.assertIsNotNone(run.pending)
            request = run.pending.offered(seq)
            self.assertEqual(session.offer((request,)).accepted_prefix_count, 1)
            key = TaskKey(session.session_id, seq)
            run.accepted(key)
            self.assertIsNone(run.pending)
            session.advance(1)
            submitted = backend.pending[key][1]
            self.assertEqual(submitted.spec.capability, capability)
            self.assertEqual(submitted.spec.operator, ("ai_embed", "ai_complete")[seq])
            self.assertEqual(submitted.spec.work_unit, "work_units")
            backend.complete(key, b"yes")
            (delivery,) = session.advance(1).deliveries
            try:
                run.complete(delivery.key, delivery.result)
                if run.pending:
                    self.assertEqual(
                        session.offer((run.pending.offered(seq + 1),)).status, "BACKPRESSURE"
                    )
            finally:
                session.release((delivery.lease_id,))
            self.assertEqual(engine.capacity.usage().held_tasks, 0)
        self.assertEqual(run.final, Final(b"yes"))
        session.seal()
        self.assertEqual(session.advance(1).state, State.FINISHED)
        session.close()

    def test_unknown_profile_rejects_entire_batch_and_default_still_works(self):
        engine, session, _, _ = setup()
        self.assertEqual(
            session.offer((task(0), task(1, profile_name="missing"))).status, "REJECTED"
        )
        self.assertEqual(engine.capacity.usage().held_tasks, 0)
        self.assertEqual(session.offer((task(0),)).accepted_prefix_count, 1)

    def test_profile_registry_is_immutable_unique_and_bounded(self):
        profile = TaskProfile("proxy", "model")
        for profiles in ([profile], (profile, profile), (None,), (profile,) * 33):
            with self.subTest(profiles=profiles), self.assertRaises(ValueError):
                SessionSpec("job", "flow", "default", task_profiles=profiles)

    def test_zero_and_one_request_methods(self):
        class Early:
            def start(self, value):
                return Final(value)

        self.assertEqual(method_run(Early()).final, Final(b"row"))

        class One(TwoStage):
            def resume(self, state, result):
                return Final(result)

        run = method_run(One())
        run.accepted(TaskKey(0, 0))
        run.complete(TaskKey(0, 0), b"yes")
        self.assertEqual(run.final, Final(b"yes"))
        with self.assertRaises(ValueError):
            run.complete(TaskKey(0, 0), b"yes")

    def test_wrong_completion_does_not_consume_the_right_one(self):
        run = method_run()
        run.accepted(TaskKey(0, 0))
        with self.assertRaises(ValueError):
            run.complete(TaskKey(1, 0), b"yes")
        run.complete(TaskKey(0, 0), b"yes")
        self.assertEqual(run.pending.profile_name, "judge")

    def test_bounded_states_results_and_stage_count(self):
        for changes in ({"state_bytes": 3}, {"input_bytes": 2}, {"result_bytes": 3}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                method_run(**changes)
        for result, changes in ((b"12345", {}), (b"yes", {"stages": 1})):
            run = method_run(**changes)
            run.accepted(TaskKey(0, 0))
            with self.assertRaises(ValueError):
                run.complete(TaskKey(0, 0), result)
            self.assertTrue(run.failed)
            self.assertIsNone(run.pending)
            self.assertIsNone(run.final)

    def test_method_failure_releases_delivery_in_driver(self):
        class Broken(TwoStage):
            def resume(self, state, result):
                raise RuntimeError("method error")

        engine, session, backend, _ = setup(held_tasks=1)
        run = method_run(Broken())
        # The default profile is enough to exercise ownership independently of routing.
        session.offer((task(0),))
        key = TaskKey(session.session_id, 0)
        run.accepted(key)
        session.advance(1)
        backend.complete(key)
        (delivery,) = session.advance(1).deliveries
        try:
            with self.assertRaises(RuntimeError):
                run.complete(key, delivery.result)
        finally:
            session.release((delivery.lease_id,))
        self.assertEqual(engine.capacity.usage().held_tasks, 0)
        self.assertTrue(run.failed)
