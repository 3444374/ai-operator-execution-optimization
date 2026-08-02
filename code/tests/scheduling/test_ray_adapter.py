from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling import ActorSubmissionState, ActorWorkerPoolSubmitter  # noqa: E402
from src.scheduling.core.models import BatchRequest, PayloadEnvelope  # noqa: E402
from src.scheduling.runtime.ray_adapter import RaySubmissionAdapter  # noqa: E402


class FakeRef:
    def __init__(self, value):
        self.value = value


class EqualButDistinctRef(FakeRef):
    def __eq__(self, other):
        return (
            isinstance(other, EqualButDistinctRef)
            and self.value == other.value
        )


class FakeRay:
    @staticmethod
    def wait(refs, num_returns):
        return refs[:num_returns], refs[num_returns:]

    @staticmethod
    def get(ref):
        return ref.value


class RecordingRemoteMethod:
    def __init__(self):
        self.payloads = []

    def remote(self, payload=None):
        self.payloads.append(payload)
        return FakeRef(payload)


class RecordingActor:
    def __init__(self):
        self.complete = RecordingRemoteMethod()
        self.ready = RecordingRemoteMethod()


def envelope() -> PayloadEnvelope:
    return PayloadEnvelope(
        BatchRequest("r1", "j1", "ai_complete", 1, 2, 3, "", 0.0, 0.0, "p1"),
        "payload",
    )


class RaySubmissionAdapterTests(unittest.TestCase):
    def test_actor_submission_state_waits_for_every_actor_ready_reference(
        self,
    ) -> None:
        actors = [RecordingActor(), RecordingActor()]
        state = ActorSubmissionState({"endpoint-0": actors}, "complete")

        class ReadyRay:
            @staticmethod
            def get(refs):
                return [ref.value for ref in refs]

        duration_s, evidence = state.wait_until_ready(
            ReadyRay,
            clock=iter([10.0, 10.25]).__next__,
        )

        self.assertEqual(duration_s, 0.25)
        self.assertEqual(evidence, (None, None))
        self.assertEqual(actors[0].ready.payloads, [None])
        self.assertEqual(actors[1].ready.payloads, [None])

    def test_actor_worker_pool_rotates_inside_one_endpoint(self) -> None:
        actors = [RecordingActor(), RecordingActor()]
        submitter = ActorWorkerPoolSubmitter(actors, "complete")

        submitter("a")
        submitter("b")
        submitter("c")

        self.assertEqual(actors[0].complete.payloads, ["a", "c"])
        self.assertEqual(actors[1].complete.payloads, ["b"])
        self.assertEqual(submitter.worker_count, 2)
        self.assertEqual(submitter.submission_counts, (2, 1))

    def test_actor_worker_pool_rejects_empty_workers(self) -> None:
        with self.assertRaisesRegex(ValueError, "actors must not be empty"):
            ActorWorkerPoolSubmitter([], "complete")

    def test_actor_worker_pool_rejects_empty_method_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "method_name must not be empty"):
            ActorWorkerPoolSubmitter([RecordingActor()], "")

    def test_actor_worker_pool_enforces_slot_capacity_until_completion(
        self,
    ) -> None:
        submitter = ActorWorkerPoolSubmitter(
            [RecordingActor()],
            "complete",
            endpoint_id="endpoint-0",
            max_concurrency_per_worker=2,
            routing_policy="round_robin",
        )

        first = submitter.submit("first", estimated_work=10)
        second = submitter.submit("second", estimated_work=20)
        with self.assertRaisesRegex(RuntimeError, "capacity exhausted"):
            submitter.submit("third", estimated_work=5)

        submitter.complete(first, failed=False)
        third = submitter.submit("third", estimated_work=5)
        self.assertEqual(submitter.assignment(second).worker_index, 0)
        self.assertEqual(submitter.assignment(third).worker_index, 0)
        self.assertEqual(submitter.snapshots()[0].running, 2)

    def test_actor_worker_pool_least_active_work_reuses_freed_worker(
        self,
    ) -> None:
        submitter = ActorWorkerPoolSubmitter(
            [RecordingActor(), RecordingActor()],
            "complete",
            endpoint_id="endpoint-0",
            max_concurrency_per_worker=2,
            routing_policy="least_active_work",
        )

        first = submitter.submit("large", estimated_work=100)
        second = submitter.submit("small", estimated_work=10)
        submitter.complete(second, failed=False)
        third = submitter.submit("next", estimated_work=5)

        self.assertEqual(submitter.assignment(first).worker_index, 0)
        self.assertEqual(submitter.assignment(third).worker_index, 1)
        snapshots = submitter.snapshots()
        self.assertEqual([item.max_running for item in snapshots], [1, 1])
        self.assertEqual([item.submitted for item in snapshots], [1, 2])

    def test_actor_worker_snapshot_records_slot_held_time(self) -> None:
        clock_values = iter([10.0, 12.5])
        submitter = ActorWorkerPoolSubmitter(
            [RecordingActor()],
            "complete",
            endpoint_id="endpoint-0",
            max_concurrency_per_worker=1,
            routing_policy="round_robin",
            clock=lambda: next(clock_values),
        )

        handle = submitter.submit("work", estimated_work=7)
        submitter.complete(handle, failed=False)

        snapshot = submitter.snapshots()[0]
        self.assertEqual(snapshot.max_active_work, 7)
        self.assertEqual(snapshot.slot_held_s, 2.5)

    def test_actor_submission_state_rejects_different_workers(self) -> None:
        actors = [RecordingActor(), RecordingActor()]
        state = ActorSubmissionState({"endpoint-0": actors}, "complete")

        with self.assertRaisesRegex(ValueError, "same actor workers"):
            state.validate(
                {"endpoint-0": [actors[0], RecordingActor()]},
                "complete",
            )

    def test_actor_submission_state_rejects_different_method(self) -> None:
        actor = RecordingActor()
        state = ActorSubmissionState({"endpoint-0": [actor]}, "complete")

        with self.assertRaisesRegex(ValueError, "same method_name"):
            state.validate({"endpoint-0": [actor]}, "embed")

    def test_submit_and_collect_preserve_request_identity(self) -> None:
        adapter = RaySubmissionAdapter(
            FakeRay,
            {"e1": lambda payload: FakeRef({"payload": payload})},
        )
        item = envelope()

        handle = adapter.submit(item, "e1")
        collected = adapter.wait_one([(handle, item)])

        self.assertEqual(collected.completion.request_id, "r1")
        self.assertEqual(collected.completion.result, {"payload": "payload"})
        self.assertGreaterEqual(collected.wait_s, 0.0)
        self.assertGreaterEqual(collected.result_s, 0.0)
        self.assertEqual(collected.actor_worker_id, "")
        self.assertEqual(collected.actor_worker_pid, 0)

    def test_poll_one_returns_only_ready_ray_completion(self) -> None:
        class PollingRay:
            ready = False

            @classmethod
            def wait(cls, refs, num_returns, timeout=None):
                self.assertEqual(timeout, 0.0)
                if not cls.ready:
                    return [], refs
                return refs[:num_returns], refs[num_returns:]

            @staticmethod
            def get(ref):
                return ref.value

        item = envelope()
        handle = FakeRef("done")
        adapter = RaySubmissionAdapter(PollingRay, {})

        self.assertIsNone(adapter.poll_one([(handle, item)]))
        PollingRay.ready = True
        collected = adapter.poll_one([(handle, item)])

        self.assertIsNotNone(collected)
        self.assertEqual(collected.completion.result, "done")

    def test_collect_returns_canonical_pending_handle(self) -> None:
        pending_handle = EqualButDistinctRef("result")

        class CopyingRay:
            @staticmethod
            def wait(refs, num_returns):
                del num_returns
                return [EqualButDistinctRef(refs[0].value)], []

            @staticmethod
            def get(ref):
                return ref.value

        adapter = RaySubmissionAdapter(CopyingRay, {})
        collected = adapter.wait_one([(pending_handle, envelope())])

        self.assertIs(collected.handle, pending_handle)
        self.assertEqual(collected.completion.result, "result")

    def test_collect_converts_ray_get_error_to_failed_completion(self) -> None:
        class FailingRay(FakeRay):
            @staticmethod
            def get(_ref):
                raise RuntimeError("worker crashed")

        item = envelope()
        handle = FakeRef(None)

        collected = RaySubmissionAdapter(FailingRay, {}).wait_one(
            [(handle, item)]
        )

        self.assertIs(collected.handle, handle)
        self.assertEqual(collected.completion.request_id, "r1")
        self.assertEqual(collected.completion.status, "failed")
        self.assertIn(
            "RuntimeError: worker crashed",
            collected.completion.error,
        )

    def test_adapter_releases_canonical_worker_assignment_after_equal_ready_ref(
        self,
    ) -> None:
        class EqualRefRemoteMethod:
            def remote(self, payload):
                return EqualButDistinctRef(payload)

        actor = RecordingActor()
        actor.complete = EqualRefRemoteMethod()
        submitter = ActorWorkerPoolSubmitter(
            [actor],
            "complete",
            endpoint_id="endpoint-0",
            max_concurrency_per_worker=1,
            routing_policy="round_robin",
        )
        adapter = RaySubmissionAdapter(
            type(
                "CopyingRay",
                (),
                {
                    "wait": staticmethod(
                        lambda refs, num_returns: (
                            [EqualButDistinctRef(refs[0].value)],
                            [],
                        )
                    ),
                    "get": staticmethod(lambda ref: ref.value),
                },
            ),
            {"e1": submitter},
        )
        item = envelope()

        handle = adapter.submit(item, "e1")
        collected = adapter.wait_one([(handle, item)])

        self.assertIs(collected.handle, handle)
        self.assertEqual(submitter.snapshots()[0].running, 0)
        self.assertEqual(submitter.snapshots()[0].completed, 1)
        self.assertEqual(collected.actor_worker_id, "endpoint-0:worker:0")
        self.assertEqual(collected.actor_worker_index, 0)

    def test_adapter_releases_failed_worker_assignment_once(self) -> None:
        class FailingRay(FakeRay):
            @staticmethod
            def get(_ref):
                raise RuntimeError("worker crashed")

        submitter = ActorWorkerPoolSubmitter(
            [RecordingActor()],
            "complete",
            endpoint_id="endpoint-0",
            max_concurrency_per_worker=1,
            routing_policy="round_robin",
        )
        adapter = RaySubmissionAdapter(FailingRay, {"e1": submitter})
        item = envelope()
        handle = adapter.submit(item, "e1")

        collected = adapter.wait_one([(handle, item)])

        self.assertEqual(collected.completion.status, "failed")
        snapshot = submitter.snapshots()[0]
        self.assertEqual(snapshot.running, 0)
        self.assertEqual(snapshot.failed, 1)
        with self.assertRaisesRegex(RuntimeError, "unknown.*assignment"):
            submitter.complete(handle, failed=True)

    def test_submit_rejects_unknown_endpoint(self) -> None:
        adapter = RaySubmissionAdapter(FakeRay, {})

        with self.assertRaisesRegex(RuntimeError, "no Ray submitter"):
            adapter.submit(envelope(), "missing")


if __name__ == "__main__":
    unittest.main()
