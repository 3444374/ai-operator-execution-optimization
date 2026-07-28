from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling import ActorSubmissionState, ActorWorkerPoolSubmitter  # noqa: E402
from src.scheduling.models import BatchRequest, PayloadEnvelope  # noqa: E402
from src.scheduling.ray_adapter import RaySubmissionAdapter  # noqa: E402


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

    def remote(self, payload):
        self.payloads.append(payload)
        return FakeRef(payload)


class RecordingActor:
    def __init__(self):
        self.complete = RecordingRemoteMethod()


def envelope() -> PayloadEnvelope:
    return PayloadEnvelope(
        BatchRequest("r1", "j1", "ai_complete", 1, 2, 3, "", 0.0, 0.0, "p1"),
        "payload",
    )


class RaySubmissionAdapterTests(unittest.TestCase):
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

    def test_submit_rejects_unknown_endpoint(self) -> None:
        adapter = RaySubmissionAdapter(FakeRay, {})

        with self.assertRaisesRegex(RuntimeError, "no Ray submitter"):
            adapter.submit(envelope(), "missing")


if __name__ == "__main__":
    unittest.main()
