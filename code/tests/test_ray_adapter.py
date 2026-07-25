from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.models import BatchRequest, PayloadEnvelope  # noqa: E402
from src.scheduling.ray_adapter import RaySubmissionAdapter  # noqa: E402


class FakeRef:
    def __init__(self, value):
        self.value = value


class FakeRay:
    @staticmethod
    def wait(refs, num_returns):
        return refs[:num_returns], refs[num_returns:]

    @staticmethod
    def get(ref):
        return ref.value


def envelope() -> PayloadEnvelope:
    return PayloadEnvelope(
        BatchRequest("r1", "j1", "ai_complete", 1, 2, 3, "", 0.0, 0.0, "p1"),
        "payload",
    )


class RaySubmissionAdapterTests(unittest.TestCase):
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

    def test_submit_rejects_unknown_endpoint(self) -> None:
        adapter = RaySubmissionAdapter(FakeRay, {})

        with self.assertRaisesRegex(RuntimeError, "no Ray submitter"):
            adapter.submit(envelope(), "missing")


if __name__ == "__main__":
    unittest.main()
