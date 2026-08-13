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

from src.scheduling.core.models import BatchRequest, PayloadEnvelope  # noqa: E402
from src.scheduling.core.ready_window import (  # noqa: E402
    BoundedReadyWindow,
    ReadySubmission,
)


def candidate(
    request_id: str,
    work: int,
    *,
    payload_bytes: int = 0,
) -> ReadySubmission:
    request = BatchRequest(
        request_id=request_id,
        job_id="job",
        operator="ai_complete",
        row_count=1,
        prompt_tokens=work,
        estimated_output_tokens=0,
        prefix_key="",
        first_arrival_s=0.0,
        oldest_arrival_s=0.0,
        payload_id=request_id,
        estimated_payload_bytes=payload_bytes,
    )
    return ReadySubmission(
        PayloadEnvelope(request, request_id),
        "default",
        "endpoint-0",
        "0",
        0.0,
        {},
    )


class BoundedReadyWindowTests(unittest.TestCase):
    def test_window_bounds_logical_payload_bytes_independently(self) -> None:
        window = BoundedReadyWindow(
            request_limit=4,
            work_limit=100,
            payload_bytes_limit=30,
        )
        first = candidate("r0", 10, payload_bytes=20)
        second = candidate("r1", 10, payload_bytes=11)

        window.append(first)

        self.assertEqual(window.payload_bytes, 20)
        self.assertFalse(
            window.can_accept(second.estimated_work, second.payload_bytes)
        )

    def test_rejects_invalid_limits(self) -> None:
        for request_limit, work_limit in ((0, 1), (1, 0), (True, 1)):
            with self.subTest(
                request_limit=request_limit,
                work_limit=work_limit,
            ):
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    BoundedReadyWindow(
                        request_limit=request_limit,
                        work_limit=work_limit,
                    )

    def test_enforces_request_and_work_bounds(self) -> None:
        window = BoundedReadyWindow(request_limit=2, work_limit=20)
        first = candidate("r0", 11)
        second = candidate("r1", 9)
        window.append(first)
        window.append(second)

        self.assertEqual(len(window), 2)
        self.assertEqual(window.work, 20)
        self.assertFalse(window.can_accept(1))

        window.remove(first)
        self.assertEqual(window.work, 9)

    def test_rejects_one_request_larger_than_work_bound(self) -> None:
        window = BoundedReadyWindow(request_limit=2, work_limit=20)

        with self.assertRaisesRegex(ValueError, "exceeds"):
            window.can_accept(21)


if __name__ == "__main__":
    unittest.main()
