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

from src.scheduling.core.execution import (  # noqa: E402
    SubmissionExecutionLedger,
)
from src.modalities.text.costs import extract_completed_token_work  # noqa: E402
from src.scheduling.core.models import (  # noqa: E402
    BatchRequest,
    CollectedSubmission,
    PayloadEnvelope,
    SubmissionCompletion,
)


def envelope(request_id: str) -> PayloadEnvelope:
    return PayloadEnvelope(
        BatchRequest(
            request_id,
            "job",
            "ai_complete",
            1,
            10,
            5,
            "",
            0.0,
            0.0,
            f"payload-{request_id}",
        ),
        request_id,
    )


class SubmissionExecutionLedgerTests(unittest.TestCase):
    def test_records_actual_work_and_lifecycle_once(self) -> None:
        ledger = SubmissionExecutionLedger(
            actual_work_extractor=extract_completed_token_work,
        )
        item = envelope("r0")
        handle = object()
        ledger.observe(item)
        ledger.submitted(
            handle,
            item,
            pool_id="default",
            endpoint_id="gpu0",
            gpu_id="0",
            submit_epoch_s=10.0,
        )

        recorded = ledger.record(
            CollectedSubmission(
                handle,
                SubmissionCompletion(
                    "r0",
                    "completed",
                    result={"token_count": 37},
                ),
                wait_s=0.1,
                result_s=0.2,
            ),
            completion_epoch_s=11.0,
        )

        self.assertEqual(recorded.actual_work, 37)
        self.assertEqual(ledger.inflight_count, 0)
        self.assertEqual(ledger.ordered_completions()[0].request_id, "r0")
        self.assertEqual(ledger.ordered_events()[0].endpoint_id, "gpu0")

    def test_duplicate_request_id_is_rejected_before_submission(self) -> None:
        ledger = SubmissionExecutionLedger()
        ledger.observe(envelope("r0"))

        with self.assertRaisesRegex(ValueError, "duplicate request_id"):
            ledger.observe(envelope("r0"))

    def test_extractor_failure_leaves_pending_state_unchanged(self) -> None:
        def fail_extraction(_completion: SubmissionCompletion) -> int | None:
            raise ValueError("bad completion payload")

        ledger = SubmissionExecutionLedger(
            actual_work_extractor=fail_extraction
        )
        item = envelope("r0")
        handle = object()
        ledger.observe(item)
        ledger.submitted(
            handle,
            item,
            pool_id="default",
            endpoint_id="gpu0",
            gpu_id="0",
            submit_epoch_s=1.0,
        )

        with self.assertRaisesRegex(ValueError, "bad completion"):
            ledger.record(
                CollectedSubmission(
                    handle,
                    SubmissionCompletion("r0", "completed", {}),
                    wait_s=0.0,
                    result_s=0.0,
                ),
                completion_epoch_s=2.0,
            )

        self.assertEqual(ledger.inflight_count, 1)
        self.assertIn("r0", ledger.contexts)


if __name__ == "__main__":
    unittest.main()
