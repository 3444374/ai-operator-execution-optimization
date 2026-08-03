from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.contracts import BaselineRequestResult, ChatRequest
from src.baselines.common.manifests import (
    assign_endpoint_shards,
    read_manifest,
    write_manifest,
)
from src.baselines.common.results import summarize_results, validate_results


def sample_request(
    doc_id: int,
    *,
    prompt_tokens: int = 4,
    endpoint_index: int = 0,
) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id,
        prompt=f"question-{doc_id}",
        arrival_time_s=float(doc_id) / 10,
        prompt_tokens=prompt_tokens,
        max_output_tokens=8,
        estimated_output_tokens=8,
        source_row_hash=f"row-{doc_id}",
        endpoint_index=endpoint_index,
    )


def sample_result(
    doc_id: int,
    *,
    status: str = "completed",
    endpoint_index: int = 0,
    submitted_at_s: float = 1.0,
    completed_at_s: float = 1.2,
) -> BaselineRequestResult:
    return BaselineRequestResult(
        doc_id=doc_id,
        endpoint_index=endpoint_index,
        status=status,
        error=None if status == "completed" else "model call failed",
        submitted_at_s=submitted_at_s,
        started_at_s=submitted_at_s + 0.1,
        completed_at_s=completed_at_s,
        input_tokens=4,
        output_tokens=1,
        output_text="ok" if status == "completed" else None,
        finish_reason="stop" if status == "completed" else None,
    )


class BaselineContractTests(unittest.TestCase):
    def test_manifest_round_trip_preserves_order_and_hash(self) -> None:
        requests = (
            ChatRequest(
                doc_id=1,
                prompt="first",
                arrival_time_s=0.0,
                prompt_tokens=3,
                max_output_tokens=8,
                estimated_output_tokens=8,
                source_row_hash="row-1",
                endpoint_index=0,
            ),
            ChatRequest(
                doc_id=2,
                prompt="second",
                arrival_time_s=0.1,
                prompt_tokens=5,
                max_output_tokens=8,
                estimated_output_tokens=8,
                source_row_hash="row-2",
                endpoint_index=1,
            ),
        )
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.jsonl"
            metadata = write_manifest(path, requests)

            self.assertEqual(read_manifest(path), requests)
            self.assertEqual(metadata.row_count, 2)
            self.assertEqual(len(metadata.sha256), 64)

    def test_manifest_rejects_duplicate_doc_id(self) -> None:
        request = sample_request(doc_id=1)
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.jsonl"
            with self.assertRaisesRegex(ValueError, "duplicate doc_id"):
                write_manifest(path, (request, request))

    def test_endpoint_assignment_is_deterministic_and_balances_work(
        self,
    ) -> None:
        requests = tuple(
            sample_request(
                doc_id=i,
                prompt_tokens=cost,
                endpoint_index=-1,
            )
            for i, cost in enumerate([20, 18, 7, 5], start=1)
        )

        first = assign_endpoint_shards(requests, endpoint_count=2)
        second = assign_endpoint_shards(requests, endpoint_count=2)

        self.assertEqual(first, second)
        self.assertEqual(
            [request.doc_id for request in first],
            [1, 2, 3, 4],
        )
        work = [0, 0]
        for request in first:
            work[request.endpoint_index] += request.estimated_work
        self.assertLessEqual(abs(work[0] - work[1]) / max(work), 0.02)

    def test_result_validation_rejects_non_exactly_once_rows(self) -> None:
        requests = (sample_request(1), sample_request(2))

        with self.assertRaisesRegex(ValueError, "exactly-once"):
            validate_results(
                requests,
                (
                    sample_result(1, status="completed"),
                    sample_result(1, status="completed"),
                ),
            )

    def test_summary_uses_request_jct_and_token_totals(self) -> None:
        requests = (sample_request(1), sample_request(2))
        results = (
            sample_result(1, submitted_at_s=1.0, completed_at_s=1.2),
            sample_result(2, submitted_at_s=1.1, completed_at_s=1.5),
        )

        summary = summarize_results(requests, results)

        self.assertEqual(summary["request_count"], 2)
        self.assertEqual(summary["input_tokens"], 8)
        self.assertEqual(summary["output_tokens"], 2)
        self.assertAlmostEqual(summary["jct_s"], 0.5)
        self.assertAlmostEqual(summary["tokens_per_s"], 20.0)
        self.assertTrue(summary["exactly_once"])


if __name__ == "__main__":
    unittest.main()
