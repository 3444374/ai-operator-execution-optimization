from __future__ import annotations

import sys
import unittest
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.contracts import BaselineRequestResult, ChatRequest
from src.baselines.gate import validate_gate
from src.baselines.provenance import adapter_provenance


def request(
    doc_id: int,
    endpoint_index: int,
    estimated_work: int,
) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id,
        prompt=f"question-{doc_id}",
        arrival_time_s=0.0,
        prompt_tokens=estimated_work - 8,
        max_output_tokens=8,
        estimated_output_tokens=8,
        source_row_hash=f"row-{doc_id}",
        endpoint_index=endpoint_index,
    )


def result(
    doc_id: int,
    endpoint_index: int,
    *,
    status: str = "completed",
) -> BaselineRequestResult:
    return BaselineRequestResult(
        doc_id=doc_id,
        endpoint_index=endpoint_index,
        status=status,
        error=None if status == "completed" else "worker failed",
        submitted_at_s=1.0,
        started_at_s=1.1,
        completed_at_s=1.2,
        input_tokens=4,
        output_tokens=1,
        output_text="ok" if status == "completed" else None,
        finish_reason="stop" if status == "completed" else None,
    )


def summary(
    endpoint: int,
    predicted_work: int,
    *,
    model_name: str = "qwen",
    protocol: str = "chat_completions",
    service_fingerprint: str = "service-a",
    running: int = 0,
    waiting: int = 0,
    worker_failures: int = 0,
) -> dict[str, object]:
    return {
        "endpoint_index": endpoint,
        "predicted_work": predicted_work,
        "model_name": model_name,
        "completion_protocol": protocol,
        "service_config_sha256": service_fingerprint,
        "vllm_num_requests_running_final": running,
        "vllm_num_requests_waiting_final": waiting,
        "worker_failures": worker_failures,
        **adapter_provenance("bounded_http").summary_fields(),
    }


class BaselineProvenanceGateTests(unittest.TestCase):
    def test_gate_rejects_missing_provenance(self) -> None:
        manifest = (request(1, 0, 100), request(2, 1, 100))
        summaries = [
            summary(endpoint=0, predicted_work=100),
            summary(endpoint=1, predicted_work=100),
        ]
        summaries[0].pop("scheduler_owner")

        report = validate_gate(
            manifest=manifest,
            summaries=summaries,
            request_results=(result(1, 0), result(2, 1)),
        )

        self.assertIn("provenance_missing", report.incidents)


class OfficialBaselineGateTests(unittest.TestCase):
    def test_gate_accepts_balanced_exactly_once_idle_run(self) -> None:
        manifest = (
            request(1, 0, 100),
            request(2, 1, 100),
        )

        report = validate_gate(
            manifest=manifest,
            summaries=(
                summary(endpoint=0, predicted_work=100),
                summary(endpoint=1, predicted_work=100),
            ),
            request_results=(
                result(1, 0),
                result(2, 1),
            ),
        )

        self.assertTrue(report.passed)
        self.assertEqual(report.incidents, ())

    def test_gate_rejects_endpoint_work_skew_over_two_percent(
        self,
    ) -> None:
        manifest = (
            request(1, 0, 100),
            request(2, 1, 90),
        )

        report = validate_gate(
            manifest=manifest,
            summaries=(
                summary(endpoint=0, predicted_work=100),
                summary(endpoint=1, predicted_work=90),
            ),
            request_results=(
                result(1, 0),
                result(2, 1),
            ),
        )

        self.assertFalse(report.passed)
        self.assertIn("endpoint_work_skew", report.incidents)

    def test_gate_reports_all_hard_incidents(self) -> None:
        manifest = (
            request(1, 0, 100),
            request(2, 1, 100),
        )

        report = validate_gate(
            manifest=manifest,
            summaries=(
                summary(
                    endpoint=0,
                    predicted_work=100,
                    model_name="wrong-model",
                    running=1,
                    worker_failures=1,
                ),
                summary(endpoint=1, predicted_work=99),
            ),
            request_results=(
                result(1, 0, status="failed"),
                result(1, 0),
            ),
        )

        self.assertFalse(report.passed)
        self.assertIn("exactly_once", report.incidents)
        self.assertIn("failed_requests", report.incidents)
        self.assertIn("unused_endpoint", report.incidents)
        self.assertIn("metadata_mismatch", report.incidents)
        self.assertIn("predicted_work_mismatch", report.incidents)
        self.assertIn("nonempty_final_queue", report.incidents)
        self.assertIn("worker_failure", report.incidents)


if __name__ == "__main__":
    unittest.main()
