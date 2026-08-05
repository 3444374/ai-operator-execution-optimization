from __future__ import annotations

import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.core.lifecycle import RequestTraceRow  # noqa: E402

_TRACES_PATH = CODE_ROOT / "src" / "observability" / "profiling" / "traces.py"
_spec = importlib.util.spec_from_file_location("profiler_traces", _TRACES_PATH)
traces = importlib.util.module_from_spec(_spec)
sys.modules["profiler_traces"] = traces
_spec.loader.exec_module(traces)


def _trace_row(
    doc_id: int, *, status: str = "completed", finish_reason: str | None = "stop",
    service_start: float | None = 1.5, error_type: str = "",
) -> RequestTraceRow:
    return RequestTraceRow(
        request_id=f"r{doc_id}", submission_id=f"s{doc_id}", doc_id=str(doc_id),
        pool_id="pool-0", endpoint_id="endpoint-0", gpu_id="gpu-0",
        prompt_tokens=40, estimated_output_tokens=10,
        client_estimated_output_tokens=10, actual_output_tokens=12,
        output_token_source="endpoint_request", total_tokens=52,
        prefix_key="", status=status, error_type=error_type,
        arrival_epoch_s=0.0, flush_epoch_s=0.0, submit_epoch_s=1.0,
        service_start_epoch_s=service_start, completion_epoch_s=2.0,
        buffer_s=0.0, submit_to_service_s=0.5, service_s=0.5, e2e_s=2.0,
        request_time_origin="offline_job_start",
        latency_granularity="request", slo_target_s=None, slo_met=None,
        finish_reason=finish_reason,
    )


class WriteCompletionEvidenceTests(unittest.TestCase):
    def test_one_row_per_doc_with_output_text_flattened(self) -> None:
        rows = (_trace_row(10), _trace_row(11))
        operator_results = [
            {"doc_id": [10, 11], "output_text": ["ans10", "ans11"]},
        ]
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "evidence.csv"
            traces.write_completion_evidence(path, rows=rows, operator_results=operator_results)
            with path.open(encoding="utf-8") as f:
                out = list(csv.DictReader(f))
        self.assertEqual(len(out), 2)
        by_doc = {int(r["doc_id"]): r for r in out}
        self.assertEqual(by_doc[10]["output_text"], "ans10")
        self.assertEqual(by_doc[11]["output_text"], "ans11")
        self.assertEqual(by_doc[10]["status"], "completed")
        self.assertEqual(by_doc[10]["finish_reason"], "stop")
        self.assertEqual(by_doc[10]["prompt_tokens"], "40")
        self.assertEqual(by_doc[10]["output_tokens"], "12")
        self.assertEqual(set(out[0].keys()), {
            "doc_id", "prompt_tokens", "output_tokens", "output_text",
            "status", "error_type", "finish_reason",
            "submit_epoch_s", "service_start_epoch_s", "completion_epoch_s",
        })

    def test_missing_output_defaults_empty_failed_row(self) -> None:
        # A failed doc_id absent from operator_results (no output collected) gets
        # empty output_text; service_start_epoch_s is empty for the failed row.
        rows = (_trace_row(10), _trace_row(20, status="failed", finish_reason=None,
                                           service_start=None, error_type="boom"))
        operator_results = [{"doc_id": [10], "output_text": ["ans10"]}]
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "evidence.csv"
            traces.write_completion_evidence(path, rows=rows, operator_results=operator_results)
            with path.open(encoding="utf-8") as f:
                out = {int(r["doc_id"]): r for r in csv.DictReader(f)}
        self.assertEqual(out[20]["output_text"], "")
        self.assertEqual(out[20]["status"], "failed")
        self.assertEqual(out[20]["error_type"], "boom")
        self.assertEqual(out[20]["service_start_epoch_s"], "")

    def test_mismatched_batch_counts_skipped_not_raised(self) -> None:
        # A malformed batch (output count != doc_id count) is skipped, not raised;
        # the affected doc_ids fall back to empty output_text.
        rows = (_trace_row(10),)
        operator_results = [{"doc_id": [10, 11], "output_text": ["only_one"]}]
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "evidence.csv"
            traces.write_completion_evidence(path, rows=rows, operator_results=operator_results)
            with path.open(encoding="utf-8") as f:
                out = {int(r["doc_id"]): r for r in csv.DictReader(f)}
        self.assertEqual(out[10]["output_text"], "")


if __name__ == "__main__":
    unittest.main()
