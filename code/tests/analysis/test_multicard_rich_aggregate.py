"""Tests for the committed multicard rich-metric aggregator.

Validates the three audit fixes: (1) EM uses the 2048-subset denominator,
(2) project TTFT is read from vllm_time_to_first_token_p50_s (not
submit_to_service), (3) project predictions are read from project2_*_evidence.csv
(the old ad-hoc script looked for the wrong filename -> n_outputs=0). Also checks
the cross-arm unified service tokens/s derivation.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

_AGG_PATH = CODE_ROOT / "scripts" / "analysis" / "multicard_rich_aggregate.py"
_spec = importlib.util.spec_from_file_location("multicard_rich_aggregate", _AGG_PATH)
agg = importlib.util.module_from_spec(_spec)
sys.modules["multicard_rich_aggregate"] = agg
_spec.loader.exec_module(agg)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _gate_shard(summary: dict, requests: list[dict]) -> dict:
    """Build one shard dir with summary.json + requests.csv."""
    return {"summary": summary, "requests": requests}


def _build_gate_cell(root: Path, arm: str, tag: str, shards: list[dict]) -> None:
    cell = root / f"{arm}_{tag}" / arm
    for idx, shard in enumerate(shards):
        sd = cell / f"shard_{idx}"
        _write(sd / "summary.json", json.dumps(shard["summary"]))
        fields = ["doc_id", "endpoint_index", "status", "error", "submitted_at_s",
                  "started_at_s", "completed_at_s", "input_tokens", "output_tokens",
                  "output_text", "finish_reason"]
        with (sd / "requests.csv").open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(shard["requests"])


def _references(doc_ids: list[int], answer: str = "correct") -> dict:
    """Full reference entries (doc_id -> {source_example_id, reference_answers})."""
    return {str(d): {"doc_id": d, "source_example_id": f"s{d}",
                     "reference_answers": [answer]} for d in doc_ids}


def _answers_only(refs: dict) -> dict[str, list[str]]:
    """The shape _quality / squad_quality_metrics expects: id -> [answers]."""
    return {k: v["reference_answers"] for k, v in refs.items()}


class QualityDenominatorTests(unittest.TestCase):
    """Audit fix #1: EM uses the 2048-subset denominator, not 10570."""

    def test_em_subset_is_correct_rows_over_subset_n(self) -> None:
        # 4/4 correct over a 4-row subset -> EM 100% (NOT 4/10570 = 0.04%).
        refs = _references([1, 2, 3, 4])
        preds = {str(d): "correct" for d in (1, 2, 3, 4)}
        q = agg._quality(preds, _answers_only(refs))
        self.assertEqual(q["correct_rows"], 4)
        self.assertAlmostEqual(q["em_pct_subset"], 100.0)
        self.assertAlmostEqual(q["em_pct_full"], 100.0 * 4 / agg.SQUAD_FULL_DENOMINATOR)
        self.assertAlmostEqual(q["f1_pct_subset"], 100.0)

    def test_two_wrong_em_subset_is_half_not_near_zero(self) -> None:
        refs = _references([1, 2, 3, 4])
        preds = {"1": "correct", "2": "correct", "3": "wrong", "4": "wrong"}
        q = agg._quality(preds, _answers_only(refs))
        self.assertEqual(q["correct_rows"], 2)
        self.assertAlmostEqual(q["em_pct_subset"], 50.0)  # 2/4, not 2/10570

    def test_predictions_read_from_project_evidence_filename(self) -> None:
        # Audit fix #3: project evidence lives at project2_<tag>_evidence.csv.
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "project2_formal0_evidence.csv"
            fields = ["doc_id", "prompt_tokens", "output_tokens", "output_text",
                      "status", "error_type", "finish_reason", "submit_epoch_s",
                      "service_start_epoch_s", "completion_epoch_s"]
            with p.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                w.writerows([{"doc_id": 1, "output_text": "ans1", **{k: "" for k in fields if k not in ("doc_id", "output_text")}},
                             {"doc_id": 2, "output_text": "ans2", **{k: "" for k in fields if k not in ("doc_id", "output_text")}}])
            preds = agg._predictions_from_evidence_csv(p)
        self.assertEqual(preds, {"1": "ans1", "2": "ans2"})


class GateArmTtftPendingTests(unittest.TestCase):
    """Gate summaries without the B1 histogram stamp -> ttft_status pending_rerun."""

    def test_gate_arm_without_histogram_marks_pending(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            refs = _references([1, 2])
            shards = [
                {"summary": {"service_total_tokens_delta": 100, "jct_s": 2.0,
                             "latency_p50_s": 1.0, "latency_p95_s": 1.5, "latency_p99_s": 2.0},
                 "requests": [{"doc_id": 1, "endpoint_index": 0, "status": "completed",
                               "error": "", "submitted_at_s": 0, "started_at_s": 0,
                               "completed_at_s": 1, "input_tokens": 10, "output_tokens": 1,
                               "output_text": "correct", "finish_reason": "stop"}]},
                {"summary": {"service_total_tokens_delta": 100, "jct_s": 2.0,
                             "latency_p50_s": 1.0, "latency_p95_s": 1.5, "latency_p99_s": 2.0},
                 "requests": [{"doc_id": 2, "endpoint_index": 1, "status": "completed",
                               "error": "", "submitted_at_s": 0, "started_at_s": 0,
                               "completed_at_s": 1, "input_tokens": 10, "output_tokens": 1,
                               "output_text": "correct", "finish_reason": "stop"}]},
            ]
            _build_gate_cell(root, "bounded_http", "formal0", shards)
            _write(root / "squad_eq2048_references.json", json.dumps(refs))
            rows = agg._aggregate_gate_arm(root, "bounded_http", {k: v["reference_answers"] for k, v in refs.items()})
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r["ttft_status"], "pending_rerun:no_histogram_in_summary")
        self.assertIsNone(r["ttft_s_p50"])
        # unified tokens/s = (100+100)/max(2.0,2.0) = 100
        self.assertAlmostEqual(r["service_tokens_per_s_unified"], 100.0)
        self.assertEqual(r["correct_rows"], 2)
        self.assertAlmostEqual(r["em_pct_subset"], 100.0)

    def test_gate_arm_with_b1_histogram_stamp_reads_ttft(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            refs = _references([1, 2])
            base = {"service_total_tokens_delta": 100, "jct_s": 2.0,
                    "latency_p50_s": 1.0, "latency_p95_s": 1.5, "latency_p99_s": 2.0,
                    "vllm_time_to_first_token_p50_s": 0.05, "vllm_time_to_first_token_p95_s": 0.07,
                    "vllm_time_to_first_token_p99_s": 0.08}
            shards = [
                {"summary": dict(base), "requests": [{"doc_id": 1, "endpoint_index": 0, "status": "completed", "error": "", "submitted_at_s": 0, "started_at_s": 0, "completed_at_s": 1, "input_tokens": 10, "output_tokens": 1, "output_text": "correct", "finish_reason": "stop"}]},
                {"summary": dict(base), "requests": [{"doc_id": 2, "endpoint_index": 1, "status": "completed", "error": "", "submitted_at_s": 0, "started_at_s": 0, "completed_at_s": 1, "input_tokens": 10, "output_tokens": 1, "output_text": "correct", "finish_reason": "stop"}]},
            ]
            _build_gate_cell(root, "bounded_http", "formal0", shards)
            rows = agg._aggregate_gate_arm(root, "bounded_http", {k: v["reference_answers"] for k, v in refs.items()})
        r = rows[0]
        self.assertEqual(r["ttft_status"], "ok")
        self.assertAlmostEqual(r["ttft_s_p50"], 0.05)


class ProjectArmTests(unittest.TestCase):
    """Audit fix #2: project TTFT read from vllm_time_to_first_token_p50_s."""

    def _project_csv(self, root: Path, tag: str, evidence_rows: list[dict]) -> None:
        prof = {
            "tokens_per_s": 64090, "operator_tokens_per_s": 81796,
            "vllm_prompt_tokens_delta": 423198, "vllm_generation_tokens_delta": 10115,
            "model_request_wall_s": 5.51, "operator_wall_s": 5.30, "e2e_s": 6.76,
            "submit_s": 1.96, "writeback_s": 0.05, "db_fetch_s": 0.61,
            "scheduling_control_overhead_pct": 36.97,
            "request_e2e_s_p50": 3.58, "request_e2e_s_p95": 6.06, "request_e2e_s_p99": 6.24,
            "vllm_time_to_first_token_p50_s": 0.0522, "vllm_time_to_first_token_p95_s": 0.0747,
            "vllm_time_to_first_token_p99_s": 0.0789, "vllm_prefix_cache_hit_rate": 0.958,
        }
        _write(root / f"project2_{tag}.csv", ",".join(prof) + "\n" + ",".join(str(v) for v in prof.values()) + "\n")
        fields = ["doc_id", "prompt_tokens", "output_tokens", "output_text", "status",
                  "error_type", "finish_reason", "submit_epoch_s", "service_start_epoch_s", "completion_epoch_s"]
        with (root / f"project2_{tag}_evidence.csv").open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(evidence_rows)

    def test_project_reads_real_ttft_and_unified_tps(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            refs = _references([1, 2])
            self._project_csv(root, "formal0", [
                {"doc_id": 1, "output_text": "correct", **{k: "" for k in ("prompt_tokens", "output_tokens", "status", "error_type", "finish_reason", "submit_epoch_s", "service_start_epoch_s", "completion_epoch_s")}},
                {"doc_id": 2, "output_text": "correct", **{k: "" for k in ("prompt_tokens", "output_tokens", "status", "error_type", "finish_reason", "submit_epoch_s", "service_start_epoch_s", "completion_epoch_s")}},
            ])
            rows = agg._aggregate_project_arm(root, {k: v["reference_answers"] for k, v in refs.items()})
        r = rows[0]
        # TTFT is the REAL 52ms, not submit-to-service
        self.assertAlmostEqual(r["ttft_s_p50"], 0.0522)
        self.assertEqual(r["ttft_status"], "ok")
        # unified tps = (423198+10115)/5.51 ~ 78643
        self.assertAlmostEqual(r["service_tokens_per_s_unified"], 78643, delta=5)
        self.assertEqual(r["correct_rows"], 2)
        self.assertAlmostEqual(r["em_pct_subset"], 100.0)
        self.assertAlmostEqual(r["scheduling_control_overhead_pct"], 36.97)


if __name__ == "__main__":
    unittest.main()
