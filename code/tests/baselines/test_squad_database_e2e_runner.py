from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.contracts import BaselineRequestResult  # noqa: E402

_RUNNER_PATH = CODE_ROOT / "scripts" / "baselines" / "squad_database_e2e_runner.py"
_spec = importlib.util.spec_from_file_location("squad_database_e2e_runner", _RUNNER_PATH)
runner = importlib.util.module_from_spec(_spec)
sys.modules["squad_database_e2e_runner"] = runner
_spec.loader.exec_module(runner)


def _row(doc_id: int, sid: str, text: str = "prompt", answers: list[str] | None = None,
         tenant: int = 0, category: str = "squad") -> tuple[dict, tuple]:
    return (
        {"doc_id": doc_id, "text": text, "source_example_id": sid,
         "answers": answers or ["ans"]},
        (tenant, category),
    )


def _result(req_doc_id: int, *, output_text, error=None, status="completed"):
    return BaselineRequestResult(
        doc_id=req_doc_id, endpoint_index=0, status=status, error=error,
        submitted_at_s=1.0, started_at_s=1.0, completed_at_s=2.0,
        input_tokens=4, output_tokens=0, output_text=output_text, finish_reason=None,
    )


class SinkAdapterTests(unittest.TestCase):
    def test_shape_and_sidecar(self) -> None:
        rows = [_row(1, "a"), _row(2, "b", tenant=7, category="x")]
        sidecar = {r["doc_id"]: tc for r, tc in rows}
        results = [_result(1, output_text="ok1"), _result(2, output_text="ok2")]
        payload = runner._results_to_sink_payload(results, sidecar, 0, "squad")
        self.assertEqual(len(payload), 1)
        p = payload[0]
        self.assertEqual(p["doc_id"], [1, 2])
        self.assertEqual(p["tenant_id"], [0, 7])
        self.assertEqual(p["category"], ["squad", "x"])
        self.assertEqual(p["output_text"], ["ok1", "ok2"])

    def test_null_output_becomes_empty_string(self) -> None:
        results = [_result(1, output_text=None)]
        payload = runner._results_to_sink_payload(results, {1: (0, "squad")}, 0, "squad")
        self.assertEqual(payload[0]["output_text"], [""])

    def test_default_fallback_when_sidecar_misses(self) -> None:
        results = [_result(99, output_text="ok")]
        payload = runner._results_to_sink_payload(results, {}, 5, "fallback")
        self.assertEqual(payload[0]["tenant_id"], [5])
        self.assertEqual(payload[0]["category"], ["fallback"])


class RunnerMetricsTests(unittest.TestCase):
    def test_division(self) -> None:
        m = runner._runner_metrics(em_rows=100, success_count=120, row_count=150,
                                   error_count=30, null_count=0, wall_s=10.0, sunk_rows=120)
        self.assertEqual(m["correct_rows_per_s"], 10.0)
        self.assertEqual(m["successful_rows_per_s"], 12.0)
        self.assertEqual(m["raw_rows_per_s"], 15.0)
        self.assertEqual(m["failure_rate"], round(30 / 150, 6))
        self.assertEqual(m["sunk_rows"], 120)

    def test_zero_wall_is_safe(self) -> None:
        m = runner._runner_metrics(10, 10, 10, 0, 0, wall_s=0.0, sunk_rows=10)
        self.assertEqual(m["correct_rows_per_s"], 0.0)
        self.assertEqual(m["raw_rows_per_s"], 0.0)


class DatabaseE2EBarrierTests(unittest.TestCase):
    """Mocked end-to-end main() run: assert E2E timing block + 3 state fields."""

    def _run_main(self, fail_one: bool) -> tuple[int, dict, Path, list[str]]:
        rows_sidecar = [_row(i, f"id{i}", text=f"p{i}") for i in range(1, 5)]
        rows = [r for r, _ in rows_sidecar]
        sidecar = {r["doc_id"]: tc for r, tc in rows_sidecar}
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "result"
            prov = Path(td) / "prov.json"
            prov.write_text(
                json.dumps({"content_hash": runner._structured_content_hash(rows),
                            "sample_count": len(rows)}) + "\n",
                encoding="utf-8",
            )
            argv = [
                "--arm", "duckdb_ai",
                "--database-url", "postgresql://u:p@localhost:5432/d",
                "--workload-name", "squad_v11_dev_short_answer",
                "--importer-provenance", str(prov),
                "--endpoint-url", "http://127.0.0.1:8000/v1/chat/completions",
                "--metrics-url", "http://127.0.0.1:8000/metrics",
                "--model", "m", "--metrics-settle-s", "0",
                "--writeback-mode", "json_text", "--write-batch-rows", "10",
                "--output-dir", str(out), "--force",
            ]

            def fake_scan(conn, workload):
                return rows, sidecar, 0.05

            def fake_complete(requests, config):
                return tuple(
                    _result(r.doc_id, output_text=None if (fail_one and i == 1) else "ok",
                            error=("max_tokens reached" if (fail_one and i == 1) else None),
                            status=("failed" if (fail_one and i == 1) else "completed"))
                    for i, r in enumerate(requests)
                )

            def fake_sink(conn, results, sidecar, mode, batch, tenant, category):
                return len(results), 0.02

            scrape_state = {"n": 0}

            def fake_scrape(url, timeout_s=5.0):
                scrape_state["n"] += 1
                base = {"vllm:num_requests_running": 0.0,
                        "vllm:num_requests_waiting": 0.0,
                        "vllm:request_success_total": 100.0}
                if scrape_state["n"] == 2:
                    base["vllm:request_success_total"] = 100.0 + len(rows)
                return base

            with patch("squad_database_e2e_runner._scan_workload", side_effect=fake_scan), \
                 patch("squad_database_e2e_runner._sink_write", side_effect=fake_sink), \
                 patch("squad_database_e2e_runner.run_duckdb_ai_complete", side_effect=fake_complete), \
                 patch("squad_database_e2e_runner.scrape_prometheus_metrics", side_effect=fake_scrape), \
                 patch("squad_database_e2e_runner.inspect_duckdb_ai_runtime",
                       return_value={"duckdb_version": "v1.5.4",
                                     "duckdb_ai_extension_version": "0.4.14",
                                     "duckdb_ai_extension_source": "community"}), \
                 patch("squad_database_e2e_runner._pg_server_identity",
                       return_value={"pg_server_version": "PG 99", "pgvector_version": "0.8.5"}), \
                 patch("squad_database_e2e_runner._vllm_version", return_value="0.25.1"), \
                 patch("squad_database_e2e_runner._gpu_identity",
                       return_value={"nvidia_smi": [], "hostname": "h"}), \
                 patch("squad_database_e2e_runner._git_commit", return_value="deadbeef"), \
                 patch.dict(sys.modules, {"psycopg": MagicMock()}):
                rc = runner.main(argv)
            report = json.loads((out / "report.json").read_text(encoding="utf-8"))
            with (out / "per_row_evidence.csv").open(encoding="utf-8") as f:
                import csv as _csv
                header = next(_csv.reader(f))
            return rc, report, out, header

    def test_success_path_e2e_structure(self) -> None:
        rc, report, _, header = self._run_main(fail_one=False)
        self.assertEqual(rc, 0)
        self.assertEqual(report["status"], "success")
        self.assertEqual(report["timing"]["boundary"], "database_e2e")
        for seg in ("database_e2e_wall_s", "scan_s", "construct_s", "adapter_wall_s",
                    "operator_only_jct_s", "sink_s"):
            self.assertIn(seg, report["timing"], seg)
        # All E2E segment fields present and non-negative. database_e2e_wall_s is
        # the real elapsed barrier (rounded to 3 decimals -> 0.0 under fast mocks;
        # it is seconds-positive on a real run, verified on the server). The
        # wall >= sum(segments) invariant also cannot be asserted here: mocked
        # scan_s/sink_s are constants, not real sub-intervals of the wall.
        wall = report["timing"]["database_e2e_wall_s"]
        self.assertGreaterEqual(wall, 0.0)
        for seg in ("scan_s", "construct_s", "adapter_wall_s",
                    "operator_only_jct_s", "sink_s"):
            self.assertGreaterEqual(report["timing"][seg], 0.0)
        self.assertEqual(report["formal_run_gate_passed"], True)
        self.assertEqual(report["comparison_admission"], "eligible_unconditional")
        self.assertEqual(report["capability_gate_status"], "success")
        self.assertIn("correct_rows_per_s", report["runner_metrics"])
        self.assertIn("server_version", header)
        self.assertIn("pgvector_version", header)
        self.assertEqual(report["sink"]["table"], "document_completions")

    def test_fail_closed_keeps_eligibility_separate(self) -> None:
        rc, report, _, _ = self._run_main(fail_one=True)
        self.assertEqual(rc, 1)
        self.assertEqual(report["status"], "failure")
        self.assertEqual(report["formal_run_gate_passed"], False)
        self.assertEqual(report["capability_gate_status"], "failure")
        self.assertEqual(report["comparison_admission"], "eligible_with_documented_failure")
        self.assertIsNotNone(report["failure_reason"])
        # failed cells still feed runner metrics
        self.assertIn("failure_rate", report["runner_metrics"])
        self.assertGreater(report["runner_metrics"]["failure_rate"], 0.0)


if __name__ == "__main__":
    unittest.main()
