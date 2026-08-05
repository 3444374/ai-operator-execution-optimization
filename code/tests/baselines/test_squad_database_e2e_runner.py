from __future__ import annotations

import contextlib
import importlib.util
import json
import shutil
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

# Repo-local fallback for environments whose system temp dir is not writable
# (e.g. codex's Windows sandbox blocks tempfile writes). The repo tree is
# writable wherever it is checked out, so this keeps the integration tests
# independently reproducible.
_REPO_TMP = Path(__file__).resolve().parent / "_e2e_runner_tmp"


@contextlib.contextmanager
def _scratch_dir():
    try:
        with tempfile.TemporaryDirectory() as td:
            yield Path(td)
        return
    except (OSError, PermissionError):
        pass
    _REPO_TMP.mkdir(parents=True, exist_ok=True)
    d = Path(tempfile.mkdtemp(dir=str(_REPO_TMP)))
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


def _row(doc_id: int, sid: str, text: str = "prompt", answers: list[str] | None = None,
         tenant: int = 0, category: str = "squad") -> tuple[dict, tuple]:
    return (
        {"doc_id": doc_id, "text": text, "source_example_id": sid,
         "answers": answers or ["ans"]},
        (tenant, category),
    )


def _result(req_doc_id: int, *, output_text="ok", error=None, status="completed",
            submitted=1.0, started=1.0, completed=2.0):
    return BaselineRequestResult(
        doc_id=req_doc_id, endpoint_index=0, status=status, error=error,
        submitted_at_s=submitted, started_at_s=started, completed_at_s=completed,
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
    def test_division_and_rates(self) -> None:
        m = runner._runner_metrics(em_rows=100, success_count=120, row_count=150,
                                   error_count=30, null_count=0, max_tokens_errors=5,
                                   wall_s=10.0, sunk_rows=120)
        self.assertEqual(m["correct_rows_per_s"], 10.0)
        self.assertEqual(m["successful_rows_per_s"], 12.0)
        self.assertEqual(m["raw_rows_per_s"], 15.0)
        self.assertEqual(m["failed_rows"], 30)  # 150 - 120 (unduplicated)
        self.assertEqual(m["failure_rate"], round(30 / 150, 6))
        self.assertEqual(m["error_rate"], round(30 / 150, 6))
        self.assertEqual(m["null_rate"], 0.0)
        self.assertEqual(m["max_tokens_rate"], round(5 / 150, 6))
        self.assertEqual(m["sunk_rows"], 120)

    def test_failure_rate_dedups_error_and_null_same_row(self) -> None:
        # One row that is BOTH an error AND a NULL response is ONE failed row,
        # not two. failure_rate must be 1/100, not 2/100. error_rate and
        # null_rate are reported separately and MAY overlap.
        m = runner._runner_metrics(em_rows=0, success_count=99, row_count=100,
                                   error_count=1, null_count=1, max_tokens_errors=1,
                                   wall_s=10.0, sunk_rows=100)
        self.assertEqual(m["failed_rows"], 1)
        self.assertEqual(m["failure_rate"], round(1 / 100, 6))
        self.assertNotEqual(m["failure_rate"], round(2 / 100, 6))
        self.assertEqual(m["error_rate"], round(1 / 100, 6))
        self.assertEqual(m["null_rate"], round(1 / 100, 6))

    def test_zero_wall_is_safe(self) -> None:
        m = runner._runner_metrics(10, 10, 10, 0, 0, 0, wall_s=0.0, sunk_rows=10)
        self.assertEqual(m["correct_rows_per_s"], 0.0)
        self.assertEqual(m["raw_rows_per_s"], 0.0)


class OperatorSpanTests(unittest.TestCase):
    def test_barrier_arm_all_rows_share_boundary(self) -> None:
        # DuckDB-ai: every row shares submitted/started/completed.
        results = [_result(i, submitted=1.0, started=2.0, completed=5.0) for i in range(5)]
        jct, setup = runner._operator_span(results)
        self.assertEqual(jct, 3.0)  # 5 - 2
        self.assertEqual(setup, 1.0)  # 2 - 1

    def test_per_request_arm_uses_min_started_max_completed(self) -> None:
        # direct_client: each row has its own started/completed; using results[0]
        # would be wrong. Span must be max(completed) - min(started).
        results = [
            _result(1, submitted=1.0, started=2.0, completed=3.0),
            _result(2, submitted=1.5, started=4.0, completed=10.0),
            _result(3, submitted=2.0, started=6.0, completed=7.0),
        ]
        jct, setup = runner._operator_span(results)
        self.assertEqual(jct, 8.0)  # max(10,7,3) - min(2,4,6)
        self.assertEqual(setup, 1.0)  # min(started)=2 - min(submitted)=1

    def test_empty(self) -> None:
        self.assertEqual(runner._operator_span([]), (0.0, 0.0))


class DatabaseE2EBarrierTests(unittest.TestCase):
    """Mocked end-to-end main() run: assert E2E timing block + decoupled state."""

    def _run_main(self, fail_one: bool) -> tuple[int, dict, list[str]]:
        rows_sidecar = [_row(i, f"id{i}", text=f"p{i}") for i in range(1, 5)]
        rows = [r for r, _ in rows_sidecar]
        sidecar = {r["doc_id"]: tc for r, tc in rows_sidecar}
        with _scratch_dir() as td:
            out = td / "result"
            prov = td / "prov.json"
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
            import csv as _csv
            with (out / "sink_audit.csv").open(encoding="utf-8") as f:
                sink_header = next(_csv.reader(f))
            return rc, report, sink_header

    def test_success_path_e2e_structure_and_decoupled_state(self) -> None:
        rc, report, sink_header = self._run_main(fail_one=False)
        self.assertEqual(rc, 0)
        self.assertEqual(report["status"], "success")
        self.assertEqual(report["timing"]["boundary"], "database_e2e")
        for seg in ("database_e2e_wall_s", "scan_s", "construct_s", "adapter_wall_s",
                    "operator_only_jct_s", "sink_s"):
            self.assertIn(seg, report["timing"], seg)
        self.assertGreaterEqual(report["timing"]["database_e2e_wall_s"], 0.0)
        # State fields DECOUPLED: single clean shot does NOT pass the formal gate.
        self.assertEqual(report["single_run_valid"], True)
        self.assertEqual(report["formal_run_gate_passed"], False)
        self.assertEqual(report["comparison_admission"], "pending_formal_repeat")
        self.assertIn("correct_rows_per_s", report["runner_metrics"])
        # sink audit recovers per-row status (so sunk empty strings are traceable)
        self.assertIn("doc_id", sink_header)
        self.assertEqual(report["evidence_files"]["sink_audit_csv"], "sink_audit.csv")
        self.assertEqual(report["sink"]["table"], "document_completions")

    def test_fail_closed_keeps_state_decoupled(self) -> None:
        rc, report, _ = self._run_main(fail_one=True)
        self.assertEqual(rc, 1)
        self.assertEqual(report["status"], "failure")
        # Decoupled: a failed single shot does NOT auto-gain comparison admission.
        self.assertEqual(report["single_run_valid"], False)
        self.assertEqual(report["formal_run_gate_passed"], False)
        self.assertEqual(report["comparison_admission"], "pending_formal_repeat")
        self.assertIsNotNone(report["failure_reason"])
        # failure_rate is unduplicated (1 failed row, not error+null=2)
        self.assertEqual(report["runner_metrics"]["failed_rows"], 1)
        self.assertEqual(report["runner_metrics"]["failure_rate"],
                         round(1 / report["row_count"], 6))


if __name__ == "__main__":
    unittest.main()
