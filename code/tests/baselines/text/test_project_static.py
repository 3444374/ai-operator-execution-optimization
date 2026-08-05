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

_PS_PATH = CODE_ROOT / "src" / "baselines" / "text" / "products" / "project_static.py"
_spec = importlib.util.spec_from_file_location("project_static", _PS_PATH)
ps = importlib.util.module_from_spec(_spec)
sys.modules["project_static"] = ps
_spec.loader.exec_module(ps)


def _cfg(**overrides) -> "ps.ProjectStaticConfig":
    base = dict(
        database_url="postgresql://u:p@localhost:5432/d",
        workload_name="squad_v11_dev_short_answer",
        endpoint_url="http://127.0.0.1:8000/v1/chat/completions",
        model="qwen2.5-7b", max_tokens=64,
        token_budget=6144, max_inflight=8,
    )
    base.update(overrides)
    return ps.ProjectStaticConfig(**base)


class ProjectStaticConfigTests(unittest.TestCase):
    def test_valid(self) -> None:
        c = _cfg()
        self.assertEqual(c.writeback_mode, "json_text")
        self.assertEqual(c.scenario_id, "project_static")

    def test_rejects_empty_required(self) -> None:
        for bad in (dict(database_url=""), dict(workload_name=""),
                    dict(endpoint_url=""), dict(model="")):
            with self.assertRaises(ValueError):
                _cfg(**bad)

    def test_rejects_unfrozen_k_or_budget(self) -> None:
        # token_budget + max_inflight are the frozen static values; zero/negative
        # must be rejected so a caller never runs an un-frozen guess.
        with self.assertRaises(ValueError):
            _cfg(token_budget=0)
        with self.assertRaises(ValueError):
            _cfg(max_inflight=0)

    def test_rejects_bad_writeback_mode(self) -> None:
        with self.assertRaises(ValueError):
            _cfg(writeback_mode="pgvector")


class BuildProfilerArgvTests(unittest.TestCase):
    def test_contains_frozen_static_k_flags(self) -> None:
        argv = ps.build_profiler_argv(
            _cfg(), "trace.csv", "summary.csv",
        )
        def pair(flag: str) -> str:
            return argv[argv.index(flag) + 1]
        # frozen static-K pipeline identity
        self.assertEqual(pair("--operator"), "ai_complete")
        self.assertEqual(pair("--scheduling-policy"), "static")
        self.assertEqual(pair("--admission-scope"), "per_endpoint")
        self.assertEqual(pair("--max-inflight"), "8")  # frozen per-endpoint K
        self.assertEqual(pair("--batching-policy"), "token_budget")
        self.assertEqual(pair("--token-budget"), "6144")
        self.assertEqual(pair("--token-budget-policy"), "static")
        self.assertEqual(pair("--executor"), "ray_actor")
        self.assertEqual(pair("--submission-granularity"), "request")
        self.assertEqual(pair("--completion-protocol"), "chat_completions")
        # single formal run (exactly one formal CSV row)
        self.assertEqual(pair("--run-phase"), "formal")
        self.assertEqual(pair("--run-repeat-index"), "1")
        self.assertEqual(pair("--warmup-runs"), "0")
        self.assertEqual(pair("--repeats"), "1")
        # profiler owns scan + sink
        self.assertEqual(pair("--source-workload-name"), "squad_v11_dev_short_answer")
        self.assertEqual(pair("--data-source"), "daft_postgres")
        self.assertEqual(pair("--organizer"), "daft")
        self.assertEqual(pair("--writeback-mode"), "json_text")
        # evidence emit paths
        self.assertEqual(pair("--request-trace-output"), "trace.csv")
        self.assertEqual(pair("--output"), "summary.csv")

    def test_total_rows_only_when_positive(self) -> None:
        self.assertNotIn("--total-rows", ps.build_profiler_argv(_cfg(), "t", "s"))
        argv = ps.build_profiler_argv(_cfg(total_rows=256), "t", "s")
        self.assertEqual(argv[argv.index("--total-rows") + 1], "256")


class ParseHelpersTests(unittest.TestCase):
    def test_to_float_empty_is_zero(self) -> None:
        self.assertEqual(ps._to_float(""), 0.0)
        self.assertEqual(ps._to_float(None), 0.0)
        self.assertEqual(ps._to_float("1.5"), 1.5)

    def test_to_int(self) -> None:
        self.assertEqual(ps._to_int("12"), 12)
        self.assertEqual(ps._to_int(""), 0)
        self.assertEqual(ps._to_int("7.0"), 7)

    def test_parse_endpoint_index(self) -> None:
        self.assertEqual(ps._parse_endpoint_index("endpoint-0"), 0)
        self.assertEqual(ps._parse_endpoint_index("endpoint-3"), 3)
        self.assertEqual(ps._parse_endpoint_index(""), 0)
        self.assertEqual(ps._parse_endpoint_index("5"), 5)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


class ReadRequestTraceTests(unittest.TestCase):
    def test_keyed_by_doc_id_skips_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "trace.csv"
            _write_csv(p, ["doc_id", "status", "submit_epoch_s"],
                       [{"doc_id": "10", "status": "completed", "submit_epoch_s": "1.0"},
                        {"doc_id": "", "status": "completed", "submit_epoch_s": "2.0"},
                        {"doc_id": "20", "status": "failed", "submit_epoch_s": "3.0"}])
            by_doc = ps.read_request_trace(p)
        self.assertEqual(set(by_doc), {10, 20})
        self.assertEqual(by_doc[10]["status"], "completed")


class ReadSummaryTimingTests(unittest.TestCase):
    _FIELDS = ["status", "phase", "scenario_id", "e2e_s", "operator_wall_s",
               "writeback_s", "db_fetch_s"]

    def _csv(self, td: str, rows: list[dict]) -> Path:
        p = Path(td) / "summary.csv"
        _write_csv(p, self._FIELDS, rows)
        return p

    def test_finds_formal_ok_row_and_skips_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = self._csv(td, [
                {"status": "dry_run", "phase": "formal", "scenario_id": "project_static",
                 "e2e_s": "0", "operator_wall_s": "0", "writeback_s": "0", "db_fetch_s": "0"},
                {"status": "ok", "phase": "formal", "scenario_id": "project_static",
                 "e2e_s": "91.5", "operator_wall_s": "90.0", "writeback_s": "0.3",
                 "db_fetch_s": "0.1"},
            ])
            timing, found = ps.read_summary_timing(p, "project_static")
        self.assertTrue(found)
        self.assertAlmostEqual(timing["e2e_s"], 91.5)
        self.assertAlmostEqual(timing["operator_wall_s"], 90.0)

    def test_no_formal_ok_returns_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = self._csv(td, [
                {"status": "dry_run", "phase": "formal", "scenario_id": "project_static",
                 "e2e_s": "0", "operator_wall_s": "0", "writeback_s": "0", "db_fetch_s": "0"},
            ])
            timing, found = ps.read_summary_timing(p, "project_static")
        self.assertFalse(found)
        self.assertEqual(timing, {})

    def test_skips_wrong_scenario(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = self._csv(td, [
                {"status": "ok", "phase": "formal", "scenario_id": "other",
                 "e2e_s": "5", "operator_wall_s": "4", "writeback_s": "0.1",
                 "db_fetch_s": "0.1"},
            ])
            _, found = ps.read_summary_timing(p, "project_static")
        self.assertFalse(found)

    def test_picks_last_formal_ok_row_when_multiple(self) -> None:
        # Defense-in-depth for the stale-file bug: even if a stale row survived
        # (append-mode CSV on a reused dir), the LAST formal-ok row wins so fresh
        # timing is never shadowed by stale timing. run_project_static also clears
        # the work dir per invocation; this test pins the reader's last-wins too.
        with tempfile.TemporaryDirectory() as td:
            p = self._csv(td, [
                {"status": "ok", "phase": "formal", "scenario_id": "project_static",
                 "e2e_s": "10.0", "operator_wall_s": "9", "writeback_s": "0.1",
                 "db_fetch_s": "0.1"},
                {"status": "ok", "phase": "formal", "scenario_id": "project_static",
                 "e2e_s": "50.0", "operator_wall_s": "49", "writeback_s": "0.2",
                 "db_fetch_s": "0.1"},
            ])
            timing, found = ps.read_summary_timing(p, "project_static")
        self.assertTrue(found)
        self.assertAlmostEqual(timing["e2e_s"], 50.0)  # last row, not first


class MergeResultsTests(unittest.TestCase):
    def test_completed_row_merges_output_text_and_tokens(self) -> None:
        trace = {10: {
            "endpoint_id": "endpoint-0", "status": "completed", "error_type": "",
            "submit_epoch_s": "1.0", "service_start_epoch_s": "1.5",
            "completion_epoch_s": "2.0", "prompt_tokens": "40",
            "actual_output_tokens": "12", "finish_reason": "stop",
        }}
        out = {10: "the answer"}
        results = ps.merge_results(trace, out)
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertEqual(r.doc_id, 10)
        self.assertEqual(r.status, "completed")
        self.assertEqual(r.output_text, "the answer")
        self.assertEqual(r.finish_reason, "stop")
        self.assertEqual(r.input_tokens, 40)
        self.assertEqual(r.output_tokens, 12)
        self.assertEqual(r.endpoint_index, 0)
        self.assertEqual(r.started_at_s, 1.5)

    def test_failed_row_falls_back_started_to_completed(self) -> None:
        # Profiler leaves service_start_epoch_s empty when a submission never
        # reached the service; started must fall back to COMPLETED (not submitted)
        # so the failed row's queue time does not pull down min(started) and
        # overstate the runner's operator span.
        trace = {20: {
            "endpoint_id": "endpoint-1", "status": "failed", "error_type": "boom",
            "submit_epoch_s": "5.0", "service_start_epoch_s": "",
            "completion_epoch_s": "5.5", "prompt_tokens": "30",
            "actual_output_tokens": "", "finish_reason": "",
        }}
        results = ps.merge_results(trace, {})
        r = results[0]
        self.assertEqual(r.status, "failed")
        self.assertEqual(r.error, "boom")
        self.assertEqual(r.started_at_s, 5.5)  # fell back to completed, not submitted
        self.assertEqual(r.completed_at_s, 5.5)
        self.assertEqual(r.output_tokens, 0)
        self.assertIsNone(r.output_text)
        self.assertIsNone(r.finish_reason)
        self.assertEqual(r.endpoint_index, 1)

    def test_status_other_than_completed_becomes_failed(self) -> None:
        trace = {30: {
            "endpoint_id": "endpoint-0", "status": "timeout", "error_type": "t",
            "submit_epoch_s": "1.0", "service_start_epoch_s": "1.0",
            "completion_epoch_s": "2.0", "prompt_tokens": "1",
            "actual_output_tokens": "", "finish_reason": "",
        }}
        results = ps.merge_results(trace, {})
        self.assertEqual(results[0].status, "failed")


if __name__ == "__main__":
    unittest.main()
