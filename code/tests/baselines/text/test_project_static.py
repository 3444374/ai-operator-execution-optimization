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
        max_active_work_per_endpoint=65536,
        actor_workers_per_endpoint=8, ray_actor_max_concurrency=1,
    )
    base.update(overrides)
    return ps.ProjectStaticConfig(**base)


class ProjectStaticConfigTests(unittest.TestCase):
    def test_valid(self) -> None:
        c = _cfg()
        self.assertEqual(c.writeback_mode, "json_text")
        self.assertEqual(c.completion_temperature, 0.0)
        self.assertEqual(c.completion_http_transport, "httpx_async")
        self.assertEqual(c.service_prefix_caching, "enabled")

    def test_rejects_empty_required(self) -> None:
        for bad in (dict(database_url=""), dict(workload_name=""),
                    dict(endpoint_url=""), dict(model="")):
            with self.assertRaises(ValueError):
                _cfg(**bad)

    def test_rejects_unfrozen_k_or_budget(self) -> None:
        with self.assertRaises(ValueError):
            _cfg(token_budget=0)
        with self.assertRaises(ValueError):
            _cfg(max_inflight=0)

    def test_rejects_actor_topology_below_k(self) -> None:
        # The #2 contract fix: actor_workers * concurrency < max_inflight would let
        # the profiler silently clamp effective K to the slot count. Must fail closed.
        with self.assertRaises(ValueError):
            _cfg(actor_workers_per_endpoint=2, ray_actor_max_concurrency=1)  # 2 < 8
        with self.assertRaises(ValueError):
            _cfg(actor_workers_per_endpoint=4, ray_actor_max_concurrency=1)  # 4 < 8

    def test_effective_k_equals_declared_when_topology_sufficient(self) -> None:
        self.assertEqual(_cfg(actor_workers_per_endpoint=8, ray_actor_max_concurrency=1).effective_k, 8)
        self.assertEqual(_cfg(actor_workers_per_endpoint=4, ray_actor_max_concurrency=2).effective_k, 8)
        self.assertEqual(_cfg(actor_workers_per_endpoint=2, ray_actor_max_concurrency=4).effective_k, 8)

    def test_rejects_bad_transport_or_prefix(self) -> None:
        with self.assertRaises(ValueError):
            _cfg(completion_http_transport="requests")
        with self.assertRaises(ValueError):
            _cfg(service_prefix_caching="maybe")


class BuildProfilerArgvTests(unittest.TestCase):
    def test_locks_frozen_static_k_and_request_semantics(self) -> None:
        argv = ps.build_profiler_argv(
            _cfg(), "trace.csv", "evidence.csv", "source.csv", "summary.csv",
        )

        def pair(flag: str) -> str:
            return argv[argv.index(flag) + 1]

        # effective-K topology (actor pool slots >= K, so effective K == declared K)
        self.assertEqual(pair("--max-inflight"), "8")
        self.assertEqual(pair("--max-active-work-per-endpoint"), "65536")
        self.assertEqual(pair("--actor-workers-per-endpoint"), "8")
        self.assertEqual(pair("--ray-actor-max-concurrency"), "1")
        # frozen request semantics (parity with the direct arm)
        self.assertEqual(pair("--completion-temperature"), "0.0")
        self.assertEqual(pair("--completion-http-transport"), "httpx_async")
        self.assertEqual(pair("--completion-prompt-format"), "raw")
        self.assertEqual(pair("--output-cost-mode"), "fixed_output_cap")
        self.assertEqual(pair("--service-prefix-caching"), "enabled")
        # completion evidence (independent output_text source, non-circular readback)
        self.assertEqual(pair("--completion-evidence-output"), "evidence.csv")
        # request-trace still requested (populates lifecycle events the evidence joins)
        self.assertEqual(pair("--request-trace-output"), "trace.csv")
        self.assertEqual(pair("--source-scan-evidence-output"), "source.csv")
        # frozen static-K pipeline identity
        self.assertEqual(pair("--operator"), "ai_complete")
        self.assertEqual(pair("--scheduling-policy"), "static")
        self.assertEqual(pair("--admission-scope"), "per_endpoint")
        self.assertEqual(pair("--batching-policy"), "token_budget")
        self.assertEqual(pair("--executor"), "ray_actor")
        self.assertEqual(pair("--submission-granularity"), "request")
        self.assertEqual(pair("--run-phase"), "formal")

    def test_request_manifest_guard_not_used_single_endpoint(self) -> None:
        # The cryptographically pinned request-set manifest guard is a 2-endpoint
        # pinned-comparison mechanism (validate_profile_manifest_contract requires
        # endpoint_count >= 2). This single-endpoint arm MUST NOT pass it.
        argv = ps.build_profiler_argv(_cfg(), "t", "e", "source", "s")
        self.assertNotIn("--request-manifest", argv)
        self.assertNotIn("manifest_pinned", argv)

    def test_total_rows_only_when_positive(self) -> None:
        self.assertNotIn("--total-rows", ps.build_profiler_argv(_cfg(), "t", "e", "source", "s"))
        argv = ps.build_profiler_argv(_cfg(total_rows=256), "t", "e", "source", "s")
        self.assertEqual(argv[argv.index("--total-rows") + 1], "256")

    def test_single_endpoint_uses_plural_flag_with_one_url(self) -> None:
        # The profiler reads --completion-endpoint-urls (comma-split); a single
        # URL still goes through the plural flag so resolved_endpoint_urls is the
        # single 1-tuple and the profiler round-robins over one endpoint.
        argv = ps.build_profiler_argv(_cfg(), "t", "e", "source", "s")
        self.assertEqual(
            argv[argv.index("--completion-endpoint-urls") + 1],
            "http://127.0.0.1:8000/v1/chat/completions",
        )
        self.assertNotIn("--completion-endpoint-url,", ",".join(argv))
        cfg = _cfg()
        self.assertEqual(cfg.resolved_endpoint_urls, (cfg.endpoint_url,))

    def test_multi_endpoint_emits_comma_joined_urls(self) -> None:
        # The ramp needs project_static to hit BOTH endpoints (comparable to the
        # 2-endpoint gate arms). endpoint_urls overrides endpoint_url.
        cfg = _cfg(endpoint_urls=(
            "http://127.0.0.1:8000/v1/chat/completions",
            "http://127.0.0.1:8001/v1/chat/completions",
        ))
        argv = ps.build_profiler_argv(cfg, "t", "e", "source", "s")
        self.assertEqual(
            argv[argv.index("--completion-endpoint-urls") + 1],
            "http://127.0.0.1:8000/v1/chat/completions,"
            "http://127.0.0.1:8001/v1/chat/completions",
        )
        self.assertEqual(len(cfg.resolved_endpoint_urls), 2)

    def test_multi_endpoint_rejects_empty_url_in_list(self) -> None:
        with self.assertRaises(ValueError):
            _cfg(endpoint_urls=("http://127.0.0.1:8000/v1/chat/completions", ""))


class ParseHelpersTests(unittest.TestCase):
    def test_to_float_empty_is_zero(self) -> None:
        self.assertEqual(ps._to_float(""), 0.0)
        self.assertEqual(ps._to_float(None), 0.0)
        self.assertEqual(ps._to_float("1.5"), 1.5)

    def test_to_int(self) -> None:
        self.assertEqual(ps._to_int("12"), 12)
        self.assertEqual(ps._to_int(""), 0)
        self.assertEqual(ps._to_int("7.0"), 7)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


class ReadCompletionEvidenceTests(unittest.TestCase):
    _FIELDS = ["doc_id", "prompt_tokens", "output_tokens", "output_text",
               "status", "error_type", "finish_reason", "submit_epoch_s",
               "service_start_epoch_s", "completion_epoch_s"]

    def test_keyed_by_doc_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "evidence.csv"
            _write_csv(p, self._FIELDS, [
                {"doc_id": "10", "prompt_tokens": "40", "output_tokens": "12",
                 "output_text": "the answer", "status": "completed",
                 "error_type": "", "finish_reason": "stop",
                 "submit_epoch_s": "1.0", "service_start_epoch_s": "1.5",
                 "completion_epoch_s": "2.0"},
                {"doc_id": "20", "prompt_tokens": "30", "output_tokens": "0",
                 "output_text": "", "status": "failed", "error_type": "boom",
                 "finish_reason": "", "submit_epoch_s": "5.0",
                 "service_start_epoch_s": "", "completion_epoch_s": "5.5"},
            ])
            by_doc = ps.read_completion_evidence(p)
        self.assertEqual(set(by_doc), {10, 20})
        self.assertEqual(by_doc[10]["output_text"], "the answer")

    def test_malformed_doc_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "evidence.csv"
            base = {field: "" for field in self._FIELDS}
            _write_csv(p, self._FIELDS, [{**base, "doc_id": ""}])
            with self.assertRaises(ValueError):
                ps.read_completion_evidence(p)

    def test_duplicate_doc_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "evidence.csv"
            base = {field: "" for field in self._FIELDS}
            _write_csv(p, self._FIELDS, [
                {**base, "doc_id": "10"}, {**base, "doc_id": "10"},
            ])
            with self.assertRaises(ValueError):
                ps.read_completion_evidence(p)


class ReadSourceScanEvidenceTests(unittest.TestCase):
    def test_valid_and_duplicate_rejected(self) -> None:
        fields = ["doc_id", "text_sha256", "prompt_tokens", "tenant_id", "category"]
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "source.csv"
            row = {"doc_id": "1", "text_sha256": "a" * 64,
                   "prompt_tokens": "3", "tenant_id": "0", "category": "squad"}
            _write_csv(p, fields, [row])
            self.assertEqual(ps.read_source_scan_evidence(p), ((1, "a" * 64),))
            _write_csv(p, fields, [row, row])
            with self.assertRaises(ValueError):
                ps.read_source_scan_evidence(p)


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

    def test_no_formal_ok_returns_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = self._csv(td, [
                {"status": "dry_run", "phase": "formal", "scenario_id": "project_static",
                 "e2e_s": "0", "operator_wall_s": "0", "writeback_s": "0", "db_fetch_s": "0"},
            ])
            timing, found = ps.read_summary_timing(p, "project_static")
        self.assertFalse(found)
        self.assertEqual(timing, {})

    def test_picks_last_formal_ok_row_when_multiple(self) -> None:
        # Defense-in-depth for the stale-file bug: even if a stale row survived
        # (append-mode CSV on a reused dir), the LAST formal-ok row wins.
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
        self.assertAlmostEqual(timing["e2e_s"], 50.0)


class MergeResultsTests(unittest.TestCase):
    def test_completed_row_merges_output_text_and_tokens(self) -> None:
        evidence = {10: {
            "prompt_tokens": "40", "output_tokens": "12", "output_text": "the answer",
            "status": "completed", "error_type": "", "finish_reason": "stop",
            "submit_epoch_s": "1.0", "service_start_epoch_s": "1.5",
            "completion_epoch_s": "2.0",
        }}
        results = ps.merge_results(evidence)
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertEqual(r.doc_id, 10)
        self.assertEqual(r.status, "completed")
        self.assertEqual(r.output_text, "the answer")
        self.assertEqual(r.finish_reason, "stop")
        self.assertEqual(r.input_tokens, 40)
        self.assertEqual(r.output_tokens, 12)
        self.assertEqual(r.endpoint_index, 0)  # single-endpoint arm
        self.assertEqual(r.started_at_s, 1.5)

    def test_failed_row_falls_back_started_to_completed(self) -> None:
        # Empty service_start_epoch_s (never reached the service): started falls
        # back to completed so the failed row does not pull down min(started).
        evidence = {20: {
            "prompt_tokens": "30", "output_tokens": "0", "output_text": "",
            "status": "failed", "error_type": "boom", "finish_reason": "",
            "submit_epoch_s": "5.0", "service_start_epoch_s": "",
            "completion_epoch_s": "5.5",
        }}
        results = ps.merge_results(evidence)
        r = results[0]
        self.assertEqual(r.status, "failed")
        self.assertEqual(r.error, "boom")
        self.assertEqual(r.started_at_s, 5.5)  # fell back to completed
        self.assertEqual(r.completed_at_s, 5.5)
        self.assertIsNone(r.output_text)  # empty evidence output_text -> None
        self.assertIsNone(r.finish_reason)

    def test_status_other_than_completed_becomes_failed(self) -> None:
        evidence = {30: {
            "prompt_tokens": "1", "output_tokens": "0", "output_text": "",
            "status": "timeout", "error_type": "t", "finish_reason": "",
            "submit_epoch_s": "1.0", "service_start_epoch_s": "1.0",
            "completion_epoch_s": "2.0",
        }}
        results = ps.merge_results(evidence)
        self.assertEqual(results[0].status, "failed")


if __name__ == "__main__":
    unittest.main()
