from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

_GATE_PATH = CODE_ROOT / "scripts" / "baselines" / "squad_capability_gate.py"
_spec = importlib.util.spec_from_file_location("squad_capability_gate", _GATE_PATH)
gate = importlib.util.module_from_spec(_spec)
sys.modules["squad_capability_gate"] = gate
_spec.loader.exec_module(gate)

_IMPORTER_PATH = CODE_ROOT / "scripts" / "data" / "import_squad_workload.py"
_ispec = importlib.util.spec_from_file_location("import_squad_workload", _IMPORTER_PATH)
importer = importlib.util.module_from_spec(_ispec)
sys.modules["import_squad_workload"] = importer
_ispec.loader.exec_module(importer)


def _row(example_id: str, answers: list[str]) -> dict:
    return {
        "doc_id": abs(hash(example_id)) % (10**12),
        "text": "prompt " + example_id,
        "source_example_id": example_id,
        "answers": answers,
    }


def _fixture() -> list[dict]:
    # 30 short (1 word) + 40 medium (2 words) + 30 long (5 words) = 100
    rows = []
    for i in range(30):
        rows.append(_row(f"short{i:02d}", ["yes"]))
    for i in range(40):
        rows.append(_row(f"medium{i:02d}", ["two words"]))
    for i in range(30):
        rows.append(_row(f"long{i:02d}", ["one two three four five"]))
    return rows


class AnswerBucketTests(unittest.TestCase):
    def test_single_answer_buckets(self) -> None:
        self.assertEqual(gate._answer_bucket(["yes"]), "short")
        self.assertEqual(gate._answer_bucket(["answer in Paris"]), "medium")
        self.assertEqual(gate._answer_bucket(["one two three four five"]), "long")

    def test_bucket_uses_squad_normalization(self) -> None:
        # Articles and punctuation do not count toward the SQuAD-normalized
        # reference length used for stratification.
        self.assertEqual(gate._answer_bucket(["The answer!"]), "short")

    def test_multi_answer_uses_max_not_first(self) -> None:
        # codex #6: bucket must not depend on answer array order.
        self.assertEqual(gate._answer_bucket(["yes", "a longer answer here"]), "medium")
        self.assertEqual(gate._answer_bucket(["a longer answer here", "yes"]), "medium")
        self.assertEqual(
            gate._answer_bucket(["x", "one two three four five words"]),
            "long",
        )

    def test_empty_answers_is_short(self) -> None:
        self.assertEqual(gate._answer_bucket([]), "short")


class LargestRemainderTests(unittest.TestCase):
    def test_sum_equals_target(self) -> None:
        sizes = [30, 40, 30]
        alloc = gate.largest_remainder_allocation(sizes, 10)
        self.assertEqual(sum(alloc), 10)
        self.assertEqual(len(alloc), 3)

    def test_proportional(self) -> None:
        # 30:40:30 of 100 -> 10 target -> 3:4:3
        self.assertEqual(gate.largest_remainder_allocation([30, 40, 30], 10), [3, 4, 3])

    def test_target_smaller_than_bucket_count(self) -> None:
        # codex #7: target < number of buckets must still sum to target.
        alloc = gate.largest_remainder_allocation([10, 10, 10], 2)
        self.assertEqual(sum(alloc), 2)
        self.assertLessEqual(max(alloc), 1)

    def test_round_sum_exceeds_target_handled(self) -> None:
        # A case where naive max(1, round(...)) would overshoot: 3 buckets,
        # target 2, each round(2*1/3)=1 -> naive sum 3 > 2. Largest-remainder
        # floors then distributes, so sum == 2.
        alloc = gate.largest_remainder_allocation([1, 1, 1], 2)
        self.assertEqual(sum(alloc), 2)

    def test_target_exceeds_total_capped_at_sizes(self) -> None:
        alloc = gate.largest_remainder_allocation([2, 3], 100)
        self.assertEqual(alloc, [2, 3])


class StratifiedSampleTests(unittest.TestCase):
    def test_deterministic_and_exact_count(self) -> None:
        rows = _fixture()
        first = gate.stratified_sample(rows, 10)
        second = gate.stratified_sample(rows, 10)
        self.assertEqual(len(first), 10)
        self.assertEqual(
            [r["source_example_id"] for r in first],
            [r["source_example_id"] for r in second],
        )
        ids = [r["source_example_id"] for r in first]
        self.assertEqual(len(set(ids)), 10)

    def test_target_exceeds_total_returns_all(self) -> None:
        rows = _fixture()[:5]
        sampled = gate.stratified_sample(rows, 10)
        self.assertEqual(len(sampled), 5)

    def test_stratification_covers_all_buckets(self) -> None:
        rows = _fixture()
        sampled = gate.stratified_sample(rows, 30)
        buckets_present = {gate._answer_bucket(r["answers"]) for r in sampled}
        self.assertEqual(buckets_present, {"short", "medium", "long"})

    def test_input_order_invariance(self) -> None:
        # codex #7: shuffling input must not change the selected IDs.
        rows = _fixture()
        forward = gate.stratified_sample(rows, 25)
        reversed_rows = list(reversed(rows))
        backward = gate.stratified_sample(reversed_rows, 25)
        self.assertEqual(
            [r["source_example_id"] for r in forward],
            [r["source_example_id"] for r in backward],
        )


class StructuredContentHashTests(unittest.TestCase):
    def test_stable(self) -> None:
        rows = _fixture()[:12]
        self.assertEqual(
            gate._structured_content_hash(rows),
            gate._structured_content_hash(rows),
        )

    def test_input_order_invariant(self) -> None:
        rows = _fixture()[:12]
        self.assertEqual(
            gate._structured_content_hash(rows),
            gate._structured_content_hash(list(reversed(rows))),
        )

    def test_id_change_changes_hash(self) -> None:
        # codex #8: a different selection (different source id) must hash
        # differently; bare-ID concat can collide, structured JSON must not.
        rows_a = _fixture()[:5]
        rows_b = [dict(r, source_example_id=r["source_example_id"] + "x") for r in rows_a]
        self.assertNotEqual(
            gate._structured_content_hash(rows_a),
            gate._structured_content_hash(rows_b),
        )

    def test_prompt_change_changes_hash(self) -> None:
        rows_a = _fixture()[:5]
        rows_b = [dict(r, text=r["text"] + " changed") for r in rows_a]
        self.assertNotEqual(
            gate._structured_content_hash(rows_a),
            gate._structured_content_hash(rows_b),
        )

    def test_content_hash_matches_importer(self) -> None:
        # Pin the gate's structured hash to the importer's canonical
        # compute_content_hash so the gate's workload hash is directly
        # comparable to the archived provenance content_hash.
        rows_dict = [
            _row(f"id{i:03d}", [f"answer {i}"] if i % 2 else [f"a{i}", f"answer number {i}"])
            for i in range(6)
        ]
        importer_rows = [
            importer.SquadRow(
                source_example_id=r["source_example_id"],
                context="ctx",
                question="q?",
                reference_answers=tuple(r["answers"]),
                prompt=r["text"],
            )
            for r in rows_dict
        ]
        self.assertEqual(
            gate._structured_content_hash(rows_dict),
            importer.compute_content_hash(importer_rows),
        )

    def test_sample_manifest_recomputes_archived_hash(self) -> None:
        rows = _fixture()[:12]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample_manifest.jsonl"
            gate._write_sample_manifest(path, rows)
            decoded = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
            ]
        reconstructed = [
            {
                "source_example_id": row["id"],
                "text": row["prompt"],
                "answers": row["references"],
            }
            for row in decoded
        ]
        self.assertEqual(
            gate._structured_content_hash(reconstructed),
            gate._structured_content_hash(rows),
        )


class EvidenceDirectoryTests(unittest.TestCase):
    def test_force_removes_only_known_stale_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "result"
            output_dir.mkdir()
            for name in gate._GENERATED_EVIDENCE_FILES:
                (output_dir / name).write_text("stale", encoding="utf-8")
            unrelated = output_dir / "README.md"
            unrelated.write_text("keep", encoding="utf-8")

            gate._prepare_output_dir(output_dir, force=True)

            self.assertTrue(unrelated.exists())
            self.assertFalse(
                any((output_dir / name).exists() for name in gate._GENERATED_EVIDENCE_FILES)
            )

    def test_existing_output_requires_force(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "result"
            output_dir.mkdir()
            with self.assertRaises(SystemExit):
                gate._prepare_output_dir(output_dir, force=False)


class ServiceIdentityTests(unittest.TestCase):
    def test_vllm_version_probes_single_slash_root(self) -> None:
        requested: list[str] = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args) -> None:
                return None

            def read(self) -> bytes:
                return b'{"version":"0.25.1"}'

        def fake_urlopen(url, timeout):
            requested.append(url)
            self.assertEqual(timeout, 3.0)
            return Response()

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            version = gate._vllm_version("http://127.0.0.1:8000/v1")

        self.assertEqual(version, "0.25.1")
        self.assertEqual(requested, ["http://127.0.0.1:8000/version"])


class WorkloadIntegrityTests(unittest.TestCase):
    def _good_rows(self, count: int) -> list[dict]:
        rows = [_row(f"id{i:03d}", ["ans"]) for i in range(count)]
        return rows

    def test_good_workload_passes(self) -> None:
        rows = self._good_rows(5)
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 5, h)
        self.assertTrue(ok, problems)

    def test_wrong_count_fails(self) -> None:
        rows = self._good_rows(5)
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 6, h)
        self.assertFalse(ok)
        self.assertTrue(any("row_count" in p for p in problems))

    def test_duplicate_source_id_fails(self) -> None:
        rows = self._good_rows(5)
        rows[1] = dict(rows[0])  # duplicate source_example_id
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 5, h)
        self.assertFalse(ok)

    def test_duplicate_doc_id_fails(self) -> None:
        rows = self._good_rows(5)
        rows[1] = dict(rows[1], doc_id=rows[0]["doc_id"])  # duplicate doc_id
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 5, h)
        self.assertFalse(ok)
        self.assertTrue(any("doc_id" in p for p in problems))

    def test_blank_reference_string_fails(self) -> None:
        # answers=['']  (non-empty list, blank element) must now be rejected.
        rows = self._good_rows(5)
        rows[2] = dict(rows[2], answers=[""])
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 5, h)
        self.assertFalse(ok)
        self.assertTrue(any("reference_answers" in p for p in problems))

    def test_empty_reference_fails(self) -> None:
        rows = self._good_rows(5)
        rows[2] = dict(rows[2], answers=[])
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 5, h)
        self.assertFalse(ok)
        self.assertTrue(any("reference_answers" in p for p in problems))

    def test_hash_mismatch_fails(self) -> None:
        rows = self._good_rows(5)
        ok, problems = gate._validate_workload_integrity(rows, 5, "0" * 64)
        self.assertFalse(ok)
        self.assertTrue(any("content_hash" in p for p in problems))


class AttributionTests(unittest.TestCase):
    def test_idle_exclusive_match_attributable(self) -> None:
        before = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0,
                  "vllm:request_success_total": 100.0}
        after = {**before, "vllm:request_success_total": 356.0}
        summary, ok = gate._assess_attribution(before, after, requests_sent=256)
        self.assertTrue(ok)
        self.assertEqual(summary["request_success_delta"], 256)

    def test_not_idle_before_unavailable(self) -> None:
        before = {"vllm:num_requests_running": 5.0, "vllm:num_requests_waiting": 0.0,
                  "vllm:request_success_total": 100.0}
        after = {**before, "vllm:request_success_total": 356.0}
        _, ok = gate._assess_attribution(before, after, requests_sent=256)
        self.assertFalse(ok)

    def test_concurrent_traffic_unavailable(self) -> None:
        before = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0,
                  "vllm:request_success_total": 100.0}
        # delta 300 != 256 -> extra concurrent traffic
        after = {**before, "vllm:request_success_total": 400.0}
        summary, ok = gate._assess_attribution(before, after, requests_sent=256)
        self.assertFalse(ok)

    def test_counter_reset_unavailable(self) -> None:
        before = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0,
                  "vllm:request_success_total": 500.0,
                  "vllm:generation_tokens_total": 9999.0}
        after = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0,
                 "vllm:request_success_total": 756.0,
                 "vllm:generation_tokens_total": 10.0}  # reset
        _, ok = gate._assess_attribution(before, after, requests_sent=256)
        self.assertFalse(ok)

    def test_empty_scrape_unavailable(self) -> None:
        after = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0,
                 "vllm:request_success_total": 256.0}
        _, ok = gate._assess_attribution({}, after, requests_sent=256)
        self.assertFalse(ok)

    def test_gauge_missing_treated_as_not_idle(self) -> None:
        # Non-empty scrape but missing the running/waiting gauges: must NOT be
        # silently treated as idle.
        partial = {"vllm:request_success_total": 100.0}
        ok, reason = gate._endpoint_idle(partial)
        self.assertFalse(ok)
        self.assertEqual(reason, "gauge_missing")
        self.assertEqual(gate._scrape_status(partial), "gauge_missing")
        self.assertEqual(gate._scrape_status({}), "empty")
        full = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0}
        self.assertEqual(gate._scrape_status(full), "ok")


class RedactionWiringTests(unittest.TestCase):
    def test_command_field_redaction_strips_password_and_api_key(self) -> None:
        # Pins that the function the gate stores in report.json["command"]
        # strips the DB-URL password and masks --api-key. Regression guard
        # against a revert to raw list(sys.argv).
        from src.baselines.common.redact import redact_argument_list
        argv = [
            "python", "squad_capability_gate.py",
            "--database-url", "postgresql://postgres:postgres@localhost:5432/ai_operator",
            "--api-key", "hf_real_token",
            "--endpoint-url", "http://127.0.0.1:8000/v1/chat/completions",
        ]
        redacted = redact_argument_list(argv)
        joined = " ".join(redacted)
        self.assertNotIn("postgres:postgres@", joined)
        self.assertNotIn("hf_real_token", joined)
        self.assertIn("postgres:***@", joined)
        self.assertIn("***", joined)


class FailClosedIntegrationTests(unittest.TestCase):
    """The gate must fail (non-zero exit, status=failure) on any row-level error/NULL.

    Mocks the DB / vLLM / DuckDB boundaries so the full main()->_run path is
    exercised against a tiny in-memory workload.
    """

    def _rows(self) -> list[dict]:
        return [
            {"doc_id": i, "text": f"prompt {i}", "source_example_id": f"id{i}",
             "answers": ["ans"]}
            for i in (1, 2, 3, 4)
        ]

    @staticmethod
    def _result(req, *, output_text, error=None, status="completed"):
        from src.baselines.common.contracts import BaselineRequestResult
        return BaselineRequestResult(
            doc_id=req.doc_id, endpoint_index=req.endpoint_index, status=status,
            error=error, submitted_at_s=1.0, started_at_s=1.0, completed_at_s=2.0,
            input_tokens=req.prompt_tokens, output_tokens=0, output_text=output_text,
            finish_reason=None,
        )

    def _run_gate(self, fail_indices: set[int]) -> tuple[int, dict, list[str]]:
        import csv as _csv
        rows = self._rows()
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "result"
            prov = Path(td) / "prov.json"
            prov.write_text(
                json.dumps({"content_hash": gate._structured_content_hash(rows),
                            "sample_count": len(rows)}) + "\n",
                encoding="utf-8",
            )
            argv = [
                "--database-url", "postgresql://u:p@localhost:5432/d",
                "--mode", "full", "--metrics-settle-s", "0",
                "--importer-provenance", str(prov),
                "--endpoint-url", "http://127.0.0.1:8000/v1/chat/completions",
                "--metrics-url", "http://127.0.0.1:8000/metrics",
                "--model", "m", "--service-prefix-caching", "enabled",
                "--output-dir", str(out), "--force",
            ]

            def fake_complete(requests, config):
                return tuple(
                    self._result(
                        r,
                        output_text=None if i in fail_indices else "ok",
                        error=("max_tokens reached" if i in fail_indices else None),
                        status=("failed" if i in fail_indices else "completed"),
                    )
                    for i, r in enumerate(requests)
                )

            scrape_state = {"n": 0}

            def fake_scrape(url, timeout_s=5.0):
                scrape_state["n"] += 1
                base = {"vllm:num_requests_running": 0.0,
                        "vllm:num_requests_waiting": 0.0,
                        "vllm:request_success_total": 100.0}
                if scrape_state["n"] == 2:  # after
                    base["vllm:request_success_total"] = 100.0 + len(rows)
                return base

            with patch("squad_capability_gate._load_workload", return_value=rows), \
                 patch("squad_capability_gate._pg_server_identity",
                       return_value={"pg_server_version": "PostgreSQL 99.0",
                                     "pgvector_version": "0.8.0"}), \
                 patch("squad_capability_gate.scrape_prometheus_metrics",
                       side_effect=fake_scrape), \
                 patch("squad_capability_gate.inspect_duckdb_ai_runtime",
                       return_value={"duckdb_version": "v1.5.4",
                                     "duckdb_ai_extension_version": "0.4.14",
                                     "duckdb_ai_extension_source": "community"}), \
                 patch("squad_capability_gate.run_duckdb_ai_complete",
                       side_effect=fake_complete), \
                 patch("squad_capability_gate._vllm_version", return_value="0.25.1"), \
                 patch("squad_capability_gate._gpu_identity",
                       return_value={"nvidia_smi": [], "hostname": "h"}), \
                 patch("squad_capability_gate._git_commit", return_value="deadbeef"):
                rc = gate.main(argv)
            report = json.loads((out / "report.json").read_text(encoding="utf-8"))
            with (out / "per_row_evidence.csv").open(encoding="utf-8") as f:
                csv_header = next(_csv.reader(f))
            return rc, report, csv_header

    def test_all_success_passes_and_csv_has_version_columns(self) -> None:
        rc, report, csv_header = self._run_gate(fail_indices=set())
        self.assertEqual(rc, 0)
        self.assertEqual(report["status"], "success")
        self.assertIsNone(report["failure_reason"])
        self.assertIn("server_version", csv_header)
        self.assertIn("pgvector_version", csv_header)

    def test_any_row_error_fails_closed(self) -> None:
        rc, report, _ = self._run_gate(fail_indices={1})
        self.assertEqual(rc, 1)
        self.assertEqual(report["status"], "failure")
        self.assertIsNotNone(report["failure_reason"])
        self.assertIn("row-level error", report["failure_reason"])

    def test_any_null_response_fails_closed(self) -> None:
        # NULL without an explicit error string still fails (DuckDB-ai can
        # return NULL response without an error field on truncation).
        rows = self._rows()
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "result"
            prov = Path(td) / "prov.json"
            prov.write_text(
                json.dumps({"content_hash": gate._structured_content_hash(rows),
                            "sample_count": len(rows)}) + "\n",
                encoding="utf-8",
            )
            argv = [
                "--database-url", "postgresql://u:p@localhost:5432/d",
                "--mode", "full", "--metrics-settle-s", "0",
                "--importer-provenance", str(prov),
                "--endpoint-url", "http://127.0.0.1:8000/v1/chat/completions",
                "--metrics-url", "http://127.0.0.1:8000/metrics",
                "--model", "m", "--service-prefix-caching", "enabled",
                "--output-dir", str(out), "--force",
            ]

            def fake_complete(requests, config):
                out_results = []
                for i, r in enumerate(requests):
                    # row 2: completed but output_text=None and no error -> NULL
                    is_null = (i == 2)
                    out_results.append(self._result(
                        r, output_text=None if is_null else "ok",
                        error=None, status="completed",
                    ))
                return tuple(out_results)

            scrape_state = {"n": 0}

            def fake_scrape(url, timeout_s=5.0):
                scrape_state["n"] += 1
                base = {"vllm:num_requests_running": 0.0,
                        "vllm:num_requests_waiting": 0.0,
                        "vllm:request_success_total": 100.0}
                if scrape_state["n"] == 2:
                    base["vllm:request_success_total"] = 100.0 + len(rows)
                return base

            with patch("squad_capability_gate._load_workload", return_value=rows), \
                 patch("squad_capability_gate._pg_server_identity",
                       return_value={"pg_server_version": "PostgreSQL 99.0",
                                     "pgvector_version": "0.8.0"}), \
                 patch("squad_capability_gate.scrape_prometheus_metrics",
                       side_effect=fake_scrape), \
                 patch("squad_capability_gate.inspect_duckdb_ai_runtime",
                       return_value={"duckdb_version": "v1.5.4",
                                     "duckdb_ai_extension_version": "0.4.14",
                                     "duckdb_ai_extension_source": "community"}), \
                 patch("squad_capability_gate.run_duckdb_ai_complete",
                       side_effect=fake_complete), \
                 patch("squad_capability_gate._vllm_version", return_value="0.25.1"), \
                 patch("squad_capability_gate._gpu_identity",
                       return_value={"nvidia_smi": [], "hostname": "h"}), \
                 patch("squad_capability_gate._git_commit", return_value="deadbeef"):
                rc = gate.main(argv)
            report = json.loads((out / "report.json").read_text(encoding="utf-8"))
        self.assertEqual(rc, 1)
        self.assertEqual(report["status"], "failure")
        self.assertEqual(report["null_response_count"], 1)


if __name__ == "__main__":
    unittest.main()