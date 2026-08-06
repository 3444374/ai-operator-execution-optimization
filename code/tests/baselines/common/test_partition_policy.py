"""Partition-policy: equal-rows vs pre-execution-work-balanced sharding + policy-aware gate.

Covers the multi-card static-sharded baseline's two sharding modes and the
gate that judges each policy on the quantity it claims to balance. Pure / local
(no psycopg/ray/duckdb): manifest assignment is math; CLI routing goes through
the file-based ``_export_manifest`` (no Postgres); the gate is called directly.
"""

from __future__ import annotations

import argparse
import json
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

from src.baselines.common.contracts import BaselineRequestResult, ChatRequest  # noqa: E402
from src.baselines.common.gate import validate_gate  # noqa: E402
from src.baselines.common.manifests import (  # noqa: E402
    assign_endpoint_equal_rows,
    assign_endpoints,
    partition_summary,
    read_manifest_metadata,
    write_manifest_metadata,
)
from src.baselines.common.provenance import adapter_provenance  # noqa: E402
from src.baselines.text.orchestration.cli import _export_manifest  # noqa: E402


def _req(doc_id: int, *, prompt_tokens: int = 10, est_out: int = 8) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id, prompt=f"q{doc_id}", arrival_time_s=0.0,
        prompt_tokens=prompt_tokens, max_output_tokens=est_out,
        estimated_output_tokens=est_out, source_row_hash=f"r{doc_id}",
        endpoint_index=0,
    )


class EqualRowsAssignmentTests(unittest.TestCase):
    def test_256_rows_strict_128_128(self) -> None:
        assigned = assign_endpoint_equal_rows([_req(i) for i in range(256)], 2, seed=0)
        counts = {0: 0, 1: 0}
        for r in assigned:
            counts[r.endpoint_index] += 1
        self.assertEqual(counts, {0: 128, 1: 128})

    def test_odd_count_diff_at_most_one(self) -> None:
        for n in (1, 3, 255, 257):
            assigned = assign_endpoint_equal_rows([_req(i) for i in range(n)], 2, seed=0)
            counts = {0: 0, 1: 0}
            for r in assigned:
                counts[r.endpoint_index] += 1
            self.assertLessEqual(abs(counts[0] - counts[1]), 1, f"n={n}")

    def test_input_order_does_not_change_mapping(self) -> None:
        import random
        reqs = [_req(i) for i in range(40)]
        baseline = {r.doc_id: r.endpoint_index for r in assign_endpoint_equal_rows(reqs, 2, seed=3)}
        shuffled = reqs[:]
        random.shuffle(shuffled)
        out = assign_endpoint_equal_rows(shuffled, 2, seed=3)
        # doc_id -> endpoint mapping must be identical (sort is by hash, not input order)
        self.assertEqual({r.doc_id: r.endpoint_index for r in out}, baseline)
        # output preserves the (shuffled) input order
        self.assertEqual([r.doc_id for r in out], [r.doc_id for r in shuffled])

    def test_same_seed_same_result_different_seed_differs(self) -> None:
        reqs = [_req(i) for i in range(60)]
        a = {r.doc_id: r.endpoint_index for r in assign_endpoint_equal_rows(reqs, 2, seed=7)}
        b = {r.doc_id: r.endpoint_index for r in assign_endpoint_equal_rows(reqs, 2, seed=7)}
        c = {r.doc_id: r.endpoint_index for r in assign_endpoint_equal_rows(reqs, 2, seed=99)}
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)  # 60 rows -> a different seed very likely reorders

    def test_duplicate_doc_id_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate doc_id"):
            assign_endpoint_equal_rows([_req(1), _req(1)], 2, seed=0)


class EndpointDispatchTests(unittest.TestCase):
    def test_routes_equal_rows_strict_and_work_balanced_uses_both(self) -> None:
        reqs = [_req(i, prompt_tokens=10 + i) for i in range(8)]
        equal = assign_endpoints(reqs, 2, policy="equal_rows", seed=0)
        counts = {0: 0, 1: 0}
        for r in equal:
            counts[r.endpoint_index] += 1
        self.assertEqual(counts, {0: 4, 1: 4})  # strict 4/4
        balanced = assign_endpoints(reqs, 2, policy="preexecution_token_work_balanced")
        self.assertEqual({r.endpoint_index for r in balanced}, {0, 1})

    def test_rejects_unknown_policy(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported partition policy"):
            assign_endpoints([_req(1)], 2, policy="round_robin")


def _input_jsonl(path: Path) -> None:
    rows = [
        {
            "doc_id": i, "prompt": f"q{i}", "arrival_time_s": 0.0,
            "prompt_tokens": 10 + i, "max_output_tokens": 8,
            "estimated_output_tokens": 8, "source_row_hash": f"r{i}",
            "endpoint_index": -1,
        }
        for i in range(8)
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


class ExportCliPolicyTests(unittest.TestCase):
    def test_cli_equal_rows_routes_and_records_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "in.jsonl"
            _input_jsonl(inp)
            args = argparse.Namespace(
                input=str(inp), output=str(Path(td) / "out.jsonl"),
                endpoint_count=2, partition_policy="equal_rows", partition_seed=0,
            )
            meta = _export_manifest(args)
        self.assertEqual(meta["partition_policy"], "equal_rows")
        self.assertEqual(meta["partition_seed"], 0)
        self.assertEqual(meta["endpoint_row_counts"], {0: 4, 1: 4})  # strict 4/4
        for key in (
            "endpoint_prompt_tokens", "endpoint_estimated_output_work",
            "endpoint_total_estimated_work", "endpoint_row_count_diff",
            "endpoint_work_skew", "sha256", "row_count",
        ):
            self.assertIn(key, meta, key)

    def test_cli_work_balanced_routes_and_balances_work(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "in.jsonl"
            _input_jsonl(inp)
            args = argparse.Namespace(
                input=str(inp), output=str(Path(td) / "out.jsonl"),
                endpoint_count=2, partition_policy="preexecution_token_work_balanced",
                partition_seed=0,
            )
            meta = _export_manifest(args)
        self.assertEqual(meta["partition_policy"], "preexecution_token_work_balanced")
        self.assertLess(meta["endpoint_work_skew"], 0.02)  # balanced under 2%


def _grequest(doc_id: int, endpoint: int, work: int) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id, prompt=f"q{doc_id}", arrival_time_s=0.0,
        prompt_tokens=work - 8, max_output_tokens=8, estimated_output_tokens=8,
        source_row_hash=f"r{doc_id}", endpoint_index=endpoint,
    )


def _gresult(doc_id: int, endpoint: int) -> BaselineRequestResult:
    return BaselineRequestResult(
        doc_id=doc_id, endpoint_index=endpoint, status="completed", error=None,
        submitted_at_s=1.0, started_at_s=1.1, completed_at_s=1.2,
        input_tokens=4, output_tokens=1, output_text="ok", finish_reason="stop",
    )


def _gsummary(endpoint: int, predicted_work: int) -> dict[str, object]:
    return {
        "endpoint_index": endpoint, "predicted_work": predicted_work,
        "model_name": "qwen", "completion_protocol": "chat_completions",
        "service_config_sha256": "svc", "vllm_num_requests_running_final": 0,
        "vllm_num_requests_waiting_final": 0, "worker_failures": 0,
        **adapter_provenance("bounded_http").summary_fields(),
    }


class PolicyAwareGateTests(unittest.TestCase):
    """equal_rows balances ROWS (work may legitimately skew -- that is the finding);
    work_balanced balances pre-execution WORK (rows may legitimately differ)."""

    def _skewed_manifest_balanced_rows(self):
        # 2 rows per endpoint (balanced) but work 200 vs 20 (heavily skewed)
        return (
            _grequest(1, 0, 100), _grequest(2, 0, 100),
            _grequest(3, 1, 10), _grequest(4, 1, 10),
        )

    def test_equal_rows_does_not_fail_on_work_skew(self) -> None:
        manifest = self._skewed_manifest_balanced_rows()
        report = validate_gate(
            manifest=manifest,
            summaries=[_gsummary(0, 200), _gsummary(1, 20)],
            request_results=(_gresult(1, 0), _gresult(2, 0), _gresult(3, 1), _gresult(4, 1)),
            partition_policy="equal_rows",
        )
        self.assertNotIn("endpoint_work_skew", report.incidents)
        self.assertNotIn("endpoint_row_skew", report.incidents)  # rows balanced 2:2
        self.assertTrue(report.passed, report.incidents)

    def test_work_balanced_fails_when_skew_over_two_percent(self) -> None:
        manifest = self._skewed_manifest_balanced_rows()
        report = validate_gate(
            manifest=manifest,
            summaries=[_gsummary(0, 200), _gsummary(1, 20)],
            request_results=(_gresult(1, 0), _gresult(2, 0), _gresult(3, 1), _gresult(4, 1)),
            partition_policy="preexecution_token_work_balanced",
            max_endpoint_work_skew=0.02,
        )
        self.assertIn("endpoint_work_skew", report.incidents)
        self.assertFalse(report.passed)


class AssignGateIntegrationTests(unittest.TestCase):
    """Bug C: an assignment's OUTPUT must satisfy its OWN policy's gate end-to-end."""

    def test_equal_rows_output_passes_equal_rows_gate(self) -> None:
        assigned = assign_endpoint_equal_rows(
            [_req(i, prompt_tokens=10 + i) for i in range(8)], 2, seed=0
        )
        ep_work = {0: 0, 1: 0}
        for r in assigned:
            ep_work[r.endpoint_index] += r.estimated_work
        report = validate_gate(
            manifest=assigned,
            summaries=[_gsummary(0, ep_work[0]), _gsummary(1, ep_work[1])],
            request_results=tuple(_gresult(r.doc_id, r.endpoint_index) for r in assigned),
            partition_policy="equal_rows",
        )
        self.assertTrue(report.passed, report.incidents)
        self.assertEqual(report.metrics["endpoint_row_counts"], {0: 4, 1: 4})

    def test_work_balanced_output_passes_work_balanced_gate(self) -> None:
        assigned = assign_endpoints(
            [_req(i, prompt_tokens=10 + i) for i in range(8)], 2,
            policy="preexecution_token_work_balanced",
        )
        ep_work = {0: 0, 1: 0}
        for r in assigned:
            ep_work[r.endpoint_index] += r.estimated_work
        report = validate_gate(
            manifest=assigned,
            summaries=[_gsummary(0, ep_work[0]), _gsummary(1, ep_work[1])],
            request_results=tuple(_gresult(r.doc_id, r.endpoint_index) for r in assigned),
            partition_policy="preexecution_token_work_balanced",
        )
        self.assertTrue(report.passed, report.incidents)


class ManifestMetadataTests(unittest.TestCase):
    """Bug B: endpoint_work alias + Bug A: sidecar round-trip."""

    _SUMMARY = {
        "endpoint_row_counts": {0: 128, 1: 128},
        "endpoint_prompt_tokens": {0: 0, 1: 0},
        "endpoint_estimated_output_work": {0: 0, 1: 0},
        "endpoint_total_estimated_work": {0: 0, 1: 0},
        "endpoint_work": {0: 0, 1: 0},
        "endpoint_row_count_diff": 0,
        "endpoint_work_skew": 0.0,
    }

    def test_sidecar_roundtrip_and_alias(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "m.jsonl"
            p.write_text("dummy\n", encoding="utf-8")
            write_manifest_metadata(
                p, partition_policy="equal_rows", partition_seed=7,
                row_count=256, manifest_sha256="abc",
                partition_summary_dict=self._SUMMARY,
            )
            meta = read_manifest_metadata(p)
        self.assertIsNotNone(meta)
        self.assertEqual(meta["partition_policy"], "equal_rows")
        self.assertEqual(meta["partition_seed"], 7)
        # JSON stringifies int dict keys -> "0"/"1" after round-trip
        self.assertEqual(meta["endpoint_row_counts"], {"0": 128, "1": 128})
        self.assertIn("endpoint_work", meta)  # backward-compat alias kept

    def test_partition_summary_emits_endpoint_work_alias(self) -> None:
        assigned = assign_endpoint_equal_rows([_req(i) for i in range(4)], 2, seed=0)
        summary = partition_summary(assigned, 2)
        self.assertEqual(summary["endpoint_work"], summary["endpoint_total_estimated_work"])

    def test_read_metadata_none_for_legacy_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "lonely.jsonl"
            p.write_text("x\n", encoding="utf-8")
            self.assertIsNone(read_manifest_metadata(p))


class PartitionPolicyCrossCheckTests(unittest.TestCase):
    """Bug A: the gate uses the manifest's ACTUAL policy and fails closed on mismatch."""

    @staticmethod
    def _write_sidecar(path: Path, policy: str) -> None:
        write_manifest_metadata(
            path, partition_policy=policy, partition_seed=0, row_count=4,
            manifest_sha256="x", partition_summary_dict=ManifestMetadataTests._SUMMARY,
        )

    def test_config_manifest_mismatch_fails_closed(self) -> None:
        from src.baselines.text.orchestration.gate_runner import _resolve_partition_policy
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "m.jsonl"
            p.write_text("x\n", encoding="utf-8")
            self._write_sidecar(p, "equal_rows")
            with self.assertRaisesRegex(ValueError, "disagrees"):
                _resolve_partition_policy(p, "preexecution_token_work_balanced")

    def test_manifest_policy_used_when_config_undeclared(self) -> None:
        from src.baselines.text.orchestration.gate_runner import _resolve_partition_policy
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "m.jsonl"
            p.write_text("x\n", encoding="utf-8")
            self._write_sidecar(p, "equal_rows")
            self.assertEqual(_resolve_partition_policy(p, None), "equal_rows")

    def test_legacy_manifest_falls_back_to_declared(self) -> None:
        from src.baselines.text.orchestration.gate_runner import _resolve_partition_policy
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "no_sidecar.jsonl"
            p.write_text("x\n", encoding="utf-8")
            self.assertEqual(_resolve_partition_policy(p, "equal_rows"), "equal_rows")


if __name__ == "__main__":
    unittest.main()
