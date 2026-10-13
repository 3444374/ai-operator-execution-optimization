"""Fail-closed tests for the opening database-E2E aggregate audit."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
SCRIPT = CODE_ROOT / "scripts" / "analysis" / "summarize_opening_database_e2e.py"
SPEC = importlib.util.spec_from_file_location("summarize_opening_database_e2e", SCRIPT)
mod = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


class OpeningDatabaseE2EAuditTests(unittest.TestCase):
    def _passing(self) -> dict:
        return {
            "expected_counts_ok": True,
            "all_status_passed": True,
            "all_exactly_once": True,
            "all_sink_readback_matched": True,
            "infrastructure_failure_count": 0,
            "manifest_and_identity_consistent": True,
            "all_project_feeding_service_token_gates_passed": True,
            "all_project_gpu_utilization_gates_passed": True,
        }

    def test_complete_correct_project_audit_passes(self) -> None:
        self.assertTrue(mod._audit_passed(self._passing()))

    def test_low_project_feeding_and_utilization_remain_diagnostics(self) -> None:
        audit = self._passing()
        audit["all_project_feeding_service_token_gates_passed"] = False
        audit["all_project_gpu_utilization_gates_passed"] = False
        self.assertTrue(mod._audit_passed(audit))

    def test_missing_performance_observations_do_not_fail_correctness_audit(self) -> None:
        audit = self._passing()
        del audit["all_project_feeding_service_token_gates_passed"]
        del audit["all_project_gpu_utilization_gates_passed"]
        self.assertTrue(mod._audit_passed(audit))

    def test_aggregate_preserves_historical_threshold_observations(self) -> None:
        rows = []
        summaries = []
        for workload in mod.EXPECTED_WORKLOADS:
            for arm in mod.EXPECTED_ARMS:
                for phase, repeats in (("warmup", 1), ("formal", 3)):
                    for _ in range(repeats):
                        rows.append({
                            "workload": workload, "arm": arm, "phase": phase,
                            "manifest_sha256": workload,
                            "server_version": "18.4", "pgvector_version": "0.8.0",
                            "status": "passed", "exactly_once": True,
                            "sink_readback_matched": True,
                            "infrastructure_failure_count": 0, "cap_semantic_failure_count": 0,
                        })
                summaries.append({
                    "workload": workload, "arm": arm,
                    "correct_rows_per_s_cv_pct": 2.0, "database_e2e_s_cv_pct": 2.0,
                    "gpu_utilization_pct_mean_mean": 40.0,
                    "service_tokens_per_s_mean": 100.0 if arm == "direct_static_sharded" else 50.0,
                })
        audit = mod._audit(rows, summaries)
        self.assertTrue(mod._audit_passed(audit))
        self.assertFalse(audit["all_project_feeding_service_token_gates_passed"])
        self.assertFalse(audit["all_project_gpu_utilization_gates_passed"])
        self.assertEqual(audit["performance_threshold_scope"],
                         "historical_opening_matrix_observation_only")
        project = next(row for row in audit["summary_gates"] if row["arm"] == "project_frozen_static")
        self.assertEqual(project["gpu_utilization_pct_mean"], 40.0)
        self.assertEqual(project["feeding_service_tokens_ratio_vs_direct"], 0.5)

    def test_product_baseline_feeding_is_not_a_project_gate(self) -> None:
        audit = self._passing()
        audit["all_arm_feeding_service_token_observations_ge_0_95"] = False
        self.assertTrue(mod._audit_passed(audit))

    def test_correctness_or_identity_failure_fails(self) -> None:
        for key in (
            "expected_counts_ok",
            "all_status_passed",
            "all_exactly_once",
            "all_sink_readback_matched",
            "manifest_and_identity_consistent",
        ):
            audit = self._passing()
            audit[key] = False
            self.assertFalse(mod._audit_passed(audit), key)
            del audit[key]
            self.assertFalse(mod._audit_passed(audit), f"missing {key}")

    def test_infrastructure_failure_or_missing_count_fails(self) -> None:
        audit = self._passing()
        audit["infrastructure_failure_count"] = 1
        self.assertFalse(mod._audit_passed(audit))
        del audit["infrastructure_failure_count"]
        self.assertFalse(mod._audit_passed(audit))


if __name__ == "__main__":
    unittest.main()
