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

    def test_complete_correct_saturated_project_audit_passes(self) -> None:
        self.assertTrue(mod._audit_passed(self._passing()))

    def test_project_feeding_failure_fails_even_when_cells_passed(self) -> None:
        audit = self._passing()
        audit["all_project_feeding_service_token_gates_passed"] = False
        self.assertFalse(mod._audit_passed(audit))

    def test_product_baseline_feeding_is_not_a_project_gate(self) -> None:
        audit = self._passing()
        audit["all_arm_feeding_service_token_observations_ge_0_95"] = False
        self.assertTrue(mod._audit_passed(audit))

    def test_correctness_or_identity_failure_fails(self) -> None:
        for key in (
            "all_exactly_once",
            "all_sink_readback_matched",
            "manifest_and_identity_consistent",
            "all_project_gpu_utilization_gates_passed",
        ):
            audit = self._passing()
            audit[key] = False
            self.assertFalse(mod._audit_passed(audit), key)


if __name__ == "__main__":
    unittest.main()
