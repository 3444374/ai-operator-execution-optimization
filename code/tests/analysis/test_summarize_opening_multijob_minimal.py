"""Tests for the opening staggered two-job summary audit."""

from __future__ import annotations

import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
SCRIPT = CODE_ROOT / "scripts" / "analysis" / "summarize_opening_multijob_minimal.py"
SPEC = importlib.util.spec_from_file_location("summarize_opening_multijob_minimal", SCRIPT)
mod = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


class OpeningMultijobSummaryTests(unittest.TestCase):
    def _row(
        self,
        scenario_id: str,
        phase: str,
        repeat: int,
        *,
        duplicate_manifests: bool = False,
    ) -> dict[str, object]:
        shared = scenario_id.endswith("shared_work")
        return {
            "scenario_id": scenario_id,
            "phase": phase,
            "repeat_index": repeat,
            "policy": "shared_drr" if shared else "static_partition",
            "metrics_status": "ok",
            "resource_metrics_status": "ok",
            "mfu_status": "ok",
            "actor_worker_failures": 0,
            "incidents": 0,
            "source_row_offsets": json.dumps([0, 512]),
            "request_manifest_sha256": json.dumps(
                ["a" * 64, "a" * 64 if duplicate_manifests else "b" * 64]
            ),
            "tokens_per_s": 1100 if shared else 1000,
            "duration_s": 90 if shared else 100,
            "gpu_utilization_pct_mean": 90,
            "mfu_estimate": 0.35,
            "jain_fairness": 0.98,
            "job_jct_s": json.dumps([80, 90] if shared else [90, 100]),
            "job_p99_s": json.dumps([20, 25] if shared else [25, 30]),
            "job_slo_violation_ratio": json.dumps([0.1, 0.2]),
            "job_slo_goodput_per_s": json.dumps([5.0, 4.0]),
            "job_slo_token_goodput_per_s": json.dumps([100.0, 80.0]),
            "job_predicted_work": json.dumps([10000, 30000]),
            "job_actual_work": json.dumps([11000, 29000]),
            "job_normalized_service_rate": json.dumps([0.95, 1.05]),
        }

    def _fixture(self, root: Path, *, duplicate_manifests: bool = False) -> None:
        manifest = {
            "status": "completed",
            "incidents": [],
        }
        (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        rows = []
        for scenario_id in sorted(mod.EXPECTED_SCENARIOS):
            rows.append(
                self._row(
                    scenario_id,
                    "warmup",
                    1,
                    duplicate_manifests=duplicate_manifests,
                )
            )
            for repeat in (1, 2, 3):
                rows.append(
                    self._row(
                        scenario_id,
                        "formal",
                        repeat,
                        duplicate_manifests=duplicate_manifests,
                    )
                )
        with (root / "group_runs.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def test_complete_short_long_matrix_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "matrix"
            output = Path(tmp) / "summary"
            root.mkdir()
            self._fixture(root)
            self.assertTrue(mod.summarize(root, output))
            audit = json.loads((output / "audit.json").read_text(encoding="utf-8"))
            self.assertEqual(audit["status"], "passed")
            with (output / "pairwise_comparison.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                comparisons = list(csv.DictReader(handle))
            self.assertEqual(len(comparisons), 1)
            self.assertAlmostEqual(float(comparisons[0]["tokens_per_s_delta_pct"]), 10.0)

    def test_duplicate_job_manifests_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "matrix"
            output = Path(tmp) / "summary"
            root.mkdir()
            self._fixture(root, duplicate_manifests=True)
            self.assertFalse(mod.summarize(root, output))
            audit = json.loads((output / "audit.json").read_text(encoding="utf-8"))
            self.assertEqual(audit["status"], "failed")
            self.assertTrue(
                any("distinct validated manifests" in error for error in audit["errors"])
            )


if __name__ == "__main__":
    unittest.main()
