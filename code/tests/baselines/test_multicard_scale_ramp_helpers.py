"""Tests for multicard_scale_ramp pure helpers.

复审 工程严谨性缺口: the driver had ~100 changed lines but the 9 existing tests
only covered the aggregator; warmup/balance/atomic/Ray/config preflight had none.
These cover the two pure helpers that the lb_rr balance gate and warmup depend on
(_backend_skew, _manifest_is_single_endpoint) without needing nginx/vLLM/Ray.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

_DRV = CODE_ROOT / "scripts" / "baselines" / "multicard_scale_ramp.py"
_spec = importlib.util.spec_from_file_location("multicard_scale_ramp", _DRV)
drv = importlib.util.module_from_spec(_spec)
sys.modules["multicard_scale_ramp"] = drv
_spec.loader.exec_module(drv)


class BackendSkewTests(unittest.TestCase):
    def test_perfect_round_robin_is_zero(self) -> None:
        self.assertEqual(drv._backend_skew({"0": 128, "1": 128}), 0.0)

    def test_one_in_ten_skew(self) -> None:
        # 128 vs 115 -> |13|/128 = 0.1016 (>10% threshold)
        self.assertGreater(drv._backend_skew({"0": 128, "1": 115}), 0.10)

    def test_small_skew_under_threshold(self) -> None:
        # 256-gate observed ~0.67% token-work skew; request skew 128 vs 127
        self.assertLess(drv._backend_skew({"0": 128, "1": 127}), 0.02)

    def test_under_two_backends_is_zero(self) -> None:
        # one backend (other idle / no success) -> caller must check len<2 separately
        self.assertEqual(drv._backend_skew({"0": 256}), 0.0)


class ManifestSingleEndpointTests(unittest.TestCase):
    def _meta(self, td: str, name: str, endpoint_row_counts: dict) -> Path:
        manifest = Path(td) / name
        manifest.write_text("{}", encoding="utf-8")
        meta = Path(td) / (name + ".meta.json")
        meta.write_text(json.dumps({"endpoint_row_counts": endpoint_row_counts}), encoding="utf-8")
        return manifest

    def test_single_endpoint_lbrr_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            m = self._meta(td, "lbrr_dev_256.jsonl", {"0": 256})
            self.assertTrue(drv._manifest_is_single_endpoint(m))

    def test_two_endpoint_gate_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            m = self._meta(td, "squad_dev_256.jsonl", {"0": 128, "1": 128})
            self.assertFalse(drv._manifest_is_single_endpoint(m))

    def test_no_meta_defaults_to_two_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            m = Path(td) / "no_meta.jsonl"
            m.write_text("{}", encoding="utf-8")
            self.assertFalse(drv._manifest_is_single_endpoint(m))


class WriteIdentityTests(unittest.TestCase):
    """复审 #2/#3: _write_identity must emit ComparisonRole Literal values
    only, use system_comparison_role as the authoritative primary role, and
    name ALL scheduling parties in scheduler_owner."""

    def test_all_roles_are_valid_comparison_role_literal(self) -> None:
        import typing
        from src.baselines.common.provenance import ComparisonRole
        valid = set(typing.get_args(ComparisonRole))
        with tempfile.TemporaryDirectory() as td:
            for arm in ("bounded_http", "duckdb_ai", "lb_rr", "project_static"):
                cell = Path(td) / arm
                cell.mkdir()
                drv._write_identity(arm, cell)
                ident = json.loads((cell / "identity.json").read_text(encoding="utf-8"))
                self.assertIn(ident["comparison_role"], valid,
                              f"{arm} comparison_role {ident['comparison_role']!r} not in ComparisonRole Literal")
                self.assertIn(ident["system_comparison_role"], valid,
                              f"{arm} system_comparison_role {ident['system_comparison_role']!r} not in Literal")
                self.assertFalse(ident["formal_baseline_eligible"], f"{arm} must not be formal-eligible at ramp layer")

    def test_system_comparison_role_per_arm(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cases = {"bounded_http": "direct_client_control",
                     "duckdb_ai": "harness_pre_split_diagnostic",
                     "lb_rr": "gateway_system_diagnostic",  # protocol §2.6 gateway, NOT harness
                     "project_static": "project_scheduled_method"}
            for arm, expected in cases.items():
                cell = Path(td) / arm
                cell.mkdir()
                drv._write_identity(arm, cell)
                ident = json.loads((cell / "identity.json").read_text(encoding="utf-8"))
                self.assertEqual(ident["system_comparison_role"], expected, f"{arm}")

    def test_lb_rr_scheduler_owner_names_all_parties(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cell = Path(td) / "lbrr"
            cell.mkdir()
            drv._write_identity("lb_rr", cell)
            ident = json.loads((cell / "identity.json").read_text(encoding="utf-8"))
            owner = ident["scheduler_owner"].lower()
            self.assertIn("duckdb", owner, "scheduler_owner must name DuckDB extension")
            self.assertIn("nginx", owner, "scheduler_owner must name nginx round-robin")
            self.assertIn("vllm", owner, "scheduler_owner must name vLLM")


if __name__ == "__main__":
    unittest.main()
