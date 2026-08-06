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
    """复审 #1/#4: _write_identity must emit ComparisonRole Literal values only,
    put the system role in the STANDARD primary field ``comparison_role`` (NOT a
    removed side field), and name ALL scheduling parties in scheduler_owner."""

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
                self.assertIn(ident["component_comparison_role"], valid,
                              f"{arm} component_comparison_role {ident['component_comparison_role']!r} not in Literal")
                self.assertNotIn("system_comparison_role", ident, f"{arm} stale system_comparison_role field")
                self.assertFalse(ident["formal_baseline_eligible"], f"{arm} must not be formal-eligible at ramp layer")

    def test_comparison_role_primary_is_system_role(self) -> None:
        # 复审 #1: comparison_role (STANDARD primary field) must BE the system role,
        # NOT the single-shard component; component moves to component_comparison_role.
        with tempfile.TemporaryDirectory() as td:
            cases = {"bounded_http": ("direct_client_control", "direct_client_control"),
                     "duckdb_ai": ("harness_pre_split_diagnostic", "database_product_native_baseline"),
                     "lb_rr": ("gateway_system_diagnostic", "database_product_native_baseline"),
                     "project_static": ("project_scheduled_method", "project_scheduled_method")}
            for arm, (sys_role, comp_role) in cases.items():
                cell = Path(td) / arm
                cell.mkdir()
                drv._write_identity(arm, cell)
                ident = json.loads((cell / "identity.json").read_text(encoding="utf-8"))
                self.assertEqual(ident["comparison_role"], sys_role, f"{arm} primary not system role")
                self.assertEqual(ident["component_comparison_role"], comp_role, f"{arm} component mismatch")

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


class WarmupEndpointIndicesTests(unittest.TestCase):
    """复审 rerun-contract (b): _warmup_cache must warm BOTH backends with the full
    prompt set for a single-endpoint manifest (independent vLLM caches don't share).
    Tests the pure decision function _warmup_endpoint_indices."""

    def test_single_endpoint_manifest_warms_both_backends_full(self) -> None:
        # lb_rr lbrr_dev (endpoint_count=1): both backends get endpoint_index=0 (full prompt)
        idxs = drv._warmup_endpoint_indices(single_endpoint=True, n_endpoints=2)
        self.assertEqual(idxs, (0, 0))

    def test_two_endpoint_manifest_warms_own_shard(self) -> None:
        # gate 2-endpoint manifest: each backend warms its own shard
        idxs = drv._warmup_endpoint_indices(single_endpoint=False, n_endpoints=2)
        self.assertEqual(idxs, (0, 1))


class StrictPreflightTests(unittest.TestCase):
    """复审 #4 (codex strict): the vLLM-config preflight is split into pure
    helpers so the 8 strict cases are unit-testable without /proc or a live
    vLLM: (1) no process, (2) missing endpoint, (3) single endpoint ok,
    (4) max-num flag missing / wrong value, (5) prefix-cache missing,
    (6) both endpoints fully match, (7) non-strict only warns,
    (8) --enable-prefix-caching=false not misread as ON."""

    @staticmethod
    def _cmd(port: int, *, seqs="256", batched="8192", prefix="on") -> str:
        flags = f"--port {port} --model qwen --max-num-seqs {seqs} --max-num-batched-tokens {batched}"
        if prefix == "on":
            return f"python -m vllm.entrypoints {flags} --enable-prefix-caching"
        if prefix == "off":
            return f"python -m vllm.entrypoints {flags} --enable-prefix-caching=false"
        return f"python -m vllm.entrypoints {flags}"  # prefix absent

    def _declared(self) -> dict:
        return {"--max-num-seqs": "256", "--max-num-batched-tokens": "8192"}

    # --- pure helpers (codex #8 substring trap) ---
    def test_cmdline_for_port_matches_space_and_equals_forms(self) -> None:
        pool = [self._cmd(8000), "python -m vllm.entrypoints --port=8001 --model qwen"]
        self.assertIsNotNone(drv._cmdline_for_port(pool, "8000"))
        self.assertIsNotNone(drv._cmdline_for_port(pool, "8001"))
        self.assertIsNone(drv._cmdline_for_port(pool, "9000"))

    def test_flag_value_present_space_and_equals(self) -> None:
        self.assertTrue(drv._flag_value_present("--max-num-seqs 256", "--max-num-seqs", "256"))
        self.assertTrue(drv._flag_value_present("--max-num-seqs=256", "--max-num-seqs", "256"))
        self.assertFalse(drv._flag_value_present("--max-num-seqs 128", "--max-num-seqs", "256"))

    def test_prefix_cache_bare_and_true_are_on(self) -> None:
        self.assertIs(drv._prefix_cache_flag_enabled("--enable-prefix-caching"), True)
        self.assertIs(drv._prefix_cache_flag_enabled("--enable-prefix-caching=true"), True)

    def test_prefix_cache_false_is_off_not_on(self) -> None:
        # codex #8: a naive '"--enable-prefix-caching" in c' substring test would
        # misread =false as ON; the token-based helper must return False.
        self.assertIs(drv._prefix_cache_flag_enabled("--enable-prefix-caching=false"), False)
        self.assertIs(drv._prefix_cache_flag_enabled("--enable-prefix-caching=0"), False)

    def test_prefix_cache_absent_is_none(self) -> None:
        self.assertIsNone(drv._prefix_cache_flag_enabled("--port 8000 --max-num-seqs 256"))

    # --- pure verifier _verify_endpoint_cmdlines (cases 2/3/4/5/6/7) ---
    def test_both_endpoints_match_passes_strict(self) -> None:  # case 6
        pool = [self._cmd(8000), self._cmd(8001)]
        urls = ["http://127.0.0.1:8000/v1/chat/completions", "http://127.0.0.1:8001/v1/chat/completions"]
        drv._verify_endpoint_cmdlines(pool, urls, self._declared(), True)  # no raise

    def test_missing_endpoint_strict_raises(self) -> None:  # case 2
        pool = [self._cmd(8000)]  # 8001 absent
        urls = ["http://127.0.0.1:8000/v1/chat/completions", "http://127.0.0.1:8001/v1/chat/completions"]
        with self.assertRaises(RuntimeError):
            drv._verify_endpoint_cmdlines(pool, urls, self._declared(), True)

    def test_single_endpoint_ok_passes_strict(self) -> None:  # case 3
        drv._verify_endpoint_cmdlines([self._cmd(8000)],
                                      ["http://127.0.0.1:8000/v1/chat/completions"], self._declared(), True)

    def test_max_num_seqs_missing_strict_raises(self) -> None:  # case 4a
        pool = [self._cmd(8000, seqs="0")]  # seqs="0" -> flag absent from cmdline
        # rebuild a cmdline genuinely missing --max-num-seqs
        pool = ["python -m vllm.entrypoints --port 8000 --model qwen --max-num-batched-tokens 8192 --enable-prefix-caching"]
        with self.assertRaises(RuntimeError):
            drv._verify_endpoint_cmdlines(pool, ["http://127.0.0.1:8000/v1/chat/completions"], self._declared(), True)

    def test_max_num_seqs_wrong_value_strict_raises(self) -> None:  # case 4b
        pool = [self._cmd(8000, seqs="128")]  # declared 256, effective 128
        with self.assertRaises(RuntimeError):
            drv._verify_endpoint_cmdlines(pool, ["http://127.0.0.1:8000/v1/chat/completions"], self._declared(), True)

    def test_prefix_cache_missing_strict_raises(self) -> None:  # case 5
        pool = [self._cmd(8000, prefix="absent")]
        with self.assertRaises(RuntimeError):
            drv._verify_endpoint_cmdlines(pool, ["http://127.0.0.1:8000/v1/chat/completions"], self._declared(), True)

    def test_prefix_cache_false_passes_strict(self) -> None:  # case 8 at verifier level
        # =false IS verified (just OFF) -> not an "unverifiable" raise
        pool = [self._cmd(8000, prefix="off")]
        drv._verify_endpoint_cmdlines(pool, ["http://127.0.0.1:8000/v1/chat/completions"], self._declared(), True)

    def test_non_strict_missing_flag_only_warns(self) -> None:  # case 7
        pool = [self._cmd(8000, seqs="128", prefix="absent")]
        drv._verify_endpoint_cmdlines(pool, ["http://127.0.0.1:8000/v1/chat/completions"],
                                      self._declared(), False)  # no raise

    # --- _verify_vllm_config proc discovery (cases 1, 7 no-proc) ---
    @staticmethod
    def _ramp(strict: bool):
        from types import SimpleNamespace
        return SimpleNamespace(vllm_config_strict=strict,
                                endpoint_urls=["http://127.0.0.1:8000/v1/chat/completions"],
                                service_max_num_seqs=256, service_max_num_batched_tokens=8192)

    def test_strict_no_vllm_process_raises(self) -> None:  # case 1
        from unittest import mock
        empty = mock.Mock(stdout="", stderr="")
        with mock.patch("subprocess.run", return_value=empty):
            with self.assertRaises(RuntimeError):
                drv._verify_vllm_config(self._ramp(True))

    def test_non_strict_no_vllm_process_warns_not_raises(self) -> None:  # case 7 (no proc)
        from unittest import mock
        empty = mock.Mock(stdout="", stderr="")
        with mock.patch("subprocess.run", return_value=empty):
            drv._verify_vllm_config(self._ramp(False))  # no raise


if __name__ == "__main__":
    unittest.main()
