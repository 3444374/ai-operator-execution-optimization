"""Direct tests for the shared vLLM preflight module (audit F8).

The deep 8-case coverage of the pure cmdline-matching helpers lives in
``test_multicard_scale_ramp_helpers.py::StrictPreflightTests`` (which imports the ramp's aliases,
which now delegate to this module). These tests anchor the shared module's PUBLIC API directly so
``run_ai_operator_scenarios`` and ``multicard_scale_ramp`` have one canonical, independently-tested
implementation (code/AGENTS.md §4 低耦合).
"""

from __future__ import annotations

import unittest

from src.infrastructure.vllm_preflight import (
    cmdline_for_port,
    flag_value_present,
    prefix_cache_flag_enabled,
    scheduler_cls_value,
    verify_endpoint_cmdlines,
    verify_endpoint_scheduler_cls,
)


class VllmPreflightPureTests(unittest.TestCase):
    def _cmd(self, port, *, seqs="256", batched="8192", prefix="on"):
        flags = f"--port {port} --max-num-seqs {seqs} --max-num-batched-tokens {batched}"
        if prefix == "on":
            return f"python -m vllm.entrypoints {flags} --enable-prefix-caching"
        if prefix == "off":
            return f"python -m vllm.entrypoints {flags} --enable-prefix-caching=false"
        return f"python -m vllm.entrypoints {flags}"

    def test_cmdline_for_port_matches_both_forms(self):
        pool = [self._cmd(8000), "python -m vllm.entrypoints --port=8001"]
        self.assertIsNotNone(cmdline_for_port(pool, "8000"))
        self.assertIsNotNone(cmdline_for_port(pool, "8001"))
        self.assertIsNone(cmdline_for_port(pool, "9000"))

    def test_prefix_cache_flag_enabled_token_based_not_substring(self):
        self.assertTrue(prefix_cache_flag_enabled(self._cmd(8000, prefix="on")))
        self.assertFalse(prefix_cache_flag_enabled(self._cmd(8000, prefix="off")))
        self.assertIsNone(prefix_cache_flag_enabled(self._cmd(8000, prefix="absent")))

    def test_verify_endpoint_cmdlines_strict_raises_on_missing_flag(self):
        # port 8001's cmdline omits --max-num-seqs 256 -> strict must fail-closed.
        pool = [self._cmd(8000), "python -m vllm.entrypoints --port=8001 --max-num-batched-tokens 8192 --enable-prefix-caching"]
        with self.assertRaises(RuntimeError):
            verify_endpoint_cmdlines(
                pool,
                ["http://127.0.0.1:8000/v1/completions", "http://127.0.0.1:8001/v1/completions"],
                {"--max-num-seqs": "256", "--max-num-batched-tokens": "8192"},
                strict=True,
                tag="cost-profile",
            )

    def test_verify_endpoint_cmdlines_passes_when_all_declared_present(self):
        pool = [self._cmd(8000), self._cmd(8001)]
        verify_endpoint_cmdlines(  # no raise
            pool,
            ["http://127.0.0.1:8000/v1/completions", "http://127.0.0.1:8001/v1/completions"],
            {"--max-num-seqs": "256", "--max-num-batched-tokens": "8192"},
            strict=True,
            tag="cost-profile",
        )

    def test_flag_value_present_space_and_equals(self):
        c = "--max-num-seqs 256 --max-num-batched-tokens=8192"
        self.assertTrue(flag_value_present(c, "--max-num-seqs", "256"))
        self.assertTrue(flag_value_present(c, "--max-num-batched-tokens", "8192"))
        self.assertFalse(flag_value_present(c, "--max-num-seqs", "512"))

    def test_scheduler_cls_identity_distinguishes_native_and_custom(self):
        native = self._cmd(8000)
        custom = native + " --scheduler-cls=module.DRRScheduler"
        self.assertIsNone(scheduler_cls_value(native))
        self.assertEqual(scheduler_cls_value(custom), "module.DRRScheduler")
        verify_endpoint_scheduler_cls(
            [native], ["http://127.0.0.1:8000/v1/completions"], None
        )
        verify_endpoint_scheduler_cls(
            [custom],
            ["http://127.0.0.1:8000/v1/completions"],
            "module.DRRScheduler",
        )

    def test_native_fcfs_gate_rejects_custom_or_malformed_scheduler_cls(self):
        url = ["http://127.0.0.1:8000/v1/completions"]
        with self.assertRaisesRegex(RuntimeError, "scheduler class drift"):
            verify_endpoint_scheduler_cls(
                [self._cmd(8000) + " --scheduler-cls module.VTCScheduler"],
                url,
                None,
            )
        with self.assertRaisesRegex(RuntimeError, "bare --scheduler-cls"):
            scheduler_cls_value(self._cmd(8000) + " --scheduler-cls")


if __name__ == "__main__":
    unittest.main()
