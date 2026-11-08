"""Recorded command-line checks for the maintained scenario experiment."""

import unittest

from src.experiments.shared_vllm.preflight import (
    cmdline_for_port,
    flag_value_present,
    prefix_cache_flag_enabled,
    verify_endpoint_cmdlines,
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
        commands = [
            "python -m vllm.entrypoints --port=8001 --max-num-batched-tokens 8192 --enable-prefix-caching",
            self._cmd(8001, seqs="2560"),
            self._cmd(8001).replace("--max-num-seqs 256", "--max-num-seqs=2560"),
        ]
        for command in commands:
            with self.subTest(command=command), self.assertRaises(RuntimeError):
                verify_endpoint_cmdlines(
                    [self._cmd(8000), command],
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
        for command in ("--max-num-seqs 320", "--max-num-seqs=320"):
            with self.subTest(command=command):
                self.assertFalse(flag_value_present(command, "--max-num-seqs", "32"))
