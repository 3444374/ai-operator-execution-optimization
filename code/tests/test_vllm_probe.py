from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.vllm_probe import (  # noqa: E402
    parse_prefix_caching_flag,
    probe_live_prefix_caching,
)
from scripts.run_ai_operator_scenarios import (  # noqa: E402
    _verify_prefix_caching_matches_live,
)


class ParsePrefixCachingFlagTests(unittest.TestCase):
    def test_explicit_enable_is_true(self) -> None:
        cmdline = (
            "python -m vllm.entrypoints.openai.api_server --model qwen "
            "--enable-prefix-caching --port 8000"
        )
        self.assertIs(parse_prefix_caching_flag(cmdline), True)

    def test_no_enable_is_false(self) -> None:
        cmdline = (
            "python -m vllm.entrypoints.openai.api_server "
            "--no-enable-prefix-caching --port 8001"
        )
        self.assertIs(parse_prefix_caching_flag(cmdline), False)

    def test_absent_returns_none(self) -> None:
        cmdline = (
            "python -m vllm.entrypoints.openai.api_server "
            "--port 8000 --max-num-seqs 256"
        )
        self.assertIsNone(parse_prefix_caching_flag(cmdline))

    def test_last_flag_wins(self) -> None:
        cmdline = (
            "python -m vllm.entrypoints.openai.api_server "
            "--enable-prefix-caching --no-enable-prefix-caching"
        )
        self.assertIs(parse_prefix_caching_flag(cmdline), False)

    def test_equals_form(self) -> None:
        self.assertIs(
            parse_prefix_caching_flag("--enable-prefix-caching=true"), True
        )
        self.assertIs(
            parse_prefix_caching_flag("--enable-prefix-caching=false"), False
        )

    def test_empty_returns_none(self) -> None:
        self.assertIsNone(parse_prefix_caching_flag(""))


class VerifyMatchesLiveTests(unittest.TestCase):
    """Runner helper ``_verify_prefix_caching_matches_live`` logic."""

    @staticmethod
    def _config(prefix_caching: object) -> object:
        class _Cfg:
            pass

        cfg = _Cfg()
        cfg.service_metadata = (
            (("prefix_caching", prefix_caching),)
            if prefix_caching is not None
            else ()
        )
        return cfg

    def test_declared_not_bool_skips_probe(self) -> None:
        with patch(
            "scripts.run_ai_operator_scenarios.probe_live_prefix_caching"
        ) as mock_probe:
            _verify_prefix_caching_matches_live(self._config(None))
            mock_probe.assert_not_called()

    def test_mismatch_raises(self) -> None:
        with patch(
            "scripts.run_ai_operator_scenarios.probe_live_prefix_caching",
            return_value=True,
        ):
            with self.assertRaises(ValueError):
                _verify_prefix_caching_matches_live(self._config(False))

    def test_match_does_not_raise(self) -> None:
        with patch(
            "scripts.run_ai_operator_scenarios.probe_live_prefix_caching",
            return_value=True,
        ):
            _verify_prefix_caching_matches_live(self._config(True))

    def test_probe_none_warns_without_raising(self) -> None:
        with patch(
            "scripts.run_ai_operator_scenarios.probe_live_prefix_caching",
            return_value=None,
        ):
            _verify_prefix_caching_matches_live(self._config(False))


class ProbeLivePrefixCachingTests(unittest.TestCase):
    def test_no_procs_returns_none(self) -> None:
        with patch("src.vllm_probe._list_process_cmdlines", return_value=[]):
            self.assertIsNone(probe_live_prefix_caching())

    def test_agreeing_procs_return_common_flag(self) -> None:
        lines = [
            "python -m vllm.entrypoints.openai.api_server "
            "--enable-prefix-caching --port 8000",
            "python -m vllm.entrypoints.openai.api_server "
            "--enable-prefix-caching --port 8001",
            "some unrelated process --port 9000",
        ]
        with patch("src.vllm_probe._list_process_cmdlines", return_value=lines):
            self.assertIs(probe_live_prefix_caching(), True)

    def test_disagreeing_procs_return_none(self) -> None:
        lines = [
            "python -m vllm.entrypoints.openai.api_server "
            "--enable-prefix-caching --port 8000",
            "python -m vllm.entrypoints.openai.api_server "
            "--no-enable-prefix-caching --port 8001",
        ]
        with patch("src.vllm_probe._list_process_cmdlines", return_value=lines):
            self.assertIsNone(probe_live_prefix_caching())

    def test_procs_without_flag_return_none(self) -> None:
        lines = [
            "python -m vllm.entrypoints.openai.api_server --port 8000"
        ]
        with patch("src.vllm_probe._list_process_cmdlines", return_value=lines):
            self.assertIsNone(probe_live_prefix_caching())

    def test_non_vllm_lines_ignored(self) -> None:
        lines = ["bash --enable-prefix-caching --no-enable-prefix-caching"]
        with patch("src.vllm_probe._list_process_cmdlines", return_value=lines):
            self.assertIsNone(probe_live_prefix_caching())


if __name__ == "__main__":
    unittest.main()
