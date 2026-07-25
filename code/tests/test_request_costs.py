from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.request_costs import output_cost_source, resolve_output_tokens


class RequestCostTests(unittest.TestCase):
    def test_modes_resolve_only_the_output_contribution(self) -> None:
        self.assertEqual(
            resolve_output_tokens(
                "prompt_only",
                completion_max_tokens=16,
                target_output_tokens=9,
            ),
            0,
        )
        self.assertEqual(
            resolve_output_tokens(
                "fixed_output_cap",
                completion_max_tokens=16,
                target_output_tokens=9,
            ),
            16,
        )
        self.assertEqual(
            resolve_output_tokens(
                "trace_target_output",
                completion_max_tokens=16,
                target_output_tokens=9,
            ),
            9,
        )

    def test_modes_have_explicit_non_oracle_sources(self) -> None:
        self.assertEqual(output_cost_source("prompt_only"), "configured_zero")
        self.assertEqual(
            output_cost_source("fixed_output_cap"),
            "backend_completion_cap",
        )
        self.assertEqual(
            output_cost_source("trace_target_output"),
            "burstgpt_unpaired_trace_metadata",
        )

    def test_invalid_values_fail_explicitly(self) -> None:
        invalid_targets = [None, True, 1.5, "4", -1]
        for value in invalid_targets:
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "target_output_tokens",
                ):
                    resolve_output_tokens(
                        "trace_target_output",
                        completion_max_tokens=16,
                        target_output_tokens=value,
                    )
        for cap in (-1, True, 1.5):
            with self.subTest(cap=cap):
                with self.assertRaisesRegex(
                    ValueError,
                    "completion_max_tokens",
                ):
                    resolve_output_tokens(
                        "fixed_output_cap",
                        completion_max_tokens=cap,
                        target_output_tokens=0,
                    )
        with self.assertRaisesRegex(ValueError, "output cost mode"):
            output_cost_source("unknown")


if __name__ == "__main__":
    unittest.main()
