"""Tests for fine-grained project request timing decomposition."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
SCRIPT = CODE_ROOT / "scripts" / "analysis" / "summarize_opening_short_job_interference.py"
SPEC = importlib.util.spec_from_file_location(
    "summarize_opening_short_job_interference", SCRIPT
)
mod = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


class ProjectRequestTimingTests(unittest.TestCase):
    def _row(
        self,
        arrival: float,
        flush: float,
        submit: float,
        service_start: float,
        completion: float,
    ) -> dict[str, object]:
        return {
            "arrival_epoch_s": arrival,
            "flush_epoch_s": flush,
            "submit_epoch_s": submit,
            "service_start_epoch_s": service_start,
            "completion_epoch_s": completion,
            "e2e_s": completion - arrival,
        }

    def test_jct_is_split_into_arrival_span_and_final_drain(self) -> None:
        metrics = mod._project_request_timing_metrics(
            [
                self._row(10.0, 10.1, 10.2, 10.3, 11.3),
                self._row(12.0, 12.1, 12.2, 12.4, 14.0),
                self._row(16.0, 16.2, 16.3, 16.5, 19.0),
            ]
        )
        self.assertAlmostEqual(metrics["jct_s"], 9.0)
        self.assertAlmostEqual(metrics["arrival_span_s"], 6.0)
        self.assertAlmostEqual(metrics["post_last_arrival_drain_s"], 3.0)
        self.assertAlmostEqual(metrics["arrival_span_fraction"], 2.0 / 3.0)
        self.assertAlmostEqual(metrics["submit_to_service_s_p50"], 0.2)
        self.assertAlmostEqual(metrics["service_s_p99"], 2.482)

    def test_negative_stage_duration_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "negative buffer"):
            mod._project_request_timing_metrics(
                [self._row(10.0, 9.9, 10.2, 10.3, 11.0)]
            )


if __name__ == "__main__":
    unittest.main()
