from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.text.contracts import (  # noqa: E402
    build_text_runtime_snapshot,
    build_text_work_descriptor,
    text_work_calibration_signature,
)


class TextWorkContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.signature = text_work_calibration_signature(
            model_revision="qwen-rev",
            serving_revision="vllm-0.25.1",
            protocol="chat_completions",
            cost_model_revision="prompt-plus-output-v1",
        )

    def test_descriptor_exposes_model_work_and_uncertainty(self) -> None:
        descriptor = build_text_work_descriptor(
            prompt_tokens=256,
            estimated_output_tokens=128,
            prompt_bytes=1024,
            result_bytes_upper=2048,
            calibration_signature=self.signature,
            prefix_key="tenant-a",
        )

        self.assertEqual(descriptor.primary.units, 384)
        self.assertEqual(descriptor.lower_primary_units, 256)
        self.assertEqual(descriptor.upper_primary_units, 384)
        self.assertEqual(descriptor.locality_key, "tenant-a")

    def test_snapshot_separates_upstream_and_service_queue(self) -> None:
        snapshot = build_text_runtime_snapshot(
            active_work=1024,
            upstream_queued_work=512,
            service_waiting_requests=2,
            active_requests=4,
            oldest_upstream_age_s=1.5,
            observed_at_s=10.0,
            capacity_work=2048,
            calibration_signature=self.signature,
            service_rate_tokens_s=1000.0,
        )

        self.assertEqual(snapshot.for_stage("organizer").queued_work, 512)
        self.assertEqual(snapshot.for_stage("model").active_work, 1024)
        self.assertEqual(snapshot.for_stage("model").queued_work, 512)
        self.assertEqual(snapshot.for_stage("model").capacity_work, 2048)


if __name__ == "__main__":
    unittest.main()
