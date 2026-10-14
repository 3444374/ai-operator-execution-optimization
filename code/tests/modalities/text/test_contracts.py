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
    parse_service_waiting_requests,
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

    def test_unknown_waiting_preserves_other_typed_observations(self) -> None:
        snapshot = build_text_runtime_snapshot(
            active_work=20, upstream_queued_work=100, service_waiting_requests=None,
            active_requests=2, oldest_upstream_age_s=2, observed_at_s=3,
            capacity_work=20, calibration_signature=self.signature,
            service_rate_tokens_s=1,
        )
        model = snapshot.for_stage("model")
        self.assertIsNone(model.queued_work)
        self.assertEqual(
            (model.active_work, model.capacity_work, model.service_rate_units_s), (20, 20, 1),
        )
        self.assertEqual(snapshot.for_stage("organizer").queued_work, 100)
        self.assertEqual(snapshot.observed_at_s, 3)
        self.assertEqual(model.observed_at_s, 3)

    def test_waiting_parser_preserves_counts_and_unknowns_without_truncation(self) -> None:
        for raw, expected in (
            (None, None), ("", None), (0, 0), ("0", 0), (1., 1), ("2.0", 2),
            (2**53 + 1, 2**53 + 1),
            ("9007199254740993", 9007199254740993),
            ("9007199254740993.0", 9007199254740993),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(parse_service_waiting_requests(raw), expected)
        for raw in (-1, True, 1.5, "nan", "inf", "bad", "1.00000000000000001"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_service_waiting_requests(raw)


if __name__ == "__main__":
    unittest.main()
