"""Contracts for exact SemFilter matched-reference calibration artifacts."""

from __future__ import annotations

import unittest
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from src.planning.semfilter_reference_calibration import (
    build_reference_calibration,
    load_json_document,
    validate_reference_calibration,
)


SEMANTIC_DIGEST = "1" * 64
PHYSICAL_DIGEST = "2" * 64
WORKLOAD_SIGNATURE = "3" * 64
SERVICE_SIGNATURE = "4" * 64
REPO_ROOT = Path(__file__).resolve().parents[3]
CLI = REPO_ROOT / "code" / "scripts" / "analysis" / "build_semfilter_reference_calibration.py"


def _source() -> dict:
    return {
        "schema_version": 1,
        "generated_at": "2026-09-01T01:02:03Z",
        "semantic_spec_digest": SEMANTIC_DIGEST,
        "physical_algorithm_digest": PHYSICAL_DIGEST,
        "provider_execution_profile": "openai-compatible-fixed",
        "model_id": "reference-model-v1",
        "model_role": "reference",
        "workload_signature": WORKLOAD_SIGNATURE,
        "service_signature": SERVICE_SIGNATURE,
        "accepted_max_relative_error": 0.05,
        "training_observations": [
            {
                "semantic_input_rows": 100,
                "output_rows": 40,
                "model_calls": 80,
                "prompt_tokens": 1600,
                "output_tokens": 80,
                "service_milliseconds": 146,
            },
            {
                "semantic_input_rows": 50,
                "output_rows": 20,
                "model_calls": 40,
                "prompt_tokens": 600,
                "output_tokens": 80,
                "service_milliseconds": 96,
            },
            {
                "semantic_input_rows": 75,
                "output_rows": 30,
                "model_calls": 60,
                "prompt_tokens": 900,
                "output_tokens": 30,
                "service_milliseconds": 94,
            },
            {
                "semantic_input_rows": 120,
                "output_rows": 48,
                "model_calls": 96,
                "prompt_tokens": 2880,
                "output_tokens": 192,
                "service_milliseconds": 230.8,
            },
        ],
        "held_out_observations": [
            {
                "semantic_input_rows": 345,
                "output_rows": 138,
                "model_calls": 276,
                "prompt_tokens": 5980,
                "output_tokens": 382,
                "service_milliseconds": 536.8,
            }
        ],
    }


class SemFilterReferenceCalibrationTests(unittest.TestCase):
    def test_builder_rejects_fixed_output_usage_even_with_zero_held_out_error(self) -> None:
        source = _source()
        source["training_observations"] = [
            dict(semantic_input_rows=calls, output_rows=rows, model_calls=calls,
                 prompt_tokens=prompt, output_tokens=output, service_milliseconds=service)
            for calls, rows, prompt, output, service in (
                (90, 36, 360, 180, 316),
                (60, 24, 1260, 120, 316),
                (100, 40, 1100, 200, 420),
                (15, 6, 270, 30, 82),
            )
        ]
        source["held_out_observations"] = [
            dict(semantic_input_rows=265, output_rows=106, model_calls=265,
                 prompt_tokens=2990, output_tokens=530, service_milliseconds=1104)
        ]

        with self.assertRaisesRegex(ValueError, "do not identify all cost coefficients"):
            build_reference_calibration(source)

    def test_builder_rejects_nearly_dependent_usage_before_held_out_validation(self) -> None:
        source = _source()
        training = source["training_observations"]
        for index, row in enumerate(training):
            for field in ("semantic_input_rows", "output_rows", "model_calls", "prompt_tokens"):
                row[field] *= 1_000_000_000
            row["output_tokens"] = 2 * row["model_calls"] + (index == 0)
            row["service_milliseconds"] = (10 + 2 * row["model_calls"]
                + row["prompt_tokens"] / 100 + row["output_tokens"] / 2)
        # One extra token makes the matrix exactly full-rank, but carries
        # negligible information compared with billions of fixed-length calls.
        held_out = {field: sum(row[field] for row in training) for field in training[0]}
        held_out["service_milliseconds"] -= 30
        source["held_out_observations"] = [held_out]

        with self.assertRaisesRegex(ValueError, "nearly collinear"):
            build_reference_calibration(source)

    def test_builder_accepts_identifiable_usage_across_units_and_row_order(self) -> None:
        source = _source()
        source["training_observations"].reverse()
        for row in source["training_observations"] + source["held_out_observations"]:
            row["prompt_tokens"] *= 1_000_000
            row["output_tokens"] *= 1_000_000

        artifact = build_reference_calibration(source)

        self.assertEqual(artifact["service_fixed_milliseconds"], "10")
        self.assertEqual(artifact["service_ms_per_model_call"], "1")
        self.assertEqual(artifact["service_ms_per_prompt_token"], "1e-08")
        self.assertEqual(artifact["service_ms_per_output_token"], "5e-07")
        self.assertEqual(artifact["held_out_max_relative_error"], "0")

    def test_builder_rejects_chained_near_dependence_despite_non_small_pivots(self) -> None:
        source = _source()
        source["training_observations"] = [
            dict(semantic_input_rows=2_000_000_000, output_rows=800_000_000,
                 model_calls=calls, prompt_tokens=prompt, output_tokens=output,
                 service_milliseconds=1000 + calls + prompt / 100 + output / 2)
            for calls, prompt, output in (
                (1_000_000_000, 1_000_000_000, 1_000_000_000),
                (1_000_000_100, 2_000_000_000, 1_000_000_000),
                (1_000_000_000, 1_000_000_100, 2_000_000_000),
                (1_000_000_000, 1_000_000_000, 1_000_000_100),
            )
        ]
        held_out = {field: sum(row[field] for row in source["training_observations"])
                    for field in source["training_observations"][0]}
        held_out["service_milliseconds"] -= 3000
        source["held_out_observations"] = [held_out]

        with self.assertRaisesRegex(ValueError, "nearly collinear"):
            build_reference_calibration(source)

    def test_builder_rejects_unobserved_or_constant_work_dimensions(self) -> None:
        for field, value in (("output_tokens", 0), ("model_calls", 40)):
            with self.subTest(field=field):
                source = _source()
                for row in source["training_observations"]:
                    row[field] = value
                with self.assertRaisesRegex(ValueError, "do not identify all cost coefficients"):
                    build_reference_calibration(source)

    def test_builder_separates_cardinality_work_service_and_held_out_evidence(
        self,
    ) -> None:
        artifact = build_reference_calibration(_source())

        self.assertEqual(artifact["schema_version"], 1)
        self.assertEqual(artifact["cost_model_id"], "semloom.exact_filter.reference-calibrated.v1")
        self.assertEqual(artifact["training_sample_count"], 4)
        self.assertEqual(artifact["held_out_sample_count"], 1)
        self.assertEqual(artifact["training_semantic_input_rows"], "345")
        self.assertEqual(artifact["held_out_semantic_input_rows"], "345")
        self.assertEqual(artifact["output_selectivity"], "0.4")
        self.assertEqual(artifact["model_calls_per_input_row"], "0.8")
        self.assertEqual(artifact["prompt_tokens_per_call"], "21.666666666666668")
        self.assertEqual(artifact["output_tokens_per_call"], "1.3840579710144927")
        self.assertEqual(artifact["service_fixed_milliseconds"], "10")
        self.assertEqual(artifact["service_ms_per_model_call"], "1")
        self.assertEqual(artifact["service_ms_per_prompt_token"], "0.01")
        self.assertEqual(artifact["service_ms_per_output_token"], "0.5")
        self.assertEqual(artifact["held_out_mean_relative_error"], "0")
        self.assertEqual(artifact["held_out_max_relative_error"], "0")
        self.assertEqual(artifact["held_out_signed_error_lower"], "0")
        self.assertEqual(artifact["held_out_signed_error_upper"], "0")
        self.assertEqual(len(artifact["evidence_digest"]), 64)
        self.assertEqual(len(artifact["artifact_id"]), 64)

        validated = validate_reference_calibration(artifact)
        self.assertEqual(validated.artifact_id, artifact["artifact_id"])
        self.assertEqual(validated.output_selectivity, 0.4)
        self.assertEqual(validated.service_fixed_milliseconds, 10.0)
        self.assertEqual(validated.service_ms_per_model_call, 1.0)
        self.assertEqual(validated.service_ms_per_prompt_token, 0.01)
        self.assertEqual(validated.service_ms_per_output_token, 0.5)

    def test_cli_writes_a_validated_repository_external_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source_path = root / "source.json"
            artifact_path = root / "artifact.json"
            source_path.write_text(
                json.dumps(_source(), ensure_ascii=False), encoding="utf-8"
            )

            completed = subprocess.run(
                [
                    sys.executable,
                    str(CLI),
                    "--source",
                    str(source_path),
                    "--output",
                    str(artifact_path),
                ],
                cwd=REPO_ROOT,
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
            self.assertEqual(
                validate_reference_calibration(artifact).artifact_id,
                artifact["artifact_id"],
            )
            self.assertEqual(completed.stdout.strip(), artifact["artifact_id"])

    def test_builder_rejects_held_out_error_above_the_registered_limit(self) -> None:
        source = _source()
        source["held_out_observations"][0]["output_rows"] = 200

        with self.assertRaisesRegex(ValueError, "held-out maximum relative error"):
            build_reference_calibration(source)

    def test_validator_rejects_artifact_content_changed_after_identity(self) -> None:
        artifact = build_reference_calibration(_source())
        artifact["service_ms_per_output_token"] = "0.6"

        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            validate_reference_calibration(artifact)

    def test_json_loader_rejects_duplicate_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate field"):
            load_json_document('{"schema_version":1,"schema_version":1}')


if __name__ == "__main__":
    unittest.main()
