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
