from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_image_clip_matrix.py"
SPEC = importlib.util.spec_from_file_location("run_image_clip_matrix", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ImageClipMatrixTests(unittest.TestCase):
    def test_load_config_builds_interleavable_scenarios(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "experiment_id": "image_formal",
                        "seed": 42,
                        "warmup_runs_per_scenario": 1,
                        "formal_repeats": 3,
                        "minimum_unique_rows": 20000,
                        "minimum_steady_state_s": 60,
                        "common_args": ["--limit", "60000"],
                        "scenarios": [
                            {"scenario_id": "a", "args": ["--arm", "project_ray"]},
                            {"scenario_id": "b", "args": ["--arm", "daft_staged"]},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = MODULE.load_config(path)

            self.assertEqual(config.formal_repeats, 3)
            self.assertEqual([item.scenario_id for item in config.scenarios], ["a", "b"])

    def test_runner_owned_flags_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "runner-owned"):
            MODULE._reject_owned_flags(("--phase", "formal"))

    def test_formal_duration_and_correctness_gate(self) -> None:
        config = MODULE.MatrixConfig(
            experiment_id="x",
            seed=1,
            warmup_runs_per_scenario=1,
            formal_repeats=3,
            minimum_unique_rows=20000,
            minimum_steady_state_s=60.0,
            common_args=(),
            scenarios=(MODULE.Scenario("a", ()),),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(
                json.dumps(
                    {
                        "row": {
                            "arm": "project_ray",
                            "rows": 60000,
                            "output_rows": 60000,
                            "exactly_once": True,
                            "operator_e2e_s": 70.0,
                            "worker_setup_s": 8.0,
                        }
                    }
                ),
                encoding="utf-8",
            )

            row = MODULE._validated_row(path, config, "formal")
            self.assertEqual(MODULE._steady_state_proxy(row), 62.0)

            row["operator_e2e_s"] = 67.0
            path.write_text(json.dumps({"row": row}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "below"):
                MODULE._validated_row(path, config, "formal")

    def test_repeated_dataset_passes_do_not_inflate_unique_image_gate(self) -> None:
        config = MODULE.MatrixConfig(
            experiment_id="x",
            seed=1,
            warmup_runs_per_scenario=0,
            formal_repeats=1,
            minimum_unique_rows=20000,
            minimum_steady_state_s=60.0,
            common_args=(),
            scenarios=(MODULE.Scenario("a", ()),),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            row = {
                "arm": "project_ray",
                "rows": 120000,
                "unique_images": 60000,
                "dataset_passes": 2,
                "output_rows": 120000,
                "exactly_once": True,
                "operator_e2e_s": 70.0,
                "worker_setup_s": 8.0,
            }
            path.write_text(json.dumps({"row": row}), encoding="utf-8")

            self.assertEqual(
                MODULE._validated_row(path, config, "formal")["unique_images"],
                60000,
            )

            row["unique_images"] = 10000
            path.write_text(json.dumps({"row": row}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "minimum_unique_rows"):
                MODULE._validated_row(path, config, "formal")


if __name__ == "__main__":
    unittest.main()
