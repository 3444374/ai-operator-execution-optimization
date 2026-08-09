from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.image_multijob.manifest import load_image_job_manifest
from src.experiments.image_multijob.native import (
    build_job_command,
    load_native_image_multijob_config,
)
from src.experiments.image_multijob.project import load_project_image_multijob_config


class ImageMultiJobContractTests(unittest.TestCase):
    def _manifest(self, root: Path) -> Path:
        path = root / "jobs.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "status": "ready",
                    "selection": {"kind": "test"},
                    "jobs": [
                        {
                            "job_id": name,
                            "workload_name": "coco",
                            "limit": rows,
                            "offset": offset,
                            "multi_job_start_offset_s": 0.0 if index == 0 else 5.0,
                            "doc_ids_sha256": str(index + 1) * 64,
                            "input_encoded_bytes": rows * 100,
                            "avg_encoded_bytes": 100.0,
                        }
                        for index, (name, rows, offset) in enumerate(
                            (("short", 2, 0), ("long1", 3, 2), ("long2", 3, 5), ("long3", 3, 8))
                        )
                    ],
                }
            ),
            encoding="utf-8",
        )
        return path

    def _native_config(self, root: Path, manifest: Path) -> Path:
        path = root / "native.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "formal": True,
                    "experiment_id": "native-image-test",
                    "output_root": str(root / "native-out"),
                    "python_executable": sys.executable,
                    "image_runner": "runner.py",
                    "ray_address": "ray://127.0.0.1:10001",
                    "job_manifest": str(manifest),
                    "common_args": [
                        "--model", "/model",
                        "--processor", "/model",
                        "--batch-size", "64",
                        "--gpu-workers", "2",
                        "--source-shards", "4",
                        "--dtype", "float16",
                        "--embedding-output-contract", "l2_normalized",
                        "--model-flops-per-image", 1,
                        "--gpu-peak-flops-per-s", 2,
                    ],
                    "launch_lead_s": 0.1,
                    "ready_timeout_s": 2.0,
                    "process_timeout_s": 3.0,
                    "gpu_sample_interval_s": 0.5,
                    "cpu_sample_interval_s": 0.25,
                    "warmup_repeats": 1,
                    "formal_repeats": 3,
                    "schedule_seed": 9,
                    "minimum_measurement_seconds": 1.0,
                    "arms": [
                        *[
                            {
                                "id": f"daft_builtin_single_{job_id}",
                                "adapter": "daft_builtin_embed",
                                "args": ["--cpu-workers", "4"],
                                "jobs": [job_id],
                            }
                            for job_id in ("short", "long1", "long2", "long3")
                        ],
                        {
                            "id": "daft_builtin_fourjob",
                            "adapter": "daft_builtin_embed",
                            "args": ["--cpu-workers", "4"],
                            "jobs": ["short", "long1", "long2", "long3"],
                        },
                        *[
                            {
                                "id": f"ray_data_single_{job_id}",
                                "adapter": "ray_data_staged",
                                "args": ["--cpu-workers", "16"],
                                "jobs": [job_id],
                            }
                            for job_id in ("short", "long1", "long2", "long3")
                        ],
                        {
                            "id": "ray_data_fourjob",
                            "adapter": "ray_data_staged",
                            "args": ["--cpu-workers", "16"],
                            "jobs": ["short", "long1", "long2", "long3"],
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )
        return path

    def _project_config(self, root: Path, manifest: Path) -> Path:
        path = root / "project.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "formal": True,
                    "experiment_id": "project-image-test",
                    "output_root": str(root / "project-out"),
                    "database_url": "postgresql://postgres:postgres@localhost:5432/ai_operator",
                    "ray_address": "ray://127.0.0.1:10001",
                    "job_manifest": str(manifest),
                    "policy_revision": "shared-work-v1",
                    "model": "/model",
                    "processor": "/model",
                    "dtype": "float16",
                    "batch_size": 64,
                    "source_shards": 4,
                    "cpu_workers": 16,
                    "gpu_workers": 2,
                    "max_active_batches": 32,
                    "model_flops_per_image": 1.0,
                    "gpu_peak_flops_per_s": 2.0,
                    "source_queue_batches_per_job": 2,
                    "warmup_rows": 64,
                    "warmup_repeats": 1,
                    "formal_repeats": 3,
                    "schedule_seed": 9,
                    "gpu_sample_interval_s": 0.5,
                    "cpu_sample_interval_s": 0.25,
                    "scenarios": [
                        *[
                            {
                                "id": f"single_{job_id}_full_pool",
                                "policy": "static_partition",
                                "jobs": [job_id],
                            }
                            for job_id in ("short", "long1", "long2", "long3")
                        ],
                        {
                            "id": "fourjob_static_partition",
                            "policy": "static_partition",
                            "jobs": ["short", "long1", "long2", "long3"],
                        },
                        {
                            "id": "fourjob_proposed",
                            "policy": "proposed",
                            "jobs": ["short", "long1", "long2", "long3"],
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_one_manifest_drives_native_and_project_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = self._manifest(root)
            manifest = load_image_job_manifest(manifest_path)
            native = load_native_image_multijob_config(self._native_config(root, manifest_path))
            project = load_project_image_multijob_config(self._project_config(root, manifest_path))

            self.assertEqual(native.job_manifest.sha256, manifest.sha256)
            self.assertEqual(project.job_manifest.sha256, manifest.sha256)
            self.assertEqual(native.arms[1].jobs[0].start_offset_s, 0.0)
            self.assertEqual(project.policy_revision, "shared-work-v1")

    def test_native_command_uses_shared_ray_and_contains_no_project_control(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = self._manifest(root)
            config = load_native_image_multijob_config(self._native_config(root, manifest_path))
            arm = config.arms[4]
            command = build_job_command(
                config, arm, arm.jobs[0], phase="formal", repeat=1, root=root / "cell"
            )

            self.assertIn("--ray-address", command)
            self.assertIn("--formal-start-barrier-file", command)
            self.assertNotIn("--max-active-batches", command)

    def test_project_config_rejects_nondivisible_static_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._manifest(root)
            path = self._project_config(root, manifest)
            payload = json.loads(path.read_text())
            payload["max_active_batches"] = 31
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "divisible by four"):
                load_project_image_multijob_config(path)

    def test_manifest_rejects_overlapping_ranges(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self._manifest(root)
            payload = json.loads(path.read_text())
            payload["jobs"][1]["offset"] = 1
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "overlap"):
                load_image_job_manifest(path)


if __name__ == "__main__":
    unittest.main()
