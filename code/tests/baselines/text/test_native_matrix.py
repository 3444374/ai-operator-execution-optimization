from __future__ import annotations

import json
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace


CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.contracts import ChatRequest
from src.baselines.common.manifests import write_manifest
from src.baselines.text.orchestration.native_matrix import (
    balanced_arm_order,
    load_native_matrix_config,
    run_native_text_matrix,
)


class NativeTextMatrixTests(unittest.TestCase):
    @staticmethod
    @contextmanager
    def _instrumentation(_urls: tuple[str, ...], path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("sample_index,gpu_index,gpu_utilization_pct\n0,0,88\n", encoding="utf-8")
        yield SimpleNamespace(
            gpu_summary={"gpu0_util_mean": 88.0, "n_samples": 1.0},
            gauge_summary={"vllm_running_mean": 8.0, "n_gauge_samples": 1.0},
            ttft_deltas={0: {"vllm_estimated_flops_delta": 1.0}},
        )

    def _config_path(self, root: Path, *, minimum_seconds: float = 60.0) -> Path:
        manifest = root / "manifest.jsonl"
        write_manifest(
            manifest,
            tuple(
                ChatRequest(
                    doc_id=index,
                    prompt=f"p-{index}",
                    arrival_time_s=0.0,
                    prompt_tokens=4,
                    max_output_tokens=8,
                    estimated_output_tokens=8,
                    source_row_hash=f"row-{index}",
                    endpoint_index=index % 2,
                )
                for index in range(4)
            ),
        )
        payload = {
            "schema_version": 1,
            "experiment_id": "native-matrix-test",
            "formal": True,
            "rows_total": 4,
            "endpoint_urls": [
                "http://127.0.0.1:8000/v1/chat/completions",
                "http://127.0.0.1:8001/v1/chat/completions",
            ],
            "completion_protocol": "chat_completions",
            "model": "qwen",
            "tokenizer": None,
            "service": {
                "prefix_caching": "enabled",
                "max_num_seqs": 256,
                "max_num_batched_tokens": 8192,
            },
            "manifest": str(manifest),
            "output_root": str(root / "out"),
            "warmup_repeats": 1,
            "formal_repeats": 3,
            "schedule_seed": 17,
            "minimum_measurement_seconds": minimum_seconds,
            "hard_gates": {
                "provenance_fields_present": True,
                "native_arms_have_no_project_scheduler": True,
                "exactly_once": True,
                "failed_rows": 0,
                "worker_failures": 0,
                "vllm_running_final": 0,
                "vllm_waiting_final": 0,
                "both_endpoints_used": True,
                "service_counter_consistency": True,
                "same_model": True,
                "same_protocol": True,
                "same_service_config": True,
                "endpoint_predicted_work_skew_max": 0.02,
            },
            "partition_policy": None,
            "arms": [
                {
                    "id": "daft_native",
                    "adapter": "daft_native",
                    "concurrency_per_endpoint": 1,
                    "batch_size": 16,
                    "ray_address": None,
                    "python_executable": "driver-python",
                    "calibration": {
                        "selection": "vendor-default",
                        "fingerprint": "daft-native-default-v1",
                    },
                },
                {
                    "id": "ray_data_http",
                    "adapter": "ray_data_http",
                    "concurrency_per_endpoint": 4,
                    "batch_size": 16,
                    "ray_address": "127.0.0.1:6380",
                    "python_executable": "driver-python",
                    "calibration": {
                        "selection": "frozen-grid-winner",
                        "fingerprint": "sha256:ray-data-test",
                    },
                },
            ],
        }
        path = root / "matrix.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_balanced_order_is_deterministic_and_rotates_formal_first_arm(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = load_native_matrix_config(self._config_path(Path(directory)))
            first = balanced_arm_order(config, "formal", 1)
            second = balanced_arm_order(config, "formal", 2)
            self.assertEqual(first, balanced_arm_order(config, "formal", 1))
            self.assertEqual(first[0].cell_id, second[-1].cell_id)
            self.assertEqual(second[0].cell_id, first[-1].cell_id)

    def test_matrix_derives_one_isolated_core_gate_per_interleaved_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = self._config_path(root)
            calls: list[dict] = []

            def fake_gate(path: Path, **_kwargs: object) -> dict[str, object]:
                payload = json.loads(path.read_text(encoding="utf-8"))
                calls.append(payload)
                run_root = Path(payload["output_root"])
                arm = payload["cells"][0]["id"]
                gate_path = run_root / arm / "gate.json"
                gate_path.parent.mkdir(parents=True)
                gate_path.write_text(
                    json.dumps({"metrics": {"group_service_wall_s": 61.0}}),
                    encoding="utf-8",
                )
                return {"status": "passed"}

            result = run_native_text_matrix(
                config_path,
                driver_python="driver-python",
                vllm_python="vllm-python",
                core_gate_invoker=fake_gate,
                cell_instrumenter=self._instrumentation,
            )

            self.assertEqual(len(calls), 8)  # 2 arms * (1 warmup + 3 formal)
            self.assertEqual(result["comparison_admission"], "admissible")
            self.assertTrue(all(call["formal"] is False for call in calls))
            self.assertTrue(all(len(call["cells"]) == 1 for call in calls))
            index = json.loads((root / "out" / "matrix_index.json").read_text())
            self.assertEqual(index["formal_runs_rankable"], 6)
            self.assertEqual(index["runs"][0]["duration_status"], "warmup_not_ranked")
            self.assertTrue(Path(index["runs"][0]["gpu_resource_trace"]).is_file())
            self.assertEqual(index["runs"][0]["gpu_summary"]["gpu0_util_mean"], 88.0)
            self.assertEqual(index["runs"][0]["gauge_summary"]["vllm_running_mean"], 8.0)

    def test_short_formal_result_is_preserved_but_not_rankable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = self._config_path(root, minimum_seconds=60.0)

            def fake_gate(path: Path, **_kwargs: object) -> dict[str, object]:
                payload = json.loads(path.read_text(encoding="utf-8"))
                run_root = Path(payload["output_root"])
                arm = payload["cells"][0]["id"]
                gate_path = run_root / arm / "gate.json"
                gate_path.parent.mkdir(parents=True)
                gate_path.write_text(
                    json.dumps({"metrics": {"group_service_wall_s": 5.0}}),
                    encoding="utf-8",
                )
                return {"status": "passed"}

            result = run_native_text_matrix(
                config_path,
                driver_python="driver-python",
                vllm_python="vllm-python",
                core_gate_invoker=fake_gate,
                cell_instrumenter=self._instrumentation,
            )

            self.assertEqual(result["status"], "not_rankable")
            self.assertEqual(result["formal_runs_rankable"], 0)
            formal = [item for item in result["runs"] if item["phase"] == "formal"]
            self.assertTrue(all(item["duration_status"] == "below_minimum_not_rankable" for item in formal))

    def test_gate_failure_is_preserved_and_stops_the_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = self._config_path(root)

            def failing_gate(_path: Path, **_kwargs: object) -> dict[str, object]:
                raise RuntimeError("simulated cell failure")

            with self.assertRaisesRegex(RuntimeError, "simulated cell failure"):
                run_native_text_matrix(
                    config_path,
                    driver_python="driver-python",
                    vllm_python="vllm-python",
                    core_gate_invoker=failing_gate,
                    cell_instrumenter=self._instrumentation,
                )
            index = json.loads((root / "out" / "matrix_index.json").read_text())
            self.assertEqual(index["status"], "failed")
            self.assertEqual(index["comparison_admission"], "not_rankable")
            self.assertEqual(len(index["runs"]), 1)
            self.assertEqual(index["runs"][0]["status"], "failed")

    def test_requires_explicit_per_arm_calibration_and_python(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self._config_path(Path(directory))
            payload = json.loads(path.read_text(encoding="utf-8"))
            del payload["arms"][0]["calibration"]
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing.*calibration"):
                load_native_matrix_config(path)


if __name__ == "__main__":
    unittest.main()
