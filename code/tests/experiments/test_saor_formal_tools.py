from __future__ import annotations

import csv
import importlib.util
import json
import os
import re
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.baselines.common.contracts import BaselineRequestResult, ChatRequest
from src.baselines.common.manifests import write_manifest
from src.experiments.shared_vllm.config import SharedVllmConfig, SharedVllmScenario
from src.experiments.shared_vllm.direct_control import run_direct_control


REPOSITORY = Path(__file__).resolve().parents[3]


def _load(name: str, relative: str):
    spec = importlib.util.spec_from_file_location(name, REPOSITORY / relative)
    if spec is None or spec.loader is None:
        raise RuntimeError(relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


SUMMARY = _load(
    "summarize_saor_active_set",
    "code/scripts/analysis/summarize_saor_active_set.py",
)
AUDIT = _load(
    "audit_saor_formal_readiness",
    "code/scripts/analysis/audit_saor_formal_readiness.py",
)


class SaorFormalToolsTests(unittest.TestCase):
    def test_repository_formal_env_covers_template_contract(self) -> None:
        template = (
            REPOSITORY / "deploy/autodl/saor_active_set_release.example.json"
        ).read_text(encoding="utf-8")
        env_example = (
            REPOSITORY / "deploy/autodl/saor_active_set_formal.env.example"
        ).read_text(encoding="utf-8")
        required = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", template))
        provided = set(
            re.findall(r"^export ([A-Z][A-Z0-9_]*)=", env_example, re.MULTILINE)
        )

        self.assertEqual(required - provided, {"DATABASE_URL"})
        self.assertIn("export COMPLETION_PROTOCOL=chat_completions", env_example)
        self.assertIn("/v1/chat/completions", env_example)
        self.assertIn("export SAOR_ARRIVAL_TIME_SCALE=0.0001", env_example)
        self.assertIn(
            "export SAOR_ACTIVE_SET_WORKLOAD=sharegpt_multiturn", env_example
        )

    def test_direct_control_emits_project_compatible_job_evidence(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "jobs").mkdir()
            manifests = (root / "bulk.jsonl", root / "foreground.jsonl")
            requests = (
                (self._request(1, 0), self._request(2, 1)),
                (self._request(3, 0), self._request(4, 1)),
            )
            for path, rows in zip(manifests, requests):
                write_manifest(path, rows)
            scenario = SharedVllmScenario(
                scenario_id="direct",
                policy="direct_no_job",
                job_count=2,
                rows_per_job=None,
                rows_per_jobs=(2, 2),
                weights=(1, 1),
                arrival_offsets_s=(0.0, 5.0),
                request_manifests=tuple(str(path) for path in manifests),
            )
            config = SharedVllmConfig(
                experiment_id="direct-test",
                seed=1,
                warmup_runs_per_scenario=0,
                formal_repeats=1,
                endpoint_ids=("endpoint-0", "endpoint-1"),
                request_limit_per_endpoint=128,
                work_limit_per_endpoint=65536,
                credit_quantum=2048,
                shared_credit_namespace="test",
                gpu_peak_tflops=165.0,
                mfu_precision="bf16",
                common_args=(
                    "--arrival-replay",
                    "--completion-endpoint-urls",
                    "http://127.0.0.1:8000/v1/completions,"
                    "http://127.0.0.1:8001/v1/completions",
                    "--completion-model",
                    "qwen",
                    "--completion-protocol",
                    "completions",
                    "--completion-prompt-format",
                    "raw",
                    "--completion-temperature",
                    "0",
                    "--request-slo-ms",
                    "30000",
                ),
                scenarios=(scenario,),
                service_metadata=(),
            )

            async def fake_jobs(jobs, _contract):
                return {
                    job.job_id: tuple(
                        BaselineRequestResult(
                            doc_id=request.doc_id,
                            endpoint_index=request.endpoint_index,
                            status="completed",
                            error=None,
                            submitted_at_s=(
                                100.0 + job.arrival_offset_s + index * 0.1
                            ),
                            started_at_s=(
                                100.0 + job.arrival_offset_s + index * 0.1
                            ),
                            completed_at_s=(
                                101.0 + job.arrival_offset_s + index * 0.1
                            ),
                            input_tokens=4,
                            output_tokens=8,
                            output_text="ok",
                            finish_reason="length",
                        )
                        for index, request in enumerate(job.requests)
                    )
                    for job in jobs
                }

            with patch(
                "src.experiments.shared_vllm.direct_control.run_bounded_http_jobs",
                side_effect=fake_jobs,
            ):
                evidence = run_direct_control(
                    config,
                    scenario,
                    start_epoch_s=100.0,
                    output_dir=root,
                    run_stem="run",
                )

            self.assertEqual(len(evidence), 2)
            self.assertEqual(evidence[0]["actual_work"], 24)
            self.assertEqual(
                evidence[1]["replay_configured_start_epoch_s"], 105.0
            )
            self.assertEqual(
                evidence[0]["endpoint_counts"],
                {"endpoint-0": 1, "endpoint-1": 1},
            )
            self.assertTrue((root / "jobs/run_job0.requests.csv").is_file())

    def test_repository_formal_template_passes_static_readiness(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bulk = root / "bulk.jsonl"
            foreground = root / "foreground.jsonl"
            write_manifest(
                bulk,
                (
                    self._request(1, 0),
                    self._request(2, 1),
                    # Exactly at the foreground boundary: this request cannot
                    # establish capacity before the second Job becomes active.
                    self._request(5, 0, arrival_time_s=5001.0),
                ),
            )
            write_manifest(
                foreground,
                (
                    self._request(3, 0),
                    self._request(4, 1),
                ),
            )
            selection = root / "selection.json"
            selection.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "ready",
                        "selection": {
                            "best_token_budget": 8192,
                            "project_static_k_per_endpoint": 128,
                            "project_active_work_per_endpoint": 65536,
                            "project_actor_workers_per_endpoint": 8,
                            "project_ray_actor_max_concurrency": 32,
                            "project_ray_worker_num_cpus": 0.25,
                        },
                        "evidence": {
                            "feeding": {"status": "passed"},
                            "token_budget": {
                                "status": "passed",
                                "frozen_token_budget": 8192,
                            },
                            "actor_pool": {"status": "passed"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            environment = {
                "VLLM_VERSION": "0.25.1",
                "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
                "VLLM_MAX_NUM_SEQS": "256",
                "VLLM_GPU_MEMORY_UTILIZATION": "0.9",
                "STRATEGY_CALIBRATION_SELECTION": str(selection),
                "BEST_TOKEN_BUDGET": "8192",
                "PROJECT_STATIC_K_PER_ENDPOINT": "128",
                "PROJECT_ACTIVE_WORK_PER_ENDPOINT": "65536",
                "PROJECT_ACTOR_WORKERS_PER_ENDPOINT": "8",
                "PROJECT_RAY_ACTOR_MAX_CONCURRENCY": "32",
                "PROJECT_RAY_WORKER_NUM_CPUS": "0.25",
                "PROJECT_SHARED_CREDIT_QUANTUM": "2048",
                "DATABASE_URL": "postgresql://postgres:postgres@localhost/db",
                "SOURCE_MAX_PROMPT_TOKENS": "1500",
                "COMPLETION_ENDPOINT_URLS": (
                    "http://127.0.0.1:8000/v1/completions,"
                    "http://127.0.0.1:8001/v1/completions"
                ),
                "COMPLETION_MODEL": "qwen",
                "COMPLETION_HTTP_KEEPALIVE_EXPIRY_S": "4",
                "COMPLETION_PROTOCOL": "completions",
                "COMPLETION_MAX_TOKENS": "8",
                "MODEL_METRICS_URLS": (
                    "http://127.0.0.1:8000/metrics,"
                    "http://127.0.0.1:8001/metrics"
                ),
                "ENDPOINT_GPU_IDS": "0,1",
                "SAOR_ACTIVE_SET_WORKLOAD": "saor-test",
                "MODEL_PATH": "/models/qwen",
                "REQUEST_SLO_MS": "30000",
                "GPU_PEAK_TFLOPS": "165",
                "MFU_PRECISION": "bf16_dense_fp32_accumulate",
                "SAOR_BULK_ROWS": "3",
                "SAOR_FOREGROUND_ROWS": "2",
                "SAOR_FOREGROUND_OFFSET_S": "5",
                "SAOR_ARRIVAL_TIME_SCALE": "0.001",
                "SAOR_MAX_EFFECTIVE_MANIFEST_SPAN_S": "120",
                "SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES": "0.0001",
                "SAOR_BULK_MANIFEST": str(bulk),
                "SAOR_FOREGROUND_MANIFEST": str(foreground),
            }
            with patch.dict(os.environ, environment, clear=True):
                result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_active_set_release.example.json"
                )
            environment["SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES"] = "1"
            with patch.dict(os.environ, environment, clear=True):
                insufficient_supply = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_active_set_release.example.json"
                )

        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["scenario_count"], 10)
        self.assertEqual(result["direct_contract"]["protocol"], "completions")
        self.assertEqual(result["direct_contract"]["prompt_format"], "raw")
        self.assertEqual(result["direct_contract"]["keepalive_expiry_s"], 4.0)
        self.assertEqual(
            result["pre_foreground_predicted_work_by_endpoint"],
            {"endpoint-0": 12, "endpoint-1": 12},
        )
        self.assertEqual(insufficient_supply["status"], "failed")
        self.assertTrue(
            any(
                "pre-foreground predicted work" in error
                for error in insufficient_supply["errors"]
            )
        )

    def test_readiness_rejects_token_budget_not_bound_to_evidence(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bulk = root / "bulk.jsonl"
            foreground = root / "foreground.jsonl"
            write_manifest(bulk, (self._request(1, 0), self._request(2, 1)))
            write_manifest(
                foreground,
                (self._request(3, 0), self._request(4, 1)),
            )
            selection = root / "selection.json"
            selection.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "ready",
                        "selection": {
                            "project_static_k_per_endpoint": 128,
                            "project_active_work_per_endpoint": 65536,
                            "project_actor_workers_per_endpoint": 8,
                            "project_ray_actor_max_concurrency": 32,
                            "project_ray_worker_num_cpus": 0.25,
                        },
                        "evidence": {
                            "feeding": {"status": "passed"},
                            "token_budget": {
                                "status": "passed",
                                "frozen_token_budget": 4096,
                            },
                            "actor_pool": {"status": "passed"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            environment = {
                "VLLM_VERSION": "0.25.1",
                "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
                "VLLM_MAX_NUM_SEQS": "256",
                "VLLM_GPU_MEMORY_UTILIZATION": "0.9",
                "STRATEGY_CALIBRATION_SELECTION": str(selection),
                "BEST_TOKEN_BUDGET": "8192",
                "PROJECT_STATIC_K_PER_ENDPOINT": "128",
                "PROJECT_ACTIVE_WORK_PER_ENDPOINT": "65536",
                "PROJECT_ACTOR_WORKERS_PER_ENDPOINT": "8",
                "PROJECT_RAY_ACTOR_MAX_CONCURRENCY": "32",
                "PROJECT_RAY_WORKER_NUM_CPUS": "0.25",
                "PROJECT_SHARED_CREDIT_QUANTUM": "2048",
                "DATABASE_URL": "postgresql://postgres:postgres@localhost/db",
                "SOURCE_MAX_PROMPT_TOKENS": "1500",
                "COMPLETION_ENDPOINT_URLS": (
                    "http://127.0.0.1:8000/v1/completions,"
                    "http://127.0.0.1:8001/v1/completions"
                ),
                "COMPLETION_MODEL": "qwen",
                "COMPLETION_HTTP_KEEPALIVE_EXPIRY_S": "4",
                "COMPLETION_PROTOCOL": "completions",
                "COMPLETION_MAX_TOKENS": "8",
                "MODEL_METRICS_URLS": (
                    "http://127.0.0.1:8000/metrics,"
                    "http://127.0.0.1:8001/metrics"
                ),
                "ENDPOINT_GPU_IDS": "0,1",
                "SAOR_ACTIVE_SET_WORKLOAD": "saor-test",
                "MODEL_PATH": "/models/qwen",
                "REQUEST_SLO_MS": "30000",
                "GPU_PEAK_TFLOPS": "165",
                "MFU_PRECISION": "bf16_dense_fp32_accumulate",
                "SAOR_BULK_ROWS": "2",
                "SAOR_FOREGROUND_ROWS": "2",
                "SAOR_FOREGROUND_OFFSET_S": "5",
                "SAOR_ARRIVAL_TIME_SCALE": "0.001",
                "SAOR_MAX_EFFECTIVE_MANIFEST_SPAN_S": "120",
                "SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES": "0.0001",
                "SAOR_BULK_MANIFEST": str(bulk),
                "SAOR_FOREGROUND_MANIFEST": str(foreground),
            }
            with patch.dict(os.environ, environment, clear=True):
                result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_active_set_release.example.json"
                )

        self.assertEqual(result["status"], "failed")
        self.assertIn(
            "configured token budget does not match calibration evidence",
            result["errors"],
        )

    def test_readiness_rejects_unscaled_multi_hour_manifest(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bulk = root / "bulk.jsonl"
            foreground = root / "foreground.jsonl"
            write_manifest(
                bulk,
                (
                    self._request(1, 0, arrival_time_s=0),
                    self._request(2, 1, arrival_time_s=66_000),
                ),
            )
            write_manifest(
                foreground,
                (
                    self._request(3, 0, arrival_time_s=0),
                    self._request(4, 1, arrival_time_s=66_000),
                ),
            )
            selection = root / "selection.json"
            selection.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "ready",
                        "selection": {
                            "best_token_budget": 8192,
                            "project_static_k_per_endpoint": 128,
                            "project_active_work_per_endpoint": 65536,
                            "project_actor_workers_per_endpoint": 8,
                            "project_ray_actor_max_concurrency": 32,
                            "project_ray_worker_num_cpus": 0.25,
                        },
                        "evidence": {
                            "feeding": {"status": "passed"},
                            "token_budget": {
                                "status": "passed",
                                "frozen_token_budget": 8192,
                            },
                            "actor_pool": {"status": "passed"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            environment = {
                "VLLM_VERSION": "0.25.1",
                "VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
                "VLLM_MAX_NUM_SEQS": "256",
                "VLLM_GPU_MEMORY_UTILIZATION": "0.9",
                "STRATEGY_CALIBRATION_SELECTION": str(selection),
                "BEST_TOKEN_BUDGET": "8192",
                "PROJECT_STATIC_K_PER_ENDPOINT": "128",
                "PROJECT_ACTIVE_WORK_PER_ENDPOINT": "65536",
                "PROJECT_ACTOR_WORKERS_PER_ENDPOINT": "8",
                "PROJECT_RAY_ACTOR_MAX_CONCURRENCY": "32",
                "PROJECT_RAY_WORKER_NUM_CPUS": "0.25",
                "PROJECT_SHARED_CREDIT_QUANTUM": "2048",
                "DATABASE_URL": "postgresql://postgres:postgres@localhost/db",
                "SOURCE_MAX_PROMPT_TOKENS": "1500",
                "COMPLETION_ENDPOINT_URLS": (
                    "http://127.0.0.1:8000/v1/completions,"
                    "http://127.0.0.1:8001/v1/completions"
                ),
                "COMPLETION_MODEL": "qwen",
                "COMPLETION_HTTP_KEEPALIVE_EXPIRY_S": "4",
                "COMPLETION_PROTOCOL": "completions",
                "COMPLETION_MAX_TOKENS": "8",
                "MODEL_METRICS_URLS": (
                    "http://127.0.0.1:8000/metrics,"
                    "http://127.0.0.1:8001/metrics"
                ),
                "ENDPOINT_GPU_IDS": "0,1",
                "SAOR_ACTIVE_SET_WORKLOAD": "saor-test",
                "MODEL_PATH": "/models/qwen",
                "REQUEST_SLO_MS": "30000",
                "GPU_PEAK_TFLOPS": "165",
                "MFU_PRECISION": "bf16_dense_fp32_accumulate",
                "SAOR_BULK_ROWS": "2",
                "SAOR_FOREGROUND_ROWS": "2",
                "SAOR_FOREGROUND_OFFSET_S": "5",
                "SAOR_ARRIVAL_TIME_SCALE": "1",
                "SAOR_MAX_EFFECTIVE_MANIFEST_SPAN_S": "120",
                "SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES": "0.0001",
                "SAOR_BULK_MANIFEST": str(bulk),
                "SAOR_FOREGROUND_MANIFEST": str(foreground),
            }
            with patch.dict(os.environ, environment, clear=True):
                result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_active_set_release.example.json"
                )

        self.assertEqual(result["status"], "failed")
        self.assertTrue(
            any("effective replay span" in item for item in result["errors"])
        )

    def test_summary_fails_closed_when_active_scenario_is_missing(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "matrix"
            output = Path(directory) / "summary"
            root.mkdir()
            (root / "manifest.json").write_text(
                json.dumps({"status": "completed", "incidents": []}),
                encoding="utf-8",
            )
            with (root / "group_runs.csv").open(
                "w", newline="", encoding="utf-8"
            ) as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=("scenario_id", "phase", "repeat_index"),
                )
                writer.writeheader()

            with self.assertRaisesRegex(ValueError, "scenario set"):
                SUMMARY.summarize(root, output)

            validation = json.loads(
                (output / "validation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(validation["status"], "failed")

    @staticmethod
    def _request(
        doc_id: int,
        endpoint_index: int,
        *,
        arrival_time_s: float | None = None,
    ) -> ChatRequest:
        return ChatRequest(
            doc_id=doc_id,
            prompt=f"prompt-{doc_id}",
            arrival_time_s=(
                float(doc_id) if arrival_time_s is None else arrival_time_s
            ),
            prompt_tokens=4,
            max_output_tokens=8,
            estimated_output_tokens=8,
            source_row_hash=f"hash-{doc_id}",
            endpoint_index=endpoint_index,
        )


if __name__ == "__main__":
    unittest.main()
