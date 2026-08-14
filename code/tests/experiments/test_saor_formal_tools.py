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
PRIORITY_SUMMARY = _load(
    "summarize_saor_priority_reachability",
    "code/scripts/analysis/summarize_saor_priority_reachability.py",
)
BOUNDED_SUMMARY = _load(
    "summarize_saor_bounded_priority_gate",
    "code/scripts/analysis/summarize_saor_bounded_priority_gate.py",
)
MATCHED_READY_SUMMARY = _load(
    "summarize_saor_matched_ready_ablation",
    "code/scripts/analysis/summarize_saor_matched_ready_ablation.py",
)
OBSERVATION_BRIDGE_SUMMARY = _load(
    "summarize_saor_ready_observation_bridge",
    "code/scripts/analysis/summarize_saor_ready_observation_bridge.py",
)


class SaorFormalToolsTests(unittest.TestCase):
    def test_ready_observation_bridge_separates_the_two_effects(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrix = root / "round-1"
            self._write_ready_observation_bridge(matrix)

            result = OBSERVATION_BRIDGE_SUMMARY.summarize(
                (matrix,), root / "summary"
            )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["native_baseline_count"], 0)
            self.assertFalse(result["shared_capacity_effect_decided"])
            self.assertFalse(result["ready_observation_effect_decided"])
            self.assertFalse(result["formal_authorized"])
            with (root / "summary/bridge_effects.csv").open(
                encoding="utf-8"
            ) as handle:
                effects = list(csv.DictReader(handle))
            self.assertEqual(
                [row["effect"] for row in effects],
                ["shared_capacity", "bounded_ready_observation"],
            )
            metrics = list(
                csv.DictReader(
                    (root / "summary/bridge_metrics.csv").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
            )
            self.assertEqual(
                metrics[0]["completion_service_lag_status"],
                "unavailable:requires_complete_registered_ready_ledger",
            )
            self.assertEqual(metrics[0]["completion_service_lag_p95_work"], "")

    def test_matched_ready_summary_preserves_internal_ablation_identity(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrix = root / "round-1"
            self._write_matched_ready_matrix(matrix)

            result = MATCHED_READY_SUMMARY.summarize(
                (matrix,), root / "summary"
            )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["native_baseline_count"], 0)
            self.assertEqual(
                result["evaluation_scope"], "single_tenant_multi_job"
            )
            self.assertEqual(
                result["fairness_mode"], "differentiated_service"
            )
            self.assertFalse(result["selector_victory_decided"])
            self.assertFalse(result["formal_authorized"])

    def test_matched_ready_summary_fails_without_completion_fairness(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrix = root / "round-1"
            self._write_matched_ready_matrix(
                matrix,
                include_fairness=False,
            )

            with self.assertRaisesRegex(ValueError, "completion-fairness"):
                MATCHED_READY_SUMMARY.summarize(
                    (matrix,), root / "summary"
                )

            validation = json.loads(
                (root / "summary/validation.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(validation["status"], "failed")

    def test_bounded_gate_uses_lossless_events_not_sampled_snapshots(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrices = (root / "round-1", root / "round-2")
            for matrix in matrices:
                self._write_bounded_matrix(matrix)

            result = BOUNDED_SUMMARY.summarize(matrices, root / "summary")

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["conclusion"], "formal_registration_candidate")
            self.assertTrue((root / "summary/gate_summary.csv").is_file())
            self.assertTrue((root / "summary/mechanism_summary.csv").is_file())

    def test_bounded_ready_gate_has_a_distinct_frozen_profile(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrices = (root / "round-1", root / "round-2")
            for matrix in matrices:
                self._write_bounded_matrix(matrix, ready_policy=True)

            result = BOUNDED_SUMMARY.summarize(
                matrices,
                root / "summary",
                profile="bounded_ready",
            )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["profile"], "bounded_ready")

    def test_bounded_gate_fails_closed_without_event_ledger(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrices = (root / "round-1", root / "round-2")
            for matrix in matrices:
                self._write_bounded_matrix(matrix)
            next((matrices[0] / "traces").glob("*0125k*.release_events.csv")).unlink()

            with self.assertRaisesRegex(ValueError, "event ledger"):
                BOUNDED_SUMMARY.summarize(matrices, root / "summary")

            validation = json.loads(
                (root / "summary/validation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(validation["status"], "failed")

    def test_bounded_gate_fails_closed_on_event_sequence_gap(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            matrices = (root / "round-1", root / "round-2")
            for matrix in matrices:
                self._write_bounded_matrix(matrix)
            event_path = next(
                (matrices[0] / "traces").glob("*0125k*.release_events.csv")
            )
            with event_path.open(encoding="utf-8") as handle:
                events = list(csv.DictReader(handle))
            events[1]["event_seq"] = "3"
            self._write_group_rows(event_path, events)

            with self.assertRaisesRegex(ValueError, "gap, duplicate, or empty"):
                BOUNDED_SUMMARY.summarize(matrices, root / "summary")

            validation = json.loads(
                (root / "summary/validation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(validation["status"], "failed")

    def test_priority_reachability_summary_passes_only_with_audited_action(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "matrix"
            output = Path(directory) / "summary"
            root.mkdir()
            self._write_clean_manifest(root)
            rows = self._priority_matrix_rows(priority_p99_s=30.0)
            self._write_group_rows(root / "group_runs.csv", rows)

            result = PRIORITY_SUMMARY.summarize(root, output)

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["release_only_reachability"], "passed")
            self.assertEqual(result["strict_priority_job_priorities"], [0, 1])
            self.assertTrue((output / "reachability_summary.csv").is_file())

    def test_priority_reachability_summary_fails_closed_on_tail_limit(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "matrix"
            output = Path(directory) / "summary"
            root.mkdir()
            self._write_clean_manifest(root)
            rows = self._priority_matrix_rows(priority_p99_s=31.0)
            self._write_group_rows(root / "group_runs.csv", rows)

            with self.assertRaisesRegex(ValueError, "foreground P99"):
                PRIORITY_SUMMARY.summarize(root, output)

            result = json.loads(
                (output / "validation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["release_only_reachability"], "failed")

    def test_legacy_near_simultaneous_drain_is_reclassified(self) -> None:
        row = {
            "active_set_mechanism_applicable": "True",
            "active_set_mechanism_passed": "False",
            "active_set_mechanism_status": "active_set_mechanism_not_observed",
            "active_set_lifecycle_passed": "True",
            "active_set_overlap_reclaim_observed": "True",
            "active_set_pre_bulk_dominant_share_max": "0.95",
            "active_set_bulk_only_post_samples": "0",
            "active_set_post_fit_violation_samples": "0",
            "arrival_offsets_s": "[0.0, 5.0]",
            "job_jct_s": "[68.743800, 63.737972]",
        }

        passed, status = SUMMARY._effective_mechanism_gate(row)

        self.assertTrue(passed)
        self.assertEqual(
            status,
            "reclassified:post_drain_below_trace_resolution",
        )

    def test_legacy_resolvable_drain_stays_failed(self) -> None:
        row = {
            "active_set_mechanism_applicable": "True",
            "active_set_mechanism_passed": "False",
            "active_set_mechanism_status": "active_set_mechanism_not_observed",
            "active_set_lifecycle_passed": "True",
            "active_set_overlap_reclaim_observed": "True",
            "active_set_pre_bulk_dominant_share_max": "0.95",
            "active_set_bulk_only_post_samples": "0",
            "active_set_post_fit_violation_samples": "0",
            "arrival_offsets_s": "[0.0, 5.0]",
            "job_jct_s": "[70.0, 63.0]",
        }

        passed, _ = SUMMARY._effective_mechanism_gate(row)

        self.assertFalse(passed)

    def test_new_schema_failure_cannot_use_legacy_reclassification(self) -> None:
        row = {
            "active_set_mechanism_applicable": "True",
            "active_set_mechanism_passed": "False",
            "active_set_mechanism_status": "active_set_mechanism_not_observed",
            "active_set_post_drain_applicable": "True",
            "active_set_lifecycle_passed": "True",
            "active_set_overlap_reclaim_observed": "True",
            "active_set_pre_bulk_dominant_share_max": "0.95",
            "active_set_bulk_only_post_samples": "0",
            "active_set_post_fit_violation_samples": "0",
            "arrival_offsets_s": "[0.0, 5.0]",
            "job_jct_s": "[68.743800, 63.737972]",
        }

        passed, _ = SUMMARY._effective_mechanism_gate(row)

        self.assertFalse(passed)

    def test_compact_mechanism_replay_is_explicitly_not_full_validation(
        self,
    ) -> None:
        matrix = (
            REPOSITORY
            / "experiments/results/"
            "saor_active_set_release_formal_20260812_69affc7e"
        )
        with TemporaryDirectory() as directory:
            payload = SUMMARY.replay_compact_mechanism_gate(
                matrix,
                Path(directory),
            )

        self.assertEqual(payload["status"], "passed")
        self.assertFalse(payload["full_formal_validation_updated"])
        self.assertEqual(len(payload["results"]), 12)
        self.assertEqual(
            sum(
                item["effective_mechanism_status"].startswith("reclassified:")
                for item in payload["results"]
            ),
            2,
        )

    def test_default_summary_declares_resolution_aware_full_validation(
        self,
    ) -> None:
        matrix = (
            REPOSITORY
            / "experiments/results/"
            "saor_active_set_release_formal_20260812_69affc7e"
        )
        with TemporaryDirectory() as directory:
            root = Path(directory) / "matrix"
            output = Path(directory) / "summary"
            root.mkdir()
            self._write_clean_manifest(root)
            (root / "group_runs.csv").write_bytes(
                (matrix / "group_runs.csv").read_bytes()
            )

            SUMMARY.summarize(root, output)

            validation = json.loads(
                (output / "validation.json").read_text(encoding="utf-8")
            )

        self.assertEqual(validation["status"], "passed")
        self.assertTrue(validation["full_formal_validation_updated"])
        self.assertEqual(
            validation["mechanism_gate_evaluation"],
            "resolution_aware_v2",
        )
        self.assertEqual(validation["trace_observation_interval_s"], 0.25)
        self.assertEqual(len(validation["mechanism_reclassifications"]), 2)

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

    def test_priority_reachability_template_uses_formal_environment(self) -> None:
        template = (
            REPOSITORY
            / "deploy/autodl/saor_priority_reachability.example.json"
        ).read_text(encoding="utf-8")
        env_example = (
            REPOSITORY / "deploy/autodl/saor_active_set_formal.env.example"
        ).read_text(encoding="utf-8")
        required = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", template))
        provided = set(
            re.findall(r"^export ([A-Z][A-Z0-9_]*)=", env_example, re.MULTILINE)
        )
        decoded = json.loads(template)

        self.assertEqual(required - provided, {"DATABASE_URL"})
        self.assertEqual(
            [item["policy"] for item in decoded["scenarios"]],
            [
                "static_partition",
                "saor_release",
                "foreground_strict_priority",
            ],
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
                service_signature=(("model", "qwen"), ("service", "vllm-test")),
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
                "SAOR_READY_PAYLOAD_BYTES_LIMIT_PER_JOB": "67108864",
                "SAOR_BULK_MANIFEST": str(bulk),
                "SAOR_FOREGROUND_MANIFEST": str(foreground),
            }
            with patch.dict(os.environ, environment, clear=True):
                result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_active_set_release.example.json"
                )
                priority_result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_priority_reachability.example.json",
                    profile="priority_reachability",
                )
                bounded_result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_bounded_priority.example.json",
                    profile="bounded_priority_development",
                )
                bounded_ready_result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_bounded_ready.example.json",
                    profile="bounded_ready_development",
                )
                matched_ready_result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_matched_ready_selector_ablation.example.json",
                    profile="matched_ready_selector_ablation",
                )
                observation_bridge_result = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_ready_observation_bridge.example.json",
                    profile="ready_observation_bridge",
                )
                drift_config = root / "matched-ready-drift.json"
                drift_payload = json.loads(
                    (
                        REPOSITORY
                        / "deploy/autodl/saor_matched_ready_selector_ablation.example.json"
                    ).read_text(encoding="utf-8")
                )
                drift_payload["scenarios"][1][
                    "request_limit_per_endpoint"
                ] = 64
                drift_payload["scenarios"][1][
                    "work_limit_per_endpoint"
                ] = 32768
                drift_payload["scenarios"][2]["weights"] = [1, 2]
                drift_config.write_text(
                    json.dumps(drift_payload),
                    encoding="utf-8",
                )
                drift_result = AUDIT.audit(
                    drift_config,
                    profile="matched_ready_selector_ablation",
                )
            environment["SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES"] = "1"
            with patch.dict(os.environ, environment, clear=True):
                insufficient_supply = AUDIT.audit(
                    REPOSITORY
                    / "deploy/autodl/saor_active_set_release.example.json"
                )

        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["scenario_count"], 10)
        self.assertEqual(priority_result["status"], "passed")
        self.assertEqual(priority_result["scenario_count"], 3)
        self.assertEqual(bounded_result["status"], "passed")
        self.assertEqual(bounded_result["scenario_count"], 4)
        self.assertEqual(bounded_ready_result["status"], "passed")
        self.assertEqual(bounded_ready_result["scenario_count"], 4)
        self.assertEqual(matched_ready_result["status"], "passed")
        self.assertEqual(matched_ready_result["scenario_count"], 6)
        self.assertEqual(observation_bridge_result["status"], "passed")
        self.assertEqual(observation_bridge_result["scenario_count"], 3)
        self.assertEqual(drift_result["status"], "failed")
        self.assertTrue(
            any(
                "request/work limits" in error
                for error in drift_result["errors"]
            )
        )
        self.assertTrue(
            any("weights drift" in error for error in drift_result["errors"])
        )
        self.assertIsNone(priority_result["direct_contract"])
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

    @staticmethod
    def _write_group_rows(path: Path, rows: list[dict[str, object]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _write_clean_manifest(root: Path) -> None:
        (root / "manifest.json").write_text(
            json.dumps({"status": "completed", "incidents": []}),
            encoding="utf-8",
        )

    @staticmethod
    def _priority_matrix_rows(
        *,
        priority_p99_s: float,
    ) -> list[dict[str, object]]:
        scenarios = (
            ("active_set_static_partition", "static_partition", 29.2, 0.0),
            ("active_set_saor_release", "saor_release", 50.3, 0.831),
            (
                "active_set_foreground_strict_priority",
                "foreground_strict_priority",
                priority_p99_s,
                0.005,
            ),
        )
        rows: list[dict[str, object]] = []
        for scenario_id, policy, foreground_p99, foreground_slo in scenarios:
            phases = (("warmup", 0), *(("formal", i) for i in range(1, 4)))
            for phase, repeat_index in phases:
                priorities = (
                    [0, 1]
                    if policy == "foreground_strict_priority"
                    else [0, 0]
                )
                credit_policy = policy in {
                    "saor_release",
                    "foreground_strict_priority",
                }
                rows.append(
                    {
                        "scenario_id": scenario_id,
                        "policy": policy,
                        "phase": phase,
                        "repeat_index": repeat_index,
                        "incidents": 0,
                        "metrics_status": "ok",
                        "resource_metrics_status": "ok",
                        "actor_worker_failures": 0,
                        "active_set_lifecycle_passed": True,
                        "active_set_mechanism_applicable": credit_policy,
                        "active_set_mechanism_passed": credit_policy,
                        "job_priorities": json.dumps(priorities),
                        "job_arrived_rows": "[512, 512]",
                        "job_completed_rows": "[512, 512]",
                        "job_failed_rows": "[0, 0]",
                        "job_p99_s": json.dumps([60.0, foreground_p99]),
                        "job_slo_violation_ratio": json.dumps(
                            [0.5, foreground_slo]
                        ),
                        "job_jct_s": json.dumps([70.0, 40.0]),
                        "tokens_per_s": 10_100.0,
                    }
                )
        return rows

    @staticmethod
    def _write_matched_ready_matrix(
        root: Path,
        *,
        include_fairness: bool = True,
    ) -> None:
        root.mkdir(parents=True)
        if include_fairness:
            (root / "jobs").mkdir()
        (root / "manifest.json").write_text(
            json.dumps(
                {
                    "status": "completed",
                    "execution_mode": "rehearsal",
                    "incidents": [],
                    "config_fingerprint": "same-config",
                    "repository_commit": "same-commit",
                    "redacted_config": {
                        "service_metadata": {
                            "vllm_version": "test",
                            "scheduling_policy": "fcfs",
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        scenarios = tuple(MATCHED_READY_SUMMARY.EXPECTED.items())
        rows = []
        for order_index, (
            scenario_id,
            (policy, identity, observation),
        ) in enumerate(scenarios):
            bounded = observation == "bounded_concrete_pre_registration"
            proposed = policy == "saor_bounded_ready"
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "policy": policy,
                    "experiment_identity": identity,
                    "ready_observation_contract": observation,
                    "phase": "warmup",
                    "repeat_index": 1,
                    "order_index": order_index,
                    "execution_mode": "rehearsal",
                    "incidents": 0,
                    "actor_worker_failures": 0,
                    "metrics_status": "ok",
                    "resource_metrics_status": "ok",
                    "active_set_lifecycle_passed": True,
                    "job_arrived_rows": "[512, 512]",
                    "job_completed_rows": "[512, 512]",
                    "job_failed_rows": "[0, 0]",
                    "job_p99_s": "[60, 20]",
                    "job_slo_violation_ratio": "[0.6, 0]",
                    "job_jct_s": "[70, 30]",
                    "tokens_per_s": 10000,
                    "duration_s": 70,
                    "mfu_estimate": 0.4,
                    "jain_fairness": 0.9,
                    "max_overlap_normalized_service_disparity": 100,
                    "bounded_ready_event_status": (
                        "ok:actor_event_join" if bounded else "not_applicable"
                    ),
                    "bounded_ready_lifecycle_complete": bounded,
                    "bounded_ready_jobs_with_intervals": 2 if bounded else 0,
                    "bounded_ready_intervals": 1024 if bounded else 0,
                    "bounded_ready_max_ready_requests_seen": 128 if bounded else 0,
                    "bounded_ready_max_ready_work_seen": 65536 if bounded else 0,
                    "bounded_ready_max_ready_payload_bytes_seen": 1024 if bounded else 0,
                    "bounded_ready_foreign_fallback_events": 0,
                    "bounded_saor_event_status": (
                        "ok:lossless_ledger" if proposed else "unavailable"
                    ),
                    "bounded_saor_event_sequence_complete": proposed,
                    "bounded_saor_slo_priority_grants": 1 if proposed else 0,
                    "bounded_saor_debt_recovery_grants": 1 if proposed else 0,
                    "bounded_saor_avoidable_idle_events": 0,
                    "bounded_saor_foreign_grant_over_debt_critical_events": 0,
                    "bounded_saor_recovery_inflight_max": 1 if proposed else 0,
                }
            )
            if include_fairness:
                for job_index in range(2):
                    stem = (
                        f"{order_index:03d}_warmup_1_{scenario_id}_"
                        f"job{job_index}"
                    )
                    submission_id = f"{scenario_id}-job{job_index}-r0"
                    SaorFormalToolsTests._write_group_rows(
                        root / "jobs" / f"{stem}.requests.csv",
                        [
                            {
                                "submission_id": submission_id,
                                "completion_epoch_s": 2.0 + job_index,
                                "prompt_tokens": 10,
                                "actual_output_tokens": 10,
                                "client_estimated_output_tokens": 10,
                                "estimated_output_tokens": 10,
                            }
                        ],
                    )
                    SaorFormalToolsTests._write_group_rows(
                        root / "jobs" / f"{stem}.submissions.csv",
                        [
                            {
                                "submission_id": submission_id,
                                "ready_epoch_s": 0.0,
                                "credit_registered_epoch_s": 0.1,
                                "credit_granted_epoch_s": 0.2,
                            }
                        ],
                    )
        SaorFormalToolsTests._write_group_rows(root / "group_runs.csv", rows)

    @staticmethod
    def _write_ready_observation_bridge(root: Path) -> None:
        root.mkdir(parents=True)
        (root / "manifest.json").write_text(
            json.dumps(
                {
                    "status": "completed",
                    "execution_mode": "rehearsal",
                    "incidents": [],
                    "config_fingerprint": "same-bridge-config",
                    "repository_commit": "same-bridge-commit",
                    "redacted_config": {
                        "service_metadata": {
                            "vllm_version": "test",
                            "scheduling_policy": "fcfs",
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        rows = []
        for index, (scenario_id, expected) in enumerate(
            OBSERVATION_BRIDGE_SUMMARY.EXPECTED.items()
        ):
            policy, identity, observation = expected
            bounded = observation == "bounded_concrete_pre_registration"
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "policy": policy,
                    "experiment_identity": identity,
                    "ready_observation_contract": observation,
                    "phase": "warmup",
                    "execution_mode": "rehearsal",
                    "incidents": 0,
                    "actor_worker_failures": 0,
                    "metrics_status": "ok",
                    "resource_metrics_status": "ok",
                    "active_set_lifecycle_passed": True,
                    "job_arrived_rows": "[512, 512]",
                    "job_completed_rows": "[512, 512]",
                    "job_failed_rows": "[0, 0]",
                    "job_jct_s": json.dumps([90 - 10 * index, 40 + index]),
                    "job_p99_s": json.dumps([80 - 10 * index, 30 + index]),
                    "job_slo_violation_ratio": "[0.6, 0]",
                    "tokens_per_s": 9000 + 1000 * index,
                    "duration_s": 92 - 10 * index,
                    "mfu_estimate": 0.35 + 0.04 * index,
                    "jain_fairness": 0.9,
                    "bounded_ready_event_status": (
                        "ok:actor_event_join" if bounded else "not_applicable"
                    ),
                    "bounded_ready_lifecycle_complete": bounded,
                    "bounded_ready_jobs_with_intervals": 2 if bounded else 0,
                    "bounded_ready_intervals": 1024 if bounded else 0,
                    "bounded_ready_max_ready_requests_seen": 128 if bounded else 0,
                    "bounded_ready_max_ready_work_seen": 65536 if bounded else 0,
                    "bounded_ready_max_ready_payload_bytes_seen": (
                        1024 if bounded else 0
                    ),
                }
            )
        SaorFormalToolsTests._write_group_rows(root / "group_runs.csv", rows)

    @staticmethod
    def _write_bounded_matrix(
        root: Path,
        *,
        ready_policy: bool = False,
    ) -> None:
        (root / "traces").mkdir(parents=True)
        (root / "manifest.json").write_text(
            json.dumps(
                {
                    "status": "completed",
                    "execution_mode": "rehearsal",
                    "incidents": [],
                    "config_fingerprint": "same-config",
                    "repository_commit": "same-commit",
                    "redacted_config": {
                        "service_metadata": {"vllm_version": "test", "scheduling_policy": "fcfs"}
                    },
                }
            ),
            encoding="utf-8",
        )
        bounded_policy = (
            "saor_bounded_ready"
            if ready_policy
            else "saor_bounded_priority"
        )
        bounded_stem = (
            "active_set_saor_bounded_ready"
            if ready_policy
            else "active_set_saor_bounded_priority"
        )
        scenarios = (
            ("active_set_static_partition", "static_partition"),
            ("active_set_saor_release", "saor_release"),
            (f"{bounded_stem}_0125k", bounded_policy),
            (f"{bounded_stem}_025k", bounded_policy),
        )
        rows = []
        for scenario_id, policy in scenarios:
            event_path = Path("traces") / f"000_warmup_0_{scenario_id}.release_events.csv"
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "policy": policy,
                    "phase": "warmup",
                    "repeat_index": 0,
                    "execution_mode": "rehearsal",
                    "incidents": 0,
                    "metrics_status": "ok",
                    "resource_metrics_status": "ok",
                    "actor_worker_failures": 0,
                    "active_set_lifecycle_passed": True,
                    "job_arrived_rows": "[512, 512]",
                    "job_completed_rows": "[512, 512]",
                    "job_failed_rows": "[0, 0]",
                    "job_p99_s": "[60.0, 30.0]",
                    "job_slo_violation_ratio": "[0.70, 0.0]",
                    "tokens_per_s": 10_100.0,
                    "release_event_trace_path": str(event_path),
                    "bounded_ready_event_status": (
                        "ok:actor_event_join"
                        if ready_policy
                        else "not_applicable"
                    ),
                    "bounded_ready_lifecycle_complete": ready_policy,
                    "bounded_ready_foreground_intervals": (
                        2 if ready_policy else 0
                    ),
                    "bounded_ready_foreign_fallback_events": 0,
                    "bounded_ready_foreground_max_ready_requests_seen": (
                        2 if ready_policy else 0
                    ),
                    "bounded_ready_foreground_max_ready_work_seen": (
                        100 if ready_policy else 0
                    ),
                }
            )
            if policy in {"saor_bounded_priority", "saor_bounded_ready"}:
                with (root / event_path).open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=(
                            "event_seq",
                            "event_time_s",
                            "endpoint_id",
                            "action",
                            "tier",
                            "recovery_inflight_by_job",
                            "constraint_conflict",
                            "avoidable_idle",
                            "foreign_grant_over_debt_critical",
                        ),
                    )
                    writer.writeheader()
                    writer.writerows(
                        (
                            {
                                "event_seq": 1,
                                "event_time_s": 10.0,
                                "endpoint_id": "endpoint-0",
                                "action": "grant",
                                "tier": "slo_priority",
                                "recovery_inflight_by_job": "[]",
                                "constraint_conflict": "False",
                                "avoidable_idle": "False",
                                "foreign_grant_over_debt_critical": "False",
                            },
                            {
                                "event_seq": 2,
                                "event_time_s": 10.005,
                                "endpoint_id": "endpoint-0",
                                "action": "grant",
                                "tier": "debt_recovery",
                                "recovery_inflight_by_job": '[["bulk", "r1"]]',
                                "constraint_conflict": "False",
                                "avoidable_idle": "False",
                                "foreign_grant_over_debt_critical": "False",
                            },
                        )
                    )
        SaorFormalToolsTests._write_group_rows(root / "group_runs.csv", rows)


if __name__ == "__main__":
    unittest.main()
