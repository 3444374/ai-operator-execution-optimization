"""Contract tests for the eight-arm SAOR matched-system readiness audit."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from src.baselines.common.contracts import ChatRequest
from src.baselines.common.manifests import write_manifest
from src.baselines.text.orchestration.native_multijob import load_native_multijob_config
from src.experiments.shared_vllm.config import load_config as load_project_config
from src.experiments.saor.native_system_matched import (
    REQUIRED_ARM_IDS,
    SELECTOR_SANITY_ARM_IDS,
    SYSTEM_ARM_IDS,
    _validate_actual_job_offset,
    audit_matched_system_config,
    balanced_matched_schedule,
    load_matched_system_config,
    run_matched_system,
)
from scripts.experiments.run_saor_native_system_matched import (
    _normalize_project,
    _canonical_config_path,
    _validate_executor_bindings,
    parse_args,
)


class MatchedSystemContractTest(unittest.TestCase):
    def test_calibration_paths_are_canonicalized_relative_to_each_config(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            matched_config = root / "matched" / "config.json"
            project_config = root / "project" / "config.json"
            expected = root / "calibration.json"
            self.assertEqual(
                _canonical_config_path("../calibration.json", matched_config),
                str(expected.resolve()),
            )
            self.assertEqual(
                _canonical_config_path("../calibration.json", project_config),
                str(expected.resolve()),
            )

    def test_representative_legacy_configs_load_without_matrix_bindings(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            job0 = root / "short.jsonl"
            job1 = root / "long.jsonl"
            requests = tuple(
                ChatRequest(
                    doc_id=index, prompt=f"p-{index}", arrival_time_s=0.0,
                    prompt_tokens=10, max_output_tokens=256,
                    estimated_output_tokens=256, source_row_hash=f"row-{index}",
                    endpoint_index=(index - 1) % 2,
                )
                for index in range(1, 5)
            )
            write_manifest(job0, requests[:2])
            write_manifest(job1, requests[2:])
            calibration = root / "calibration.json"
            calibration.write_text(json.dumps({
                "schema_version": 1, "status": "ready",
                "selection": {
                    "best_token_budget": 8192,
                    "project_static_k_per_endpoint": 8,
                    "project_active_work_per_endpoint": 65536,
                    "project_actor_workers_per_endpoint": 1,
                    "project_ray_actor_max_concurrency": 256,
                    "project_ray_worker_num_cpus": 0.25,
                },
                "evidence": {
                    "feeding": {"status": "passed"},
                    "token_budget": {"status": "passed"},
                    "actor_pool": {"status": "passed"},
                },
            }), encoding="utf-8")
            environment = {
                "BEST_TOKEN_BUDGET": "8192", "COMPLETION_MAX_TOKENS": "256",
                "COMPLETION_MODEL": "test", "COMPLETION_PROMPT_FORMAT": "raw",
                "DATABASE_URL": "postgresql://localhost/test",
                "GPU_PEAK_TFLOPS": "165", "MFU_PRECISION": "bf16",
                "COMPLETION_ENDPOINT_URLS": "http://localhost:8000/v1/completions,http://localhost:8001/v1/completions",
                "MODEL_METRICS_URLS": "http://localhost:8000/metrics,http://localhost:8001/metrics",
                "PROJECT_ACTIVE_WORK_PER_ENDPOINT": "65536",
                "PROJECT_ACTOR_WORKERS_PER_ENDPOINT": "1",
                "PROJECT_RAY_ACTOR_MAX_CONCURRENCY": "256",
                "PROJECT_RAY_WORKER_NUM_CPUS": "0.25",
                "PROJECT_STATIC_K_PER_ENDPOINT": "8",
                "SOURCE_MAX_PROMPT_TOKENS": "1500",
                "SOURCE_WORKLOAD_NAME": "test",
                "STRATEGY_CALIBRATION_SELECTION": str(calibration),
                "VLLM_MAX_NUM_BATCHED_TOKENS": "8192", "VLLM_MAX_NUM_SEQS": "256",
                "COMPLETION_CHAT_ENDPOINT_URL_0": "http://localhost:8000/v1/chat/completions",
                "COMPLETION_CHAT_ENDPOINT_URL_1": "http://localhost:8001/v1/chat/completions",
                "RAY_ADDRESS": "ray://localhost:10001", "RAY_DATA_BATCH_SIZE": "1",
                "RAY_DATA_CONCURRENCY_PER_ENDPOINT": "1",
                "TEXT_BASELINES_PYTHON": sys.executable,
                "TEXT_NATIVE_SHORT_JOB_MANIFEST": str(job0),
                "TEXT_NATIVE_LONG_JOB_MANIFEST": str(job1),
                "TEXT_NATIVE_MULTIJOB_OFFSET_S": "5",
                "TEXT_NATIVE_MULTIJOB_OUTPUT_ROOT": str(root / "native-output"),
            }
            with patch.dict(os.environ, environment, clear=True):
                shared = load_project_config(
                    repository / "deploy/autodl/dual_gpu_shared_vllm_formal.example.json"
                )
                native = load_native_multijob_config(
                    repository / "deploy/autodl/opening_text_native_multijob.example.json"
                )

        self.assertEqual(shared.service_signature, ())
        self.assertEqual(native.service_signature, ())
        self.assertEqual(native.endpoint_ids, ())

    def test_actual_child_offset_uses_preregistered_quarter_second_tolerance(self) -> None:
        for actual in (4.75, 5.0, 5.25):
            with self.subTest(actual=actual):
                self.assertEqual(_validate_actual_job_offset(actual), actual - 5.0)
        for actual in (4.5, 5.5):
            with self.subTest(actual=actual), self.assertRaisesRegex(
                RuntimeError, "actual child-source offset"
            ):
                _validate_actual_job_offset(actual)

    def test_checked_in_cli_config_trio_loads_and_maps_exact_ids(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        matched_path = repository / "deploy/autodl/saor_native_system_matched.example.json"
        native_path = repository / "deploy/autodl/saor_native_system_matched_native.example.json"
        project_path = repository / "deploy/autodl/saor_native_system_matched_project.example.json"
        self.assertTrue(native_path.is_file())
        self.assertTrue(project_path.is_file())

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            job0 = root / "job0.jsonl"
            job1 = root / "job1.jsonl"
            combined = root / "combined.jsonl"
            requests = tuple(
                ChatRequest(
                    doc_id=index, prompt=f"p-{index}", arrival_time_s=0.0,
                    prompt_tokens=10, max_output_tokens=256,
                    estimated_output_tokens=256, source_row_hash=f"row-{index}",
                    endpoint_index=(index - 1) % 2,
                )
                for index in range(1, 5)
            )
            write_manifest(job0, requests[:2])
            write_manifest(job1, requests[2:])
            write_manifest(combined, requests)
            calibration = root / "calibration.json"
            calibration.write_text(json.dumps({
                "schema_version": 1, "status": "ready",
                "selection": {
                    "project_static_k_per_endpoint": 8,
                    "project_active_work_per_endpoint": 65536,
                    "project_actor_workers_per_endpoint": 1,
                    "project_ray_actor_max_concurrency": 256,
                },
                "evidence": {
                    "feeding": {"status": "passed"},
                    "token_budget": {"status": "passed"},
                    "actor_pool": {"status": "passed"},
                },
            }), encoding="utf-8")
            raw_matched = json.loads(matched_path.read_text(encoding="utf-8"))
            raw_matched["matched_manifest_status"] = "ready_frozen"
            digest = hashlib.sha256(combined.read_bytes()).hexdigest()
            for index, arm in enumerate(raw_matched["arms"]):
                arm["manifest_path"] = str(combined)
                arm["manifest_sha256"] = digest
                arm["output_root"] = str(root / f"matched-output-{index}")
                arm["calibration_path"] = str(calibration)
            resolved_matched = root / "matched.json"
            resolved_matched.write_text(json.dumps(raw_matched), encoding="utf-8")
            environment = {
                "SAOR_MATCHED_JOB0_MANIFEST": str(job0),
                "SAOR_MATCHED_JOB1_MANIFEST": str(job1),
                "SAOR_MATRIX_CALIBRATION": str(calibration),
                "SAOR_MATRIX_OUTPUT_ROOT": str(root / "matrix-output"),
                "DATABASE_URL": "postgresql://localhost/test",
                "SAOR_MATCHED_WORKLOAD": "test", "SAOR_MATCHED_ROW_OFFSET": "0",
                "COMPLETION_MODEL": "test", "VLLM_VERSION": "vllm-test",
                "COMPLETION_PROTOCOL": "chat_completions", "COMPLETION_MAX_TOKENS": "256",
                "SAOR_ORGANIZER": "daft", "TEXT_BASELINES_PYTHON": sys.executable,
                "COMPLETION_CHAT_ENDPOINT_URL_0": "http://localhost:8000/v1/chat/completions",
                "COMPLETION_CHAT_ENDPOINT_URL_1": "http://localhost:8001/v1/chat/completions",
                "RAY_ADDRESS": "ray://localhost:10001",
                "PROJECT_STATIC_K_PER_ENDPOINT": "8",
                "PROJECT_ACTIVE_WORK_PER_ENDPOINT": "65536",
                "PROJECT_READY_BYTES": "4096",
                "PROJECT_ACTOR_WORKERS_PER_ENDPOINT": "1",
                "PROJECT_RAY_ACTOR_MAX_CONCURRENCY": "256",
            }
            with patch.dict(os.environ, environment, clear=True):
                matched = load_matched_system_config(resolved_matched)
                native = load_native_multijob_config(native_path)
                project = load_project_config(project_path)
                _validate_executor_bindings(matched, native, project)

        self.assertEqual({arm.arm_id for arm in native.arms}, set(SYSTEM_ARM_IDS[:3]))
        self.assertEqual(
            {scenario.scenario_id for scenario in project.scenarios},
            set(REQUIRED_ARM_IDS[3:]),
        )

    def test_project_normalization_uses_child_arrival_and_completion_times(self) -> None:
        with self._config() as path:
            arm = next(
                item
                for item in load_matched_system_config(path).arms
                if item.arm_id == "project_frozen_static"
            )
            output_dir = path.parent / "project-cell"
            (output_dir / "jobs").mkdir(parents=True)
            (output_dir / "traces").mkdir()
            for index, total_rows in enumerate((2, 1)):
                (output_dir / "jobs" / f"job{index}.runs.csv").write_text(
                    "total_rows,request_manifest_validation_status,db_fetch_s\n"
                    f"{total_rows},ok,0.1\n",
                    encoding="utf-8",
                )
            (output_dir / "traces" / "cell.commands.json").write_text(
                json.dumps({"commands": [["runner"], ["runner"]]}),
                encoding="utf-8",
            )
            (output_dir / "traces" / "cell.resources.csv").write_text(
                "observed_epoch_s,gpu_utilization_pct\n100.5,80\n",
                encoding="utf-8",
            )
            record = {
                "replay_configured_start_epoch_s": "[90.0, 95.0]",
                "replay_observed_start_epoch_s": "[90.0, 95.0]",
                "job_actual_work": "[10, 20]",
                "job_completed_count": "[2, 1]",
                "job_expected_count": "[2, 2]",
                "job_exactly_once": "[true, false]",
                "job_jct_s": "[1.0, 1.0]",
                "job_arrival_start_epoch_s": "[100.0, 105.0]",
                "job_completion_end_epoch_s": "[110.0, 108.0]",
                "start_epoch_s": 90.0,
                "end_epoch_s": 111.0,
                "metrics_status": "ok",
                "prompt_tokens_delta": 10,
                "generation_tokens_delta": 20,
                "resource_metrics_status": "ok",
            }

            normalized = _normalize_project(arm, record, output_dir)

            self.assertEqual(
                [job["actual_launch_epoch_s"] for job in normalized["jobs"]],
                [100.0, 105.0],
            )
            self.assertEqual(
                [job["ended_epoch_s"] for job in normalized["jobs"]],
                [110.0, 108.0],
            )
            self.assertEqual(
                [job["completed_count"] for job in normalized["jobs"]],
                [2, 1],
            )
            self.assertEqual(
                [job["exactly_once"] for job in normalized["jobs"]],
                [True, False],
            )
            self.assertFalse(normalized["exactly_once"])

    def test_executor_bindings_fail_closed_on_actual_config_drift(self) -> None:
        with self._config() as path:
            matched = load_matched_system_config(path)
            combined = Path(matched.arms[0].manifest_path)
            requests = tuple(
                ChatRequest(**json.loads(line))
                for line in combined.read_text(encoding="utf-8").splitlines()
            )
            job_paths = (path.parent / "job0.jsonl", path.parent / "job1.jsonl")
            write_manifest(job_paths[0], requests[:2])
            write_manifest(job_paths[1], requests[2:])
            source = SimpleNamespace(
                database_url="postgresql://localhost/test",
                workload_name="test",
            )
            native = SimpleNamespace(
                endpoint_ids=("endpoint-0", "endpoint-1"),
                service_signature=(("model", "test"), ("service", "vllm")),
                protocol="completions", output_cap=256,
                job_internal_arrival_contract="eager", source=source,
                organizer="daft",
                arms=tuple(
                    SimpleNamespace(
                        arm_id=arm_id,
                        jobs=tuple(
                            SimpleNamespace(manifest=job_paths[index], offset_s=float(index * 5))
                            for index in range(2)
                        ),
                    )
                    for arm_id in ("daft_native", "daft_ray", "ray_data_http")
                ),
            )
            scenarios = tuple(
                SimpleNamespace(
                    scenario_id=arm.arm_id,
                    request_manifests=tuple(str(item) for item in job_paths),
                    source_row_offsets=(0, 0),
                    arrival_offsets_s=(0.0, 5.0), policy=arm.project_value("policy"),
                    debt_cap_fractions=(
                        arm.project_value("debt_caps") or ()
                    ),
                    ready_observation_contract=(
                        arm.project_value("ready_observation") or "single_head"
                    ),
                    request_limit_per_endpoint=arm.project_value("k_per_endpoint"),
                    work_limit_per_endpoint=arm.project_value("work_limit_per_endpoint"),
                    endpoint_limits=lambda request, work: (request, work),
                )
                for arm in matched.arms if arm.kind == "project"
            )
            project = SimpleNamespace(
                endpoint_ids=("endpoint-0", "endpoint-1"),
                service_signature=(("model", "test"), ("service", "vllm")),
                job_internal_arrival_contract="eager", scenarios=scenarios,
                request_limit_per_endpoint=8, work_limit_per_endpoint=65536,
                ready_payload_bytes_limit_per_job=4096,
                common_args=(
                    "--database-url", "postgresql://localhost/test",
                    "--source-workload-name", "test", "--completion-protocol",
                    "completions", "--completion-max-tokens", "256",
                    "--organizer", "daft", "--actor-workers-per-endpoint", "1",
                    "--ray-actor-max-concurrency", "256",
                ),
                calibration_contract=SimpleNamespace(path="project-calibration.json"),
            )

            _validate_executor_bindings(matched, native, project)
            native.service_signature = ()
            with self.assertRaisesRegex(ValueError, "service_signature"):
                _validate_executor_bindings(matched, native, project)
            native.service_signature = (("model", "test"), ("service", "other"))
            with self.assertRaisesRegex(ValueError, "service_signature"):
                _validate_executor_bindings(matched, native, project)
            native.service_signature = (("model", "test"), ("service", "vllm"))
            project.service_signature = ()
            with self.assertRaisesRegex(ValueError, "service_signature"):
                _validate_executor_bindings(matched, native, project)
            project.service_signature = (("model", "test"), ("service", "vllm"))
            native.source = None
            with self.assertRaisesRegex(ValueError, "explicit source"):
                _validate_executor_bindings(matched, native, project)
            native.source = source
            saor = next(
                item
                for item in project.scenarios
                if item.scenario_id == "project_bounded_ready_saor_0125we"
            )
            saor.debt_cap_fractions = (0.2, None)
            with self.assertRaisesRegex(ValueError, "debt_caps"):
                _validate_executor_bindings(matched, native, project)
            saor.debt_cap_fractions = (0.125, None)
            saor.source_row_offsets = (1, 0)
            with self.assertRaisesRegex(ValueError, "source_row_offsets"):
                _validate_executor_bindings(matched, native, project)

    def test_arm_identity_has_eight_unique_physical_arms_and_shared_saor(self) -> None:
        self.assertEqual(
            SYSTEM_ARM_IDS,
            (
                "daft_native", "daft_ray", "ray_data_http",
                "project_frozen_static", "project_bounded_ready_saor_0125we",
            ),
        )
        self.assertEqual(
            SELECTOR_SANITY_ARM_IDS,
            (
                "project_bounded_ready_fifo", "project_bounded_ready_drr",
                "project_bounded_ready_vtc_style",
                "project_bounded_ready_saor_0125we",
            ),
        )
        self.assertEqual(len(REQUIRED_ARM_IDS), 8)
        self.assertEqual(len(set(REQUIRED_ARM_IDS)), 8)
        self.assertIn("project_bounded_ready_saor_0125we", SYSTEM_ARM_IDS)
        self.assertIn("project_bounded_ready_saor_0125we", SELECTOR_SANITY_ARM_IDS)

    def test_two_tables_schedule_one_shared_saor_cell_per_repeat(self) -> None:
        with self._config() as path:
            config = load_matched_system_config(path)
        schedule = balanced_matched_schedule(config, phase="formal", repeat=1)
        self.assertEqual({cell.arm_id for cell in schedule}, set(REQUIRED_ARM_IDS))
        self.assertEqual(
            sum(cell.arm_id == "project_bounded_ready_saor_0125we" for cell in schedule),
            1,
        )
        self.assertNotEqual(
            tuple(cell.arm_id for cell in schedule),
            tuple(cell.arm_id for cell in balanced_matched_schedule(config, phase="formal", repeat=2)),
        )

    def test_audit_emits_readiness_evidence_without_execution(self) -> None:
        with self._config() as path:
            report = audit_matched_system_config(load_matched_system_config(path))
        self.assertEqual(report["status"], "passed")
        self.assertFalse(report["gpu_formal_locally_authorized"])
        self.assertEqual(len(report["immutable_manifest_hashes"]), 8)
        self.assertEqual(len(report["planned_schedule"]), 6)

    def test_example_uses_a_literal_tracked_manifest_path_without_expansion(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        example_path = repository / "deploy/autodl/saor_native_system_matched.example.json"
        example = json.loads(example_path.read_text(encoding="utf-8"))
        paths = {arm["manifest_path"] for arm in example["arms"]}
        self.assertEqual(len(paths), 1)
        manifest_path = paths.pop()
        self.assertNotIn("${", manifest_path)
        self.assertTrue((example_path.parent / manifest_path).resolve().is_file())

    def test_example_cannot_pass_when_environment_and_sha_are_supplied(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        example_path = repository / "deploy/autodl/saor_native_system_matched.example.json"
        example = json.loads(example_path.read_text(encoding="utf-8"))
        manifest = (example_path.parent / example["arms"][0]["manifest_path"]).resolve()
        environment = {
            name: "value"
            for name in {
                value.removeprefix("${").removesuffix("}")
                for arm in example["arms"]
                for value in self._walk_values(arm)
                if isinstance(value, str) and value.startswith("${") and value.endswith("}")
            }
        }
        environment["DATABASE_URL"] = "postgresql://localhost/test"
        environment["SAOR_MATCHED_MANIFEST_SHA256"] = hashlib.sha256(manifest.read_bytes()).hexdigest()
        environment["COMPLETION_MODEL"] = "test"
        environment["VLLM_VERSION"] = "test"
        environment["COMPLETION_PROTOCOL"] = "chat_completions"
        environment["COMPLETION_MAX_TOKENS"] = "256"
        environment["SAOR_MATCHED_ROW_OFFSET"] = "0"
        environment["SAOR_ORGANIZER"] = "daft"
        environment["PROJECT_STATIC_K_PER_ENDPOINT"] = "8"
        environment["PROJECT_ACTIVE_WORK_PER_ENDPOINT"] = "65536"
        environment["PROJECT_READY_BYTES"] = "4096"
        environment["PROJECT_ACTOR_WORKERS_PER_ENDPOINT"] = "1"
        environment["PROJECT_RAY_ACTOR_MAX_CONCURRENCY"] = "256"
        with TemporaryDirectory() as temporary:
            for index, name in enumerate(
                sorted(key for key in environment if key.endswith("OUTPUT_ROOT"))
            ):
                environment[name] = str(Path(temporary) / f"output-{index}")
            with patch.dict(os.environ, environment, clear=True):
                with self.assertRaisesRegex(
                    ValueError,
                    "matched_manifest_status must be ready_frozen",
                ):
                    load_matched_system_config(example_path)

    @staticmethod
    def _walk_values(value: object) -> list[object]:
        if isinstance(value, dict):
            return [item for child in value.values() for item in MatchedSystemContractTest._walk_values(child)]
        if isinstance(value, list):
            return [item for child in value for item in MatchedSystemContractTest._walk_values(child)]
        return [value]

    def test_audit_rejects_one_field_contract_drift(self) -> None:
        mutations = {
            "missing arm": lambda value: value["arms"].pop(),
            "duplicate arm": lambda value: value["arms"].append(copy.deepcopy(value["arms"][0])),
            "extra arm": lambda value: value["arms"].append({**copy.deepcopy(value["arms"][0]), "arm_id": "extra"}),
            "native project field": lambda value: value["arms"][0].__setitem__("k_per_endpoint", 8),
            "native owner": lambda value: value["arms"][0].__setitem__("scheduler_owner", "project"),
            "selector labelled native": lambda value: value["arms"][5].__setitem__("kind", "native"),
            "arrival offset": lambda value: value["arms"][0].__setitem__("arrival_offsets_s", [0, 6]),
            "arrival contract": lambda value: value["arms"][0].__setitem__("job_internal_arrival_contract", "manifest_timed"),
            "writeback": lambda value: value["arms"][0].__setitem__("performance_writeback_mode", "pgvector"),
            "manifest drift": lambda value: value["arms"][1].__setitem__("manifest_path", "other.jsonl"),
            "sha drift": lambda value: value["arms"][1].__setitem__("manifest_sha256", "f" * 64),
            "endpoint drift": lambda value: value["arms"][1].__setitem__("endpoint_ids", ["endpoint-0"]),
            "service drift": lambda value: value["arms"][1]["service_signature"].__setitem__("model", "other"),
            "protocol drift": lambda value: value["arms"][1].__setitem__("protocol", "chat_completions"),
            "output cap drift": lambda value: value["arms"][1].__setitem__("output_cap", 8),
            "project k drift": lambda value: value["arms"][4].__setitem__("k_per_endpoint", 7),
            "ready bytes drift": lambda value: value["arms"][4].__setitem__("ready_bytes", 7),
            "actor drift": lambda value: value["arms"][4]["actor_topology"].__setitem__("workers", 7),
            "organizer drift": lambda value: value["arms"][4].__setitem__("organizer", "other"),
            "source drift": lambda value: value["arms"][4]["source"].__setitem__("workload", "other"),
            "saor policy": lambda value: value["arms"][7].__setitem__("policy", "shared_fifo"),
            "saor observation": lambda value: value["arms"][7].__setitem__("ready_observation", "single_head"),
            "saor debt": lambda value: value["arms"][7].__setitem__("debt_caps", [0.2, None]),
            "reused output": lambda value: value["arms"][1].__setitem__("output_root", value["arms"][0]["output_root"]),
            "existing output": lambda value: value["arms"][1].__setitem__("output_root", str(path.parent)),
            "source kind": lambda value: value["arms"][0]["source"].__setitem__("kind", "postgres"),
            "source boundary": lambda value: value["arms"][0]["source"].__setitem__("timing_boundary", "outside_job"),
            "source database": lambda value: value["arms"][0]["source"].__setitem__("database_url", ""),
            "source workload": lambda value: value["arms"][0]["source"].__setitem__("workload_name", ""),
            "native normalized credit": lambda value: value["arms"][0].__setitem__("request-credit", 8),
            "native normalized coordinator": lambda value: value["arms"][0].__setitem__("shared_credit_coordinator", "x"),
            "native normalized router": lambda value: value["arms"][0].__setitem__("endpoint-router", "x"),
            "native bounded ready": lambda value: value["arms"][0].__setitem__("ready_observation", "bounded_concrete_pre_registration"),
            "selector ready observation": lambda value: value["arms"][5].__setitem__("ready_observation", "single_head"),
            "frozen static bounded ready": lambda value: value["arms"][3].__setitem__("ready_observation", "bounded_concrete_pre_registration"),
            "formal local authorization": lambda value: value.__setitem__("gpu_formal_locally_authorized", True),
            "manifest readiness": lambda value: value.__setitem__("matched_manifest_status", "placeholder_not_ready"),
            "project calibration drift": lambda value: value["arms"][5].__setitem__("calibration_path", "other-calibration.json"),
            "project request-tail drift": lambda value: value["arms"][5]["unsupported_request_tails"].__setitem__("reason", "other"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), self._config() as path:
                raw = json.loads(path.read_text(encoding="utf-8"))
                mutate(raw)
                path.write_text(json.dumps(raw), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_matched_system_config(path)

    def test_matrix_runs_each_physical_cell_once_in_balanced_order(self) -> None:
        with self._config() as path:
            config = load_matched_system_config(path)
            expected = [
                cell
                for phase, count in (
                    ("warmup", config.warmup_repeats),
                    ("formal", config.formal_repeats),
                    (
                        "selector_sanity_development",
                        config.selector_sanity_development_repeats,
                    ),
                )
                for repeat in range(1, count + 1)
                for cell in balanced_matched_schedule(
                    config, phase=phase, repeat=repeat
                )
            ]
            calls: list[tuple[str, str, int, int]] = []
            idle_calls: list[str] = []
            instrumenter_calls: list[object] = []
            lease = SimpleNamespace(released=False)
            lease.release = lambda: setattr(lease, "released", True)

            def executor(arm, identity, output_dir):
                calls.append(
                    (arm.kind, arm.arm_id, identity.repeat, identity.order_index)
                )
                return self._cell_evidence(arm, identity, output_dir)

            result = run_matched_system(
                path,
                native_executor=executor,
                project_executor=executor,
                idle_gate=lambda position: idle_calls.append(position),
                instrumenter=lambda *args: instrumenter_calls.append(args),
                repository_commit_getter=lambda: "abc123",
                host_lease_acquirer=lambda *_args, **_kwargs: lease,
            )

            self.assertEqual(
                [item[1:] for item in calls],
                [
                    (cell.arm_id, cell.repeat, cell.order_index)
                    for cell in expected
                ],
            )
            self.assertEqual(
                sum(item[1] == "project_bounded_ready_saor_0125we" for item in calls),
                config.warmup_repeats
                + config.formal_repeats
                + config.selector_sanity_development_repeats,
            )
            saor = [
                record for record in result["cells"]
                if record["arm_id"] == "project_bounded_ready_saor_0125we"
            ]
            self.assertTrue(all(
                record["report_blocks"] == ["system", "selector_sanity"]
                for record in saor
            ))
            self.assertEqual(idle_calls, [item for _ in expected for item in ("before", "after")])
            self.assertEqual(instrumenter_calls, [])
            self.assertTrue(lease.released)
            persisted = json.loads((path.parent / "matrix_index.json").read_text())
            self.assertEqual(persisted["status"], "completed")
            self.assertEqual(len(persisted["cells"]), len(expected))

    def test_matrix_retains_first_failure_stops_and_releases_lease(self) -> None:
        with self._config() as path:
            config = load_matched_system_config(path)
            first_two = balanced_matched_schedule(
                config, phase="warmup", repeat=1
            )[:2]
            calls: list[str] = []
            idle_calls: list[str] = []
            lease = SimpleNamespace(released=False)
            lease.release = lambda: setattr(lease, "released", True)

            def executor(arm, identity, output_dir):
                calls.append(arm.arm_id)
                if len(calls) == 2:
                    raise RuntimeError("cell exploded")
                return self._cell_evidence(arm, identity, output_dir)

            with self.assertRaisesRegex(RuntimeError, "cell exploded"):
                run_matched_system(
                    path,
                    native_executor=executor,
                    project_executor=executor,
                    idle_gate=lambda position: idle_calls.append(position),
                    instrumenter=lambda *_args: None,
                    repository_commit_getter=lambda: "abc123",
                    host_lease_acquirer=lambda *_args, **_kwargs: lease,
                )

            self.assertEqual(calls, [cell.arm_id for cell in first_two])
            persisted = json.loads((path.parent / "matrix_index.json").read_text())
            self.assertEqual(persisted["status"], "failed")
            self.assertEqual(persisted["cells"][-1]["status"], "failed")
            self.assertIn("cell exploded", persisted["cells"][-1]["error"])
            self.assertEqual(
                idle_calls,
                ["before", "after", "before", "after"],
            )
            self.assertTrue(lease.released)

    def test_matrix_preserves_primary_error_and_records_after_idle_error(self) -> None:
        with self._config() as path:
            lease = SimpleNamespace(released=False)
            lease.release = lambda: setattr(lease, "released", True)
            idle_calls: list[str] = []

            def idle_gate(position: str) -> None:
                idle_calls.append(position)
                if position == "after":
                    raise RuntimeError("after idle failed")

            with self.assertRaisesRegex(RuntimeError, "executor failed"):
                run_matched_system(
                    path,
                    native_executor=lambda *_args: (_ for _ in ()).throw(
                        RuntimeError("executor failed")
                    ),
                    project_executor=lambda *_args: {},
                    idle_gate=idle_gate,
                    instrumenter=lambda *_args: None,
                    repository_commit_getter=lambda: "abc123",
                    host_lease_acquirer=lambda *_args, **_kwargs: lease,
                )

            self.assertEqual(idle_calls, ["before", "after"])
            persisted = json.loads((path.parent / "matrix_index.json").read_text())
            self.assertIn("executor failed", persisted["cells"][0]["error"])
            self.assertIn(
                "after idle failed",
                persisted["cells"][0]["details"]["after_idle_error"],
            )

    def test_matrix_attempts_after_idle_when_before_idle_fails(self) -> None:
        with self._config() as path:
            lease = SimpleNamespace(released=False)
            lease.release = lambda: setattr(lease, "released", True)
            idle_calls: list[str] = []

            def idle_gate(position: str) -> None:
                idle_calls.append(position)
                if position == "before":
                    raise RuntimeError("before idle failed")

            with self.assertRaisesRegex(RuntimeError, "before idle failed"):
                run_matched_system(
                    path,
                    native_executor=lambda *_args: {},
                    project_executor=lambda *_args: {},
                    idle_gate=idle_gate,
                    instrumenter=lambda *_args: None,
                    repository_commit_getter=lambda: "abc123",
                    host_lease_acquirer=lambda *_args, **_kwargs: lease,
                )

            self.assertEqual(idle_calls, ["before", "after"])

    def test_matrix_rejects_existing_output_root_before_lease(self) -> None:
        with self._config() as path:
            (path.parent / "matrix_index.json").write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(FileExistsError, "matrix output root"):
                run_matched_system(
                    path,
                    native_executor=lambda *_args: {},
                    project_executor=lambda *_args: {},
                    idle_gate=lambda _position: None,
                    instrumenter=lambda *_args: None,
                )

    def test_matrix_records_lease_acquisition_failure(self) -> None:
        with self._config() as path:
            with self.assertRaisesRegex(RuntimeError, "lease unavailable"):
                run_matched_system(
                    path,
                    native_executor=lambda *_args: {},
                    project_executor=lambda *_args: {},
                    idle_gate=lambda _position: None,
                    instrumenter=lambda *_args: None,
                    repository_commit_getter=lambda: "abc123",
                    host_lease_acquirer=lambda *_args, **_kwargs: (
                        (_ for _ in ()).throw(RuntimeError("lease unavailable"))
                    ),
                )
            persisted = json.loads((path.parent / "matrix_index.json").read_text())
            self.assertEqual(persisted["status"], "failed")
            self.assertIn("lease unavailable", persisted["lease_error"])

    def test_matrix_rejects_missing_job_overlap_evidence(self) -> None:
        with self._config() as path:
            lease = SimpleNamespace(released=False)
            lease.release = lambda: setattr(lease, "released", True)

            def executor(arm, identity, output_dir):
                evidence = self._cell_evidence(arm, identity, output_dir)
                evidence["jobs"][0]["ended_epoch_s"] = 104.0
                return evidence

            with self.assertRaisesRegex(RuntimeError, "overlap"):
                run_matched_system(
                    path,
                    native_executor=executor,
                    project_executor=executor,
                    idle_gate=lambda _position: None,
                    instrumenter=lambda *_args: None,
                    repository_commit_getter=lambda: "abc123",
                    host_lease_acquirer=lambda *_args, **_kwargs: lease,
                )
            self.assertTrue(lease.released)

    def test_matrix_rejects_status_only_counter_and_resource_evidence(self) -> None:
        mutations = {
            "row count": lambda evidence, _root: evidence["jobs"][0].update(
                {"completed_count": 1, "expected_count": 2}
            ),
            "token delta": lambda evidence, _root: evidence["service_metrics"].update(
                {"prompt_tokens_delta": 0, "generation_tokens_delta": 0}
            ),
            "empty resource": lambda evidence, root: Path(
                evidence["resource_metrics"]["path"]
            ).write_text("observed_epoch_s,gpu\n", encoding="utf-8"),
            "untimed resource": lambda evidence, root: Path(
                evidence["resource_metrics"]["path"]
            ).write_text("gpu_utilization_pct\n80\n", encoding="utf-8"),
            "missing output": lambda evidence, root: evidence["output_paths"].update(
                {"commands": str(root / "missing.json")}
            ),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), self._config() as path:
                lease = SimpleNamespace(released=False)
                lease.release = lambda: setattr(lease, "released", True)

                def executor(arm, identity, output_dir):
                    evidence = self._cell_evidence(arm, identity, output_dir)
                    mutate(evidence, output_dir)
                    return evidence

                with self.assertRaises(RuntimeError):
                    run_matched_system(
                        path, native_executor=executor, project_executor=executor,
                        idle_gate=lambda _position: None,
                        instrumenter=lambda *_args: None,
                        repository_commit_getter=lambda: "abc123",
                        host_lease_acquirer=lambda *_args, **_kwargs: lease,
                    )

    def test_cli_accepts_runner_profiler_and_lifecycle_options(self) -> None:
        options = parse_args(
            [
                "--config", "matched.json",
                "--native-config", "native.json",
                "--project-config", "project.json",
                "--native-runner", "native-runner.py",
                "--profiler", "profiler.py",
                "--python-executable", sys.executable,
                "--health-url", "http://health",
                "--metrics-urls", "http://metrics0,http://metrics1",
                "--ray-address", "ray://cluster",
                "--rehearsal",
            ]
        )

        self.assertEqual(options.metrics_urls, ("http://metrics0", "http://metrics1"))
        self.assertTrue(options.rehearsal)

    def test_cli_rejects_unsupported_resume_flags_at_parse_time(self) -> None:
        base = [
            "--config", "matched.json", "--native-config", "native.json",
            "--project-config", "project.json", "--native-runner", "native.py",
            "--profiler", "profiler.py", "--python-executable", sys.executable,
            "--health-url", "http://health", "--metrics-urls", "http://metrics",
            "--ray-address", "ray://cluster",
        ]
        for flag in ("--resume", "--recover-stale-lease"):
            with self.subTest(flag=flag), self.assertRaises(SystemExit):
                parse_args([*base, flag])

    def test_rehearsal_runs_all_eight_warmup_cells_once_and_no_other_phase(self) -> None:
        with self._config() as path:
            calls = []
            lease = SimpleNamespace(released=False)
            lease.release = lambda: setattr(lease, "released", True)

            def executor(arm, identity, output_dir):
                calls.append(identity)
                return self._cell_evidence(arm, identity, output_dir)

            run_matched_system(
                path, native_executor=executor, project_executor=executor,
                idle_gate=lambda _position: None, instrumenter=lambda *_args: None,
                repository_commit_getter=lambda: "abc123",
                host_lease_acquirer=lambda *_args, **_kwargs: lease,
                rehearsal=True,
            )

            self.assertEqual(len(calls), 8)
            self.assertEqual({item.phase for item in calls}, {"warmup"})
            self.assertEqual({item.repeat for item in calls}, {1})
            self.assertEqual(len({item.arm_id for item in calls}), 8)

    @staticmethod
    def _cell_evidence(arm, identity, output_dir: Path) -> dict[str, object]:
        output_dir.mkdir(parents=True)
        command = ["runner", "--adapter", arm.arm_id]
        (output_dir / "commands.json").write_text(json.dumps(command), encoding="utf-8")
        (output_dir / "resources.csv").write_text(
            "observed_epoch_s,gpu\n100.5,0\n", encoding="utf-8"
        )
        return {
            "command": command,
            "implementation_source": "official_native" if arm.kind == "native" else "project_runner",
            "start_epoch_s": 100.0,
            "end_epoch_s": 101.0,
            "database_operator_e2e_s": 1.0,
            "jobs": [
                {
                    "job_id": "a",
                    "actual_launch_epoch_s": 100.0,
                    "ended_epoch_s": 106.0,
                    "completed_count": 2,
                    "expected_count": 2,
                    "exactly_once": True,
                    "shard_provenance": [{
                        "source_kind": "timed_postgres_manifest",
                        "source_timing_boundary": "inside_job_barrier",
                        "source_validation_status": "ok",
                    }],
                },
                {
                    "job_id": "b",
                    "actual_launch_epoch_s": 105.0,
                    "ended_epoch_s": 105.4,
                    "completed_count": 2,
                    "expected_count": 2,
                    "exactly_once": True,
                    "shard_provenance": [{
                        "source_kind": "timed_postgres_manifest",
                        "source_timing_boundary": "inside_job_barrier",
                        "source_validation_status": "ok",
                    }],
                },
            ],
            "service_metrics": {
                "metrics_status": "ok", "request_success_delta": 4,
                "prompt_tokens_delta": 10, "generation_tokens_delta": 10,
            },
            "resource_metrics": {"resource_metrics_status": "ok", "path": str(output_dir / "resources.csv")},
            "exactly_once": True,
            "request_tail_status": dict(arm.unsupported_request_tails),
            "output_paths": {"commands": str(output_dir / "commands.json")},
            "status": "passed",
        }

    class _ConfigPath:
        def __init__(self, owner: "MatchedSystemContractTest") -> None:
            self._temporary = tempfile.TemporaryDirectory()
            self.path = Path(self._temporary.name) / "config.json"
            manifest = Path(self._temporary.name) / "manifest.jsonl"
            write_manifest(
                manifest,
                tuple(
                    ChatRequest(
                        doc_id=index, prompt=f"p-{index}", arrival_time_s=0.0,
                        prompt_tokens=10, max_output_tokens=256,
                        estimated_output_tokens=256, source_row_hash=f"row-{index}",
                        endpoint_index=(index - 1) % 2,
                    )
                    for index in range(1, 5)
                ),
            )
            digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
            common = {
                "manifest_path": str(manifest), "manifest_sha256": digest,
                "endpoint_ids": ["endpoint-0", "endpoint-1"],
                "service_signature": {"model": "test", "service": "vllm"},
                "protocol": "completions", "output_cap": 256,
                "arrival_offsets_s": [0, 5], "job_internal_arrival_contract": "eager",
                "performance_writeback_mode": "none",
                "unsupported_request_tails": {"status": "unavailable", "reason": "unsupported"},
                "source": {"kind": "timed_postgres_manifest", "timing_boundary": "inside_job_barrier", "database_url": "postgresql://localhost/test", "workload_name": "test", "row_offset": 512}, "organizer": "daft",
            }
            native = lambda arm_id, owner: {**common, "arm_id": arm_id, "kind": "native", "scheduler_owner": owner, "output_root": f"out/{arm_id}", "calibration_path": "native-calibration.json"}
            project = lambda arm_id, policy: {**common, "arm_id": arm_id, "kind": "project", "scheduler_owner": "project", "output_root": f"out/{arm_id}", "policy": policy, "k_per_endpoint": 8, "work_limit_per_endpoint": 65536, "ready_bytes": 4096, "actor_topology": {"workers": 1, "concurrency": 256}, "calibration_path": "project-calibration.json"}
            arms = [native("daft_native", "daft"), native("daft_ray", "daft"), native("ray_data_http", "ray_data"), project("project_frozen_static", "static_partition"), project("project_bounded_ready_fifo", "shared_fifo"), project("project_bounded_ready_drr", "shared_drr"), project("project_bounded_ready_vtc_style", "external_vtc"), project("project_bounded_ready_saor_0125we", "saor_bounded_ready")]
            for arm in arms[4:]: arm["ready_observation"] = "bounded_concrete_pre_registration"
            arms[7]["debt_caps"] = [0.125, None]
            self.path.write_text(json.dumps({"schema_version": 1, "seed": 7, "warmup_repeats": 1, "formal_repeats": 3, "selector_sanity_development_repeats": 2, "gpu_formal_locally_authorized": False, "matched_manifest_status": "ready_frozen", "arms": arms}), encoding="utf-8")
        def __enter__(self) -> Path: return self.path
        def __exit__(self, *args: object) -> None: self._temporary.cleanup()

    def _config(self) -> "MatchedSystemContractTest._ConfigPath":
        return self._ConfigPath(self)


if __name__ == "__main__":
    unittest.main()
