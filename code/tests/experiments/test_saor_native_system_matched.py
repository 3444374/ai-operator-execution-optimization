"""Fail-closed contract tests for the SAOR five-arm and official VTC groups."""

from __future__ import annotations

import copy
import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from src.baselines.common.contracts import ChatRequest
from src.baselines.common.manifests import write_manifest
from src.experiments.saor.native_system_evidence import (
    persisted_command,
    persisted_failure,
)
from src.experiments.saor.native_system_matched import (
    REQUIRED_ARM_IDS,
    SYSTEM_ARM_IDS,
    audit_matched_system_config,
    balanced_matched_schedule,
    formal_authorization_requirements,
    load_matched_system_config,
    run_matched_system,
    validate_release_gated_events,
)
from src.experiments.saor.native_system_publisher import (
    RANKING_OUTPUT_NAMES,
    publish_failed_generation,
)
from src.experiments.saor.native_system_sink import collect_completion_rows
from src.experiments.saor.native_system_validator import (
    validate_uniform_cell_identity,
)
from src.experiments.saor.official_vtc_capability import (
    OFFICIAL_VTC_ARM_IDS,
    build_serving_mechanism_report,
    load_official_vtc_capability,
)
from scripts.experiments.run_saor_native_system_matched import parse_args


class MatchedSystemContractTest(unittest.TestCase):
    def test_release_examples_define_exact_five_arm_db_e2e_matrix(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        matched = json.loads((
            repository / "deploy/autodl/saor_native_system_matched.example.json"
        ).read_text(encoding="utf-8"))
        project = json.loads((
            repository / "deploy/autodl/saor_native_system_matched_project.example.json"
        ).read_text(encoding="utf-8"))
        self.assertEqual(
            tuple(arm["arm_id"] for arm in matched["arms"]),
            SYSTEM_ARM_IDS,
        )
        self.assertEqual(
            [scenario["scenario_id"] for scenario in project["scenarios"]],
            ["project_frozen_static", "project_bounded_ready_saor_0125we"],
        )
        forbidden = {"project_bounded_ready_fifo", "project_bounded_ready_drr", "project_bounded_ready_vtc_style"}
        self.assertTrue(forbidden.isdisjoint(arm["arm_id"] for arm in matched["arms"]))
        for arm in matched["arms"]:
            self.assertEqual(arm["job_release_schedule"], [
                {"job_id": "job0", "release_time_s": 0},
                {"job_id": "job1", "release_time_s": 5},
            ])
            self.assertEqual(arm["arrival_replay_capability"], "not_used")
            self.assertIn("mfu_contract", arm)
            self.assertEqual(arm["performance_writeback_mode"], "json_text")

    def test_native_arm_rejects_bounded_ready_k_w_and_credit_controls(self) -> None:
        for field, value in (
            ("ready_observation", "bounded_concrete_pre_registration"),
            ("k_per_endpoint", 8),
            ("work_limit_per_endpoint", 65536),
            ("credit_policy", "shared"),
        ):
            with self.subTest(field=field), Fixture() as fixture:
                raw = fixture.read()
                raw["arms"][0][field] = value
                fixture.write(raw)
                with self.assertRaisesRegex(ValueError, "native arm rejects"):
                    load_matched_system_config(fixture.path)

    def test_saor_observation_registration_or_submit_before_release_fails(self) -> None:
        with Fixture() as fixture:
            config = load_matched_system_config(fixture.path)
            arm = next(item for item in config.arms if item.arm_id.endswith("0125we"))
            jobs = [
                {
                    "job_id": "job0", "scheduled_launch_epoch_s": 100.0,
                    "concrete_ready_epoch_s": 100.0,
                    "credit_registered_epoch_s": 100.1,
                    "first_submit_epoch_s": 100.2,
                },
                {
                    "job_id": "job1", "scheduled_launch_epoch_s": 105.0,
                    "concrete_ready_epoch_s": 104.9,
                    "credit_registered_epoch_s": 105.1,
                    "first_submit_epoch_s": 105.2,
                },
            ]
            with self.assertRaisesRegex(RuntimeError, "before external Job release"):
                validate_release_gated_events(arm, jobs)

    def test_cross_arm_job_release_mismatch_fails(self) -> None:
        with Fixture() as fixture:
            raw = fixture.read()
            raw["arms"][-1]["job_release_schedule"][1]["release_time_s"] = 6
            fixture.write(raw)
            with self.assertRaisesRegex(ValueError, "typed job0/job1 releases|job_release_schedule drifts"):
                load_matched_system_config(fixture.path)

    def test_official_vtc_group_cannot_report_without_fcfs_control(self) -> None:
        with VtcFixture() as fixture:
            raw = fixture.read()
            raw["arms"] = [raw["arms"][1]]
            fixture.write(raw)
            with self.assertRaisesRegex(ValueError, "requires exactly FCFS and VTC"):
                load_official_vtc_capability(fixture.path)

    def test_official_vtc_evidence_never_enters_db_e2e_ranking(self) -> None:
        with VtcFixture() as fixture:
            config = load_official_vtc_capability(fixture.path)
            evidence = official_vtc_evidence(config)
            evidence[0]["comparison_scope"] = "database_e2e"
            with self.assertRaisesRegex(ValueError, "cannot enter DB-E2E"):
                build_serving_mechanism_report(config, evidence)

    def test_official_vtc_commit_owner_and_workload_sha_drift_fail(self) -> None:
        with VtcFixture() as fixture:
            config = load_official_vtc_capability(fixture.path)
            for field, value in (
                ("artifact_commit", "0" * 40),
                ("scheduler_owner", "project"),
                ("manifest_sha256", "0" * 64),
            ):
                with self.subTest(field=field):
                    evidence = official_vtc_evidence(config)
                    if field == "scheduler_owner":
                        evidence[1][field] = value
                    else:
                        evidence[0][field] = value
                    with self.assertRaisesRegex(ValueError, "evidence drift"):
                        build_serving_mechanism_report(config, evidence)

    def test_gpu_peak_or_precision_drift_fails_and_is_fingerprinted(self) -> None:
        for field, value in (
            ("gpu_peak_tflops_per_gpu", 999.0),
            ("precision", "fp16"),
        ):
            with self.subTest(field=field), Fixture() as fixture:
                raw = fixture.read()
                raw["arms"][-1]["mfu_contract"][field] = value
                fixture.write(raw)
                with self.assertRaisesRegex(ValueError, "mfu_contract drifts"):
                    load_matched_system_config(fixture.path)
        with Fixture() as fixture:
            config = load_matched_system_config(fixture.path)
            identity = formal_authorization_requirements(
                fixture.path, config, "a" * 40
            )
            self.assertEqual(len(identity["resolved_config_sha256"]), 64)
            self.assertEqual(
                identity["mfu_contract"], config.arms[0].mfu_contract.__dict__
            )

    def test_formal_cells_are_not_created_or_executed_without_authorization(self) -> None:
        with Fixture() as fixture:
            calls: list[str] = []
            output_root = Path(load_matched_system_config(fixture.path).matrix_output_root)
            with self.assertRaises(PermissionError):
                run_matched_system(
                    fixture.path,
                    native_executor=lambda *_args: calls.append("native") or {},
                    project_executor=lambda *_args: calls.append("project") or {},
                    idle_gate=lambda *_args: calls.append("idle"),
                    instrumenter=lambda *_args: calls.append("instrument"),
                    repository_commit_getter=lambda: "b" * 40,
                    formal_authorization_path=None,
                )
            self.assertEqual(calls, [])
            self.assertFalse(output_root.exists())

    def test_failed_cell_stays_in_all_runs_but_no_ranking_is_published(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            for name in RANKING_OUTPUT_NAMES:
                (output / name).write_text("stale\n", encoding="utf-8")
            publish_failed_generation(
                output,
                [{"run_id": "r1", "status": "failed", "failure_reason": "guard"}],
                ["guard failed"],
                {"comparison_scope": "database_e2e_five_arm_system_matrix"},
            )
            with (output / "all_runs.csv").open(newline="", encoding="utf-8") as stream:
                self.assertEqual(list(csv.DictReader(stream))[0]["status"], "failed")
            self.assertFalse(any((output / name).exists() for name in RANKING_OUTPUT_NAMES))

    def test_mixed_root_manifest_service_owner_commit_or_fingerprint_fails(self) -> None:
        owners = {arm_id: ("daft" if arm_id.startswith("daft") else "ray_data" if arm_id == "ray_data_http" else "project") for arm_id in REQUIRED_ARM_IDS}
        base = {
            "repository_commit": "a" * 40,
            "matrix_instance_id": "root",
            "config_sha256": "b" * 64,
            "config_fingerprint": "c" * 64,
            "authorization_sha256": "d" * 64,
            "manifest_sha256": "e" * 64,
            "service_signature": {"model": "m", "service": "v"},
        }
        rows = [{**base, "arm_id": arm_id, "scheduler_owner": owners[arm_id]} for arm_id in REQUIRED_ARM_IDS]
        validate_uniform_cell_identity(rows, owners)
        for field, value in (
            ("repository_commit", "f" * 40),
            ("matrix_instance_id", "other"),
            ("config_fingerprint", "f" * 64),
            ("manifest_sha256", "f" * 64),
            ("service_signature", {"model": "other"}),
            ("scheduler_owner", "wrong"),
        ):
            with self.subTest(field=field):
                corrupted = copy.deepcopy(rows)
                corrupted[-1][field] = value
                with self.assertRaisesRegex(ValueError, "identity|scheduler-owner"):
                    validate_uniform_cell_identity(corrupted, owners)

    def test_persisted_exception_and_command_are_redacted(self) -> None:
        secret = "synthetic-sensitive-value"
        error = persisted_failure(RuntimeError(f"api_key={secret}"))
        command = persisted_command(["runner", "--api-key", secret])
        self.assertNotIn(secret, error)
        self.assertNotIn("password", error)
        self.assertNotIn(secret, " ".join(command))

    def test_schedule_contains_only_five_db_e2e_arms(self) -> None:
        with Fixture() as fixture:
            config = load_matched_system_config(fixture.path)
            for phase in ("warmup", "formal"):
                cells = balanced_matched_schedule(config, phase=phase, repeat=1)
                self.assertEqual({cell.arm_id for cell in cells}, set(SYSTEM_ARM_IDS))
                self.assertTrue(all(cell.report_blocks == ("db_e2e_system",) for cell in cells))
            audit = audit_matched_system_config(config)
            self.assertEqual(audit["report_blocks"], {"db_e2e_system": list(SYSTEM_ARM_IDS)})

    def test_cli_is_thin_and_keeps_formal_authorization_explicit(self) -> None:
        options = parse_args([
            "--config", "a.json", "--native-config", "n.json",
            "--project-config", "p.json", "--native-runner", "native.py",
            "--profiler", "profiler.py", "--python-executable", "python3",
            "--health-url", "http://127.0.0.1:8000/health",
            "--metrics-urls", "http://127.0.0.1:8000/metrics",
            "--ray-address", "auto",
        ])
        self.assertIsNone(options.formal_authorization)
        self.assertFalse(options.rehearsal)

    def test_completion_sink_collects_independent_trace_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "jobs"
            root.mkdir()
            path = root / "job0.requests.csv"
            with path.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(
                    stream, fieldnames=("doc_id", "status", "output_text")
                )
                writer.writeheader()
                writer.writerow({"doc_id": 2, "status": "completed", "output_text": "b"})
                writer.writerow({"doc_id": 1, "status": "completed", "output_text": "a"})
            self.assertEqual(
                collect_completion_rows(Path(directory)), [(1, "a"), (2, "b")]
            )

    def test_completion_sink_rejects_duplicate_doc_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "jobs"
            root.mkdir()
            for index in range(2):
                path = root / f"job{index}.requests.csv"
                with path.open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(
                        stream, fieldnames=("doc_id", "status", "output_text")
                    )
                    writer.writeheader()
                    writer.writerow({"doc_id": 1, "status": "completed", "output_text": "a"})
            with self.assertRaisesRegex(RuntimeError, "duplicate doc_id"):
                collect_completion_rows(Path(directory))


def official_vtc_evidence(config) -> list[dict[str, object]]:
    return [{
        "arm_id": arm.arm_id,
        "status": "passed",
        "artifact_commit": config.artifact_commit,
        "scheduler_owner": arm.scheduler_owner,
        "manifest_sha256": config.manifest_sha256,
        "job_manifest_sha256": list(config.job_manifest_sha256),
        "job_release_times_s": list(config.job_release_times_s),
        "job_mapping": config.job_mapping,
        "model_contract": dict(config.model_contract),
        "output_contract": dict(config.output_contract),
        "comparison_scope": "serving_mechanism_only",
    } for arm in config.arms]


class VtcFixture:
    def __enter__(self):
        repository = Path(__file__).resolve().parents[3]
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "vtc.json"
        self.path.write_text((
            repository / "deploy/autodl/saor_official_vtc_capability.example.json"
        ).read_text(encoding="utf-8"), encoding="utf-8")
        return self

    def read(self):
        return json.loads(self.path.read_text(encoding="utf-8"))

    def write(self, value):
        self.path.write_text(json.dumps(value), encoding="utf-8")

    def __exit__(self, *_args):
        self.temp.cleanup()


class Fixture:
    def __enter__(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.path = root / "config.json"
        requests = tuple(ChatRequest(
            doc_id=index,
            prompt=f"p-{index}",
            arrival_time_s=0.0,
            prompt_tokens=10,
            max_output_tokens=256,
            estimated_output_tokens=256,
            source_row_hash=f"row-{index}",
            endpoint_index=(index - 1) % 2,
        ) for index in range(1, 5))
        combined = root / "combined.jsonl"
        jobs = (root / "job0.jsonl", root / "job1.jsonl")
        write_manifest(jobs[0], requests[:2])
        write_manifest(jobs[1], requests[2:])
        write_manifest(combined, requests)
        job_contract = [{
            "job_id": f"job{index}", "path": str(path), "rows": 2,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        } for index, path in enumerate(jobs)]
        calibrations = {}
        for arm_id, adapter, concurrency, batch in (
            ("daft_native", "daft_native", 1, 1),
            ("daft_ray", "daft_ray", 1, 1),
            ("ray_data_http", "ray_data_http", 8, 16),
        ):
            path = root / f"{arm_id}.json"
            path.write_text(json.dumps({
                "schema_version": 1, "status": "ready",
                "selection": {"adapter": adapter, "concurrency_per_endpoint": concurrency, "batch_size": batch},
                "evidence": {
                    "configuration_identity": {"status": "verified"},
                    "performance_selection": {
                        "status": "development_screen_only" if adapter == "ray_data_http" else "not_applicable",
                        "reason": "fixture",
                    },
                },
            }), encoding="utf-8")
            calibrations[arm_id] = path
        project_calibration = root / "project.json"
        project_calibration.write_text(json.dumps({
            "schema_version": 1, "status": "ready", "selection": {},
        }), encoding="utf-8")
        common = {
            "manifest_path": str(combined),
            "manifest_sha256": hashlib.sha256(combined.read_bytes()).hexdigest(),
            "job_manifests": job_contract,
            "endpoint_ids": ["endpoint-0", "endpoint-1"],
            "service_signature": {"model": "test", "service": "vllm"},
            "protocol": "completions", "output_cap": 256,
            "job_release_schedule": [{"job_id": "job0", "release_time_s": 0}, {"job_id": "job1", "release_time_s": 5}],
            "arrival_replay_capability": "not_used",
            "job_internal_arrival_contract": "eager",
            "mfu_contract": {"status": "unavailable", "gpu_peak_tflops_per_gpu": 82.58, "precision": "bf16_dense_fp32_accumulate", "reason": "uniform numerator unavailable"},
            "performance_writeback_mode": "json_text",
            "unsupported_request_tails": {"status": "unavailable", "reason": "unsupported"},
            "source": {"kind": "timed_postgres_manifest", "timing_boundary": "inside_job_barrier", "database_url": "postgresql://localhost/test", "workload_name": "test"},
            "organizer": "native_framework_owned",
        }
        def native(arm_id, owner):
            path = calibrations[arm_id]
            return {**common, "arm_id": arm_id, "kind": "native", "scheduler_owner": owner, "output_root": str(root / f"out-{arm_id}"), "calibration_path": str(path), "calibration_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        def project(arm_id, policy):
            return {**common, "organizer": "sequential", "arm_id": arm_id, "kind": "project", "scheduler_owner": "project", "policy": policy, "output_root": str(root / f"out-{arm_id}"), "calibration_path": str(project_calibration), "calibration_sha256": hashlib.sha256(project_calibration.read_bytes()).hexdigest(), "k_per_endpoint": 8, "work_limit_per_endpoint": 65536, "ready_bytes": 4096, "actor_topology": {"workers": 1, "concurrency": 256, "cpus_per_worker": 0.25}, "batching_contract": {"policy": "token_budget", "token_budget": 6144, "token_budget_policy": "static"}}
        arms = [
            native("daft_native", "daft"),
            native("daft_ray", "daft"),
            native("ray_data_http", "ray_data"),
            project("project_frozen_static", "static_partition"),
            project("project_bounded_ready_saor_0125we", "saor_bounded_ready"),
        ]
        arms[-1]["ready_observation"] = "bounded_concrete_pre_registration"
        arms[-1]["debt_caps"] = [0.125, None]
        self.write({
            "schema_version": 1, "seed": 7, "warmup_repeats": 1,
            "formal_repeats": 3, "matrix_output_root": str(root / "matrix"),
            "endpoint_urls": ["http://127.0.0.1:8000/v1/chat/completions", "http://127.0.0.1:8001/v1/chat/completions"],
            "gpu_formal_locally_authorized": False,
            "matched_manifest_status": "ready_frozen", "arms": arms,
        })
        return self

    def read(self):
        return json.loads(self.path.read_text(encoding="utf-8"))

    def write(self, value):
        self.path.write_text(json.dumps(value), encoding="utf-8")

    def __exit__(self, *_args):
        self.temp.cleanup()


if __name__ == "__main__":
    unittest.main()
