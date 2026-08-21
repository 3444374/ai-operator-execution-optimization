"""Fail-closed contract tests for the SAOR five-arm and official VTC groups."""

from __future__ import annotations

import contextlib
import copy
import csv
import hashlib
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.baselines.common.contracts import ChatRequest
from src.baselines.common.manifests import write_manifest
from src.experiments.saor.native_system_evidence import (
    persisted_command,
    persisted_failure,
)
from src.experiments.saor.native_system_bindings import validate_executor_bindings
from src.experiments.saor.native_system_matched import (
    REQUIRED_ARM_IDS,
    SYSTEM_ARM_IDS,
    audit_matched_system_config,
    balanced_matched_schedule,
    build_rehearsal_validation_payload,
    formal_authorization_requirements,
    load_matched_system_config,
    run_identity_requirements,
    run_matched_system,
    validate_rehearsal_evidence,
    validate_release_gated_events,
)
from src.experiments.saor.native_system_publisher import (
    RANKING_OUTPUT_NAMES,
    publish_failed_generation,
)
from src.experiments.saor.native_system_readiness import (
    validate_correctness_smoke_evidence,
    validate_system_preflight_evidence,
    verify_rehearsal_service_identity,
)
from src.experiments.saor.native_system_preflight import (
    _gpu_compute_pids,
    _is_descendant_of,
    build_system_preflight_payload,
    validate_gpu_compute_processes,
)
from src.experiments.saor.native_system_sink import collect_completion_rows
from src.experiments.saor.native_system_summary import (
    _validate_formal_authorization_binding,
)
from src.experiments.saor.native_system_validator import (
    validate_uniform_cell_identity,
)
from src.experiments.saor.official_vtc_capability import (
    OFFICIAL_VTC_ARM_IDS,
    build_serving_mechanism_report,
    load_official_vtc_capability,
)
from scripts.experiments.run_saor_native_system_matched import parse_args
from scripts.analysis import run_saor_native_system_preflight as system_preflight_cli
from src.infrastructure.vllm_preflight import (
    VLLM_DISTRIBUTION_HASH_FIELDS,
    VLLM_SOURCE_HASH_FIELDS,
)


class MatchedSystemContractTest(unittest.TestCase):
    def test_summary_formal_authorization_binds_all_three_config_hashes(self) -> None:
        authorization = {
            "schema_version": 1,
            "status": "authorized",
            "scope": "saor_native_system_matched_formal",
            "formal_authorized": True,
            "repository_commit": "a" * 40,
            "config_sha256": "b" * 64,
            "native_config_sha256": "c" * 64,
            "project_config_sha256": "d" * 64,
            "resolved_config_sha256": "e" * 64,
            "manifest_sha256": "f" * 64,
            "job_manifests": [],
            "mfu_contract": {"status": "unavailable"},
            "rehearsal_evidence": {"validation_sha256": "1" * 64},
        }
        runtime = {
            **authorization,
            "execution_mode": "formal",
            "authorization_sha256": "2" * 64,
        }
        _validate_formal_authorization_binding(authorization, runtime)

        for field in ("native_config_sha256", "project_config_sha256"):
            with self.subTest(field=field):
                drifted = dict(runtime)
                drifted[field] = "9" * 64
                with self.assertRaisesRegex(ValueError, field):
                    _validate_formal_authorization_binding(
                        authorization, drifted
                    )

    def test_release_examples_define_exact_five_arm_db_e2e_matrix(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        matched = json.loads((
            repository / "deploy/autodl/saor_native_system_matched.example.json"
        ).read_text(encoding="utf-8"))
        project = json.loads((
            repository / "deploy/autodl/saor_native_system_matched_project.example.json"
        ).read_text(encoding="utf-8"))
        native = json.loads((
            repository / "deploy/autodl/saor_native_system_matched_native.example.json"
        ).read_text(encoding="utf-8"))
        frozen_env = (
            repository / "deploy/autodl/saor_native_system_matched.env.example"
        ).read_text(encoding="utf-8")
        self.assertIn("export VLLM_SCHEDULING_POLICY=fcfs", frozen_env)
        self.assertEqual(
            tuple(arm["arm_id"] for arm in matched["arms"]),
            SYSTEM_ARM_IDS,
        )
        self.assertEqual(
            [scenario["scenario_id"] for scenario in project["scenarios"]],
            ["project_frozen_static", "project_bounded_ready_saor_0125we"],
        )
        self.assertEqual(
            matched["service_identity"], native["service_identity"]
        )
        self.assertEqual(
            matched["service_identity"], project["service_identity"]
        )
        self.assertTrue(project["require_complete_service_metadata"])
        self.assertEqual(
            set(native["native_implementation_provenance"]),
            {"daft_native", "daft_ray", "ray_data_http"},
        )
        for provenance in native["native_implementation_provenance"].values():
            self.assertEqual(len(provenance["upstream_commit"]), 40)
            self.assertEqual(len(provenance["adapter_sha256"]), 64)
            self.assertFalse(provenance["upstream_source_modified"])
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
            self.assertEqual(arm["service_signature"]["scheduler"], "vllm_native_fcfs")
            if arm["kind"] == "project":
                self.assertEqual(arm["organizer"], "${SAOR_ORGANIZER}")
                self.assertEqual(arm["executor"], "ray_actor")
                self.assertEqual(
                    arm["scheduler_owner"],
                    "project_daft_ray_submission_then_vllm_fcfs",
                )
                self.assertEqual(
                    arm["model_service_scheduler"], "vllm_native_fcfs"
                )

    def test_active_runbooks_keep_driver_vllm_split_and_five_arm_calibration(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        autodl = (repository / "deploy/autodl/README.md").read_text(encoding="utf-8")
        scripts = (repository / "code/scripts/README.md").read_text(encoding="utf-8")
        calibration = (
            repository
            / "experiments/results/state_aware_work_unit/"
            "saor_native_system_matched_calibration_20260819/README.md"
        ).read_text(encoding="utf-8")
        self.assertIn('PYTHONPATH=code "$DRIVER_PYTHON"', autodl)
        self.assertIn("--vllm-python", autodl)
        self.assertIn("四阶段", scripts)
        self.assertIn("当前五臂", calibration.splitlines()[2])
        self.assertNotIn("八臂 native-system matched comparison 的四份", calibration)
        self.assertNotIn("project ×5", calibration)

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

    def test_project_arms_require_resolved_daft_ray_actor_and_native_fcfs(self) -> None:
        for field, value, message in (
            ("organizer", "sequential", "organizer must be daft"),
            ("executor", "ray_task", "executor must be ray_actor"),
            ("scheduler_owner", "project", "scheduler owner must be"),
            (
                "model_service_scheduler",
                "custom_fcfs",
                "model service scheduler must be vllm_native_fcfs",
            ),
        ):
            with self.subTest(field=field), Fixture() as fixture:
                raw = fixture.read()
                raw["arms"][-1][field] = value
                fixture.write(raw)
                with self.assertRaisesRegex(ValueError, message):
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
                (
                    "scheduler_owner",
                    "project_daft_ray_submission_then_vllm_fcfs",
                ),
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
            identity = run_identity_requirements(
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

    def test_rehearsal_readiness_rejects_an_existing_matrix_root(self) -> None:
        with Fixture() as fixture:
            config = load_matched_system_config(fixture.path)
            output_root = Path(config.matrix_output_root)
            output_root.mkdir()
            with self.assertRaisesRegex(ValueError, "matrix_output_root already exists"):
                run_matched_system(
                    fixture.path,
                    native_executor=lambda *_args: {},
                    project_executor=lambda *_args: {},
                    idle_gate=lambda *_args: None,
                    instrumenter=lambda *_args: None,
                    repository_commit_getter=lambda: "b" * 40,
                    rehearsal=True,
                )

    def test_formal_authorization_is_bound_to_actual_rehearsal_root_and_archive(self) -> None:
        commit = "a" * 40
        with Fixture() as fixture:
            config = load_matched_system_config(fixture.path)
            root = Path(config.matrix_output_root)
            native_path = fixture.path.parent / "native-contract.json"
            project_path = fixture.path.parent / "project-contract.json"
            provenance = successful_native_provenance()
            native_path.write_text(json.dumps({
                "native_implementation_provenance": provenance
            }), encoding="utf-8")
            project_path.write_text(json.dumps({"kind": "project"}), encoding="utf-8")
            expected = run_identity_requirements(
                fixture.path, config, commit, native_path, project_path
            )
            preflight = {
                "schema_version": 1,
                "status": "rehearsal_ready",
                "rehearsal_ready": True,
                "binding": {
                    "repository_commit": commit,
                    "config_sha256": expected["config_sha256"],
                    "resolved_config_sha256": expected["resolved_config_sha256"],
                    "native_config_sha256": expected["native_config_sha256"],
                    "project_config_sha256": expected["project_config_sha256"],
                },
                "stages": {
                    "static_config": "passed",
                    "service_identity": "passed",
                    "system_preflight": "passed",
                    "correctness_smoke": "passed",
                },
                "service_identity": {
                    "installed_source": {"status": "passed"},
                    "live_service": {"status": "passed"},
                },
                "system_preflight": {"status": "passed"},
                "correctness_smoke": {"status": "passed"},
            }
            run_matched_system(
                fixture.path,
                native_executor=successful_cell_evidence,
                project_executor=successful_cell_evidence,
                idle_gate=lambda *_args: None,
                instrumenter=lambda *_args: None,
                repository_commit_getter=lambda: commit,
                host_lease_acquirer=lambda *_args, **_kwargs: SimpleNamespace(
                    release=lambda: None
                ),
                rehearsal=True,
                service_identity_preflight=preflight,
                native_config_path=native_path,
                project_config_path=project_path,
                native_implementation_provenance=provenance,
            )
            archive = fixture.path.parent / "reviewed-rehearsal.tar.gz"
            with tarfile.open(archive, "w:gz") as stream:
                stream.add(root, arcname=root.name)
            validation = fixture.path.parent / "rehearsal_validation.json"
            validation.write_text(json.dumps(build_rehearsal_validation_payload(
                fixture.path, config, commit, root, archive,
                native_path, project_path, provenance,
            )), encoding="utf-8")
            evidence = validate_rehearsal_evidence(
                fixture.path, config, commit, validation, root, archive,
                native_path, project_path, provenance,
            )
            requirements = formal_authorization_requirements(
                fixture.path, config, commit, evidence
            )
            self.assertEqual(
                requirements["rehearsal_evidence"]["archive_sha256"],
                hashlib.sha256(archive.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                requirements["native_config_sha256"],
                hashlib.sha256(native_path.read_bytes()).hexdigest(),
            )
            drifted_provenance = copy.deepcopy(provenance)
            drifted_provenance["daft_native"]["adapter_sha256"] = "e" * 64
            with self.assertRaisesRegex(RuntimeError, "provenance"):
                build_rehearsal_validation_payload(
                    fixture.path, config, commit, root, archive,
                    native_path, project_path, drifted_provenance,
                )
            forged = json.loads(validation.read_text(encoding="utf-8"))
            forged["valid_rehearsal"] = False
            validation.write_text(json.dumps(forged), encoding="utf-8")
            with self.assertRaisesRegex(PermissionError, "identity drifted"):
                validate_rehearsal_evidence(
                    fixture.path, config, commit, validation, root, archive,
                    native_path, project_path, provenance,
                )

            index = json.loads((root / "matrix_index.json").read_text())
            self.assertEqual(
                index["native_config_sha256"],
                expected["native_config_sha256"],
            )
            self.assertEqual(
                index["project_config_sha256"],
                expected["project_config_sha256"],
            )
            self.assertTrue(all(
                cell["native_config_sha256"] == expected["native_config_sha256"]
                and cell["project_config_sha256"]
                == expected["project_config_sha256"]
                for cell in index["cells"]
            ))
            native_cells = [
                cell for cell in index["cells"]
                if cell["arm_id"] in {"daft_native", "daft_ray", "ray_data_http"}
            ]
            self.assertTrue(all(
                "native_implementation_provenance" in cell for cell in native_cells
            ))

    def test_minimal_handwritten_rehearsal_root_cannot_authorize_formal(self) -> None:
        commit = "a" * 40
        with Fixture() as fixture:
            config = load_matched_system_config(fixture.path)
            native_path = fixture.path.parent / "native-contract.json"
            project_path = fixture.path.parent / "project-contract.json"
            native_path.write_text("{}", encoding="utf-8")
            project_path.write_text("{}", encoding="utf-8")
            provenance = successful_native_provenance()
            base = run_identity_requirements(
                fixture.path, config, commit, native_path, project_path
            )
            root = fixture.path.parent / "forged"
            root.mkdir()
            (root / "matrix_index.json").write_text(json.dumps({
                "schema_version": 1,
                "status": "completed",
                "execution_mode": "rehearsal",
                "repository_commit": commit,
                "config_sha256": base["config_sha256"],
                "config_fingerprint": base["resolved_config_sha256"],
                "manifest_sha256": base["manifest_sha256"],
                "authorization_sha256": "",
                "schedule": [],
                "cells": [],
                "service_identity_preflight": {"rehearsal_ready": True},
            }), encoding="utf-8")
            (root / "matrix_contract_snapshot.json").write_text("{}")
            archive = fixture.path.parent / "forged.tar.gz"
            with tarfile.open(archive, "w:gz") as stream:
                stream.add(root, arcname=root.name)
            with self.assertRaisesRegex(RuntimeError, "snapshot|matrix"):
                build_rehearsal_validation_payload(
                    fixture.path, config, commit, root, archive,
                    native_path, project_path, provenance,
                )

    def test_correctness_smoke_uses_a_separate_root_and_is_deep_validated(self) -> None:
        commit = "a" * 40
        with Fixture() as fixture:
            config = load_matched_system_config(fixture.path)
            native_path = fixture.path.parent / "native-contract.json"
            project_path = fixture.path.parent / "project-contract.json"
            native_path.write_text("{}", encoding="utf-8")
            project_path.write_text("{}", encoding="utf-8")
            provenance = successful_native_provenance()
            expected = run_identity_requirements(
                fixture.path, config, commit, native_path, project_path
            )
            binding = {
                "repository_commit": commit,
                "config_sha256": expected["config_sha256"],
                "resolved_config_sha256": expected["resolved_config_sha256"],
                "native_config_sha256": expected["native_config_sha256"],
                "project_config_sha256": expected["project_config_sha256"],
            }
            system_sha = "d" * 64
            run_matched_system(
                fixture.path,
                native_executor=successful_cell_evidence,
                project_executor=successful_cell_evidence,
                idle_gate=lambda *_args: None,
                instrumenter=lambda *_args: None,
                repository_commit_getter=lambda: commit,
                host_lease_acquirer=lambda *_args, **_kwargs: SimpleNamespace(
                    release=lambda: None
                ),
                correctness_smoke=True,
                matrix_output_root_override=(
                    fixture.path.parent / "smoke-attempt-001"
                ),
                service_identity_preflight={
                    "binding": binding,
                    "status": "system_preflight_passed",
                    "rehearsal_ready": False,
                    "stages": {
                        "static_config": "passed",
                        "service_identity": "passed",
                        "system_preflight": "passed",
                        "correctness_smoke": "not_checked",
                    },
                    "system_preflight": {
                        "status": "passed", "evidence_sha256": system_sha
                    },
                },
                native_config_path=native_path,
                project_config_path=project_path,
                native_implementation_provenance=provenance,
            )
            base_root = Path(config.matrix_output_root)
            smoke_index = fixture.path.parent / "smoke-attempt-001" / "matrix_index.json"
            self.assertFalse(base_root.exists())
            result = validate_correctness_smoke_evidence(
                smoke_index, binding, system_sha, config, provenance
            )
            self.assertEqual(result["completed_cells"], 5)
            index = json.loads(smoke_index.read_text())
            artifact = (
                Path(result["root"])
                / index["cells"][0]["cell_artifact_root"]
                / "raw_executor.json"
            )
            artifact.write_text("tampered", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "raw artifact manifest"):
                validate_correctness_smoke_evidence(
                    smoke_index, binding, system_sha, config, provenance
                )

    def test_correctness_smoke_cannot_create_the_canonical_rehearsal_root(self) -> None:
        for nested in (False, True):
            with self.subTest(nested=nested), Fixture() as fixture:
                config = load_matched_system_config(fixture.path)
                canonical = Path(config.matrix_output_root)
                override = canonical / "child" if nested else canonical
                with self.assertRaisesRegex(ValueError, "canonical rehearsal root"):
                    run_matched_system(
                        fixture.path,
                        native_executor=lambda *_args: {},
                        project_executor=lambda *_args: {},
                        idle_gate=lambda *_args: None,
                        instrumenter=lambda *_args: None,
                        repository_commit_getter=lambda: "a" * 40,
                        correctness_smoke=True,
                        matrix_output_root_override=override,
                    )
                self.assertFalse(canonical.exists())

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
        owners = {
            arm_id: (
                "daft" if arm_id.startswith("daft") else
                "ray_data" if arm_id == "ray_data_http" else
                "project_daft_ray_submission_then_vllm_fcfs"
            )
            for arm_id in REQUIRED_ARM_IDS
        }
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
            "--vllm-python", "/vllm/bin/python",
            "--vllm-runtime-identity", "ep8000.json",
            "--vllm-runtime-identity", "ep8001.json",
            "--installed-source-audit", "source-audit.json",
            "--system-preflight-evidence", "system.json",
            "--correctness-smoke-evidence", "smoke.json",
        ])
        self.assertIsNone(options.formal_authorization)
        self.assertFalse(options.rehearsal)
        self.assertFalse(options.correctness_smoke)
        self.assertEqual(options.vllm_python, Path("/vllm/bin/python"))

        smoke = parse_args([
            "--config", "a.json", "--native-config", "n.json",
            "--project-config", "p.json", "--native-runner", "native.py",
            "--profiler", "profiler.py", "--driver-python", "python3",
            "--health-url", "http://127.0.0.1:8000/health",
            "--metrics-urls", "http://127.0.0.1:8000/metrics",
            "--ray-address", "auto", "--vllm-python", "/vllm/bin/python",
            "--vllm-runtime-identity", "ep8000.json",
            "--installed-source-audit", "source-audit.json",
            "--system-preflight-evidence", "system.json",
            "--correctness-smoke", "--correctness-smoke-root", "smoke-root",
        ])
        self.assertTrue(smoke.correctness_smoke)
        self.assertIsNone(smoke.correctness_smoke_evidence)
        self.assertEqual(smoke.correctness_smoke_root.name, "smoke-root")

    def test_readiness_and_source_audit_clis_import_from_repository_root(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        for script in (
            "code/scripts/analysis/audit_saor_native_system_matched.py",
            "code/scripts/analysis/run_saor_native_system_preflight.py",
            "code/scripts/analysis/audit_vllm_0251_source.py",
            "code/scripts/analysis/validate_saor_native_system_rehearsal.py",
        ):
            with self.subTest(script=script):
                completed = subprocess.run(
                    [sys.executable, script, "--help"],
                    cwd=repository,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertNotIn("ModuleNotFoundError", completed.stderr)

    def test_source_audit_cli_redacts_exception_before_persisting(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        secret = "synthetic-sensitive-value"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "audit.json"
            missing = Path(directory) / f"api_key={secret}" / "missing.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    "code/scripts/analysis/audit_vllm_0251_source.py",
                    "--config", str(missing), "--output", str(output),
                ],
                cwd=repository,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 2)
            self.assertNotIn(secret, output.read_text(encoding="utf-8"))

    def test_system_preflight_cli_redacts_arbitrary_third_party_exception(self) -> None:
        class ThirdPartyFailure(Exception):
            pass

        secret = "synthetic-sensitive-value"
        stdout = io.StringIO()
        with patch.object(
            system_preflight_cli,
            "load_and_validate_static_readiness",
            side_effect=ThirdPartyFailure(f"api_key={secret}"),
        ), contextlib.redirect_stdout(stdout):
            return_code = system_preflight_cli.main([
                "--config", "matched.json",
                "--native-config", "native.json",
                "--project-config", "project.json",
                "--vllm-runtime-identity", "runtime.json",
                "--ray-address", "auto",
                "--bounded-baseline-root", "bounded",
                "--output", "preflight.json",
            ])
        self.assertEqual(return_code, 2)
        self.assertNotIn(secret, stdout.getvalue())
        self.assertIn("failed", stdout.getvalue())

    def test_three_config_service_identity_binding_fails_on_project_drift(self) -> None:
        with Fixture() as fixture:
            matched_config = load_matched_system_config(fixture.path)
            identity = matched_config.service_identity
        endpoints = matched_config.endpoint_urls
        expected_metadata = tuple(sorted({
            "vllm_version": "0.25.1",
            "enforce_eager": False,
            "compilation_mode": "vllm_compile",
            "chunked_prefill": True,
            "max_num_batched_tokens": 8192,
            "max_num_seqs": 256,
            "gpu_memory_utilization": 0.9,
            "prefix_caching": True,
            "mfu_metrics": True,
            "scheduling_policy": "fcfs",
        }.items()))
        matched = SimpleNamespace(
            endpoint_urls=endpoints, service_identity=identity, arms=()
        )
        native = SimpleNamespace(
            endpoint_urls=endpoints,
            service_identity=identity,
            service_prefix_caching="enabled",
            service_max_num_seqs=256,
            service_max_num_batched_tokens=8192,
            arms=(),
        )
        common_args = (
            "--completion-protocol", "completions",
            "--completion-max-tokens", "256",
            "--organizer", "daft", "--executor", "ray_actor",
            "--completion-endpoint-urls", ",".join(endpoints),
            "--model-metrics-urls",
            "http://127.0.0.1:8000/metrics,http://127.0.0.1:8001/metrics",
            "--database-url", "postgresql://localhost/test",
            "--gpu-peak-tflops", "82.58",
            "--mfu-precision", "bf16_dense_fp32_accumulate",
            "--writeback-mode", "json_text",
            "--source-workload-name", "test",
            "--actor-workers-per-endpoint", "1",
            "--ray-actor-max-concurrency", "256",
            "--ray-worker-num-cpus", "0.25",
            "--batching-policy", "token_budget",
            "--token-budget", "6144",
            "--token-budget-policy", "static",
        )
        project = SimpleNamespace(
            common_args=common_args,
            service_identity=identity,
            service_metadata=expected_metadata,
            scenarios=(),
        )
        validate_executor_bindings(matched, native, project)
        project.service_identity = tuple(
            (key, "drift" if key == "dtype" else value)
            for key, value in identity
        )
        with self.assertRaisesRegex(ValueError, "project.service_identity"):
            validate_executor_bindings(matched, native, project)

    def test_rehearsal_gate_reaudits_current_installed_source(self) -> None:
        with Fixture() as fixture:
            matched = load_matched_system_config(fixture.path)
            identity = dict(matched.service_identity)
            stored = exact_source_audit(identity)
            evidence_path = fixture.path.parent / "source-audit.json"
            evidence_path.write_text(json.dumps(stored), encoding="utf-8")
            current = copy.deepcopy(stored)
            current["status"] = "blocked_source_drift"
            current["errors"] = ["installed source SHA-256 drifted"]
            with patch(
                "src.experiments.saor.native_system_readiness.run_vllm_source_audit",
                return_value=current,
            ), patch(
                "src.experiments.saor.native_system_readiness.verify_live_vllm_service_identity"
            ) as live:
                with self.assertRaisesRegex(RuntimeError, "did not pass"):
                    verify_rehearsal_service_identity(
                        matched,
                        fixture.path,
                        Path(sys.executable),
                        evidence_path,
                        (fixture.path.parent / "runtime.json",),
                    )
                live.assert_not_called()

    def test_declarative_system_preflight_and_smoke_json_cannot_pass(self) -> None:
        binding = {
            "repository_commit": "a" * 40,
            "config_sha256": "b" * 64,
            "resolved_config_sha256": "c" * 64,
            "service_identity_sha256": "d" * 64,
        }
        with Fixture() as fixture:
            matched = load_matched_system_config(fixture.path)
            root = fixture.path.parent
            runtime = root / "runtime.json"
            runtime.write_text(json.dumps({"pid": 123}), encoding="utf-8")
            system = root / "system.json"
            system.write_text(json.dumps({
                "schema_version": 1,
                "status": "passed",
                "binding": binding,
                "checks": {
                    "endpoint_health": {"status": "passed"},
                    "postgresql": {
                        "status": "passed", "server_version": "18.3",
                        "pgvector_version": "0.8.1",
                    },
                    "ray_gpu_clean": {"status": "passed"},
                    "bounded_baseline": {
                        "status": "passed", "feeding_saturation_ratio": 0.97,
                    },
                },
            }), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "schema"):
                validate_system_preflight_evidence(
                    system, binding, matched, (runtime,)
                )
            smoke = root / "smoke.json"
            smoke.write_text(json.dumps({
                "schema_version": 1,
                "status": "passed",
                "binding": binding,
                "system_preflight_sha256": "f" * 64,
                "checks": {
                    "manifest_validation": {"status": "passed"},
                    "exactly_once": {"status": "passed"},
                    "sink_validation": {"status": "passed"},
                },
                "completed_rows": 10,
            }), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "matrix_index"):
                validate_correctness_smoke_evidence(
                    smoke, binding, "f" * 64, matched
                )

    def test_system_preflight_builder_rehashes_real_bounded_root(self) -> None:
        with Fixture() as fixture:
            matched = load_matched_system_config(fixture.path)
            runtime = fixture.path.parent / "runtime.json"
            runtime.write_text(json.dumps({"pid": 123}), encoding="utf-8")
            not_before = runtime.stat().st_mtime_ns
            root = fixture.path.parent / "bounded"
            cell = root / "bounded_http"
            cell.mkdir(parents=True)
            (root / "run_status.json").write_text(json.dumps({
                "status": "passed", "blocked_cells": []
            }), encoding="utf-8")
            (cell / "gate.json").write_text(json.dumps({
                "status": "passed", "passed": True
            }), encoding="utf-8")
            for index, url in enumerate(matched.endpoint_urls):
                shard = cell / f"shard_{index}"
                shard.mkdir()
                service = dict(matched.service_identity)
                service_sha = hashlib.sha256(json.dumps({
                    "model": service["model"],
                    "protocol": matched.arms[0].protocol,
                    "temperature": 0.0,
                    "ignore_eos": False,
                    "service_prefix_caching": "enabled",
                    "service_max_num_seqs": service["max_num_seqs"],
                    "service_max_num_batched_tokens": service["max_num_batched_tokens"],
                }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                (shard / "summary.json").write_text(json.dumps({
                    "adapter": "bounded_http",
                    "status": "completed",
                    "exactly_once": True,
                    "failed_count": 0,
                    "completion_protocol": matched.arms[0].protocol,
                    "model_name": service["model"],
                    "service_config_sha256": service_sha,
                    "endpoint_url": url,
                    "tokens_per_s": 100.0 + index,
                }), encoding="utf-8")
            payload = build_system_preflight_payload(
                {"repository_commit": "a" * 40}, matched,
                ray_address="auto", bounded_baseline_root=root,
                not_before_mtime_ns=not_before,
                allowed_vllm_root_pids=(123,),
                endpoint_probe=lambda urls: {
                    "status": "passed", "endpoints": list(urls)
                },
                postgresql_probe=lambda _url: {
                    "status": "passed", "server_version": "18.3",
                    "pgvector_version": "0.8.1",
                },
                ray_probe=lambda address, pids: {
                    "status": "passed", "ray_address": address,
                    "allowed_vllm_root_pids": list(pids),
                },
            )
            self.assertEqual(
                payload["checks"]["bounded_baseline"]["total_tokens_per_s"],
                201.0,
            )
            evidence = fixture.path.parent / "system-preflight.json"
            evidence.write_text(json.dumps(payload), encoding="utf-8")
            with patch(
                "src.experiments.saor.native_system_readiness.build_system_preflight_payload",
                return_value=payload,
            ) as reprobe:
                result = validate_system_preflight_evidence(
                    evidence, payload["binding"], matched, (runtime,)
                )

            self.assertEqual(result["status"], "passed")
            reprobe.assert_called_once()
            summary = cell / "shard_0" / "summary.json"
            corrupted = json.loads(summary.read_text())
            corrupted["exactly_once"] = False
            summary.write_text(json.dumps(corrupted))
            with self.assertRaisesRegex(RuntimeError, "correctness"):
                build_system_preflight_payload(
                    {}, matched, ray_address="auto", bounded_baseline_root=root,
                    not_before_mtime_ns=not_before,
                    allowed_vllm_root_pids=(123,),
                    endpoint_probe=lambda _urls: {"status": "passed"},
                    postgresql_probe=lambda _url: {"status": "passed"},
                    ray_probe=lambda _address, _pids: {"status": "passed"},
                )

    def test_gpu_process_probe_parses_compute_pids_and_rejects_unrelated_tree(self) -> None:
        with patch(
            "src.experiments.saor.native_system_preflight.subprocess.run",
            return_value=SimpleNamespace(returncode=0, stdout="123\n999\n"),
        ):
            self.assertEqual(_gpu_compute_pids(), {123, 999})
        self.assertTrue(_is_descendant_of(123, {123}))
        self.assertFalse(_is_descendant_of(99999999, {123}))
        with patch(
            "src.experiments.saor.native_system_preflight._gpu_compute_pids",
            return_value={123, 99999999},
        ), self.assertRaisesRegex(RuntimeError, "outside verified vLLM"):
            validate_gpu_compute_processes((123,))

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


def successful_native_provenance() -> dict[str, dict[str, object]]:
    """Return frozen native provenance matching the synthetic executors."""

    record = {
        "upstream_url": "https://example.invalid/upstream",
        "upstream_version": "1.0",
        "upstream_commit": "b" * 40,
        "adapter_path": "code/src/adapter.py",
        "adapter_sha256": "c" * 64,
        "upstream_source_modified": False,
        "adapter_diff_status": "thin_adapter_only_no_upstream_patch",
    }
    return {
        arm_id: dict(record)
        for arm_id in ("daft_native", "daft_ray", "ray_data_http")
    }


def successful_cell_evidence(arm, _cell, output_dir: Path) -> dict[str, object]:
    """Create one fully backed synthetic executor record for contract tests."""

    output_dir.mkdir(parents=True, exist_ok=True)
    commands = output_dir / "commands.json"
    commands.write_text(json.dumps({"commands": [["framework", "run"]]}))
    resources = output_dir / "resources.csv"
    resources.write_text("sample_epoch_s,gpu_utilization_pct\n1.0,80\n")
    (output_dir / "raw_executor.json").write_text(
        json.dumps({"status": "passed"}), encoding="utf-8"
    )
    native = arm.kind == "native"
    jobs = []
    for index, job in enumerate(arm.job_manifests):
        start = 100.0 + 5.0 * index
        row = {
            "job_id": job.job_id,
            "scheduled_launch_epoch_s": start,
            "actual_launch_epoch_s": start,
            "ended_epoch_s": 110.0 + 5.0 * index,
            "completed_count": job.rows,
            "expected_count": job.rows,
            "actual_work": job.rows * 10,
            "manifest_sha256": job.sha256,
            "exactly_once": True,
            "shard_provenance": [{
                "source_kind": "timed_postgres_manifest",
                "source_timing_boundary": "inside_job_barrier",
                "source_validation_status": "ok",
                "source_read_s": 0.1,
            }],
        }
        if native:
            row.update({
                "request_p50_s": "unavailable",
                "request_p95_s": "unavailable",
                "request_p99_status": "unavailable",
                "request_p99_s": "unavailable",
                "slo_status": "unavailable",
                "slo_violation_ratio": "unavailable",
                "tail_reason": "framework lacks request clocks",
            })
        else:
            row.update({
                "request_p50_s": 0.1,
                "request_p95_s": 0.2,
                "request_p99_status": "available",
                "request_p99_s": 0.3,
                "slo_status": "available",
                "slo_violation_ratio": 0.0,
                "tail_reason": "",
            })
        if arm.arm_id == "project_bounded_ready_saor_0125we":
            row.update({
                "concrete_ready_epoch_s": start,
                "credit_registered_epoch_s": start,
                "first_submit_epoch_s": start,
            })
        jobs.append(row)
    expected_rows = sum(job.rows for job in arm.job_manifests)
    fairness = (
        {
            "starvation_status": "unavailable",
            "longest_no_service_s": "unavailable",
            "completion_service_lag_status": "unavailable",
            "completion_service_lag_p95_work": "unavailable",
            "completion_service_lag_max_work": "unavailable",
            "reason": "framework lacks a completion ledger",
        }
        if native else {
            "starvation_status": "available",
            "longest_no_service_s": 0.0,
            "completion_service_lag_status": "available",
            "completion_service_lag_p95_work": 0.0,
            "completion_service_lag_max_work": 0.0,
            "reason": "",
        }
    )
    record = {
        "implementation_source": "official" if native else "project",
        "start_epoch_s": 100.0,
        "end_epoch_s": 115.0,
        "database_operator_e2e_s": 15.0,
        "jobs": jobs,
        "service_metrics": {
            "metrics_status": "ok",
            "prompt_tokens_delta": 100,
            "generation_tokens_delta": 10,
        },
        "resource_metrics": {
            "resource_metrics_status": "ok", "path": str(resources)
        },
        "exactly_once": True,
        "request_tail_status": (
            {
                metric: {
                    "status": "unavailable", "value": "unavailable",
                    "reason": "unsupported",
                }
                for metric in ("request_p99", "slo")
            }
        ),
        "service_fairness_metrics": fairness,
        "output_paths": {
            "commands": str(commands), "resources": str(resources)
        },
        "status": "passed",
        "server_version": "18.3",
        "pgvector_version": "0.8.1",
        "mfu_contract": arm.mfu_contract.__dict__,
        "sink_metrics": {
            "status": "passed",
            "mode": "json_text",
            "table": "document_completions",
            "written_by": "matrix_adapter" if native else "project_profiler",
            "expected_rows": expected_rows,
            "observed_rows": expected_rows,
            "expected_digest": "a" * 64,
            "observed_digest": "a" * 64,
            "exactly_once": True,
            "sink_wall_s": 0.1,
            "verified_epoch_s": 116.0,
        },
        "command": ["framework", "run"],
    }
    if native:
        record.update({
            "queue_final": {"endpoint-0": {"running": 0, "waiting": 0}},
            "native_implementation_provenance": successful_native_provenance()[
                arm.arm_id
            ],
        })
    elif arm.arm_id == "project_frozen_static":
        record["shared_credit_final"] = []
    else:
        record["shared_credit_final"] = [{
            "endpoint_id": "endpoint-0",
            "request_limit": 8,
            "work_limit": 65536,
            "active_requests": 0,
            "active_work": 0,
            "waiting_requests": 0,
            "waiting_work": 0,
            "active_by_job": {},
            "active_work_by_job": {},
            "waiting_by_job": {},
            "waiting_work_by_job": {},
            "waiting_head_work_by_job": {},
        }]
    return record


def exact_source_audit(identity: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "status": "passed",
        "installed_version": identity["service"],
        "package_root": "/frozen/vllm",
        "python_runtime": {
            "executable_argv0": "/frozen/bin/python",
            "sys_prefix": "/frozen",
            "package_root": "/frozen/vllm",
            "package_version": identity["service"],
        },
        "errors": [],
        "source_files": {
            relative: {
                "sha256": identity[field],
                "expected_sha256": identity[field],
                "markers_present": True,
            }
            for field, relative in VLLM_SOURCE_HASH_FIELDS.items()
        },
        "distribution_files": {
            filename: {
                "sha256": identity[field],
                "expected_sha256": identity[field],
            }
            for field, filename in VLLM_DISTRIBUTION_HASH_FIELDS.items()
        },
    }


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
        model = root / "model"
        model.mkdir()
        model_hashes = {}
        for name in (
            "config.json",
            "tokenizer_config.json",
            "tokenizer.json",
            "model.safetensors.index.json",
            "generation_config.json",
            "model-00001-of-00004.safetensors",
            "model-00002-of-00004.safetensors",
            "model-00003-of-00004.safetensors",
            "model-00004-of-00004.safetensors",
        ):
            (model / name).write_text(name, encoding="utf-8")
            model_hashes[name] = hashlib.sha256(
                (model / name).read_bytes()
            ).hexdigest()
        service_identity = {
            "model": "test", "model_path": str(model),
            "model_revision": "a" * 40,
            "model_config_sha256": model_hashes["config.json"],
            "tokenizer_config_sha256": model_hashes["tokenizer_config.json"],
            "tokenizer_json_sha256": model_hashes["tokenizer.json"],
            "model_safetensors_index_sha256": model_hashes[
                "model.safetensors.index.json"
            ],
            "generation_config_sha256": model_hashes["generation_config.json"],
            "model_weight_00001_sha256": model_hashes[
                "model-00001-of-00004.safetensors"
            ],
            "model_weight_00002_sha256": model_hashes[
                "model-00002-of-00004.safetensors"
            ],
            "model_weight_00003_sha256": model_hashes[
                "model-00003-of-00004.safetensors"
            ],
            "model_weight_00004_sha256": model_hashes[
                "model-00004-of-00004.safetensors"
            ],
            "dtype": "bfloat16", "service": "0.25.1",
            "vllm_metadata_sha256": "1" * 64,
            "vllm_wheel_sha256": "2" * 64,
            "vllm_record_sha256": "3" * 64,
            "vllm_source_config_scheduler_sha256": "4" * 64,
            "vllm_source_scheduler_sha256": "5" * 64,
            "vllm_source_async_scheduler_sha256": "6" * 64,
            "vllm_source_request_queue_sha256": "7" * 64,
            "vllm_source_request_sha256": "8" * 64,
            "scheduler": "vllm_native_fcfs", "scheduling_policy": "fcfs",
            "max_model_len": 8192,
            "max_num_seqs": 256, "max_num_batched_tokens": 8192,
            "chunked_prefill": True, "prefix_caching": True,
            "mfu_metrics": True, "enforce_eager": False,
            "compilation_mode": "vllm_compile",
            "gpu_memory_utilization": 0.9,
        }
        common = {
            "manifest_path": str(combined),
            "manifest_sha256": hashlib.sha256(combined.read_bytes()).hexdigest(),
            "job_manifests": job_contract,
            "endpoint_ids": ["endpoint-0", "endpoint-1"],
            "service_signature": {
                "model": "test", "service": "vllm",
                "scheduler": "vllm_native_fcfs",
            },
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
            return {**common, "organizer": "daft", "arm_id": arm_id, "kind": "project", "scheduler_owner": "project_daft_ray_submission_then_vllm_fcfs", "executor": "ray_actor", "model_service_scheduler": "vllm_native_fcfs", "policy": policy, "output_root": str(root / f"out-{arm_id}"), "calibration_path": str(project_calibration), "calibration_sha256": hashlib.sha256(project_calibration.read_bytes()).hexdigest(), "k_per_endpoint": 8, "work_limit_per_endpoint": 65536, "ready_bytes": 4096, "actor_topology": {"workers": 1, "concurrency": 256, "cpus_per_worker": 0.25}, "batching_contract": {"policy": "token_budget", "token_budget": 6144, "token_budget_policy": "static"}}
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
            "matched_manifest_status": "ready_frozen",
            "service_identity": service_identity,
            "arms": arms,
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
