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
from types import SimpleNamespace

from src.experiments.saor.native_system_matched import (
    REQUIRED_ARM_IDS,
    SELECTOR_SANITY_ARM_IDS,
    SYSTEM_ARM_IDS,
    audit_matched_system_config,
    balanced_matched_schedule,
    load_matched_system_config,
    run_matched_system,
)
from scripts.experiments.run_saor_native_system_matched import parse_args


class MatchedSystemContractTest(unittest.TestCase):
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
            name: "1"
            for name in {
                value.removeprefix("${").removesuffix("}")
                for arm in example["arms"]
                for value in self._walk_values(arm)
                if isinstance(value, str) and value.startswith("${") and value.endswith("}")
            }
        }
        environment["DATABASE_URL"] = "postgresql://localhost/test"
        environment["SAOR_MATCHED_MANIFEST_SHA256"] = hashlib.sha256(manifest.read_bytes()).hexdigest()
        old_environment = os.environ.copy()
        try:
            os.environ.update(environment)
            with self.assertRaises(ValueError):
                load_matched_system_config(example_path)
        finally:
            os.environ.clear()
            os.environ.update(old_environment)

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
                "--rehearsal", "--resume", "--recover-stale-lease",
            ]
        )

        self.assertEqual(options.metrics_urls, ("http://metrics0", "http://metrics1"))
        self.assertTrue(options.rehearsal)
        self.assertTrue(options.resume)
        self.assertTrue(options.recover_stale_lease)

    @staticmethod
    def _cell_evidence(arm, identity, output_dir: Path) -> dict[str, object]:
        output_dir.mkdir(parents=True)
        command = ["runner", "--adapter", arm.arm_id]
        (output_dir / "commands.json").write_text(json.dumps(command), encoding="utf-8")
        (output_dir / "resources.csv").write_text("sample,gpu\n0,0\n", encoding="utf-8")
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
                    "exactly_once": True,
                    "shard_provenance": [{
                        "source_kind": "timed_postgres_manifest",
                        "source_timing_boundary": "inside_job_barrier",
                        "source_validation_status": "ok",
                    }],
                },
            ],
            "service_metrics": {"metrics_status": "ok", "request_success_delta": 4},
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
            manifest.write_text('{"id": 1}\n', encoding="utf-8")
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
