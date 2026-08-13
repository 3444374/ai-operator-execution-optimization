"""Contract tests for the eight-arm SAOR matched-system readiness audit."""

from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from src.experiments.saor.native_system_matched import (
    REQUIRED_ARM_IDS,
    SELECTOR_SANITY_ARM_IDS,
    SYSTEM_ARM_IDS,
    audit_matched_system_config,
    balanced_matched_schedule,
    load_matched_system_config,
)


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
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), self._config() as path:
                raw = json.loads(path.read_text(encoding="utf-8"))
                mutate(raw)
                path.write_text(json.dumps(raw), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_matched_system_config(path)

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
                "source": {"workload": "test", "row_offset": 512}, "organizer": "daft",
            }
            native = lambda arm_id, owner: {**common, "arm_id": arm_id, "kind": "native", "scheduler_owner": owner, "output_root": f"out/{arm_id}", "calibration_path": "native-calibration.json"}
            project = lambda arm_id, policy: {**common, "arm_id": arm_id, "kind": "project", "scheduler_owner": "project", "output_root": f"out/{arm_id}", "policy": policy, "k_per_endpoint": 8, "work_limit_per_endpoint": 65536, "ready_bytes": 4096, "actor_topology": {"workers": 1, "concurrency": 256}, "calibration_path": "project-calibration.json"}
            arms = [native("daft_native", "daft"), native("daft_ray", "daft"), native("ray_data_http", "ray_data"), project("project_frozen_static", "static_partition"), project("project_bounded_ready_fifo", "shared_fifo"), project("project_bounded_ready_drr", "shared_drr"), project("project_bounded_ready_vtc_style", "external_vtc"), project("project_bounded_ready_saor_0125we", "saor_bounded_ready")]
            for arm in arms[4:]: arm["ready_observation"] = "bounded_concrete_pre_registration"
            arms[7]["debt_caps"] = [0.125, None]
            self.path.write_text(json.dumps({"schema_version": 1, "seed": 7, "warmup_repeats": 1, "formal_repeats": 3, "selector_sanity_development_repeats": 2, "gpu_formal_locally_authorized": False, "arms": arms}), encoding="utf-8")
        def __enter__(self) -> Path: return self.path
        def __exit__(self, *args: object) -> None: self._temporary.cleanup()

    def _config(self) -> "MatchedSystemContractTest._ConfigPath":
        return self._ConfigPath(self)


if __name__ == "__main__":
    unittest.main()
