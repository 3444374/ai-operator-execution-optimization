"""Fail-closed capability contract for the official VTC/S-LoRA artifact.

This module never dispatches a server and never mixes serving-mechanism evidence
with the database-E2E SAOR matrix.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from src.infrastructure.config_env import expand_structure


OFFICIAL_VTC_ARTIFACT_URL = "https://github.com/Ying1123/VTC-artifact"
OFFICIAL_VTC_ARM_IDS = ("official_vtc_fcfs", "official_vtc")
OFFICIAL_VTC_SCHEDULER_OWNERS = {
    "official_vtc_fcfs": "slora_fcfs",
    "official_vtc": "vtc",
}


@dataclass(frozen=True)
class OfficialVtcArm:
    arm_id: str
    scheduler_owner: str
    implementation_path: str


@dataclass(frozen=True)
class OfficialVtcCapabilityConfig:
    artifact_url: str
    artifact_commit: str
    runtime_owner: str
    capability_status: str
    blocker: str
    formal_authorized: bool
    server_validation: str
    db_e2e_ranking_eligible: bool
    manifest_sha256: str
    job_manifest_sha256: tuple[str, str]
    job_release_times_s: tuple[float, float]
    job_mapping: str
    model_contract: tuple[tuple[str, object], ...]
    output_contract: tuple[tuple[str, object], ...]
    arms: tuple[OfficialVtcArm, ...]


def load_official_vtc_capability(path: Path) -> OfficialVtcCapabilityConfig:
    """Load a same-stack FCFS/VTC capability contract without side effects."""

    raw = expand_structure(json.loads(path.read_text(encoding="utf-8")), "vtc_config")
    required = {
        "schema_version", "artifact_url", "artifact_commit", "runtime_owner",
        "capability_status", "blocker", "formal_authorized", "server_validation",
        "db_e2e_ranking_eligible", "workload", "arms",
    }
    if not isinstance(raw, dict) or set(raw) != required or raw.get("schema_version") != 1:
        raise ValueError("official VTC capability config schema is invalid")
    workload = raw["workload"]
    if not isinstance(workload, dict) or set(workload) != {
        "manifest_sha256", "job_manifest_sha256", "job_release_times_s",
        "job_mapping", "model_contract", "output_contract",
    }:
        raise ValueError("official VTC workload identity schema is invalid")
    arms_raw = raw["arms"]
    if not isinstance(arms_raw, list):
        raise ValueError("official VTC arms must be a list")
    arms = tuple(_load_arm(item) for item in arms_raw)
    config = OfficialVtcCapabilityConfig(
        artifact_url=_string(raw["artifact_url"], "artifact_url"),
        artifact_commit=_sha(raw["artifact_commit"], "artifact_commit"),
        runtime_owner=_string(raw["runtime_owner"], "runtime_owner"),
        capability_status=_string(raw["capability_status"], "capability_status"),
        blocker=_string(raw["blocker"], "blocker"),
        formal_authorized=_bool(raw["formal_authorized"], "formal_authorized"),
        server_validation=_string(raw["server_validation"], "server_validation"),
        db_e2e_ranking_eligible=_bool(
            raw["db_e2e_ranking_eligible"], "db_e2e_ranking_eligible"
        ),
        manifest_sha256=_sha(workload["manifest_sha256"], "manifest_sha256"),
        job_manifest_sha256=_two_shas(workload["job_manifest_sha256"]),
        job_release_times_s=_two_times(workload["job_release_times_s"]),
        job_mapping=_string(workload["job_mapping"], "job_mapping"),
        model_contract=_object_items(workload["model_contract"], "model_contract"),
        output_contract=_object_items(workload["output_contract"], "output_contract"),
        arms=arms,
    )
    errors = validate_official_vtc_capability(config)
    if errors:
        raise ValueError("; ".join(errors))
    return config


def validate_official_vtc_capability(
    config: OfficialVtcCapabilityConfig,
) -> list[str]:
    """Return readiness errors; a missing same-stack FCFS control always fails."""

    errors: list[str] = []
    ids = tuple(arm.arm_id for arm in config.arms)
    if set(ids) != set(OFFICIAL_VTC_ARM_IDS) or len(ids) != 2:
        errors.append("official VTC group requires exactly FCFS and VTC arms")
    if config.artifact_url != OFFICIAL_VTC_ARTIFACT_URL:
        errors.append("official VTC artifact URL drifted")
    if config.runtime_owner != "official_s_lora_artifact":
        errors.append("official VTC runtime owner drifted")
    if config.db_e2e_ranking_eligible:
        errors.append("official VTC evidence is never eligible for DB-E2E ranking")
    if config.formal_authorized:
        errors.append("official VTC formal execution is not authorized")
    if config.server_validation != "not_run":
        errors.append("official VTC server validation must remain not_run")
    if config.capability_status != "blocked_unverified_runtime":
        errors.append("official VTC capability must remain fail-closed pending runtime proof")
    if config.job_release_times_s != (0.0, 5.0):
        errors.append("official VTC logical Job release schedule drifted")
    if config.job_mapping != "one_database_job_per_vtc_client":
        errors.append("official VTC Job-to-client mapping drifted")
    if dict(config.model_contract).get("compatibility_status") != "unverified":
        errors.append("official VTC model compatibility must remain unverified")
    if dict(config.output_contract) != {
        "max_output_tokens": 256,
        "prompt_format": "raw",
        "temperature": 0,
    }:
        errors.append("official VTC logical output contract drifted")
    for arm in config.arms:
        expected = OFFICIAL_VTC_SCHEDULER_OWNERS.get(arm.arm_id)
        if expected is None or arm.scheduler_owner != expected:
            errors.append(f"{arm.arm_id} scheduler owner drifted")
        expected_path = {
            "official_vtc_fcfs": "fair_bench/FCFS",
            "official_vtc": "fair_bench/VTC",
        }.get(arm.arm_id)
        if arm.implementation_path != expected_path:
            errors.append(f"{arm.arm_id} implementation path drifted")
    return errors


def build_serving_mechanism_report(
    config: OfficialVtcCapabilityConfig,
    evidence: list[dict[str, object]],
) -> dict[str, object]:
    """Build a separate non-DB report only after both same-stack arms pass."""

    by_id = {str(row.get("arm_id", "")): row for row in evidence}
    if set(by_id) != set(OFFICIAL_VTC_ARM_IDS):
        raise ValueError("official VTC report requires same-stack FCFS and VTC evidence")
    for arm in config.arms:
        row = by_id[arm.arm_id]
        expected = {
            "status": "passed",
            "artifact_commit": config.artifact_commit,
            "scheduler_owner": arm.scheduler_owner,
            "manifest_sha256": config.manifest_sha256,
            "job_manifest_sha256": list(config.job_manifest_sha256),
            "job_release_times_s": list(config.job_release_times_s),
            "job_mapping": config.job_mapping,
            "model_contract": dict(config.model_contract),
            "output_contract": dict(config.output_contract),
        }
        drift = [name for name, value in expected.items() if row.get(name) != value]
        if drift:
            raise ValueError(
                f"{arm.arm_id} official VTC evidence drift: {', '.join(drift)}"
            )
        if row.get("comparison_scope") != "serving_mechanism_only":
            raise ValueError("official VTC evidence cannot enter DB-E2E ranking")
    return {
        "schema_version": 1,
        "comparison_scope": "serving_mechanism_only",
        "db_e2e_ranking_eligible": False,
        "attribution": "official_vtc_fcfs_to_official_vtc_is_vtc_only",
        "rows": [by_id[arm_id] for arm_id in OFFICIAL_VTC_ARM_IDS],
    }


def capability_fingerprint(config: OfficialVtcCapabilityConfig) -> str:
    payload = {
        **config.__dict__,
        "arms": [arm.__dict__ for arm in config.arms],
    }
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def _load_arm(value: object) -> OfficialVtcArm:
    if not isinstance(value, dict) or set(value) != {
        "arm_id", "scheduler_owner", "implementation_path",
    }:
        raise ValueError("official VTC arm schema is invalid")
    return OfficialVtcArm(
        arm_id=_string(value["arm_id"], "arm_id"),
        scheduler_owner=_string(value["scheduler_owner"], "scheduler_owner"),
        implementation_path=_string(value["implementation_path"], "implementation_path"),
    )


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _sha(value: object, name: str) -> str:
    text = _string(value, name)
    if len(text) not in {40, 64} or any(ch not in "0123456789abcdef" for ch in text):
        raise ValueError(f"{name} must be a lowercase SHA")
    return text


def _bool(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean")
    return value


def _two_shas(value: object) -> tuple[str, str]:
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("job_manifest_sha256 must contain two SHAs")
    return (_sha(value[0], "job0 SHA"), _sha(value[1], "job1 SHA"))


def _two_times(value: object) -> tuple[float, float]:
    if (
        not isinstance(value, list) or len(value) != 2
        or any(isinstance(item, bool) or not isinstance(item, (int, float)) for item in value)
    ):
        raise ValueError("job_release_times_s must contain two numeric values")
    return (float(value[0]), float(value[1]))


def _object_items(value: object, name: str) -> tuple[tuple[str, object], ...]:
    if not isinstance(value, dict) or not value:
        raise ValueError(f"{name} must be a non-empty object")
    return tuple(sorted(value.items()))
