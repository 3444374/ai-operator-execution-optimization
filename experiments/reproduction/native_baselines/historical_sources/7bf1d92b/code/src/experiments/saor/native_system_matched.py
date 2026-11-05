"""Fail-closed five-arm DB-E2E configuration and scheduling for SAOR."""

from __future__ import annotations

import hashlib
import csv
import json
import math
import random
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit, urlunsplit

from src.baselines.common.database_identity import (
    DatabaseIdentity,
    DatabaseIdentityError,
)
from src.baselines.common.manifests import read_manifest
from src.baselines.common.redact import redact_text
from src.infrastructure.runner_lease import acquire_host_runner_lease
from src.infrastructure.vllm_preflight import validate_service_identity
from src.experiments.saor.native_system_contract import (
    JobManifestIdentity,
    JobReleaseEpoch,
    MatchedArm,
    MatchedSystemConfig,
    MfuContract,
    ScheduledMatchedCell,
)
from src.experiments.saor.native_system_evidence import (
    atomic_json as seal_json,
    persisted_command,
    persisted_failure,
)
from src.experiments.saor.native_system_artifacts import (
    validate_archive_mirror,
    validate_completed_matrix_root,
)
from src.experiments.saor.native_system_parser import parse_matched_system_config


SYSTEM_ARM_IDS = (
    "daft_native", "daft_ray", "ray_data_http",
    "project_frozen_static", "project_bounded_ready_saor_0125we",
)
REQUIRED_ARM_IDS = SYSTEM_ARM_IDS
NOMINAL_JOB_OFFSET_S = 5.0
ACTUAL_LAUNCH_OFFSET_TOLERANCE_S = 0.25
FORMAL_AUTHORIZATION_SCOPE = "saor_native_system_matched_formal"

_PROJECT_FIELDS = {
    "k_per_endpoint", "work_limit_per_endpoint", "ready_bytes", "actor_topology",
    "batching_contract", "policy", "ready_observation", "debt_caps", "executor",
    "model_service_scheduler",
}
_COMMON_FIELDS = (
    "manifest_path", "manifest_sha256", "job_manifests", "endpoint_ids", "service_signature",
    "protocol", "output_cap", "job_release_schedule", "arrival_replay_capability",
    "job_internal_arrival_contract", "mfu_contract",
    "performance_writeback_mode", "unsupported_request_tails", "source", "organizer",
)

_CELL_EVIDENCE_FIELDS = {
    "implementation_source", "start_epoch_s", "end_epoch_s",
    "database_operator_e2e_s", "jobs", "service_metrics",
    "resource_metrics", "exactly_once", "request_tail_status",
    "service_fairness_metrics", "output_paths", "status",
    "server_version", "pgvector_version", "mfu_contract",
    "completion_evidence",
}
_NATIVE_PROVENANCE_FIELDS = {
    "upstream_url", "upstream_version", "upstream_commit",
    "adapter_path", "adapter_sha256", "upstream_source_modified",
    "adapter_diff_status",
}
_PERSISTED_JOB_FIELDS = (
    "job_id", "scheduled_launch_epoch_s", "actual_launch_epoch_s",
    "source_arrival_epoch_s", "ended_epoch_s", "completed_count",
    "expected_count", "actual_work", "manifest_sha256", "exactly_once",
    "shard_provenance", "concrete_ready_epoch_s",
    "credit_registered_epoch_s", "first_submit_epoch_s",
    "first_batch_ready_epoch_s", "result_visible_epoch_s",
    "t0_job_release_epoch_s", "t1_first_batch_epoch_s",
    "t2_first_request_epoch_s", "t3_last_request_completion_epoch_s",
    "t4_result_visible_epoch_s", "jct_s", "source_s", "execution_s",
    "service_span_s", "role", "weight", "request_count",
    "actual_prompt_tokens", "actual_output_tokens", "actual_total_tokens",
    "request_slo_s", "job_jct_slo_s", "job_jct_slo_status",
    "job_jct_slo_violation", "request_p50_s", "request_p95_s",
    "request_p99_status", "request_p99_s", "slo_status",
    "slo_violation_ratio", "tail_reason",
)


def endpoint_auxiliary_url(endpoint_url: str, path: str) -> str:
    """Map a completion URL to the same service origin at ``path``."""

    parsed = urlsplit(endpoint_url)
    if not parsed.scheme or not parsed.netloc or not path.startswith("/"):
        raise ValueError("endpoint URL or auxiliary path is invalid")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _validate_actual_job_offset(actual_offset_s: float) -> float:
    """Validate observed launcher timing without claiming zero jitter."""

    deviation_s = actual_offset_s - NOMINAL_JOB_OFFSET_S
    if abs(deviation_s) > ACTUAL_LAUNCH_OFFSET_TOLERANCE_S:
        raise RuntimeError(
            "actual Job launch offset is outside the pre-registered "
            f"{ACTUAL_LAUNCH_OFFSET_TOLERANCE_S:.2f}s tolerance"
        )
    return deviation_s


def validate_release_gated_events(
    arm: MatchedArm,
    jobs: list[dict[str, object]],
) -> None:
    """Reject SAOR observation, credit registration, or submit before Job release."""

    if arm.arm_id != "project_bounded_ready_saor_0125we":
        return
    releases = {
        item.job_id: item.release_time_s for item in arm.job_release_schedule
    }
    for job in jobs:
        job_id = str(job.get("job_id", ""))
        scheduled = float(job.get("scheduled_launch_epoch_s", 0.0))
        if job_id not in releases:
            raise RuntimeError("SAOR release evidence contains an unknown Job")
        origin = scheduled - releases[job_id]
        release_epoch_s = origin + releases[job_id]
        for field in (
            "concrete_ready_epoch_s",
            "credit_registered_epoch_s",
            "first_submit_epoch_s",
        ):
            value = job.get(field)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise RuntimeError(f"SAOR {field} release evidence is missing")
            if float(value) + 1e-9 < release_epoch_s:
                raise RuntimeError(
                    f"SAOR {field} occurred before external Job release"
                )


def normalize_request_tail_status(value: object) -> dict[str, dict[str, object]]:
    """Convert legacy flat unavailable tails or validate the neutral nested schema."""

    def thaw(item: object) -> object:
        if isinstance(item, tuple):
            if all(
                isinstance(entry, tuple) and len(entry) == 2
                and isinstance(entry[0], str)
                for entry in item
            ):
                return {str(key): thaw(child) for key, child in item}
            return [thaw(child) for child in item]
        if isinstance(item, list):
            return [thaw(child) for child in item]
        if isinstance(item, dict):
            return {str(key): thaw(child) for key, child in item.items()}
        return item

    decoded = thaw(value)
    if not isinstance(decoded, dict):
        raise RuntimeError("request-tail contract must be an object")
    if set(decoded) == {"status", "reason"}:
        if decoded["status"] != "unavailable" or not str(decoded["reason"]):
            raise RuntimeError("legacy request-tail contract must be unavailable")
        decoded = {
            metric: {
                "status": "unavailable", "value": "unavailable",
                "reason": str(decoded["reason"]),
            }
            for metric in ("request_p99", "slo")
        }
    if set(decoded) != {"request_p99", "slo"}:
        raise RuntimeError("request-tail contract must contain request_p99 and slo")
    output: dict[str, dict[str, object]] = {}
    for metric in ("request_p99", "slo"):
        entry = decoded[metric]
        if not isinstance(entry, dict) or set(entry) != {"status", "value", "reason"}:
            raise RuntimeError(f"request-tail {metric} must contain status/value/reason")
        status = str(entry["status"])
        reason = str(entry["reason"])
        metric_value = entry["value"]
        if status == "unavailable":
            if metric_value != "unavailable" or not reason:
                raise RuntimeError(f"request-tail {metric} unavailable evidence is invalid")
        elif status == "available":
            if isinstance(metric_value, bool) or not isinstance(metric_value, (int, float)):
                raise RuntimeError(f"request-tail {metric} available value must be numeric")
        else:
            raise RuntimeError(f"request-tail {metric} status is invalid")
        output[metric] = {
            "status": status, "value": metric_value, "reason": reason,
        }
    return output


def load_matched_system_config(
    path: Path, *, allow_existing_matrix_output_root: bool = False
) -> MatchedSystemConfig:
    """Load a portable config and reject every mismatch before execution."""

    config = parse_matched_system_config(
        path,
        arm_loader=_load_arm,
        path_resolver=_resolve_config_path,
        integer=_integer,
        nonnegative=_nonnegative,
        string=_string,
        boolean=_boolean,
        mapping=_mapping,
    )
    errors = _validation_errors(
        config,
        check_matrix_output_root=not allow_existing_matrix_output_root,
    )
    if errors:
        raise ValueError("; ".join(errors))
    return config


def balanced_matched_schedule(
    config: MatchedSystemConfig, *, phase: str, repeat: int
) -> tuple[ScheduledMatchedCell, ...]:
    """Seed-shuffle then rotate physical arms, preserving the shared SAOR cell."""

    if phase not in {"warmup", "formal"}:
        raise ValueError("unsupported schedule phase")
    if repeat < 1:
        raise ValueError("repeat must be positive")
    phase_arm_ids = {"warmup": REQUIRED_ARM_IDS, "formal": SYSTEM_ARM_IDS}
    arm_ids = list(phase_arm_ids[phase])
    random.Random(f"{config.seed}:{phase}").shuffle(arm_ids)
    rotation = (repeat - 1) % len(arm_ids)
    ordered = arm_ids[rotation:] + arm_ids[:rotation]
    return tuple(
        ScheduledMatchedCell(
            phase=phase,
            repeat=repeat,
            order_index=index,
            arm_id=arm_id,
            report_blocks=("db_e2e_system",),
        )
        for index, arm_id in enumerate(ordered)
    )


def audit_matched_system_config(
    config: MatchedSystemConfig, *, check_output_roots: bool = True
) -> dict[str, object]:
    """Return a read-only readiness record without starting any external system."""

    errors = _validation_errors(
        config, check_matrix_output_root=check_output_roots
    )
    if not check_output_roots:
        errors = [
            error for error in errors
            if not error.endswith("output_root already exists")
        ]
    manifests = {
        arm.arm_id: {
            "path": arm.manifest_path,
            "sha256": arm.manifest_sha256,
            "jobs": [job.__dict__ for job in arm.job_manifests],
        }
        for arm in config.arms
    }
    return {
        "schema_version": 1,
        "status": "passed" if not errors else "failed",
        "errors": errors,
        "resolved_arm_identities": [arm.arm_id for arm in config.arms],
        "report_blocks": {"db_e2e_system": list(SYSTEM_ARM_IDS)},
        "immutable_manifest_hashes": manifests,
        "service_signature": (
            dict(config.arms[0].service_signature) if config.arms else {}
        ),
        "calibration_paths": {
            arm.arm_id: arm.calibration_path for arm in config.arms
        },
        "calibration_sha256": {
            arm.arm_id: arm.calibration_sha256 for arm in config.arms
        },
        "matrix_output_root": config.matrix_output_root,
        "gpu_formal_locally_authorized": config.gpu_formal_locally_authorized,
        "planned_schedule": [
            {
                "phase": phase,
                "repeat": repeat,
                "cells": [
                    cell.__dict__
                    for cell in balanced_matched_schedule(
                        config, phase=phase, repeat=repeat
                    )
                ],
            }
            for phase, count in (
                ("warmup", config.warmup_repeats),
                ("formal", config.formal_repeats),
            )
            for repeat in range(1, count + 1)
        ],
    }


def _atomic_json(path: Path, payload: object) -> None:
    seal_json(path, payload)


def sha256_file(path: Path) -> str:
    """Return the immutable SHA-256 identity of one artifact."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_executor_job_manifests(
    arm: MatchedArm,
    job_paths: tuple[str | Path, ...],
    row_counts: tuple[int, ...],
) -> None:
    """Bind two executor Jobs to the frozen split and combined workload."""

    if len(job_paths) != len(arm.job_manifests):
        raise ValueError("executor must define exactly the frozen two Job manifests")
    if row_counts != tuple(job.rows for job in arm.job_manifests):
        raise ValueError("executor rows_per_jobs drift from frozen Job row counts")
    combined_requests = []
    for expected, actual_path in zip(arm.job_manifests, job_paths, strict=True):
        path = Path(actual_path)
        if not path.is_file():
            raise ValueError(f"executor {expected.job_id} manifest is missing")
        if sha256_file(path) != expected.sha256:
            raise ValueError(f"executor {expected.job_id} manifest SHA-256 drift")
        requests = read_manifest(path)
        if len(requests) != expected.rows:
            raise ValueError(f"executor {expected.job_id} manifest row-count drift")
        combined_requests.extend(requests)
    if tuple(combined_requests) != read_manifest(arm.manifest_path):
        raise ValueError(
            "executor Job manifests do not equal the authoritative matched "
            "combined manifest in Job order"
        )


def validate_native_calibration_selection(
    arm: MatchedArm,
    *,
    adapter: str,
    concurrency_per_endpoint: int,
    batch_size: int,
) -> None:
    """Bind a native selection artifact to the actual framework dispatch."""

    selection = native_calibration_selection(arm)
    expected = {
        "adapter": adapter,
        "concurrency_per_endpoint": concurrency_per_endpoint,
        "batch_size": batch_size,
    }
    drift = [
        name for name, value in expected.items()
        if selection.get(name) != value
    ]
    if drift:
        raise ValueError(
            f"{arm.arm_id} calibration selection drift: {', '.join(drift)}"
        )


def native_calibration_selection(arm: MatchedArm) -> dict[str, object]:
    """Return the evidence-bound native adapter/concurrency/batch identity."""

    path = Path(arm.calibration_path)
    if sha256_file(path) != arm.calibration_sha256:
        raise ValueError(f"{arm.arm_id} calibration SHA-256 drift")
    payload = json.loads(path.read_text(encoding="utf-8"))
    selection = payload.get("selection")
    evidence = payload.get("evidence")
    if not isinstance(selection, dict) or not isinstance(evidence, dict):
        raise ValueError(f"{arm.arm_id} calibration schema is invalid")
    required = {"adapter", "concurrency_per_endpoint", "batch_size"}
    if not required.issubset(selection):
        raise ValueError(f"{arm.arm_id} calibration selection schema is invalid")
    if set(evidence) != {"configuration_identity", "performance_selection"}:
        raise ValueError(f"{arm.arm_id} calibration evidence roles are invalid")
    identity = evidence["configuration_identity"]
    performance = evidence["performance_selection"]
    if not isinstance(identity, dict) or identity.get("status") != "verified":
        raise ValueError(f"{arm.arm_id} calibration identity is unverified")
    allowed_status = (
        {"development_screen_only"}
        if selection["adapter"] == "ray_data_http"
        else {"not_applicable"}
    )
    if (
        not isinstance(performance, dict)
        or performance.get("status") not in allowed_status
        or not str(performance.get("reason", ""))
    ):
        raise ValueError(f"{arm.arm_id} calibration evidence strength is invalid")
    return {name: selection[name] for name in sorted(required)}


def relativize_matrix_evidence_path(
    matrix_root: Path,
    artifact_path: str | Path,
    evidence_name: str,
) -> tuple[Path, str]:
    """Validate a live artifact is inside the matrix root and store it relatively."""

    root = matrix_root.resolve()
    raw_path = Path(artifact_path)
    resolved = (
        raw_path.resolve()
        if raw_path.is_absolute()
        else (root / raw_path).resolve()
    )
    try:
        relative = resolved.relative_to(root)
    except ValueError as error:
        raise RuntimeError(
            f"{evidence_name} escapes the matrix evidence root"
        ) from error
    if not resolved.exists() or (resolved.is_file() and resolved.stat().st_size <= 0):
        raise RuntimeError(f"{evidence_name} is missing or empty")
    return resolved, relative.as_posix()


def resolve_matrix_evidence_path(
    matrix_root: Path,
    stored_path: object,
    evidence_name: str,
) -> Path:
    """Resolve one stored root-relative artifact without allowing path escape."""

    relative = Path(str(stored_path))
    if relative.is_absolute() or not relative.parts:
        raise ValueError(f"{evidence_name} must be matrix-root relative")
    root = matrix_root.resolve()
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ValueError(f"{evidence_name} escapes the matrix evidence root") from error
    if not resolved.exists() or (resolved.is_file() and resolved.stat().st_size <= 0):
        raise ValueError(f"{evidence_name} is missing or empty")
    return resolved


def resolved_matched_system_identity(
    config: MatchedSystemConfig,
) -> dict[str, object]:
    """Build a secret-safe resolved identity for authorization and replay."""

    arms = []
    for arm in config.arms:
        source = dict(arm.source)
        database_url = str(source.pop("database_url", ""))
        source["database_url_sha256"] = hashlib.sha256(
            database_url.encode("utf-8")
        ).hexdigest()
        arms.append(
            {
                "arm_id": arm.arm_id,
                "kind": arm.kind,
                "scheduler_owner": arm.scheduler_owner,
                "manifest_path": arm.manifest_path,
                "manifest_sha256": arm.manifest_sha256,
                "job_manifests": [job.__dict__ for job in arm.job_manifests],
                "endpoint_ids": list(arm.endpoint_ids),
                "service_signature": dict(arm.service_signature),
                "protocol": arm.protocol,
                "output_cap": arm.output_cap,
                "job_release_schedule": [item.__dict__ for item in arm.job_release_schedule],
                "arrival_replay_capability": arm.arrival_replay_capability,
                "job_internal_arrival_contract": (
                    arm.job_internal_arrival_contract
                ),
                "performance_writeback_mode": arm.performance_writeback_mode,
                "source": source,
                "organizer": arm.organizer,
                "calibration_path": arm.calibration_path,
                "calibration_sha256": arm.calibration_sha256,
                "mfu_contract": arm.mfu_contract.__dict__,
                "executor_selection": (
                    native_calibration_selection(arm)
                    if arm.kind == "native" else None
                ),
                "project_contract": dict(arm.project_contract),
            }
        )
    metrics_urls = [
        endpoint_auxiliary_url(url, "/metrics") for url in config.endpoint_urls
    ]
    return {
        "schema_version": 1,
        "seed": config.seed,
        "warmup_repeats": config.warmup_repeats,
        "formal_repeats": config.formal_repeats,
        "matched_manifest_status": config.matched_manifest_status,
        "service_identity": dict(config.service_identity),
        "endpoint_urls": list(config.endpoint_urls),
        "metrics_urls": metrics_urls,
        "health_url": endpoint_auxiliary_url(config.endpoint_urls[0], "/health"),
        "job_observation_contracts": [
            item.__dict__ for item in config.job_observation_contracts
        ],
        "observation_gateway": {
            "mode": "pass_through_no_queue_no_retry",
            "request_timeout_s": config.observation_gateway_request_timeout_s,
        },
        "arms": arms,
    }


def sha256_payload(payload: object) -> str:
    """Return the canonical JSON SHA-256 identity of a resolved contract."""

    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def run_identity_requirements(
    config_path: Path,
    config: MatchedSystemConfig,
    repository_commit: str,
    native_config_path: Path | None = None,
    project_config_path: Path | None = None,
) -> dict[str, object]:
    """Return the immutable config/workload identity shared by every mode."""

    if not repository_commit:
        raise ValueError("repository commit must be non-empty")
    manifests = {arm.manifest_sha256 for arm in config.arms}
    if len(manifests) != 1:
        raise ValueError("formal authorization requires one frozen manifest SHA")
    job_manifests = {
        tuple((job.job_id, job.rows, job.sha256) for job in arm.job_manifests)
        for arm in config.arms
    }
    if len(job_manifests) != 1:
        raise ValueError("formal authorization requires one frozen two-Job split")
    mfu_contracts = {
        json.dumps(arm.mfu_contract.__dict__, sort_keys=True)
        for arm in config.arms
    }
    if len(mfu_contracts) != 1:
        raise ValueError("formal authorization requires one frozen MFU contract")
    identity = {
        "schema_version": 1,
        "repository_commit": repository_commit,
        "config_sha256": sha256_file(config_path),
        "resolved_config_sha256": sha256_payload(
            resolved_matched_system_identity(config)
        ),
        "manifest_sha256": next(iter(manifests)),
        "job_manifests": [
            {"job_id": job_id, "rows": rows, "sha256": sha256}
            for job_id, rows, sha256 in next(iter(job_manifests))
        ],
        "mfu_contract": config.arms[0].mfu_contract.__dict__,
    }
    if (native_config_path is None) != (project_config_path is None):
        raise ValueError("native and Project config paths must be supplied together")
    if native_config_path is not None and project_config_path is not None:
        identity.update({
            "native_config_sha256": sha256_file(native_config_path),
            "project_config_sha256": sha256_file(project_config_path),
        })
    return identity


def build_rehearsal_validation_payload(
    config_path: Path,
    config: MatchedSystemConfig,
    repository_commit: str,
    rehearsal_root: Path,
    rehearsal_archive: Path,
    native_config_path: Path,
    project_config_path: Path,
    expected_native_provenance: dict[str, dict[str, object]],
) -> dict[str, object]:
    """Deep-check a completed five-arm rehearsal and derive its sealed identity."""

    expected = run_identity_requirements(
        config_path, config, repository_commit,
        native_config_path, project_config_path,
    )
    root = rehearsal_root.resolve()
    index_path = root / "matrix_index.json"
    if not root.is_dir() or not index_path.is_file():
        raise RuntimeError("rehearsal root or matrix index is missing")
    index = validate_completed_matrix_root(
        root, config, expected, execution_mode="rehearsal",
        expected_native_provenance=expected_native_provenance,
    )
    validate_archive_mirror(root, rehearsal_archive)
    cells = index["cells"]
    matrix_instance_id = index.get("matrix_instance_id")
    if (
        not isinstance(matrix_instance_id, str)
        or len(matrix_instance_id) != 32
        or any(character not in "0123456789abcdef" for character in matrix_instance_id)
    ):
        raise RuntimeError("rehearsal matrix instance identity is invalid")
    return {
        "schema_version": 1,
        "status": "passed",
        "scope": "saor_five_arm_rehearsal_validation",
        "valid_rehearsal": True,
        "repository_commit": repository_commit,
        "config_sha256": expected["config_sha256"],
        "resolved_config_sha256": expected["resolved_config_sha256"],
        "native_config_sha256": expected["native_config_sha256"],
        "project_config_sha256": expected["project_config_sha256"],
        "rehearsal_root": str(root),
        "root_id": root.name,
        "matrix_instance_id": matrix_instance_id,
        "matrix_index_sha256": sha256_file(index_path),
        "archive_sha256": sha256_file(rehearsal_archive),
        "arm_ids": list(SYSTEM_ARM_IDS),
        "completed_cells": len(cells),
        "exactly_once": True,
    }


def validate_rehearsal_evidence(
    config_path: Path,
    config: MatchedSystemConfig,
    repository_commit: str,
    rehearsal_validation_path: Path | None,
    rehearsal_root: Path | None,
    rehearsal_archive: Path | None,
    native_config_path: Path | None,
    project_config_path: Path | None,
    expected_native_provenance: dict[str, dict[str, object]] | None,
) -> dict[str, object]:
    """Bind formal eligibility to actual reviewed rehearsal artifacts."""

    if (
        rehearsal_validation_path is None or rehearsal_root is None
        or rehearsal_archive is None or native_config_path is None
        or project_config_path is None or expected_native_provenance is None
    ):
        raise PermissionError(
            "formal authorization requires rehearsal validation/root/archive, "
            "native/Project configs, and frozen native provenance"
        )
    expected = build_rehearsal_validation_payload(
        config_path, config, repository_commit, rehearsal_root, rehearsal_archive,
        native_config_path, project_config_path, expected_native_provenance,
    )
    artifact = json.loads(rehearsal_validation_path.read_text(encoding="utf-8"))
    if not isinstance(artifact, dict) or artifact != expected:
        raise PermissionError("rehearsal validation artifact identity drifted")
    return {**expected, "validation_sha256": sha256_file(rehearsal_validation_path)}


def formal_authorization_requirements(
    config_path: Path,
    config: MatchedSystemConfig,
    repository_commit: str,
    rehearsal_evidence: dict[str, object],
) -> dict[str, object]:
    """Return the exact fields an independent formal authorization must bind."""

    return {
        **run_identity_requirements(config_path, config, repository_commit),
        "native_config_sha256": rehearsal_evidence["native_config_sha256"],
        "project_config_sha256": rehearsal_evidence["project_config_sha256"],
        "status": "authorized",
        "scope": FORMAL_AUTHORIZATION_SCOPE,
        "formal_authorized": True,
        "rehearsal_evidence": rehearsal_evidence,
    }


def validate_formal_authorization(
    authorization_path: Path | None,
    requirements: dict[str, object],
) -> str:
    """Fail closed unless a separate artifact exactly matches the frozen run."""

    if authorization_path is None:
        raise PermissionError("formal authorization artifact is required")
    if not authorization_path.is_file():
        raise PermissionError("formal authorization artifact is missing")
    try:
        artifact = json.loads(authorization_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PermissionError("formal authorization artifact is invalid") from error
    if not isinstance(artifact, dict) or set(artifact) != set(requirements):
        raise PermissionError("formal authorization schema is invalid")
    drift = [
        field
        for field, expected in requirements.items()
        if artifact.get(field) != expected
    ]
    if drift:
        raise PermissionError(
            "formal authorization identity drift: " + ", ".join(drift)
        )
    return sha256_file(authorization_path)


def _repository_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parents[4],
    ).stdout.strip()


def _all_cells(
    config: MatchedSystemConfig, *, rehearsal: bool = False
) -> tuple[ScheduledMatchedCell, ...]:
    if rehearsal:
        return balanced_matched_schedule(config, phase="warmup", repeat=1)
    return tuple(
        cell
        for phase, count in (
            ("warmup", config.warmup_repeats),
            ("formal", config.formal_repeats),
        )
        for repeat in range(1, count + 1)
        for cell in balanced_matched_schedule(config, phase=phase, repeat=repeat)
    )


def _validate_cell_header(
    arm: MatchedArm,
    evidence: dict[str, object],
) -> tuple[float, float, float]:
    missing = sorted(_CELL_EVIDENCE_FIELDS - evidence.keys())
    if missing:
        raise RuntimeError(f"cell evidence missing required fields: {missing}")
    if evidence["status"] != "passed":
        raise RuntimeError("executor returned non-passing cell evidence")
    if arm.kind == "native":
        provenance = evidence.get("native_implementation_provenance")
        if (
            not isinstance(provenance, dict)
            or set(provenance) != _NATIVE_PROVENANCE_FIELDS
            or provenance.get("upstream_source_modified") is not False
            or provenance.get("adapter_diff_status")
            != "thin_adapter_only_no_upstream_patch"
        ):
            raise RuntimeError("native implementation provenance is incomplete")
    if evidence["exactly_once"] is not True:
        raise RuntimeError("cell exactly-once evidence failed")
    try:
        DatabaseIdentity.from_record(evidence, "cell")
    except DatabaseIdentityError as error:
        raise RuntimeError(str(error)) from error
    start = evidence["start_epoch_s"]
    end = evidence["end_epoch_s"]
    boundary = evidence["database_operator_e2e_s"]
    if not all(isinstance(item, (int, float)) for item in (start, end, boundary)):
        raise RuntimeError("cell common timing boundary is invalid")
    if float(end) < float(start) or float(boundary) < 0:
        raise RuntimeError("cell common timing boundary is inconsistent")
    return float(start), float(end), float(boundary)


def _validate_job_tail(job: dict[str, object]) -> None:
    p50 = job.get("request_p50_s")
    p95 = job.get("request_p95_s")
    p99 = job.get("request_p99_s")
    slo = job.get("slo_violation_ratio")
    if (
        job.get("request_p99_status") != "available"
        or job.get("slo_status") != "available"
        or not isinstance(p50, (int, float))
        or isinstance(p50, bool)
        or float(p50) < 0
        or not isinstance(p95, (int, float))
        or isinstance(p95, bool)
        or float(p95) < float(p50)
        or not isinstance(p99, (int, float))
        or isinstance(p99, bool)
        or float(p99) < float(p95)
        or not isinstance(slo, (int, float))
        or isinstance(slo, bool)
        or not 0 <= float(slo) <= 1
    ):
        raise RuntimeError("gateway per-Job tail/SLO evidence is invalid")


def _validate_jobs(
    arm: MatchedArm,
    evidence: dict[str, object],
) -> tuple[list[dict[str, object]], float, float]:
    jobs = evidence["jobs"]
    if not isinstance(jobs, list) or len(jobs) != 2:
        raise RuntimeError("cell must retain both Job evidence blocks")
    for job in jobs:
        if not isinstance(job, dict) or job.get("exactly_once") is not True:
            raise RuntimeError("Job exactly-once evidence is incomplete")
        if not isinstance(job.get("completed_count"), int):
            raise RuntimeError("Job counter evidence is incomplete")
        if (
            not isinstance(job.get("expected_count"), int)
            or job["completed_count"] != job["expected_count"]
            or job["completed_count"] <= 0
        ):
            raise RuntimeError("Job completed/expected row counts do not match")
        provenance = job.get("shard_provenance")
        if not isinstance(provenance, list) or not provenance:
            raise RuntimeError("Job source/provenance evidence is incomplete")
        for shard in provenance:
            if not isinstance(shard, dict) or any(
                shard.get(name) != expected
                for name, expected in {
                    "source_kind": "timed_postgres_manifest",
                    "source_timing_boundary": "inside_job_barrier",
                    "source_validation_status": "ok",
                }.items()
            ):
                raise RuntimeError("timed PostgreSQL source evidence is invalid")
        _validate_job_tail(job)
    for job, expected_job in zip(jobs, arm.job_manifests, strict=True):
        if (
            job.get("job_id") != expected_job.job_id
            or job.get("manifest_sha256") != expected_job.sha256
            or job.get("expected_count") != expected_job.rows
            or job.get("completed_count") != expected_job.rows
        ):
            raise RuntimeError("Job identity or frozen row boundary drifted")
    validate_release_gated_events(arm, jobs)
    if evidence["mfu_contract"] != arm.mfu_contract.__dict__:
        raise RuntimeError("cell MFU peak/precision contract drifted")
    starts = [float(job["actual_launch_epoch_s"]) for job in jobs]
    actual_offset_s = starts[1] - starts[0]
    offset_deviation_s = _validate_actual_job_offset(actual_offset_s)
    if float(jobs[0]["ended_epoch_s"]) <= starts[1]:
        raise RuntimeError("Job overlap evidence is missing")
    return jobs, actual_offset_s, offset_deviation_s


def _validate_service_metrics(
    evidence: dict[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    service = evidence["service_metrics"]
    resource = evidence["resource_metrics"]
    if not isinstance(service, dict) or service.get("metrics_status") != "ok":
        raise RuntimeError("service counter evidence is incomplete")
    prompt_delta = service.get("prompt_tokens_delta")
    generation_delta = service.get("generation_tokens_delta")
    if (
        not isinstance(prompt_delta, (int, float))
        or not isinstance(generation_delta, (int, float))
        or prompt_delta < 0
        or generation_delta < 0
        or prompt_delta + generation_delta <= 0
    ):
        raise RuntimeError("service token counter evidence is invalid")
    if not isinstance(resource, dict) or resource.get("resource_metrics_status") != "ok":
        raise RuntimeError("resource evidence is incomplete")
    return service, resource


def _validate_resource_trace(
    matrix_output_root: Path,
    resource: dict[str, object],
    *,
    start: float,
    end: float,
    boundary: float,
) -> str:
    resource_path, relative_path = relativize_matrix_evidence_path(
        matrix_output_root,
        str(resource.get("path", "")),
        "resource trace artifact",
    )
    if not resource_path.is_file():
        raise RuntimeError("resource trace artifact must be a file")
    with resource_path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise RuntimeError("resource trace has no samples")
    observed = [
        float(row["observed_epoch_s"])
        for row in rows
        if row.get("observed_epoch_s") not in (None, "")
    ]
    relative = [
        float(row["sample_epoch_s"])
        for row in rows
        if row.get("sample_epoch_s") not in (None, "")
    ]
    if observed and not any(start <= item <= end for item in observed):
        raise RuntimeError("resource trace has no in-boundary sample")
    if relative and not any(0.0 <= item <= boundary for item in relative):
        raise RuntimeError("resource trace has no in-boundary relative sample")
    if not observed and not relative:
        raise RuntimeError("resource trace has no timestamped samples")
    return relative_path


def _validate_output_artifacts(
    matrix_output_root: Path,
    output_paths: object,
) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    if not isinstance(output_paths, dict) or not output_paths:
        raise RuntimeError("output artifact evidence is incomplete")
    relative_paths: dict[str, str] = {}
    identities: dict[str, dict[str, str]] = {}
    for name, value in output_paths.items():
        artifact, relative_path = relativize_matrix_evidence_path(
            matrix_output_root,
            str(value),
            f"output artifact {name}",
        )
        if not artifact.is_file():
            raise RuntimeError(f"output artifact {name} must be a file")
        relative_paths[str(name)] = relative_path
        identities[str(name)] = {
            "path": relative_path,
            "sha256": sha256_file(artifact),
        }
    return relative_paths, identities


def _validate_observation_evidence(
    arm: MatchedArm,
    evidence: dict[str, object],
    runtime_identity: dict[str, object],
    artifact_identities: dict[str, dict[str, str]],
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    gateway = evidence.get("observation_gateway")
    system_observation = evidence.get("system_observation")
    if not isinstance(gateway, dict) or not isinstance(system_observation, dict):
        raise RuntimeError("common observation gateway evidence is missing")
    request_tail = evidence["request_tail_status"]
    if not isinstance(request_tail, dict) or any(
        not isinstance(request_tail.get(metric), dict)
        or request_tail[metric].get("status") != "available"
        or not isinstance(request_tail[metric].get("value"), (int, float))
        for metric in ("request_p99", "slo")
    ):
        raise RuntimeError("gateway-observed request-tail evidence is invalid")
    fairness = evidence["service_fairness_metrics"]
    expected_fairness_fields = {
        "starvation_status", "longest_no_service_s",
        "completion_service_lag_status", "completion_service_lag_p95_work",
        "completion_service_lag_max_work", "reason",
    }
    if not isinstance(fairness, dict) or set(fairness) != expected_fairness_fields:
        raise RuntimeError("service fairness evidence schema is invalid")
    if (
        str(fairness["starvation_status"]).startswith("unavailable")
        or str(fairness["completion_service_lag_status"]).startswith("unavailable")
        or not fairness["reason"]
    ):
        raise RuntimeError("gateway-observed service fairness evidence is unavailable")
    if (
        gateway.get("status") != "passed"
        or gateway.get("mode") != "pass_through_no_queue_no_retry"
        or not isinstance(gateway.get("trace_sha256"), str)
        or len(str(gateway["trace_sha256"])) != 64
        or not isinstance(gateway.get("integrity"), dict)
        or gateway["integrity"].get("retry_count") != 0
        or gateway["integrity"].get("body_identity_passed") is not True
    ):
        raise RuntimeError("observation gateway integrity evidence failed")
    routes = gateway.get("routes")
    runtime_endpoints = runtime_identity.get("endpoint_urls")
    expected_routes = {
        (job.job_id, endpoint_id, str(runtime_endpoints[index]))
        for job in arm.job_manifests
        for index, endpoint_id in enumerate(arm.endpoint_ids)
    } if isinstance(runtime_endpoints, list) else set()
    observed_routes = {
        (
            str(route.get("job_id", "")),
            str(route.get("endpoint_id", "")),
            str(route.get("upstream_url", "")),
        )
        for route in routes
        if isinstance(route, dict)
    } if isinstance(routes, list) else set()
    if observed_routes != expected_routes:
        raise RuntimeError("observation gateway route binding drifted")
    gateway_artifact = artifact_identities.get("observation_gateway_trace")
    if (
        not isinstance(gateway_artifact, dict)
        or gateway.get("trace_sha256") != gateway_artifact.get("sha256")
    ):
        raise RuntimeError("observation gateway trace SHA-256 drifted")
    if (
        system_observation.get("status") != "passed"
        or system_observation.get("timed_boundary")
        != "job_release_before_postgres_to_validated_result_visibility"
        or float(system_observation.get("group_jct_s", -1.0))
        != float(evidence["database_operator_e2e_s"])
        or not isinstance(system_observation.get("jobs"), dict)
        or set(system_observation["jobs"])
        != {job.job_id for job in arm.job_manifests}
    ):
        raise RuntimeError("T0-T4 system observation evidence failed")
    return gateway, system_observation, fairness


def _validate_completion_evidence(
    arm: MatchedArm,
    evidence: dict[str, object],
) -> dict[str, object]:
    completion = evidence["completion_evidence"]
    expected_fields = {
        "status", "mode", "producer", "expected_rows", "observed_rows",
        "expected_doc_id_digest", "observed_doc_id_digest", "output_digest",
        "exactly_once", "verified_epoch_s",
    }
    if not isinstance(completion, dict) or set(completion) != expected_fields:
        raise RuntimeError("completion evidence schema is invalid")
    expected_producer = (
        "native_official_adapter" if arm.kind == "native" else "project_profiler"
    )
    if (
        completion["status"] != "passed"
        or completion["mode"] != "completion_trace_digest"
        or completion["producer"] != expected_producer
        or completion["exactly_once"] is not True
        or completion["expected_rows"] != sum(job.rows for job in arm.job_manifests)
        or completion["observed_rows"] != completion["expected_rows"]
        or completion["observed_doc_id_digest"]
        != completion["expected_doc_id_digest"]
        or not isinstance(completion["output_digest"], str)
        or len(completion["output_digest"]) != 64
        or not isinstance(completion["verified_epoch_s"], (int, float))
    ):
        raise RuntimeError("completion trace correctness evidence failed")
    return completion


def _validate_executor_final_state(
    arm: MatchedArm,
    cell: ScheduledMatchedCell,
    evidence: dict[str, object],
) -> list[object]:
    try:
        if arm.kind == "native":
            evidence["queue_final"] = validate_native_final_queue(
                evidence.get("queue_final"),
                f"{cell.phase}-{cell.repeat}-{arm.arm_id}",
            )
        else:
            evidence["shared_credit_final"] = validate_project_final_credit(
                evidence.get("shared_credit_final"),
                f"{cell.phase}-{cell.repeat}-{arm.arm_id}",
                frozen_static=arm.arm_id == "project_frozen_static",
            )
    except ValueError as error:
        raise RuntimeError(str(error)) from error
    command = evidence.get("command", [])
    if arm.kind == "native" and any(
        token in " ".join(str(item) for item in command).lower()
        for token in (
            "credit", "coordinator", "router", "bounded-ready",
            "max-active-work",
        )
    ):
        raise RuntimeError("native dispatch contains Project flags")
    return list(command)


def _raw_cell_artifact_manifest(
    matrix_output_root: Path,
    cell_output_dir: Path,
) -> tuple[str, dict[str, str]]:
    cell_relative_root = cell_output_dir.resolve().relative_to(
        matrix_output_root.resolve()
    ).as_posix()
    raw_paths = sorted(cell_output_dir.rglob("*"))
    if any(path.is_symlink() for path in raw_paths):
        raise RuntimeError("cell raw artifact tree must not contain symlinks")
    manifest = {
        path.relative_to(matrix_output_root).as_posix(): sha256_file(path)
        for path in raw_paths if path.is_file()
    }
    if not manifest:
        raise RuntimeError("cell raw artifact manifest is empty")
    return cell_relative_root, manifest


def _curated_cell_evidence(
    evidence: dict[str, object],
    *,
    jobs: list[dict[str, object]],
    service: dict[str, object],
    resource: dict[str, object],
    resource_relative_path: str,
    relative_output_paths: dict[str, str],
    artifact_identities: dict[str, dict[str, str]],
    gateway: dict[str, object],
    system_observation: dict[str, object],
    fairness: dict[str, object],
    completion: dict[str, object],
    cell_relative_root: str,
    raw_artifact_manifest: dict[str, str],
    command: list[object],
) -> dict[str, object]:
    persisted_resource = {
        field: resource[field]
        for field in (
            "resource_metrics_status", "gpu_summary", "gauge_summary",
        )
        if field in resource
    }
    persisted_resource["path"] = resource_relative_path
    persisted: dict[str, object] = {
        "implementation_source": evidence["implementation_source"],
        "start_epoch_s": evidence["start_epoch_s"],
        "end_epoch_s": evidence["end_epoch_s"],
        "database_operator_e2e_s": evidence["database_operator_e2e_s"],
        "correct_throughput_tokens_per_s": evidence[
            "correct_throughput_tokens_per_s"
        ],
        "jobs": [
            {field: job[field] for field in _PERSISTED_JOB_FIELDS if field in job}
            for job in jobs
        ],
        "service_metrics": {
            field: service[field]
            for field in (
                "metrics_status", "prompt_tokens_delta",
                "generation_tokens_delta", "request_success_delta",
            )
            if field in service
        },
        "resource_metrics": persisted_resource,
        "exactly_once": evidence["exactly_once"],
        "request_tail_status": evidence["request_tail_status"],
        "service_fairness_metrics": fairness,
        "system_observation": system_observation,
        "observation_gateway": {
            **gateway,
            "trace_path": relative_output_paths["observation_gateway_trace"],
        },
        "completion_evidence": completion,
        "output_paths": relative_output_paths,
        "artifact_identities": artifact_identities,
        "cell_artifact_root": cell_relative_root,
        "raw_artifact_manifest": raw_artifact_manifest,
        "status": evidence["status"],
        "server_version": evidence["server_version"],
        "pgvector_version": evidence["pgvector_version"],
        "mfu_contract": evidence["mfu_contract"],
        "command": persisted_command(command),
    }
    for field in (
        "run_id", "queue_final", "shared_credit_final",
        "request_limit_per_endpoint", "work_limit_per_endpoint",
        "native_implementation_provenance",
    ):
        if field in evidence:
            persisted[field] = evidence[field]
    return persisted


def _validate_cell_evidence(
    arm: MatchedArm,
    cell: ScheduledMatchedCell,
    evidence: dict[str, object],
    repository_commit: str,
    runtime_identity: dict[str, object],
    matrix_output_root: Path,
    cell_output_dir: Path,
) -> dict[str, object]:
    """Normalize one executor result and reject incomplete comparison evidence."""

    start, end, boundary = _validate_cell_header(arm, evidence)
    jobs, actual_offset_s, offset_deviation_s = _validate_jobs(arm, evidence)
    service, resource = _validate_service_metrics(evidence)
    resource_relative_path = _validate_resource_trace(
        matrix_output_root,
        resource,
        start=start,
        end=end,
        boundary=boundary,
    )
    relative_output_paths, artifact_identities = _validate_output_artifacts(
        matrix_output_root,
        evidence["output_paths"],
    )
    gateway, system_observation, fairness = _validate_observation_evidence(
        arm,
        evidence,
        runtime_identity,
        artifact_identities,
    )
    completion = _validate_completion_evidence(arm, evidence)
    command = _validate_executor_final_state(arm, cell, evidence)
    cell_relative_root, raw_artifact_manifest = _raw_cell_artifact_manifest(
        matrix_output_root,
        cell_output_dir,
    )
    persisted = _curated_cell_evidence(
        evidence,
        jobs=jobs,
        service=service,
        resource=resource,
        resource_relative_path=resource_relative_path,
        relative_output_paths=relative_output_paths,
        artifact_identities=artifact_identities,
        gateway=gateway,
        system_observation=system_observation,
        fairness=fairness,
        completion=completion,
        cell_relative_root=cell_relative_root,
        raw_artifact_manifest=raw_artifact_manifest,
        command=command,
    )
    return {
        **persisted,
        "actual_job_offset_s": actual_offset_s,
        "nominal_job_offset_s": NOMINAL_JOB_OFFSET_S,
        "job_offset_deviation_s": offset_deviation_s,
        "job_offset_tolerance_s": ACTUAL_LAUNCH_OFFSET_TOLERANCE_S,
        "arm_id": arm.arm_id,
        "report_blocks": list(cell.report_blocks),
        "scheduler_owner": arm.scheduler_owner,
        "phase": cell.phase,
        "repeat": cell.repeat,
        "order_index": cell.order_index,
        "repository_commit": repository_commit,
        "config_sha256": runtime_identity["config_sha256"],
        **{
            field: runtime_identity[field]
            for field in ("native_config_sha256", "project_config_sha256")
            if field in runtime_identity
        },
        "config_fingerprint": runtime_identity["resolved_config_sha256"],
        "authorization_sha256": runtime_identity["authorization_sha256"],
        "matrix_instance_id": runtime_identity["matrix_instance_id"],
        "manifest_path": runtime_identity["manifest_evidence_path"],
        "manifest_sha256": arm.manifest_sha256,
        "service_signature": dict(arm.service_signature),
        "mfu_contract": arm.mfu_contract.__dict__,
        "endpoint_urls": list(runtime_identity["endpoint_urls"]),
        "metrics_urls": list(runtime_identity["metrics_urls"]),
        "health_url": runtime_identity["health_url"],
        "executor_selection": (
            native_calibration_selection(arm)
            if arm.kind == "native" else None
        ),
    }


def run_matched_system(
    config_path: Path,
    *,
    native_executor: Callable[[MatchedArm, ScheduledMatchedCell, Path], dict[str, object]],
    project_executor: Callable[[MatchedArm, ScheduledMatchedCell, Path], dict[str, object]],
    idle_gate: Callable[[str], None],
    instrumenter: Callable[..., object],
    repository_commit_getter: Callable[[], str] = _repository_commit,
    host_lease_acquirer: Callable[..., object] = acquire_host_runner_lease,
    rehearsal: bool = False,
    correctness_smoke: bool = False,
    matrix_output_root_override: Path | None = None,
    formal_authorization_path: Path | None = None,
    rehearsal_validation_path: Path | None = None,
    rehearsal_root: Path | None = None,
    rehearsal_archive: Path | None = None,
    service_identity_preflight: dict[str, object] | None = None,
    native_config_path: Path | None = None,
    project_config_path: Path | None = None,
    native_implementation_provenance: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    """Run the balanced five-arm DB-E2E matrix; executors retain instrumentation."""

    del instrumenter  # Deliberately injected for asserting the outer layer never samples.
    if rehearsal and correctness_smoke:
        raise ValueError("rehearsal and correctness smoke are mutually exclusive")
    if (matrix_output_root_override is not None) != correctness_smoke:
        raise ValueError(
            "an explicit fresh output-root override is required only for correctness smoke"
        )
    config = load_matched_system_config(config_path)
    repository_commit = repository_commit_getter()
    if rehearsal or correctness_smoke:
        requirements = run_identity_requirements(
            config_path, config, repository_commit,
            native_config_path, project_config_path,
        )
    else:
        rehearsal_evidence = validate_rehearsal_evidence(
            config_path, config, repository_commit,
            rehearsal_validation_path, rehearsal_root, rehearsal_archive,
            native_config_path, project_config_path,
            native_implementation_provenance,
        )
        requirements = formal_authorization_requirements(
            config_path, config, repository_commit, rehearsal_evidence
        )
    authorization_sha256 = (
        ""
        if rehearsal or correctness_smoke
        else validate_formal_authorization(
            formal_authorization_path,
            requirements,
        )
    )
    # Engineering decision: this nonce binds one authorized physical matrix
    # instance. Generate it only after the zero-side-effect authorization gate,
    # then copy it into the snapshot, index, and every cell.
    matrix_instance_id = uuid.uuid4().hex
    runtime_identity = {
        **requirements,
        "matrix_instance_id": matrix_instance_id,
        "status": (
            "correctness_smoke_pending" if correctness_smoke
            else "rehearsal_not_applicable" if rehearsal else "authorized"
        ),
        "formal_authorized": not (rehearsal or correctness_smoke),
        "authorization_sha256": authorization_sha256,
        "execution_mode": (
            "correctness_smoke" if correctness_smoke
            else "rehearsal" if rehearsal else "formal"
        ),
        "endpoint_urls": list(config.endpoint_urls),
        "metrics_urls": [
            endpoint_auxiliary_url(url, "/metrics")
            for url in config.endpoint_urls
        ],
        "health_url": endpoint_auxiliary_url(config.endpoint_urls[0], "/health"),
    }
    if native_implementation_provenance is not None:
        runtime_identity["native_implementation_provenance"] = (
            native_implementation_provenance
        )
    if service_identity_preflight is not None:
        runtime_identity["service_identity_preflight"] = (
            service_identity_preflight
        )
    canonical_output_root = Path(config.matrix_output_root).resolve()
    matrix_output_root = (
        matrix_output_root_override.resolve()
        if matrix_output_root_override is not None else canonical_output_root
    )
    if correctness_smoke and (
        matrix_output_root == canonical_output_root
        or canonical_output_root in matrix_output_root.parents
    ):
        raise ValueError(
            "correctness smoke root must not occupy or descend from the "
            "canonical rehearsal root"
        )
    try:
        matrix_output_root.mkdir(parents=True, exist_ok=False)
    except FileExistsError as error:
        raise FileExistsError(
            f"matrix output root already exists: {matrix_output_root}"
        ) from error
    sealed_manifest_relative = "evidence/frozen_manifest.jsonl"
    sealed_manifest_path = matrix_output_root / sealed_manifest_relative
    sealed_manifest_path.parent.mkdir(parents=True)
    shutil.copyfile(Path(config.arms[0].manifest_path), sealed_manifest_path)
    if sha256_file(sealed_manifest_path) != runtime_identity["manifest_sha256"]:
        raise RuntimeError("sealed manifest SHA drifted during matrix initialization")
    runtime_identity["manifest_evidence_path"] = sealed_manifest_relative
    sealed_jobs: list[dict[str, object]] = []
    for job in config.arms[0].job_manifests:
        relative = f"evidence/{job.job_id}_manifest.jsonl"
        sealed_path = matrix_output_root / relative
        shutil.copyfile(Path(job.path), sealed_path)
        if sha256_file(sealed_path) != job.sha256:
            raise RuntimeError(
                f"sealed {job.job_id} manifest SHA drifted during initialization"
            )
        sealed_jobs.append({
            "job_id": job.job_id,
            "rows": job.rows,
            "sha256": job.sha256,
            "evidence_path": relative,
        })
    runtime_identity["job_manifest_evidence"] = sealed_jobs
    matrix_index = matrix_output_root / "matrix_index.json"
    contract_snapshot = matrix_output_root / "matrix_contract_snapshot.json"
    _atomic_json(
        contract_snapshot,
        {
            "schema_version": 1,
            "runtime_identity": runtime_identity,
            "resolved_config": resolved_matched_system_identity(config),
        },
    )
    contract_snapshot_sha256 = sha256_file(contract_snapshot)
    index: dict[str, object] = {
        "schema_version": 1,
        "status": "running",
        "repository_commit": repository_commit,
        "matrix_instance_id": matrix_instance_id,
        "execution_mode": runtime_identity["execution_mode"],
        "config_sha256": runtime_identity["config_sha256"],
        **{
            field: runtime_identity[field]
            for field in ("native_config_sha256", "project_config_sha256")
            if field in runtime_identity
        },
        "config_fingerprint": runtime_identity["resolved_config_sha256"],
        "manifest_sha256": runtime_identity["manifest_sha256"],
        "manifest_evidence_path": sealed_manifest_relative,
        "job_manifest_evidence": sealed_jobs,
        "authorization_sha256": authorization_sha256,
        "contract_snapshot_sha256": contract_snapshot_sha256,
        "service_signature": dict(config.arms[0].service_signature),
        "service_identity": dict(config.service_identity),
        "service_identity_preflight": service_identity_preflight,
        "native_implementation_provenance": native_implementation_provenance,
        "mfu_contract": config.arms[0].mfu_contract.__dict__,
        "endpoint_urls": list(config.endpoint_urls),
        "metrics_urls": runtime_identity["metrics_urls"],
        "health_url": runtime_identity["health_url"],
        "matrix_output_root": str(matrix_output_root),
        "scheduler_owners": {
            arm.arm_id: arm.scheduler_owner for arm in config.arms
        },
        "repeat_contract": {
            "warmup": config.warmup_repeats,
            "formal": config.formal_repeats,
        },
        "schedule": [
            cell.__dict__ for cell in _all_cells(
                config, rehearsal=rehearsal or correctness_smoke
            )
        ],
        "cells": [],
    }
    _atomic_json(matrix_index, index)
    try:
        lease = host_lease_acquirer(
            matrix_output_root,
            repository_commit=repository_commit,
        )
    except Exception as exc:
        index.update(
            {
                "status": "failed",
                "lease_error": persisted_failure(exc),
            }
        )
        _atomic_json(matrix_index, index)
        raise
    by_id = {arm.arm_id: arm for arm in config.arms}
    try:
        for ordinal, cell in enumerate(_all_cells(
            config, rehearsal=rehearsal or correctness_smoke
        )):
            arm = by_id[cell.arm_id]
            output_dir = matrix_output_root / "cells" / (
                f"{ordinal:03d}_{cell.phase}_{cell.repeat:02d}_{arm.arm_id}"
            )
            running = {
                "arm_id": arm.arm_id,
                "report_blocks": list(cell.report_blocks),
                "scheduler_owner": arm.scheduler_owner,
                "phase": cell.phase,
                "repeat": cell.repeat,
                "order_index": cell.order_index,
                "repository_commit": repository_commit,
                "matrix_instance_id": matrix_instance_id,
                "config_sha256": runtime_identity["config_sha256"],
                **{
                    field: runtime_identity[field]
                    for field in (
                        "native_config_sha256", "project_config_sha256"
                    )
                    if field in runtime_identity
                },
                "config_fingerprint": runtime_identity[
                    "resolved_config_sha256"
                ],
                "authorization_sha256": authorization_sha256,
                "manifest_path": sealed_manifest_relative,
                "manifest_sha256": arm.manifest_sha256,
                "service_signature": dict(arm.service_signature),
                "mfu_contract": arm.mfu_contract.__dict__,
                "endpoint_urls": list(config.endpoint_urls),
                "metrics_urls": runtime_identity["metrics_urls"],
                "health_url": runtime_identity["health_url"],
                "executor_selection": (
                    native_calibration_selection(arm)
                    if arm.kind == "native" else None
                ),
                "status": "running",
            }
            index["cells"].append(running)  # type: ignore[index]
            _atomic_json(matrix_index, index)
            try:
                try:
                    idle_gate("before")
                    executor = (
                        native_executor if arm.kind == "native" else project_executor
                    )
                    completed = _validate_cell_evidence(
                        arm,
                        cell,
                        executor(arm, cell, output_dir),
                        repository_commit,
                        runtime_identity,
                        matrix_output_root,
                        output_dir,
                    )
                    running.update(completed)
                    primary_error: Exception | None = None
                except Exception as exc:
                    primary_error = exc
                try:
                    idle_gate("after")
                    after_idle_error: Exception | None = None
                except Exception as exc:
                    after_idle_error = exc
                if after_idle_error is not None:
                    running.setdefault("details", {})["after_idle_error"] = (
                        redact_text(
                            f"{type(after_idle_error).__name__}: {after_idle_error}"
                        )
                    )
                if primary_error is not None:
                    raise primary_error
                if after_idle_error is not None:
                    raise after_idle_error
            except Exception as exc:
                running.update(
                    {
                        "status": "failed",
                        "error": persisted_failure(exc),
                    }
                )
                index["status"] = "failed"
                _atomic_json(matrix_index, index)
                raise
            _atomic_json(matrix_index, index)
        index["status"] = "completed"
        _atomic_json(matrix_index, index)
        return index
    finally:
        lease.release()  # type: ignore[union-attr]


def _load_arm(value: object, config_directory: Path) -> MatchedArm:
    if not isinstance(value, dict):
        raise ValueError("each arm must be an object")
    missing = [
        field
        for field in _COMMON_FIELDS
        + (
            "arm_id", "kind", "scheduler_owner", "output_root",
            "calibration_path", "calibration_sha256",
        )
        if field not in value
    ]
    if missing:
        raise ValueError("arm is missing required fields: " + ", ".join(missing))
    project_contract = tuple(sorted((key, _freeze(value[key])) for key in _PROJECT_FIELDS if key in value))
    return MatchedArm(
        arm_id=_string(value["arm_id"], "arm_id"), kind=_string(value["kind"], "kind"),
        scheduler_owner=_string(value["scheduler_owner"], "scheduler_owner"),
        output_root=_resolve_config_path(value["output_root"], "output_root", config_directory),
        manifest_path=_resolve_config_path(value["manifest_path"], "manifest_path", config_directory),
        manifest_sha256=_string(value["manifest_sha256"], "manifest_sha256"),
        job_manifests=_load_job_manifests(value["job_manifests"], config_directory),
        endpoint_ids=tuple(value["endpoint_ids"]), service_signature=_mapping(value["service_signature"], "service_signature"),
        protocol=_string(value["protocol"], "protocol"), output_cap=_integer(value["output_cap"], "output_cap"),
        job_release_schedule=_load_job_release_schedule(value["job_release_schedule"]),
        arrival_replay_capability=_string(
            value["arrival_replay_capability"], "arrival_replay_capability"
        ),
        job_internal_arrival_contract=_string(value["job_internal_arrival_contract"], "job_internal_arrival_contract"),
        performance_writeback_mode=_string(value["performance_writeback_mode"], "performance_writeback_mode"),
        unsupported_request_tails=_mapping(value["unsupported_request_tails"], "unsupported_request_tails"),
        source=_mapping(value["source"], "source"), organizer=_string(value["organizer"], "organizer"),
        calibration_path=_resolve_config_path(
            value["calibration_path"], "calibration_path", config_directory
        ),
        calibration_sha256=_string(
            value["calibration_sha256"], "calibration_sha256"
        ),
        mfu_contract=_load_mfu_contract(value["mfu_contract"]),
        project_contract=project_contract,
        raw_field_names=tuple(value),
    )


def _load_job_manifests(
    value: object,
    config_directory: Path,
) -> tuple[JobManifestIdentity, ...]:
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("job_manifests must contain exactly two Jobs")
    output: list[JobManifestIdentity] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict) or set(raw) != {
            "job_id", "path", "rows", "sha256",
        }:
            raise ValueError("job_manifests entries have an invalid schema")
        rows = _integer(raw["rows"], f"job_manifests[{index}].rows")
        if rows <= 0:
            raise ValueError("job manifest rows must be positive")
        output.append(JobManifestIdentity(
            job_id=_string(raw["job_id"], f"job_manifests[{index}].job_id"),
            path=_resolve_config_path(
                raw["path"], f"job_manifests[{index}].path", config_directory
            ),
            rows=rows,
            sha256=_string(raw["sha256"], f"job_manifests[{index}].sha256"),
        ))
    return tuple(output)


def _load_job_release_schedule(value: object) -> tuple[JobReleaseEpoch, ...]:
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("job_release_schedule must contain exactly two Jobs")
    output: list[JobReleaseEpoch] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict) or set(raw) != {"job_id", "release_time_s"}:
            raise ValueError("job_release_schedule entries have an invalid schema")
        release_time = raw["release_time_s"]
        if isinstance(release_time, bool) or not isinstance(release_time, (int, float)):
            raise ValueError(f"job_release_schedule[{index}].release_time_s must be numeric")
        output.append(JobReleaseEpoch(
            job_id=_string(raw["job_id"], f"job_release_schedule[{index}].job_id"),
            release_time_s=float(release_time),
        ))
    return tuple(output)


def _load_mfu_contract(value: object) -> MfuContract:
    required = {"status", "gpu_peak_tflops_per_gpu", "precision", "reason"}
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError("mfu_contract has an invalid schema")
    peak = value["gpu_peak_tflops_per_gpu"]
    if isinstance(peak, bool) or not isinstance(peak, (int, float)) or float(peak) <= 0:
        raise ValueError("mfu_contract gpu_peak_tflops_per_gpu must be positive")
    status = _string(value["status"], "mfu_contract.status")
    if status not in {"available", "unavailable"}:
        raise ValueError("mfu_contract.status must be available or unavailable")
    reason = _string(value["reason"], "mfu_contract.reason")
    return MfuContract(
        status=status,
        gpu_peak_tflops_per_gpu=float(peak),
        precision=_string(value["precision"], "mfu_contract.precision"),
        reason=reason,
    )


def _validation_errors(
    config: MatchedSystemConfig, *, check_matrix_output_root: bool = True
) -> list[str]:
    errors: list[str] = []
    arm_ids = tuple(arm.arm_id for arm in config.arms)
    if len(arm_ids) != len(set(arm_ids)) or set(arm_ids) != set(REQUIRED_ARM_IDS) or len(arm_ids) != len(REQUIRED_ARM_IDS):
        errors.append("arms must contain exactly the five unique required arm IDs")
    if not config.arms:
        return errors
    if tuple(item.job_id for item in config.job_observation_contracts) != (
        "job0", "job1"
    ):
        errors.append("Job observation contracts must bind ordered job0/job1")
    if len({item.role for item in config.job_observation_contracts}) != 2:
        errors.append("Job observation roles must be distinct")
    try:
        validate_service_identity(dict(config.service_identity))
    except ValueError as error:
        errors.append(f"service_identity is invalid: {error}")
    if config.matched_manifest_status != "ready_frozen":
        errors.append("matched_manifest_status must be ready_frozen")
    if len(config.endpoint_urls) != 2 or len(set(config.endpoint_urls)) != 2:
        errors.append("endpoint_urls must contain two unique frozen endpoints")
    if config.gpu_formal_locally_authorized:
        errors.append("local authorization never permits GPU formal execution")
    if check_matrix_output_root and Path(config.matrix_output_root).exists():
        errors.append("matrix_output_root already exists")
    output_roots = [arm.output_root for arm in config.arms]
    if len(output_roots) != len(set(output_roots)):
        errors.append("output_root values must be unique")
    reference = config.arms[0]
    for arm in config.arms:
        if dict(arm.service_signature).get("scheduler") != "vllm_native_fcfs":
            errors.append(
                f"{arm.arm_id} service signature scheduler must be vllm_native_fcfs"
            )
        expected_kind = "native" if arm.arm_id in SYSTEM_ARM_IDS[:3] else "project"
        if arm.kind != expected_kind:
            errors.append(f"{arm.arm_id} must be a {expected_kind} arm")
        if (
            tuple(item.job_id for item in arm.job_release_schedule) != ("job0", "job1")
            or arm.arrival_offsets_s != (0.0, 5.0)
            or arm.job_internal_arrival_contract != "eager"
        ):
            errors.append(
                f"{arm.arm_id} must use typed job0/job1 releases at [0, 5] with eager internal arrival"
            )
        if arm.arrival_replay_capability != "not_used":
            errors.append(
                f"{arm.arm_id} request arrival replay must remain executor-internal and unused"
            )
        if arm.performance_writeback_mode != "none":
            errors.append(
                f"{arm.arm_id} must end at model completion with no writeback"
            )
        try:
            normalize_request_tail_status(arm.unsupported_request_tails)
        except RuntimeError as error:
            errors.append(f"{arm.arm_id} has invalid unsupported request tails: {error}")
        path = Path(arm.manifest_path)
        if not path.is_file():
            errors.append(f"{arm.arm_id} manifest is missing: {path}")
        elif hashlib.sha256(path.read_bytes()).hexdigest() != arm.manifest_sha256:
            errors.append(f"{arm.arm_id} manifest SHA-256 mismatch")
        if tuple(job.job_id for job in arm.job_manifests) != ("job0", "job1"):
            errors.append(f"{arm.arm_id} must bind ordered job0/job1 manifests")
        job_requests = []
        for job in arm.job_manifests:
            job_path = Path(job.path)
            if not job_path.is_file():
                errors.append(
                    f"{arm.arm_id} {job.job_id} manifest is missing: {job_path}"
                )
                continue
            if sha256_file(job_path) != job.sha256:
                errors.append(
                    f"{arm.arm_id} {job.job_id} manifest SHA-256 mismatch"
                )
                continue
            try:
                requests = read_manifest(job_path)
            except (OSError, ValueError) as error:
                errors.append(
                    f"{arm.arm_id} {job.job_id} manifest is invalid: {error}"
                )
                continue
            if len(requests) != job.rows:
                errors.append(
                    f"{arm.arm_id} {job.job_id} manifest row count mismatch"
                )
            if any(
                request.max_output_tokens != arm.output_cap
                or request.estimated_output_tokens != arm.output_cap
                for request in requests
            ):
                errors.append(
                    f"{arm.arm_id} {job.job_id} output cap drifts from service contract"
                )
            job_requests.extend(requests)
        if path.is_file() and len(job_requests) == sum(
            job.rows for job in arm.job_manifests
        ):
            try:
                if tuple(job_requests) != read_manifest(path):
                    errors.append(
                        f"{arm.arm_id} combined manifest does not equal job0+job1"
                    )
            except (OSError, ValueError) as error:
                errors.append(f"{arm.arm_id} combined manifest is invalid: {error}")
        doc_ids = [request.doc_id for request in job_requests]
        if len(doc_ids) != len(set(doc_ids)):
            errors.append(f"{arm.arm_id} Job manifests contain duplicate doc_id")
        calibration = Path(arm.calibration_path)
        if not calibration.is_file():
            errors.append(f"{arm.arm_id} calibration is missing: {calibration}")
        elif sha256_file(calibration) != arm.calibration_sha256:
            errors.append(f"{arm.arm_id} calibration SHA-256 mismatch")
        else:
            try:
                calibration_payload = json.loads(
                    calibration.read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError) as error:
                errors.append(f"{arm.arm_id} calibration is invalid: {error}")
            else:
                if (
                    not isinstance(calibration_payload, dict)
                    or calibration_payload.get("schema_version") != 1
                    or calibration_payload.get("status") != "ready"
                    or not isinstance(calibration_payload.get("selection"), dict)
                ):
                    errors.append(
                        f"{arm.arm_id} calibration contract is not ready"
                    )
        for field in (
            "manifest_path", "manifest_sha256", "job_manifests",
            "endpoint_ids", "service_signature", "protocol", "output_cap",
            "job_release_schedule", "mfu_contract",
        ):
            if getattr(arm, field) != getattr(reference, field):
                errors.append(f"{arm.arm_id} {field} drifts from matched contract")
        source = dict(arm.source)
        if (
            source.get("kind") != "timed_postgres_manifest"
            or source.get("timing_boundary") != "inside_job_barrier"
            or not source.get("database_url")
            or not source.get("workload_name")
        ):
            errors.append(f"{arm.arm_id} must use an exact timed PostgreSQL source contract")
        if arm.source != reference.source:
            errors.append(f"{arm.arm_id} source drifts from matched contract")
        if check_matrix_output_root and Path(arm.output_root).exists():
            errors.append(f"{arm.arm_id} output_root already exists")
        if arm.kind == "native":
            if arm.scheduler_owner not in {"daft", "ray_data"}:
                errors.append(f"{arm.arm_id} native scheduler owner must be daft or ray_data")
            if arm.project_contract:
                errors.append(f"{arm.arm_id} native arm rejects Project controls")
            if _native_project_control_names(arm.raw_field_names):
                errors.append(f"{arm.arm_id} native arm rejects explicit Project controls")
        elif arm.kind == "project":
            if arm.scheduler_owner != "project_daft_ray_submission_then_vllm_fcfs":
                errors.append(
                    f"{arm.arm_id} project scheduler owner must be "
                    "project_daft_ray_submission_then_vllm_fcfs"
                )
            if arm.organizer != "daft":
                errors.append(f"{arm.arm_id} resolved organizer must be daft")
            if arm.project_value("executor") != "ray_actor":
                errors.append(f"{arm.arm_id} resolved executor must be ray_actor")
            if arm.project_value("model_service_scheduler") != "vllm_native_fcfs":
                errors.append(
                    f"{arm.arm_id} model service scheduler must be vllm_native_fcfs"
                )
        else:
            errors.append(f"{arm.arm_id} kind must be native or project")
    by_id = {arm.arm_id: arm for arm in config.arms}
    frozen = by_id.get("project_frozen_static")
    if frozen and (
        frozen.project_value("ready_observation") is not None
        or frozen.project_value("debt_caps") is not None
        or frozen.project_value("policy") != "static_partition"
    ):
        errors.append("project_frozen_static must not use bounded-ready, dynamic selection, or debt")
    project_arms = [arm for arm in config.arms if arm.kind == "project"]
    if project_arms:
        project_reference = project_arms[0]
        for arm in project_arms:
            for field in (
                "k_per_endpoint", "work_limit_per_endpoint", "ready_bytes",
                "actor_topology", "batching_contract",
            ):
                if arm.project_value(field) != project_reference.project_value(field):
                    errors.append(f"{arm.arm_id} {field} drifts across Project comparison arms")
            if arm.source != project_reference.source or arm.organizer != project_reference.organizer:
                errors.append(f"{arm.arm_id} source or organizer drifts across Project comparison arms")
            if arm.calibration_path != project_reference.calibration_path:
                errors.append(f"{arm.arm_id} calibration_path drifts across Project comparison arms")
            if arm.calibration_sha256 != project_reference.calibration_sha256:
                errors.append(
                    f"{arm.arm_id} calibration_sha256 drifts across Project comparison arms"
                )
            if arm.unsupported_request_tails != project_reference.unsupported_request_tails:
                errors.append(f"{arm.arm_id} unsupported request tails drift across Project comparison arms")
    saor = by_id.get("project_bounded_ready_saor_0125we")
    if saor and (saor.project_value("policy") != "saor_bounded_ready" or saor.project_value("ready_observation") != "bounded_concrete_pre_registration" or saor.project_value("debt_caps") != (0.125, None)):
        errors.append("SAOR must use bounded-ready policy/observation and debt caps [0.125, null]")
    return errors


def _decode_json_container(value: object, run_id: str, field: str) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError(f"{run_id} {field} is not valid JSON") from error
    return value


def _finite_number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be finite")
    return parsed


def validate_native_final_queue(
    value: object, run_id: str
) -> dict[str, dict[str, object]]:
    """Canonicalize and require an empty final queue from a native system."""

    decoded = _decode_json_container(value, run_id, "queue_final")
    if not isinstance(decoded, dict) or not decoded:
        raise ValueError(f"{run_id} native queue_final has an invalid schema")
    output: dict[str, dict[str, object]] = {}
    for endpoint, state in decoded.items():
        if not str(endpoint) or not isinstance(state, dict):
            raise ValueError(f"{run_id} native queue_final has an invalid schema")
        if not {"running", "waiting"}.issubset(state):
            raise ValueError(f"{run_id} native queue_final lacks live fields")
        if any(
            _finite_number(state[name], f"{run_id} queue_final {name}") != 0
            for name in ("running", "waiting")
        ):
            raise ValueError(f"{run_id} native final queue is not empty")
        output[str(endpoint)] = dict(state)
    return output


def validate_project_final_credit(
    value: object, run_id: str, *, frozen_static: bool
) -> list[dict[str, object]]:
    """Canonicalize Project credit snapshots and reject every live remainder."""

    decoded = _decode_json_container(value, run_id, "shared_credit_final")
    if frozen_static:
        if decoded != []:
            raise ValueError(f"{run_id} frozen-static shared_credit_final must be []")
        return []
    if not isinstance(decoded, list) or not decoded:
        raise ValueError(f"{run_id} shared_credit_final has an invalid schema")
    scalar_live = (
        "active_requests", "active_work", "waiting_requests", "waiting_work",
    )
    mapping_live = (
        "active_by_job", "active_work_by_job", "waiting_by_job",
        "waiting_work_by_job", "waiting_head_work_by_job",
    )
    required = {
        "endpoint_id", "request_limit", "work_limit", *scalar_live, *mapping_live,
    }
    output: list[dict[str, object]] = []
    endpoint_ids: set[str] = set()
    for raw_snapshot in decoded:
        if not isinstance(raw_snapshot, dict) or not required.issubset(raw_snapshot):
            raise ValueError(f"{run_id} shared_credit_final has an invalid schema")
        snapshot = dict(raw_snapshot)
        endpoint_id = str(snapshot["endpoint_id"])
        if not endpoint_id or endpoint_id in endpoint_ids:
            raise ValueError(f"{run_id} shared_credit_final has invalid endpoint IDs")
        endpoint_ids.add(endpoint_id)
        if any(
            _finite_number(snapshot[name], f"{run_id} shared credit {name}") <= 0
            for name in ("request_limit", "work_limit")
        ):
            raise ValueError(f"{run_id} shared credit limits must be positive")
        for name in mapping_live:
            child = _decode_json_container(
                snapshot[name], run_id, f"shared credit {name}"
            )
            if not isinstance(child, (list, dict)):
                raise ValueError(
                    f"{run_id} shared credit {name} must encode a container"
                )
            snapshot[name] = child
        if any(
            _finite_number(snapshot[name], f"{run_id} shared credit {name}") != 0
            for name in scalar_live
        ) or any(snapshot[name] not in ([], {}) for name in mapping_live):
            raise ValueError(f"{run_id} final shared credit is not empty")
        output.append(snapshot)
    return output


def _freeze(value: object) -> object:
    if isinstance(value, dict): return tuple(sorted((key, _freeze(item)) for key, item in value.items()))
    if isinstance(value, list): return tuple(_freeze(item) for item in value)
    return value

def _native_project_control_names(names: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(name.lower().replace("-", "_") for name in names)
    forbidden = (
        "credit", "coordinator", "router", "ready_observation", "bounded_ready",
    )
    return tuple(name for name in normalized if any(word in name for word in forbidden))

def _resolve_config_path(value: object, name: str, config_directory: Path) -> str:
    path = Path(_string(value, name))
    if not path.is_absolute():
        path = config_directory / path
    return str(path.resolve())

def _mapping(value: object, name: str) -> tuple[tuple[str, object], ...]:
    if not isinstance(value, dict): raise ValueError(f"{name} must be an object")
    return tuple(sorted((str(key), _freeze(item)) for key, item in value.items()))

def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value: raise ValueError(f"{name} must be a non-empty string")
    return value

def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool): raise ValueError(f"{name} must be an integer")
    return value

def _nonnegative(value: object, name: str) -> int:
    result = _integer(value, name)
    if result < 0: raise ValueError(f"{name} must be non-negative")
    return result

def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool): raise ValueError(f"{name} must be boolean")
    return value
