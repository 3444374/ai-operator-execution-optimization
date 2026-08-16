"""Fail-closed eight-arm configuration and scheduling for SAOR readiness."""

from __future__ import annotations

import hashlib
import csv
import json
import math
import random
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from src.infrastructure.config_env import expand_structure
from src.infrastructure.runner_lease import acquire_host_runner_lease


SYSTEM_ARM_IDS = (
    "daft_native", "daft_ray", "ray_data_http",
    "project_frozen_static", "project_bounded_ready_saor_0125we",
)
SELECTOR_SANITY_ARM_IDS = (
    "project_bounded_ready_fifo", "project_bounded_ready_drr",
    "project_bounded_ready_vtc_style",
    "project_bounded_ready_saor_0125we",
)
REQUIRED_ARM_IDS = tuple(dict.fromkeys(SYSTEM_ARM_IDS + SELECTOR_SANITY_ARM_IDS))
NOMINAL_JOB_OFFSET_S = 5.0
ACTUAL_CHILD_OFFSET_TOLERANCE_S = 0.25
FORMAL_AUTHORIZATION_SCOPE = "saor_native_system_matched_formal"

_PROJECT_FIELDS = {
    "k_per_endpoint", "work_limit_per_endpoint", "ready_bytes", "actor_topology",
    "policy", "ready_observation", "debt_caps",
}
_COMMON_FIELDS = (
    "manifest_path", "manifest_sha256", "endpoint_ids", "service_signature",
    "protocol", "output_cap", "arrival_offsets_s", "job_internal_arrival_contract",
    "performance_writeback_mode", "unsupported_request_tails", "source", "organizer",
)


def _validate_actual_job_offset(actual_offset_s: float) -> float:
    """Validate observed child-source timing without claiming zero jitter."""

    deviation_s = actual_offset_s - NOMINAL_JOB_OFFSET_S
    if abs(deviation_s) > ACTUAL_CHILD_OFFSET_TOLERANCE_S:
        raise RuntimeError(
            "actual child-source offset is outside the pre-registered "
            f"{ACTUAL_CHILD_OFFSET_TOLERANCE_S:.2f}s tolerance"
        )
    return deviation_s


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


@dataclass(frozen=True)
class MatchedArm:
    """One immutable physical execution arm; no service work is performed here."""

    arm_id: str
    kind: str
    scheduler_owner: str
    output_root: str
    manifest_path: str
    manifest_sha256: str
    endpoint_ids: tuple[str, ...]
    service_signature: tuple[tuple[str, object], ...]
    protocol: str
    output_cap: int
    arrival_offsets_s: tuple[float, ...]
    job_internal_arrival_contract: str
    performance_writeback_mode: str
    unsupported_request_tails: tuple[tuple[str, object], ...]
    source: tuple[tuple[str, object], ...]
    organizer: str
    calibration_path: str
    project_contract: tuple[tuple[str, object], ...] = ()
    raw_field_names: tuple[str, ...] = ()

    def project_value(self, name: str) -> object | None:
        return dict(self.project_contract).get(name)


@dataclass(frozen=True)
class MatchedSystemConfig:
    """Immutable experiment-wide readiness inputs."""

    seed: int
    warmup_repeats: int
    formal_repeats: int
    selector_sanity_development_repeats: int
    matrix_output_root: str
    gpu_formal_locally_authorized: bool
    matched_manifest_status: str
    arms: tuple[MatchedArm, ...]


@dataclass(frozen=True)
class ScheduledMatchedCell:
    """A single physical arm placement in one planned phase/repeat."""

    phase: str
    repeat: int
    order_index: int
    arm_id: str
    report_blocks: tuple[str, ...]


def load_matched_system_config(
    path: Path, *, allow_existing_matrix_output_root: bool = False
) -> MatchedSystemConfig:
    """Load a portable config and reject every mismatch before execution."""

    decoded = expand_structure(json.loads(path.read_text(encoding="utf-8")), "config")
    if not isinstance(decoded, dict) or decoded.get("schema_version") != 1:
        raise ValueError("matched-system config schema_version must be 1")
    arms_raw = decoded.get("arms")
    if not isinstance(arms_raw, list):
        raise ValueError("arms must be a list")
    config_directory = path.parent.resolve()
    arms = tuple(_load_arm(item, config_directory) for item in arms_raw)
    config = MatchedSystemConfig(
        seed=_integer(decoded.get("seed"), "seed"),
        warmup_repeats=_nonnegative(decoded.get("warmup_repeats"), "warmup_repeats"),
        formal_repeats=_nonnegative(decoded.get("formal_repeats"), "formal_repeats"),
        selector_sanity_development_repeats=_nonnegative(
            decoded.get("selector_sanity_development_repeats"),
            "selector_sanity_development_repeats",
        ),
        matrix_output_root=_resolve_config_path(
            decoded.get("matrix_output_root"), "matrix_output_root", config_directory
        ),
        gpu_formal_locally_authorized=_boolean(
            decoded.get("gpu_formal_locally_authorized"), "gpu_formal_locally_authorized"
        ),
        matched_manifest_status=_string(
            decoded.get("matched_manifest_status"), "matched_manifest_status"
        ),
        arms=arms,
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

    if phase not in {"warmup", "formal", "selector_sanity_development"}:
        raise ValueError("unsupported schedule phase")
    if repeat < 1:
        raise ValueError("repeat must be positive")
    phase_arm_ids = {
        "warmup": REQUIRED_ARM_IDS,
        "formal": SYSTEM_ARM_IDS,
        "selector_sanity_development": tuple(
            arm_id
            for arm_id in SELECTOR_SANITY_ARM_IDS
            if arm_id != "project_bounded_ready_saor_0125we"
        ),
    }
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
            report_blocks=tuple(
                name for name, identities in (
                    ("system", SYSTEM_ARM_IDS), ("selector_sanity", SELECTOR_SANITY_ARM_IDS)
                ) if arm_id in identities
            ),
        )
        for index, arm_id in enumerate(ordered)
    )


def audit_matched_system_config(config: MatchedSystemConfig) -> dict[str, object]:
    """Return a read-only readiness record without starting any external system."""

    errors = _validation_errors(config)
    manifests = {
        arm.arm_id: {"path": arm.manifest_path, "sha256": arm.manifest_sha256}
        for arm in config.arms
    }
    return {
        "schema_version": 1,
        "status": "passed" if not errors else "failed",
        "errors": errors,
        "resolved_arm_identities": [arm.arm_id for arm in config.arms],
        "report_blocks": {
            "system": list(SYSTEM_ARM_IDS),
            "selector_sanity": list(SELECTOR_SANITY_ARM_IDS),
        },
        "immutable_manifest_hashes": manifests,
        "service_signature": (
            dict(config.arms[0].service_signature) if config.arms else {}
        ),
        "calibration_paths": {
            arm.arm_id: arm.calibration_path for arm in config.arms
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
                (
                    "selector_sanity_development",
                    config.selector_sanity_development_repeats,
                ),
            )
            for repeat in range(1, count + 1)
        ],
    }


def _atomic_json(path: Path, payload: object) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def sha256_file(path: Path) -> str:
    """Return the immutable SHA-256 identity of one artifact."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
                "endpoint_ids": list(arm.endpoint_ids),
                "service_signature": dict(arm.service_signature),
                "protocol": arm.protocol,
                "output_cap": arm.output_cap,
                "arrival_offsets_s": list(arm.arrival_offsets_s),
                "job_internal_arrival_contract": (
                    arm.job_internal_arrival_contract
                ),
                "performance_writeback_mode": arm.performance_writeback_mode,
                "source": source,
                "organizer": arm.organizer,
                "calibration_path": arm.calibration_path,
                "project_contract": dict(arm.project_contract),
            }
        )
    return {
        "schema_version": 1,
        "seed": config.seed,
        "warmup_repeats": config.warmup_repeats,
        "formal_repeats": config.formal_repeats,
        "selector_sanity_development_repeats": (
            config.selector_sanity_development_repeats
        ),
        "matched_manifest_status": config.matched_manifest_status,
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


def formal_authorization_requirements(
    config_path: Path,
    config: MatchedSystemConfig,
    repository_commit: str,
) -> dict[str, object]:
    """Return the exact fields an independent formal authorization must bind."""

    if not repository_commit:
        raise ValueError("repository commit must be non-empty")
    manifests = {arm.manifest_sha256 for arm in config.arms}
    if len(manifests) != 1:
        raise ValueError("formal authorization requires one frozen manifest SHA")
    return {
        "schema_version": 1,
        "status": "authorized",
        "scope": FORMAL_AUTHORIZATION_SCOPE,
        "formal_authorized": True,
        "repository_commit": repository_commit,
        "config_sha256": sha256_file(config_path),
        "resolved_config_sha256": sha256_payload(
            resolved_matched_system_identity(config)
        ),
        "manifest_sha256": next(iter(manifests)),
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
            (
                "selector_sanity_development",
                config.selector_sanity_development_repeats,
            ),
        )
        for repeat in range(1, count + 1)
        for cell in balanced_matched_schedule(config, phase=phase, repeat=repeat)
    )


def _validate_cell_evidence(
    arm: MatchedArm,
    cell: ScheduledMatchedCell,
    evidence: dict[str, object],
    repository_commit: str,
    runtime_identity: dict[str, object],
) -> dict[str, object]:
    """Normalize one executor result and reject incomplete comparison evidence."""

    required = {
        "implementation_source", "start_epoch_s", "end_epoch_s",
        "database_operator_e2e_s", "jobs", "service_metrics",
        "resource_metrics", "exactly_once", "request_tail_status",
        "output_paths", "status",
    }
    missing = sorted(required - evidence.keys())
    if missing:
        raise RuntimeError(f"cell evidence missing required fields: {missing}")
    if evidence["status"] != "passed":
        raise RuntimeError("executor returned non-passing cell evidence")
    if evidence["exactly_once"] is not True:
        raise RuntimeError("cell exactly-once evidence failed")
    start = evidence["start_epoch_s"]
    end = evidence["end_epoch_s"]
    boundary = evidence["database_operator_e2e_s"]
    if not all(isinstance(item, (int, float)) for item in (start, end, boundary)):
        raise RuntimeError("cell common timing boundary is invalid")
    if float(end) < float(start) or float(boundary) < 0:
        raise RuntimeError("cell common timing boundary is inconsistent")
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
    starts = [float(job["actual_launch_epoch_s"]) for job in jobs]
    actual_offset_s = starts[1] - starts[0]
    offset_deviation_s = _validate_actual_job_offset(actual_offset_s)
    if float(jobs[0]["ended_epoch_s"]) <= starts[1]:
        raise RuntimeError("Job overlap evidence is missing")
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
    resource_path = Path(str(resource.get("path", "")))
    if not resource_path.is_file() or resource_path.stat().st_size <= 0:
        raise RuntimeError("resource trace artifact is missing or empty")
    with resource_path.open(encoding="utf-8", newline="") as stream:
        resource_rows = list(csv.DictReader(stream))
    if not resource_rows:
        raise RuntimeError("resource trace has no samples")
    observed = [
        float(row["observed_epoch_s"])
        for row in resource_rows
        if row.get("observed_epoch_s") not in (None, "")
    ]
    relative = [
        float(row["sample_epoch_s"])
        for row in resource_rows
        if row.get("sample_epoch_s") not in (None, "")
    ]
    if observed and not any(float(start) <= item <= float(end) for item in observed):
        raise RuntimeError("resource trace has no in-boundary sample")
    if relative and not any(0.0 <= item <= float(boundary) for item in relative):
        raise RuntimeError("resource trace has no in-boundary relative sample")
    if not observed and not relative:
        raise RuntimeError("resource trace has no timestamped samples")
    output_paths = evidence["output_paths"]
    if not isinstance(output_paths, dict) or not output_paths:
        raise RuntimeError("output artifact evidence is incomplete")
    for name, value in output_paths.items():
        artifact = Path(str(value))
        if not artifact.exists() or (artifact.is_file() and artifact.stat().st_size <= 0):
            raise RuntimeError(f"output artifact {name} is missing or empty")
    if normalize_request_tail_status(arm.unsupported_request_tails) != evidence["request_tail_status"]:
        raise RuntimeError("unsupported request-tail evidence drifted")
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
        for token in ("credit", "coordinator", "router", "bounded-ready", "max-active-work")
    ):
        raise RuntimeError("native dispatch contains Project flags")
    return {
        **evidence,
        "actual_job_offset_s": actual_offset_s,
        "nominal_job_offset_s": NOMINAL_JOB_OFFSET_S,
        "job_offset_deviation_s": offset_deviation_s,
        "job_offset_tolerance_s": ACTUAL_CHILD_OFFSET_TOLERANCE_S,
        "arm_id": arm.arm_id,
        "report_blocks": list(cell.report_blocks),
        "scheduler_owner": arm.scheduler_owner,
        "phase": cell.phase,
        "repeat": cell.repeat,
        "order_index": cell.order_index,
        "repository_commit": repository_commit,
        "config_sha256": runtime_identity["config_sha256"],
        "config_fingerprint": runtime_identity["resolved_config_sha256"],
        "authorization_sha256": runtime_identity["authorization_sha256"],
        "manifest_path": arm.manifest_path,
        "manifest_sha256": arm.manifest_sha256,
        "service_signature": dict(arm.service_signature),
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
    formal_authorization_path: Path | None = None,
) -> dict[str, object]:
    """Run the balanced eight-arm matrix; executors retain all instrumentation."""

    del instrumenter  # Deliberately injected for asserting the outer layer never samples.
    config = load_matched_system_config(
        config_path, allow_existing_matrix_output_root=True
    )
    repository_commit = repository_commit_getter()
    requirements = formal_authorization_requirements(
        config_path,
        config,
        repository_commit,
    )
    authorization_sha256 = (
        ""
        if rehearsal
        else validate_formal_authorization(
            formal_authorization_path,
            requirements,
        )
    )
    runtime_identity = {
        **requirements,
        "status": "rehearsal_not_applicable" if rehearsal else "authorized",
        "formal_authorized": not rehearsal,
        "authorization_sha256": authorization_sha256,
        "execution_mode": "rehearsal" if rehearsal else "formal",
    }
    matrix_output_root = Path(config.matrix_output_root)
    try:
        matrix_output_root.mkdir(parents=True, exist_ok=False)
    except FileExistsError as error:
        raise FileExistsError(
            f"matrix output root already exists: {matrix_output_root}"
        ) from error
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
        "execution_mode": runtime_identity["execution_mode"],
        "config_sha256": runtime_identity["config_sha256"],
        "config_fingerprint": runtime_identity["resolved_config_sha256"],
        "manifest_sha256": runtime_identity["manifest_sha256"],
        "authorization_sha256": authorization_sha256,
        "contract_snapshot_sha256": contract_snapshot_sha256,
        "service_signature": dict(config.arms[0].service_signature),
        "scheduler_owners": {
            arm.arm_id: arm.scheduler_owner for arm in config.arms
        },
        "repeat_contract": {
            "warmup": config.warmup_repeats,
            "formal": config.formal_repeats,
            "selector_sanity_development": (
                config.selector_sanity_development_repeats
            ),
        },
        "schedule": [
            cell.__dict__ for cell in _all_cells(config, rehearsal=rehearsal)
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
                "lease_error": f"{type(exc).__name__}: {exc}",
            }
        )
        _atomic_json(matrix_index, index)
        raise
    by_id = {arm.arm_id: arm for arm in config.arms}
    try:
        for ordinal, cell in enumerate(_all_cells(config, rehearsal=rehearsal)):
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
                "config_sha256": runtime_identity["config_sha256"],
                "config_fingerprint": runtime_identity[
                    "resolved_config_sha256"
                ],
                "authorization_sha256": authorization_sha256,
                "manifest_path": arm.manifest_path,
                "manifest_sha256": arm.manifest_sha256,
                "service_signature": dict(arm.service_signature),
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
                        f"{type(after_idle_error).__name__}: {after_idle_error}"
                    )
                if primary_error is not None:
                    raise primary_error
                if after_idle_error is not None:
                    raise after_idle_error
            except Exception as exc:
                running.update(
                    {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
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
    missing = [field for field in _COMMON_FIELDS + ("arm_id", "kind", "scheduler_owner", "output_root", "calibration_path") if field not in value]
    if missing:
        raise ValueError("arm is missing required fields: " + ", ".join(missing))
    project_contract = tuple(sorted((key, _freeze(value[key])) for key in _PROJECT_FIELDS if key in value))
    return MatchedArm(
        arm_id=_string(value["arm_id"], "arm_id"), kind=_string(value["kind"], "kind"),
        scheduler_owner=_string(value["scheduler_owner"], "scheduler_owner"),
        output_root=_resolve_config_path(value["output_root"], "output_root", config_directory),
        manifest_path=_resolve_config_path(value["manifest_path"], "manifest_path", config_directory),
        manifest_sha256=_string(value["manifest_sha256"], "manifest_sha256"),
        endpoint_ids=tuple(value["endpoint_ids"]), service_signature=_mapping(value["service_signature"], "service_signature"),
        protocol=_string(value["protocol"], "protocol"), output_cap=_integer(value["output_cap"], "output_cap"),
        arrival_offsets_s=tuple(value["arrival_offsets_s"]), job_internal_arrival_contract=_string(value["job_internal_arrival_contract"], "job_internal_arrival_contract"),
        performance_writeback_mode=_string(value["performance_writeback_mode"], "performance_writeback_mode"),
        unsupported_request_tails=_mapping(value["unsupported_request_tails"], "unsupported_request_tails"),
        source=_mapping(value["source"], "source"), organizer=_string(value["organizer"], "organizer"),
        calibration_path=_string(value["calibration_path"], "calibration_path"), project_contract=project_contract,
        raw_field_names=tuple(value),
    )


def _validation_errors(
    config: MatchedSystemConfig, *, check_matrix_output_root: bool = True
) -> list[str]:
    errors: list[str] = []
    arm_ids = tuple(arm.arm_id for arm in config.arms)
    if len(arm_ids) != len(set(arm_ids)) or set(arm_ids) != set(REQUIRED_ARM_IDS) or len(arm_ids) != len(REQUIRED_ARM_IDS):
        errors.append("arms must contain exactly the eight unique required arm IDs")
    if not config.arms:
        return errors
    if config.matched_manifest_status != "ready_frozen":
        errors.append("matched_manifest_status must be ready_frozen")
    if config.gpu_formal_locally_authorized:
        errors.append("local authorization never permits GPU formal execution")
    if config.selector_sanity_development_repeats > config.formal_repeats:
        errors.append(
            "selector_sanity_development_repeats must not exceed formal_repeats"
        )
    if check_matrix_output_root and Path(config.matrix_output_root).exists():
        errors.append("matrix_output_root already exists")
    output_roots = [arm.output_root for arm in config.arms]
    if len(output_roots) != len(set(output_roots)):
        errors.append("output_root values must be unique")
    reference = config.arms[0]
    for arm in config.arms:
        expected_kind = "native" if arm.arm_id in SYSTEM_ARM_IDS[:3] else "project"
        if arm.kind != expected_kind:
            errors.append(f"{arm.arm_id} must be a {expected_kind} arm")
        if arm.arrival_offsets_s != (0, 5) or arm.job_internal_arrival_contract != "eager":
            errors.append(f"{arm.arm_id} must use eager internal arrival with offsets [0, 5]")
        if arm.performance_writeback_mode != "none":
            errors.append(f"{arm.arm_id} performance writeback must be none")
        try:
            normalize_request_tail_status(arm.unsupported_request_tails)
        except RuntimeError as error:
            errors.append(f"{arm.arm_id} has invalid unsupported request tails: {error}")
        path = Path(arm.manifest_path)
        if not path.is_file():
            errors.append(f"{arm.arm_id} manifest is missing: {path}")
        elif hashlib.sha256(path.read_bytes()).hexdigest() != arm.manifest_sha256:
            errors.append(f"{arm.arm_id} manifest SHA-256 mismatch")
        for field in ("manifest_path", "manifest_sha256", "endpoint_ids", "service_signature", "protocol", "output_cap"):
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
        if Path(arm.output_root).exists():
            errors.append(f"{arm.arm_id} output_root already exists")
        if arm.kind == "native":
            if arm.scheduler_owner not in {"daft", "ray_data"}:
                errors.append(f"{arm.arm_id} native scheduler owner must be daft or ray_data")
            if arm.project_contract:
                errors.append(f"{arm.arm_id} native arm rejects Project controls")
            if _native_project_control_names(arm.raw_field_names):
                errors.append(f"{arm.arm_id} native arm rejects explicit Project controls")
        elif arm.kind == "project":
            if arm.scheduler_owner != "project":
                errors.append(f"{arm.arm_id} project scheduler owner must be project")
        else:
            errors.append(f"{arm.arm_id} kind must be native or project")
    by_id = {arm.arm_id: arm for arm in config.arms}
    for arm_id in ("project_bounded_ready_fifo", "project_bounded_ready_drr", "project_bounded_ready_vtc_style"):
        if by_id.get(arm_id) and by_id[arm_id].kind != "project":
            errors.append(f"{arm_id} is a Project selector control, never native")
    for arm_id in SELECTOR_SANITY_ARM_IDS:
        if by_id.get(arm_id) and by_id[arm_id].project_value("ready_observation") != "bounded_concrete_pre_registration":
            errors.append(f"{arm_id} must use bounded concrete pre-registration")
    frozen = by_id.get("project_frozen_static")
    if frozen and frozen.project_value("ready_observation") == "bounded_concrete_pre_registration":
        errors.append("project_frozen_static must not be bounded-ready")
    project_arms = [arm for arm in config.arms if arm.kind == "project"]
    if project_arms:
        project_reference = project_arms[0]
        for arm in project_arms:
            for field in ("k_per_endpoint", "work_limit_per_endpoint", "ready_bytes", "actor_topology"):
                if arm.project_value(field) != project_reference.project_value(field):
                    errors.append(f"{arm.arm_id} {field} drifts across Project selector arms")
            if arm.source != project_reference.source or arm.organizer != project_reference.organizer:
                errors.append(f"{arm.arm_id} source or organizer drifts across Project selector arms")
            if arm.calibration_path != project_reference.calibration_path:
                errors.append(f"{arm.arm_id} calibration_path drifts across Project selector arms")
            if arm.unsupported_request_tails != project_reference.unsupported_request_tails:
                errors.append(f"{arm.arm_id} unsupported request tails drift across Project selector arms")
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
