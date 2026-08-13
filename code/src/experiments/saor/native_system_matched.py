"""Fail-closed eight-arm configuration and scheduling for SAOR readiness."""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from pathlib import Path

from src.infrastructure.config_env import expand_structure


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

_PROJECT_FIELDS = {
    "k_per_endpoint", "work_limit_per_endpoint", "ready_bytes", "actor_topology",
    "policy", "ready_observation", "debt_caps",
}
_COMMON_FIELDS = (
    "manifest_path", "manifest_sha256", "endpoint_ids", "service_signature",
    "protocol", "output_cap", "arrival_offsets_s", "job_internal_arrival_contract",
    "performance_writeback_mode", "unsupported_request_tails", "source", "organizer",
)


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
    gpu_formal_locally_authorized: bool
    arms: tuple[MatchedArm, ...]


@dataclass(frozen=True)
class ScheduledMatchedCell:
    """A single physical arm placement in one planned phase/repeat."""

    phase: str
    repeat: int
    order_index: int
    arm_id: str
    report_blocks: tuple[str, ...]


def load_matched_system_config(path: Path) -> MatchedSystemConfig:
    """Load a portable config and reject every mismatch before execution."""

    decoded = expand_structure(json.loads(path.read_text(encoding="utf-8")), "config")
    if not isinstance(decoded, dict) or decoded.get("schema_version") != 1:
        raise ValueError("matched-system config schema_version must be 1")
    arms_raw = decoded.get("arms")
    if not isinstance(arms_raw, list):
        raise ValueError("arms must be a list")
    arms = tuple(_load_arm(item) for item in arms_raw)
    config = MatchedSystemConfig(
        seed=_integer(decoded.get("seed"), "seed"),
        warmup_repeats=_nonnegative(decoded.get("warmup_repeats"), "warmup_repeats"),
        formal_repeats=_nonnegative(decoded.get("formal_repeats"), "formal_repeats"),
        selector_sanity_development_repeats=_nonnegative(
            decoded.get("selector_sanity_development_repeats"),
            "selector_sanity_development_repeats",
        ),
        gpu_formal_locally_authorized=_boolean(
            decoded.get("gpu_formal_locally_authorized"), "gpu_formal_locally_authorized"
        ),
        arms=arms,
    )
    errors = _validation_errors(config)
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
    arm_ids = list(REQUIRED_ARM_IDS)
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
        "report_blocks": {"system": list(SYSTEM_ARM_IDS), "selector_sanity": list(SELECTOR_SANITY_ARM_IDS)},
        "immutable_manifest_hashes": manifests,
        "service_signature": dict(config.arms[0].service_signature) if config.arms else {},
        "calibration_paths": {arm.arm_id: arm.calibration_path for arm in config.arms},
        "gpu_formal_locally_authorized": config.gpu_formal_locally_authorized,
        "planned_schedule": [
            {
                "phase": phase,
                "repeat": repeat,
                "cells": [cell.__dict__ for cell in balanced_matched_schedule(config, phase=phase, repeat=repeat)],
            }
            for phase, count in (
                ("warmup", config.warmup_repeats),
                ("formal", config.formal_repeats),
                ("selector_sanity_development", config.selector_sanity_development_repeats),
            )
            for repeat in range(1, count + 1)
        ],
    }


def _load_arm(value: object) -> MatchedArm:
    if not isinstance(value, dict):
        raise ValueError("each arm must be an object")
    missing = [field for field in _COMMON_FIELDS + ("arm_id", "kind", "scheduler_owner", "output_root", "calibration_path") if field not in value]
    if missing:
        raise ValueError("arm is missing required fields: " + ", ".join(missing))
    project_contract = tuple(sorted((key, _freeze(value[key])) for key in _PROJECT_FIELDS if key in value))
    return MatchedArm(
        arm_id=_string(value["arm_id"], "arm_id"), kind=_string(value["kind"], "kind"),
        scheduler_owner=_string(value["scheduler_owner"], "scheduler_owner"),
        output_root=_string(value["output_root"], "output_root"),
        manifest_path=_string(value["manifest_path"], "manifest_path"),
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


def _validation_errors(config: MatchedSystemConfig) -> list[str]:
    errors: list[str] = []
    arm_ids = tuple(arm.arm_id for arm in config.arms)
    if len(arm_ids) != len(set(arm_ids)) or set(arm_ids) != set(REQUIRED_ARM_IDS) or len(arm_ids) != len(REQUIRED_ARM_IDS):
        errors.append("arms must contain exactly the eight unique required arm IDs")
    if not config.arms:
        return errors
    if config.gpu_formal_locally_authorized:
        errors.append("local authorization never permits GPU formal execution")
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
        if dict(arm.unsupported_request_tails).get("status") != "unavailable" or not dict(arm.unsupported_request_tails).get("reason"):
            errors.append(f"{arm.arm_id} must record unavailable unsupported request tails with reason")
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
