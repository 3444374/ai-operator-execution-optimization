"""Contract for SAOR versus in-engine DRR/VTC complete-system comparison."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


HEADLINE_ARM_IDS = (
    "daft_ray_vllm_native_fcfs",
    "daft_ray_drr_on_vllm_reproduction",
    "daft_ray_vtc_on_vllm_reproduction",
    "saor_vllm_native_fcfs",
)
CUSTOM_FCFS_CONTROL_ID = "daft_ray_custom_fcfs_capability"
COMPARISON_SCOPE = "cross_layer_complete_system_experience"

_PLUGIN_PREFIX = "src.experiments.saor.vllm_scheduler_plugin."
_CONTROL_FIELDS = {
    "bounded_ready", "project_k_w", "shared_credit", "debt_recovery",
    "upstream_state_awareness", "project_request_reordering",
}
_REQUIRED_METRICS = {
    "database_e2e_throughput", "group_jct", "per_job_jct",
    "request_p50_p95_p99_slo", "service_lag", "maximum_no_service_interval",
    "starvation", "work_conservation", "gpu_vllm_energy",
    "per_client_accumulated_service", "correctness_exactly_once",
}
_COMMON_CONTRACT_FIELDS = {
    "database_source_sink", "combined_manifest_sha256", "job_manifest_sha256",
    "job_release_schedule_s", "model", "tokenizer_chat_template",
    "vllm_version", "vllm_commit_build", "output_cap", "temperature",
    "ignore_eos", "gpu_endpoint_kv_contract", "timed_boundary", "correctness",
}


@dataclass(frozen=True)
class CrossLayerArm:
    arm_id: str
    display_name: str
    data_execution: str
    scheduler_owner: str
    model_service_scheduler: str
    scheduler_cls: str | None
    controls: tuple[tuple[str, bool], ...]


@dataclass(frozen=True)
class CrossLayerCapabilityConfig:
    capability_status: str
    blockers: tuple[str, ...]
    formal_authorized: bool
    server_validation: str
    vllm_source_audit: tuple[tuple[str, object], ...]
    scheduler_module_path: str
    scheduler_module_sha256: str
    identity_contract: tuple[tuple[str, object], ...]
    custom_fcfs_parity: tuple[tuple[str, object], ...]
    common_contract: tuple[tuple[str, object], ...]
    headline_arms: tuple[CrossLayerArm, ...]
    capability_control: CrossLayerArm
    required_metrics: tuple[str, ...]
    claim_boundary: tuple[tuple[str, object], ...]
    official_vtc_reference: tuple[tuple[str, object], ...]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_cross_layer_capability(path: Path) -> CrossLayerCapabilityConfig:
    """Load and validate a non-running capability configuration."""

    raw = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_version", "comparison_scope", "capability_status", "blockers",
        "formal_authorized", "server_validation", "vllm_source_audit",
        "scheduler_module", "identity_contract", "custom_fcfs_parity",
        "common_contract", "headline_arms", "capability_control",
        "required_metrics", "claim_boundary", "official_vtc_reference",
    }
    if not isinstance(raw, dict) or set(raw) != required:
        raise ValueError("cross-layer capability config schema is invalid")
    if raw.get("schema_version") != 1 or raw.get("comparison_scope") != COMPARISON_SCOPE:
        raise ValueError("cross-layer capability identity drifted")
    module = _mapping(raw["scheduler_module"], "scheduler_module")
    if set(module) != {"path", "sha256", "class_paths"}:
        raise ValueError("scheduler module schema is invalid")
    module_path = (path.parent / _string(module["path"], "scheduler module path")).resolve()
    arms_raw = raw["headline_arms"]
    if not isinstance(arms_raw, list):
        raise ValueError("headline_arms must be a list")
    config = CrossLayerCapabilityConfig(
        capability_status=_string(raw["capability_status"], "capability_status"),
        blockers=tuple(_string(item, "blocker") for item in _list(raw["blockers"], "blockers")),
        formal_authorized=_boolean(raw["formal_authorized"], "formal_authorized"),
        server_validation=_string(raw["server_validation"], "server_validation"),
        vllm_source_audit=_items(raw["vllm_source_audit"], "vllm_source_audit"),
        scheduler_module_path=str(module_path),
        scheduler_module_sha256=_sha(module["sha256"], "scheduler module SHA"),
        identity_contract=_items(raw["identity_contract"], "identity_contract"),
        custom_fcfs_parity=_items(raw["custom_fcfs_parity"], "custom_fcfs_parity"),
        common_contract=_items(raw["common_contract"], "common_contract"),
        headline_arms=tuple(_load_arm(item) for item in arms_raw),
        capability_control=_load_arm(raw["capability_control"]),
        required_metrics=tuple(
            _string(item, "required metric")
            for item in _list(raw["required_metrics"], "required_metrics")
        ),
        claim_boundary=_items(raw["claim_boundary"], "claim_boundary"),
        official_vtc_reference=_items(
            raw["official_vtc_reference"], "official_vtc_reference"
        ),
    )
    errors = validate_cross_layer_capability(config, module.get("class_paths"))
    if errors:
        raise ValueError("; ".join(errors))
    return config


def validate_cross_layer_capability(
    config: CrossLayerCapabilityConfig,
    class_paths: object,
) -> list[str]:
    """Return fail-closed contract errors without starting a server."""

    errors: list[str] = []
    if config.capability_status not in {"blocked", "ready_for_rehearsal"}:
        errors.append("capability_status must be blocked or ready_for_rehearsal")
    if config.formal_authorized:
        errors.append("cross-layer formal execution is not authorized")
    if config.server_validation not in {"not_run", "passed"}:
        errors.append("server_validation must be not_run or passed")
    if not config.blockers and config.capability_status == "blocked":
        errors.append("blocked capability must list blockers")
    if config.blockers and config.capability_status == "ready_for_rehearsal":
        errors.append("ready capability cannot retain blockers")
    module_path = Path(config.scheduler_module_path)
    if not module_path.is_file():
        errors.append("scheduler module is missing")
    elif sha256_file(module_path) != config.scheduler_module_sha256:
        errors.append("scheduler module SHA-256 drifted")
    expected_classes = {
        "custom_fcfs": _PLUGIN_PREFIX + "CustomFCFSScheduler",
        "drr": _PLUGIN_PREFIX + "DRRScheduler",
        "vtc": _PLUGIN_PREFIX + "VTCScheduler",
    }
    if class_paths != expected_classes:
        errors.append("scheduler class paths drifted")

    source = dict(config.vllm_source_audit)
    if source.get("frozen_version") != "0.25.1":
        errors.append("vLLM version must be frozen at 0.25.1")
    if source.get("builtin_policies") != ["fcfs", "priority"]:
        errors.append("vLLM built-in policy audit drifted")
    if source.get("custom_scheduler_interface") != "non_public":
        errors.append("vLLM custom scheduler stability audit drifted")
    if source.get("required_base_class") != "vllm.v1.core.sched.async_scheduler.AsyncScheduler":
        errors.append("custom scheduler must preserve AsyncScheduler inheritance")

    identity = dict(config.identity_contract)
    if identity.get("http_header") != "X-Request-Id":
        errors.append("Job identity transport header drifted")
    if identity.get("missing_or_invalid") != "fail_closed":
        errors.append("Job identity must fail closed")
    if identity.get("fallback_client_id") is not None:
        errors.append("Job identity cannot define a fallback client")

    parity = dict(config.custom_fcfs_parity)
    required_parity = {
        "request_order", "chunked_prefill", "prefix_cache", "kv_allocation",
        "preemption", "async_scheduling", "throughput", "module_sha",
    }
    checks = parity.get("checks")
    if not isinstance(checks, dict) or set(checks) != required_parity:
        errors.append("custom FCFS parity check set is incomplete")

    common = dict(config.common_contract)
    if set(common) != _COMMON_CONTRACT_FIELDS:
        errors.append("cross-layer common contract schema drifted")
    if common.get("vllm_version") != "0.25.1":
        errors.append("cross-layer common vLLM version drifted")
    if common.get("job_release_schedule_s") != {"job0": 0, "job1": 5}:
        errors.append("cross-layer typed Job release schedule drifted")
    if common.get("output_cap") != 256 or common.get("temperature") != 0:
        errors.append("cross-layer fixed-output semantics drifted")

    ids = tuple(arm.arm_id for arm in config.headline_arms)
    if ids != HEADLINE_ARM_IDS:
        errors.append("headline arms must retain the exact four-arm order")
    by_id = {arm.arm_id: arm for arm in config.headline_arms}
    expected_service = {
        "daft_ray_vllm_native_fcfs": ("vllm_native_fcfs", None),
        "daft_ray_drr_on_vllm_reproduction": (
            "drr_on_vllm_reproduction", expected_classes["drr"]
        ),
        "daft_ray_vtc_on_vllm_reproduction": (
            "vtc_on_vllm_reproduction", expected_classes["vtc"]
        ),
        "saor_vllm_native_fcfs": ("vllm_native_fcfs", None),
    }
    expected_owners = {
        "daft_ray_vllm_native_fcfs": "daft_ray_submission_then_vllm_native_fcfs",
        "daft_ray_drr_on_vllm_reproduction": (
            "daft_ray_submission_then_drr_on_vllm_reproduction"
        ),
        "daft_ray_vtc_on_vllm_reproduction": (
            "daft_ray_submission_then_vtc_on_vllm_reproduction"
        ),
        "saor_vllm_native_fcfs": "project_daft_ray_submission_then_vllm_fcfs",
    }
    expected_names = {
        "daft_ray_vllm_native_fcfs": "Daft Native/Ray + vLLM native FCFS",
        "daft_ray_drr_on_vllm_reproduction": (
            "Daft Native/Ray + DRR-on-vLLM reproduction"
        ),
        "daft_ray_vtc_on_vllm_reproduction": (
            "Daft Native/Ray + VTC-on-vLLM reproduction"
        ),
        "saor_vllm_native_fcfs": "SAOR + vLLM native FCFS",
    }
    for arm_id, (scheduler, scheduler_cls) in expected_service.items():
        arm = by_id.get(arm_id)
        if arm is None:
            continue
        if (arm.model_service_scheduler, arm.scheduler_cls) != (
            scheduler, scheduler_cls
        ):
            errors.append(f"{arm_id} model-service scheduler drifted")
        if arm.scheduler_owner != expected_owners[arm_id]:
            errors.append(f"{arm_id} scheduler owner drifted")
        if arm.display_name != expected_names[arm_id]:
            errors.append(f"{arm_id} reproduction/native display name drifted")
        controls = dict(arm.controls)
        if set(controls) != _CONTROL_FIELDS:
            errors.append(f"{arm_id} control schema drifted")
        if arm_id.startswith("daft_ray_"):
            if arm.data_execution != "daft_prompt_ray_native":
                errors.append(f"{arm_id} data path is not matched Daft/Ray native")
            if any(controls.values()):
                errors.append(f"{arm_id} illegally contains Project/SAOR controls")
        elif arm_id == "saor_vllm_native_fcfs":
            if arm.data_execution != "project_daft_ray_saor":
                errors.append("SAOR data path drifted")
            if not all(controls.values()):
                errors.append("SAOR arm must retain its complete upstream package")
            if arm.scheduler_owner != "project_daft_ray_submission_then_vllm_fcfs":
                errors.append("SAOR scheduler owner drifted")

    control = config.capability_control
    if (
        control.arm_id != CUSTOM_FCFS_CONTROL_ID
        or control.data_execution != "daft_prompt_ray_native"
        or control.model_service_scheduler != "custom_fcfs_capability_control"
        or control.scheduler_cls != expected_classes["custom_fcfs"]
        or control.scheduler_owner != "daft_ray_submission_then_custom_fcfs_capability"
        or any(dict(control.controls).values())
    ):
        errors.append("custom FCFS capability control drifted")
    if set(config.required_metrics) != _REQUIRED_METRICS:
        errors.append("cross-layer required metric set drifted")

    claims = dict(config.claim_boundary)
    if claims.get("allowed") != (
        "complete-system empirical difference between upstream SAOR and in-engine DRR/VTC reproductions"
    ):
        errors.append("allowed cross-layer claim drifted")
    if claims.get("forbidden") != (
        "same-layer selector superiority of SAOR over DRR or VTC"
    ):
        errors.append("forbidden cross-layer claim drifted")

    official = dict(config.official_vtc_reference)
    if official != {
        "module": "src.experiments.saor.official_vtc_capability",
        "artifact": (
            "Ying1123/VTC-artifact@"
            "192c2e2014c69c8c6c699d7113c3822e4db632e6"
        ),
        "role": "semantic_reference_only_not_headline_performance_arm",
    }:
        errors.append("official VTC semantic reference drifted")

    if config.capability_status == "ready_for_rehearsal":
        if source.get("installed_source_status") != "passed":
            errors.append("ready capability requires installed-source audit")
        if identity.get("status") != "passed":
            errors.append("ready capability requires Job identity proof")
        if parity.get("status") != "passed" or any(
            status != "passed" for status in checks.values()
        ):
            errors.append("ready capability requires all custom FCFS parity checks")
        if config.server_validation != "passed":
            errors.append("ready capability requires server validation")
        unresolved = [
            name
            for name, value in common.items()
            if isinstance(value, str)
            and any(marker in value for marker in ("required", "unverified", "must_be"))
        ]
        if unresolved:
            errors.append(
                "ready capability has unresolved common fields: "
                + ", ".join(sorted(unresolved))
            )
    return errors


def audit_cross_layer_capability(config: CrossLayerCapabilityConfig) -> dict[str, object]:
    """Return a compact capability report; never emit performance rankings."""

    return {
        "schema_version": 1,
        "comparison_scope": COMPARISON_SCOPE,
        "capability_status": config.capability_status,
        "blockers": list(config.blockers),
        "formal_authorized": config.formal_authorized,
        "server_validation": config.server_validation,
        "scheduler_module": {
            "path": config.scheduler_module_path,
            "sha256": config.scheduler_module_sha256,
        },
        "vllm_source_audit": dict(config.vllm_source_audit),
        "identity_contract": dict(config.identity_contract),
        "custom_fcfs_parity": dict(config.custom_fcfs_parity),
        "headline_arms": [arm.__dict__ for arm in config.headline_arms],
        "performance_ranking_published": False,
    }


def build_cross_layer_evidence_report(
    config: CrossLayerCapabilityConfig,
    rows: list[dict[str, object]],
    *,
    formal_authorization_verified: bool = False,
) -> dict[str, object]:
    """Validate future evidence; blocked/unapproved configs cannot publish it."""

    if config.capability_status != "ready_for_rehearsal":
        raise PermissionError("cross-layer capability is blocked")
    if not formal_authorization_verified:
        raise PermissionError("cross-layer formal execution remains unauthorized")
    by_id = {str(row.get("arm_id", "")): row for row in rows}
    if set(by_id) != set(HEADLINE_ARM_IDS) or len(rows) != len(HEADLINE_ARM_IDS):
        raise ValueError("cross-layer evidence requires exactly four headline arms")
    common_sha = None
    for arm in config.headline_arms:
        row = by_id[arm.arm_id]
        required = {
            "status", "comparison_scope", "scheduler_owner",
            "model_service_scheduler", "common_contract_sha256",
            "scheduler_module_sha256", "identity_proof_status",
            "custom_fcfs_parity_status", "metrics",
        }
        if not required.issubset(row):
            raise ValueError(f"{arm.arm_id} evidence schema is incomplete")
        if row["status"] != "passed" or row["comparison_scope"] != COMPARISON_SCOPE:
            raise ValueError(f"{arm.arm_id} evidence did not pass")
        if row["scheduler_owner"] != arm.scheduler_owner:
            raise ValueError(f"{arm.arm_id} scheduler owner drifted")
        if row["model_service_scheduler"] != arm.model_service_scheduler:
            raise ValueError(f"{arm.arm_id} service scheduler drifted")
        if row["scheduler_module_sha256"] != config.scheduler_module_sha256:
            raise ValueError(f"{arm.arm_id} scheduler module SHA drifted")
        if row["identity_proof_status"] != "passed":
            raise ValueError(f"{arm.arm_id} identity proof is missing")
        if row["custom_fcfs_parity_status"] != "passed":
            raise ValueError(f"{arm.arm_id} FCFS parity proof is missing")
        metrics = row["metrics"]
        if not isinstance(metrics, dict) or set(metrics) != _REQUIRED_METRICS:
            raise ValueError(f"{arm.arm_id} metric evidence is incomplete")
        if common_sha is None:
            common_sha = row["common_contract_sha256"]
        elif row["common_contract_sha256"] != common_sha:
            raise ValueError("cross-layer common contract identity drifted")
    return {
        "schema_version": 1,
        "comparison_scope": COMPARISON_SCOPE,
        "claim_boundary": dict(config.claim_boundary),
        "rows": [by_id[arm_id] for arm_id in HEADLINE_ARM_IDS],
    }


def _load_arm(value: object) -> CrossLayerArm:
    raw = _mapping(value, "arm")
    if set(raw) != {
        "arm_id", "display_name", "data_execution", "scheduler_owner",
        "model_service_scheduler", "scheduler_cls", "controls",
    }:
        raise ValueError("cross-layer arm schema is invalid")
    scheduler_cls = raw["scheduler_cls"]
    if scheduler_cls is not None:
        scheduler_cls = _string(scheduler_cls, "scheduler_cls")
    controls = _mapping(raw["controls"], "controls")
    if any(not isinstance(item, bool) for item in controls.values()):
        raise ValueError("cross-layer controls must be booleans")
    return CrossLayerArm(
        arm_id=_string(raw["arm_id"], "arm_id"),
        display_name=_string(raw["display_name"], "display_name"),
        data_execution=_string(raw["data_execution"], "data_execution"),
        scheduler_owner=_string(raw["scheduler_owner"], "scheduler_owner"),
        model_service_scheduler=_string(
            raw["model_service_scheduler"], "model_service_scheduler"
        ),
        scheduler_cls=scheduler_cls,
        controls=tuple(sorted(controls.items())),
    )


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _items(value: object, name: str) -> tuple[tuple[str, object], ...]:
    return tuple(sorted(_mapping(value, name).items()))


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")
    return value


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean")
    return value


def _sha(value: object, name: str) -> str:
    text = _string(value, name)
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return text
