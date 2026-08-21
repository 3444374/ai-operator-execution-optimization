"""Compose static config, installed-source, and live-service readiness gates."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from src.baselines.text.orchestration.native_multijob import (
    load_native_multijob_config,
)
from src.experiments.saor.native_system_bindings import validate_executor_bindings
from src.experiments.saor.native_system_contract import MatchedSystemConfig
from src.experiments.saor.native_system_matched import (
    audit_matched_system_config,
    load_matched_system_config,
)
from src.experiments.saor.vllm_0251_source_audit import (
    audit_installed_vllm_0251,
    validate_source_audit_evidence,
)
from src.experiments.shared_vllm import load_config as load_project_config
from src.infrastructure.vllm_preflight import (
    validate_service_identity,
    verify_live_vllm_service_identity,
)


def _fingerprint(value: object) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_and_validate_static_readiness(
    matched_config: Path,
    native_config: Path,
    project_config: Path,
) -> tuple[MatchedSystemConfig, dict[str, object]]:
    """Jointly load all three real configs and validate executor bindings."""

    matched = load_matched_system_config(
        matched_config, allow_existing_matrix_output_root=True
    )
    native = load_native_multijob_config(
        native_config, allow_existing_output_root=True
    )
    project = load_project_config(project_config)
    validate_executor_bindings(
        matched,
        native,
        project,
        matched_config_path=matched_config,
        project_config_path=project_config,
    )
    identity = dict(matched.service_identity)
    validate_service_identity(identity)
    matrix_audit = audit_matched_system_config(
        matched, check_output_roots=False
    )
    if matrix_audit["status"] != "passed":
        raise ValueError("matched config audit did not pass")
    return matched, identity


def verify_rehearsal_service_identity(
    matched: MatchedSystemConfig,
    installed_source_audit: Path,
) -> dict[str, object]:
    """Require exact source hashes, model artifacts, and live process flags."""

    identity = dict(matched.service_identity)
    evidence = json.loads(installed_source_audit.read_text(encoding="utf-8"))
    if not isinstance(evidence, dict):
        raise RuntimeError("installed-source audit must be a JSON object")
    validate_source_audit_evidence(evidence, identity)
    current_source = audit_installed_vllm_0251(identity)
    validate_source_audit_evidence(current_source, identity)
    live = verify_live_vllm_service_identity(
        matched.endpoint_urls,
        identity,
        tag="saor-five-arm-service-identity",
    )
    return {
        "installed_source": {
            "status": "passed",
            "evidence_sha256": hashlib.sha256(
                installed_source_audit.read_bytes()
            ).hexdigest(),
            "runtime_reaudit_status": current_source["status"],
            "vllm_version": current_source["installed_version"],
            "package_root": current_source["package_root"],
            "distribution_files": {
                name: {
                    "sha256": item["sha256"],
                    "expected_sha256": item["expected_sha256"],
                }
                for name, item in current_source["distribution_files"].items()
            },
            "source_files": {
                name: {
                    "sha256": item["sha256"],
                    "expected_sha256": item["expected_sha256"],
                    "markers_present": item["markers_present"],
                }
                for name, item in current_source["source_files"].items()
            },
        },
        "live_service": live,
    }


def audit_readiness(
    matched_config: Path,
    native_config: Path,
    project_config: Path,
    *,
    live_service: bool = False,
    installed_source_audit: Path | None = None,
) -> dict[str, object]:
    """Return a report that cannot confuse static success with rehearsal readiness."""

    matched, identity = load_and_validate_static_readiness(
        matched_config, native_config, project_config
    )
    report: dict[str, object] = {
        "schema_version": 1,
        "status": "static_config_passed",
        "rehearsal_ready": False,
        "config_scope": [
            str(matched_config.resolve()),
            str(native_config.resolve()),
            str(project_config.resolve()),
        ],
        "service_identity_sha256": _fingerprint(identity),
        "static_bindings": "passed",
        "installed_source": "not_checked",
        "live_service": "not_checked",
    }
    if not live_service:
        return report
    if installed_source_audit is None:
        raise ValueError(
            "--live-service requires --installed-source-audit exact-SHA evidence"
        )
    report.update(
        verify_rehearsal_service_identity(matched, installed_source_audit)
    )
    report["status"] = "passed"
    report["rehearsal_ready"] = True
    return report
