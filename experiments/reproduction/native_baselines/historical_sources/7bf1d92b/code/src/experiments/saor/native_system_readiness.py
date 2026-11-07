"""Compose four fail-closed stages before any SAOR rehearsal may start.

Static config and live service identity are followed by an actual read-only
system preflight and a deep-validated correctness-smoke root.  Only all four
stages together may emit ``rehearsal_ready=true``.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

from src.baselines.text.orchestration.native_multijob import (
    load_native_multijob_config,
)
from src.experiments.saor.native_system_bindings import validate_executor_bindings
from src.experiments.saor.native_system_contract import MatchedSystemConfig
from src.experiments.saor.native_system_matched import (
    audit_matched_system_config,
    load_matched_system_config,
    resolved_matched_system_identity,
    sha256_file,
    sha256_payload,
)
from src.experiments.saor.native_system_artifacts import (
    validate_completed_matrix_root,
)
from src.experiments.saor.native_system_preflight import (
    build_system_preflight_payload,
)
from src.experiments.saor.vllm_0251_source_audit import validate_source_audit_evidence
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

    matched = load_matched_system_config(matched_config)
    native = load_native_multijob_config(native_config)
    if set(dict(native.native_implementation_provenance)) != {
        "daft_native", "daft_ray", "ray_data_http"
    }:
        raise ValueError(
            "matched native config lacks complete upstream/adapter provenance"
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
    matrix_audit = audit_matched_system_config(matched)
    if matrix_audit["status"] != "passed":
        raise ValueError("matched config audit did not pass")
    return matched, identity


def run_vllm_source_audit(
    vllm_python: Path, matched_config: Path
) -> dict[str, object]:
    """Recompute package/source identity inside the explicit vLLM runtime."""

    if not vllm_python.is_file():
        raise RuntimeError(f"VLLM_PYTHON is missing: {vllm_python}")
    repository = Path(__file__).resolve().parents[4]
    script = repository / "code/scripts/analysis/audit_vllm_0251_source.py"
    completed = subprocess.run(
        [
            str(vllm_python), str(script), "--config", str(matched_config),
            "--stdout",
        ],
        cwd=repository,
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        decoded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("VLLM_PYTHON source audit returned invalid JSON") from exc
    if not isinstance(decoded, dict) or completed.returncode != 0:
        raise RuntimeError("VLLM_PYTHON source audit did not pass")
    return decoded


def verify_rehearsal_service_identity(
    matched: MatchedSystemConfig,
    matched_config: Path,
    vllm_python: Path,
    installed_source_audit: Path,
    runtime_identity_paths: tuple[Path, ...],
) -> dict[str, object]:
    """Require exact source hashes, model artifacts, and live process flags."""

    identity = dict(matched.service_identity)
    evidence = json.loads(installed_source_audit.read_text(encoding="utf-8"))
    if not isinstance(evidence, dict):
        raise RuntimeError("installed-source audit must be a JSON object")
    validate_source_audit_evidence(evidence, identity)
    current_source = run_vllm_source_audit(vllm_python, matched_config)
    validate_source_audit_evidence(current_source, identity)
    if evidence != current_source:
        raise RuntimeError(
            "stored installed-source audit does not match fresh VLLM_PYTHON audit"
        )
    live = verify_live_vllm_service_identity(
        matched.endpoint_urls,
        identity,
        current_source["python_runtime"],
        runtime_identity_paths,
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
            "python_runtime": current_source["python_runtime"],
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


def _load_evidence(path: Path, label: str) -> dict[str, object]:
    """Load one readiness artifact as a required JSON object."""

    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label} evidence is unreadable") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError(f"{label} evidence must be a JSON object")
    return decoded


def validate_system_preflight_evidence(
    path: Path,
    expected_binding: dict[str, object],
    matched: MatchedSystemConfig,
    runtime_identity_paths: tuple[Path, ...],
) -> dict[str, object]:
    """Re-run live probes and require byte-equivalent sealed observations."""

    evidence = _load_evidence(path, "system preflight")
    required = {
        "schema_version", "status", "binding",
        "service_runtime_not_before_mtime_ns", "inputs", "checks",
    }
    if set(evidence) != required or evidence.get("schema_version") != 1:
        raise RuntimeError("system preflight evidence schema is invalid")
    if evidence.get("status") != "passed" or evidence.get("binding") != expected_binding:
        raise RuntimeError("system preflight status or binding drifted")
    if not runtime_identity_paths:
        raise RuntimeError("system preflight lacks vLLM runtime sidecars")
    not_before = max(item.stat().st_mtime_ns for item in runtime_identity_paths)
    runtime_records = [_load_evidence(item, "vLLM runtime") for item in runtime_identity_paths]
    try:
        allowed_pids = tuple(int(item["pid"]) for item in runtime_records)
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("vLLM runtime sidecar PID is invalid") from exc
    if evidence.get("service_runtime_not_before_mtime_ns") != not_before:
        raise RuntimeError("system preflight vLLM runtime epoch drifted")
    inputs = evidence.get("inputs")
    if not isinstance(inputs, dict) or set(inputs) != {
        "ray_address", "bounded_baseline_root"
    }:
        raise RuntimeError("system preflight inputs are incomplete")
    observed = build_system_preflight_payload(
        expected_binding,
        matched,
        ray_address=str(inputs["ray_address"]),
        bounded_baseline_root=Path(str(inputs["bounded_baseline_root"])),
        not_before_mtime_ns=not_before,
        allowed_vllm_root_pids=allowed_pids,
    )
    if evidence != observed:
        raise RuntimeError("system preflight live observations drifted")
    return {
        "status": "passed",
        "evidence_sha256": sha256_file(path),
        "checks": observed["checks"],
    }


def validate_correctness_smoke_evidence(
    path: Path,
    expected_binding: dict[str, object],
    system_preflight_sha256: str,
    matched: MatchedSystemConfig,
    expected_native_provenance: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    """Deep-check the actual five-arm smoke root referenced by its index."""

    if path.name != "matrix_index.json":
        raise RuntimeError("correctness smoke evidence must be matrix_index.json")
    expected = {
        "repository_commit": expected_binding["repository_commit"],
        "config_sha256": expected_binding["config_sha256"],
        "resolved_config_sha256": expected_binding["resolved_config_sha256"],
        "manifest_sha256": matched.arms[0].manifest_sha256,
    }
    index = validate_completed_matrix_root(
        path.parent,
        matched,
        expected,
        execution_mode="correctness_smoke",
        expected_readiness_binding=expected_binding,
        expected_system_preflight_sha256=system_preflight_sha256,
        expected_native_provenance=expected_native_provenance,
    )
    return {
        "status": "passed",
        "evidence_sha256": sha256_file(path),
        "root": str(path.parent.resolve()),
        "completed_cells": len(index["cells"]),
    }


def audit_readiness(
    matched_config: Path,
    native_config: Path,
    project_config: Path,
    *,
    live_service: bool = False,
    installed_source_audit: Path | None = None,
    vllm_python: Path | None = None,
    runtime_identity_paths: tuple[Path, ...] = (),
    system_preflight_evidence: Path | None = None,
    correctness_smoke_evidence: Path | None = None,
    repository_commit: str | None = None,
) -> dict[str, object]:
    """Return a report that cannot confuse static success with rehearsal readiness."""

    matched, identity = load_and_validate_static_readiness(
        matched_config, native_config, project_config
    )
    native = load_native_multijob_config(native_config)
    expected_native_provenance = {
        arm_id: dict(fields)
        for arm_id, fields in native.native_implementation_provenance
    }
    if repository_commit is None:
        repository = Path(__file__).resolve().parents[4]
        repository_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repository, check=True,
            capture_output=True, text=True,
        ).stdout.strip()
    binding = {
        "repository_commit": repository_commit,
        "config_sha256": sha256_file(matched_config),
        "native_config_sha256": sha256_file(native_config),
        "project_config_sha256": sha256_file(project_config),
        "resolved_config_sha256": sha256_payload(
            resolved_matched_system_identity(matched)
        ),
        "service_identity_sha256": _fingerprint(identity),
    }
    report: dict[str, object] = {
        "schema_version": 1,
        "status": "static_config_passed",
        "rehearsal_ready": False,
        "config_scope": [
            str(matched_config.resolve()),
            str(native_config.resolve()),
            str(project_config.resolve()),
        ],
        "binding": binding,
        "static_bindings": "passed",
        "stages": {
            "static_config": "passed",
            "service_identity": "not_checked",
            "system_preflight": "not_checked",
            "correctness_smoke": "not_checked",
        },
    }
    if not live_service:
        return report
    if installed_source_audit is None:
        raise ValueError(
            "--live-service requires --installed-source-audit exact-SHA evidence"
        )
    if vllm_python is None or not runtime_identity_paths:
        raise ValueError(
            "--live-service requires --vllm-python and runtime identity sidecars"
        )
    report["service_identity"] = verify_rehearsal_service_identity(
        matched, matched_config, vllm_python, installed_source_audit,
        runtime_identity_paths,
    )
    report["stages"]["service_identity"] = "passed"
    report["status"] = "service_identity_passed"
    if system_preflight_evidence is None:
        return report
    system = validate_system_preflight_evidence(
        system_preflight_evidence, binding, matched, runtime_identity_paths
    )
    report["system_preflight"] = system
    report["stages"]["system_preflight"] = "passed"
    report["status"] = "system_preflight_passed"
    if correctness_smoke_evidence is None:
        return report
    smoke = validate_correctness_smoke_evidence(
        correctness_smoke_evidence, binding, system["evidence_sha256"], matched,
        expected_native_provenance,
    )
    report["correctness_smoke"] = smoke
    report["stages"]["correctness_smoke"] = "passed"
    report["status"] = "rehearsal_ready"
    report["rehearsal_ready"] = True
    return report
