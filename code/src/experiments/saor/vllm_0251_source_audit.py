"""Read-only installed-source audit for the frozen vLLM 0.25.1 boundary."""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path

from src.infrastructure.config_env import expand_structure
from src.infrastructure.vllm_preflight import (
    VLLM_DISTRIBUTION_HASH_FIELDS,
    VLLM_SOURCE_HASH_FIELDS,
)


FROZEN_VLLM_VERSION = "0.25.1"
VLLM_0251_TAG_URL = "https://github.com/vllm-project/vllm/tree/v0.25.1"

_SOURCE_MARKERS = {
    "config/scheduler.py": (
        "scheduler_cls",
        "AsyncScheduler",
        'Literal["fcfs", "priority"]',
    ),
    "v1/core/sched/scheduler.py": (
        "class Scheduler",
        "create_request_queue",
        "self.waiting",
    ),
    "v1/core/sched/async_scheduler.py": ("class AsyncScheduler",),
    "v1/core/sched/request_queue.py": (
        "class FCFSRequestQueue",
        "class SchedulingPolicy",
    ),
    "v1/request.py": ("request_id", "client_index", "trace_headers"),
}


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_expected_service_identity(path: Path) -> dict[str, object]:
    """Load only the frozen signature from a matched config, without heavy deps."""

    decoded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("matched config must be an object")
    identity = expand_structure(
        decoded.get("service_identity"),
        "matched.service_identity",
    )
    if not isinstance(identity, dict):
        raise ValueError("matched service_identity must be an object")
    return identity


def _runtime_location() -> tuple[Path | None, str]:
    spec = importlib.util.find_spec("vllm")
    if spec is None or spec.origin is None:
        return None, "unavailable"
    try:
        installed_version = importlib.metadata.version("vllm")
    except importlib.metadata.PackageNotFoundError:
        installed_version = "unavailable"
    return Path(spec.origin).resolve().parent, installed_version


def audit_installed_vllm_0251(
    expected_identity: dict[str, object] | None = None,
    *,
    package_root: Path | None = None,
    installed_version: str | None = None,
) -> dict[str, object]:
    """Audit installed source; only exact frozen hashes may return ``passed``."""

    if package_root is None:
        package_root, detected_version = _runtime_location()
        installed_version = installed_version or detected_version
    if package_root is None:
        return {
            "status": "blocked_runtime_not_installed",
            "frozen_version": FROZEN_VLLM_VERSION,
            "installed_version": "unavailable",
            "package_root": "unavailable",
            "source_files": {},
            "errors": ["vLLM is not installed in the current Python runtime"],
        }
    package_root = Path(package_root).resolve()
    installed_version = installed_version or "unavailable"
    errors: list[str] = []
    if expected_identity is None:
        errors.append("frozen expected service/source identity was not provided")
    if installed_version != FROZEN_VLLM_VERSION:
        errors.append(
            f"installed vLLM {installed_version} is not frozen {FROZEN_VLLM_VERSION}"
        )
    evidence: dict[str, dict[str, object]] = {}
    for relative, markers in _SOURCE_MARKERS.items():
        path = package_root / relative
        if not path.is_file():
            errors.append(f"installed source is missing vllm/{relative}")
            continue
        source = path.read_text(encoding="utf-8")
        missing = [marker for marker in markers if marker not in source]
        if missing:
            errors.append(
                f"vllm/{relative} is missing audited markers: {', '.join(missing)}"
            )
        evidence[relative] = {
            "path": str(path),
            "sha256": sha256_file(path),
            "markers_present": not missing,
            "expected_sha256": (
                expected_identity.get(
                    next(
                        field for field, source_path in VLLM_SOURCE_HASH_FIELDS.items()
                        if source_path == relative
                    )
                )
                if expected_identity is not None else "unavailable"
            ),
        }
        if (
            expected_identity is not None
            and evidence[relative]["sha256"] != evidence[relative]["expected_sha256"]
        ):
            errors.append(f"installed source SHA-256 drifted: vllm/{relative}")
    distribution: dict[str, dict[str, object]] = {}
    dist_root = package_root.parent / f"vllm-{installed_version}.dist-info"
    for field, filename in VLLM_DISTRIBUTION_HASH_FIELDS.items():
        path = dist_root / filename
        if not path.is_file():
            errors.append(f"installed distribution is missing {dist_root.name}/{filename}")
            continue
        digest = sha256_file(path)
        expected = (
            expected_identity.get(field)
            if expected_identity is not None else "unavailable"
        )
        distribution[filename] = {
            "path": str(path),
            "sha256": digest,
            "expected_sha256": expected,
        }
        if expected_identity is not None and digest != expected:
            errors.append(f"installed distribution SHA-256 drifted: {filename}")
    if expected_identity is None:
        status = "blocked_expected_identity_missing"
    else:
        status = "passed" if not errors else "blocked_source_drift"
    return {
        "schema_version": 1,
        "status": status,
        "frozen_version": FROZEN_VLLM_VERSION,
        "installed_version": installed_version,
        "package_root": str(package_root),
        "distribution_files": distribution,
        "source_files": evidence,
        "errors": errors,
    }


def validate_source_audit_evidence(
    evidence: dict[str, object], expected_identity: dict[str, object]
) -> None:
    """Reject stale, incomplete, or non-exact installed-source evidence."""

    if evidence.get("status") != "passed":
        raise RuntimeError("installed-source audit did not pass exact hash checks")
    if evidence.get("installed_version") != expected_identity.get("service"):
        raise RuntimeError("installed-source audit vLLM version drifted")
    sources = evidence.get("source_files")
    distribution = evidence.get("distribution_files")
    if not isinstance(sources, dict) or not isinstance(distribution, dict):
        raise RuntimeError("installed-source audit evidence is incomplete")
    for field, relative in VLLM_SOURCE_HASH_FIELDS.items():
        entry = sources.get(relative)
        if (
            not isinstance(entry, dict)
            or entry.get("sha256") != expected_identity[field]
            or entry.get("expected_sha256") != expected_identity[field]
            or entry.get("markers_present") is not True
        ):
            raise RuntimeError(f"installed-source evidence drifted: {relative}")
    for field, filename in VLLM_DISTRIBUTION_HASH_FIELDS.items():
        entry = distribution.get(filename)
        if (
            not isinstance(entry, dict)
            or entry.get("sha256") != expected_identity[field]
            or entry.get("expected_sha256") != expected_identity[field]
        ):
            raise RuntimeError(f"installed distribution evidence drifted: {filename}")
