"""Read-only source and declared service identity checks for vLLM 0.25.1."""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import math
import re
import sys
from pathlib import Path

from src.infrastructure.config_env import expand_structure


SERVICE_IDENTITY_HASH_FIELDS = {
    "model_config_sha256": "config.json",
    "tokenizer_config_sha256": "tokenizer_config.json",
    "tokenizer_json_sha256": "tokenizer.json",
    "model_safetensors_index_sha256": "model.safetensors.index.json",
    "generation_config_sha256": "generation_config.json",
    "model_weight_00001_sha256": "model-00001-of-00004.safetensors",
    "model_weight_00002_sha256": "model-00002-of-00004.safetensors",
    "model_weight_00003_sha256": "model-00003-of-00004.safetensors",
    "model_weight_00004_sha256": "model-00004-of-00004.safetensors",
}

VLLM_DISTRIBUTION_HASH_FIELDS = {
    "vllm_metadata_sha256": "METADATA",
    "vllm_wheel_sha256": "WHEEL",
    "vllm_record_sha256": "RECORD",
}

VLLM_SOURCE_HASH_FIELDS = {
    "vllm_source_config_scheduler_sha256": "config/scheduler.py",
    "vllm_source_scheduler_sha256": "v1/core/sched/scheduler.py",
    "vllm_source_async_scheduler_sha256": "v1/core/sched/async_scheduler.py",
    "vllm_source_request_queue_sha256": "v1/core/sched/request_queue.py",
    "vllm_source_request_sha256": "v1/request.py",
}

_REQUIRED_SERVICE_IDENTITY_FIELDS = {
    "model",
    "model_path",
    "model_revision",
    "dtype",
    "service",
    "scheduler",
    "scheduling_policy",
    "max_model_len",
    "max_num_seqs",
    "max_num_batched_tokens",
    "chunked_prefill",
    "prefix_caching",
    "mfu_metrics",
    "enforce_eager",
    "compilation_mode",
    "gpu_memory_utilization",
    *SERVICE_IDENTITY_HASH_FIELDS,
    *VLLM_DISTRIBUTION_HASH_FIELDS,
    *VLLM_SOURCE_HASH_FIELDS,
}

_SHA256 = re.compile(r"[0-9a-f]{64}")

_REVISION = re.compile(r"[0-9a-f]{40}")

def validate_service_identity(identity: dict[str, object]) -> None:
    """Validate the complete frozen model/build/runtime identity."""

    missing = sorted(_REQUIRED_SERVICE_IDENTITY_FIELDS - set(identity))
    unknown = sorted(set(identity) - _REQUIRED_SERVICE_IDENTITY_FIELDS)
    if missing or unknown:
        raise ValueError(
            f"service identity fields invalid: missing={missing} unknown={unknown}"
        )
    for field in ("model", "model_path", "dtype", "service"):
        if not isinstance(identity[field], str) or not identity[field]:
            raise ValueError(f"service identity {field} must be non-empty")
    if identity["scheduler"] != "vllm_native_fcfs":
        raise ValueError("service identity scheduler must be vllm_native_fcfs")
    if identity["scheduling_policy"] != "fcfs":
        raise ValueError("service identity scheduling_policy must be fcfs")
    if identity["dtype"] != "bfloat16":
        raise ValueError("matched service identity dtype must be bfloat16")
    if identity["service"] != "0.25.1":
        raise ValueError("matched service identity vLLM version must be 0.25.1")
    if identity["compilation_mode"] != "vllm_compile":
        raise ValueError("matched service identity must use vllm_compile")
    revision = identity["model_revision"]
    if not isinstance(revision, str) or _REVISION.fullmatch(revision) is None:
        raise ValueError("service identity model_revision must be a 40-hex commit")
    for field in (
        *SERVICE_IDENTITY_HASH_FIELDS,
        *VLLM_DISTRIBUTION_HASH_FIELDS,
        *VLLM_SOURCE_HASH_FIELDS,
    ):
        value = identity[field]
        if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
            raise ValueError(f"service identity {field} must be a SHA-256")
    for field in ("max_model_len", "max_num_seqs", "max_num_batched_tokens"):
        value = identity[field]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"service identity {field} must be a positive integer")
    for field in (
        "chunked_prefill",
        "prefix_caching",
        "mfu_metrics",
        "enforce_eager",
    ):
        if not isinstance(identity[field], bool):
            raise ValueError(f"service identity {field} must be boolean")
    if not identity["chunked_prefill"] or not identity["prefix_caching"]:
        raise ValueError("matched service requires chunked prefill and prefix caching")
    if not identity["mfu_metrics"] or identity["enforce_eager"]:
        raise ValueError("matched service requires MFU metrics and compile mode")
    utilization = identity["gpu_memory_utilization"]
    if (
        isinstance(utilization, bool)
        or not isinstance(utilization, (int, float))
        or not math.isfinite(float(utilization))
        or not 0 < float(utilization) <= 1
    ):
        raise ValueError("service identity gpu_memory_utilization must be in (0, 1]")


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
    validate_service_identity(identity)
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
        "python_runtime": {
            "executable_argv0": sys.executable,
            "sys_prefix": sys.prefix,
            "package_root": str(package_root),
            "package_version": installed_version,
        },
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
    runtime = evidence.get("python_runtime")
    if not isinstance(runtime, dict):
        raise RuntimeError("installed-source audit Python runtime identity is missing")
    for field in ("executable_argv0", "sys_prefix", "package_root", "package_version"):
        if not isinstance(runtime.get(field), str) or not runtime[field]:
            raise RuntimeError(
                f"installed-source audit Python runtime {field} is missing"
            )
    if runtime["package_root"] != evidence.get("package_root"):
        raise RuntimeError("installed-source audit package path binding drifted")
    if runtime["package_version"] != evidence.get("installed_version"):
        raise RuntimeError("installed-source audit package version binding drifted")
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
