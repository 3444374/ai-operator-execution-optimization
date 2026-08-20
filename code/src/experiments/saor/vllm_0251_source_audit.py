"""Read-only installed-source audit for the frozen vLLM 0.25.1 boundary."""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
from pathlib import Path


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


def audit_installed_vllm_0251() -> dict[str, object]:
    """Audit the actual installed package without importing a model or server."""

    spec = importlib.util.find_spec("vllm")
    if spec is None or spec.origin is None:
        return {
            "status": "blocked_runtime_not_installed",
            "frozen_version": FROZEN_VLLM_VERSION,
            "installed_version": "unavailable",
            "package_root": "unavailable",
            "source_files": {},
            "errors": ["vLLM is not installed in the current Python runtime"],
        }
    try:
        installed_version = importlib.metadata.version("vllm")
    except importlib.metadata.PackageNotFoundError:
        installed_version = "unavailable"
    package_root = Path(spec.origin).resolve().parent
    errors: list[str] = []
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
        }
    return {
        "status": "passed" if not errors else "blocked_source_drift",
        "frozen_version": FROZEN_VLLM_VERSION,
        "installed_version": installed_version,
        "package_root": str(package_root),
        "source_files": evidence,
        "errors": errors,
    }
