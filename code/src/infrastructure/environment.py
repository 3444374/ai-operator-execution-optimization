"""Machine, Python capability, and external-asset contracts for reproducible runs."""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import shutil
import subprocess
import sys
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .config_env import ENV_REFERENCE, expand_text


@dataclass(frozen=True)
class CheckResult:
    """One preflight observation with a stable machine-readable status."""

    check: str
    status: str
    detail: str


def load_env_file(path: Path) -> dict[str, str]:
    """Parse a command-free KEY=VALUE file with strict ``${NAME}`` expansion."""

    raw: dict[str, str] = {}
    for line_number, source_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = source_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            raise ValueError(f"{path}:{line_number}: expected KEY=VALUE")
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or not key.replace("_", "a").isalnum() or key[0].isdigit():
            raise ValueError(f"{path}:{line_number}: invalid environment key {key!r}")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        raw[key] = value

    # Explicit process variables override a checked-in example profile. Expansion is
    # iterative because MODEL_PATH commonly depends on MODEL_ROOT from the same file.
    combined = {**raw, **{key: os.environ[key] for key in raw if key in os.environ}}
    resolved = dict(os.environ)
    pending = dict(combined)
    while pending:
        progressed = False
        for key, value in list(pending.items()):
            missing = set(ENV_REFERENCE.findall(value)) - set(resolved)
            if missing:
                continue
            resolved[key] = expand_text(value, key, environment=resolved)
            del pending[key]
            progressed = True
        if not progressed:
            unresolved = ", ".join(
                f"{key} -> {sorted(set(ENV_REFERENCE.findall(value)) - set(resolved))}"
                for key, value in sorted(pending.items())
            )
            raise ValueError(f"unresolved environment references: {unresolved}")
    return {key: resolved[key] for key in raw}


def load_json_contract(path: Path, expected_kind: str) -> dict[str, Any]:
    """Load a versioned JSON contract and reject the wrong schema or kind."""

    decoded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict) or decoded.get("schema_version") != 1:
        raise ValueError(f"{path}: schema_version must be 1")
    if decoded.get("kind") != expected_kind:
        raise ValueError(f"{path}: kind must be {expected_kind!r}")
    return decoded


def selected_group_names(raw: str) -> tuple[str, ...]:
    """Normalize a comma-separated capability group list."""

    names = tuple(dict.fromkeys(item.strip() for item in raw.split(",") if item.strip()))
    if not names:
        raise ValueError("at least one capability group is required")
    return names


def check_environment(
    profile: dict[str, Any],
    manifest: dict[str, Any],
    environment: dict[str, str],
    groups: tuple[str, ...],
) -> tuple[CheckResult, ...]:
    """Check one machine profile without changing packages, files, or services."""

    results: list[CheckResult] = []
    _check_profile(profile, environment, results)
    python_groups = manifest.get("python_groups", {})
    if not isinstance(python_groups, dict):
        raise ValueError("assets manifest python_groups must be an object")
    seen_modules: set[str] = set()
    for group in groups:
        dependencies = python_groups.get(group)
        if not isinstance(dependencies, list):
            raise ValueError(f"unknown Python capability group: {group}")
        for dependency in dependencies:
            module = _required_string(dependency, "module")
            if module in seen_modules:
                continue
            seen_modules.add(module)
            distribution = _required_string(dependency, "install")
            available = importlib.util.find_spec(module) is not None
            results.append(
                CheckResult(
                    f"python:{group}:{module}",
                    "ok" if available else "missing",
                    f"import={module}; version={_module_version(module)}; "
                    f"install={distribution}",
                )
            )
    for asset in _assets_for_groups(manifest, groups):
        results.append(_check_asset(asset, environment))
    return tuple(results)


def missing_install_specs(
    manifest: dict[str, Any], groups: tuple[str, ...]
) -> tuple[str, ...]:
    """Return de-duplicated pip specs only for modules missing now."""

    specs: list[str] = []
    python_groups = manifest.get("python_groups", {})
    for group in groups:
        dependencies = python_groups.get(group)
        if not isinstance(dependencies, list):
            raise ValueError(f"unknown Python capability group: {group}")
        for dependency in dependencies:
            module = _required_string(dependency, "module")
            if importlib.util.find_spec(module) is None:
                specs.append(_required_string(dependency, "install"))
    return tuple(dict.fromkeys(specs))


def install_missing_python(
    python_executable: Path,
    specs: tuple[str, ...],
    *,
    dry_run: bool,
) -> list[str]:
    """Install explicitly selected missing capabilities into one interpreter."""

    command = [str(python_executable), "-m", "pip", "install", *specs]
    if specs and not dry_run:
        subprocess.run(command, check=True)
    return command


def download_asset(
    manifest: dict[str, Any],
    asset_id: str,
    environment: dict[str, str],
) -> Path:
    """Download one declared public asset; manual/licensed assets fail closed."""

    matches = [item for item in manifest.get("assets", []) if item.get("id") == asset_id]
    if len(matches) != 1:
        raise ValueError(f"unknown or duplicate asset id: {asset_id}")
    asset = matches[0]
    kind = _required_string(asset, "kind")
    target = Path(expand_text(_required_string(asset, "target"), asset_id, environment=environment))
    if _check_asset(asset, environment).status == "ok":
        return target
    if kind == "manual":
        raise RuntimeError(
            f"{asset_id} requires manual authorization: "
            + _required_string(asset, "instructions")
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    expected_bytes = int(asset.get("minimum_bytes", 1))
    partial = target.with_suffix(target.suffix + ".partial")
    already_present = partial.stat().st_size if partial.exists() else 0
    free_bytes = shutil.disk_usage(_nearest_existing_parent(target.parent)).free
    if free_bytes < max(0, expected_bytes - already_present):
        raise RuntimeError(
            f"insufficient disk for {asset_id}: free={free_bytes}, "
            f"minimum_remaining={max(0, expected_bytes - already_present)}"
        )
    if kind == "http_file":
        _download_http(_required_string(asset, "url"), target)
    elif kind == "huggingface_snapshot":
        try:
            from huggingface_hub import snapshot_download
        except ImportError as exc:
            raise RuntimeError(
                "huggingface_hub is missing; install the download capability group first"
            ) from exc
        snapshot_download(
            repo_id=_required_string(asset, "repo_id"),
            repo_type=str(asset.get("repo_type", "model")),
            local_dir=target,
        )
    else:
        raise ValueError(f"unsupported downloadable asset kind: {kind}")
    checked = _check_asset(asset, environment)
    if checked.status != "ok":
        raise RuntimeError(f"downloaded asset failed validation: {checked.detail}")
    return target


def report_payload(
    profile_path: Path,
    manifest_path: Path,
    groups: tuple[str, ...],
    results: tuple[CheckResult, ...],
) -> dict[str, Any]:
    """Build the portable preflight report stored beside experiment artifacts."""

    return {
        "schema_version": 1,
        "profile": str(profile_path),
        "assets_manifest": str(manifest_path),
        "groups": list(groups),
        "python": sys.version,
        "platform": platform.platform(),
        "status": (
            "ok"
            if all(item.status in {"ok", "optional_missing"} for item in results)
            else "missing"
        ),
        "checks": [asdict(item) for item in results],
    }


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Write a report without exposing a partially written JSON file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _check_profile(
    profile: dict[str, Any], environment: dict[str, str], results: list[CheckResult]
) -> None:
    allowed_platforms = profile.get("platforms", [])
    current_platform = platform.system()
    platform_ok = isinstance(allowed_platforms, list) and current_platform in allowed_platforms
    results.append(
        CheckResult(
            "machine:platform",
            "ok" if platform_ok else "missing",
            f"observed={current_platform}; allowed={allowed_platforms}",
        )
    )
    minimum_python = tuple(int(item) for item in profile.get("minimum_python", [3, 10]))
    python_ok = sys.version_info[: len(minimum_python)] >= minimum_python
    results.append(
        CheckResult(
            "machine:python",
            "ok" if python_ok else "missing",
            f"observed={platform.python_version()}; minimum={'.'.join(map(str, minimum_python))}",
        )
    )
    cpu_slots = (
        len(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else (os.cpu_count() or 1)
    )
    minimum_cpu = int(profile.get("minimum_cpu_slots", 1))
    results.append(
        CheckResult(
            "machine:cpu_slots",
            "ok" if cpu_slots >= minimum_cpu else "missing",
            f"observed={cpu_slots}; minimum={minimum_cpu}",
        )
    )
    for key in profile.get("required_environment", []):
        present = bool(environment.get(str(key), "").strip())
        results.append(
            CheckResult(
                f"environment:{key}",
                "ok" if present else "missing",
                "set" if present else "unset",
            )
        )
    for command in profile.get("commands", []):
        name = _required_string(command, "name")
        required = bool(command.get("required", True))
        path = shutil.which(name)
        results.append(
            CheckResult(
                f"command:{name}",
                "ok" if path else ("missing" if required else "optional_missing"),
                path or "not found on PATH",
            )
        )
    for declared_path in profile.get("paths", []):
        env_name = _required_string(declared_path, "env")
        required = bool(declared_path.get("required", True))
        raw_path = environment.get(env_name)
        exists = bool(raw_path) and Path(raw_path).expanduser().exists()
        results.append(
            CheckResult(
                f"path:{env_name}",
                "ok" if exists else ("missing" if required else "optional_missing"),
                raw_path or "unset",
            )
        )
    disk = profile.get("disk", {})
    if isinstance(disk, dict) and disk:
        root_key = str(disk.get("root_env", "ARTIFACT_ROOT"))
        raw_path = environment.get(root_key)
        if raw_path:
            probe = _nearest_existing_parent(Path(raw_path))
            free_gib = shutil.disk_usage(probe).free / (1024**3)
            minimum_gib = float(disk.get("minimum_free_gib", 0))
            results.append(
                CheckResult(
                    "machine:disk_free",
                    "ok" if free_gib >= minimum_gib else "missing",
                    f"path={probe}; free_gib={free_gib:.2f}; minimum={minimum_gib:.2f}",
                )
            )
        else:
            results.append(CheckResult("machine:disk_free", "missing", f"{root_key} unset"))
    _check_gpus(profile.get("gpu", {}), results)


def _check_gpus(gpu: object, results: list[CheckResult]) -> None:
    if not isinstance(gpu, dict) or not gpu.get("required", False):
        results.append(CheckResult("machine:gpu", "ok", "GPU not required by profile"))
        return
    executable = shutil.which("nvidia-smi")
    if executable is None:
        results.append(CheckResult("machine:gpu", "missing", "nvidia-smi not found"))
        return
    completed = subprocess.run(
        [
            executable,
            "--query-gpu=name,memory.total,driver_version",
            "--format=csv,noheader,nounits",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    rows = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    minimum_count = int(gpu.get("minimum_count", 1))
    minimum_memory = int(gpu.get("minimum_memory_mib", 0))
    memories: list[int] = []
    for row in rows:
        parts = [item.strip() for item in row.split(",")]
        if len(parts) >= 2:
            try:
                memories.append(int(parts[1]))
            except ValueError:
                pass
    ok = completed.returncode == 0 and len(rows) >= minimum_count and all(
        value >= minimum_memory for value in memories[:minimum_count]
    )
    results.append(
        CheckResult(
            "machine:gpu",
            "ok" if ok else "missing",
            f"count={len(rows)}; minimum_count={minimum_count}; "
            f"memory_mib={memories}; minimum_memory_mib={minimum_memory}; rows={rows}",
        )
    )


def _assets_for_groups(
    manifest: dict[str, Any], groups: tuple[str, ...]
) -> tuple[dict[str, Any], ...]:
    assets = manifest.get("assets", [])
    if not isinstance(assets, list):
        raise ValueError("assets manifest assets must be a list")
    selected = []
    for asset in assets:
        asset_groups = asset.get("groups", [])
        if (
            isinstance(asset, dict)
            and isinstance(asset_groups, list)
            and set(groups) & set(asset_groups)
        ):
            selected.append(asset)
    return tuple(selected)


def _check_asset(asset: dict[str, Any], environment: dict[str, str]) -> CheckResult:
    asset_id = _required_string(asset, "id")
    target = Path(expand_text(_required_string(asset, "target"), asset_id, environment=environment))
    required = bool(asset.get("required", True))
    directory_kinds = {"huggingface_snapshot", "manual"}
    exists = target.is_dir() if asset.get("kind") in directory_kinds else target.is_file()
    minimum_bytes = int(asset.get("minimum_bytes", 1))
    size = _path_size(target) if exists else 0
    checksum = asset.get("sha256")
    checksum_ok = True
    if exists and checksum:
        checksum_ok = _sha256(target) == checksum
    ok = exists and size >= minimum_bytes and checksum_ok
    status = "ok" if ok else ("missing" if required else "optional_missing")
    return CheckResult(
        f"asset:{asset_id}",
        status,
        f"target={target}; exists={exists}; bytes={size}; "
        f"minimum={minimum_bytes}; checksum_ok={checksum_ok}",
    )


def _download_http(url: str, target: Path) -> None:
    partial = target.with_suffix(target.suffix + ".partial")
    headers: dict[str, str] = {}
    mode = "wb"
    if partial.exists() and partial.stat().st_size:
        headers["Range"] = f"bytes={partial.stat().st_size}-"
        mode = "ab"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request) as response:
        # Some object stores ignore Range and return a full 200 response. Appending
        # that response would silently corrupt a resumed dataset.
        if mode == "ab" and getattr(response, "status", None) != 206:
            mode = "wb"
        with partial.open(mode) as output:
            shutil.copyfileobj(response, output, length=1024 * 1024)
    partial.replace(target)


def _path_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    if path.is_dir():
        return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
    return 0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _nearest_existing_parent(path: Path) -> Path:
    candidate = path.expanduser()
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def _required_string(mapping: object, key: str) -> str:
    if not isinstance(mapping, dict):
        raise ValueError("manifest entries must be objects")
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"manifest field {key} must be a non-empty string")
    return value


def _module_version(module: str) -> str:
    distributions = importlib.metadata.packages_distributions().get(module, [])
    versions = []
    for distribution in distributions:
        try:
            versions.append(f"{distribution}=={importlib.metadata.version(distribution)}")
        except importlib.metadata.PackageNotFoundError:
            continue
    if versions:
        return ",".join(versions)
    return "unknown" if importlib.util.find_spec(module) else "missing"
