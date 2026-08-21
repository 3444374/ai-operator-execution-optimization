"""Run and seal the live, read-only system checks required before SAOR smoke.

This module probes endpoint health, PostgreSQL/pgvector, and a clean Ray/GPU
cluster, then deep-validates a same-service bounded HTTP baseline root.  It does
not issue inference requests or mutate the database/cluster.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Callable, Mapping
from urllib.request import Request, urlopen

from src.experiments.saor.native_system_contract import MatchedSystemConfig
from src.experiments.saor.native_system_matched import endpoint_auxiliary_url


def _sha256(path: Path) -> str:
    """Hash one raw preflight artifact in bounded-memory chunks."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_object(path: Path, label: str) -> dict[str, object]:
    """Load one required JSON object from a raw baseline root."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"bounded baseline {label} is unreadable") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"bounded baseline {label} must be an object")
    return value


def probe_endpoint_health(endpoint_urls: tuple[str, ...]) -> dict[str, object]:
    """GET every endpoint health route and retain status/body digests only."""

    endpoints: list[dict[str, object]] = []
    for endpoint in endpoint_urls:
        health_url = endpoint_auxiliary_url(endpoint, "/health")
        request = Request(health_url, method="GET")
        with urlopen(request, timeout=10) as response:  # noqa: S310 - frozen localhost URLs
            body = response.read(1024 * 1024)
            status = int(response.status)
        if not 200 <= status < 300:
            raise RuntimeError(f"endpoint health returned HTTP {status}")
        endpoints.append({
            "health_url": health_url,
            "http_status": status,
            "body_sha256": hashlib.sha256(body).hexdigest(),
        })
    return {"status": "passed", "endpoints": endpoints}


def probe_postgresql(database_url: str) -> dict[str, object]:
    """Read server and pgvector versions without exposing the database URL."""

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("DRIVER_PYTHON lacks psycopg") from exc
    with psycopg.connect(database_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW server_version")
            server_row = cursor.fetchone()
            cursor.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'vector'"
            )
            vector_row = cursor.fetchone()
    if not server_row or not server_row[0] or not vector_row or not vector_row[0]:
        raise RuntimeError("PostgreSQL or pgvector identity is unavailable")
    return {
        "status": "passed",
        "database_url_sha256": hashlib.sha256(database_url.encode("utf-8")).hexdigest(),
        "server_version": str(server_row[0]),
        "pgvector_version": str(vector_row[0]),
    }


def _is_descendant_of(pid: int, allowed_roots: set[int]) -> bool:
    """Return whether a Linux PID is one of, or descends from, a vLLM root."""

    seen: set[int] = set()
    current = pid
    while current > 1 and current not in seen:
        if current in allowed_roots:
            return True
        seen.add(current)
        try:
            status = Path(f"/proc/{current}/status").read_text(encoding="utf-8")
        except OSError:
            return False
        parent_line = next(
            (line for line in status.splitlines() if line.startswith("PPid:")), None
        )
        if parent_line is None:
            return False
        try:
            current = int(parent_line.split(":", 1)[1].strip())
        except ValueError:
            return False
    return current in allowed_roots


def _gpu_compute_pids() -> set[int]:
    """Read actual CUDA compute PIDs from nvidia-smi without shell parsing."""

    completed = subprocess.run(
        [
            "nvidia-smi", "--query-compute-apps=pid",
            "--format=csv,noheader,nounits",
        ],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("nvidia-smi compute-process query failed")
    pids: set[int] = set()
    for line in completed.stdout.splitlines():
        value = line.strip()
        if not value:
            continue
        if not value.isdigit():
            raise RuntimeError("nvidia-smi returned a malformed compute PID")
        pids.add(int(value))
    return pids


def validate_gpu_compute_processes(
    allowed_vllm_root_pids: tuple[int, ...],
) -> dict[str, object]:
    """Reject CUDA PIDs outside the verified vLLM process trees."""

    allowed_roots = set(allowed_vllm_root_pids)
    if not allowed_roots:
        raise RuntimeError("GPU process gate lacks verified vLLM root PIDs")
    compute_pids = _gpu_compute_pids()
    unexpected = sorted(
        pid for pid in compute_pids if not _is_descendant_of(pid, allowed_roots)
    )
    if unexpected:
        raise RuntimeError("GPU has a compute process outside verified vLLM trees")
    return {
        "allowed_vllm_root_pids": sorted(allowed_roots),
        "gpu_compute_pids": sorted(compute_pids),
        "unexpected_gpu_compute_pid_count": len(unexpected),
    }


def _is_allowed_idle_control_plane_actor(item: Mapping[str, object]) -> bool:
    """Recognize Ray's single official, zero-CPU/GPU LLM telemetry actor."""

    required = item.get("required_resources")
    return (
        item.get("class_name") == "_TelemetryAgent"
        and item.get("name") == "llm_batch_telemetry"
        and item.get("ray_namespace") == "llm_batch_telemetry"
        and item.get("is_detached") is True
        and item.get("placement_group_id") in (None, "")
        and required == {"node:__internal_head__": 0.001}
    )


def probe_ray_gpu_clean(
    ray_address: str, allowed_vllm_root_pids: tuple[int, ...]
) -> dict[str, object]:
    """Reject live workloads while allowing Ray's inert telemetry control plane."""

    try:
        import ray
        from ray.util.state import list_actors, list_placement_groups
    except (ImportError, AttributeError) as exc:
        raise RuntimeError("DRIVER_PYTHON lacks Ray state APIs") from exc
    if ray.is_initialized():
        raise RuntimeError("Ray was already initialized before system preflight")
    ray.init(address=ray_address)
    try:
        cluster = ray.cluster_resources()
        available = ray.available_resources()
        total_cpu = float(cluster.get("CPU", 0.0))
        available_cpu = float(available.get("CPU", 0.0))
        total_gpu = float(cluster.get("GPU", 0.0))
        available_gpu = float(available.get("GPU", 0.0))
        alive_actors = [
            item for item in list_actors(detail=True)
            if item.get("state") == "ALIVE"
        ]
        control_plane_actors = [
            item for item in alive_actors
            if _is_allowed_idle_control_plane_actor(item)
        ]
        workload_actors = [
            item for item in alive_actors
            if not _is_allowed_idle_control_plane_actor(item)
        ]
        groups = [
            item for item in list_placement_groups(detail=True)
            if item.get("state") != "REMOVED"
        ]
    finally:
        ray.shutdown()
    if (
        total_cpu <= 0
        or available_cpu != total_cpu
        or total_gpu <= 0
        or available_gpu != total_gpu
        or workload_actors
        or len(control_plane_actors) > 1
        or groups
    ):
        raise RuntimeError("Ray/GPU cluster is not clean and fully idle")
    gpu_processes = validate_gpu_compute_processes(allowed_vllm_root_pids)
    return {
        "status": "passed",
        "ray_address": ray_address,
        "total_cpu": total_cpu,
        "available_cpu": available_cpu,
        "total_gpu": total_gpu,
        "available_gpu": available_gpu,
        "alive_workload_actor_count": len(workload_actors),
        "allowed_control_plane_actor_count": len(control_plane_actors),
        "allowed_control_plane_actors": [
            {
                "class_name": str(item.get("class_name")),
                "name": str(item.get("name")),
                "ray_namespace": str(item.get("ray_namespace")),
                "required_resources": dict(item.get("required_resources")),
            }
            for item in control_plane_actors
        ],
        "active_placement_group_count": len(groups),
        **gpu_processes,
    }


def validate_bounded_baseline_root(
    root: Path,
    config: MatchedSystemConfig,
    *,
    not_before_mtime_ns: int,
) -> dict[str, object]:
    """Recompute the bounded gate from its raw run status, gate, and shards."""

    root = root.resolve()
    status_path = root / "run_status.json"
    gate_paths = list(root.glob("*/gate.json"))
    summary_paths = sorted(root.glob("*/shard_*/summary.json"))
    if len(gate_paths) != 1 or len(summary_paths) != len(config.endpoint_urls):
        raise RuntimeError("bounded baseline root is incomplete")
    paths = [status_path, gate_paths[0], *summary_paths]
    if any(not path.is_file() for path in paths):
        raise RuntimeError("bounded baseline raw artifact is missing")
    if any(path.stat().st_mtime_ns < not_before_mtime_ns for path in paths):
        raise RuntimeError("bounded baseline predates the current vLLM runtime")
    status = _json_object(status_path, "run status")
    gate = _json_object(gate_paths[0], "gate")
    if status.get("status") != "passed" or status.get("blocked_cells") not in (None, []):
        raise RuntimeError("bounded baseline run did not pass")
    if gate.get("status") != "passed" or gate.get("passed") is not True:
        raise RuntimeError("bounded baseline gate did not pass")
    protocol = config.arms[0].protocol
    service = dict(config.service_identity)
    service_payload = json.dumps({
        "model": service["model"],
        "protocol": protocol,
        "temperature": 0.0,
        "ignore_eos": False,
        "service_prefix_caching": (
            "enabled" if service["prefix_caching"] else "disabled"
        ),
        "service_max_num_seqs": service["max_num_seqs"],
        "service_max_num_batched_tokens": service["max_num_batched_tokens"],
    }, sort_keys=True, separators=(",", ":")).encode("utf-8")
    expected_service_sha256 = hashlib.sha256(service_payload).hexdigest()
    expected_urls = set(config.endpoint_urls)
    observed_urls: set[str] = set()
    total_tokens_per_s = 0.0
    artifacts: list[dict[str, object]] = []
    for path in summary_paths:
        summary = _json_object(path, "shard summary")
        url = summary.get("endpoint_url")
        throughput = summary.get("tokens_per_s")
        if (
            summary.get("adapter") != "bounded_http"
            or summary.get("status") != "completed"
            or summary.get("exactly_once") is not True
            or summary.get("failed_count") != 0
            or summary.get("completion_protocol") != protocol
            or summary.get("model_name") != service["model"]
            or summary.get("service_config_sha256") != expected_service_sha256
            or not isinstance(url, str)
            or isinstance(throughput, bool)
            or not isinstance(throughput, (int, float))
            or float(throughput) <= 0
        ):
            raise RuntimeError("bounded baseline shard correctness evidence failed")
        observed_urls.add(url)
        total_tokens_per_s += float(throughput)
        artifacts.append({
            "path": path.relative_to(root).as_posix(),
            "sha256": _sha256(path),
            "endpoint_url": url,
            "tokens_per_s": float(throughput),
        })
    if observed_urls != expected_urls:
        raise RuntimeError("bounded baseline endpoint identity drifted")
    return {
        "status": "passed",
        "root": str(root),
        "run_status_sha256": _sha256(status_path),
        "gate_sha256": _sha256(gate_paths[0]),
        "shards": artifacts,
        "total_tokens_per_s": total_tokens_per_s,
        "service_config_sha256": expected_service_sha256,
    }


def build_system_preflight_payload(
    binding: dict[str, object],
    config: MatchedSystemConfig,
    *,
    ray_address: str,
    bounded_baseline_root: Path,
    not_before_mtime_ns: int,
    allowed_vllm_root_pids: tuple[int, ...],
    endpoint_probe: Callable[[tuple[str, ...]], dict[str, object]] = probe_endpoint_health,
    postgresql_probe: Callable[[str], dict[str, object]] = probe_postgresql,
    ray_probe: Callable[[str, tuple[int, ...]], dict[str, object]] = probe_ray_gpu_clean,
) -> dict[str, object]:
    """Execute all read-only probes and return the exact sealable evidence."""

    database_urls = {str(dict(arm.source)["database_url"]) for arm in config.arms}
    if len(database_urls) != 1:
        raise RuntimeError("matched arms do not share one PostgreSQL source")
    checks = {
        "endpoint_health": endpoint_probe(config.endpoint_urls),
        "postgresql": postgresql_probe(next(iter(database_urls))),
        "ray_gpu_clean": ray_probe(ray_address, allowed_vllm_root_pids),
        "bounded_baseline": validate_bounded_baseline_root(
            bounded_baseline_root, config,
            not_before_mtime_ns=not_before_mtime_ns,
        ),
    }
    if any(item.get("status") != "passed" for item in checks.values()):
        raise RuntimeError("one or more live system preflight probes failed")
    return {
        "schema_version": 1,
        "status": "passed",
        "binding": binding,
        "service_runtime_not_before_mtime_ns": not_before_mtime_ns,
        "inputs": {
            "ray_address": ray_address,
            "bounded_baseline_root": str(bounded_baseline_root.resolve()),
        },
        "checks": checks,
    }
