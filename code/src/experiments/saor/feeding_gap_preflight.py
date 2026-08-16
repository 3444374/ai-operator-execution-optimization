"""Structured PG, Ray, and endpoint clean gate for the feeding-gap diagnostic."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from urllib import request
from urllib.parse import urlsplit, urlunsplit

from src.experiments.shared_vllm.config import (
    SharedVllmConfig,
    _argument_value,
    _csv_argument_values,
)
from src.observability.metrics import parse_prometheus_metrics


Probe = Callable[[], dict[str, object]]


def collect_pre_run_clean_gate(
    config: SharedVllmConfig,
    *,
    metrics_urls: tuple[str, ...],
    ray_address: str,
    endpoint_probe: Probe | None = None,
    postgres_probe: Probe | None = None,
    ray_probe: Probe | None = None,
) -> dict[str, object]:
    """Collect explicit clean-state evidence before creating experiment state."""

    def observe(probe: Probe) -> dict[str, object]:
        try:
            return probe()
        except Exception as exc:
            return {
                "status": "failed",
                "reason": f"probe_exception:{type(exc).__name__}",
            }

    endpoint = observe(endpoint_probe or _endpoint_probe(config, metrics_urls))
    postgres = observe(postgres_probe or _postgres_probe(config))
    ray_state = observe(ray_probe or _ray_probe(config, ray_address))
    checks = {
        "endpoints": endpoint,
        "postgresql": postgres,
        "ray": ray_state,
    }
    passed = all(item.get("status") == "passed" for item in checks.values())
    return {
        "schema_version": 1,
        "status": "passed" if passed else "failed",
        "observed_epoch_s": time.time(),
        "gate_scope": "pre_run_clean_state_not_performance_evidence",
        "checks": checks,
    }


def write_pre_run_clean_gate(path: Path, payload: dict[str, object]) -> None:
    """Publish the gate atomically so a partial record cannot look passed."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _endpoint_probe(
    config: SharedVllmConfig,
    metrics_urls: tuple[str, ...],
) -> Probe:
    def run() -> dict[str, object]:
        completion_urls = _csv_argument_values(
            config.common_args,
            "--completion-endpoint-urls",
        )
        if len(completion_urls) != len(metrics_urls):
            return {
                "status": "failed",
                "reason": "endpoint_and_metrics_topology_mismatch",
            }
        observations = []
        for index, (completion_url, metrics_url) in enumerate(
            zip(completion_urls, metrics_urls)
        ):
            parsed = urlsplit(completion_url)
            health_url = urlunsplit(
                (parsed.scheme, parsed.netloc, "/health", "", "")
            )
            with request.urlopen(health_url, timeout=3.0) as response:
                health_status = response.status
            with request.urlopen(metrics_url, timeout=3.0) as response:
                metrics = parse_prometheus_metrics(
                    response.read().decode("utf-8", errors="replace")
                )
            running = metrics.get("vllm:num_requests_running")
            waiting = metrics.get("vllm:num_requests_waiting")
            observations.append(
                {
                    "endpoint_index": index,
                    "health_status": health_status,
                    "running": running,
                    "waiting": waiting,
                    "idle": (
                        health_status == 200
                        and running == 0
                        and waiting == 0
                    ),
                }
            )
        passed = all(item["idle"] is True for item in observations)
        return {
            "status": "passed" if passed else "failed",
            "definition": "health_200_and_vllm_running_waiting_zero",
            "observations": observations,
        }

    return run


def _postgres_probe(config: SharedVllmConfig) -> Probe:
    def run() -> dict[str, object]:
        database_url = _argument_value(config.common_args, "--database-url", "")
        if not database_url:
            return {"status": "failed", "reason": "database_url_missing"}
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("PostgreSQL clean gate requires psycopg") from exc
        with psycopg.connect(database_url, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT current_database(), current_setting('server_version')"
                )
                database_name, server_version = cursor.fetchone()
                cursor.execute(
                    "SELECT extversion FROM pg_extension WHERE extname = 'vector'"
                )
                extension = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT count(*)
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                      AND state IS DISTINCT FROM 'idle'
                    """
                )
                other_active_sessions = int(cursor.fetchone()[0])
        passed = extension is not None and other_active_sessions == 0
        return {
            "status": "passed" if passed else "failed",
            "definition": (
                "connectable_pgvector_and_no_other_nonidle_session_in_database"
            ),
            "database_name": str(database_name),
            "server_version": str(server_version),
            "pgvector_version": str(extension[0]) if extension else "",
            "other_active_sessions": other_active_sessions,
        }

    return run


def _ray_probe(config: SharedVllmConfig, ray_address: str) -> Probe:
    def run() -> dict[str, object]:
        if not ray_address:
            return {"status": "failed", "reason": "ray_address_missing"}
        try:
            import ray
        except ImportError as exc:
            raise RuntimeError("Ray clean gate requires ray") from exc
        context = ray.init(
            address=ray_address,
            namespace=config.shared_credit_namespace,
            ignore_reinit_error=False,
            logging_level="ERROR",
        )
        del context
        try:
            actors = ray.util.list_named_actors(all_namespaces=True)
            relevant = []
            for actor in actors:
                if isinstance(actor, dict):
                    name = str(actor.get("name", ""))
                    namespace = str(actor.get("namespace", ""))
                else:
                    name = str(actor)
                    namespace = ""
                if (
                    namespace == config.shared_credit_namespace
                    or config.shared_credit_namespace in name
                ):
                    relevant.append({"name": name, "namespace": namespace})
            nodes = ray.nodes()
            alive_nodes = sum(bool(node.get("Alive")) for node in nodes)
            total = ray.cluster_resources()
            available = ray.available_resources()
        finally:
            ray.shutdown()
        held_cpu = max(
            0.0,
            float(total.get("CPU", 0.0))
            - float(available.get("CPU", 0.0)),
        )
        held_gpu = max(
            0.0,
            float(total.get("GPU", 0.0))
            - float(available.get("GPU", 0.0)),
        )
        passed = (
            alive_nodes > 0
            and not relevant
            and held_cpu <= 0.5
            and held_gpu <= 0.5
        )
        return {
            "status": "passed" if passed else "failed",
            "definition": (
                "alive_cluster_no_diagnostic_actor_and_no_held_cpu_gpu"
            ),
            "alive_nodes": alive_nodes,
            "diagnostic_named_actors": relevant,
            "cluster_cpu": float(total.get("CPU", 0.0)),
            "available_cpu": float(available.get("CPU", 0.0)),
            "cluster_gpu": float(total.get("GPU", 0.0)),
            "available_gpu": float(available.get("GPU", 0.0)),
            "held_cpu": held_cpu,
            "held_gpu": held_gpu,
        }

    return run
