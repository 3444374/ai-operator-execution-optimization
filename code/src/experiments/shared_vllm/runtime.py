"""Ray shared-credit observation and group resource sampling."""

from __future__ import annotations

import json
import time
import warnings
from dataclasses import asdict
from pathlib import Path

from src.infrastructure.runtime_env import ray_runtime_env
from src.modalities.text.contracts import build_text_runtime_snapshot
from src.observability.metrics import gpu_metadata, scrape_prometheus_metrics
from src.scheduling.runtime.shared_credit_ray import get_or_create_shared_credit_client


_CODE_ROOT = Path(__file__).resolve().parents[3]

def _ray_runtime_env() -> dict[str, dict[str, str]]:
    return ray_runtime_env(_CODE_ROOT)

class _RayCreditObserver:
    def __init__(
        self,
        address: str,
        namespace: str,
        actor_name: str,
        endpoint_ids: tuple[str, ...],
    ) -> None:
        import ray

        self.ray = ray
        self.namespace = namespace
        self.actor_name = actor_name
        self.endpoint_ids = endpoint_ids
        self.actor = None
        if not ray.is_initialized():
            ray.init(
                address=address,
                ignore_reinit_error=True,
                runtime_env=_ray_runtime_env(),
            )

    def prewarm(
        self,
        *,
        request_limit: int,
        work_limit: int,
        quantum: int,
        policy: str = "drr",
    ) -> None:
        capacities = {
            endpoint_id: (request_limit, work_limit)
            for endpoint_id in self.endpoint_ids
        }
        client = get_or_create_shared_credit_client(
            self.ray,
            name=self.actor_name,
            namespace=self.namespace,
            capacities=capacities,
            quantum=quantum,
            policy=policy,
        )
        for endpoint_id in self.endpoint_ids:
            client.snapshot(endpoint_id)
        self.actor = client.actor

    def _resolve_actor(self):
        if self.actor is not None:
            return self.actor
        try:
            self.actor = self.ray.get_actor(
                self.actor_name,
                namespace=self.namespace,
            )
        except ValueError:
            return None
        return self.actor

    def sample(self, origin_epoch_s: float) -> list[dict[str, object]]:
        actor = self._resolve_actor()
        if actor is None:
            return []
        observed_epoch_s = time.time()
        snapshots = self.ray.get(
            [
                actor.snapshot.remote(endpoint_id)
                for endpoint_id in self.endpoint_ids
            ]
        )
        return [
            {
                "schema_version": 1,
                "observed_epoch_s": observed_epoch_s,
                "elapsed_s": observed_epoch_s - origin_epoch_s,
                **_snapshot_mapping(snapshot),
            }
            for snapshot in snapshots
        ]

    def final_snapshots(self) -> list[dict[str, object]]:
        actor = self._resolve_actor()
        if actor is None:
            raise RuntimeError("shared credit actor was never observed")
        snapshots = self.ray.get(
            [
                actor.snapshot.remote(endpoint_id)
                for endpoint_id in self.endpoint_ids
            ]
        )
        return [_snapshot_mapping(snapshot) for snapshot in snapshots]

    def cleanup(self) -> None:
        actor = self._resolve_actor()
        if actor is None:
            return
        try:
            self.ray.kill(actor, no_restart=True)
        except Exception as exc:
            warnings.warn(
                "shared credit actor cleanup failed: "
                f"{type(exc).__name__}:{exc}",
                RuntimeWarning,
                stacklevel=2,
            )
        finally:
            self.actor = None

def _snapshot_mapping(snapshot) -> dict[str, object]:
    mapping = asdict(snapshot)
    for key, value in tuple(mapping.items()):
        if isinstance(value, (list, tuple)):
            mapping[key] = json.dumps(value)
    return mapping

def _resource_sample(
    metrics_urls: tuple[str, ...],
    origin_epoch_s: float,
) -> list[dict[str, object]]:
    observed_epoch_s = time.time()
    gpu = gpu_metadata()
    rows = []
    for endpoint_index, metrics_url in enumerate(metrics_urls):
        metrics = scrape_prometheus_metrics(metrics_url)
        rows.append(
            {
                "schema_version": 1,
                "observed_epoch_s": observed_epoch_s,
                "elapsed_s": observed_epoch_s - origin_epoch_s,
                "endpoint_index": endpoint_index,
                "metrics_url": metrics_url,
                "running": metrics.get(
                    "vllm:num_requests_running",
                    "",
                ),
                "waiting": metrics.get(
                    "vllm:num_requests_waiting",
                    "",
                ),
                "kv_usage": metrics.get(
                    "vllm:kv_cache_usage_perc",
                    "",
                ),
                "gpu_metrics_status": gpu["gpu_metrics_status"],
                "gpu_utilization_pct": gpu["gpu_utilization_pct"],
                "gpu_memory_used_mib": gpu["gpu_memory_used_mib"],
                "gpu_power_w": gpu["gpu_power_w"],
            }
        )
    return rows


def build_observe_only_text_state_rows(
    credit_rows: list[dict[str, object]],
    resource_rows: list[dict[str, object]],
    *,
    endpoint_ids: tuple[str, ...],
    calibration_signature: str,
) -> list[dict[str, object]]:
    """Join one credit/resource sample into typed staged state evidence."""
    resources = {
        endpoint_ids[int(row["endpoint_index"])]: row
        for row in resource_rows
        if 0 <= int(row["endpoint_index"]) < len(endpoint_ids)
    }
    rows = []
    for credit in credit_rows:
        endpoint_id = str(credit["endpoint_id"])
        resource = resources.get(endpoint_id)
        if resource is None:
            continue
        waiting_raw = resource.get("waiting")
        waiting = (
            int(float(waiting_raw))
            if waiting_raw not in (None, "")
            else 0
        )
        observed_at_s = float(credit["observed_epoch_s"])
        snapshot = build_text_runtime_snapshot(
            active_work=int(credit["active_work"]),
            upstream_queued_work=int(credit["waiting_work"]),
            service_waiting_requests=waiting,
            active_requests=int(credit["active_requests"]),
            oldest_upstream_age_s=float(credit["oldest_waiting_age_s"]),
            observed_at_s=observed_at_s,
            capacity_work=int(credit["work_limit"]),
            calibration_signature=calibration_signature,
        )
        organizer = snapshot.for_stage("organizer")
        model = snapshot.for_stage("model")
        rows.append(
            {
                "schema_version": 1,
                "runtime_state_mode": "observe_only",
                "observed_epoch_s": observed_at_s,
                "elapsed_s": credit["elapsed_s"],
                "endpoint_id": endpoint_id,
                "calibration_signature": calibration_signature,
                "request_limit": int(credit["request_limit"]),
                "organizer_queued_work": organizer.queued_work,
                "organizer_oldest_queue_age_s": organizer.oldest_queue_age_s,
                "model_active_work": model.active_work,
                "model_queued_work_estimated": model.queued_work,
                "model_capacity_work": model.capacity_work,
                "vllm_running": resource.get("running", ""),
                "vllm_waiting": resource.get("waiting", ""),
                "vllm_kv_usage": resource.get("kv_usage", ""),
            }
        )
    return rows
