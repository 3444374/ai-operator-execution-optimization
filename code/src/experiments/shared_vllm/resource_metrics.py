"""Group-level GPU, vLLM, CPU, memory, and energy summaries."""

from __future__ import annotations

import math

from src.observability.metrics import percentile


TAIL_PERCENTILE = 95


def group_resource_summary(
    samples: list[dict[str, object]],
    *,
    start_epoch_s: float | None = None,
    end_epoch_s: float | None = None,
    observed_tokens: int | None = None,
) -> dict[str, float | str]:
    if (
        observed_tokens is not None
        and (
            isinstance(observed_tokens, bool)
            or not isinstance(observed_tokens, int)
            or observed_tokens < 0
        )
    ):
        raise ValueError("observed_tokens must be a non-negative integer")
    by_epoch: dict[float, list[dict[str, object]]] = {}
    for sample in samples:
        observed_epoch_s = float(sample["observed_epoch_s"])
        if (
            start_epoch_s is not None
            and observed_epoch_s < start_epoch_s
        ):
            continue
        if end_epoch_s is not None and observed_epoch_s > end_epoch_s:
            continue
        by_epoch.setdefault(observed_epoch_s, []).append(sample)
    gpu_values = []
    gpu_power_values = []
    timed_gpu_power: list[tuple[float, float]] = []
    running_values = []
    waiting_values = []
    kv_values = []
    host_cpu_busy_cores = []
    host_cpu_per_core_max_pct = []
    host_memory_used_pct = []
    host_memory_available_mib = []
    for observed_epoch_s, epoch_samples in by_epoch.items():
        gpu_value = _optional_float(
            epoch_samples[0].get("gpu_utilization_pct")
        )
        if gpu_value is not None:
            gpu_values.append(gpu_value)
        gpu_power = _optional_float(epoch_samples[0].get("gpu_power_w"))
        if gpu_power is not None:
            gpu_power_values.append(gpu_power)
            timed_gpu_power.append((observed_epoch_s, gpu_power))
        running = [
            value
            for sample in epoch_samples
            if (value := _optional_float(sample.get("running"))) is not None
        ]
        waiting = [
            value
            for sample in epoch_samples
            if (value := _optional_float(sample.get("waiting"))) is not None
        ]
        kv = [
            value
            for sample in epoch_samples
            if (value := _optional_float(sample.get("kv_usage"))) is not None
        ]
        if running:
            running_values.append(sum(running))
        if waiting:
            waiting_values.append(sum(waiting))
        if kv:
            kv_values.append(max(kv))
        for field, target in (
            ("host_cpu_busy_cores", host_cpu_busy_cores),
            ("host_cpu_per_core_max_pct", host_cpu_per_core_max_pct),
            ("host_memory_used_pct", host_memory_used_pct),
            ("host_memory_available_mib", host_memory_available_mib),
        ):
            value = _optional_float(epoch_samples[0].get(field))
            if value is not None:
                target.append(value)
    if not by_epoch:
        return {
            "resource_metrics_status": "unavailable:no_samples",
            "gpu_utilization_pct_mean": "",
            "gpu_utilization_pct_p95": "",
            "gpu_utilization_pct_max": "",
            **_distribution_fields("gpu_power_w", []),
            "gpu_energy_j": "",
            "energy_j_per_1k_observed_tokens": "",
            "vllm_running_mean": "",
            "vllm_running_p95": "",
            "vllm_running_max": "",
            "vllm_waiting_mean": "",
            "vllm_waiting_p95": "",
            "vllm_waiting_max": "",
            "vllm_kv_usage_mean": "",
            "vllm_kv_usage_p95": "",
            "vllm_kv_usage_max": "",
            **_distribution_fields("host_cpu_busy_cores", []),
            **_distribution_fields("host_cpu_per_core_max_pct", []),
            **_distribution_fields("host_memory_used_pct", []),
            **_distribution_fields("host_memory_available_mib", []),
        }
    status = (
        "ok"
        if gpu_values and running_values and waiting_values and kv_values
        else "unavailable:incomplete_samples"
    )
    energy_j = sum(
        (start_w + end_w) / 2.0 * (end_s - start_s)
        for (start_s, start_w), (end_s, end_w) in zip(
            timed_gpu_power,
            timed_gpu_power[1:],
        )
    )
    energy_observed = len(timed_gpu_power) >= 2
    return {
        "resource_metrics_status": status,
        **_distribution_fields("gpu_utilization_pct", gpu_values),
        **_distribution_fields("gpu_power_w", gpu_power_values),
        "gpu_energy_j": energy_j if energy_observed else "",
        "energy_j_per_1k_observed_tokens": (
            energy_j / observed_tokens * 1000.0
            if energy_observed and observed_tokens
            else ""
        ),
        **_distribution_fields("vllm_running", running_values),
        **_distribution_fields("vllm_waiting", waiting_values),
        **_distribution_fields("vllm_kv_usage", kv_values),
        **_distribution_fields("host_cpu_busy_cores", host_cpu_busy_cores),
        **_distribution_fields(
            "host_cpu_per_core_max_pct",
            host_cpu_per_core_max_pct,
        ),
        **_distribution_fields("host_memory_used_pct", host_memory_used_pct),
        **_distribution_fields(
            "host_memory_available_mib",
            host_memory_available_mib,
        ),
    }

def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        return None
    return resolved if math.isfinite(resolved) else None

def _distribution_fields(
    prefix: str,
    values: list[float],
) -> dict[str, float | str]:
    if not values:
        return {
            f"{prefix}_mean": "",
            f"{prefix}_p95": "",
            f"{prefix}_max": "",
        }
    return {
        f"{prefix}_mean": sum(values) / len(values),
        f"{prefix}_p95": percentile(values, TAIL_PERCENTILE),
        f"{prefix}_max": max(values),
    }
