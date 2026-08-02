"""Shared-vLLM throughput, fairness, resource, and distribution summaries."""

from __future__ import annotations

import math

from src.observability.metrics import (
    aggregate_model_metric_snapshots,
    percentile,
    vllm_metric_delta_stats,
)


def group_metric_delta(
    before_snapshots: list[dict[str, float]],
    after_snapshots: list[dict[str, float]],
    *,
    duration_s: float,
) -> dict[str, float | int | str]:
    if not math.isfinite(duration_s) or duration_s <= 0:
        raise ValueError("duration_s must be finite and positive")
    before = aggregate_model_metric_snapshots(before_snapshots)
    after = aggregate_model_metric_snapshots(after_snapshots)
    raw = vllm_metric_delta_stats(before, after)
    prompt_tokens = int(raw["vllm_prompt_tokens_delta"])
    generation_tokens = int(raw["vllm_generation_tokens_delta"])
    return {
        "metrics_status": raw["vllm_metrics_status"],
        "prompt_tokens_delta": prompt_tokens,
        "generation_tokens_delta": generation_tokens,
        "request_success_delta": int(raw["vllm_request_success_delta"]),
        "estimated_flops_per_gpu_delta": float(
            raw["vllm_estimated_flops_per_gpu_delta"]
        ),
        "tokens_per_s": (prompt_tokens + generation_tokens) / duration_s,
        "duration_s": duration_s,
    }

def jain_fairness(values: list[float]) -> float:
    if not values:
        raise ValueError("fairness values must not be empty")
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
        for value in values
    ):
        raise ValueError("fairness values must be finite and non-negative")
    total = float(sum(values))
    if total == 0:
        return 0.0
    return total * total / (
        len(values) * sum(float(value) ** 2 for value in values)
    )

def normalized_job_service_rates(
    job_evidence: list[dict[str, object]],
    weights: tuple[int, ...],
) -> list[float]:
    if len(job_evidence) != len(weights):
        raise ValueError("job evidence and weights must have equal length")
    rates = []
    for evidence, weight in zip(job_evidence, weights):
        predicted_work = float(evidence["predicted_work"])
        jct_s = float(evidence["jct_s"])
        if (
            not math.isfinite(predicted_work)
            or predicted_work < 0
            or not math.isfinite(jct_s)
            or jct_s <= 0
            or weight <= 0
        ):
            raise ValueError(
                "service inputs must contain finite non-negative work, "
                "positive JCT, and positive weights"
            )
        rates.append(predicted_work / jct_s / weight)
    return rates

def group_resource_summary(
    samples: list[dict[str, object]],
    *,
    start_epoch_s: float | None = None,
    end_epoch_s: float | None = None,
) -> dict[str, float | str]:
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
    running_values = []
    waiting_values = []
    kv_values = []
    for epoch_samples in by_epoch.values():
        gpu_value = _optional_float(
            epoch_samples[0].get("gpu_utilization_pct")
        )
        if gpu_value is not None:
            gpu_values.append(gpu_value)
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
    if not by_epoch:
        return {
            "resource_metrics_status": "unavailable:no_samples",
            "gpu_utilization_pct_mean": "",
            "gpu_utilization_pct_p95": "",
            "gpu_utilization_pct_max": "",
            "vllm_running_mean": "",
            "vllm_running_p95": "",
            "vllm_running_max": "",
            "vllm_waiting_mean": "",
            "vllm_waiting_p95": "",
            "vllm_waiting_max": "",
            "vllm_kv_usage_mean": "",
            "vllm_kv_usage_p95": "",
            "vllm_kv_usage_max": "",
        }
    status = (
        "ok"
        if gpu_values and running_values and waiting_values and kv_values
        else "unavailable:incomplete_samples"
    )
    return {
        "resource_metrics_status": status,
        **_distribution_fields("gpu_utilization_pct", gpu_values),
        **_distribution_fields("vllm_running", running_values),
        **_distribution_fields("vllm_waiting", waiting_values),
        **_distribution_fields("vllm_kv_usage", kv_values),
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
        f"{prefix}_p95": percentile(values, 95),
        f"{prefix}_max": max(values),
    }
