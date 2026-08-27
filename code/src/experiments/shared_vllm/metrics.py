"""Shared-vLLM throughput, fairness, resource, and distribution summaries."""

from __future__ import annotations

import json
import math

from src.observability.metrics import (
    aggregate_model_metric_snapshots,
    vllm_metric_delta_stats,
)
from src.experiments.shared_vllm.active_set import active_set_phase_summary
from src.experiments.shared_vllm.fairness_metrics import (
    completion_accounted_service_fairness,
    cumulative_service_disparity,
    jain_fairness,
    normalized_job_service_rates,
)
from src.experiments.shared_vllm.ready_event_metrics import (
    bounded_ready_event_summary,
)
from src.experiments.shared_vllm.resource_metrics import group_resource_summary
from src.experiments.shared_vllm.saor_event_metrics import (
    bounded_saor_event_summary,
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
        "vllm_e2e_request_latency_mean_s": float(
            raw["vllm_e2e_request_latency_mean_s"]
        ),
        "vllm_request_queue_time_mean_s": float(
            raw["vllm_request_queue_time_mean_s"]
        ),
        "vllm_request_inference_time_mean_s": float(
            raw["vllm_request_inference_time_mean_s"]
        ),
        "vllm_request_prefill_time_mean_s": float(
            raw["vllm_request_prefill_time_mean_s"]
        ),
        "vllm_request_decode_time_mean_s": float(
            raw["vllm_request_decode_time_mean_s"]
        ),
        "vllm_prefix_cache_hit_rate": float(
            raw["vllm_prefix_cache_hit_rate"]
        ),
        "vllm_ttft_histogram_status": str(
            raw["vllm_ttft_histogram_status"]
        ),
        "vllm_itl_histogram_status": str(raw["vllm_itl_histogram_status"]),
        "vllm_time_to_first_token_p50_s": float(
            raw["vllm_time_to_first_token_p50_s"]
        ),
        "vllm_time_to_first_token_p95_s": float(
            raw["vllm_time_to_first_token_p95_s"]
        ),
        "vllm_time_to_first_token_p99_s": float(
            raw["vllm_time_to_first_token_p99_s"]
        ),
        "vllm_inter_token_latency_p50_s": float(
            raw["vllm_inter_token_latency_p50_s"]
        ),
        "vllm_inter_token_latency_p95_s": float(
            raw["vllm_inter_token_latency_p95_s"]
        ),
        "vllm_inter_token_latency_p99_s": float(
            raw["vllm_inter_token_latency_p99_s"]
        ),
        "tokens_per_s": (prompt_tokens + generation_tokens) / duration_s,
        "duration_s": duration_s,
    }

def shared_credit_trace_summary(
    samples: list[dict[str, object]],
    *,
    work_limit_per_endpoint: int,
    job_count: int,
) -> dict[str, float | str]:
    """Summarize observable idle credit and work borrowed above equal share."""
    if work_limit_per_endpoint <= 0 or job_count <= 0:
        raise ValueError("credit limits and job count must be positive")
    if not samples:
        return {
            "credit_trace_status": "unavailable:not_a_shared_policy",
            "credit_endpoint_idle_sample_fraction": 0.0,
            "credit_idle_capacity_fraction_mean": 0.0,
            "credit_borrowed_work_mean": 0.0,
            "credit_borrowed_work_max": 0.0,
        }
    idle_endpoint_samples = 0
    idle_capacity_fractions = []
    borrowed_work = []
    for sample in samples:
        sample_work_limit = float(
            sample.get("work_limit", work_limit_per_endpoint)
        )
        if sample_work_limit <= 0:
            raise ValueError("credit trace work limit must be positive")
        equal_share = sample_work_limit / job_count
        active_work = float(sample["active_work"])
        # A downshift does not revoke existing leases, so active work may
        # temporarily exceed the newly applied capacity while it drains.
        if active_work < 0:
            raise ValueError("credit trace active_work is outside capacity")
        idle_endpoint_samples += active_work == 0
        idle_capacity_fractions.append(
            max(0.0, sample_work_limit - active_work) / sample_work_limit
        )
        raw_by_job = sample.get("active_work_by_job", "[]")
        by_job = json.loads(raw_by_job) if isinstance(raw_by_job, str) else raw_by_job
        borrowed_work.append(
            sum(max(0.0, float(work) - equal_share) for _job, work in by_job)
        )
    return {
        "credit_trace_status": "ok:sampled_endpoint_credit",
        "credit_endpoint_idle_sample_fraction": (
            idle_endpoint_samples / len(samples)
        ),
        "credit_idle_capacity_fraction_mean": (
            sum(idle_capacity_fractions) / len(idle_capacity_fractions)
        ),
        "credit_borrowed_work_mean": sum(borrowed_work) / len(borrowed_work),
        "credit_borrowed_work_max": max(borrowed_work),
    }
