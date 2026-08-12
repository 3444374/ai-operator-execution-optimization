"""Shared-vLLM throughput, fairness, resource, and distribution summaries."""

from __future__ import annotations

import json
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
        observed_work = float(
            evidence.get("actual_work", evidence["predicted_work"])
        )
        jct_s = float(evidence["jct_s"])
        if (
            not math.isfinite(observed_work)
            or observed_work < 0
            or not math.isfinite(jct_s)
            or jct_s <= 0
            or weight <= 0
        ):
            raise ValueError(
                "service inputs must contain finite non-negative work, "
                "positive JCT, and positive weights"
            )
        rates.append(observed_work / jct_s / weight)
    return rates


def cumulative_service_disparity(
    job_evidence: list[dict[str, object]],
    weights: tuple[int, ...],
) -> dict[str, float | str]:
    """Report the final weighted cumulative-service gap across jobs.

    This is a descriptive trace-derived quantity, not a theoretical VTC/DRR
    lag bound: completed jobs may have different offered work.  Keeping the
    status explicit prevents it from being over-interpreted.
    """

    if len(job_evidence) != len(weights) or not job_evidence:
        raise ValueError("job evidence and weights must be aligned and non-empty")
    normalized = []
    for evidence, weight in zip(job_evidence, weights):
        actual_work = float(evidence["actual_work"])
        if weight <= 0 or not math.isfinite(actual_work) or actual_work < 0:
            raise ValueError("actual work must be finite and weights positive")
        normalized.append(actual_work / weight)
    disparity = max(normalized) - min(normalized)
    mean_service = sum(normalized) / len(normalized)
    cumulative = [0.0] * len(job_evidence)
    events = sorted(
        (
            float(completion_epoch_s),
            job_index,
            float(work),
        )
        for job_index, evidence in enumerate(job_evidence)
        for completion_epoch_s, work in evidence.get(
            "service_completion_events",
            (),
        )
    )
    max_overlap_disparity = 0.0
    max_overlap_ratio = 0.0
    overlap_samples = 0
    for completion_epoch_s, job_index, work in events:
        cumulative[job_index] += work
        active = []
        for index, evidence in enumerate(job_evidence):
            intervals = evidence.get("request_backlog_intervals", ())
            if intervals:
                backlogged = any(
                    float(arrival_epoch_s) <= completion_epoch_s
                    <= float(request_completion_epoch_s)
                    for arrival_epoch_s, request_completion_epoch_s in intervals
                )
            else:
                # Compatibility for historical evidence that only recorded the
                # coarse job lifetime. New formal runs always use request-level
                # backlog intervals.
                backlogged = (
                    float(evidence.get("arrival_start_epoch_s", float("inf")))
                    <= completion_epoch_s
                    <= float(
                        evidence.get("completion_end_epoch_s", float("-inf"))
                    )
                )
            if backlogged:
                active.append(index)
        if len(active) < 2:
            continue
        overlap_samples += 1
        active_service = [cumulative[index] / weights[index] for index in active]
        observed_disparity = max(active_service) - min(active_service)
        observed_mean = sum(active_service) / len(active_service)
        max_overlap_disparity = max(max_overlap_disparity, observed_disparity)
        max_overlap_ratio = max(
            max_overlap_ratio,
            observed_disparity / observed_mean if observed_mean > 0 else 0.0,
        )
    return {
        "service_disparity_status": (
            "ok:simultaneously_backlogged_jobs_descriptive"
            if overlap_samples
            else "unavailable:no_overlapping_completion_samples"
        ),
        "service_disparity_bound_status": (
            "unavailable:not_proven_for_current_credit_implementation"
        ),
        "normalized_cumulative_service_min": min(normalized),
        "normalized_cumulative_service_max": max(normalized),
        "normalized_cumulative_service_disparity": disparity,
        "normalized_cumulative_service_disparity_ratio": (
            disparity / mean_service if mean_service > 0 else 0.0
        ),
        "overlap_service_disparity_samples": overlap_samples,
        "max_overlap_normalized_service_disparity": max_overlap_disparity,
        "max_overlap_normalized_service_disparity_ratio": max_overlap_ratio,
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


def active_set_phase_summary(
    job_evidence: list[dict[str, object]],
    samples: list[dict[str, object]],
) -> dict[str, float | int | bool | str]:
    """Audit workload lifecycle separately from credit-policy mechanism.

    The audit derives phase boundaries from request arrival/completion evidence,
    never from configured labels. Lifecycle applies to every arm. Credit
    borrow/reclaim/reborrow only applies to policies that emit a credit trace.
    Neither gate claims that the selected policy improved performance.
    """

    unavailable = {
        "active_set_contract_status": "unavailable:requires_staggered_two_job_trace",
        "active_set_contract_passed": False,
        "active_set_lifecycle_status": "unavailable:requires_staggered_two_job_trace",
        "active_set_lifecycle_passed": False,
        "active_set_mechanism_applicable": False,
        "active_set_mechanism_status": "not_applicable:no_credit_trace",
        "active_set_mechanism_passed": False,
        "active_set_bulk_job_index": -1,
        "active_set_foreground_job_index": -1,
        "active_set_overlap_s": 0.0,
        "active_set_foreground_drained_first": False,
        "active_set_bulk_only_pre_samples": 0,
        "active_set_overlap_samples": 0,
        "active_set_bulk_only_post_samples": 0,
        "active_set_bulk_borrow_fraction_max": 0.0,
        "active_set_bulk_reborrow_fraction_max": 0.0,
    }
    if len(job_evidence) != 2:
        return unavailable
    starts = [float(item["arrival_start_epoch_s"]) for item in job_evidence]
    if math.isclose(starts[0], starts[1], abs_tol=1e-6):
        return unavailable
    bulk_index = min(range(2), key=starts.__getitem__)
    foreground_index = 1 - bulk_index
    bulk = job_evidence[bulk_index]
    foreground = job_evidence[foreground_index]
    foreground_start = float(foreground["arrival_start_epoch_s"])
    foreground_end = float(foreground["completion_end_epoch_s"])
    bulk_end = float(bulk["completion_end_epoch_s"])
    overlap_s = max(
        0.0,
        min(bulk_end, foreground_end) - foreground_start,
    )
    # Validity is defined only by the externally imposed lifecycle. Requiring
    # the foreground job to finish first would select baselines by the outcome
    # that the experiment is intended to compare.
    lifecycle_passed = bool(
        starts[bulk_index] < foreground_start
        and foreground_start < bulk_end
        and overlap_s > 0
    )
    foreground_drained_first = foreground_end < bulk_end
    lifecycle_status = (
        "ok:observed_staggered_two_job_overlap"
        if lifecycle_passed
        else "active_set_lifecycle_not_observed"
    )
    bulk_job_id = str(bulk["runtime_job_id"])
    foreground_job_id = str(foreground["runtime_job_id"])
    by_epoch: dict[float, dict[str, object]] = {}
    for sample in samples:
        observed_at = float(sample["observed_epoch_s"])
        aggregate = by_epoch.setdefault(
            observed_at,
            {"work_limit": 0.0, "active_work_by_job": {}},
        )
        work_limit = float(sample["work_limit"])
        if work_limit <= 0:
            raise ValueError("active-set trace work limit must be positive")
        aggregate["work_limit"] = float(aggregate["work_limit"]) + work_limit
        raw = sample.get("active_work_by_job", ())
        pairs = json.loads(raw) if isinstance(raw, str) else raw
        aggregate_by_job = aggregate["active_work_by_job"]
        if not isinstance(aggregate_by_job, dict):
            raise ValueError("active-set aggregate has invalid job mapping")
        for job_id, work in pairs:
            key = str(job_id)
            aggregate_by_job[key] = aggregate_by_job.get(key, 0.0) + float(work)

    pre_samples = 0
    overlap_samples = 0
    post_samples = 0
    post_bulk_fractions = []
    pre_bulk_fractions = []
    for observed_at, aggregate in sorted(by_epoch.items()):
        by_job = aggregate["active_work_by_job"]
        if not isinstance(by_job, dict):
            raise ValueError("active-set aggregate has invalid job mapping")
        bulk_work = by_job.get(bulk_job_id, 0.0)
        foreground_work = by_job.get(foreground_job_id, 0.0)
        if observed_at < foreground_start:
            is_bulk_only = bulk_work > 0 and foreground_work == 0
            pre_samples += is_bulk_only
            if is_bulk_only:
                pre_bulk_fractions.append(
                    bulk_work / float(aggregate["work_limit"])
                )
        elif observed_at <= foreground_end:
            overlap_samples += bulk_work > 0 and foreground_work > 0
        elif observed_at <= bulk_end:
            is_bulk_only = bulk_work > 0 and foreground_work == 0
            post_samples += is_bulk_only
            if is_bulk_only:
                work_limit = float(aggregate["work_limit"])
                post_bulk_fractions.append(bulk_work / work_limit)
    mechanism_applicable = bool(samples)
    equal_share_fraction = 1.0 / len(job_evidence)
    pre_borrow_observed = bool(
        pre_bulk_fractions
        and max(pre_bulk_fractions) > equal_share_fraction
    )
    post_reborrow_observed = bool(
        post_bulk_fractions
        and max(post_bulk_fractions) > equal_share_fraction
    )
    mechanism_passed = bool(
        mechanism_applicable
        and lifecycle_passed
        and pre_borrow_observed
        and overlap_samples
        and post_reborrow_observed
    )
    return {
        "active_set_contract_status": lifecycle_status,
        "active_set_contract_passed": lifecycle_passed,
        "active_set_lifecycle_status": lifecycle_status,
        "active_set_lifecycle_passed": lifecycle_passed,
        "active_set_mechanism_applicable": mechanism_applicable,
        "active_set_mechanism_status": (
            "ok:observed_bulk_borrow_reclaim_reborrow"
            if mechanism_passed
            else "active_set_mechanism_not_observed"
            if mechanism_applicable
            else "not_applicable:no_credit_trace"
        ),
        "active_set_mechanism_passed": mechanism_passed,
        "active_set_bulk_job_index": bulk_index,
        "active_set_foreground_job_index": foreground_index,
        "active_set_overlap_s": overlap_s,
        "active_set_foreground_drained_first": foreground_drained_first,
        "active_set_bulk_only_pre_samples": pre_samples,
        "active_set_overlap_samples": overlap_samples,
        "active_set_bulk_only_post_samples": post_samples,
        "active_set_bulk_borrow_fraction_max": (
            max(pre_bulk_fractions) if pre_bulk_fractions else 0.0
        ),
        "active_set_bulk_reborrow_fraction_max": (
            max(post_bulk_fractions) if post_bulk_fractions else 0.0
        ),
    }

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
