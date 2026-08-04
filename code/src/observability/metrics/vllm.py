"""Prometheus scraping and vLLM counter-delta summaries."""

from __future__ import annotations

import math
import re
import statistics
from urllib import error, request


_LE_LABEL = re.compile(r'(?:^|,)le="([^"]+)"(?:,|$)')


def parse_prometheus_metrics(text: str) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name_and_labels, _, value_text = line.rpartition(" ")
        if not name_and_labels or not value_text:
            continue
        name, separator, labels = name_and_labels.partition("{")
        # Preserve only the histogram boundary.  Other labels (model name,
        # endpoint, finish reason) remain intentionally aggregated, matching
        # the pre-existing counter semantics and allowing multi-endpoint
        # histogram buckets to be summed safely.
        if separator and name.endswith("_bucket"):
            match = _LE_LABEL.search(labels.rstrip("}"))
            if match:
                name = f'{name}{{le="{match.group(1)}"}}'
        try:
            value = float(value_text)
        except ValueError:
            continue
        metrics[name] = metrics.get(name, 0.0) + value
    return metrics

def scrape_prometheus_metrics(url: str, timeout_s: float = 5.0) -> dict[str, float]:
    try:
        with request.urlopen(url, timeout=timeout_s) as response:
            body = response.read()
    except (OSError, error.URLError):
        return {}
    return parse_prometheus_metrics(body.decode("utf-8", errors="replace"))

def aggregate_model_metric_snapshots(
    snapshots: list[dict[str, float]],
) -> dict[str, float]:
    """Aggregate independent model-service metrics without losing units."""
    if not snapshots or any(not snapshot for snapshot in snapshots):
        return {}
    names = {name for snapshot in snapshots for name in snapshot}
    aggregated = {}
    for name in names:
        values = [snapshot.get(name, 0.0) for snapshot in snapshots]
        if name == "vllm:kv_cache_usage_perc":
            aggregated[name] = max(values)
        elif name == "vllm:estimated_flops_per_gpu_total":
            aggregated[name] = statistics.mean(values)
        else:
            aggregated[name] = sum(values)
    return aggregated

def _metric_delta(before: dict[str, float], after: dict[str, float], name: str) -> float:
    return max(0.0, after.get(name, 0.0) - before.get(name, 0.0))

def _mean_delta(before: dict[str, float], after: dict[str, float], base_name: str) -> float:
    count_delta = _metric_delta(before, after, f"{base_name}_count")
    if count_delta <= 0:
        return 0.0
    sum_delta = _metric_delta(before, after, f"{base_name}_sum")
    return sum_delta / count_delta


def _histogram_quantile_delta(
    before: dict[str, float],
    after: dict[str, float],
    base_name: str,
    quantile: float,
) -> float | None:
    """Estimate a Prometheus histogram quantile from counter deltas.

    Buckets are cumulative.  The interpolation follows PromQL's classic
    histogram rule closely enough for experiment telemetry; ``None`` means
    that the server did not expose a usable histogram in this interval.
    """

    prefix = f"{base_name}_bucket{{le=\""
    buckets: list[tuple[float, float]] = []
    for name in set(before) | set(after):
        if not name.startswith(prefix) or not name.endswith('"}'):
            continue
        boundary_text = name[len(prefix) : -2]
        try:
            boundary = float(boundary_text)
        except ValueError:
            continue
        buckets.append((boundary, _metric_delta(before, after, name)))
    buckets.sort(key=lambda item: item[0])
    if not buckets or not 0.0 <= quantile <= 1.0:
        return None
    total = buckets[-1][1]
    if not math.isinf(buckets[-1][0]):
        count_delta = _metric_delta(before, after, f"{base_name}_count")
        total = max(total, count_delta)
    if total <= 0:
        return None
    rank = quantile * total
    previous_count = 0.0
    previous_bound = 0.0
    for index, (upper_bound, cumulative_count) in enumerate(buckets):
        if cumulative_count < rank:
            previous_count = cumulative_count
            previous_bound = upper_bound
            continue
        if math.isinf(upper_bound):
            return buckets[index - 1][0] if index else 0.0
        bucket_count = cumulative_count - previous_count
        if bucket_count <= 0:
            return upper_bound
        fraction = (rank - previous_count) / bucket_count
        return previous_bound + (upper_bound - previous_bound) * fraction
    return None

def vllm_metric_delta_stats(before: dict[str, float], after: dict[str, float]) -> dict[str, float | int | str]:
    status = "ok" if before and after else "unavailable"
    prompt_tokens = _metric_delta(before, after, "vllm:prompt_tokens_total")
    generation_tokens = _metric_delta(before, after, "vllm:generation_tokens_total")
    request_success = _metric_delta(before, after, "vllm:request_success_total")
    estimated_flops = _metric_delta(
        before,
        after,
        "vllm:estimated_flops_per_gpu_total",
    )
    # Prefix-cache attribution (P0#3): vLLM exposes both as cumulative token
    # counters; the delta ratio attributes routing gains to cache reuse.
    prefix_cache_queries = _metric_delta(
        before, after, "vllm:prefix_cache_queries_total"
    )
    prefix_cache_hits = _metric_delta(
        before, after, "vllm:prefix_cache_hits_total"
    )
    prefix_cache_hit_rate = (
        prefix_cache_hits / prefix_cache_queries
        if prefix_cache_queries > 0
        else 0.0
    )
    ttft_quantiles = [
        _histogram_quantile_delta(
            before, after, "vllm:time_to_first_token_seconds", quantile
        )
        for quantile in (0.50, 0.95, 0.99)
    ]
    itl_quantiles = [
        _histogram_quantile_delta(
            before, after, "vllm:inter_token_latency_seconds", quantile
        )
        for quantile in (0.50, 0.95, 0.99)
    ]
    ttft_histogram_status = (
        "ok"
        if all(value is not None for value in ttft_quantiles)
        else "unavailable:histogram_buckets_missing"
    )
    itl_histogram_status = (
        "ok"
        if all(value is not None for value in itl_quantiles)
        else "unavailable:histogram_buckets_missing"
    )
    latency_histogram_status = (
        "ok"
        if ttft_histogram_status == itl_histogram_status == "ok"
        else "unavailable:one_or_more_histograms_missing"
    )
    return {
        "vllm_metrics_status": status,
        "vllm_prompt_tokens_delta": int(prompt_tokens),
        "vllm_generation_tokens_delta": int(generation_tokens),
        "vllm_request_success_delta": int(request_success),
        "vllm_estimated_flops_per_gpu_delta": estimated_flops,
        "vllm_e2e_request_latency_mean_s": _mean_delta(before, after, "vllm:e2e_request_latency_seconds"),
        "vllm_request_queue_time_mean_s": _mean_delta(before, after, "vllm:request_queue_time_seconds"),
        "vllm_request_inference_time_mean_s": _mean_delta(before, after, "vllm:request_inference_time_seconds"),
        "vllm_request_prefill_time_mean_s": _mean_delta(before, after, "vllm:request_prefill_time_seconds"),
        "vllm_request_decode_time_mean_s": _mean_delta(before, after, "vllm:request_decode_time_seconds"),
        "vllm_num_requests_running_after": int(after.get("vllm:num_requests_running", 0.0)),
        "vllm_num_requests_waiting_after": int(after.get("vllm:num_requests_waiting", 0.0)),
        "vllm_kv_cache_usage_perc_after": after.get("vllm:kv_cache_usage_perc", 0.0),
        "vllm_prefix_cache_queries_delta": int(prefix_cache_queries),
        "vllm_prefix_cache_hits_delta": int(prefix_cache_hits),
        "vllm_prefix_cache_hit_rate": prefix_cache_hit_rate,
        "vllm_latency_histogram_status": latency_histogram_status,
        "vllm_ttft_histogram_status": ttft_histogram_status,
        "vllm_itl_histogram_status": itl_histogram_status,
        "vllm_time_to_first_token_mean_s": _mean_delta(before, after, "vllm:time_to_first_token_seconds"),
        "vllm_time_to_first_token_p50_s": ttft_quantiles[0] or 0.0,
        "vllm_time_to_first_token_p95_s": ttft_quantiles[1] or 0.0,
        "vllm_time_to_first_token_p99_s": ttft_quantiles[2] or 0.0,
        "vllm_inter_token_latency_mean_s": _mean_delta(
            before, after, "vllm:inter_token_latency_seconds"
        ),
        "vllm_inter_token_latency_p50_s": itl_quantiles[0] or 0.0,
        "vllm_inter_token_latency_p95_s": itl_quantiles[1] or 0.0,
        "vllm_inter_token_latency_p99_s": itl_quantiles[2] or 0.0,
    }


def observed_slo_scale_metrics(
    vllm_stats: dict,
    *,
    ttft_target_ms: float,
    itl_target_ms: float,
) -> dict[str, float | str]:
    """Report how many times the configured TTFT/ITL target the P99 uses."""

    ratios = []
    if ttft_target_ms > 0:
        if vllm_stats["vllm_ttft_histogram_status"] != "ok":
            return {
                "observed_p99_slo_scale_status": "unavailable:ttft_histogram_missing",
                "observed_p99_slo_scale": 0.0,
            }
        ratios.append(
            float(vllm_stats["vllm_time_to_first_token_p99_s"])
            / (ttft_target_ms / 1000.0)
        )
    if itl_target_ms > 0:
        if vllm_stats["vllm_itl_histogram_status"] != "ok":
            return {
                "observed_p99_slo_scale_status": "unavailable:itl_histogram_missing",
                "observed_p99_slo_scale": 0.0,
            }
        ratios.append(
            float(vllm_stats["vllm_inter_token_latency_p99_s"])
            / (itl_target_ms / 1000.0)
        )
    return {
        "observed_p99_slo_scale_status": (
            "ok" if ratios else "unavailable:slo_targets_not_configured"
        ),
        "observed_p99_slo_scale": max(ratios, default=0.0),
    }


def token_cost_metrics(
    vllm_stats: dict,
    *,
    input_price: float | None,
    output_price: float | None,
) -> dict[str, float | str]:
    """Apply explicit provider prices to observed token counter deltas."""

    if input_price is None or output_price is None:
        return {
            "token_cost_status": "unavailable:prices_not_configured",
            "input_cost_per_million_tokens_usd": input_price or 0.0,
            "output_cost_per_million_tokens_usd": output_price or 0.0,
            "observed_input_token_cost_usd": 0.0,
            "observed_output_token_cost_usd": 0.0,
            "observed_total_token_cost_usd": 0.0,
            "observed_cost_per_million_tokens_usd": 0.0,
        }
    input_tokens = float(vllm_stats["vllm_prompt_tokens_delta"])
    output_tokens = float(vllm_stats["vllm_generation_tokens_delta"])
    input_cost = input_tokens * input_price / 1_000_000.0
    output_cost = output_tokens * output_price / 1_000_000.0
    total_tokens = input_tokens + output_tokens
    total_cost = input_cost + output_cost
    return {
        "token_cost_status": (
            "ok"
            if vllm_stats["vllm_metrics_status"] == "ok"
            else "unavailable:vllm_metrics_missing"
        ),
        "input_cost_per_million_tokens_usd": input_price,
        "output_cost_per_million_tokens_usd": output_price,
        "observed_input_token_cost_usd": input_cost,
        "observed_output_token_cost_usd": output_cost,
        "observed_total_token_cost_usd": total_cost,
        "observed_cost_per_million_tokens_usd": (
            total_cost * 1_000_000.0 / total_tokens if total_tokens > 0 else 0.0
        ),
    }
