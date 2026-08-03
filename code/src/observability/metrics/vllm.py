"""Prometheus scraping and vLLM counter-delta summaries."""

from __future__ import annotations

import statistics
from urllib import error, request


def parse_prometheus_metrics(text: str) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name_and_labels, _, value_text = line.rpartition(" ")
        if not name_and_labels or not value_text:
            continue
        name = name_and_labels.split("{", 1)[0]
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
        # TTFT mean (P0#1, simple part). The Histogram percentiles (P50/P95/P99)
        # need bucket handling and are out of scope here; only the mean is captured.
        "vllm_time_to_first_token_mean_s": _mean_delta(before, after, "vllm:time_to_first_token_seconds"),
    }
