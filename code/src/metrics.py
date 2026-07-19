"""Shared timing, host snapshot, and CSV metric helpers."""

from __future__ import annotations

import csv
import math
import statistics
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from urllib import error, request


@dataclass
class StageTimer:
    name: str
    start_s: float
    elapsed_s: float = 0.0

    @classmethod
    def start(cls, name: str) -> "StageTimer":
        return cls(name=name, start_s=time.perf_counter())

    def stop(self) -> float:
        self.elapsed_s = time.perf_counter() - self.start_s
        return self.elapsed_s


def append_metrics(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil((p / 100.0) * len(ordered)) - 1
    index = min(max(index, 0), len(ordered) - 1)
    return ordered[index]


def batch_result_stats(results: list[dict]) -> dict[str, float | int]:
    rows = [int(result.get("rows", 0)) for result in results]
    tokens = [int(result.get("token_count", 0)) for result in results]
    service_s = [float(result.get("service_s", 0.0)) for result in results]
    return {
        "batch_rows_min": min(rows) if rows else 0,
        "batch_rows_max": max(rows) if rows else 0,
        "batch_rows_mean": statistics.mean(rows) if rows else 0.0,
        "batch_tokens_min": min(tokens) if tokens else 0,
        "batch_tokens_max": max(tokens) if tokens else 0,
        "batch_tokens_mean": statistics.mean(tokens) if tokens else 0.0,
        "batch_tokens_p50": percentile([float(value) for value in tokens], 50),
        "batch_tokens_p95": percentile([float(value) for value in tokens], 95),
        "batch_service_s_p50": percentile(service_s, 50),
        "batch_service_s_p95": percentile(service_s, 95),
        "batch_service_s_p99": percentile(service_s, 99),
    }


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
    return {
        "vllm_metrics_status": status,
        "vllm_prompt_tokens_delta": int(prompt_tokens),
        "vllm_generation_tokens_delta": int(generation_tokens),
        "vllm_request_success_delta": int(request_success),
        "vllm_e2e_request_latency_mean_s": _mean_delta(before, after, "vllm:e2e_request_latency_seconds"),
        "vllm_request_queue_time_mean_s": _mean_delta(before, after, "vllm:request_queue_time_seconds"),
        "vllm_request_inference_time_mean_s": _mean_delta(before, after, "vllm:request_inference_time_seconds"),
        "vllm_request_prefill_time_mean_s": _mean_delta(before, after, "vllm:request_prefill_time_seconds"),
        "vllm_request_decode_time_mean_s": _mean_delta(before, after, "vllm:request_decode_time_seconds"),
        "vllm_num_requests_running_after": int(after.get("vllm:num_requests_running", 0.0)),
        "vllm_num_requests_waiting_after": int(after.get("vllm:num_requests_waiting", 0.0)),
        "vllm_kv_cache_usage_perc_after": after.get("vllm:kv_cache_usage_perc", 0.0),
    }


def gpu_metadata() -> dict[str, str]:
    try:
        completed = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {
            "gpu_metrics_status": f"unavailable:{type(exc).__name__}",
            "gpu_name": "",
            "gpu_utilization_pct": "",
            "gpu_memory_used_mib": "",
            "gpu_memory_total_mib": "",
        }
    first_line = completed.stdout.strip().splitlines()[0] if completed.stdout.strip() else ""
    parts = [part.strip() for part in first_line.split(",")]
    if len(parts) != 4:
        return {
            "gpu_metrics_status": "unavailable:unexpected_nvidia_smi_output",
            "gpu_name": "",
            "gpu_utilization_pct": "",
            "gpu_memory_used_mib": "",
            "gpu_memory_total_mib": "",
        }
    return {
        "gpu_metrics_status": "snapshot",
        "gpu_name": parts[0],
        "gpu_utilization_pct": parts[1],
        "gpu_memory_used_mib": parts[2],
        "gpu_memory_total_mib": parts[3],
    }
