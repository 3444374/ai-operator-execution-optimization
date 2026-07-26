"""Shared timing, host snapshot, and CSV metric helpers."""

from __future__ import annotations

import csv
import math
import statistics
import subprocess
import threading
import time
from collections.abc import Callable
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


class PeriodicSampler:
    """Collect timestamped resource snapshots without blocking the run loop."""

    def __init__(
        self,
        sample: Callable[[], dict[str, object]],
        *,
        interval_s: float = 0.25,
    ) -> None:
        if interval_s <= 0:
            raise ValueError("interval_s must be positive")
        self._sample = sample
        self._interval_s = interval_s
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._samples: list[dict[str, object]] = []
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    @property
    def samples(self) -> tuple[dict[str, object], ...]:
        with self._lock:
            return tuple(dict(item) for item in self._samples)

    @property
    def is_running(self) -> bool:
        return self._thread.is_alive()

    def close(self) -> None:
        self._stop.set()
        self._thread.join()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                values = self._sample()
            except Exception as exc:
                values = {"sample_status": f"unavailable:{type(exc).__name__}"}
            with self._lock:
                self._samples.append(
                    {
                        "sample_index": len(self._samples),
                        "sample_epoch_s": time.time(),
                        **values,
                    }
                )
            self._stop.wait(self._interval_s)


def preflight_metrics_schema(
    path: Path,
    fieldnames,
    *,
    allow_additional_fields: bool = False,
) -> None:
    expected = list(fieldnames)
    has_content = path.exists() and path.stat().st_size > 0
    if has_content:
        with path.open(newline="", encoding="utf-8") as existing:
            header = next(csv.reader(existing), [])
        matches = (
            set(expected).issubset(header)
            if allow_additional_fields
            else header == expected
        )
        if not matches:
            raise ValueError(
                "CSV schema mismatch: "
                f"existing header {header!r} != row keys {expected!r}"
            )


def append_metrics(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(row.keys())
    preflight_metrics_schema(path, fieldnames)
    has_content = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not has_content:
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


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        return None
    return resolved if math.isfinite(resolved) else None


def _series_stats(
    values: list[float],
    prefix: str,
) -> dict[str, float | str]:
    if not values:
        return {
            f"{prefix}_mean": "",
            f"{prefix}_p50": "",
            f"{prefix}_p95": "",
            f"{prefix}_max": "",
        }
    return {
        f"{prefix}_mean": statistics.mean(values),
        f"{prefix}_p50": percentile(values, 50),
        f"{prefix}_p95": percentile(values, 95),
        f"{prefix}_max": max(values),
    }


def resource_sample_stats(
    samples: list[dict],
    *,
    observed_tokens: int,
) -> dict[str, float | int | str]:
    if (
        not isinstance(observed_tokens, int)
        or isinstance(observed_tokens, bool)
        or observed_tokens < 0
    ):
        raise ValueError("observed_tokens must be a non-negative integer")

    gpu_utilization = []
    gpu_memory_used = []
    gpu_memory_utilization = []
    gpu_power = []
    running = []
    waiting = []
    kv_usage = []
    timed_power: list[tuple[float, float]] = []
    for sample in samples:
        utilization = _finite_number(sample.get("gpu_utilization_pct"))
        memory_used = _finite_number(sample.get("gpu_memory_used_mib"))
        memory_total = _finite_number(sample.get("gpu_memory_total_mib"))
        power = _finite_number(sample.get("gpu_power_w"))
        timestamp = _finite_number(sample.get("sample_epoch_s"))
        running_value = _finite_number(
            sample.get("vllm_num_requests_running")
        )
        waiting_value = _finite_number(
            sample.get("vllm_num_requests_waiting")
        )
        kv_value = _finite_number(sample.get("vllm_kv_cache_usage_perc"))
        if utilization is not None:
            gpu_utilization.append(utilization)
        if memory_used is not None:
            gpu_memory_used.append(memory_used)
        if (
            memory_used is not None
            and memory_total is not None
            and memory_total > 0
        ):
            gpu_memory_utilization.append(
                memory_used / memory_total * 100.0
            )
        if power is not None:
            gpu_power.append(power)
            if timestamp is not None:
                timed_power.append((timestamp, power))
        if running_value is not None:
            running.append(running_value)
        if waiting_value is not None:
            waiting.append(waiting_value)
        if kv_value is not None:
            kv_usage.append(kv_value)

    energy_j = 0.0
    energy_intervals = 0
    for (start_s, start_w), (end_s, end_w) in zip(
        timed_power,
        timed_power[1:],
    ):
        if end_s < start_s:
            raise ValueError("resource sample timestamps must be ordered")
        energy_j += (start_w + end_w) / 2.0 * (end_s - start_s)
        energy_intervals += 1

    metrics: dict[str, float | int | str] = {
        "resource_metrics_status": "ok" if samples else "unavailable",
        **_series_stats(gpu_utilization, "gpu_utilization_pct"),
        "gpu_utilization_below_10pct_ratio": (
            sum(value < 10.0 for value in gpu_utilization)
            / len(gpu_utilization)
            if gpu_utilization
            else ""
        ),
        "gpu_memory_used_mib_mean": (
            statistics.mean(gpu_memory_used)
            if gpu_memory_used
            else ""
        ),
        "gpu_memory_used_mib_max": (
            max(gpu_memory_used) if gpu_memory_used else ""
        ),
        "gpu_memory_utilization_pct_mean": (
            statistics.mean(gpu_memory_utilization)
            if gpu_memory_utilization
            else ""
        ),
        "gpu_memory_utilization_pct_max": (
            max(gpu_memory_utilization)
            if gpu_memory_utilization
            else ""
        ),
        "gpu_power_w_mean": (
            statistics.mean(gpu_power) if gpu_power else ""
        ),
        "gpu_power_w_max": max(gpu_power) if gpu_power else "",
        "gpu_energy_j": energy_j if energy_intervals else "",
        "energy_j_per_1k_observed_tokens": (
            energy_j / observed_tokens * 1000.0
            if energy_intervals and observed_tokens > 0
            else ""
        ),
        **_series_stats(running, "vllm_running"),
        **_series_stats(waiting, "vllm_waiting"),
        **_series_stats(kv_usage, "vllm_kv_cache_usage"),
    }
    return metrics


def estimate_mfu(
    *,
    estimated_flops: float,
    observed_tokens: int,
    operator_wall_s: float,
    model_flops_per_token: float,
    gpu_peak_tflops: float,
    precision: str,
) -> dict[str, float | str]:
    method = (
        "vllm_estimated_flops_per_gpu_delta"
        if estimated_flops > 0
        else "configured_flops_per_observed_token"
    )
    time_basis = "operator_wall_s"
    common: dict[str, float | str] = {
        "mfu_estimation_method": method,
        "mfu_time_basis": time_basis,
        "model_flops_per_token": model_flops_per_token,
        "gpu_peak_tflops": gpu_peak_tflops,
        "mfu_precision": precision,
    }
    inputs = {
        "estimated_flops": estimated_flops,
        "observed_tokens": observed_tokens,
        "operator_wall_s": operator_wall_s,
        "model_flops_per_token": model_flops_per_token,
        "gpu_peak_tflops": gpu_peak_tflops,
    }
    for name, value in inputs.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{name} must be numeric")
        if not math.isfinite(float(value)) or value < 0:
            raise ValueError(f"{name} must be finite and non-negative")
    if estimated_flops == 0 and model_flops_per_token == 0:
        return {
            **common,
            "mfu_status": "unavailable:missing_model_flops_per_token",
            "mfu_estimate": "",
        }
    if gpu_peak_tflops == 0:
        return {
            **common,
            "mfu_status": "unavailable:missing_gpu_peak_tflops",
            "mfu_estimate": "",
        }
    if operator_wall_s == 0:
        return {
            **common,
            "mfu_status": "unavailable:zero_operator_wall_s",
            "mfu_estimate": "",
        }
    if estimated_flops == 0 and observed_tokens == 0:
        return {
            **common,
            "mfu_status": "unavailable:zero_observed_tokens",
            "mfu_estimate": "",
        }
    numerator_flops = (
        estimated_flops
        if estimated_flops > 0
        else observed_tokens * model_flops_per_token
    )
    estimate = numerator_flops / (
        operator_wall_s * gpu_peak_tflops * 1e12
    )
    return {
        **common,
        "mfu_status": "ok" if estimate <= 1.0 else "estimate_exceeds_peak",
        "mfu_estimate": estimate,
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
    estimated_flops = _metric_delta(
        before,
        after,
        "vllm:estimated_flops_per_gpu_total",
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
    }


def gpu_metadata() -> dict[str, str]:
    try:
        completed = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,utilization.gpu,memory.used,memory.total,power.draw",
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
            "gpu_power_w": "",
        }
    first_line = completed.stdout.strip().splitlines()[0] if completed.stdout.strip() else ""
    parts = [part.strip() for part in first_line.split(",")]
    if len(parts) != 5:
        return {
            "gpu_metrics_status": "unavailable:unexpected_nvidia_smi_output",
            "gpu_name": "",
            "gpu_utilization_pct": "",
            "gpu_memory_used_mib": "",
            "gpu_memory_total_mib": "",
            "gpu_power_w": "",
        }
    power_value = parts[4]
    if power_value.lower() in {
        "n/a",
        "[n/a]",
        "not supported",
        "[not supported]",
    }:
        power_value = ""
    return {
        "gpu_metrics_status": "snapshot",
        "gpu_name": parts[0],
        "gpu_utilization_pct": parts[1],
        "gpu_memory_used_mib": parts[2],
        "gpu_memory_total_mib": parts[3],
        "gpu_power_w": power_value,
    }
