"""GPU resource, energy, and MFU summaries."""

from __future__ import annotations

import math
import statistics
import subprocess
from collections.abc import Sequence

from .statistics import percentile


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
        timestamp = _finite_number(sample.get("sample_monotonic_s", sample.get("sample_epoch_s")))
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

def gpu_metadata(gpu_ids: Sequence[str] | None = None, *, use_nvml: bool = False) -> dict[str, str]:
    if use_nvml:
        return _gpu_metadata_nvml(gpu_ids)
    command = [
        "nvidia-smi",
        "--query-gpu=name,utilization.gpu,memory.used,memory.total,power.draw",
        "--format=csv,noheader,nounits",
    ]
    if gpu_ids:
        command.insert(1, f"--id={','.join(gpu_ids)}")
    try:
        completed = subprocess.run(
            command,
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
    lines = completed.stdout.strip().splitlines()
    rows = [[part.strip() for part in line.split(",")] for line in lines]
    if not rows or any(len(parts) != 5 for parts in rows):
        return {
            "gpu_metrics_status": "unavailable:unexpected_nvidia_smi_output",
            "gpu_name": "",
            "gpu_utilization_pct": "",
            "gpu_memory_used_mib": "",
            "gpu_memory_total_mib": "",
            "gpu_power_w": "",
        }
    unsupported_power = {
        "n/a",
        "[n/a]",
        "not supported",
        "[not supported]",
    }
    power_values = [
        float(parts[4])
        for parts in rows
        if parts[4].lower() not in unsupported_power
    ]
    if len(rows) == 1:
        parts = rows[0]
        return {
            "gpu_metrics_status": "snapshot",
            "gpu_name": parts[0],
            "gpu_utilization_pct": parts[1],
            "gpu_memory_used_mib": parts[2],
            "gpu_memory_total_mib": parts[3],
            "gpu_power_w": (
                parts[4]
                if parts[4].lower() not in unsupported_power
                else ""
            ),
        }
    return {
        "gpu_metrics_status": "snapshot",
        "gpu_name": ";".join(parts[0] for parts in rows),
        "gpu_utilization_pct": str(
            sum(float(parts[1]) for parts in rows) / len(rows)
        ),
        "gpu_memory_used_mib": str(
            sum(float(parts[2]) for parts in rows)
        ),
        "gpu_memory_total_mib": str(
            sum(float(parts[3]) for parts in rows)
        ),
        "gpu_power_w": str(sum(power_values)) if power_values else "",
    }


def _gpu_metadata_nvml(gpu_ids):
    """In-process observation only; never spawn an unregistered probe child."""
    fields = ("gpu_name", "gpu_utilization_pct", "gpu_memory_used_mib",
              "gpu_memory_total_mib", "gpu_power_w")
    missing = {field: "" for field in fields}
    if (not gpu_ids or any(not isinstance(value, str) or not value.startswith("GPU-") for value in gpu_ids)
            or len(set(gpu_ids)) != len(gpu_ids)):
        return dict(missing, gpu_metrics_status="unavailable:explicit_physical_GPU_UUIDs_required")
    initialized = False
    try:
        import pynvml

        pynvml.nvmlInit()
        initialized = True
        rows = []
        errors = []
        for gpu_uuid in gpu_ids:
            handle = pynvml.nvmlDeviceGetHandleByUUID(gpu_uuid)
            observed_uuid = pynvml.nvmlDeviceGetUUID(handle)
            observed_uuid = observed_uuid.decode() if isinstance(observed_uuid, bytes) else observed_uuid
            if observed_uuid != gpu_uuid:
                raise ValueError("NVML assigned physical UUID differs")
            row = {}
            readers = {
                "gpu_utilization_pct": lambda: pynvml.nvmlDeviceGetUtilizationRates(handle).gpu,
                "gpu_power_w": lambda: pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0,
            }
            for field, reader in readers.items():
                try:
                    row[field] = reader()
                except pynvml.NVMLError as error:
                    errors.append(field + ":" + type(error).__name__)
            try:
                memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
                row.update(gpu_memory_used_mib=memory.used / 1024**2,
                           gpu_memory_total_mib=memory.total / 1024**2)
            except pynvml.NVMLError as error:
                errors.append("gpu_memory:" + type(error).__name__)
            rows.append(row)
        result = dict(missing, gpu_name=";".join(gpu_ids),
                      gpu_metrics_status="partial:" + ",".join(errors) if errors else "snapshot:nvml")
        for field in fields[1:]:
            if all(field in row for row in rows):
                value = sum(row[field] for row in rows)
                result[field] = str(value / len(rows) if field == "gpu_utilization_pct" else value)
        return result
    except Exception as error:
        return dict(missing, gpu_metrics_status="unavailable:nvml:" + type(error).__name__)
    finally:
        if initialized:
            try:
                pynvml.nvmlShutdown()
            except Exception:
                pass


def host_metadata(*, include_status=False) -> dict[str, float | str]:
    """Host-wide CPU/memory observation, shared with the records driver.

    cpu_percent needs two observations on the same sampling thread; callers
    must mark its first observation as a baseline rather than a measured zero.
    """
    try:
        import psutil

        per_cpu = [float(value) for value in psutil.cpu_percent(interval=None, percpu=True)]
        memory = psutil.virtual_memory()
        result = {
            "host_cpu_busy_cores": sum(per_cpu) / 100.0,
            "host_cpu_per_core_max_pct": max(per_cpu, default=0.0),
            "host_memory_used_pct": float(memory.percent),
            "host_memory_available_mib": float(memory.available) / (1024 * 1024),
        }
        if include_status:
            result["host_metrics_status"] = "observed" if per_cpu else "partial:empty_cpu_observation"
            if not per_cpu:
                result.update(host_cpu_busy_cores="", host_cpu_per_core_max_pct="")
        return result
    except (ImportError, OSError) as error:
        result = {name: "" for name in (
            "host_cpu_busy_cores", "host_cpu_per_core_max_pct",
            "host_memory_used_pct", "host_memory_available_mib",
        )}
        if include_status:
            result["host_metrics_status"] = "unavailable:" + type(error).__name__
        return result
