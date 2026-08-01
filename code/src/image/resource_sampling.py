"""Low-overhead host/GPU resource samplers for image operator experiments."""

from __future__ import annotations

import json
import statistics
import subprocess
import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class GpuResourceSample:
    """One low-frequency device sample; optional values may be unsupported."""

    timestamp_s: float
    index: int
    utilization_pct: float
    memory_mib: float
    power_w: float | None = None
    sm_clock_mhz: float | None = None
    memory_clock_mhz: float | None = None
    pcie_generation: float | None = None
    pcie_width: float | None = None
    pcie_generation_max: float | None = None
    pcie_width_max: float | None = None


def summarize_gpu_samples(
    samples: list[GpuResourceSample],
    *,
    active_device_count: int,
    sample_window_s: float = 0.0,
) -> dict[str, object]:
    """Summarize visible-device and active-device GPU utilization separately."""
    if active_device_count <= 0:
        raise ValueError("active_device_count must be positive")
    per_device: dict[int, list[GpuResourceSample]] = {}
    for sample in samples:
        per_device.setdefault(sample.index, []).append(sample)
    summaries = {
        str(index): {
            "util_mean_pct": statistics.fmean(
                sample.utilization_pct for sample in values
            ),
            "util_peak_pct": max(sample.utilization_pct for sample in values),
            "memory_peak_mib": max(sample.memory_mib for sample in values),
            "power_mean_w": _optional_mean(sample.power_w for sample in values),
            "sm_clock_mean_mhz": _optional_mean(
                sample.sm_clock_mhz for sample in values
            ),
            "memory_clock_mean_mhz": _optional_mean(
                sample.memory_clock_mhz for sample in values
            ),
            "pcie_generation": _optional_max(
                sample.pcie_generation for sample in values
            ),
            "pcie_width": _optional_max(sample.pcie_width for sample in values),
            "pcie_generation_max": _optional_max(
                sample.pcie_generation_max for sample in values
            ),
            "pcie_width_max": _optional_max(
                sample.pcie_width_max for sample in values
            ),
            "samples": len(values),
        }
        for index, values in per_device.items()
    }
    ranked_devices = sorted(
        per_device,
        key=lambda index: max(sample.memory_mib for sample in per_device[index]),
        reverse=True,
    )
    active_devices = ranked_devices[: min(active_device_count, len(ranked_devices))]
    active_samples = [sample for sample in samples if sample.index in active_devices]
    active_util = [sample.utilization_pct for sample in active_samples]
    visible_util = [sample.utilization_pct for sample in samples]
    visible_memory = [sample.memory_mib for sample in samples]
    power_by_timestamp: dict[float, float] = {}
    for sample in active_samples:
        if sample.power_w is not None:
            power_by_timestamp[sample.timestamp_s] = (
                power_by_timestamp.get(sample.timestamp_s, 0.0) + sample.power_w
            )
    active_total_power = list(power_by_timestamp.values())
    active_power_mean_w = (
        statistics.fmean(active_total_power) if active_total_power else 0.0
    )
    return {
        # Backward-compatible visible-device aggregates. Single-GPU tracks may
        # include an idle second physical GPU and must prefer the active fields.
        "gpu_util_mean_pct": statistics.fmean(visible_util) if visible_util else 0.0,
        "gpu_util_peak_pct": max(visible_util, default=0.0),
        "gpu_memory_peak_mib": max(visible_memory, default=0.0),
        "gpu_samples": len(samples),
        "gpu_per_device_json": json.dumps(summaries, sort_keys=True),
        "gpu_active_util_mean_pct": (
            statistics.fmean(active_util) if active_util else 0.0
        ),
        "gpu_active_util_peak_pct": max(active_util, default=0.0),
        "gpu_active_device_count": len(active_devices),
        "gpu_active_devices_json": json.dumps(active_devices),
        "gpu_active_power_mean_w": active_power_mean_w,
        "gpu_active_power_peak_w": max(active_total_power, default=0.0),
        "gpu_energy_estimate_j": active_power_mean_w * max(0.0, sample_window_s),
        "gpu_active_sm_clock_mean_mhz": _optional_mean(
            sample.sm_clock_mhz for sample in active_samples
        ),
        "gpu_active_memory_clock_mean_mhz": _optional_mean(
            sample.memory_clock_mhz for sample in active_samples
        ),
        "gpu_active_pcie_generation": _optional_max(
            sample.pcie_generation for sample in active_samples
        ),
        "gpu_active_pcie_width": _optional_max(
            sample.pcie_width for sample in active_samples
        ),
        "gpu_active_pcie_generation_max": _optional_max(
            sample.pcie_generation_max for sample in active_samples
        ),
        "gpu_active_pcie_width_max": _optional_max(
            sample.pcie_width_max for sample in active_samples
        ),
    }


def _optional_mean(values) -> float | str:
    present = [value for value in values if value is not None]
    return statistics.fmean(present) if present else ""


def _optional_max(values) -> float | str:
    present = [value for value in values if value is not None]
    return max(present) if present else ""


def _parse_optional_float(value: str) -> float | None:
    normalized = value.strip().lower()
    if not normalized or "n/a" in normalized or "not supported" in normalized:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def summarize_cpu_samples(samples: list[list[float]]) -> dict[str, object]:
    """Summarize system-wide per-core samples without claiming actor attribution."""
    if not samples:
        return {
            "cpu_system_mean_pct": 0.0,
            "cpu_system_peak_pct": 0.0,
            "cpu_busy_cores_mean": 0.0,
            "cpu_busy_cores_peak": 0.0,
            "cpu_per_core_peak_pct": 0.0,
            "cpu_logical_count": 0,
            "cpu_samples": 0,
        }
    logical_count = max(len(sample) for sample in samples)
    aggregate_pct = [statistics.fmean(sample) for sample in samples if sample]
    busy_cores = [sum(sample) / 100.0 for sample in samples if sample]
    per_core = [value for sample in samples for value in sample]
    return {
        "cpu_system_mean_pct": statistics.fmean(aggregate_pct),
        "cpu_system_peak_pct": max(aggregate_pct),
        "cpu_busy_cores_mean": statistics.fmean(busy_cores),
        "cpu_busy_cores_peak": max(busy_cores),
        "cpu_per_core_peak_pct": max(per_core),
        "cpu_logical_count": logical_count,
        "cpu_samples": len(samples),
    }


class NvidiaSmiSampler:
    """Sample per-device utilization and distinguish active from idle GPUs."""

    def __init__(self, interval_s: float, *, active_device_count: int) -> None:
        if interval_s <= 0:
            raise ValueError("GPU sample interval must be positive")
        self.interval_s = interval_s
        self.active_device_count = active_device_count
        self._stop = threading.Event()
        self._samples: list[GpuResourceSample] = []
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._started_at: float | None = None

    def start(self) -> None:
        self._started_at = time.perf_counter()
        self._thread.start()

    def stop(self) -> dict[str, object]:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_s * 4))
        return summarize_gpu_samples(
            self._samples,
            active_device_count=self.active_device_count,
            sample_window_s=(
                time.perf_counter() - self._started_at if self._started_at else 0.0
            ),
        )

    def _run(self) -> None:
        extended_command = [
            "nvidia-smi",
            "--query-gpu=index,utilization.gpu,memory.used,power.draw,"
            "clocks.sm,clocks.mem,pcie.link.gen.current,pcie.link.width.current,"
            "pcie.link.gen.max,pcie.link.width.max",
            "--format=csv,noheader,nounits",
        ]
        basic_command = [
            "nvidia-smi",
            "--query-gpu=index,utilization.gpu,memory.used",
            "--format=csv,noheader,nounits",
        ]
        while not self._stop.is_set():
            sampled_at = time.perf_counter()
            try:
                completed = subprocess.run(
                    extended_command,
                    capture_output=True,
                    check=False,
                    text=True,
                    timeout=max(2.0, self.interval_s * 4),
                )
            except subprocess.TimeoutExpired:
                self._stop.wait(self.interval_s)
                continue
            if completed.returncode != 0:
                try:
                    completed = subprocess.run(
                        basic_command,
                        capture_output=True,
                        check=False,
                        text=True,
                        timeout=max(2.0, self.interval_s * 4),
                    )
                except subprocess.TimeoutExpired:
                    self._stop.wait(self.interval_s)
                    continue
            if completed.returncode == 0:
                for line in completed.stdout.splitlines():
                    fields = [item.strip() for item in line.split(",")]
                    if len(fields) == 10:
                        self._samples.append(
                            GpuResourceSample(
                                timestamp_s=sampled_at,
                                index=int(fields[0]),
                                utilization_pct=float(fields[1]),
                                memory_mib=float(fields[2]),
                                power_w=_parse_optional_float(fields[3]),
                                sm_clock_mhz=_parse_optional_float(fields[4]),
                                memory_clock_mhz=_parse_optional_float(fields[5]),
                                pcie_generation=_parse_optional_float(fields[6]),
                                pcie_width=_parse_optional_float(fields[7]),
                                pcie_generation_max=_parse_optional_float(fields[8]),
                                pcie_width_max=_parse_optional_float(fields[9]),
                            )
                        )
                    elif len(fields) == 3:
                        self._samples.append(
                            GpuResourceSample(
                                timestamp_s=sampled_at,
                                index=int(fields[0]),
                                utilization_pct=float(fields[1]),
                                memory_mib=float(fields[2]),
                            )
                        )
            self._stop.wait(self.interval_s)


class SystemCpuSampler:
    """Sample host-wide CPU utilization; values are not per-actor attribution."""

    def __init__(self, interval_s: float) -> None:
        if interval_s <= 0:
            raise ValueError("CPU sample interval must be positive")
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._samples: list[list[float]] = []
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> dict[str, object]:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_s * 4))
        return summarize_cpu_samples(self._samples)

    def _run(self) -> None:
        import psutil

        psutil.cpu_percent(interval=None, percpu=True)
        while not self._stop.wait(self.interval_s):
            self._samples.append(
                [float(value) for value in psutil.cpu_percent(interval=None, percpu=True)]
            )
