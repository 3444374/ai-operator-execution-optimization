"""Per-cell TTFT + per-GPU resource instrumentation for the multicard sweep.

The shared ``gate_runner`` captures vLLM token/queue counters but NOT the TTFT
histogram, and it does not sample GPU utilisation/power. The multicard sweep
needs both for the gate arms (bounded_http / duckdb_ai) so their TTFT and raw
GPU time-series are comparable to project_static (which emits both natively via
its profiler).

Rather than modify the shared runner (used by every gate, fully tested), the
sweep driver wraps each gate cell: it snapshots the full ``/metrics`` body on
every endpoint immediately before the cell, samples both GPUs on a background
thread for the duration of the cell, then snapshots ``/metrics`` again. The
per-endpoint TTFT/ITL histogram delta is computed with
``vllm_metric_delta_stats`` (the same helper the project profiler uses); the GPU
samples are written to a per-cell CSV (one row per (sample, gpu)).

This module is pure-Python and injectable (``metrics_snapshotter``,
``gpu_snapshotter``) so the delta/CSV logic is unit-testable without vLLM or
nvidia-smi.
"""

from __future__ import annotations

import csv
import statistics
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator

from src.observability.metrics.vllm import (
    scrape_prometheus_metrics,
    vllm_metric_delta_stats,
)


MetricsSnapshotter = Callable[[str], dict[str, float]]
GpuSnapshotter = Callable[[], list[dict[str, float]]]


def default_metrics_snapshotter(url: str, timeout_s: float = 5.0) -> dict[str, float]:
    """Real /metrics fetcher (used on the server). Injected away in tests."""
    return scrape_prometheus_metrics(url, timeout_s=timeout_s)


_NVIDIA_SMI_ARGS = [
    "--query-gpu=index,name,utilization.gpu,memory.used,memory.total,power.draw",
    "--format=csv,noheader,nounits",
]
_UNSUPPORTED_POWER = {"n/a", "[n/a]", "not supported", "[not supported]"}


def default_gpu_snapshotter() -> list[dict[str, float | str]]:
    """One snapshot per GPU index from nvidia-smi. Empty list if unavailable.

    Returns per-GPU rows (NOT aggregated, unlike metrics.resources.gpu_metadata)
    so the sweep records gpu0/gpu1 separately -- the project resource trace only
    captured gpu0, which is the gap this fills.
    """
    try:
        completed = subprocess.run(
            ["nvidia-smi", *_NVIDIA_SMI_ARGS],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    rows: list[dict[str, float | str]] = []
    for line in completed.stdout.strip().splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 6:
            continue
        index_s, name, util_s, mem_used_s, mem_total_s, power_s = parts
        power_s_lower = power_s.lower()
        try:
            rows.append(
                {
                    "gpu_index": int(index_s),
                    "gpu_name": name,
                    "gpu_utilization_pct": float(util_s),
                    "gpu_memory_used_mib": float(mem_used_s),
                    "gpu_memory_total_mib": float(mem_total_s),
                    "gpu_power_w": (
                        float(power_s) if power_s_lower not in _UNSUPPORTED_POWER else ""
                    ),
                }
            )
        except ValueError:
            continue
    return rows


def snapshot_all_endpoints(
    metrics_urls: tuple[str, ...],
    *,
    snapshotter: MetricsSnapshotter = default_metrics_snapshotter,
) -> dict[int, dict[str, float]]:
    """Full /metrics snapshot keyed by endpoint index."""
    return {index: snapshotter(url) for index, url in enumerate(metrics_urls)}


def endpoint_latency_deltas(
    before: dict[int, dict[str, float]],
    after: dict[int, dict[str, float]],
) -> dict[int, dict[str, float | int | str]]:
    """Per-endpoint TTFT/ITL histogram-quantile + counter deltas.

    ``before``/``after`` are full ``/metrics`` snapshots taken with the service
    idle (the gate guarantees idle before+after each cell), so the delta is the
    cell's own latency distribution. Returns the full ``vllm_metric_delta_stats``
    dict per endpoint (caller picks TTFT/ITL fields).
    """
    if set(before) != set(after):
        raise ValueError(
            f"endpoint set changed across the cell: before={sorted(before)} "
            f"after={sorted(after)}"
        )
    return {
        index: vllm_metric_delta_stats(before[index], after[index])
        for index in sorted(before)
    }


@dataclass
class GpuResourceSampler:
    """Background thread that samples both GPUs at a fixed interval.

    Started before a cell, stopped after; ``write_csv`` emits the per-sample
    rows. Injectable ``snapshotter`` + ``sleep`` for unit tests.
    """

    interval_s: float = 0.3
    snapshotter: GpuSnapshotter = default_gpu_snapshotter
    sleep: Callable[[float], None] = time.sleep
    monotonic: Callable[[], float] = time.monotonic
    samples: list[dict[str, float | str]] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None

    def start(self, *, base_epoch_s: float) -> None:
        self._stop.clear()
        self.samples = []
        self._thread = threading.Thread(
            target=self._loop, args=(base_epoch_s,), daemon=True
        )
        self._thread.start()

    def _loop(self, base_epoch_s: float) -> None:
        sample_index = 0
        while not self._stop.is_set():
            t = self.monotonic() - base_epoch_s
            for gpu in self.snapshotter():
                gpu = dict(gpu)
                gpu["sample_index"] = sample_index
                gpu["sample_epoch_s"] = t
                self.samples.append(gpu)
            sample_index += 1
            self._stop.wait(self.interval_s)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def write_csv(self, path: Path, *, experiment_id: str = "", phase: str = "formal") -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fields = [
            "sample_index", "sample_epoch_s", "gpu_index", "gpu_name",
            "gpu_utilization_pct", "gpu_memory_used_mib", "gpu_memory_total_mib",
            "gpu_power_w",
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for sample in self.samples:
                row = {**sample, "experiment_id": experiment_id, "phase": phase}
                writer.writerow({k: row.get(k, "") for k in fields})

    def summary(self) -> dict[str, float]:
        """Mean util/power per GPU index (gpu0/gpu1), matching the aggregator."""
        by_gpu: dict[int, list[dict[str, float | str]]] = {}
        for sample in self.samples:
            by_gpu.setdefault(int(sample["gpu_index"]), []).append(sample)
        out: dict[str, float] = {}
        for gpu_index, rows in sorted(by_gpu.items()):
            label = f"gpu{gpu_index}"
            utils = [float(r["gpu_utilization_pct"]) for r in rows if r.get("gpu_utilization_pct") != ""]
            powers = [float(r["gpu_power_w"]) for r in rows if r.get("gpu_power_w") != ""]
            out[f"{label}_util_mean"] = statistics.mean(utils) if utils else 0.0
            out[f"{label}_power_mean"] = statistics.mean(powers) if powers else 0.0
        out["n_samples"] = float(max((len(v) for v in by_gpu.values()), default=0))
        return out


@dataclass
class CellInstrumentation:
    """Result of an instrumented cell -- what the sweep driver writes per cell."""

    ttft_deltas: dict[int, dict] | None
    gpu_summary: dict[str, float]
    gpu_csv_path: Path | None


@contextmanager
def instrumented_cell(
    metrics_urls: tuple[str, ...],
    gpu_csv_path: Path | None,
    *,
    metrics_snapshotter: MetricsSnapshotter = default_metrics_snapshotter,
    gpu_snapshotter: GpuSnapshotter = default_gpu_snapshotter,
    sample_gpu: bool = True,
    interval_s: float = 0.3,
) -> Iterator[CellInstrumentation]:
    """Bracket a gate cell with a /metrics before/after + GPU sampler.

    Yields a ``CellInstrumentation`` whose ``ttft_deltas`` is populated only on
    exit (after the with-block's body -- the cell -- completes). The caller runs
    the actual cell (run_core_gate / a single shard) inside the with-block.
    """
    before = snapshot_all_endpoints(metrics_urls, snapshotter=metrics_snapshotter)
    sampler: GpuResourceSampler | None = None
    if sample_gpu and gpu_csv_path is not None:
        sampler = GpuResourceSampler(
            interval_s=interval_s,
            snapshotter=gpu_snapshotter,
        )
        sampler.start(base_epoch_s=time.monotonic())
    instrumentation = CellInstrumentation(
        ttft_deltas=None, gpu_summary={}, gpu_csv_path=gpu_csv_path
    )
    try:
        yield instrumentation
    finally:
        if sampler is not None:
            sampler.stop()
            sampler.write_csv(gpu_csv_path)  # type: ignore[arg-type]
            instrumentation.gpu_summary = sampler.summary()
        after = snapshot_all_endpoints(metrics_urls, snapshotter=metrics_snapshotter)
        try:
            instrumentation.ttft_deltas = endpoint_latency_deltas(before, after)
        except ValueError:
            instrumentation.ttft_deltas = None
