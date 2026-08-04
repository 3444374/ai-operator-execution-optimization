"""Derived, scale-aware metrics for image AI operator runs.

These metrics add no instrumentation overhead.  They normalize already
observed timing and resource totals so differently sized capacity runs can be
described without pretending that their absolute JCTs are matched-workload
measurements.
"""

from __future__ import annotations

import math


STEADY_STATE_MIN_S = 60.0


# Stored in every schema-v12 manifest and beside every augmented historical CSV.
# ``measurement_kind`` distinguishes direct clocks/counters from sampled
# estimates and algebraic derivations, which is essential when auditing an
# unexpected value.
IMAGE_METRIC_DEFINITIONS: dict[str, dict[str, object]] = {
    "operator_e2e_s": {
        "meaning_zh": "算子执行从冷模型 worker 边界到最后一批 embedding 返回的墙钟时间",
        "unit": "seconds",
        "measurement_kind": "direct_monotonic_clock",
        "formula": "result.total_s + explicit_project_worker_setup_s",
        "source": "time.perf_counter in each execution arm",
        "comparison_scope": "absolute ranking requires the same workload size and boundary",
        "limitations": "excludes Ray cluster startup and pgvector writeback",
    },
    "first_output_s": {
        "meaning_zh": "从同一冷启动边界到第一个完整 Arrow record batch 返回的时间",
        "unit": "seconds",
        "measurement_kind": "direct_monotonic_clock",
        "formula": "first completed output timestamp - operator start",
        "source": "stream iterator or first completed project Ray batch",
        "comparison_scope": "absolute ranking requires matched workload scale",
        "limitations": "first batch size may differ; it is not LLM TTFT or per-image latency",
    },
    "post_first_output_s": {
        "meaning_zh": "首批结果返回后到整个算子结束的剩余墙钟时间",
        "unit": "seconds",
        "measurement_kind": "derived",
        "formula": "operator_e2e_s - first_output_s",
        "source": "operator_e2e_s and first_output_s",
        "comparison_scope": "matched workload for absolute ranking",
        "limitations": "does not reveal how many images were already materialized internally",
    },
    "first_output_fraction_of_e2e": {
        "meaning_zh": "首批结果出现时已经消耗的算子总时间比例，越接近 1 越像先物化后返回",
        "unit": "ratio_0_to_1",
        "measurement_kind": "derived",
        "formula": "first_output_s / operator_e2e_s",
        "source": "operator_e2e_s and first_output_s",
        "comparison_scope": "cross-scale descriptive streaming/materialization signal only",
        "limitations": "dimensionless does not mean workload-independent; not normalized latency",
    },
    "images_per_s": {
        "meaning_zh": "处理图片行数吞吐，logical pass 会计入处理行数但不增加 unique images",
        "unit": "images_per_second",
        "measurement_kind": "derived",
        "formula": "processed rows / operator_e2e_s",
        "source": "workload row contract and operator monotonic clock",
        "comparison_scope": "cross-scale only after each arm independently reaches a plateau",
        "limitations": "short runs can be dominated by setup and are not steady-state capacity",
    },
    "steady_state_duration_gate_met": {
        "meaning_zh": "单次算子运行是否达到预注册的 60 秒最低稳态代理时长",
        "unit": "boolean",
        "measurement_kind": "derived_guard",
        "formula": "operator_e2e_s >= 60",
        "source": "operator_e2e_s",
        "comparison_scope": "run validity guard",
        "limitations": "passing 60 seconds does not prove a throughput plateau",
    },
    "cpu_core_seconds_estimate": {
        "meaning_zh": "整个主机在算子窗口内估算消耗的 CPU 核秒",
        "unit": "core_seconds",
        "measurement_kind": "sampled_estimate",
        "formula": "host mean equivalent busy cores * operator_e2e_s",
        "source": "psutil host-wide per-core utilization samples",
        "comparison_scope": "same host and sampling contract; normalize per image across scale",
        "limitations": "includes unrelated host work and is not actor-attributed",
    },
    "images_per_cpu_core_second": {
        "meaning_zh": "每消耗一个 CPU 核秒完成的图片数",
        "unit": "images_per_core_second",
        "measurement_kind": "derived_from_sampled_estimate",
        "formula": "processed rows / cpu_core_seconds_estimate",
        "source": "row count and host CPU estimate",
        "comparison_scope": "per-image resource comparison on the same host",
        "limitations": "inherits host-wide CPU sampling noise",
    },
    "gpu_active_util_mean_pct": {
        "meaning_zh": "按显存占用识别出的活跃 GPU 在采样点上的平均 nvidia-smi utilization",
        "unit": "percent",
        "measurement_kind": "low_frequency_sample",
        "formula": "mean utilization over samples from active devices",
        "source": "nvidia-smi utilization.gpu",
        "comparison_scope": "diagnostic starvation/busy signal",
        "limitations": "not MFU; short kernels and sub-sample idle gaps may be missed",
    },
    "gpu_energy_estimate_j": {
        "meaning_zh": "活跃 GPU 在算子采样窗口内的估算能耗",
        "unit": "joules",
        "measurement_kind": "sampled_estimate",
        "formula": "mean summed active-device power watts * sampler wall seconds",
        "source": "nvidia-smi power.draw samples",
        "comparison_scope": "same sampler interval and timing boundary",
        "limitations": "not hardware energy counter integration; misses pre/post-window energy",
    },
    "joules_per_1k_images": {
        "meaning_zh": "每处理一千张图片的估算 GPU 能耗",
        "unit": "joules_per_1000_images",
        "measurement_kind": "derived_from_sampled_estimate",
        "formula": "gpu_energy_estimate_j * 1000 / processed rows",
        "source": "GPU energy estimate and row count",
        "comparison_scope": "per-image cross-scale resource comparison",
        "limitations": "inherits power-sampling error and excludes CPU/platform energy",
    },
    "gpu_seconds_per_image": {
        "meaning_zh": "每张图片分摊的已分配 GPU 墙钟秒数",
        "unit": "allocated_gpu_seconds_per_image",
        "measurement_kind": "derived_allocation_cost",
        "formula": "gpu_workers * operator_e2e_s / processed rows",
        "source": "declared GPU workers and operator wall",
        "comparison_scope": "per-image resource allocation comparison",
        "limitations": "allocation time, not active kernel time",
    },
    "host_io_bytes_per_image": {
        "meaning_zh": "算子窗口内主机磁盘或网络计数器增量除以处理图片数",
        "unit": "bytes_per_image",
        "measurement_kind": "derived_from_host_counter_delta",
        "formula": "host counter delta / processed rows",
        "source": "psutil disk_io_counters and net_io_counters",
        "comparison_scope": "same isolated host; diagnostic cross-scale comparison",
        "limitations": (
            "host-wide counters include unrelated traffic and are not process-attributed"
        ),
    },
    "logical_h2d_effective_gbps": {
        "meaning_zh": "主机到 GPU 的逻辑 tensor 字节除以显式 H2D 阶段墙钟",
        "unit": "decimal_gigabits_per_second",
        "measurement_kind": "derived_from_intrusive_stage_timing",
        "formula": "input tensor bytes * 8 / H2D seconds / 1e9",
        "source": "project telemetry with CUDA-synchronized detailed stage timing",
        "comparison_scope": "project diagnostic runs with identical tensor contract",
        "limitations": (
            "not a PCIe hardware-counter measurement; unavailable for hidden native engines"
        ),
    },
    "estimated_e2e_mfu": {
        "meaning_zh": "按给定模型每图 FLOPs 和对应 dtype GPU 峰值估算的端到端模型 FLOPs 利用率",
        "unit": "ratio_0_to_1",
        "measurement_kind": "model_based_estimate",
        "formula": "rows * model_flops_per_image / (e2e * gpu_workers * peak_flops_per_s)",
        "source": "explicit CLI model FLOPs and dtype-matched hardware peak",
        "comparison_scope": "only when both supplied constants are independently verified",
        "limitations": "blank by default; not measured FLOPs and includes non-model E2E stages",
    },
}


def image_run_derived_metrics(
    *,
    rows: int,
    operator_e2e_s: float,
    first_output_s: float,
    cpu_core_seconds: float,
    gpu_seconds: float,
    gpu_energy_j: float,
    host_disk_read_bytes: int,
    host_disk_write_bytes: int,
    host_net_recv_bytes: int,
    host_net_sent_bytes: int,
) -> dict[str, object]:
    """Return per-image resources and streaming-onset diagnostics.

    ``first_output_fraction_of_e2e`` is a dimensionless materialization versus
    streaming signal.  It remains workload-sensitive and therefore is only
    descriptive across different row counts; it is not a normalized latency.
    """
    if rows <= 0:
        raise ValueError("rows must be positive")
    for name, value in (
        ("operator_e2e_s", operator_e2e_s),
        ("first_output_s", first_output_s),
        ("cpu_core_seconds", cpu_core_seconds),
        ("gpu_seconds", gpu_seconds),
        ("gpu_energy_j", gpu_energy_j),
    ):
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} must be finite and non-negative")
    if operator_e2e_s <= 0:
        raise ValueError("operator_e2e_s must be positive")
    if first_output_s > operator_e2e_s:
        raise ValueError("first_output_s cannot exceed operator_e2e_s")
    byte_totals = (
        host_disk_read_bytes,
        host_disk_write_bytes,
        host_net_recv_bytes,
        host_net_sent_bytes,
    )
    if any(value < 0 for value in byte_totals):
        raise ValueError("host byte totals must be non-negative")

    post_first_output_s = operator_e2e_s - first_output_s
    return {
        "image_derived_metrics_status": "available",
        "post_first_output_s": post_first_output_s,
        "first_output_fraction_of_e2e": first_output_s / operator_e2e_s,
        "post_first_output_fraction_of_e2e": post_first_output_s / operator_e2e_s,
        "first_output_cross_scale_semantics": (
            "descriptive_streaming_onset_only_not_normalized_latency"
        ),
        "steady_state_min_s": STEADY_STATE_MIN_S,
        "steady_state_duration_gate_met": operator_e2e_s >= STEADY_STATE_MIN_S,
        "throughput_cross_scale_semantics": (
            "rate_comparable_only_after_each_arm_reaches_independent_plateau"
        ),
        "joules_per_1k_images": gpu_energy_j * 1000.0 / rows,
        "gpu_seconds_per_image": gpu_seconds / rows,
        "images_per_cpu_core_second": (
            rows / cpu_core_seconds if cpu_core_seconds > 0 else ""
        ),
        "host_disk_read_bytes_per_image": host_disk_read_bytes / rows,
        "host_disk_write_bytes_per_image": host_disk_write_bytes / rows,
        "host_net_recv_bytes_per_image": host_net_recv_bytes / rows,
        "host_net_sent_bytes_per_image": host_net_sent_bytes / rows,
    }
