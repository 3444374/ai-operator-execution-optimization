#!/usr/bin/env python3
"""Generate the first-principles opening story figures from frozen evidence.

Each output has one job: motivate the design, connect organization to
scheduling, or show one preliminary evidence boundary. The corrected text
database-E2E matrix remains appendix-only because its ShareGPT arms are not a
matched-saturation performance ranking, even though the replacement run passed
the source/sink, identity, exactly-once, and stability gates. DuckDB's
ShareGPT output-cap semantic failure remains explicit.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
import numpy as np
import pandas as pd

from generate_opening_core_evidence_figures import (
    BLUE,
    DARK,
    GREY,
    LIGHT_GRID,
    ORANGE,
    OUTPUT,
    PALE_BLUE,
    PURPLE,
    RED,
    ROOT,
    TEAL,
    apply_style,
    finish,
    soft_grid,
)


def _formal(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    return frame.loc[frame["phase"].eq("formal")].copy()


def _finish_slide(fig: plt.Figure, stem: str) -> None:
    """Export a split figure without tight-cropping its 16:9 slide canvas."""

    OUTPUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT / f"{stem}.pdf")
    fig.savefig(OUTPUT / f"{stem}.svg")
    fig.savefig(OUTPUT / f"{stem}.png", dpi=300)
    plt.close(fig)


def figure_motivation_work_state() -> None:
    token_runs = _formal(
        ROOT
        / "experiments/results/local_vllm_qwen15b_baseline/"
        "sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv"
    )
    fixed16 = token_runs.loc[
        token_runs["batching_policy"].eq("fixed_rows")
        & token_runs["ray_batch_rows"].eq(16)
    ]
    light = float(fixed16["batch_tokens_min"].median())
    heavy = float(fixed16["batch_tokens_max"].median())

    state = pd.read_csv(
        ROOT
        / "experiments/results/dual_gpu_slo_ewma_flush_formal_20260729/"
        "formal_summary.csv"
    )
    state = state.loc[state["policy"].eq("fixed-50")].set_index("load")
    frontier = pd.read_csv(
        ROOT
        / "experiments/results/dual_gpu_active_work_saturation_20260729/"
        "formal_summary.csv"
    ).sort_values("active_work_per_endpoint")

    fig = plt.figure(figsize=(13.2, 4.9), constrained_layout=True)
    gs = fig.add_gridspec(1, 3, width_ratios=[0.95, 1.05, 1.55])

    ax = fig.add_subplot(gs[0, 0])
    bars = ax.barh(
        [1, 0],
        [light / 1000, heavy / 1000],
        color=[GREY, BLUE],
        height=0.48,
    )
    ax.set_yticks([1, 0])
    ax.set_yticklabels(["轻负载批次\n16 行", "重负载批次\n16 行"])
    ax.set_xlabel("输入 + 输出上限工作量（千 token）")
    ax.set_xlim(0, 7.5)
    for bar, value in zip(bars, [light, heavy], strict=True):
        ax.text(
            bar.get_width() + 0.12,
            bar.get_y() + bar.get_height() / 2,
            f"{value:,.0f}",
            va="center",
            fontsize=9,
        )
    ax.text(
        0.98,
        0.90,
        f"同为 16 行，工作量相差 {heavy / light:.1f}×",
        transform=ax.transAxes,
        ha="right",
        color=BLUE,
        fontweight="bold",
    )
    soft_grid(ax, axis="x")
    ax.set_title("记录数掩盖模型工作量", loc="left")

    ax = fig.add_subplot(gs[0, 1])
    labels = ["高供给负载", "到达受限负载"]
    active = [
        float(state.loc["high", "max_active_work_seen_mean"]) / 65536,
        float(state.loc["near", "max_active_work_seen_mean"]) / 65536,
    ]
    bars = ax.barh([1, 0], active, color=[BLUE, GREY], height=0.48)
    ax.axvline(1.0, color=DARK, linestyle="--", linewidth=1.0)
    ax.set_yticks([1, 0])
    ax.set_yticklabels(labels)
    ax.set_xlim(0, 1.12)
    ax.set_xlabel("运行内峰值 active work / 配置上限 W65K")
    mfus = [
        float(state.loc["high", "mfu_pct_mean"]),
        float(state.loc["near", "mfu_pct_mean"]),
    ]
    for bar, ratio, mfu in zip(bars, active, mfus, strict=True):
        ax.text(
            min(ratio + 0.03, 1.02),
            bar.get_y() + bar.get_height() / 2,
            f"{ratio:.0%}; MFU {mfu:.0f}%",
            va="center",
            fontsize=8.7,
        )
    soft_grid(ax, axis="x")
    ax.set_title("静态上限不等于运行状态", loc="left")

    ax = fig.add_subplot(gs[0, 2])
    x = frontier["active_work_per_endpoint"].to_numpy() / 1024
    y = frontier["tokens_per_s_mean"].to_numpy() / 1000
    ax.errorbar(
        x,
        y,
        yerr=frontier["tokens_per_s_sd"].to_numpy() / 1000,
        color=BLUE,
        marker="o",
        linewidth=2.2,
        capsize=3,
    )
    ax.axvline(64, color=BLUE, linestyle="--", linewidth=1.1)
    ax.text(28, 5.25, "低供给段", ha="center", color=DARK)
    ax.text(68, 8.53, "最小近饱和点", ha="center", color=BLUE, fontweight="bold")
    ax.text(108, 8.53, "边际收益递减", ha="center", color=DARK)
    y64 = y[np.where(x == 64)[0][0]]
    ax.text(66.5, y64 - 0.33, "65K：已测峰值的 97.8%", fontsize=8.8, color=DARK)
    ax.set(
        xlabel="每 endpoint active work（千 token）",
        ylabel="吞吐（千 token/s）",
        xlim=(12, 136),
        ylim=(4.4, 8.7),
    )
    soft_grid(ax)
    ax.set_title("在途工作量存在最小近饱和区", loc="left")
    ax.legend(
        [Line2D([0], [0], color=BLUE, marker="o", linewidth=1.6, markersize=5)],
        ["圆点=均值；误差线=SD（n=3 formal）"],
        loc="lower right",
        fontsize=8.0,
        handlelength=2.0,
    )

    for panel_ax, label in zip(fig.axes, ["a", "b", "c"], strict=True):
        panel_ax.text(
            -0.12,
            1.05,
            label,
            transform=panel_ax.transAxes,
            fontsize=11,
            fontweight="bold",
            ha="left",
            va="bottom",
        )

    fig.suptitle(
        "动机：行数、静态上限与实际运行状态并不等价",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "a：RTX 5070 / Qwen2.5-1.5B 机制证据；b–c：2×RTX 4090 / Qwen2.5-7B。点与误差线=均值±SD（n=3 formal）；65K→98K 时 P99 由 36.8s 增至 40.0s。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_motivation_work_state")


def figure_motivation_work_state_split() -> None:
    """Render the existing P08 evidence as two slide-scale figures.

    This is a layout-only split: values, labels, annotations and evidence
    wording are identical to ``figure_motivation_work_state``.
    """

    token_runs = _formal(
        ROOT
        / "experiments/results/local_vllm_qwen15b_baseline/"
        "sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv"
    )
    fixed16 = token_runs.loc[
        token_runs["batching_policy"].eq("fixed_rows")
        & token_runs["ray_batch_rows"].eq(16)
    ]
    light = float(fixed16["batch_tokens_min"].median())
    heavy = float(fixed16["batch_tokens_max"].median())

    fig, ax = plt.subplots(figsize=(12.8, 7.2), constrained_layout=False)
    fig.subplots_adjust(left=0.18, right=0.94, bottom=0.20, top=0.78)
    bars = ax.barh(
        [1, 0],
        [light / 1000, heavy / 1000],
        color=[GREY, BLUE],
        height=0.48,
    )
    ax.set_yticks([1, 0])
    ax.set_yticklabels(["轻负载批次\n16 行", "重负载批次\n16 行"])
    ax.set_xlabel("输入 + 输出上限工作量（千 token）")
    ax.set_xlim(0, 7.5)
    for bar, value in zip(bars, [light, heavy], strict=True):
        ax.text(
            bar.get_width() + 0.12,
            bar.get_y() + bar.get_height() / 2,
            f"{value:,.0f}",
            va="center",
            fontsize=9,
        )
    ax.text(
        0.98,
        0.90,
        f"同为 16 行，工作量相差 {heavy / light:.1f}×",
        transform=ax.transAxes,
        ha="right",
        color=BLUE,
        fontweight="bold",
    )
    soft_grid(ax, axis="x")
    ax.set_title("记录数掩盖模型工作量", loc="left")
    fig.suptitle(
        "动机：行数、静态上限与实际运行状态并不等价",
        fontsize=15,
        fontweight="bold",
        y=0.94,
    )
    fig.text(
        0.5,
        0.07,
        "实验配置：RTX 5070，Qwen2.5-1.5B；固定 16 rows，工作量为输入 token 与输出上限之和。",
        ha="center",
        va="top",
        fontsize=9.5,
        color=GREY,
    )
    _finish_slide(fig, "opening_motivation_work_state_part1_work")

    state = pd.read_csv(
        ROOT
        / "experiments/results/dual_gpu_slo_ewma_flush_formal_20260729/"
        "formal_summary.csv"
    )
    state = state.loc[state["policy"].eq("fixed-50")].set_index("load")
    frontier = pd.read_csv(
        ROOT
        / "experiments/results/dual_gpu_active_work_saturation_20260729/"
        "formal_summary.csv"
    ).sort_values("active_work_per_endpoint")

    fig = plt.figure(figsize=(12.8, 7.2), constrained_layout=False)
    gs = fig.add_gridspec(1, 2, width_ratios=[1.0, 1.45])
    fig.subplots_adjust(left=0.10, right=0.97, bottom=0.20, top=0.78, wspace=0.28)

    ax = fig.add_subplot(gs[0, 0])
    labels = ["高供给负载", "到达受限负载"]
    active = [
        float(state.loc["high", "max_active_work_seen_mean"]) / 65536,
        float(state.loc["near", "max_active_work_seen_mean"]) / 65536,
    ]
    bars = ax.barh([1, 0], active, color=[BLUE, GREY], height=0.48)
    ax.axvline(1.0, color=DARK, linestyle="--", linewidth=1.0)
    ax.set_yticks([1, 0])
    ax.set_yticklabels(labels)
    ax.set_xlim(0, 1.12)
    ax.set_xlabel("运行内峰值 active work / 配置上限 W65K")
    mfus = [
        float(state.loc["high", "mfu_pct_mean"]),
        float(state.loc["near", "mfu_pct_mean"]),
    ]
    for bar, ratio, mfu in zip(bars, active, mfus, strict=True):
        ax.text(
            min(ratio + 0.03, 1.02),
            bar.get_y() + bar.get_height() / 2,
            f"{ratio:.0%}; MFU {mfu:.0f}%",
            va="center",
            fontsize=8.7,
        )
    soft_grid(ax, axis="x")
    ax.set_title("静态上限不等于运行状态", loc="left")

    ax = fig.add_subplot(gs[0, 1])
    x = frontier["active_work_per_endpoint"].to_numpy() / 1024
    y = frontier["tokens_per_s_mean"].to_numpy() / 1000
    ax.errorbar(
        x,
        y,
        yerr=frontier["tokens_per_s_sd"].to_numpy() / 1000,
        color=BLUE,
        marker="o",
        linewidth=2.2,
        capsize=3,
    )
    ax.axvline(64, color=BLUE, linestyle="--", linewidth=1.1)
    ax.text(28, 5.25, "低供给段", ha="center", color=DARK)
    ax.text(68, 8.53, "最小近饱和点", ha="center", color=BLUE, fontweight="bold")
    ax.text(108, 8.53, "边际收益递减", ha="center", color=DARK)
    y64 = y[np.where(x == 64)[0][0]]
    ax.text(66.5, y64 - 0.33, "65K：已测峰值的 97.8%", fontsize=8.8, color=DARK)
    ax.set(
        xlabel="每 endpoint active work（千 token）",
        ylabel="吞吐（千 token/s）",
        xlim=(12, 136),
        ylim=(4.4, 8.7),
    )
    soft_grid(ax)
    ax.set_title("在途工作量存在最小近饱和区", loc="left")
    ax.legend(
        [Line2D([0], [0], color=BLUE, marker="o", linewidth=1.6, markersize=5)],
        ["圆点=均值；误差线=SD（n=3 formal）"],
        loc="lower right",
        fontsize=8.0,
        handlelength=2.0,
    )
    fig.suptitle(
        "动机：行数、静态上限与实际运行状态并不等价",
        fontsize=15,
        fontweight="bold",
        y=0.94,
    )
    fig.text(
        0.5,
        0.07,
        "实验配置：2×RTX 4090，Qwen2.5-7B，2 endpoints；每点 3 次 formal，active work 按每 endpoint 扫描。",
        ha="center",
        va="top",
        fontsize=9.5,
        color=GREY,
    )
    _finish_slide(fig, "opening_motivation_work_state_part2_state_capacity")


def _organization_data() -> tuple[pd.DataFrame, list[str], list[str]]:
    paths = {
        "Low KV pressure": ROOT
        / "experiments/results/rc1_data_organization/"
        "dataorg_2ep_1.5b_cacheON_20260731/raw/runs.csv",
        "High KV pressure": ROOT
        / "experiments/results/rc1_data_organization/"
        "dataorg_4ep_1.5b_cacheON_20260731/raw/runs.csv",
    }
    frames = []
    for regime, path in paths.items():
        frame = _formal(path)
        frame["regime"] = regime
        frames.append(frame)
    methods = [
        "fixed_rows_16",
        "sequential_tb",
        "length_align_tb",
        "best_fit_tb",
        "row_cap_aware_tb",
    ]
    labels = [
        "固定行数成批",
        "按 token 工作量成批",
        "长度相近成批",
        "最佳适配装箱",
        "行数上限感知",
    ]
    return pd.concat(frames, ignore_index=True), methods, labels


def figure_work_organization_v2() -> None:
    runs, methods, labels = _organization_data()
    regimes = ["Low KV pressure", "High KV pressure"]
    styles = [
        (GREY, "-", "o"),
        (BLUE, "-", "o"),
        (ORANGE, "--", "s"),
        (PURPLE, "--", "s"),
        (TEAL, "--", "s"),
    ]
    throughput = np.zeros((len(methods), len(regimes)), dtype=float)
    cache_hit = np.zeros_like(throughput)
    for regime_index, regime in enumerate(regimes):
        subset = runs.loc[runs["regime"].eq(regime)]
        for method_index, method in enumerate(methods):
            policy = subset.loc[subset["scenario_id"].eq(method)]
            throughput[method_index, regime_index] = policy["tokens_per_s"].median() / 1000
            cache_hit[method_index, regime_index] = policy["vllm_prefix_cache_hit_rate"].median()

    fig, axes = plt.subplots(1, 2, figsize=(13.2, 5.45), constrained_layout=False)
    fig.subplots_adjust(
        left=0.07,
        right=0.985,
        bottom=0.20,
        top=0.69,
        wspace=0.16,
    )
    panel_defs = [
        (axes[0], throughput, "端到端吞吐", "k token/s", (35, 61)),
        (axes[1], cache_hit * 100, "Prefix cache 命中率", "%", (0, 86)),
    ]
    label_offsets = [
        [0.0, 0.8, -1.0, 0.0, 1.0],
        [1.5, -1.5, -2.8, 0.0, 2.8],
    ]
    x = np.array([0.0, 1.0])
    for panel_index, (ax, values, title, unit, ylim) in enumerate(panel_defs):
        for method_index, style in enumerate(styles):
            color, linestyle, marker = style
            y_values = values[method_index]
            ax.plot(
                x,
                y_values,
                color=color,
                linestyle=linestyle,
                marker=marker,
                linewidth=2.0,
                markersize=5.5,
                zorder=3,
            )
            label_y = y_values[1] + label_offsets[panel_index][method_index]
            ax.plot([1.0, 1.06], [y_values[1], label_y], color=color, linewidth=0.8)
            value_text = (
                f"{y_values[0]:.1f}→{y_values[1]:.1f} {unit}"
                if unit != "%"
                else f"{y_values[0]:.1f}%→{y_values[1]:.1f}%"
            )
            ax.text(
                1.08,
                label_y,
                value_text,
                ha="left",
                va="center",
                fontsize=8.2,
                color=color,
            )
        ax.set_xlim(-0.12, 1.58)
        ax.set_ylim(*ylim)
        ax.set_xticks([0, 1])
        ax.set_xticklabels(
            [
                "低压力\n2 endpoint · KV max 7%–10%",
                "高压力\n4 endpoint · KV max 98%–100%",
            ]
        )
        ax.set_ylabel(unit)
        ax.set_title(
            f"{chr(ord('a') + panel_index)}   {title}",
            loc="left",
            pad=10,
            fontsize=11.5,
        )
        soft_grid(ax, axis="y")
    fig.suptitle(
        "压力升高后，重排策略的 Cache 命中与吞吐同步下降",
        fontsize=14.5,
        fontweight="bold",
        y=0.965,
    )
    strategy_handles = [
        Line2D(
            [0],
            [0],
            color=color,
            linestyle=linestyle,
            marker=marker,
            linewidth=2.0,
            markersize=5.0,
            label=label,
        )
        for label, (color, linestyle, marker) in zip(labels, styles, strict=True)
    ]
    fig.legend(
        handles=strategy_handles,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.855),
        ncol=5,
        frameon=False,
        fontsize=8.1,
    )
    fig.text(
        0.5,
        -0.025,
        "每条线连接同一策略的低→高压力3次formal中位数；实线圆点=保持输入顺序，虚线方点=重排/装箱，不画误差线。相同双卡硬件，仅endpoint拓扑与运行压力不同；说明机制，不作容量排名。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_work_organization_regime_v2")


TEXT_ARM_ORDER = ["bounded_http", "daft_native", "daft_ray", "ray_data_http"]
TEXT_ARM_LABELS = [
    "直接调用\n（容量参照）",
    "Daft Native",
    "Daft Ray",
    "Ray Data（欠供给）",
]
TEXT_ARM_COLORS = [DARK, TEAL, PURPLE, GREY]


def _axis_align_multiline_yticklabels(ax: plt.Axes) -> None:
    """Anchor label blocks to the axis while centering lines inside each block."""

    for tick_label in ax.get_yticklabels():
        tick_label.set_ha("right")
        tick_label.set_multialignment("center")


def _arm_point_panel(
    ax: plt.Axes,
    runs: pd.DataFrame,
    column: str,
    *,
    title: str,
    xlabel: str,
    scale: float = 1.0,
    decimals: int = 1,
    xlim: tuple[float, float] | None = None,
    annotation_labels: dict[str, str] | None = None,
) -> None:
    """Draw one original-unit state metric as a mean point over three formal runs."""

    y = np.arange(len(TEXT_ARM_ORDER))[::-1]
    for yi, arm, color in zip(
        y,
        TEXT_ARM_ORDER,
        TEXT_ARM_COLORS,
        strict=True,
    ):
        values = runs.loc[runs["arm"].eq(arm), column].to_numpy(dtype=float) * scale
        if len(values) != 3:
            raise ValueError(f"{arm}/{column} requires exactly three formal runs")
        mean = float(values.mean())
        ax.scatter(
            [mean],
            [yi],
            color=color,
            marker="o",
            s=42,
            linewidths=0,
            zorder=3,
        )
        if xlim is not None:
            span = xlim[1] - xlim[0]
            text_x = min(mean + span * 0.018, xlim[1] - span * 0.02)
            ha = "right" if text_x >= xlim[1] - span * 0.04 else "left"
            ax.text(
                text_x,
                yi + 0.18,
                (
                    annotation_labels[arm]
                    if annotation_labels is not None
                    else f"{mean:.{decimals}f}"
                ),
                color=color,
                fontsize=8.2,
                ha=ha,
                va="bottom",
            )
    ax.set_yticks(y)
    ax.set_yticklabels(TEXT_ARM_LABELS)
    _axis_align_multiline_yticklabels(ax)
    ax.set_xlabel(xlabel)
    if xlim is not None:
        ax.set_xlim(*xlim)
    ax.set_ylim(-0.65, len(TEXT_ARM_ORDER) - 0.35)
    ax.set_title(title, loc="left")
    soft_grid(ax, axis="x")


def _native_single_job_runs() -> pd.DataFrame:
    runs = pd.read_csv(
        ROOT
        / "experiments/results/opening_text_native_single_job_formal_20260808/"
        "formal_runs.csv"
    )
    if set(runs["arm"]) != set(TEXT_ARM_ORDER) or len(runs) != 12:
        raise ValueError("native single-job figure requires 4 arms × 3 formal runs")
    return runs


def _metric_annotation_labels(
    runs: pd.DataFrame,
    definitions: list[tuple[str, str, str, float, int, tuple[float, float]]],
) -> dict[str, dict[str, str]]:
    labels_by_column: dict[str, dict[str, str]] = {}
    second_columns = {"wall_s", "queue_mean_s", "ttft_mean_s"}
    for column, _, _, scale, decimals, _ in definitions:
        labels: dict[str, str] = {}
        for arm in TEXT_ARM_ORDER:
            mean = float(runs.loc[runs["arm"].eq(arm), column].mean()) * scale
            if column == "queue_mean_s" and mean < 0.01:
                labels[arm] = "≈0s"
            elif column in second_columns:
                labels[arm] = f"{mean:.{decimals}f}s"
            elif column == "gpu_util_mean_pct":
                labels[arm] = f"{mean:.1f}%"
            else:
                labels[arm] = f"{mean:.{decimals}f}"
        labels_by_column[column] = labels
    return labels_by_column


def figure_native_single_job_request_latency() -> None:
    """Contrast job completion with request-level waiting and first-token delay."""

    runs = _native_single_job_runs()

    fig, axes = plt.subplots(1, 4, figsize=(13.5, 4.8), constrained_layout=False)
    fig.subplots_adjust(
        left=0.15,
        right=0.985,
        bottom=0.24,
        top=0.74,
        wspace=0.16,
    )
    definitions = [
        ("wall_s", "任务级完成", "Job JCT（秒）", 1.0, 1, (0, 520)),
        (
            "waiting_mean",
            "服务端积压",
            "vLLM waiting 均值（请求数）",
            1.0,
            1,
            (0, 850),
        ),
        (
            "queue_mean_s",
            "单请求排队",
            "queue time 均值（秒）",
            1.0,
            2,
            (0, 43),
        ),
        ("ttft_mean_s", "首 token 延迟", "TTFT 均值（秒）", 1.0, 2, (0, 46)),
    ]
    annotation_by_column = _metric_annotation_labels(runs, definitions)

    for index, (ax, definition) in enumerate(zip(axes, definitions, strict=True)):
        column, title, xlabel, scale, decimals, xlim = definition
        ax.axhspan(-0.45, 0.45, color="#F4F6F8", zorder=0)
        _arm_point_panel(
            ax,
            runs,
            column,
            title=title,
            xlabel=xlabel,
            scale=scale,
            decimals=decimals,
            xlim=xlim,
            annotation_labels=annotation_by_column[column],
        )
        ax.tick_params(axis="y", length=0)
        if index != 0:
            ax.set_yticklabels([])
        else:
            for tick_label, color in zip(
                ax.get_yticklabels(), TEXT_ARM_COLORS, strict=True
            ):
                tick_label.set_color(color)
        ax.text(
            -0.16,
            1.08,
            chr(ord("a") + index),
            transform=ax.transAxes,
            fontsize=12,
            fontweight="bold",
            ha="left",
            va="bottom",
        )

    fig.suptitle(
        "批任务完成时间相近，不代表请求等待相近",
        fontsize=15,
        fontweight="bold",
        y=0.965,
    )
    fig.text(
        0.5,
        0.875,
        "前三条已饱和路径的 JCT 只差 6.0s，但 Daft 请求平均在 vLLM 内部排队约 37.5s",
        ha="center",
        va="center",
        fontsize=9.2,
        color=DARK,
    )
    fig.text(
        0.5,
        0.055,
        "同一 2,048-row ShareGPT manifest；实心圆=3 次 formal 均值。Job JCT 为执行路径触发→全部结果返回，\n"
        "含上游准入、vLLM 排队与推理，不含 manifest 准备和 DB source/sink。灰底 Ray Data 为欠供给诊断；"
        "Project 暂无同一 graph→gather 合同的正式点。",
        ha="center",
        va="bottom",
        fontsize=8.5,
        color=GREY,
    )
    finish(fig, "opening_native_single_job_request_latency")


def figure_native_single_job_state_fingerprint() -> None:
    """Explain the service and resource state behind the observed job JCT."""

    runs = _native_single_job_runs()

    fig, axes = plt.subplots(2, 3, figsize=(13.5, 7.8), constrained_layout=False)
    fig.subplots_adjust(
        left=0.13,
        right=0.985,
        bottom=0.16,
        top=0.77,
        wspace=0.16,
        hspace=0.40,
    )
    definitions = [
        (
            "tokens_per_s",
            "服务完成速率",
            "吞吐（千 token/s）",
            1 / 1000,
            1,
            (0, 20),
        ),
        (
            "running_mean",
            "vLLM 执行中请求",
            "running 均值（请求数）",
            1.0,
            0,
            (0, 360),
        ),
        (
            "waiting_mean",
            "vLLM 内部排队请求",
            "waiting 均值（请求数）",
            1.0,
            1,
            (0, 850),
        ),
        ("kv_mean", "KV 占用", "KV fraction（0–1）", 1.0, 2, (0, 1.05)),
        ("mfu", "模型计算利用率", "MFU（0–1）", 1.0, 2, (0, 0.72)),
        (
            "gpu_util_mean_pct",
            "GPU 活跃率",
            "GPU utilization 均值（%）",
            1.0,
            1,
            (0, 100),
        ),
    ]
    annotation_by_column = _metric_annotation_labels(runs, definitions)

    for index, (ax, definition) in enumerate(
        zip(axes.flat, definitions, strict=True)
    ):
        column, title, xlabel, scale, decimals, xlim = definition
        ax.axhspan(-0.45, 0.45, color="#F4F6F8", zorder=0)
        _arm_point_panel(
            ax,
            runs,
            column,
            title=title,
            xlabel=xlabel,
            scale=scale,
            decimals=decimals,
            xlim=xlim,
            annotation_labels=annotation_by_column[column],
        )
        ax.tick_params(axis="y", length=0)
        if index not in {0, 3}:
            ax.set_yticklabels([])
        else:
            for tick_label, color in zip(
                ax.get_yticklabels(), TEXT_ARM_COLORS, strict=True
            ):
                tick_label.set_color(color)
        ax.text(
            -0.16,
            1.08,
            chr(ord("a") + index),
            transform=ax.transAxes,
            fontsize=12,
            fontweight="bold",
            ha="left",
            va="bottom",
        )

    fig.suptitle(
        "相近 Job JCT 背后的服务供给与资源状态并不相同",
        fontsize=15,
        fontweight="bold",
        y=0.965,
    )
    fig.text(
        0.5,
        0.895,
        "Daft Native/Ray 形成高 waiting 与近满 KV；Ray Data 当前路径则是低 running、低 KV、低 MFU 的欠供给",
        ha="center",
        va="center",
        fontsize=9.2,
        color=DARK,
    )
    fig.text(
        0.5,
        0.045,
        "同一 2,048-row ShareGPT manifest；实心圆=3 次 formal 均值，离散度见审计数据。灰底 Ray Data 为欠供给诊断。\n"
        "GPU utilization 都较高仍不能区分供给状态，必须与吞吐、running/waiting、KV 和 MFU 联合解读；"
        "Project 暂无同一 graph→gather 合同的正式点。",
        ha="center",
        va="bottom",
        fontsize=8.5,
        color=GREY,
    )
    finish(fig, "opening_native_single_job_state_fingerprint")


def figure_native_single_job_evidence() -> None:
    """Render the headline task/request figure and its state diagnostic companion."""

    figure_native_single_job_request_latency()
    figure_native_single_job_state_fingerprint()


def figure_motivation_work_state_evidence() -> None:
    """Render the original P08 figure and its two layout-only split variants."""

    figure_motivation_work_state()
    figure_motivation_work_state_split()


def figure_text_baseline_evidence_map() -> None:
    """Keep product DB-E2E and official Chat-graph comparisons in honest tracks."""

    database = pd.read_csv(
        ROOT
        / "experiments/results/opening_database_e2e_text_refeed_20260808/summary/"
        "formal_summary.csv"
    )
    squad = database.loc[database["workload"].eq("squad_uniform")].set_index("arm")
    native = pd.read_csv(
        ROOT
        / "experiments/results/opening_text_native_single_job_formal_20260808/"
        "formal_summary.csv"
    ).set_index("arm")

    fig, axes = plt.subplots(1, 2, figsize=(12.8, 4.7), constrained_layout=True)
    ax = axes[0]
    product_arms = [
        ("direct_static_sharded", "Direct static", GREY),
        ("duckdb_ai_static_sharded", "DuckDB AI", ORANGE),
        ("project_frozen_static", "冻结静态基线", BLUE),
    ]
    y = np.arange(len(product_arms))[::-1]
    means = [float(squad.loc[arm, "correct_rows_per_s_mean"]) for arm, _, _ in product_arms]
    errors = [float(squad.loc[arm, "correct_rows_per_s_sd"]) for arm, _, _ in product_arms]
    colors = [color for _, _, color in product_arms]
    ax.barh(y, means, color=colors, height=0.56)
    ax.set_yticks(y)
    ax.set_yticklabels([label for _, label, _ in product_arms])
    ax.set_xlim(0, 155)
    ax.set_xlabel("SQuAD correct rows/s")
    for yi, value, error in zip(y, means, errors, strict=True):
        ax.text(value + 1.4, yi, f"{value:.1f} ± {error:.1f}", va="center", fontsize=8.5)
    ax.set_ylim(-1.0, 2.5)
    ax.text(
        3,
        -0.78,
        "统一 PostgreSQL source/sink；3 次 formal\nShareGPT 中 DuckDB 有 4,921/6,144 cap 语义失败",
        ha="left",
        va="bottom",
        fontsize=8.1,
        color=DARK,
    )
    ax.set_title("产品 / database-E2E 轨：仅 SQuAD 可排名", loc="left")
    soft_grid(ax, axis="x")

    ax = axes[1]
    chat_arms = [
        ("bounded_http", "直接调用\n（容量参照）", DARK),
        ("daft_native", "Daft Native", TEAL),
        ("daft_ray", "Daft Ray", PURPLE),
        ("ray_data_http", "Ray Data", GREY),
    ]
    y = np.arange(len(chat_arms))[::-1]
    means = [float(native.loc[arm, "tokens_per_s_mean"]) / 1000 for arm, _, _ in chat_arms]
    errors = [float(native.loc[arm, "tokens_per_s_sd"]) / 1000 for arm, _, _ in chat_arms]
    colors = [color for _, _, color in chat_arms]
    ax.barh(y, means, color=colors, height=0.54)
    ax.set_yticks(y)
    ax.set_yticklabels([label for _, label, _ in chat_arms])
    _axis_align_multiline_yticklabels(ax)
    ax.set_xlim(0, 21.5)
    ax.set_xlabel("ShareGPT 服务吞吐（千 token/s）")
    for yi, value, error in zip(y, means, errors, strict=True):
        ax.text(value + 0.22, yi, f"{value:.1f} ± {error:.1f}", va="center", fontsize=8.5)
    ax.set_ylim(-1.0, 3.5)
    ax.text(
        19.5,
        -0.78,
        "同 Chat manifest；3 次 formal；vendor scheduler ownership\n"
        "Project 暂无同一 2,048-row graph→gather 正式点，不混入排名",
        ha="right",
        va="bottom",
        fontsize=8.1,
        color=GREY,
    )
    ax.set_title("官方 Chat graph 轨：服务状态与供给差异", loc="left")
    soft_grid(ax, axis="x")

    fig.suptitle(
        "文本 baseline 需要分轨比较，不能把不同语义与计时边界混成总排行榜",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "两 panel 使用相同双卡模型服务，但 workload、source/sink 与输出语义合同不同；只在 panel 内比较。条末数字为均值 ± SD。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_text_baseline_evidence_map")


def _project_job_scenario_runs() -> tuple[pd.DataFrame, list[tuple[str, str]]]:
    runs = pd.read_csv(
        ROOT
        / "experiments/results/opening_fourjob_interference_20260809/data/combined/"
        "job_formal_runs.csv"
    )
    project = runs.loc[runs["system"].eq("project")].copy()
    scenario_by_job = []
    for job in ["short", "long1", "long2", "long3"]:
        scenario_by_job.extend(
            [
                (job, f"single_{job}_full_pool"),
                (job, f"single_{job}_quarter_pool"),
                (job, "staggered_fourjob_static_partition"),
                (job, "staggered_fourjob_shared_work"),
            ]
        )
    return project, scenario_by_job


def figure_multijob_interference_tradeoff() -> None:
    """Decompose quota, competition, and shared-credit efficiency/fairness."""

    project, expected_cells = _project_job_scenario_runs()
    observed_cells = set(zip(project["job"], project["scenario"], strict=False))
    if not set(expected_cells).issubset(observed_cells):
        raise ValueError("four-job figure is missing one or more frozen project cells")

    group_runs = pd.read_csv(
        ROOT
        / "experiments/results/opening_fourjob_interference_20260809/data/combined/"
        "group_formal_runs.csv"
    )
    group_runs = group_runs.loc[
        group_runs["system"].eq("project")
        & group_runs["scenario"].isin(
            ["staggered_fourjob_static_partition", "staggered_fourjob_shared_work"]
        )
    ].copy()
    fairness = pd.read_csv(
        ROOT
        / "experiments/results/opening_fourjob_interference_20260809/data/combined/"
        "isolated_normalized_fairness.csv"
    )
    fairness = fairness.loc[
        fairness["system"].eq("project")
        & fairness["comparison"].isin(["matched_competition_static", "shared_fourjob"])
    ].copy()
    long_spread = pd.read_csv(
        ROOT
        / "experiments/results/opening_fourjob_interference_20260809/data/combined/"
        "long_job_spread.csv"
    )
    long_spread = long_spread.loc[
        long_spread["system"].eq("project")
        & long_spread["scenario"].isin(
            ["staggered_fourjob_static_partition", "staggered_fourjob_shared_work"]
        )
    ].copy()

    jobs = ["short", "long1", "long2", "long3"]
    job_labels = ["Short", "Long 1", "Long 2", "Long 3"]
    progress = {}
    for _, row in fairness.iterrows():
        progress[str(row["comparison"])] = json.loads(row["normalized_progress_by_job"])
    static_progress = progress["matched_competition_static"]
    shared_progress = progress["shared_fourjob"]
    static_jain = float(
        fairness.loc[
            fairness["comparison"].eq("matched_competition_static"),
            "jain_normalized_progress",
        ].iloc[0]
    )
    shared_jain = float(
        fairness.loc[
            fairness["comparison"].eq("shared_fourjob"),
            "jain_normalized_progress",
        ].iloc[0]
    )
    static_spread = float(
        long_spread.loc[
            long_spread["scenario"].eq("staggered_fourjob_static_partition"),
            "long_jct_range_s",
        ].mean()
    )
    shared_spread = float(
        long_spread.loc[
            long_spread["scenario"].eq("staggered_fourjob_shared_work"),
            "long_jct_range_s",
        ].mean()
    )

    scenario_names = {
        "full": lambda job: f"single_{job}_full_pool",
        "quarter": lambda job: f"single_{job}_quarter_pool",
        "static": lambda job: "staggered_fourjob_static_partition",
        "shared": lambda job: "staggered_fourjob_shared_work",
    }
    jct = np.zeros((len(jobs), 4), dtype=float)
    for row, job in enumerate(jobs):
        for column, key in enumerate(["full", "quarter", "static", "shared"]):
            values = project.loc[
                project["job"].eq(job)
                & project["scenario"].eq(scenario_names[key](job)),
                "job_jct_s",
            ].to_numpy(dtype=float)
            if len(values) != 3:
                raise ValueError(f"{job}/{key} requires exactly three formal runs")
            jct[row, column] = values.mean()
    fig = plt.figure(figsize=(13.2, 6.2), constrained_layout=True)
    gs = fig.add_gridspec(2, 2, width_ratios=[1.38, 1.0], height_ratios=[0.76, 1.24])
    ax_jct = fig.add_subplot(gs[:, 0])
    ax_eff = fig.add_subplot(gs[0, 1])
    ax_fair = fig.add_subplot(gs[1, 1])

    normalized_jct = jct / jct[:, [0]]
    scenario_x = np.arange(4)
    scenario_labels = ["独立运行", "1/4 配额", "四 Job\n静态竞争", "四 Job\n共享调度"]
    job_colors = [BLUE, TEAL, ORANGE, PURPLE]
    job_markers = ["o", "s", "^", "D"]
    for row, (job_label, color, marker) in enumerate(
        zip(job_labels, job_colors, job_markers, strict=True)
    ):
        values = normalized_jct[row]
        ax_jct.plot(
            scenario_x,
            values,
            color=color,
            marker=marker,
            markersize=5.8,
            linewidth=1.9,
            label=job_label,
            zorder=3,
        )
    ax_jct.axhline(1.0, color=LIGHT_GRID, linewidth=1.2, zorder=1)
    ax_jct.set_xticks(scenario_x)
    ax_jct.set_xticklabels(scenario_labels)
    ax_jct.set_xlim(-0.18, 3.18)
    ax_jct.set_ylim(0.72, 5.05)
    ax_jct.set_ylabel("归一化 JCT（独立运行 = 1，越低越好）")
    ax_jct.set_title("Project 内同一 Job 在配额、竞争与共享调度下如何变化", loc="left", pad=12)
    soft_grid(ax_jct, axis="y")
    ax_jct.legend(loc="upper left", frameon=False, ncol=4, title="每条线代表同一个 Job")

    static_group = group_runs.loc[group_runs["policy"].eq("static_partition")]
    shared_group = group_runs.loc[group_runs["policy"].eq("shared_work")]

    def relative_change(static_value: float, shared_value: float) -> str:
        percentage = (shared_value / static_value - 1.0) * 100
        sign = "+" if percentage >= 0 else "−"
        return f"{sign}{abs(percentage):.2f}%"

    static_throughput = float(static_group["group_tokens_per_s"].mean())
    shared_throughput = float(shared_group["group_tokens_per_s"].mean())
    static_jct = float(static_group["group_jct_s"].mean())
    shared_jct = float(shared_group["group_jct_s"].mean())
    static_mfu = float(static_group["mfu_fraction"].mean())
    shared_mfu = float(shared_group["mfu_fraction"].mean())
    efficiency_rows = [
        (
            "组吞吐",
            f"{static_throughput / 1000:.2f}K",
            f"{shared_throughput / 1000:.2f}K",
            relative_change(static_throughput, shared_throughput),
        ),
        (
            "Group JCT",
            f"{static_jct:.1f}s",
            f"{shared_jct:.1f}s",
            relative_change(static_jct, shared_jct),
        ),
        (
            "MFU",
            f"{static_mfu:.1%}",
            f"{shared_mfu:.1%}",
            relative_change(static_mfu, shared_mfu),
        ),
    ]
    ax_eff.set_axis_off()
    x_positions = [0.02, 0.48, 0.70, 0.94]
    for x, header, align in zip(
        x_positions,
        ["指标", "静态", "共享", "相对变化"],
        ["left", "center", "center", "right"],
        strict=True,
    ):
        ax_eff.text(
            x,
            0.88,
            header,
            transform=ax_eff.transAxes,
            ha=align,
            va="center",
            fontweight="bold",
            color=DARK,
        )
    for row_index, row_values in enumerate(efficiency_rows):
        y = 0.66 - row_index * 0.24
        change_color = "#C44E52" if row_values[-1].startswith("+") else "#2A7F62"
        for x, value, align, color in zip(
            x_positions,
            row_values,
            ["left", "center", "center", "right"],
            [DARK, DARK, DARK, change_color],
            strict=True,
        ):
            ax_eff.text(x, y, value, transform=ax_eff.transAxes, ha=align, va="center", color=color)
        ax_eff.plot([0.02, 0.96], [y - 0.12, y - 0.12], transform=ax_eff.transAxes, color=LIGHT_GRID, lw=0.8)
    ax_eff.set_title("共享调度提高总效率", loc="left", pad=10)

    progress_values = np.array(
        [[static_progress[job], shared_progress[job]] for job in jobs]
    )
    policy_x = np.arange(2)
    for row, (job_label, color, marker) in enumerate(
        zip(job_labels, job_colors, job_markers, strict=True)
    ):
        values = progress_values[row]
        ax_fair.plot(
            policy_x,
            values,
            color=color,
            marker=marker,
            markersize=5.5,
            linewidth=1.7,
            label=job_label,
            zorder=3,
        )
    ax_fair.set_xticks(policy_x)
    ax_fair.set_xticklabels(["静态竞争", "共享调度"])
    ax_fair.set_xlim(-0.18, 1.18)
    ax_fair.set_ylim(0.25, 0.86)
    ax_fair.set_ylabel("归一化完成进度（独立运行 = 1，越高越好）")
    soft_grid(ax_fair, axis="y")
    ax_fair.set_title("共享调度改变各 Job 的完成进度", loc="left", pad=10)
    ax_fair.legend(loc="upper center", frameon=False, ncol=4, fontsize=7.5)
    ax_fair.text(
        0.02,
        0.03,
        f"Jain：{static_jain:.3f} → {shared_jain:.3f}    "
        f"Long JCT spread：{static_spread:.1f}s → {shared_spread:.1f}s",
        transform=ax_fair.transAxes,
        ha="left",
        va="bottom",
        fontsize=8.3,
        color=DARK,
    )

    for ax, label in zip([ax_jct, ax_eff, ax_fair], ["a", "b", "c"], strict=True):
        ax.text(
            -0.12,
            1.05,
            label,
            transform=ax.transAxes,
            fontsize=12,
            fontweight="bold",
            ha="left",
            va="bottom",
        )
    fig.suptitle(
        "Project 机制 A/B：共享 work credit 的效率—隔离—公平权衡",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.015,
        "Short@0s，3×Long@5s；每条线始终代表同一个 Job，点为3次formal均值，数值由纵轴读取，不重复标注。静态/共享是同一总上限下互斥A/B臂；独立与1/4配额用于分离配额损失。",
        ha="center",
        va="top",
        fontsize=8.5,
        color=GREY,
    )
    finish(fig, "opening_multijob_interference_tradeoff")


def figure_native_fourjob_normalized_impact() -> None:
    """Show within-system four-job slowdown as a direct impact matrix."""

    runs = pd.read_csv(
        ROOT
        / "experiments/results/opening_fourjob_interference_20260809/data/combined/"
        "job_formal_runs.csv"
    )
    systems = ["daft_native", "daft_ray", "ray_data_http"]
    system_labels = ["Daft Native", "Daft Ray", "Ray Data"]
    jobs = ["short", "long1", "long2", "long3"]
    job_labels = ["Short\n前台任务", "Long 1", "Long 2", "Long 3"]
    slowdown = np.zeros((len(systems), len(jobs)), dtype=float)

    for system_index, system in enumerate(systems):
        one_system = runs.loc[runs["system"].eq(system)]
        for job_index, job in enumerate(jobs):
            isolated = one_system.loc[
                one_system["scenario"].eq("single_full") & one_system["job"].eq(job),
                "job_jct_s",
            ].to_numpy(dtype=float)
            concurrent = one_system.loc[
                one_system["scenario"].eq("fourjob") & one_system["job"].eq(job),
                "job_jct_s",
            ].to_numpy(dtype=float)
            if len(isolated) != 3 or len(concurrent) != 3:
                raise ValueError(f"{system}/{job} requires 3 isolated and 3 four-job runs")
            denominator = float(isolated.mean())
            normalized = concurrent / denominator
            slowdown[system_index, job_index] = normalized.mean()

    # A matrix is the honest visual encoding here: the claim is the magnitude
    # of impact for a small, crossed system × job design, not uncertainty of a
    # population estimate. Formal-repeat SD remains preserved in the data and
    # caption rather than dominating the main visual with twelve error bars.
    fig, ax = plt.subplots(figsize=(10.8, 4.8), constrained_layout=True)
    mesh = ax.pcolormesh(
        np.arange(len(jobs) + 1),
        np.arange(len(systems) + 1),
        slowdown,
        cmap="Blues",
        vmin=1.0,
        vmax=3.05,
        edgecolors="white",
        linewidth=8,
        shading="flat",
    )
    ax.invert_yaxis()
    ax.set_aspect("auto")
    ax.set_xticks(np.arange(len(jobs)) + 0.5)
    ax.set_xticklabels(job_labels)
    ax.xaxis.tick_top()
    ax.tick_params(axis="x", length=0, pad=8)
    ax.set_yticks(np.arange(len(systems)) + 0.5)
    ax.set_yticklabels(system_labels)
    ax.tick_params(axis="y", length=0, pad=8)

    for row in range(len(systems)):
        for column in range(len(jobs)):
            value = slowdown[row, column]
            text_color = "white" if value >= 2.15 else DARK
            ax.text(
                column + 0.5,
                row + 0.44,
                f"{value:.2f}×",
                ha="center",
                va="center",
                fontsize=13,
                fontweight="bold",
                color=text_color,
            )
            ax.text(
                column + 0.5,
                row + 0.67,
                f"JCT +{(value - 1.0) * 100:.0f}%",
                ha="center",
                va="center",
                fontsize=8.2,
                color=text_color,
                alpha=0.92,
            )

    colorbar = fig.colorbar(mesh, ax=ax, fraction=0.032, pad=0.025)
    colorbar.set_label("并发 JCT / 独立 JCT", rotation=90, labelpad=10)
    colorbar.set_ticks([1.0, 1.5, 2.0, 2.5, 3.0])
    colorbar.ax.set_yticklabels(["1.0×\n无影响", "1.5×", "2.0×", "2.5×", "3.0×"])
    # Matplotlib rasterizes dense colorbar meshes by default.  Keep the
    # publication SVG fully vector so the compact matrix remains sharp when
    # resized in the proposal deck and report.
    colorbar.solids.set_rasterized(False)
    colorbar.outline.set_visible(False)

    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_title(
        "现有原生框架：四 Job 并发普遍延长 Short 与全部 Long Job",
        loc="left",
        fontsize=15,
        fontweight="bold",
        pad=22,
    )
    fig.text(
        0.5,
        -0.015,
        "格内为 four-job JCT ÷ 本 Job isolated-single JCT；数值越大，受并发影响越强。均值来自 3 次 formal，SD 保留于附录；只比较系统内退化，不作跨框架绝对 JCT 排名。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_native_fourjob_normalized_impact")


def figure_image_stage_evidence() -> None:
    """Motivate staged work and bounded admission for image operators."""

    profile = pd.read_csv(
        ROOT
        / "motivation/results/gpu/image_clip_preprocess_variants_20260801/"
        "raw_repeats.csv"
    )
    fast = profile.loc[
        profile["variant"].eq("torchvision_tensor_pt")
        & profile["batch_size"].isin([16, 64, 256])
    ]
    fast = fast.assign(
        prepare_to_actor=fast["cpu_preprocess_s"] / fast["actor_call_wall_s"]
    )
    transfer = pd.read_csv(
        ROOT
        / "motivation/results/gpu/image_clip_transfer_ceiling_20260803/raw.csv"
    )
    transfer = transfer.loc[transfer["batch_size"].eq(64)].copy()
    host = pd.read_csv(
        ROOT
        / "motivation/results/gpu/image_host_path_screening_20260802/summary.csv"
    )
    active = host.loc[host["experiment"].eq("active_batch_screen")].sort_values(
        "max_active_batches"
    )

    fig, axes = plt.subplots(1, 3, figsize=(13.2, 4.9), constrained_layout=True)
    ax = axes[0]
    batches = [16, 64, 256]
    medians = []
    q1s = []
    q3s = []
    for batch in batches:
        values = fast.loc[fast["batch_size"].eq(batch), "prepare_to_actor"].to_numpy(
            dtype=float
        )
        if len(values) != 30:
            raise ValueError(f"image prepare ratio requires 30 repeats for batch={batch}")
        medians.append(float(np.median(values)))
        q1s.append(float(np.quantile(values, 0.25)))
        q3s.append(float(np.quantile(values, 0.75)))
    x = np.arange(len(batches))
    ax.errorbar(
        x,
        medians,
        yerr=[np.asarray(medians) - np.asarray(q1s), np.asarray(q3s) - np.asarray(medians)],
        color=ORANGE,
        marker="o",
        linewidth=1.8,
        capsize=3,
        markersize=6,
    )
    for xi, value in zip(x, medians, strict=True):
        ax.text(
            xi,
            value + 0.8,
            f"{value:.1f}×",
            ha="center",
            fontsize=9,
        )
    ax.set_xticks(x)
    ax.set_xticklabels([str(item) for item in batches])
    ax.set(
        xlabel="每批图像数",
        ylabel="CPU 准备时间 / GPU actor 时间",
        ylim=(0, 35),
    )
    ax.legend(
        [Line2D([0], [0], color=ORANGE, marker="o", linewidth=1.6)],
        ["圆点=中位数；误差线=IQR（n=30）"],
        loc="lower right",
        fontsize=7.6,
    )
    ax.set_title("prepare 是独立且占主导的工作阶段", loc="left")
    soft_grid(ax)

    ax = axes[1]
    transfer_defs = [
        ("r0_gpu_resident", "R0 GPU-resident", DARK, "o"),
        ("r1_pinned_fp16", "R1 pinned FP16", TEAL, "s"),
        ("r2_pageable_fp32", "R2 pageable FP32", ORANGE, "D"),
    ]
    y = np.arange(len(transfer_defs))[::-1]
    for yi, (mode, _, color, marker) in zip(y, transfer_defs, strict=True):
        values = transfer.loc[transfer["mode"].eq(mode), "images_per_s"].to_numpy(
            dtype=float
        )
        if len(values) != 30:
            raise ValueError(f"transfer mode {mode} requires 30 repeats at batch64")
        median = float(np.median(values)) / 1000
        q1 = float(np.quantile(values, 0.25)) / 1000
        q3 = float(np.quantile(values, 0.75)) / 1000
        ax.errorbar(
            median,
            yi,
            xerr=[[median - q1], [q3 - median]],
            fmt=marker,
            color=color,
            ecolor=color,
            markersize=7,
            capsize=3,
            linewidth=1.6,
        )
        ax.text(median + 0.18, yi + 0.16, f"{median:.2f}K", color=color, fontsize=8.4)
    labels = [item[1] for item in transfer_defs]
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel("吞吐（千 image/s，batch=64）")
    ax.set_xlim(0, 10.8)
    ax.set_ylim(-0.45, 2.45)
    ax.text(
        0.98,
        0.06,
        "R0→R1 仅约 −11%\nR0→R2 约 −80%",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=8.0,
        color=DARK,
    )
    ax.legend(
        [Line2D([0], [0], color=DARK, marker="o", linewidth=1.6)],
        ["形状=传输形态；点=中位数；误差线=IQR（n=30）"],
        loc="lower center",
        fontsize=7.3,
    )
    ax.set_title("瓶颈不是 PCIe，而是 host ownership-copy", loc="left")
    soft_grid(ax, axis="x")

    ax = axes[2]
    windows = active["max_active_batches"].to_numpy(dtype=int)
    throughput = active["post_setup_images_per_s"].to_numpy(dtype=float) / 1000
    waits = active["batch_unattributed_wait_p50_s"].to_numpy(dtype=float)
    ax.plot(windows, throughput, color=DARK, marker="o", linewidth=1.8, markersize=6)
    best_idx = int(np.argmax(throughput))
    ax.scatter(
        windows[best_idx],
        throughput[best_idx],
        color=BLUE,
        marker="D",
        s=52,
        zorder=4,
    )
    ax.scatter(windows[-1], throughput[-1], color=RED, marker="X", s=58, zorder=4)
    for window, value, wait in zip(windows, throughput, waits, strict=True):
        ax.text(window, value + 0.055, f"wait {wait:.2f}s", ha="center", fontsize=7.7)
    ax.set(
        xlabel="max active batches",
        ylabel="setup 后吞吐（千 image/s）",
        xlim=(1, 67),
        ylim=(0.35, 1.18),
    )
    ax.set_xticks(windows)
    ax.legend(
        [
            Line2D([0], [0], color=DARK, marker="o", linewidth=1.6),
            Line2D([0], [0], color=BLUE, marker="D", linewidth=0),
            Line2D([0], [0], color=RED, marker="X", linewidth=0),
        ],
        ["点=单次 screening", "菱形=最高吞吐点", "红叉=继续增压后回退"],
        loc="lower right",
        fontsize=7.2,
    )
    ax.set_title("提交窗口过小会欠供给，过大又积累等待", loc="left")
    soft_grid(ax)

    for panel_ax, label in zip(axes, ["a", "b", "c"], strict=True):
        panel_ax.text(
            -0.12,
            1.05,
            label,
            transform=panel_ax.transAxes,
            fontsize=11,
            fontweight="bold",
            ha="left",
            va="bottom",
        )

    fig.suptitle(
        "图像动机：阶段失衡、传输形态与提交窗口共同影响执行",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "a–b：每 cell 30 次重复，中位数与 IQR；c：5K 单次 screening，仅作窗口选择动机，不作策略胜出证据。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_image_stage_aware_evidence")


def figure_image_stage_evidence_split() -> None:
    """Render the existing P08 evidence as two slide-scale figures."""

    profile = pd.read_csv(
        ROOT
        / "motivation/results/gpu/image_clip_preprocess_variants_20260801/"
        "raw_repeats.csv"
    )
    fast = profile.loc[
        profile["variant"].eq("torchvision_tensor_pt")
        & profile["batch_size"].isin([16, 64, 256])
    ]
    fast = fast.assign(
        prepare_to_actor=fast["cpu_preprocess_s"] / fast["actor_call_wall_s"]
    )

    fig, ax = plt.subplots(figsize=(12.8, 7.2), constrained_layout=False)
    fig.subplots_adjust(left=0.14, right=0.95, bottom=0.20, top=0.78)
    batches = [16, 64, 256]
    medians = []
    q1s = []
    q3s = []
    for batch in batches:
        values = fast.loc[fast["batch_size"].eq(batch), "prepare_to_actor"].to_numpy(
            dtype=float
        )
        if len(values) != 30:
            raise ValueError(f"image prepare ratio requires 30 repeats for batch={batch}")
        medians.append(float(np.median(values)))
        q1s.append(float(np.quantile(values, 0.25)))
        q3s.append(float(np.quantile(values, 0.75)))
    x = np.arange(len(batches))
    ax.errorbar(
        x,
        medians,
        yerr=[np.asarray(medians) - np.asarray(q1s), np.asarray(q3s) - np.asarray(medians)],
        color=ORANGE,
        marker="o",
        linewidth=1.8,
        capsize=3,
        markersize=6,
    )
    for xi, value in zip(x, medians, strict=True):
        ax.text(xi, value + 0.8, f"{value:.1f}×", ha="center", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([str(item) for item in batches])
    ax.set(
        xlabel="每批图像数",
        ylabel="CPU 准备时间 / GPU actor 时间",
        ylim=(0, 35),
    )
    ax.legend(
        [Line2D([0], [0], color=ORANGE, marker="o", linewidth=1.6)],
        ["圆点=中位数；误差线=IQR（n=30）"],
        loc="lower right",
        fontsize=7.6,
    )
    ax.set_title("prepare 是独立且占主导的工作阶段", loc="left")
    soft_grid(ax)
    fig.suptitle(
        "图像动机：阶段失衡、传输形态与提交窗口共同影响执行",
        fontsize=15,
        fontweight="bold",
        y=0.94,
    )
    fig.text(
        0.5,
        0.07,
        "实验配置：单卡 RTX 4090，CLIP ViT-B/32，COCO val2017 5K；batch=16/64/256，每格 30 次 formal。",
        ha="center",
        va="top",
        fontsize=9.5,
        color=GREY,
    )
    _finish_slide(fig, "opening_image_stage_aware_evidence_part1_prepare")

    transfer = pd.read_csv(
        ROOT
        / "motivation/results/gpu/image_clip_transfer_ceiling_20260803/raw.csv"
    )
    transfer = transfer.loc[transfer["batch_size"].eq(64)].copy()
    host = pd.read_csv(
        ROOT
        / "motivation/results/gpu/image_host_path_screening_20260802/summary.csv"
    )
    active = host.loc[host["experiment"].eq("active_batch_screen")].sort_values(
        "max_active_batches"
    )

    fig, axes = plt.subplots(1, 2, figsize=(12.8, 7.2), constrained_layout=False)
    fig.subplots_adjust(left=0.12, right=0.97, bottom=0.20, top=0.78, wspace=0.28)
    ax = axes[0]
    transfer_defs = [
        ("r0_gpu_resident", "R0 GPU-resident", DARK, "o"),
        ("r1_pinned_fp16", "R1 pinned FP16", TEAL, "s"),
        ("r2_pageable_fp32", "R2 pageable FP32", ORANGE, "D"),
    ]
    y = np.arange(len(transfer_defs))[::-1]
    for yi, (mode, _, color, marker) in zip(y, transfer_defs, strict=True):
        values = transfer.loc[transfer["mode"].eq(mode), "images_per_s"].to_numpy(
            dtype=float
        )
        if len(values) != 30:
            raise ValueError(f"transfer mode {mode} requires 30 repeats at batch64")
        median = float(np.median(values)) / 1000
        q1 = float(np.quantile(values, 0.25)) / 1000
        q3 = float(np.quantile(values, 0.75)) / 1000
        ax.errorbar(
            median,
            yi,
            xerr=[[median - q1], [q3 - median]],
            fmt=marker,
            color=color,
            ecolor=color,
            markersize=7,
            capsize=3,
            linewidth=1.6,
        )
        ax.text(median + 0.18, yi + 0.16, f"{median:.2f}K", color=color, fontsize=8.4)
    ax.set_yticks(y)
    ax.set_yticklabels([item[1] for item in transfer_defs])
    ax.set_xlabel("吞吐（千 image/s，batch=64）")
    ax.set_xlim(0, 10.8)
    ax.set_ylim(-0.45, 2.45)
    ax.text(
        0.98,
        0.06,
        "R0→R1 仅约 −11%\nR0→R2 约 −80%",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=8.0,
        color=DARK,
    )
    ax.legend(
        [Line2D([0], [0], color=DARK, marker="o", linewidth=1.6)],
        ["形状=传输形态；点=中位数；误差线=IQR（n=30）"],
        loc="lower center",
        fontsize=7.3,
    )
    ax.set_title("瓶颈不是 PCIe，而是 host ownership-copy", loc="left")
    soft_grid(ax, axis="x")

    ax = axes[1]
    windows = active["max_active_batches"].to_numpy(dtype=int)
    throughput = active["post_setup_images_per_s"].to_numpy(dtype=float) / 1000
    waits = active["batch_unattributed_wait_p50_s"].to_numpy(dtype=float)
    ax.plot(windows, throughput, color=DARK, marker="o", linewidth=1.8, markersize=6)
    best_idx = int(np.argmax(throughput))
    ax.scatter(
        windows[best_idx],
        throughput[best_idx],
        color=BLUE,
        marker="D",
        s=52,
        zorder=4,
    )
    ax.scatter(windows[-1], throughput[-1], color=RED, marker="X", s=58, zorder=4)
    for window, value, wait in zip(windows, throughput, waits, strict=True):
        ax.text(window, value + 0.055, f"wait {wait:.2f}s", ha="center", fontsize=7.7)
    ax.set(
        xlabel="max active batches",
        ylabel="setup 后吞吐（千 image/s）",
        xlim=(1, 67),
        ylim=(0.35, 1.18),
    )
    ax.set_xticks(windows)
    ax.legend(
        [
            Line2D([0], [0], color=DARK, marker="o", linewidth=1.6),
            Line2D([0], [0], color=BLUE, marker="D", linewidth=0),
            Line2D([0], [0], color=RED, marker="X", linewidth=0),
        ],
        ["点=单次 screening", "菱形=最高吞吐点", "红叉=继续增压后回退"],
        loc="lower right",
        fontsize=7.2,
    )
    ax.set_title("提交窗口过小会欠供给，过大又积累等待", loc="left")
    soft_grid(ax)

    fig.suptitle(
        "图像动机：阶段失衡、传输形态与提交窗口共同影响执行",
        fontsize=15,
        fontweight="bold",
        y=0.94,
    )
    fig.text(
        0.5,
        0.07,
        "实验配置：CLIP ViT-B/32；传输 ceiling 为单卡 RTX 4090、batch=64、30 次 formal；窗口扫描为 2×RTX 4090、COCO 5K、batch=64、单次 screening。",
        ha="center",
        va="top",
        fontsize=9.5,
        color=GREY,
    )
    _finish_slide(fig, "opening_image_stage_aware_evidence_part2_transfer_window")


def figure_image_stage_evidence_set() -> None:
    """Render the original P08 figure and its two layout-only split variants."""

    figure_image_stage_evidence()
    figure_image_stage_evidence_split()


def figure_image_baseline_evidence_map() -> None:
    """Show image baseline measurements without mixing in role diagrams."""

    image_root = ROOT / "experiments/results/image_ai_embed_operator_formal_20260803"
    consistency = _formal(image_root / "raw/runs_3arm_12k_consistency_20260804.csv")
    matched = _formal(image_root / "raw/runs_matched_resource_schemav12_20260804.csv")
    fig, axes = plt.subplots(
        1,
        2,
        figsize=(12.6, 5.1),
        constrained_layout=True,
        gridspec_kw={"width_ratios": [1.0, 1.26]},
    )

    ax = axes[0]
    diagnostic_defs = [
        ("daft_builtin_embed", "Daft Built-in", TEAL),
        ("ray_data_staged", "Ray Data", GREY),
        ("project_ray", "Project Static", BLUE),
    ]
    y = np.arange(len(diagnostic_defs))[::-1]
    diagnostic_means = []
    diagnostic_sds = []
    for yi, (arm, label, color) in zip(y, diagnostic_defs, strict=True):
        values = consistency.loc[
            consistency["arm"].eq(arm), "operator_e2e_s"
        ].to_numpy(dtype=float)
        if len(values) != 3:
            raise ValueError(f"12K image diagnostic requires three formal runs for {arm}")
        mean = float(values.mean())
        sd = float(values.std(ddof=1))
        diagnostic_means.append(mean)
        diagnostic_sds.append(sd)
    ax.barh(
        y,
        diagnostic_means,
        height=0.34,
        color=[item[2] for item in diagnostic_defs],
        alpha=0.86,
    )
    for yi, mean, sd, color in zip(
        y,
        diagnostic_means,
        diagnostic_sds,
        [item[2] for item in diagnostic_defs],
        strict=True,
    ):
        ax.text(
            mean + 1.15,
            yi,
            f"{mean:.1f} ± {sd:.1f} s",
            color=color,
            fontsize=8.2,
            va="center",
            fontweight="bold",
        )
    ax.set_yticks(y)
    ax.set_yticklabels([item[1] for item in diagnostic_defs])
    ax.set_xlim(0, 78)
    ax.set_ylim(-0.55, 2.55)
    ax.set_xlabel("12K operator JCT（秒）")
    ax.set_title("12K 同语义诊断（setup-dominated，不排名）", loc="left")
    soft_grid(ax, axis="x")

    ax = axes[1]
    cpu_levels = [8, 16]
    y = np.arange(len(cpu_levels))[::-1]
    ranking_defs = [
        ("ray_data_staged", "Ray Data", GREY, 0.13),
        ("project_ray", "Project", BLUE, -0.13),
    ]
    for arm, label, color, offset in ranking_defs:
        for cpu, yi in zip(cpu_levels, y, strict=True):
            values = matched.loc[
                matched["arm"].eq(arm)
                & matched["cpu_workers"].eq(cpu)
                & matched["phase"].eq("formal"),
                "operator_e2e_s",
            ].to_numpy(dtype=float)
            if len(values) != 3:
                raise ValueError(f"120K matched cell requires three formal runs for {arm}/cpu{cpu}")
            mean = float(values.mean())
            sd = float(values.std(ddof=1))
            ax.barh(
                yi + offset,
                mean,
                height=0.22,
                color=color,
                alpha=0.86,
            )
            ax.text(
                3.0,
                yi + offset,
                label,
                color="white" if color == BLUE else DARK,
                fontsize=7.5,
                va="center",
                fontweight="bold",
            )
            ax.text(
                mean + 1.5,
                yi + offset,
                f"{mean:.1f} ± {sd:.1f} s",
                color=color,
                fontsize=8.2,
                va="center",
                fontweight="bold",
            )
    ax.set_yticks(y)
    ax.set_yticklabels(["8 个 CPU worker", "16 个 CPU worker"])
    ax.set_xlim(0, 148)
    ax.set_ylim(-0.5, 1.68)
    ax.set_xlabel("120K operator JCT（秒，越低越好）")
    ax.text(
        2.0,
        1.53,
        "Daft Built-in：20K OutOfDisk，未形成 120K formal cell",
        ha="left",
        va="center",
        color=RED,
        fontsize=7.4,
    )
    ax.set_title("120K 同资源比较：仅两条路径通过规模门禁", loc="left")
    soft_grid(ax, axis="x")

    for panel_ax, label in zip(axes, ["a", "b"], strict=True):
        panel_ax.text(
            -0.12,
            1.05,
            label,
            transform=panel_ax.transAxes,
            fontsize=11,
            fontweight="bold",
            ha="left",
            va="bottom",
        )

    fig.suptitle(
        "图像 baseline 数据结果：短规模诊断与同资源比较分开",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "统一 PostgreSQL 图像输入、CLIP 与 L2-normalized 输出；条末数字=均值±SD（n=3 formal）；panel a 只作诊断，panel b 仅比较通过规模门禁的路径。路径角色与能力边界见报告独立表格。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_image_baseline_evidence_map")


def figure_image_fourjob_normalized_impact() -> None:
    """Show within-path image four-job impact without cross-system ranking."""

    native_root = ROOT / "experiments/results/opening_image_native_fourjob_formal_20260810"
    project_root = (
        ROOT
        / "experiments/results/opening_image_project_fourjob_observe_only_formal_20260810"
    )
    native_audit = json.loads((native_root / "data/audit.json").read_text())
    project_audit = json.loads((project_root / "data/audit.json").read_text())
    if native_audit["status"] != "passed" or project_audit["status"] != "passed":
        raise ValueError("image four-job evidence requires both formal audits to pass")
    if native_audit["job_manifest_sha256"] != project_audit["job_manifest_sha256"]:
        raise ValueError("image native/project four-job manifests must match")
    if not project_audit["trace_observe_only_all"] or not project_audit["trace_fresh_all"]:
        raise ValueError("project proposed-role evidence must remain observe-only and fresh")

    native = pd.read_csv(native_root / "data/slowdown_summary.csv")
    project = pd.read_csv(project_root / "data/job_summary.csv")
    jobs = ["short", "long1", "long2", "long3"]
    job_labels = ["Short\n前台任务", "Long 1", "Long 2", "Long 3"]

    def native_ratios(system: str) -> np.ndarray:
        subset = native.loc[native["system"].eq(system)].set_index("job_id")
        if set(subset.index) != set(jobs):
            raise ValueError(f"{system} must contain exactly the four frozen image jobs")
        return np.asarray([1.0 + float(subset.loc[job, "slowdown_pct"]) / 100 for job in jobs])

    project_indexed = project.set_index(["scenario_id", "job_id"])

    def project_ratios(scenario: str) -> np.ndarray:
        ratios = []
        for job in jobs:
            single_key = (f"single_{job}_full_pool", job)
            four_key = (scenario, job)
            if single_key not in project_indexed.index or four_key not in project_indexed.index:
                raise ValueError(f"project image four-job evidence missing {job}/{scenario}")
            if int(project_indexed.loc[single_key, "n"]) != 3 or int(
                project_indexed.loc[four_key, "n"]
            ) != 3:
                raise ValueError("project image four-job cells require three formal repeats")
            if not bool(project_indexed.loc[four_key, "exactly_once_all"]):
                raise ValueError("project image four-job evidence must be exactly-once")
            ratios.append(
                float(project_indexed.loc[four_key, "jct_s_mean"])
                / float(project_indexed.loc[single_key, "jct_s_mean"])
            )
        return np.asarray(ratios)

    daft_values = native_ratios("daft_builtin_embed")
    ray_values = native_ratios("ray_data_staged")
    static_values = project_ratios("fourjob_static_partition")
    proposed_values = project_ratios("fourjob_proposed")
    matrix = np.vstack([daft_values, ray_values, static_values, proposed_values])
    row_labels = [
        "Daft Built-in",
        "Ray Data",
        "Project static",
        "Project shared\n（状态仅观测）",
    ]

    # Mirror the text four-job figure: rows encode system/policy, columns encode
    # the same four jobs, and color encodes only within-row slowdown.  Project
    # static/shared remain separate mutually exclusive rows instead of being
    # overplotted as two marker types in a special panel.
    fig, ax = plt.subplots(figsize=(10.8, 5.25), constrained_layout=True)
    mesh = ax.pcolormesh(
        np.arange(len(jobs) + 1),
        np.arange(len(row_labels) + 1),
        matrix,
        cmap="Blues",
        vmin=1.0,
        vmax=3.2,
        edgecolors="white",
        linewidth=8,
        shading="flat",
    )
    ax.invert_yaxis()
    ax.set_aspect("auto")
    ax.set_xticks(np.arange(len(jobs)) + 0.5)
    ax.set_xticklabels(job_labels)
    ax.xaxis.tick_top()
    ax.tick_params(axis="x", length=0, pad=8)
    ax.set_yticks(np.arange(len(row_labels)) + 0.5)
    ax.set_yticklabels(row_labels)
    _axis_align_multiline_yticklabels(ax)
    ax.tick_params(axis="y", length=0, pad=8)

    for row in range(matrix.shape[0]):
        for column in range(matrix.shape[1]):
            value = matrix[row, column]
            text_color = "white" if value >= 2.2 else DARK
            ax.text(
                column + 0.5,
                row + 0.43,
                f"{value:.2f}×",
                ha="center",
                va="center",
                fontsize=12.5,
                fontweight="bold",
                color=text_color,
            )
            ax.text(
                column + 0.5,
                row + 0.66,
                f"JCT +{(value - 1.0) * 100:.0f}%",
                ha="center",
                va="center",
                fontsize=8.0,
                color=text_color,
                alpha=0.92,
            )

    colorbar = fig.colorbar(mesh, ax=ax, fraction=0.032, pad=0.025)
    colorbar.set_label("并发 JCT / 独立 JCT", rotation=90, labelpad=10)
    colorbar.set_ticks([1.0, 1.5, 2.0, 2.5, 3.0])
    colorbar.ax.set_yticklabels(["1.0×\n无影响", "1.5×", "2.0×", "2.5×", "3.0×"])
    colorbar.solids.set_rasterized(False)
    colorbar.outline.set_visible(False)

    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_title(
        "图像：四 Job 并发对 Short 与全部 Long 的影响依赖执行路径",
        loc="left",
        fontsize=15,
        fontweight="bold",
        pad=22,
    )
    fig.text(
        0.5,
        -0.02,
        "格内为 four-job JCT ÷ 本 Job isolated-single JCT；Short@0s，3×Long@0.5s。均值来自3次formal，SD保留于附录；只比较路径内退化，不作跨框架绝对JCT排名。Project shared的状态快照仅观测、不驱动动作。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_image_fourjob_normalized_impact")


def _load_json_replacing_invalid_utf8(path: Path) -> dict:
    return json.loads(path.read_bytes().decode("utf-8", errors="replace"))


def figure_cost_decision_v2() -> None:
    source = (
        ROOT
        / "experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/"
        "ce_context_loo_rerun_20260807.json"
    )
    estimators = _load_json_replacing_invalid_utf8(source)["estimators"]
    names = [
        "CE0_mean",
        "CE1_analytical",
        "CE2_lookup",
        "CE3_ridge",
        "CE4_lightgbm",
        "CE5_hybrid",
    ]
    if set(estimators) != set(names):
        raise ValueError("cost figure requires exactly the frozen CE0–CE5 estimators")
    labels = ["均值", "解析模型", "查表", "Ridge", "LightGBM", "混合模型"]
    worst_grey = "#5F6B75"
    rows = []
    for name, label in zip(names, labels, strict=True):
        estimator = estimators[name]
        summary = estimator["summary"]
        regret = summary["macro_fold_distributions"]["decision_regret_pct"]
        pairwise = summary["macro_fold_distributions"]["candidate_pairwise_accuracy"]
        folds = sorted(estimator["folds"], key=lambda fold: fold["context_id"])
        context_regrets = np.asarray(
            [fold["selection"]["decision_regret_pct"] for fold in folds],
            dtype=float,
        )
        if len(context_regrets) != 20:
            raise ValueError(f"{name} must contain exactly 20 decision contexts")
        if not np.isclose(context_regrets.mean(), regret["mean"]):
            raise ValueError(f"{name} context regrets do not reproduce macro mean")
        if not np.isclose(np.median(context_regrets), regret["median"]):
            raise ValueError(f"{name} context regrets do not reproduce median regret")
        if not np.isclose(context_regrets.max(), regret["max"]):
            raise ValueError(f"{name} context regrets do not reproduce max regret")
        rows.append(
            {
                "label": label,
                "pairwise": float(pairwise["mean"]),
                "regrets": context_regrets,
                "median_regret": float(regret["median"]),
                "mean_regret": float(regret["mean"]),
                "max_regret": float(regret["max"]),
            }
        )

    fig, axes = plt.subplots(
        1,
        2,
        figsize=(13.2, 6.15),
        constrained_layout=False,
        sharey=True,
        gridspec_kw={"width_ratios": [0.82, 2.18]},
    )
    fig.subplots_adjust(
        left=0.095,
        right=0.985,
        bottom=0.19,
        top=0.78,
        wspace=0.08,
    )
    y = np.arange(len(rows))[::-1]
    row_separators = (y[:-1] + y[1:]) / 2

    ax = axes[0]
    ax.axvspan(0.75, 0.86, color="#EAF7F5", zorder=0)
    ax.axvline(0.75, color=TEAL, linestyle="--", linewidth=1.0, zorder=1)
    for separator in row_separators:
        ax.axhline(separator, color="#E5EAED", linewidth=0.75, zorder=1.5)
    for yi, row in zip(y, rows, strict=True):
        color = BLUE if row["label"] == "混合模型" else GREY
        value = row["pairwise"]
        ax.scatter(value, yi, color=color, s=48, zorder=3)
        ax.text(
            value + 0.009,
            yi,
            f"{value:.3f}",
            color=color,
            ha="left",
            va="center",
            fontsize=8.0,
            fontweight="bold" if row["label"] == "混合模型" else "normal",
        )
    ax.set_xlim(0.45, 0.86)
    ax.set_ylim(-0.65, len(rows) - 0.35)
    ax.set_xlabel("Pairwise accuracy")
    ax.set_title("a   配置排序", loc="left", pad=10, fontsize=11.2)
    ax.text(
        0.97,
        0.96,
        "通过门槛 ≥ 0.75",
        transform=ax.transAxes,
        color=TEAL,
        ha="right",
        va="top",
        fontsize=7.6,
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.8, "pad": 1.5},
    )
    soft_grid(ax, axis="x")

    ax = axes[1]
    ax.axvspan(0, 5, color="#EAF7F5", zorder=0)
    ax.axvspan(5, 15, color="#F3F6F7", zorder=0)
    ax.axvline(5, color=TEAL, linestyle="--", linewidth=1.0, zorder=1)
    ax.axvline(15, color=GREY, linestyle=":", linewidth=1.15, zorder=1)
    for separator in row_separators:
        ax.axhline(separator, color="#E5EAED", linewidth=0.75, zorder=1.5)
    for row_index, (yi, row) in enumerate(zip(y, rows, strict=True)):
        color = BLUE if row["label"] == "混合模型" else GREY
        regrets = row["regrets"]
        rng = np.random.default_rng(20260810 + row_index)
        jitter = np.linspace(-0.24, 0.24, len(regrets))[rng.permutation(len(regrets))]
        ax.scatter(
            regrets,
            yi + jitter,
            color=color,
            s=23,
            alpha=0.66 if row["label"] == "混合模型" else 0.46,
            edgecolors="white",
            linewidths=0.35,
            zorder=3,
        )
        ax.scatter(
            row["median_regret"],
            yi,
            marker="D",
            color=color,
            edgecolors="none",
            linewidths=0,
            s=38,
            zorder=5,
        )
        max_index = int(np.argmax(regrets))
        ax.scatter(
            regrets[max_index],
            yi + jitter[max_index],
            color=BLUE if row["label"] == "混合模型" else worst_grey,
            edgecolors="none",
            linewidths=0,
            s=23,
            zorder=6,
        )
        if row["label"] == "混合模型":
            ax.text(
                row["median_regret"] + 1.2,
                yi + 0.10,
                f"中位 {row['median_regret']:.1f}%",
                color=BLUE,
                fontsize=8.0,
                fontweight="bold",
                ha="left",
                va="bottom",
            )
            ax.text(
                row["max_regret"] + 1.2,
                yi - 0.20,
                f"最坏 {row['max_regret']:.1f}%",
                color=BLUE,
                fontsize=8.0,
                fontweight="bold",
                ha="left",
                va="top",
            )
    ax.set_xlim(-2.5, 85)
    ax.set_ylim(-0.65, len(rows) - 0.35)
    ax.set_xlabel("单个 context 的 decision regret (%)")
    ax.set_title("b   决策损失分布（20 个场景）", loc="left", pad=10, fontsize=11.2)
    soft_grid(ax, axis="x")

    axes[0].set_yticks(y)
    axes[0].set_yticklabels([item["label"] for item in rows])
    axes[1].tick_params(axis="y", left=False, labelleft=False)
    fig.legend(
        handles=[
            Line2D(
                [0],
                [0],
                marker="o",
                color="none",
                markerfacecolor=GREY,
                markeredgecolor="white",
                markersize=6,
                alpha=0.58,
                label="单个场景（每行 n=20）",
            ),
            Line2D(
                [0],
                [0],
                marker="D",
                color="none",
                markerfacecolor=GREY,
                markeredgecolor="none",
                markersize=6.3,
                label="小菱形：中位数",
            ),
            Line2D(
                [0],
                [0],
                marker="o",
                color="none",
                markerfacecolor=worst_grey,
                markeredgecolor="none",
                markeredgewidth=0,
                markersize=6,
                label="深色点：最坏场景",
            ),
            Line2D(
                [0],
                [0],
                color=TEAL,
                linestyle="--",
                linewidth=1.1,
                label="平均门槛 5%",
            ),
            Line2D(
                [0],
                [0],
                color=GREY,
                linestyle=":",
                linewidth=1.2,
                label="最坏门槛 15%",
            ),
        ],
        loc="upper center",
        bbox_to_anchor=(0.63, 0.875),
        ncol=5,
        frameon=False,
        fontsize=7.75,
        handletextpad=0.4,
        columnspacing=1.35,
    )
    fig.suptitle(
        "代价估计必须同时通过配置排序与决策风险门禁",
        fontsize=14.5,
        fontweight="bold",
        y=0.965,
    )
    fig.text(
        0.5,
        0.055,
        "20-context leave-one-context-out；右图每行完整展示 20 个真实 decision regret，纵向抖动仅用于避免同值点重叠。"
        "小菱形为中位数；晋级同时要求 pairwise≥0.75、平均 regret≤5%、最坏 regret≤15%。Hybrid平均2.90%、最坏14.72%。\n"
        "逐行 MAE：Ridge 3.23s < 混合模型 3.98s，但最坏 regret 为 22.71% > 14.72%，"
        "说明点预测误差不能替代配置排序与决策风险评价。",
        ha="center",
        va="bottom",
        fontsize=8.1,
        color=GREY,
    )
    finish(fig, "opening_cost_model_decision_quality_v2")


def _box(ax, x, y, w, h, title, detail, color) -> None:
    patch = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle="round,pad=0.012,rounding_size=0.018",
        linewidth=1.4,
        edgecolor=color,
        facecolor="white",
    )
    ax.add_patch(patch)
    ax.text(x + 0.03 * w, y + 0.64 * h, title, color=color, fontweight="bold")
    ax.text(x + 0.03 * w, y + 0.25 * h, detail, color=DARK, fontsize=8.5)


def figure_work_to_schedule_overview() -> None:
    fig, ax = plt.subplots(figsize=(13.2, 5.8), constrained_layout=True)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    boxes = [
        (0.03, 0.58, 0.15, 0.20, "数据库记录", "prompt / image\njob + SLO"),
        (0.27, 0.58, 0.18, 0.20, "工作单元组织", "预算 + 均衡\n+ 局部性"),
        (0.55, 0.58, 0.20, 0.20, "准入与路由", "work credit\n+ 公平队列"),
        (0.82, 0.58, 0.15, 0.20, "执行阶段", "vLLM / CPU→GPU\n+ 数据库写回"),
    ]
    colors = [GREY, BLUE, TEAL, ORANGE]
    for item, color in zip(boxes, colors, strict=True):
        _box(ax, *item, color)
    for left, right in zip(boxes[:-1], boxes[1:], strict=True):
        start = (left[0] + left[2], left[1] + left[3] / 2)
        end = (right[0], right[1] + right[3] / 2)
        ax.add_patch(
            FancyArrowPatch(
                start,
                end,
                arrowstyle="-|>",
                mutation_scale=12,
                linewidth=1.3,
                color=DARK,
            )
        )

    estimator = FancyBboxPatch(
        (0.03, 0.26),
        0.25,
        0.18,
        boxstyle="round,pad=0.012,rounding_size=0.018",
        linewidth=1.2,
        edgecolor=ORANGE,
        facecolor="#FFF4E8",
    )
    ax.add_patch(estimator)
    ax.text(0.05, 0.375, "算子代价估计 · 共同使能", color=ORANGE, fontweight="bold")
    ax.text(
        0.05,
        0.305,
        "阶段 / 服务 / 剩余工作量\nSLO slack · 不确定区间 · 残差修正",
        fontsize=8.4,
        color=DARK,
    )

    descriptor = FancyBboxPatch(
        (0.34, 0.26),
        0.28,
        0.18,
        boxstyle="round,pad=0.012,rounding_size=0.018",
        linewidth=1.2,
        edgecolor=BLUE,
        facecolor=PALE_BLUE,
    )
    ax.add_patch(descriptor)
    ax.text(0.36, 0.375, "WorkDescriptor", color=BLUE, fontweight="bold")
    ax.text(
        0.36,
        0.305,
        "source / prepare / model / result\nlocality  ·  deadline  ·  calibrated interval",
        fontsize=8.4,
        color=DARK,
    )
    ax.add_patch(
        FancyArrowPatch(
            (0.28, 0.35),
            (0.34, 0.35),
            arrowstyle="-|>",
            mutation_scale=12,
            color=ORANGE,
        )
    )
    for target_x in [0.36, 0.65]:
        ax.add_patch(
            FancyArrowPatch(
                (target_x if target_x == 0.36 else 0.60, 0.44),
                (target_x, 0.58),
                arrowstyle="-|>",
                mutation_scale=12,
                color=BLUE,
            )
        )

    state = FancyBboxPatch(
        (0.66, 0.10),
        0.31,
        0.34,
        boxstyle="round,pad=0.012,rounding_size=0.018",
        linewidth=1.2,
        edgecolor=TEAL,
        facecolor="#EAF7F4",
    )
    ax.add_patch(state)
    ax.text(0.68, 0.36, "新鲜运行状态", color=TEAL, fontweight="bold")
    ax.text(
        0.68,
        0.255,
        "active / ready / queued work\n完成 / 服务速率 · 队龄\n"
        "状态过期或签名不匹配\n→ 回退冻结静态点",
        fontsize=8.4,
        color=DARK,
    )
    for target_x in [0.65, 0.895]:
        ax.add_patch(
            FancyArrowPatch(
                (target_x, 0.44),
                (target_x, 0.58),
                arrowstyle="-|>",
                mutation_scale=12,
                linestyle="--",
                color=TEAL,
            )
        )

    ax.text(
        0.03,
        0.92,
        "代价估计与运行状态让组织后的数据可被调度",
        fontsize=16,
        fontweight="bold",
        color=DARK,
    )
    ax.text(
        0.03,
        0.86,
        "代价估计同时使能两项研究内容；新鲜状态只改变有界的上游决策。",
        fontsize=10,
        color=GREY,
    )

    architecture_output = ROOT / "figures" / "architecture"
    architecture_output.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        architecture_output / "opening_work_to_schedule_overview.svg",
        bbox_inches="tight",
    )
    fig.savefig(
        architecture_output / "opening_work_to_schedule_overview.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)


def figure_ai_data_execution_boundary() -> None:
    fig, ax = plt.subplots(figsize=(13.2, 5.8), constrained_layout=True)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    _box(
        ax,
        0.03,
        0.37,
        0.15,
        0.28,
        "数据库 AI 算子",
        "记录 / 图像\njob + SLO + 语义",
        GREY,
    )
    _box(
        ax,
        0.80,
        0.42,
        0.17,
        0.23,
        "执行后端",
        "vLLM 生成\n或 typed GPU actor",
        ORANGE,
    )
    _box(
        ax,
        0.80,
        0.16,
        0.17,
        0.17,
        "数据库 / 向量写回",
        "正确性 + 质量\n外部 database-E2E",
        GREY,
    )

    layer = FancyBboxPatch(
        (0.24, 0.18),
        0.50,
        0.58,
        boxstyle="round,pad=0.016,rounding_size=0.02",
        linewidth=1.6,
        edgecolor=BLUE,
        facecolor="#F8FBFF",
    )
    ax.add_patch(layer)
    ax.text(
        0.27,
        0.70,
        "AI 数据执行层 · 研究边界",
        color=BLUE,
        fontweight="bold",
        fontsize=13,
    )
    ax.text(
        0.27,
        0.655,
        "位于模型服务上游；不修改模型、kernel 或服务内部调度器",
        color=GREY,
        fontsize=8.5,
    )

    _box(
        ax,
        0.27,
        0.41,
        0.20,
        0.18,
        "研究内容一",
        "工作单元构造\n+ 数据组织",
        BLUE,
    )
    _box(
        ax,
        0.51,
        0.41,
        0.20,
        0.18,
        "研究内容二",
        "状态感知准入 / 路由\n+ 多作业协调",
        TEAL,
    )
    shared = FancyBboxPatch(
        (0.27, 0.24),
        0.44,
        0.11,
        boxstyle="round,pad=0.010,rounding_size=0.015",
        linewidth=1.2,
        edgecolor=ORANGE,
        facecolor="#FFF4E8",
    )
    ax.add_patch(shared)
    ax.text(
        0.29,
        0.30,
        "共同算子代价估计",
        color=ORANGE,
        fontweight="bold",
    )
    ax.text(
        0.42,
        0.30,
        "阶段 / 服务 / 剩余工作量 · SLO slack · 不确定区间",
        color=DARK,
        fontsize=8.3,
    )
    for target_x in [0.37, 0.61]:
        ax.add_patch(
            FancyArrowPatch(
                (target_x, 0.35),
                (target_x, 0.41),
                arrowstyle="-|>",
                mutation_scale=11,
                color=ORANGE,
            )
        )

    for start, end in [
        ((0.18, 0.51), (0.24, 0.51)),
        ((0.74, 0.51), (0.80, 0.51)),
        ((0.885, 0.42), (0.885, 0.33)),
    ]:
        ax.add_patch(
            FancyArrowPatch(
                start,
                end,
                arrowstyle="-|>",
                mutation_scale=12,
                linewidth=1.3,
                color=DARK,
            )
        )

    ax.text(
        0.03,
        0.91,
        "研究对象是模型服务上游的 AI 数据执行层",
        fontsize=16,
        fontweight="bold",
        color=DARK,
    )
    ax.text(
        0.03,
        0.85,
        "Daft 与 Ray 提供机制；vLLM 与 typed GPU actor 是执行后端和对照。",
        fontsize=10,
        color=GREY,
    )

    architecture_output = ROOT / "figures" / "architecture"
    architecture_output.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        architecture_output / "opening_ai_data_execution_boundary.svg",
        bbox_inches="tight",
    )
    fig.savefig(
        architecture_output / "opening_ai_data_execution_boundary.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate selected opening-story figures from frozen evidence."
    )
    parser.add_argument(
        "--figures",
        nargs="+",
        choices=["A", "B", "C", "D", "E", "F", "H", "I", "J", "N", "T", "work-descriptor", "all"],
        default=["all"],
        help="Frozen figure identifiers to render; defaults to the historical full set.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    apply_style()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    selected = set(args.figures)
    if "all" in selected:
        selected = {"A", "B", "C", "D", "E", "F", "H", "I", "J", "N", "T", "work-descriptor"}
    renderers = {
        "A": figure_motivation_work_state_evidence,
        "B": figure_ai_data_execution_boundary,
        "C": figure_work_organization_v2,
        "D": figure_image_stage_evidence_set,
        "I": figure_image_baseline_evidence_map,
        "J": figure_image_fourjob_normalized_impact,
        "E": figure_cost_decision_v2,
        "F": figure_native_single_job_evidence,
        "H": figure_multijob_interference_tradeoff,
        "N": figure_native_fourjob_normalized_impact,
        "T": figure_text_baseline_evidence_map,
        "work-descriptor": figure_work_to_schedule_overview,
    }
    for figure_id in ["A", "T", "N", "B", "C", "H", "D", "I", "J", "E", "F", "work-descriptor"]:
        if figure_id in selected:
            renderers[figure_id]()


if __name__ == "__main__":
    main()
