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
    ax.set_xlabel("预计词元工作量（千词元）")
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
    labels = ["输入供应充足", "请求到达较慢"]
    active = [
        float(state.loc["high", "max_active_work_seen_mean"]) / 65536,
        float(state.loc["near", "max_active_work_seen_mean"]) / 65536,
    ]
    bars = ax.barh([1, 0], active, color=[BLUE, GREY], height=0.48)
    ax.axvline(1.0, color=DARK, linestyle="--", linewidth=1.0)
    ax.set_yticks([1, 0])
    ax.set_yticklabels(labels)
    ax.set_xlim(0, 1.12)
    ax.set_xlabel("运行期间在途工作峰值 / 每实例上限 65,536")
    mfus = [
        float(state.loc["high", "mfu_pct_mean"]),
        float(state.loc["near", "mfu_pct_mean"]),
    ]
    for bar, ratio, mfu in zip(bars, active, mfus, strict=True):
        ax.text(
            min(ratio + 0.03, 1.02),
            bar.get_y() + bar.get_height() / 2,
            f"达到上限 {ratio:.0%}；模型计算利用率 {mfu:.0f}%",
            va="center",
            fontsize=8.7,
        )
    soft_grid(ax, axis="x")
    ax.set_title("配置允许的上限不等于实际在途工作", loc="left")

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
    ax.text(28, 5.25, "工作不足，吞吐较低", ha="center", color=DARK)
    point65 = frontier.loc[frontier["active_work_per_endpoint"].eq(65536)].iloc[0]
    point98 = frontier.loc[frontier["active_work_per_endpoint"].eq(98304)].iloc[0]
    throughput_gain_pct = (
        float(point98["tokens_per_s_mean"]) / float(point65["tokens_per_s_mean"]) - 1
    ) * 100
    p99_gain_pct = (
        float(point98["request_p99_s_mean"]) / float(point65["request_p99_s_mean"]) - 1
    ) * 100
    y64 = y[np.where(x == 64)[0][0]]
    ax.text(66.5, y64 - 0.33, "65K：已测峰值的 97.8%", fontsize=8.8, color=DARK)
    ax.text(
        130,
        6.25,
        "继续增加在途工作（65K→98K）\n"
        f"吞吐仅 +{throughput_gain_pct:.1f}%\n"
        f"P99：{float(point65['request_p99_s_mean']):.1f}→"
        f"{float(point98['request_p99_s_mean']):.1f} 秒（+{p99_gain_pct:.1f}%）",
        ha="right",
        va="center",
        fontsize=8.6,
        linespacing=1.30,
        color=DARK,
        bbox={
            "boxstyle": "round,pad=0.45",
            "facecolor": "#FFF7ED",
            "edgecolor": ORANGE,
            "linewidth": 1.1,
        },
    )
    ax.set(
        xlabel="每个服务实例的在途工作量上限（千词元）",
        ylabel="吞吐（千词元/秒）",
        xlim=(12, 136),
        ylim=(4.4, 8.7),
    )
    soft_grid(ax)
    ax.set_title("增加在途工作到约 65K 后，吞吐已接近最高值", loc="left")
    ax.legend(
        [Line2D([0], [0], color=BLUE, marker="o", linewidth=1.6, markersize=5)],
        ["圆点=3 次统计运行均值；误差线=标准差"],
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
        "行数、配置上限与实际在途工作量是三个不同概念",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "a：RTX 5070 / Qwen2.5-1.5B；b–c：2×RTX 4090 / Qwen2.5-7B。点与误差线表示 3 次统计运行的均值±标准差；上限从 65K 增至 98K 时，P99 请求延迟由 36.8 秒增至 40.0 秒。",
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
    ax.set_xlabel("预计词元工作量（千词元）")
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
        "行数、配置上限与实际在途工作量是三个不同概念",
        fontsize=15,
        fontweight="bold",
        y=0.94,
    )
    fig.text(
        0.5,
        0.07,
        "实验配置：RTX 5070，Qwen2.5-1.5B；每批固定 16 条记录，预计工作量为输入词元数与输出上限之和。",
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
    labels = ["输入供应充足", "请求到达较慢"]
    active = [
        float(state.loc["high", "max_active_work_seen_mean"]) / 65536,
        float(state.loc["near", "max_active_work_seen_mean"]) / 65536,
    ]
    bars = ax.barh([1, 0], active, color=[BLUE, GREY], height=0.48)
    ax.axvline(1.0, color=DARK, linestyle="--", linewidth=1.0)
    ax.set_yticks([1, 0])
    ax.set_yticklabels(labels)
    ax.set_xlim(0, 1.12)
    ax.set_xlabel("运行期间在途工作峰值 / 每实例上限 65,536")
    mfus = [
        float(state.loc["high", "mfu_pct_mean"]),
        float(state.loc["near", "mfu_pct_mean"]),
    ]
    for bar, ratio, mfu in zip(bars, active, mfus, strict=True):
        ax.text(
            min(ratio + 0.03, 1.02),
            bar.get_y() + bar.get_height() / 2,
            f"达到上限 {ratio:.0%}；模型计算利用率 {mfu:.0f}%",
            va="center",
            fontsize=8.7,
        )
    soft_grid(ax, axis="x")
    ax.set_title("配置允许的上限不等于实际在途工作", loc="left")

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
    ax.text(28, 5.25, "工作不足，吞吐较低", ha="center", color=DARK)
    point65 = frontier.loc[frontier["active_work_per_endpoint"].eq(65536)].iloc[0]
    point98 = frontier.loc[frontier["active_work_per_endpoint"].eq(98304)].iloc[0]
    throughput_gain_pct = (
        float(point98["tokens_per_s_mean"]) / float(point65["tokens_per_s_mean"]) - 1
    ) * 100
    p99_gain_pct = (
        float(point98["request_p99_s_mean"]) / float(point65["request_p99_s_mean"]) - 1
    ) * 100
    y64 = y[np.where(x == 64)[0][0]]
    ax.text(66.5, y64 - 0.33, "65K：已测峰值的 97.8%", fontsize=8.8, color=DARK)
    ax.text(
        130,
        6.25,
        "继续增加在途工作（65K→98K）\n"
        f"吞吐仅 +{throughput_gain_pct:.1f}%\n"
        f"P99：{float(point65['request_p99_s_mean']):.1f}→"
        f"{float(point98['request_p99_s_mean']):.1f} 秒（+{p99_gain_pct:.1f}%）",
        ha="right",
        va="center",
        fontsize=8.6,
        linespacing=1.30,
        color=DARK,
        bbox={
            "boxstyle": "round,pad=0.45",
            "facecolor": "#FFF7ED",
            "edgecolor": ORANGE,
            "linewidth": 1.1,
        },
    )
    ax.set(
        xlabel="每个服务实例的在途工作量上限（千词元）",
        ylabel="吞吐（千词元/秒）",
        xlim=(12, 136),
        ylim=(4.4, 8.7),
    )
    soft_grid(ax)
    ax.set_title("增加在途工作到约 65K 后，吞吐已接近最高值", loc="left")
    ax.legend(
        [Line2D([0], [0], color=BLUE, marker="o", linewidth=1.6, markersize=5)],
        ["圆点=3 次统计运行均值；误差线=标准差"],
        loc="lower right",
        fontsize=8.0,
        handlelength=2.0,
    )
    fig.suptitle(
        "行数、配置上限与实际在途工作量是三个不同概念",
        fontsize=15,
        fontweight="bold",
        y=0.94,
    )
    fig.text(
        0.5,
        0.07,
        "实验配置：2×RTX 4090，Qwen2.5-7B，2 个模型服务实例；每个工作量上限进行 3 次统计运行。",
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
        (axes[1], cache_hit * 100, "共享前缀缓存命中率", "%", (0, 86)),
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
                "低压力\n2 个服务实例 · 键值缓存峰值 7%–10%",
                "高压力\n4 个服务实例 · 键值缓存峰值 98%–100%",
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
        "服务部署条件改变后，重排方法的缓存命中率与吞吐同时下降",
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
        "每条线连接同一方法在低、高压力条件下 3 次统计运行的中位数；实线圆点=保持输入顺序，虚线方点=重排/装箱，不画误差线。硬件相同，但服务实例数、每个实例的显存比例和运行压力同时改变；不作跨条件服务容量排名。",
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
        ("direct_static_sharded", "直接静态分片", GREY),
        ("duckdb_ai_static_sharded", "DuckDB AI", ORANGE),
        ("project_frozen_static", "项目固定参数方案", BLUE),
    ]
    y = np.arange(len(product_arms))[::-1]
    means = [float(squad.loc[arm, "correct_rows_per_s_mean"]) for arm, _, _ in product_arms]
    errors = [float(squad.loc[arm, "correct_rows_per_s_sd"]) for arm, _, _ in product_arms]
    colors = [color for _, _, color in product_arms]
    ax.barh(y, means, color=colors, height=0.56)
    ax.set_yticks(y)
    ax.set_yticklabels([label for _, label, _ in product_arms])
    ax.set_xlim(0, 155)
    ax.set_xlabel("SQuAD 正确结果行数（行/秒）")
    for yi, value, error in zip(y, means, errors, strict=True):
        ax.text(value + 1.4, yi, f"{value:.1f} ± {error:.1f}", va="center", fontsize=8.5)
    ax.set_ylim(-1.0, 2.5)
    ax.text(
        3,
        -0.78,
        "统一 PostgreSQL 数据源与结果收集；3 次统计运行\n"
        "ShareGPT 完整路径的请求提交速度和输出要求不同，不作排名",
        ha="left",
        va="bottom",
        fontsize=8.1,
        color=DARK,
    )
    ax.set_title("完整数据库执行路径：SQuAD 结果可直接比较", loc="left")
    soft_grid(ax, axis="x")

    ax = axes[1]
    chat_arms = [
        ("bounded_http", "直接调用\n（服务能力参照）", DARK),
        ("daft_native", "Daft 原生执行", TEAL),
        ("daft_ray", "Daft Ray 执行", PURPLE),
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
        "同一 ShareGPT 输入清单；3 次统计运行；调度由被测框架负责\n"
        "项目方法尚无相同输入与计时范围的对应结果，因此不加入本组排名",
        ha="right",
        va="bottom",
        fontsize=8.1,
        color=GREY,
    )
    ax.set_title("框架原生执行路径：ShareGPT 模型服务吞吐", loc="left")
    soft_grid(ax, axis="x")

    fig.suptitle(
        "两组文本执行路径需要分别比较",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "左右两图的输入、输出要求和指标不同，只能分别解释。条末数字为均值 ± 标准差。",
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
    # Panel c compares the two policies on one shared basis: full-pool isolated
    # JCT / concurrent JCT. `static_fourjob` and `shared_fourjob` both use that
    # basis; `matched_competition_static` (quarter-pool isolated baseline) is a
    # different denominator and must not be plotted against the shared arm.
    fairness = fairness.loc[
        fairness["system"].eq("project")
        & fairness["comparison"].isin(["static_fourjob", "shared_fourjob"])
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
    job_labels = ["短作业", "长作业 1", "长作业 2", "长作业 3"]
    progress = {}
    for _, row in fairness.iterrows():
        progress[str(row["comparison"])] = json.loads(row["normalized_progress_by_job"])
    static_progress = progress["static_fourjob"]
    shared_progress = progress["shared_fourjob"]
    static_jain = float(
        fairness.loc[
            fairness["comparison"].eq("static_fourjob"),
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
    scenario_labels = ["独立运行", "1/4 份额", "四作业\n静态竞争", "四作业\n共享方式"]
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
    ax_jct.set_ylabel("相对完成时间（独立运行 = 1，越低越好）")
    ax_jct.set_title("同一作业在份额减少、并发竞争和共享方式下的完成时间变化", loc="left", pad=12)
    soft_grid(ax_jct, axis="y")
    ax_jct.legend(loc="upper left", frameon=False, ncol=4, title="每条线代表同一个作业")

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
            "整组完成时间",
            f"{static_jct:.1f}s",
            f"{shared_jct:.1f}s",
            relative_change(static_jct, shared_jct),
        ),
        (
            "模型计算利用率",
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
    ax_eff.set_title("共享未使用份额提高总体效率", loc="left", pad=10)

    progress_values = np.array(
        [[static_progress[job], shared_progress[job]] for job in jobs]
    )
    policy_x = np.arange(2)
    for row, (job_label, color, marker) in enumerate(
        zip(job_labels, job_colors, job_markers, strict=True)
    ):
        values = progress_values[row]
        # Plain line between the two measured policy points; it marks the
        # controlled static/shared A/B contrast, not a continuous path.
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
    ax_fair.set_ylim(0.18, 0.88)
    ax_fair.set_ylabel("相对独立运行的完成速率（独立运行 = 1，越高越好）")
    soft_grid(ax_fair, axis="y")
    ax_fair.set_title("四个作业均更快，但改善幅度不同", loc="left", pad=10)
    ax_fair.legend(loc="upper center", frameon=False, ncol=4, fontsize=7.5)
    ax_fair.text(
        0.97,
        0.04,
        f"Jain 指数：{static_jain:.3f} → {shared_jain:.3f}    "
        f"长作业完成时间极差：{static_spread:.1f}s → {shared_spread:.1f}s",
        transform=ax_fair.transAxes,
        ha="right",
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
        "相同总容量下：共享未使用份额的效率与作业差异",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.015,
        "短作业在0秒启动，三个长作业在5秒启动；每条线始终代表同一个作业，点为3次统计运行均值。静态分区和共享未使用份额采用相同总上限；独立运行与1/4份额用于区分份额减少和并发竞争的影响；图c统一按各作业独占完整资源时的完成时间归一化。",
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
        ylabel="CPU 准备时间 / GPU 执行时间",
        ylim=(0, 35),
    )
    ax.legend(
        [Line2D([0], [0], color=ORANGE, marker="o", linewidth=1.6)],
        ["圆点=中位数；误差线=IQR（n=30）"],
        loc="lower right",
        fontsize=7.6,
    )
    ax.set_title("图像准备是独立且耗时较长的工作阶段", loc="left")
    soft_grid(ax)

    ax = axes[1]
    transfer_defs = [
        ("r0_gpu_resident", "张量已在 GPU", DARK, "o"),
        ("r1_pinned_fp16", "不可换页主机内存（FP16）", TEAL, "s"),
        ("r2_pageable_fp32", "普通主机内存（FP32）", ORANGE, "D"),
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
    ax.set_xlabel("吞吐（千张图像/秒，每批 64 张）")
    ax.set_xlim(0, 10.8)
    ax.set_ylim(-0.45, 2.45)
    ax.text(
        0.98,
        0.06,
        "GPU 张量→不可换页内存：约 −11%\nGPU 张量→普通内存：约 −80%",
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
    ax.set_title("输入表示与主机端数据组织会改变阶段效率", loc="left")
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
        ax.text(window, value + 0.055, f"等待 {wait:.2f} 秒", ha="center", fontsize=7.7)
    ax.set(
        xlabel="最大在途批次数",
        ylabel="初始化后吞吐（千张图像/秒）",
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
        ["圆点=单次范围筛查", "菱形=最高吞吐点", "红叉=继续增加在途批次后回落"],
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
        "图像工作需要区分准备、数据传递与模型执行阶段",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "a–b：每种配置重复 30 次，显示中位数与四分位距；c：5K 图像单次范围筛查，只用于选择后续实验的提交窗口范围，不证明某种动态方法已经有效。",
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
    ax.set_title("图像也是分阶段工作量：张数描述不了阶段压力", loc="left")
    soft_grid(ax)
    fig.suptitle(
        "图像工作需要区分准备、数据传递与模型执行阶段",
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
    ax.set_title("输入表示改变阶段执行效率", loc="left")
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
    ax.set_title("阶段供给不匹配导致欠供给或等待堆积", loc="left")
    soft_grid(ax)

    fig.suptitle(
        "AI Work 需要分阶段描述",
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


def figure_cost_decision_v3() -> None:
    """v3: 两 panel，panel b 标题注明“= 模型预测最优与实际最优的偏离”。

    与 v2 的区别：v2 曾是两 panel（配置排序 + 决策损失分布）。本版确认
    “模型预测最优相对实际最优的偏离”在数值上等于 decision regret（由
    candidates[].predicted_mean_s 的 argmin 重建并与 selection.decision_regret_pct
    校验一致），因此不单独设 panel c，只在 panel b 标题和图注中写明两个说法
    是同一个量。v2 文件保留不动，v3 输出到新文件。
    """

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
    ax.set_xlabel("候选配置两两排序准确率")
    ax.set_title("a   配置排序", loc="left", pad=10, fontsize=11.2)
    ax.text(
        0.97,
        0.96,
        "参考值 ≥ 0.75",
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
    ax.set_xlabel("单个执行情境的决策损失 (%)")
    ax.set_title(
        "b   决策损失分布（模型预测最优与实际最优的偏离）（20 个场景）",
        loc="left",
        pad=10,
        fontsize=11.2,
    )
    ax.tick_params(axis="y", left=False, labelleft=False)
    soft_grid(ax, axis="x")

    axes[0].set_yticks(y)
    axes[0].set_yticklabels([item["label"] for item in rows])
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
                label="单个执行情境（每种方法 n=20）",
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
                label="平均损失参考值 5%",
            ),
            Line2D(
                [0],
                [0],
                color=GREY,
                linestyle=":",
                linewidth=1.2,
                label="最坏情境参考值 15%",
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
        "代价估计需要同时评价配置排序与决策损失",
        fontsize=14.5,
        fontweight="bold",
        y=0.965,
    )
    fig.text(
        0.5,
        0.055,
        "20个执行情境采用逐一留出验证；图b每行完整展示20个留出情境，纵向抖动仅用于避免相同数值的点重叠。"
        "小菱形为中位数；参考要求为排序准确率≥0.75、平均决策损失≤5%、最坏情境损失≤15%。混合模型平均2.90%、最坏14.72%。\n"
        "逐次执行时间平均绝对误差：Ridge 3.23s < 混合模型 3.98s，但最坏决策损失为22.71% > 14.72%，"
        "说明点预测误差不能替代配置排序与决策损失评价。图b的决策损失就是“模型预测最优与实际最优的偏离”"
        "（= 100×(预测最优候选实际耗时 − 实际最优耗时)/实际最优耗时），两者是同一个量。",
        ha="center",
        va="bottom",
        fontsize=8.1,
        color=GREY,
    )
    finish(fig, "opening_cost_model_decision_quality_v3")


def figure_cost_decision_v4() -> None:
    """三 panel：六种方法的预测偏差、四种上限排序、选择后的额外执行时间。"""

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
        raise ValueError("cost figure requires exactly CE0-CE5 estimators")

    labels = ["均值", "解析模型", "查表", "岭回归", "LightGBM", "混合模型"]
    rows = []
    for name, label in zip(names, labels, strict=True):
        estimator = estimators[name]
        summary = estimator["summary"]["macro_fold_distributions"]
        folds = sorted(estimator["folds"], key=lambda fold: fold["context_id"])
        regrets = np.asarray(
            [fold["selection"]["decision_regret_pct"] for fold in folds],
            dtype=float,
        )
        if len(regrets) != 20:
            raise ValueError(f"{name} must contain exactly 20 decision contexts")
        expected = summary["decision_regret_pct"]
        if not np.isclose(regrets.mean(), expected["mean"]):
            raise ValueError(f"{name} regrets do not reproduce macro mean")
        if not np.isclose(np.median(regrets), expected["median"]):
            raise ValueError(f"{name} regrets do not reproduce median")
        if not np.isclose(regrets.max(), expected["max"]):
            raise ValueError(f"{name} regrets do not reproduce maximum")
        candidates = [
            candidate
            for fold in folds
            for candidate in fold["candidates"]
        ]
        if len(candidates) != 80:
            raise ValueError(f"{name} must contain 20 contexts x 4 candidates")
        actual = np.asarray(
            [float(candidate["actual_mean_s"]) for candidate in candidates],
            dtype=float,
        )
        predicted = np.asarray(
            [float(candidate["predicted_mean_s"]) for candidate in candidates],
            dtype=float,
        )
        signed_error_pct = 100 * (predicted - actual) / actual
        rows.append(
            {
                "label": label,
                "pairwise": float(
                    summary["candidate_pairwise_accuracy"]["mean"]
                ),
                "regrets": regrets,
                "median": float(expected["median"]),
                "mean": float(expected["mean"]),
                "max": float(expected["max"]),
                "actual": actual,
                "predicted": predicted,
                "signed_error_pct": signed_error_pct,
                "mae_s": float(np.mean(np.abs(predicted - actual))),
                "median_abs_error_pct": float(
                    np.median(np.abs(signed_error_pct))
                ),
            }
        )

    fig = plt.figure(figsize=(15.6, 6.25), constrained_layout=False)
    gs = fig.add_gridspec(1, 3, width_ratios=[1.60, 0.65, 1.55])
    fig.subplots_adjust(
        left=0.058,
        right=0.988,
        bottom=0.205,
        top=0.745,
        wspace=0.28,
    )
    pred_grid = gs[0, 0].subgridspec(2, 3, wspace=0.12, hspace=0.30)
    pred_axes = [
        fig.add_subplot(pred_grid[row_index, column_index])
        for row_index in range(2)
        for column_index in range(3)
    ]
    ax_rank = fig.add_subplot(gs[0, 1])
    ax_regret = fig.add_subplot(gs[0, 2])

    y = np.arange(len(rows))[::-1]
    separators = (y[:-1] + y[1:]) / 2
    method_colors = [GREY, TEAL, ORANGE, "#7D6AA5", "#4F8A6B", BLUE]
    time_limit = 52
    for panel_index, (ax_pred, row, color) in enumerate(
        zip(pred_axes, rows, method_colors, strict=True)
    ):
        under_mask = row["predicted"] < row["actual"]
        over_mask = ~under_mask
        ax_pred.vlines(
            row["actual"][under_mask],
            row["predicted"][under_mask],
            row["actual"][under_mask],
            color="#3F88A8",
            linewidth=0.65,
            alpha=0.24,
            zorder=1.5,
        )
        ax_pred.vlines(
            row["actual"][over_mask],
            row["actual"][over_mask],
            row["predicted"][over_mask],
            color=ORANGE,
            linewidth=0.65,
            alpha=0.24,
            zorder=1.5,
        )
        ax_pred.scatter(
            row["actual"],
            row["actual"],
            facecolors="white",
            edgecolors=GREY,
            s=10,
            alpha=0.52,
            linewidths=0.45,
            zorder=2.5,
        )
        ax_pred.scatter(
            row["actual"],
            row["predicted"],
            color=color,
            s=13,
            alpha=0.48,
            edgecolors="white",
            linewidths=0.25,
            zorder=3,
        )
        ax_pred.plot(
            [0, time_limit],
            [0, time_limit],
            color=DARK,
            linewidth=0.75,
            linestyle="--",
            zorder=2,
        )
        ax_pred.set_xlim(0, time_limit)
        ax_pred.set_ylim(0, time_limit)
        ax_pred.set_aspect("equal", adjustable="box")
        ax_pred.set_xticks([0, 25, 50])
        ax_pred.set_yticks([0, 25, 50])
        ax_pred.tick_params(labelsize=6.2, pad=1.5, length=2)
        ax_pred.set_title(row["label"], loc="left", pad=2, fontsize=8.3)
        ax_pred.text(
            0.04,
            0.94,
            f"中位相对偏差 {row['median_abs_error_pct']:.1f}%\n平均误差 {row['mae_s']:.2f} s",
            transform=ax_pred.transAxes,
            ha="left",
            va="top",
            fontsize=6.6,
            color=DARK,
            bbox={"facecolor": "white", "edgecolor": "none", "pad": 0.7, "alpha": 0.76},
            zorder=4,
        )
        if panel_index // 3 == 1:
            ax_pred.set_xlabel("实测时间 (s)", fontsize=6.8, labelpad=1.5)
        else:
            ax_pred.tick_params(axis="x", labelbottom=False)
        if panel_index % 3 == 0:
            ax_pred.set_ylabel("预测时间 (s)", fontsize=6.8, labelpad=1.5)
        else:
            ax_pred.tick_params(axis="y", labelleft=False)
        soft_grid(ax_pred, axis="both")
    fig.text(
        0.058,
        0.805,
        "a   六种方法：预测时间与真实时间",
        ha="left",
        va="center",
        fontsize=11.2,
        fontweight="bold",
    )
    fig.text(
        0.42,
        0.805,
        "空心点：真实｜实心点：预测｜竖线：相差的秒数",
        ha="right",
        va="center",
        fontsize=7.2,
        color=GREY,
    )

    ax_rank.axvspan(0.75, 0.86, color="#EAF7F5", zorder=0)
    ax_rank.axvline(0.75, color=TEAL, linestyle="--", linewidth=1.0, zorder=1)
    for separator in separators:
        ax_rank.axhline(separator, color="#E5EAED", linewidth=0.75, zorder=1.5)
    for yi, row in zip(y, rows, strict=True):
        color = BLUE if row["label"] == "混合模型" else GREY
        ax_rank.scatter(row["pairwise"], yi, color=color, s=46, zorder=3)
        ax_rank.text(
            row["pairwise"] + 0.009,
            yi,
            f"{row['pairwise']:.3f}",
            color=color,
            ha="left",
            va="center",
            fontsize=7.8,
            fontweight="bold" if row["label"] == "混合模型" else "normal",
        )
    ax_rank.set_xlim(0.45, 0.86)
    ax_rank.set_ylim(-0.65, len(rows) - 0.35)
    ax_rank.set_yticks(y)
    ax_rank.set_yticklabels([row["label"] for row in rows])
    ax_rank.set_xlabel("两两排序准确率")
    ax_rank.set_title("b   四种上限的快慢排序", loc="left", pad=10, fontsize=11.2)
    ax_rank.text(
        0.97,
        0.96,
        "参考值 0.75",
        transform=ax_rank.transAxes,
        color=TEAL,
        ha="right",
        va="top",
        fontsize=7.4,
    )
    soft_grid(ax_rank, axis="x")

    worst_grey = "#5F6B75"
    ax_regret.axvspan(0, 5, color="#EAF7F5", zorder=0)
    ax_regret.axvspan(5, 15, color="#F3F6F7", zorder=0)
    ax_regret.axvline(5, color=TEAL, linestyle="--", linewidth=1.0, zorder=1)
    ax_regret.axvline(15, color=GREY, linestyle=":", linewidth=1.15, zorder=1)
    for separator in separators:
        ax_regret.axhline(separator, color="#E5EAED", linewidth=0.75, zorder=1.5)
    for row_index, (yi, row) in enumerate(zip(y, rows, strict=True)):
        color = BLUE if row["label"] == "混合模型" else GREY
        regrets = row["regrets"]
        rng = np.random.default_rng(20260810 + row_index)
        jitter = np.linspace(-0.24, 0.24, len(regrets))[rng.permutation(len(regrets))]
        ax_regret.scatter(
            regrets,
            yi + jitter,
            color=color,
            s=22,
            alpha=0.66 if row["label"] == "混合模型" else 0.46,
            edgecolors="white",
            linewidths=0.35,
            zorder=3,
        )
        ax_regret.scatter(
            row["median"],
            yi,
            marker="D",
            color=color,
            edgecolors="none",
            s=36,
            zorder=5,
        )
        max_index = int(np.argmax(regrets))
        ax_regret.scatter(
            regrets[max_index],
            yi + jitter[max_index],
            color=BLUE if row["label"] == "混合模型" else worst_grey,
            edgecolors="none",
            s=22,
            zorder=6,
        )
        if row["label"] == "混合模型":
            ax_regret.scatter(
                row["mean"],
                yi,
                marker="^",
                color=BLUE,
                edgecolors="white",
                linewidths=0.4,
                s=48,
                zorder=7,
            )
            ax_regret.text(
                row["median"] + 1.2,
                yi + 0.18,
                f"中位 {row['median']:.1f}%",
                color=BLUE,
                fontsize=7.8,
                fontweight="bold",
                ha="left",
                va="bottom",
            )
            ax_regret.text(
                row["mean"] + 1.2,
                yi - 0.02,
                f"平均 {row['mean']:.2f}%",
                color=BLUE,
                fontsize=7.8,
                fontweight="bold",
                ha="left",
                va="center",
            )
            ax_regret.text(
                row["max"] + 1.2,
                yi - 0.20,
                f"最差 {row['max']:.1f}%",
                color=BLUE,
                fontsize=7.8,
                fontweight="bold",
                ha="left",
                va="top",
            )
    ax_regret.set_xlim(-2.5, 85)
    ax_regret.set_ylim(-0.65, len(rows) - 0.35)
    ax_regret.set_xlabel("所选上限比实测最快上限多耗时 (%)")
    ax_regret.set_title(
        "c   六种方法选定上限后的实际结果",
        loc="left",
        pad=10,
        fontsize=11.2,
    )
    ax_regret.set_yticks(y)
    ax_regret.set_yticklabels([row["label"] for row in rows], fontsize=7.2)
    ax_regret.tick_params(axis="y", left=False, labelleft=True, pad=3)
    soft_grid(ax_regret, axis="x")

    fig.legend(
        handles=[
            Line2D(
                [0],
                [0],
                marker="o",
                color="none",
                markerfacecolor=GREY,
                markeredgecolor="white",
                markersize=5.8,
                alpha=0.58,
                label="单个留出场景",
            ),
            Line2D(
                [0],
                [0],
                marker="D",
                color="none",
                markerfacecolor=GREY,
                markeredgecolor="none",
                markersize=6.0,
                label="菱形：中位数",
            ),
            Line2D(
                [0],
                [0],
                color=TEAL,
                linestyle="--",
                linewidth=1.1,
                label="平均多耗时参考值 5%",
            ),
            Line2D(
                [0],
                [0],
                color=GREY,
                linestyle=":",
                linewidth=1.2,
                label="最差场景参考值 15%",
            ),
        ],
        loc="upper center",
        bbox_to_anchor=(0.74, 0.865),
        ncol=4,
        frameon=False,
        fontsize=7.4,
        handletextpad=0.35,
        columnspacing=1.0,
    )
    fig.suptitle(
        "代价估计同时检查预测偏差、四种上限的排序和选择结果",
        fontsize=14.5,
        fontweight="bold",
        y=0.965,
    )
    fig.text(
        0.5,
        0.055,
        "图a：每种方法包含80组留一场景结果；同一横坐标上的空心点和实心点分别表示真实时间和预测时间，竖线长度表示两者相差的秒数。\n"
        "图c：比较错误选择造成的额外耗时；混合模型的中位数为0，20个场景平均为2.90%，最差为14.72%。",
        ha="center",
        va="bottom",
        fontsize=8.8,
        color=GREY,
    )
    finish(fig, "opening_cost_model_decision_quality_v4")


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
        "E": figure_cost_decision_v4,
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
