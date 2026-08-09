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
        color=RED,
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
    ax.text(
        0.97,
        0.50,
        "同一上限，不同运行状态",
        transform=ax.transAxes,
        ha="right",
        color=RED,
        fontweight="bold",
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
    ax.text(108, 8.53, "边际收益递减", ha="center", color=RED)
    y64 = y[np.where(x == 64)[0][0]]
    ax.text(66.5, y64 - 0.33, "65K：已测峰值的 97.8%", fontsize=8.8, color=DARK)
    ax.text(
        0.98,
        0.05,
        "P99: 36.8 s @65K → 40.0 s @98K",
        transform=ax.transAxes,
        ha="right",
        color=RED,
        fontsize=8.8,
    )
    ax.set(
        xlabel="每 endpoint active work（千 token）",
        ylabel="吞吐（千 token/s）",
        xlim=(12, 136),
        ylim=(4.4, 8.7),
    )
    soft_grid(ax)
    ax.set_title("提交控制应先标定最小近饱和点", loc="left")

    fig.suptitle(
        "动机：描述工作量、感知运行状态、约束提交压力",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "a：RTX 5070 / Qwen2.5-1.5B 机制证据；b–c：2×RTX 4090 / Qwen2.5-7B。均为 n=3 formal，不作跨 panel 性能比较。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_motivation_work_state")


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
    labels = ["固定行数", "顺序 token 预算", "长度对齐", "最佳适配", "行上限感知"]
    return pd.concat(frames, ignore_index=True), methods, labels


def figure_work_organization_v2() -> None:
    runs, methods, labels = _organization_data()
    fig, axes = plt.subplots(1, 2, figsize=(12.6, 5.1), constrained_layout=True)
    colors = [GREY, BLUE, ORANGE, ORANGE, ORANGE]
    for ax, regime, subtitle in zip(
        axes,
        ["Low KV pressure", "High KV pressure"],
        [
            "低 KV 压力（2 endpoint，KV max 7%–10%）：策略差异约 12%",
            "高 KV 压力（4 endpoint，KV max 98%–100%）：局部性主导",
        ],
        strict=True,
    ):
        subset = runs.loc[runs["regime"].eq(regime)]
        medians = np.array(
            [
                subset.loc[subset["scenario_id"].eq(method), "tokens_per_s"].median()
                / 1000
                for method in methods
            ]
        )
        y = np.arange(len(methods))[::-1]
        bars = ax.barh(y, medians, color=colors, height=0.58)
        ax.set_yticks(y)
        ax.set_yticklabels(labels)
        ax.set_xlim(0, 60)
        ax.set_xlabel("端到端吞吐（千 token/s）")
        ax.set_title(subtitle, loc="left")
        for bar, method, value in zip(bars, methods, medians, strict=True):
            raw = (
                subset.loc[subset["scenario_id"].eq(method), "tokens_per_s"]
                .to_numpy(dtype=float)
                / 1000
            )
            ax.errorbar(
                value,
                bar.get_y() + bar.get_height() / 2,
                xerr=np.array([[value - raw.min()], [raw.max() - value]]),
                fmt="none",
                ecolor=DARK,
                elinewidth=1.1,
                capsize=2.5,
                zorder=4,
            )
            annotation = f"{value:.1f}"
            if regime == "High KV pressure":
                hit = subset.loc[
                    subset["scenario_id"].eq(method),
                    "vllm_prefix_cache_hit_rate",
                ].median()
                annotation += f"  |  缓存命中 {hit:.2f}"
            label_inside = regime == "Low KV pressure"
            ax.text(
                value - 1.0 if label_inside else value + 2.0,
                bar.get_y() + bar.get_height() / 2,
                annotation,
                ha="right" if label_inside else "left",
                va="center",
                fontsize=8.7,
                color="white" if label_inside else DARK,
                fontweight="semibold" if label_inside else "normal",
            )
        soft_grid(ax, axis="x")
    fig.suptitle(
        "数据组织效果取决于是否保住真正稀缺的资源",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "相同双卡硬件，不同 endpoint 拓扑；柱=3 次 formal 的中位数，细线=最小–最大。说明 regime/locality 机制，不作容量排名。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_work_organization_regime_v2")


TEXT_ARM_ORDER = ["bounded_http", "daft_native", "daft_ray", "ray_data_http"]
TEXT_ARM_LABELS = ["Bounded HTTP", "Daft Native", "Daft Ray", "Ray Data"]
TEXT_ARM_COLORS = [DARK, BLUE, TEAL, ORANGE]
TEXT_ARM_MARKERS = ["o", "s", "^", "D"]


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
) -> None:
    """Draw one original-unit state metric as mean ± SD over three formal runs."""

    y = np.arange(len(TEXT_ARM_ORDER))[::-1]
    for yi, arm, color, marker in zip(
        y,
        TEXT_ARM_ORDER,
        TEXT_ARM_COLORS,
        TEXT_ARM_MARKERS,
        strict=True,
    ):
        values = runs.loc[runs["arm"].eq(arm), column].to_numpy(dtype=float) * scale
        if len(values) != 3:
            raise ValueError(f"{arm}/{column} requires exactly three formal runs")
        mean = float(values.mean())
        sd = float(values.std(ddof=1))
        ax.errorbar(
            mean,
            yi,
            xerr=sd,
            fmt=marker,
            color=color,
            ecolor=color,
            markeredgecolor="white",
            markeredgewidth=0.7,
            markersize=7,
            linewidth=1.6,
            capsize=3,
            zorder=3,
        )
        if xlim is not None:
            span = xlim[1] - xlim[0]
            text_x = min(mean + max(sd, span * 0.018), xlim[1] - span * 0.02)
            ha = "right" if text_x >= xlim[1] - span * 0.04 else "left"
            ax.text(
                text_x,
                yi + 0.18,
                f"{mean:.{decimals}f}",
                color=color,
                fontsize=8.2,
                ha=ha,
                va="bottom",
            )
    ax.set_yticks(y)
    ax.set_yticklabels(TEXT_ARM_LABELS)
    ax.set_xlabel(xlabel)
    if xlim is not None:
        ax.set_xlim(*xlim)
    ax.set_ylim(-0.65, len(TEXT_ARM_ORDER) - 0.35)
    ax.set_title(title, loc="left")
    soft_grid(ax, axis="x")


def figure_native_single_job_state_fingerprint() -> None:
    """Show how native graphs create distinct externally observable service states."""

    runs = pd.read_csv(
        ROOT
        / "experiments/results/opening_text_native_single_job_formal_20260808/"
        "formal_runs.csv"
    )
    if set(runs["arm"]) != set(TEXT_ARM_ORDER) or len(runs) != 12:
        raise ValueError("native single-job figure requires 4 arms × 3 formal runs")

    fig, axes = plt.subplots(2, 3, figsize=(13.2, 7.6), constrained_layout=True)
    definitions = [
        ("wall_s", "完成同一任务的时间", "Job JCT（秒，越低越好）", 1.0, 0, (0, 530)),
        ("tokens_per_s", "服务完成速率", "吞吐（千 token/s）", 1 / 1000, 1, (0, 20)),
        ("running_mean", "运行中请求", "running（请求数）", 1.0, 0, (0, 360)),
        ("waiting_mean", "等待中请求", "waiting（请求数）", 1.0, 0, (0, 880)),
        ("kv_mean", "KV 占用", "KV fraction（0–1）", 1.0, 2, (0, 1.05)),
        ("mfu", "模型计算利用率", "MFU（0–1）", 1.0, 2, (0, 0.72)),
    ]
    for index, (ax, definition) in enumerate(zip(axes.flat, definitions, strict=True)):
        column, title, xlabel, scale, decimals, xlim = definition
        _arm_point_panel(
            ax,
            runs,
            column,
            title=title,
            xlabel=xlabel,
            scale=scale,
            decimals=decimals,
            xlim=xlim,
        )
        if index % 3 != 0:
            ax.set_yticklabels([])
        ax.text(
            -0.12,
            1.05,
            chr(ord("a") + index),
            transform=ax.transAxes,
            fontsize=12,
            fontweight="bold",
            ha="left",
            va="bottom",
        )

    axes[1, 2].text(
        0.98,
        0.08,
        "GPU util 均为 86%–97%\n但 MFU 为 11%–65%",
        transform=axes[1, 2].transAxes,
        ha="right",
        va="bottom",
        color=RED,
        fontsize=8.3,
    )
    fig.suptitle(
        "同一 ShareGPT 任务被不同原生执行图送入不同服务压力状态",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.015,
        "点与误差线为 3 次 formal 的均值 ± SD；用于状态指纹与当前路径诊断，不外推为框架通用排名。",
        ha="center",
        va="top",
        fontsize=8.5,
        color=GREY,
    )
    finish(fig, "opening_native_single_job_state_fingerprint")


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
        ("project_frozen_static", "Project static", BLUE),
    ]
    y = np.arange(len(product_arms))[::-1]
    means = [float(squad.loc[arm, "correct_rows_per_s_mean"]) for arm, _, _ in product_arms]
    errors = [float(squad.loc[arm, "correct_rows_per_s_sd"]) for arm, _, _ in product_arms]
    colors = [color for _, _, color in product_arms]
    ax.barh(y, means, xerr=errors, capsize=3, color=colors, height=0.56)
    ax.set_yticks(y)
    ax.set_yticklabels([label for _, label, _ in product_arms])
    ax.set_xlim(0, 150)
    ax.set_xlabel("SQuAD correct rows/s")
    for yi, value in zip(y, means, strict=True):
        ax.text(value + 1.2, yi, f"{value:.1f}", va="center", fontsize=8.7)
    ax.set_ylim(-1.0, 2.5)
    ax.text(
        3,
        -0.78,
        "统一 PostgreSQL source/sink；3 次 formal\nShareGPT 中 DuckDB 有 4,921/6,144 cap 语义失败",
        ha="left",
        va="bottom",
        fontsize=8.1,
        color=RED,
    )
    ax.set_title("产品 / database-E2E 轨：仅 SQuAD 可排名", loc="left")
    soft_grid(ax, axis="x")

    ax = axes[1]
    chat_arms = [
        ("bounded_http", "Bounded HTTP", DARK),
        ("daft_native", "Daft Native", BLUE),
        ("daft_ray", "Daft Ray", TEAL),
        ("ray_data_http", "Ray Data", ORANGE),
    ]
    y = np.arange(len(chat_arms))[::-1]
    means = [float(native.loc[arm, "tokens_per_s_mean"]) / 1000 for arm, _, _ in chat_arms]
    errors = [float(native.loc[arm, "tokens_per_s_sd"]) / 1000 for arm, _, _ in chat_arms]
    colors = [color for _, _, color in chat_arms]
    ax.barh(y, means, xerr=errors, capsize=3, color=colors, height=0.54)
    ax.set_yticks(y)
    ax.set_yticklabels([label for _, label, _ in chat_arms])
    ax.set_xlim(0, 20)
    ax.set_xlabel("ShareGPT 服务吞吐（千 token/s）")
    for yi, value in zip(y, means, strict=True):
        ax.text(value + 0.25, yi, f"{value:.1f}", va="center", fontsize=8.7)
    ax.set_ylim(-1.0, 3.5)
    ax.text(
        19.5,
        -0.78,
        "同 Chat manifest；3 次 formal\nDaft/Ray Data 保留 vendor scheduler ownership",
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
        "两 panel 使用相同双卡模型服务，但 workload、source/sink 与输出语义合同不同；只在 panel 内比较。误差线为 SD。",
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

    fig = plt.figure(figsize=(13.2, 6.8), constrained_layout=True)
    gs = fig.add_gridspec(2, 2, width_ratios=[1.45, 1.0], height_ratios=[0.78, 1.22])
    ax_jct = fig.add_subplot(gs[:, 0])
    ax_eff = fig.add_subplot(gs[0, 1])
    ax_fair = fig.add_subplot(gs[1, 1])

    jobs = ["short", "long1", "long2", "long3"]
    job_labels = ["Short", "Long 1", "Long 2", "Long 3"]
    y_base = np.arange(len(jobs))[::-1]
    scenario_defs = [
        ("full single", GREY, "o", -0.24),
        ("quarter single", ORANGE, "s", -0.08),
        ("static four-job", RED, "X", 0.08),
        ("shared four-job", BLUE, "D", 0.24),
    ]
    for label, color, marker, offset in scenario_defs:
        for job, yi in zip(jobs, y_base, strict=True):
            if label == "full single":
                scenario = f"single_{job}_full_pool"
            elif label == "quarter single":
                scenario = f"single_{job}_quarter_pool"
            elif label == "static four-job":
                scenario = "staggered_fourjob_static_partition"
            else:
                scenario = "staggered_fourjob_shared_work"
            values = project.loc[
                project["job"].eq(job) & project["scenario"].eq(scenario),
                "job_jct_s",
            ].to_numpy(dtype=float)
            if len(values) != 3:
                raise ValueError(f"{job}/{scenario} requires exactly three formal runs")
            ax_jct.errorbar(
                values.mean(),
                yi + offset,
                xerr=values.std(ddof=1),
                fmt=marker,
                color=color,
                ecolor=color,
                markersize=6.5,
                markeredgecolor="white",
                markeredgewidth=0.6,
                capsize=2.5,
                linewidth=1.4,
                label=label if job == "short" else None,
                zorder=3,
            )
    ax_jct.set_yticks(y_base)
    ax_jct.set_yticklabels(job_labels)
    ax_jct.set_xlim(0, 150)
    ax_jct.set_ylim(-0.65, 3.65)
    ax_jct.set_xlabel("Job JCT（秒，越低越好）")
    ax_jct.set_title("配额损失、真实竞争与共享调度可被分离", loc="left")
    ax_jct.legend(loc="lower right", ncol=2)
    soft_grid(ax_jct, axis="x")
    ax_jct.text(
        0.98,
        0.95,
        "Short: full→quarter +180%\nquarter→static +60%\nstatic→shared −72%",
        transform=ax_jct.transAxes,
        ha="right",
        va="top",
        color=DARK,
        fontsize=8.5,
    )

    policy_defs = [
        ("static_partition", "Static", RED, "X"),
        ("shared_work", "Shared", BLUE, "D"),
    ]
    y_eff = [1, 0]
    for yi, (policy, label, color, marker) in zip(y_eff, policy_defs, strict=True):
        values = (
            group_runs.loc[group_runs["policy"].eq(policy), "group_tokens_per_s"]
            .to_numpy(dtype=float)
            / 1000
        )
        row = group_runs.loc[group_runs["policy"].eq(policy)]
        ax_eff.errorbar(
            values.mean(),
            yi,
            xerr=values.std(ddof=1),
            fmt=marker,
            color=color,
            ecolor=color,
            markersize=7,
            markeredgecolor="white",
            markeredgewidth=0.6,
            capsize=3,
            linewidth=1.5,
        )
        ax_eff.text(
            values.mean() + 0.12,
            yi + 0.20,
            f"{values.mean():.2f}",
            color=color,
            fontsize=8.3,
        )
        ax_eff.text(
            0.98,
            yi - 0.20,
            f"JCT {row['group_jct_s'].mean():.1f}s · MFU {row['mfu_fraction'].mean():.1%}",
            transform=ax_eff.get_yaxis_transform(),
            ha="right",
            va="center",
            fontsize=8.0,
            color=DARK,
        )
    ax_eff.set_yticks(y_eff)
    ax_eff.set_yticklabels(["Static", "Shared"])
    ax_eff.set_xlim(10.8, 13.8)
    ax_eff.set_ylim(-0.55, 1.55)
    ax_eff.set_xlabel("组吞吐（千 token/s）")
    ax_eff.set_title("共享 credit 提高总效率", loc="left")
    soft_grid(ax_eff, axis="x")
    ax_eff.text(
        0.02,
        0.08,
        "吞吐 +8.68% · group JCT −7.97% · MFU +8.56pp",
        transform=ax_eff.transAxes,
        color=BLUE,
        fontweight="bold",
        fontsize=8.2,
    )

    progress = {}
    for _, row in fairness.iterrows():
        progress[str(row["comparison"])] = json.loads(row["normalized_progress_by_job"])
    static_progress = progress["matched_competition_static"]
    shared_progress = progress["shared_fourjob"]
    y = np.arange(len(jobs))[::-1]
    ax_fair.hlines(
        y,
        [static_progress[job] for job in jobs],
        [shared_progress[job] for job in jobs],
        color=LIGHT_GRID,
        linewidth=2.2,
        zorder=1,
    )
    ax_fair.scatter(
        [static_progress[job] for job in jobs],
        y,
        color=RED,
        marker="X",
        s=55,
        label="Static / matched quarter control",
        zorder=3,
    )
    ax_fair.scatter(
        [shared_progress[job] for job in jobs],
        y,
        color=BLUE,
        marker="D",
        s=45,
        label="Shared / full-pool control",
        zorder=3,
    )
    ax_fair.set_yticks(y)
    ax_fair.set_yticklabels(job_labels)
    ax_fair.set_xlim(0.25, 0.86)
    ax_fair.set_ylim(-0.65, 3.65)
    ax_fair.set_xlabel("isolated-normalized progress（越高表示保留进度越多）")
    ax_fair.set_title("效率提升并不等于公平性完成", loc="left")
    ax_fair.legend(loc="lower left", fontsize=7.8)
    soft_grid(ax_fair, axis="x")
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
    ax_fair.text(
        0.02,
        0.95,
        f"Jain {static_jain:.3f} → {shared_jain:.3f}\n"
        f"Long JCT spread {static_spread:.1f}s → {shared_spread:.1f}s",
        transform=ax_fair.transAxes,
        ha="left",
        va="top",
        color=RED,
        fontsize=8.2,
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
        "四 Job 暴露静态配额、共享服务竞争与效率—公平权衡",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.015,
        "Short@0s，3×Long@5s；点与误差线为 3 次 formal 的均值 ± SD。仅覆盖一个 offset 与 equal-weight workload。",
        ha="center",
        va="top",
        fontsize=8.5,
        color=GREY,
    )
    finish(fig, "opening_multijob_interference_tradeoff")


def figure_native_fourjob_normalized_impact() -> None:
    """Plot within-system four-job slowdown without cross-framework JCT ranking."""

    runs = pd.read_csv(
        ROOT
        / "experiments/results/opening_fourjob_interference_20260809/data/combined/"
        "job_formal_runs.csv"
    )
    systems = ["daft_native", "daft_ray", "ray_data_http"]
    system_labels = ["Daft Native", "Daft Ray", "Ray Data"]
    system_colors = [BLUE, TEAL, ORANGE]
    jobs = ["short", "long1", "long2", "long3"]
    job_labels = ["Short", "Long 1", "Long 2", "Long 3"]
    y = np.arange(len(jobs))[::-1]

    fig, axes = plt.subplots(1, 3, figsize=(13.2, 4.8), sharex=True, sharey=True, constrained_layout=True)
    for index, (ax, system, label, color) in enumerate(
        zip(axes, systems, system_labels, system_colors, strict=True)
    ):
        one_system = runs.loc[runs["system"].eq(system)]
        for yi, job in zip(y, jobs, strict=True):
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
            ax.scatter(
                normalized,
                np.full(len(normalized), yi),
                s=55,
                marker="|",
                color=color,
                linewidth=1.2,
                alpha=0.50,
                zorder=3,
            )
            ax.errorbar(
                normalized.mean(),
                yi,
                xerr=normalized.std(ddof=1),
                fmt="D",
                color=color,
                ecolor=color,
                markeredgecolor="white",
                markeredgewidth=0.6,
                markersize=6.5,
                linewidth=1.5,
                capsize=3,
                zorder=4,
            )
            ax.text(
                min(normalized.mean() + max(normalized.std(ddof=1), 0.05), 3.26),
                yi + 0.18,
                f"{normalized.mean():.2f}×",
                color=color,
                fontsize=8.3,
                ha="right" if normalized.mean() > 3.08 else "left",
                va="bottom",
            )
        ax.axvline(1.0, color=DARK, linestyle="--", linewidth=1.0)
        ax.set_xlim(0.9, 3.35)
        ax.set_ylim(-0.62, 3.62)
        ax.set_xlabel("four-job JCT / isolated-single JCT")
        ax.set_title(label, loc="left")
        soft_grid(ax, axis="x")
        ax.text(
            -0.12,
            1.05,
            chr(ord("a") + index),
            transform=ax.transAxes,
            fontsize=12,
            fontweight="bold",
            ha="left",
            va="bottom",
        )
    axes[0].set_yticks(y)
    axes[0].set_yticklabels(job_labels)
    axes[0].text(
        1.02,
        -0.46,
        "无退化",
        color=DARK,
        fontsize=8.0,
        ha="left",
    )
    fig.suptitle(
        "原生执行图中的 Short 与全部 Long Job 均受到共享服务竞争影响",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "细刻度=3 次 four-job formal 相对各自 3-run isolated-single 均值；菱形与误差线=均值 ± SD。只比较系统内退化，不作跨框架绝对性能排名。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_native_fourjob_normalized_impact")


def figure_image_stage_evidence() -> None:
    profile = pd.read_csv(
        ROOT
        / "motivation/results/gpu/image_clip_preprocess_variants_20260801/"
        "raw_repeats.csv"
    )
    fast = profile.loc[
        profile["variant"].eq("torchvision_tensor_pt")
        & profile["batch_size"].isin([16, 64, 256])
    ]
    ratio = (
        fast.groupby("batch_size")["cpu_preprocess_s"].median()
        / fast.groupby("batch_size")["actor_call_wall_s"].median()
    )
    image_root = ROOT / "experiments/results/image_ai_embed_operator_formal_20260803"
    consistency = _formal(image_root / "raw/runs_3arm_12k_consistency_20260804.csv")
    matched = _formal(image_root / "raw/runs_matched_resource_schemav12_20260804.csv")

    fig, axes = plt.subplots(1, 3, figsize=(13.2, 4.9), constrained_layout=True)
    ax = axes[0]
    batches = [16, 64, 256]
    values = [float(ratio.loc[item]) for item in batches]
    bars = ax.bar([str(item) for item in batches], values, color=ORANGE, width=0.58)
    for bar, value in zip(bars, values, strict=True):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value + 0.8,
            f"{value:.1f}×",
            ha="center",
            fontsize=9,
        )
    ax.set(
        xlabel="每批图像数",
        ylabel="CPU 准备时间 / GPU actor 时间",
        ylim=(0, 35),
    )
    ax.set_title("当前图像路径受 CPU 准备阶段限制", loc="left")
    soft_grid(ax)

    ax = axes[1]
    diagnostic_arms = [
        ("daft_builtin_embed", "Daft built-in", ORANGE),
        ("ray_data_staged", "Ray Data", GREY),
        ("project_ray", "Project static", BLUE),
    ]
    labels = [item[1] for item in diagnostic_arms]
    means = []
    errors = []
    colors = []
    for arm, _, color in diagnostic_arms:
        values_12k = consistency.loc[consistency["arm"].eq(arm), "operator_e2e_s"].to_numpy(dtype=float)
        if len(values_12k) != 3:
            raise ValueError(f"12K image diagnostic requires 3 formal runs for {arm}")
        means.append(float(values_12k.mean()))
        errors.append(float(values_12k.std(ddof=1)))
        colors.append(color)
    y = np.arange(len(labels))[::-1]
    ax.barh(y, means, xerr=errors, capsize=3, color=colors, height=0.56)
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel("12K 算子 JCT（秒，越低越好）")
    ax.set_xlim(0, 75)
    for yi, value in zip(y, means, strict=True):
        ax.text(value + 1.2, yi, f"{value:.1f}s", va="center", fontsize=8.6)
    ax.text(
        0.98,
        0.06,
        "结构诊断：fast arms 未达稳态\nDaft 20K 已触发 OutOfDisk",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=8.0,
        color=RED,
    )
    ax.set_title("12K 同语义三臂：只作结构诊断", loc="left")
    soft_grid(ax, axis="x")

    ax = axes[2]
    cpu_levels = [8, 16]
    x = np.arange(len(cpu_levels))
    width = 0.34
    for index, (arm, label, color) in enumerate(
        [
            ("ray_data_staged", "Ray Data 原生图", GREY),
            ("project_ray", "项目冻结静态", BLUE),
        ]
    ):
        values = []
        errors = []
        for cpu in cpu_levels:
            cell = matched.loc[
                matched["arm"].eq(arm) & matched["cpu_workers"].eq(cpu),
                "operator_e2e_s",
            ].to_numpy(dtype=float)
            if len(cell) != 3:
                raise ValueError(f"120K matched image figure requires 3 formal runs for {arm}/cpu{cpu}")
            values.append(float(cell.mean()))
            errors.append(float(cell.std(ddof=1)))
        positions = x + (index - 0.5) * width
        bars = ax.bar(
            positions,
            values,
            width,
            yerr=errors,
            capsize=3,
            color=color,
            label=label,
        )
        for bar, value in zip(bars, values, strict=True):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                value + 4,
                f"{value:.1f}s",
                ha="center",
                fontsize=8.7,
            )
    ax.set_xticks(x)
    ax.set_xticklabels(["8 个 CPU worker", "16 个 CPU worker"])
    ax.set_ylabel("算子 JCT（秒，越低越好）")
    ax.set_ylim(0, 150)
    ax.legend(loc="upper right")
    ax.set_title("120K 同资源正式排名：Ray Data vs Project", loc="left")
    soft_grid(ax)

    fig.suptitle(
        "图像 baseline 必须同时交代阶段瓶颈、原生能力与可排名边界",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        -0.02,
        "相同双卡硬件；左为每 batch 30 次 formal 中位数比，中央/右为 3 次 formal 均值 ± SD。12K 三臂不作稳态排名；120K 仅 Ray Data 与 Project 可比。",
        ha="center",
        va="top",
        fontsize=8.3,
        color=GREY,
    )
    finish(fig, "opening_image_stage_aware_evidence")


def _load_json_replacing_invalid_utf8(path: Path) -> dict:
    return json.loads(path.read_bytes().decode("utf-8", errors="replace"))


def figure_cost_decision_v2() -> None:
    source = (
        ROOT
        / "experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/"
        "ce_context_loo_rerun_20260807.json"
    )
    estimators = _load_json_replacing_invalid_utf8(source)["estimators"]
    names = list(estimators)
    labels = ["均值", "解析模型", "查表", "Ridge", "LightGBM", "混合模型"]
    rows = []
    for name, label in zip(names, labels, strict=True):
        summary = estimators[name]["summary"]
        regret = summary["macro_fold_distributions"]["decision_regret_pct"]
        rows.append((label, regret["median"], regret["mean"], regret["max"]))

    fig, ax = plt.subplots(figsize=(10.6, 5.2), constrained_layout=True)
    y = np.arange(len(rows))[::-1]
    for yi, (label, median, macro, maximum) in zip(y, rows, strict=True):
        color = BLUE if label == "混合模型" else GREY
        ax.hlines(yi, median, maximum, color=color, linewidth=3)
        ax.scatter(median, yi, marker="|", s=180, color=color, linewidth=2.2)
        ax.scatter(macro, yi, marker="D", s=48, color=color, edgecolor=DARK)
        ax.scatter(maximum, yi, marker="o", s=55, color=color, edgecolor=DARK)
        if label == "混合模型":
            ax.text(
                maximum + 1.2,
                yi,
                f"最大 {maximum:.2f}%",
                va="center",
                color=RED,
                fontweight="bold",
            )
    ax.axvline(5, color=TEAL, linestyle="--", linewidth=1.1)
    ax.axvline(15, color=RED, linestyle="--", linewidth=1.1)
    ax.set_yticks(y)
    ax.set_yticklabels([item[0] for item in rows])
    ax.set_xlim(0, 86)
    ax.set_xlabel("候选配置选择 regret（%）")
    ax.set_title(
        "只有混合模型同时通过平均与最坏情况门槛",
        loc="left",
        fontsize=14,
    )
    ax.text(5.5, 4.62, "中位数 / macro 门槛", color=TEAL, fontsize=8.5)
    ax.text(15.5, 4.22, "最大值门槛", color=RED, fontsize=8.5)
    ax.scatter([], [], marker="|", s=180, color=DARK, label="中位数")
    ax.scatter([], [], marker="D", s=48, color=DARK, label="Macro 均值")
    ax.scatter([], [], marker="o", s=55, color=DARK, label="最大值")
    ax.legend(loc="lower right", ncol=3)
    soft_grid(ax, axis="x")
    fig.text(
        0.5,
        -0.02,
        "20-context leave-one-context-out；竖线=中位数，菱形=macro 均值，圆点=最大值。5%/15% 为预注册决策 regret 门槛。",
        ha="center",
        va="top",
        fontsize=8.3,
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
        choices=["A", "B", "C", "D", "E", "F", "H", "N", "T", "work-descriptor", "all"],
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
        selected = {"A", "B", "C", "D", "E", "F", "H", "N", "T", "work-descriptor"}
    renderers = {
        "A": figure_motivation_work_state,
        "B": figure_ai_data_execution_boundary,
        "C": figure_work_organization_v2,
        "D": figure_image_stage_evidence,
        "E": figure_cost_decision_v2,
        "F": figure_native_single_job_state_fingerprint,
        "H": figure_multijob_interference_tradeoff,
        "N": figure_native_fourjob_normalized_impact,
        "T": figure_text_baseline_evidence_map,
        "work-descriptor": figure_work_to_schedule_overview,
    }
    for figure_id in ["A", "T", "N", "B", "C", "H", "D", "E", "F", "work-descriptor"]:
        if figure_id in selected:
            renderers[figure_id]()


if __name__ == "__main__":
    main()
