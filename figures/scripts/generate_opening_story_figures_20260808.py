#!/usr/bin/env python3
"""Generate the first-principles opening story figures from frozen evidence.

Each output has one job: motivate the design, connect organization to
scheduling, or show one preliminary evidence boundary. The corrected text
database-E2E matrix is intentionally excluded until its replacement formal run
passes feeding and correctness gates.
"""

from __future__ import annotations

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
    PALE_RED,
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
    ax.set_xlabel("实际 active work / 配置上限 W65K")
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
    ax.axvspan(0, 64, color="#F2F4F6", zorder=-2)
    ax.axvspan(64, 82, color=PALE_BLUE, zorder=-2)
    ax.axvspan(82, 136, color=PALE_RED, zorder=-2)
    ax.axvline(64, color=BLUE, linestyle="--", linewidth=1.1)
    ax.text(28, 5.25, "供给不足", ha="center", color=DARK)
    ax.text(73, 8.53, "安全工作区", ha="center", color=BLUE, fontweight="bold")
    ax.text(108, 8.53, "过载尾部", ha="center", color=RED)
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
    ax.set_title("提交控制应维持安全工作区", loc="left")

    fig.suptitle(
        "动机：描述工作量、感知运行状态、约束提交压力",
        fontsize=15,
        fontweight="bold",
    )
    finish(fig, "opening_motivation_work_state")


def _organization_data() -> tuple[pd.DataFrame, list[str], list[str]]:
    paths = {
        "Large KV pool": ROOT
        / "experiments/results/rc1_data_organization/"
        "dataorg_2ep_1.5b_cacheON_20260731/raw/runs.csv",
        "Small KV pool": ROOT
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
        ["Large KV pool", "Small KV pool"],
        ["低压力：不同方法近似中性", "KV 饱和：局部性成为主导因素"],
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
            annotation = f"{value:.1f}"
            if regime == "Small KV pool":
                hit = subset.loc[
                    subset["scenario_id"].eq(method),
                    "vllm_prefix_cache_hit_rate",
                ].median()
                annotation += f"  |  缓存命中 {hit:.2f}"
            ax.text(
                value + 0.7,
                bar.get_y() + bar.get_height() / 2,
                annotation,
                va="center",
                fontsize=8.7,
            )
        soft_grid(ax, axis="x")
    axes[1].axvspan(0, 42, color=PALE_RED, zorder=-2)
    axes[1].text(
        58,
        4.55,
        "重排序方法：缓存命中降至 0.06–0.07",
        ha="right",
        va="center",
        color=RED,
        fontsize=8.8,
    )
    fig.suptitle(
        "数据组织效果取决于是否保住真正稀缺的资源",
        fontsize=15,
        fontweight="bold",
    )
    finish(fig, "opening_work_organization_regime_v2")


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
    summary = pd.read_csv(
        ROOT / "experiments/results/image_ai_embed_operator_formal_20260803/summary.csv"
    )

    fig, axes = plt.subplots(
        1,
        2,
        figsize=(12.6, 5.1),
        constrained_layout=True,
        gridspec_kw={"width_ratios": [0.9, 1.35]},
    )
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
            row = summary.loc[
                summary["arm"].eq(arm) & summary["cpu_workers"].eq(cpu)
            ].iloc[0]
            values.append(float(row["operator_jct_s"]))
            errors.append(float(row["operator_jct_s"]) * float(row["cv_pct"]) / 100)
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
    ax.set_title("资源匹配后仍保留约 13%–15% 初步信号", loc="left")
    soft_grid(ax)

    fig.suptitle(
        "图像证据：需要分阶段工作量；动态收益仍待验证",
        fontsize=15,
        fontweight="bold",
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


def main() -> None:
    apply_style()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    figure_motivation_work_state()
    figure_work_organization_v2()
    figure_image_stage_evidence()
    figure_cost_decision_v2()
    figure_ai_data_execution_boundary()
    figure_work_to_schedule_overview()


if __name__ == "__main__":
    main()
