#!/usr/bin/env python3
"""RC1 数据组织策略实验数据图生成脚本(批次 1)。

数据源(2026-07-26 真实单 GPU vLLM 0.25.1 + Qwen2.5-1.5B BF16):
- output_aware_bfd_512_v2_20260726/summary_long.csv  (6 scenario × 3 formal)
- output_aware_bfd_1024_20260726/summary_long.csv    (3 scenario × 3 formal)
- row_cap_aware_packing_512_20260726/nocache_repeats/summary_long.csv (3 × 3)
- row_cap_aware_packing_1024_20260726/summary_long.csv (3 × 3)
- prefix_aware_batching_20260726/screen_v3/runs.csv  (8 scenario × 1 formal)

输出三张图到 figures/data/report_main/:
- rc1_bfd_scaling_512_vs_1024.png/svg       BFD 与 sequential 在 512 vs 1024 的对照
- rc1_row_cap_first_slo_collapse.png/svg    row-cap-first 在 1024 规模 SLO 崩溃
- rc1_prefix_aware_cache_off_no_signal.png/svg  cache-off 下 prefix-aware 无收益

每张图附图注说明证据层级与边界。绘图规则遵循 figures/AGENTS.md §5/§11。

复现:
    .conda/pg-ai-profile/python.exe figures/scripts/generate_rc1_data_organization_charts.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# 项目四色体系(figures/AGENTS.md §11.1)
COLOR_SEQ = "#2F6FEB"       # sequential baseline 蓝
COLOR_BFD = "#F97316"       # classic BFD 橙
COLOR_ROWCAP = "#7C3AED"    # row-cap-first 紫
COLOR_PREFIX = "#16A34A"    # prefix-aware 绿
COLOR_NEUTRAL = "#475569"   # 中性灰
GRID_COLOR = "#E2E8F0"
TITLE_KW = dict(fontsize=13, fontweight="bold", color="#0F172A")
SUBTITLE_KW = dict(fontsize=10.5, color="#334155")
NOTE_KW = dict(fontsize=8.5, color="#64748B", style="italic")

# 中文字体回退链(优先 YaHei → SimHei → Arial)
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["axes.edgecolor"] = "#334155"
plt.rcParams["axes.labelcolor"] = "#0F172A"
plt.rcParams["xtick.color"] = "#334155"
plt.rcParams["ytick.color"] = "#334155"
plt.rcParams["axes.grid"] = True
plt.rcParams["grid.color"] = GRID_COLOR
plt.rcParams["grid.linewidth"] = 0.6
plt.rcParams["grid.alpha"] = 0.7
plt.rcParams["axes.axisbelow"] = True


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------


def load_summary_long(csv_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """读取 summary_long.csv,返回 (mean_wide, std_wide),index=scenario_id,columns=metric。"""
    df = pd.read_csv(csv_path)
    mean = df.pivot_table(index="scenario_id", columns="metric",
                          values="mean", aggfunc="first")
    std = df.pivot_table(index="scenario_id", columns="metric",
                         values="sample_std", aggfunc="first")
    return mean, std


def load_runs_formal(csv_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """读取 runs.csv 并按 scenario_id 聚合 formal 行,返回 (mean, std)。"""
    df = pd.read_csv(csv_path)
    if "phase" in df.columns:
        df = df[df.phase == "formal"].copy()
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    grouped = df.groupby("scenario_id")[numeric_cols]
    mean = grouped.mean()
    # 与 summary_long 的 sample_std 一致(ddof=1);单 repeat 时 std=NaN,绘图时填 0
    std = grouped.std(ddof=1)
    return mean, std


# ---------------------------------------------------------------------------
# Figure 1: BFD scaling 512 vs 1024
# ---------------------------------------------------------------------------


def _plot_grouped_axis(ax, scenarios_512, scenarios_1024, metric,
                       mean_512, std_512, mean_1024, std_1024,
                       ylabel, title, colors_map_512, colors_map_1024):
    """在 ax 上画 6 个 scenario × 2 个 scale 的分组柱图。"""
    # 在 512 矩阵中按指定顺序取,1024 只取存在的 3 个
    scenarios_512_present = [s for s in scenarios_512 if s in mean_512.index]
    scenarios_1024_present = [s for s in scenarios_1024 if s in mean_1024.index]
    n_512 = len(scenarios_512_present)
    n_1024 = len(scenarios_1024_present)
    # 两组:左半 512,右半 1024
    x_512 = np.arange(n_512)
    x_1024 = np.arange(n_512) + n_512 + 1  # 中间留一个空位
    # 我们实际上要把 6 个 512 和 3 个 1024 拼到同一坐标轴
    # 用更直观的设计:左半是 512 的 6 个 scenario,右半是 1024 的 3 个 scenario
    bar_width = 0.42

    # 512 组
    means_512 = [mean_512.loc[s, metric] for s in scenarios_512_present]
    stds_512 = [std_512.loc[s, metric] if not np.isnan(std_512.loc[s, metric]) else 0
                for s in scenarios_512_present]
    colors_512 = [colors_map_512[s] for s in scenarios_512_present]
    labels_512 = [s.replace("_", "\n") for s in scenarios_512_present]

    ax.bar(x_512, means_512, bar_width, yerr=stds_512, color=colors_512,
           edgecolor="#0F172A", linewidth=0.6, capsize=3,
           error_kw=dict(ecolor="#0F172A", lw=0.8))

    # 1024 组
    means_1024 = [mean_1024.loc[s, metric] for s in scenarios_1024_present]
    stds_1024 = [std_1024.loc[s, metric] if not np.isnan(std_1024.loc[s, metric]) else 0
                 for s in scenarios_1024_present]
    colors_1024 = [colors_map_1024[s] for s in scenarios_1024_present]
    labels_1024 = [s.replace("_", "\n") for s in scenarios_1024_present]

    ax.bar(x_1024, means_1024, bar_width, yerr=stds_1024, color=colors_1024,
           edgecolor="#0F172A", linewidth=0.6, capsize=3, hatch="//",
           error_kw=dict(ecolor="#0F172A", lw=0.8))

    all_x = list(x_512) + list(x_1024)
    all_labels = labels_512 + labels_1024
    ax.set_xticks(all_x)
    ax.set_xticklabels(all_labels, fontsize=8.5)

    # 分组分隔标注
    ax.text(np.mean(x_512), ax.get_ylim()[1] * 0.96 if ax.get_ylim()[1] > 0 else 0,
            "512 行", ha="center", fontsize=10, fontweight="bold", color="#0F172A")
    ax.text(np.mean(x_1024), ax.get_ylim()[1] * 0.96 if ax.get_ylim()[1] > 0 else 0,
            "1024 行", ha="center", fontsize=10, fontweight="bold", color="#0F172A")
    ax.set_ylabel(ylabel, fontsize=10)
    ax.set_title(title, fontsize=11, color="#0F172A", loc="left", pad=4)


def make_bfd_scaling_figure(mean_512, std_512, mean_1024, std_1024, output_dir):
    """Figure 1:BFD vs sequential 在 512 vs 1024 的对照。"""
    scenarios_512_order = ["seq_prompt", "seq_fixed", "seq_trace",
                            "bfd_prompt", "bfd_fixed", "bfd_trace"]
    scenarios_1024_order = ["seq_fixed", "seq_trace", "bfd_trace"]
    # 颜色:sequential 蓝、BFD 橙
    colors_map_512 = {
        "seq_prompt": COLOR_SEQ, "seq_fixed": COLOR_SEQ, "seq_trace": COLOR_SEQ,
        "bfd_prompt": COLOR_BFD, "bfd_fixed": COLOR_BFD, "bfd_trace": COLOR_BFD,
    }
    colors_map_1024 = {s: COLOR_BFD if s.startswith("bfd") else COLOR_SEQ
                       for s in scenarios_1024_order}

    fig, axes = plt.subplots(2, 2, figsize=(13.5, 9.0))
    fig.suptitle("RC1 · BFD 与 sequential 在 512→1024 规模的反转",
                 fontsize=14, fontweight="bold", color="#0F172A", y=0.985)

    # (a) tokens_per_s
    _plot_grouped_axis(
        axes[0, 0], scenarios_512_order, scenarios_1024_order, "tokens_per_s",
        mean_512, std_512, mean_1024, std_1024,
        ylabel="吞吐 (tokens/s)",
        title="(a) 吞吐:BFD trace 512 +12%,1024 反转为 -5%",
        colors_map_512=colors_map_512, colors_map_1024=colors_map_1024,
    )
    # 标注关键对照
    seq_trace_512 = mean_512.loc["seq_trace", "tokens_per_s"]
    bfd_trace_512 = mean_512.loc["bfd_trace", "tokens_per_s"]
    seq_trace_1024 = mean_1024.loc["seq_trace", "tokens_per_s"]
    bfd_trace_1024 = mean_1024.loc["bfd_trace", "tokens_per_s"]
    pct_512 = (bfd_trace_512 / seq_trace_512 - 1) * 100
    pct_1024 = (bfd_trace_1024 / seq_trace_1024 - 1) * 100
    axes[0, 0].text(0.02, 0.92,
                    f"bfd_trace vs seq_trace\n  512:  {pct_512:+.1f}%\n  1024: {pct_1024:+.1f}%",
                    transform=axes[0, 0].transAxes, fontsize=9,
                    bbox=dict(boxstyle="round,pad=0.3", fc="#FEF3C7", ec="#F59E0B", lw=0.6))

    # (b) request_e2e_s_p95
    _plot_grouped_axis(
        axes[0, 1], scenarios_512_order, scenarios_1024_order, "request_e2e_s_p95",
        mean_512, std_512, mean_1024, std_1024,
        ylabel="请求 E2E P95 (s)",
        title="(b) 尾延迟:1024 全部突破 10 秒 SLO",
        colors_map_512=colors_map_512, colors_map_1024=colors_map_1024,
    )
    # SLO 线
    axes[0, 1].axhline(10.0, color="#DC2626", linestyle="--", linewidth=1.0, alpha=0.7)
    axes[0, 1].text(0.02, 0.05, "10 秒 SLO", transform=axes[0, 1].transAxes,
                    fontsize=8, color="#DC2626")

    # (c) energy_j_per_1k_observed_tokens
    _plot_grouped_axis(
        axes[1, 0], scenarios_512_order, scenarios_1024_order,
        "energy_j_per_1k_observed_tokens",
        mean_512, std_512, mean_1024, std_1024,
        ylabel="能耗 (J / 千 observed tokens)",
        title="(c) 能耗效率:1024 整体升高,BFD trace 最差",
        colors_map_512=colors_map_512, colors_map_1024=colors_map_1024,
    )

    # (d) mfu_estimate
    _plot_grouped_axis(
        axes[1, 1], scenarios_512_order, scenarios_1024_order, "mfu_estimate",
        mean_512, std_512, mean_1024, std_1024,
        ylabel="MFU (估计)",
        title="(d) MFU:512 BFD trace 最佳,1024 反转",
        colors_map_512=colors_map_512, colors_map_1024=colors_map_1024,
    )
    # 把 MFU y 轴改成百分比
    axes[1, 1].set_yticklabels([f"{y*100:.0f}%" for y in axes[1, 1].get_yticks()])

    # 图注脚
    fig.text(0.5, 0.012,
             "证据:output_aware_bfd_512_v2 + output_aware_bfd_1024(2026-07-26,3 formal repeats 各)。"
             "误差棒:样本标准差(n=3)。\n"
             "边界:RTX 5070 单 GPU + Qwen2.5-1.5B BF16 + vLLM 0.25.1 + prefix cache off + CUDA Graph on;"
             "固定 K_max=8 + immediate flush + fixed_output_cap。Source: figures/scripts/generate_rc1_data_organization_charts.py",
             ha="center", **NOTE_KW)

    # 自定义图例
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLOR_SEQ, edgecolor="#0F172A", label="sequential token-budget"),
        Patch(facecolor=COLOR_BFD, edgecolor="#0F172A", label="best-fit-decreasing (BFD)"),
        Patch(facecolor="white", edgecolor="#0F172A", hatch="//", label="1024 行 held-out"),
    ]
    fig.legend(handles=legend_elements, loc="upper right",
               bbox_to_anchor=(0.99, 0.94), fontsize=9, framealpha=0.95)

    plt.tight_layout(rect=[0, 0.03, 1, 0.96])
    fig.savefig(output_dir / "rc1_bfd_scaling_512_vs_1024.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc1_bfd_scaling_512_vs_1024.svg", bbox_inches="tight")
    plt.close(fig)
    return {"bfd_vs_seq_512_pct": pct_512, "bfd_vs_seq_1024_pct": pct_1024}


# ---------------------------------------------------------------------------
# Figure 2: row-cap-first SLO collapse
# ---------------------------------------------------------------------------


def make_rowcap_slo_figure(mean_512, std_512, mean_1024, std_1024, output_dir):
    """Figure 2:row-cap-first 在 1024 规模 SLO 崩溃。"""
    scenarios = ["r64_b6144_seq", "r64_b6144_bfd", "r64_b6144_rowcap"]
    labels = ["sequential\ntoken-budget", "classic BFD", "row-cap-first"]
    colors = [COLOR_SEQ, COLOR_BFD, COLOR_ROWCAP]

    fig, axes = plt.subplots(1, 2, figsize=(13.0, 5.6))
    fig.suptitle("RC1 · row-cap-first 在 1024 行的 SLO 崩溃(512 行无 SLO 违规)",
                 fontsize=13, fontweight="bold", color="#0F172A", y=1.0)

    bar_width = 0.35
    x = np.arange(len(scenarios))

    # (a) tokens_per_s
    means_512 = [mean_512.loc[s, "tokens_per_s"] for s in scenarios]
    stds_512 = [std_512.loc[s, "tokens_per_s"] if not np.isnan(std_512.loc[s, "tokens_per_s"]) else 0
                for s in scenarios]
    means_1024 = [mean_1024.loc[s, "tokens_per_s"] for s in scenarios]
    stds_1024 = [std_1024.loc[s, "tokens_per_s"] if not np.isnan(std_1024.loc[s, "tokens_per_s"]) else 0
                 for s in scenarios]

    axes[0].bar(x - bar_width/2, means_512, bar_width, yerr=stds_512,
                color=colors, edgecolor="#0F172A", linewidth=0.6, capsize=3,
                label="512 行", error_kw=dict(ecolor="#0F172A", lw=0.8))
    # 给 1024 用相同颜色 + hatch
    axes[0].bar(x + bar_width/2, means_1024, bar_width, yerr=stds_1024,
                color=colors, edgecolor="#0F172A", linewidth=0.6, capsize=3,
                hatch="//", label="1024 行", error_kw=dict(ecolor="#0F172A", lw=0.8))
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(labels, fontsize=10)
    axes[0].set_ylabel("吞吐 (tokens/s)", fontsize=11)
    axes[0].set_title("(a) 吞吐:三种策略在 1024 接近,row-cap-first 仅 +0.82% vs sequential",
                      fontsize=10.5, color="#0F172A", loc="left", pad=4)
    axes[0].legend(loc="lower right", fontsize=9)

    # 标注关键差异
    seq_1024 = mean_1024.loc["r64_b6144_seq", "tokens_per_s"]
    rowcap_1024 = mean_1024.loc["r64_b6144_rowcap", "tokens_per_s"]
    pct = (rowcap_1024 / seq_1024 - 1) * 100
    axes[0].text(0.02, 0.92,
                 f"rowcap vs seq @ 1024\n  tokens/s {pct:+.2f}%",
                 transform=axes[0].transAxes, fontsize=9,
                 bbox=dict(boxstyle="round,pad=0.3", fc="#FEF3C7", ec="#F59E0B", lw=0.6))

    # (b) request_slo_violation_ratio
    slo_512 = [mean_512.loc[s, "request_slo_violation_ratio"] for s in scenarios]
    slo_1024 = [mean_1024.loc[s, "request_slo_violation_ratio"] for s in scenarios]
    # 乘以 100 显示百分比
    slo_512_pct = [v * 100 for v in slo_512]
    slo_1024_pct = [v * 100 for v in slo_1024]

    axes[1].bar(x - bar_width/2, slo_512_pct, bar_width,
                color=colors, edgecolor="#0F172A", linewidth=0.6, label="512 行")
    axes[1].bar(x + bar_width/2, slo_1024_pct, bar_width,
                color=colors, edgecolor="#0F172A", linewidth=0.6,
                hatch="//", label="1024 行")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels, fontsize=10)
    axes[1].set_ylabel("10 秒 SLO 违规率 (%)", fontsize=11)
    axes[1].set_title("(b) SLO 违规:row-cap-first 从 0% 跳到 88.7%,sequential 50.4%",
                      fontsize=10.5, color="#0F172A", loc="left", pad=4)
    axes[1].legend(loc="upper left", fontsize=9)
    axes[1].set_ylim(0, 105)

    # 在 1024 行柱顶标注数字
    for i, v in enumerate(slo_1024_pct):
        axes[1].text(x[i] + bar_width/2, v + 2, f"{v:.1f}%",
                     ha="center", fontsize=9, fontweight="bold", color="#0F172A")
    for i, v in enumerate(slo_512_pct):
        if v > 0:
            axes[1].text(x[i] - bar_width/2, v + 2, f"{v:.1f}%",
                         ha="center", fontsize=9, color="#475569")

    fig.text(0.5, 0.005,
             "证据:row_cap_aware_packing_512/nocache_repeats + row_cap_aware_packing_1024"
             "(2026-07-26,3 formal repeats 各)。边界:同 Figure 1;r64_b6144 = ray_batch_rows=64 + token_budget=6144。\n"
             "结论:row-cap-first 不采用为默认,classic BFD 也不采用;sequential 保持默认。"
             "Source: figures/scripts/generate_rc1_data_organization_charts.py",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.05, 1, 0.96])
    fig.savefig(output_dir / "rc1_row_cap_first_slo_collapse.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc1_row_cap_first_slo_collapse.svg", bbox_inches="tight")
    plt.close(fig)
    return {"rowcap_vs_seq_1024_tokens_pct": pct,
            "seq_slo_1024": slo_1024_pct[0] / 100,
            "rowcap_slo_1024": slo_1024_pct[2] / 100}


# ---------------------------------------------------------------------------
# Figure 3: prefix-aware cache-off
# ---------------------------------------------------------------------------


def make_prefix_aware_figure(mean, std, output_dir):
    """Figure 3:cache-off 下 prefix-aware 无收益。"""
    # 8 scenario:p{0,30,70,100}_{sequential,prefix_aware}
    ratios = [0, 30, 70, 100]
    seq_scenarios = [f"p{r}_sequential" for r in ratios]
    pref_scenarios = [f"p{r}_prefix_aware" for r in ratios]

    tokens_seq = [mean.loc[s, "tokens_per_s"] for s in seq_scenarios]
    tokens_pref = [mean.loc[s, "tokens_per_s"] for s in pref_scenarios]
    pgr_seq = [mean.loc[s, "prefix_group_ratio"] for s in seq_scenarios]
    pgr_pref = [mean.loc[s, "prefix_group_ratio"] for s in pref_scenarios]

    fig, axes = plt.subplots(1, 2, figsize=(12.5, 5.4))
    fig.suptitle("RC1 · prefix-aware grouping 在 prefix cache 关闭时无收益",
                 fontsize=13, fontweight="bold", color="#0F172A", y=1.0)

    # (a) tokens_per_s vs prefix ratio
    axes[0].plot(ratios, tokens_seq, "o-", color=COLOR_SEQ, linewidth=2.2,
                 markersize=9, label="sequential token-budget", markeredgecolor="#0F172A")
    axes[0].plot(ratios, tokens_pref, "s--", color=COLOR_PREFIX, linewidth=2.2,
                 markersize=9, label="prefix-aware token-budget", markeredgecolor="#0F172A")
    axes[0].set_xlabel("受控 prefix 重用率 (%)", fontsize=11)
    axes[0].set_ylabel("吞吐 (tokens/s)", fontsize=11)
    axes[0].set_title("(a) 吞吐随 prefix 重用率提升(prefix-aware 无显著差异)",
                      fontsize=10.5, color="#0F172A", loc="left", pad=4)
    axes[0].set_xticks(ratios)
    axes[0].legend(loc="upper left", fontsize=10)

    # 标注每个点的值
    for r, v in zip(ratios, tokens_seq):
        axes[0].annotate(f"{v:.0f}", (r, v), textcoords="offset points",
                         xytext=(0, -15), ha="center", fontsize=8.5, color=COLOR_SEQ)
    for r, v in zip(ratios, tokens_pref):
        axes[0].annotate(f"{v:.0f}", (r, v), textcoords="offset points",
                         xytext=(0, 8), ha="center", fontsize=8.5, color=COLOR_PREFIX)

    # (b) prefix_group_ratio:验证 prefix grouping 工作,但 cache 是关的所以无收益
    axes[1].plot(ratios, [r/100 for r in ratios], ":", color="#94A3B8",
                 linewidth=1.5, label="y = x 参考")
    axes[1].plot(ratios, pgr_seq, "o-", color=COLOR_SEQ, linewidth=2.2,
                 markersize=9, label="sequential", markeredgecolor="#0F172A")
    axes[1].plot(ratios, pgr_pref, "s--", color=COLOR_PREFIX, linewidth=2.2,
                 markersize=9, label="prefix-aware", markeredgecolor="#0F172A")
    axes[1].set_xlabel("受控 prefix 重用率 (%)", fontsize=11)
    axes[1].set_ylabel("实际 prefix 分组率 (prefix_group_ratio)", fontsize=11)
    axes[1].set_title("(b) prefix 分组机制工作正常(显著高于 sequential),但 cache off 无收益",
                      fontsize=10.5, color="#0F172A", loc="left", pad=4)
    axes[1].set_xticks(ratios)
    axes[1].legend(loc="upper left", fontsize=10)

    # 标注 prefix_aware 提升幅度
    for r, vs, vp in zip(ratios, pgr_seq, pgr_pref):
        delta = (vp / vs - 1) * 100 if vs > 0 else 0
        axes[1].annotate(f"+{delta:.0f}%" if delta > 0 else f"{delta:+.0f}%",
                         (r, vp), textcoords="offset points",
                         xytext=(8, -3), ha="left", fontsize=8.5, color=COLOR_PREFIX,
                         fontweight="bold")

    fig.text(0.5, 0.005,
             "证据:prefix_aware_batching/screen_v3(2026-07-26,8 scenario × 1 formal,因单 repeat 无误差棒)。\n"
             "边界:prefix cache 显式关闭(--no-enable-prefix-caching);修复了唯一 prefix 哈希重排与隐式 length-align 耦合。\n"
             "结论:prefix-aware 机制本身工作(分组率提升),但 cache off 下吞吐与 sequential 不可分辨;须 prefix cache 开启后才值得重新评估。",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.07, 1, 0.96])
    fig.savefig(output_dir / "rc1_prefix_aware_cache_off_no_signal.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc1_prefix_aware_cache_off_no_signal.svg", bbox_inches="tight")
    plt.close(fig)
    return {"tokens_seq": tokens_seq, "tokens_pref": tokens_pref,
            "pgr_seq": pgr_seq, "pgr_pref": pgr_pref}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="RC1 数据组织策略实验数据图(批次 1)")
    parser.add_argument("--project-root", default=None,
                        help="项目根路径。默认按本文件位置自动推断。")
    parser.add_argument("--output-dir", default=None,
                        help="图输出目录。默认 figures/data/report_main/。")
    args = parser.parse_args()

    here = Path(__file__).resolve()
    project_root = Path(args.project_root) if args.project_root else here.parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "figures" / "data" / "report_main"
    output_dir.mkdir(parents=True, exist_ok=True)

    results = project_root / "experiments" / "results"

    print(f"[RC1] project_root = {project_root}")
    print(f"[RC1] output_dir   = {output_dir}")

    # ---------- Figure 1: BFD scaling ----------
    bfd_512_csv = results / "output_aware_bfd_512_v2_20260726" / "summary_long.csv"
    bfd_1024_csv = results / "output_aware_bfd_1024_20260726" / "summary_long.csv"
    if not bfd_512_csv.exists() or not bfd_1024_csv.exists():
        print(f"[RC1][ERROR] 缺少 BFD summary_long.csv: {bfd_512_csv} / {bfd_1024_csv}")
        return 1
    bfd_512_mean, bfd_512_std = load_summary_long(bfd_512_csv)
    bfd_1024_mean, bfd_1024_std = load_summary_long(bfd_1024_csv)
    print(f"[RC1] Figure 1 数据: 512 scenarios={list(bfd_512_mean.index)}, 1024 scenarios={list(bfd_1024_mean.index)}")
    bfd_stats = make_bfd_scaling_figure(bfd_512_mean, bfd_512_std, bfd_1024_mean, bfd_1024_std, output_dir)
    print(f"[RC1] Figure 1 saved: rc1_bfd_scaling_512_vs_1024.png/svg  stats={bfd_stats}")

    # ---------- Figure 2: row-cap-first SLO collapse ----------
    rcap_512_csv = results / "row_cap_aware_packing_512_20260726" / "nocache_repeats" / "summary_long.csv"
    rcap_1024_csv = results / "row_cap_aware_packing_1024_20260726" / "summary_long.csv"
    if not rcap_512_csv.exists() or not rcap_1024_csv.exists():
        print(f"[RC1][ERROR] 缺少 row_cap summary_long.csv: {rcap_512_csv} / {rcap_1024_csv}")
        return 1
    rcap_512_mean, rcap_512_std = load_summary_long(rcap_512_csv)
    rcap_1024_mean, rcap_1024_std = load_summary_long(rcap_1024_csv)
    print(f"[RC1] Figure 2 数据: 512 scenarios={list(rcap_512_mean.index)}, 1024 scenarios={list(rcap_1024_mean.index)}")
    rcap_stats = make_rowcap_slo_figure(rcap_512_mean, rcap_512_std, rcap_1024_mean, rcap_1024_std, output_dir)
    print(f"[RC1] Figure 2 saved: rc1_row_cap_first_slo_collapse.png/svg  stats={rcap_stats}")

    # ---------- Figure 3: prefix-aware cache-off ----------
    prefix_v3_csv = results / "prefix_aware_batching_20260726" / "screen_v3" / "runs.csv"
    if not prefix_v3_csv.exists():
        print(f"[RC1][ERROR] 缺少 prefix screen_v3 runs.csv: {prefix_v3_csv}")
        return 1
    prefix_mean, prefix_std = load_runs_formal(prefix_v3_csv)
    print(f"[RC1] Figure 3 数据: scenarios={list(prefix_mean.index)}")
    prefix_stats = make_prefix_aware_figure(prefix_mean, prefix_std, output_dir)
    print(f"[RC1] Figure 3 saved: rc1_prefix_aware_cache_off_no_signal.png/svg  stats={prefix_stats}")

    print("\n[RC1] 全部完成。三张图位于:", output_dir)
    print("  - rc1_bfd_scaling_512_vs_1024.{png,svg}")
    print("  - rc1_row_cap_first_slo_collapse.{png,svg}")
    print("  - rc1_prefix_aware_cache_off_no_signal.{png,svg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
