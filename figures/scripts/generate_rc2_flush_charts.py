#!/usr/bin/env python3
"""RC2 flush 策略实验数据图生成脚本(批次 2)。

数据源(2026-07-26 真实单 GPU vLLM 0.25.1 + Qwen2.5-1.5B BF16 + 自然 EOS ChatML):
- adaptive_flush_randomized_20260726/chatml_three_way_512/summary_long.csv
    fixed_25ms / fixed_50ms / queue_adaptive,各 3 formal repeats
- adaptive_flush_randomized_20260726/chatml_flush_formal_512/summary_long.csv
    fixed_timeout(=fixed_25) / queue_adaptive,各 5 formal repeats
- adaptive_flush_cross_rate_20260726/screen/runs.csv
    fast(51.4 req/s) / slow(12.85 req/s) × 3 policy,各 1 repeat(screen)
- text_heldout_2048_20260726/screen/runs.csv
    fixed_50ms / adaptive,各 1 repeat(2048 行 held-out)

输出两张图到 figures/data/report_main/:
- rc2_flush_three_way_natural_eos.png/svg     自然 EOS 三组(fixed-25/50/adaptive)对照
- rc2_flush_cross_rate_and_heldout.png/svg    跨 arrival rate + 2048 held-out

复现:
    .conda/pg-ai-profile/python.exe figures/scripts/generate_rc2_flush_charts.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch


# 项目色系 + flush 三档色
COLOR_FIXED_25 = "#94A3B8"   # 短窗口 中性灰(基准对照)
COLOR_FIXED_50 = "#2F6FEB"   # 长窗口 蓝(当前默认)
COLOR_ADAPTIVE = "#F97316"   # adaptive 橙
COLOR_NEUTRAL = "#475569"
GRID_COLOR = "#E2E8F0"
NOTE_KW = dict(fontsize=8.5, color="#64748B", style="italic")

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


def load_summary_long(csv_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(csv_path)
    mean = df.pivot_table(index="scenario_id", columns="metric",
                          values="mean", aggfunc="first")
    std = df.pivot_table(index="scenario_id", columns="metric",
                         values="sample_std", aggfunc="first")
    return mean, std


def load_runs_formal(csv_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(csv_path)
    if "phase" in df.columns:
        df = df[df.phase == "formal"].copy()
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    grouped = df.groupby("scenario_id")[numeric_cols]
    return grouped.mean(), grouped.std(ddof=1)


# ---------------------------------------------------------------------------
# Figure 1: 三组自然 EOS 对照
# ---------------------------------------------------------------------------


def make_three_way_figure(three_way_mean, three_way_std,
                          formal_512_mean, formal_512_std,
                          output_dir):
    """Figure 1:自然 EOS 512 三组(fixed-25/50/adaptive)对照。"""
    # three_way 有 fixed_25ms / fixed_50ms;adaptive 从 formal_512 取(queue_adaptive 5 repeats)
    # 为了保持 n 一致,主图用 three_way 的 fixed_25/fixed_50(n=3),adaptive 用 formal_512 的(n=5)
    scenarios = ["fixed_25ms", "fixed_50ms", "adaptive"]
    colors = [COLOR_FIXED_25, COLOR_FIXED_50, COLOR_ADAPTIVE]
    labels = ["fixed 25ms\n(短窗口)", "fixed 50ms\n(当前默认)", "queue-adaptive\n(25/50ms 两档)"]

    # 拉取每个 scenario 的指标
    def get_metric(scenario, metric, source_mean, source_std):
        if scenario in source_mean.index:
            m = source_mean.loc[scenario, metric]
            s = source_std.loc[scenario, metric]
            return m, (s if not np.isnan(s) else 0)
        return None, None

    metrics_to_plot = [
        ("tokens_per_s", "吞吐 (tokens/s)", "(a) 吞吐:fixed-50 与 adaptive 均比 fixed-25 高 ~32%"),
        ("request_e2e_s_p99", "请求 E2E P99 (s)", "(b) 尾延迟:fixed-25 在持续积压下 P99 达 114s"),
        ("e2e_s", "Operator E2E (s)", "(c) 端到端耗时:fixed-25 比 fixed-50/adaptive 多 ~30s"),
        ("packing_batch_count", "Submission 数", "(d) Coalescing 效果:fixed-25 flush 频繁,submissions 多 45%"),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(12.5, 8.5))
    fig.suptitle("RC2 · 自然 EOS 512 请求下 flush 策略三组随机化重复对照",
                 fontsize=13, fontweight="bold", color="#0F172A", y=0.985)

    bar_width = 0.5
    x = np.arange(len(scenarios))

    for ax, (metric, ylabel, title) in zip(axes.flat, metrics_to_plot):
        means = []
        stds = []
        for sc in scenarios:
            if sc == "adaptive":
                # adaptive 从 formal_512 取(scenario 名 = queue_adaptive)
                m, s = get_metric("queue_adaptive", metric, formal_512_mean, formal_512_std)
            elif sc == "fixed_25ms":
                m, s = get_metric("fixed_25ms", three_way_mean, three_way_std)
            elif sc == "fixed_50ms":
                m, s = get_metric("fixed_50ms", three_way_mean, three_way_std)
            means.append(m)
            stds.append(s)

        bars = ax.bar(x, means, bar_width, yerr=stds, color=colors,
                       edgecolor="#0F172A", linewidth=0.6, capsize=4,
                       error_kw=dict(ecolor="#0F172A", lw=0.8))
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_ylabel(ylabel, fontsize=10)
        ax.set_title(title, fontsize=10.5, color="#0F172A", loc="left", pad=4)

        # 在柱顶标值
        for i, (v, s) in enumerate(zip(means, stds)):
            if v is not None:
                ax.text(i, v + s + (max(means) * 0.015), f"{v:.1f}",
                        ha="center", fontsize=8.5, color="#0F172A")

    # 在 (a) 上标注关键百分比
    ax_a = axes[0, 0]
    t25 = three_way_mean.loc["fixed_25ms", "tokens_per_s"]
    t50 = three_way_mean.loc["fixed_50ms", "tokens_per_s"]
    t_ad = formal_512_mean.loc["queue_adaptive", "tokens_per_s"]
    pct_50_vs_25 = (t50 / t25 - 1) * 100
    pct_ad_vs_25 = (t_ad / t25 - 1) * 100
    pct_ad_vs_50 = (t_ad / t50 - 1) * 100
    ax_a.text(0.02, 0.92,
              f"相对 fixed-25:\n  fixed-50  {pct_50_vs_25:+.1f}%\n  adaptive  {pct_ad_vs_25:+.1f}%\nadaptive vs fixed-50: {pct_ad_vs_50:+.1f}%",
              transform=ax_a.transAxes, fontsize=9,
              bbox=dict(boxstyle="round,pad=0.3", fc="#FEF3C7", ec="#F59E0B", lw=0.6))

    legend_elements = [
        Patch(facecolor=COLOR_FIXED_25, edgecolor="#0F172A", label="fixed 25ms(n=3)"),
        Patch(facecolor=COLOR_FIXED_50, edgecolor="#0F172A", label="fixed 50ms(n=3)"),
        Patch(facecolor=COLOR_ADAPTIVE, edgecolor="#0F172A", label="queue-adaptive(n=5)"),
    ]
    fig.legend(handles=legend_elements, loc="upper right",
               bbox_to_anchor=(0.99, 0.93), fontsize=9, framealpha=0.95)

    fig.text(0.5, 0.012,
             "证据:adaptive_flush_randomized_20260726/chatml_three_way_512(fixed_25/50 各 n=3)"
             " + chatml_flush_formal_512(queue_adaptive n=5)。"
             "自然 EOS ChatML,completion_max_tokens=512,prefix cache off,CUDA Graph on。\n"
             "结论:fixed-50 与 adaptive 在所有指标上不可分辨(±2%);"
             "fixed-25 因过度 flush 导致持续积压。Source: figures/scripts/generate_rc2_flush_charts.py",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.03, 1, 0.96])
    fig.savefig(output_dir / "rc2_flush_three_way_natural_eos.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc2_flush_three_way_natural_eos.svg", bbox_inches="tight")
    plt.close(fig)
    return {"pct_50_vs_25": pct_50_vs_25, "pct_ad_vs_25": pct_ad_vs_25,
            "pct_ad_vs_50": pct_ad_vs_50}


# ---------------------------------------------------------------------------
# Figure 2: 跨 arrival rate + 2048 held-out
# ---------------------------------------------------------------------------


def make_cross_rate_heldout_figure(cross_rate_mean, cross_rate_std,
                                    heldout_mean, heldout_std,
                                    output_dir):
    """Figure 2:跨 arrival rate + 2048 held-out。"""
    fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.8))
    fig.suptitle("RC2 · flush 策略在跨 arrival rate 与 2048 留出下均未反转 adaptive 劣势",
                 fontsize=13, fontweight="bold", color="#0F172A", y=1.0)

    # ---------- (a) 跨 arrival rate ----------
    # fast ~51.4 req/s(arrival_scale=0.00025),slow ~12.85 req/s(arrival_scale=0.001)
    # 中间档 25.7 req/s 未在此 screen 中,只画两档
    rates = ["fast (~51.4 req/s)", "slow (~12.85 req/s)"]
    policies = ["fixed_25", "fixed_50", "adaptive"]
    policy_colors = {
        "fixed_25": COLOR_FIXED_25,
        "fixed_50": COLOR_FIXED_50,
        "adaptive": COLOR_ADAPTIVE,
    }
    policy_labels = {
        "fixed_25": "fixed 25ms",
        "fixed_50": "fixed 50ms",
        "adaptive": "adaptive",
    }
    # scenario_id 命名:fast_fixed_25ms / fast_fixed_50ms / fast_adaptive_25_50ms
    #                   slow_fixed_25ms / slow_fixed_50ms / slow_adaptive_25_50ms
    scenarios_map = {
        "fast": {"fixed_25": "fast_fixed_25ms", "fixed_50": "fast_fixed_50ms",
                 "adaptive": "fast_adaptive_25_50ms"},
        "slow": {"fixed_25": "slow_fixed_25ms", "fixed_50": "slow_fixed_50ms",
                 "adaptive": "slow_adaptive_25_50ms"},
    }

    bar_width = 0.25
    x = np.arange(len(rates))

    for i, policy in enumerate(policies):
        means = [cross_rate_mean.loc[scenarios_map[rt][policy], "tokens_per_s"]
                 for rt in ["fast", "slow"]]
        offsets = (i - 1) * bar_width
        axes[0].bar(x + offsets, means, bar_width,
                    color=policy_colors[policy], edgecolor="#0F172A", linewidth=0.6,
                    label=policy_labels[policy])
        # 在柱顶标值
        for j, v in enumerate(means):
            axes[0].text(x[j] + offsets, v + max(means) * 0.015, f"{v:.0f}",
                         ha="center", fontsize=8.5, color="#0F172A")

    axes[0].set_xticks(x)
    axes[0].set_xticklabels(rates, fontsize=10)
    axes[0].set_ylabel("吞吐 (tokens/s)", fontsize=11)
    axes[0].set_title("(a) 跨 arrival rate 筛选:fixed-50 全程最优,adaptive 未获默认资格",
                      fontsize=10.5, color="#0F172A", loc="left", pad=4)
    axes[0].legend(loc="upper right", fontsize=9)

    # 标注 fixed_50 vs fixed_25 在两档下的百分比
    for j, rt in enumerate(["fast", "slow"]):
        t25 = cross_rate_mean.loc[scenarios_map[rt]["fixed_25"], "tokens_per_s"]
        t50 = cross_rate_mean.loc[scenarios_map[rt]["fixed_50"], "tokens_per_s"]
        tad = cross_rate_mean.loc[scenarios_map[rt]["adaptive"], "tokens_per_s"]
        pct_50 = (t50 / t25 - 1) * 100
        pct_ad = (tad / t50 - 1) * 100
        axes[0].text(j, -max([cross_rate_mean.loc[scenarios_map[rt][p], "tokens_per_s"]
                              for p in policies]) * 0.12,
                     f"fixed-50 vs 25: {pct_50:+.1f}%\nadaptive vs 50: {pct_ad:+.1f}%",
                     ha="center", fontsize=8, color="#475569",
                     bbox=dict(boxstyle="round,pad=0.2", fc="#F1F5F9", ec="#CBD5E1", lw=0.4))

    # ---------- (b) 2048 held-out ----------
    scenarios_ho = ["fixed_50ms", "adaptive_25_50ms"]
    labels_ho = ["fixed 50ms", "adaptive\n(25/50ms)"]
    colors_ho = [COLOR_FIXED_50, COLOR_ADAPTIVE]

    metrics_ho = [
        ("tokens_per_s", "吞吐 (tokens/s)", False),
        ("e2e_s", "Operator E2E (s)", True),  # lower is better
        ("request_e2e_s_p99", "请求 E2E P99 (s)", True),
    ]

    # 用 grouped bar:3 metric × 2 scenario,但每个 metric 量级不同 → 改用 3 个 subplot 在 ax 上方
    # 简化:直接画 2 个 scenario × 3 metric 的归一化对比(每个 metric 各自归一化到 fixed_50=1)
    # 但更直观的是:画 3 个 metric 的实际值,用左右轴
    # 最简洁:画 tokens/s 和 req_p99(最关键两个),用双面板
    # 我用 1x2 中的右边:画 2 scenario 在 tokens/s 上的差异 + 注释其他指标

    # 取 tokens/s 作为主指标
    means_ho = [heldout_mean.loc[s, "tokens_per_s"] for s in scenarios_ho]
    e2e_ho = [heldout_mean.loc[s, "e2e_s"] for s in scenarios_ho]
    p99_ho = [heldout_mean.loc[s, "request_e2e_s_p99"] for s in scenarios_ho]

    bar_width_ho = 0.5
    x_ho = np.arange(len(scenarios_ho))

    # 用双 y 轴:左=tokens/s(高越好),右=e2e + p99(低更好)
    ax2 = axes[1]
    ax2b = ax2.twinx()
    ax2b.grid(False)

    bars1 = ax2.bar(x_ho - bar_width_ho/4, means_ho, bar_width_ho/2,
                    color=colors_ho, edgecolor="#0F172A", linewidth=0.6,
                    label="tokens/s")
    for i, v in enumerate(means_ho):
        ax2.text(x_ho[i] - bar_width_ho/4, v + max(means_ho) * 0.015,
                 f"{v:.0f}", ha="center", fontsize=9, color="#0F172A")

    bars2 = ax2b.bar(x_ho + bar_width_ho/4, p99_ho, bar_width_ho/2,
                     color=[COLOR_FIXED_50, COLOR_ADAPTIVE], edgecolor="#0F172A",
                     linewidth=0.6, alpha=0.5, hatch="\\\\", label="req P99 (s)")
    for i, v in enumerate(p99_ho):
        ax2b.text(x_ho[i] + bar_width_ho/4, v + max(p99_ho) * 0.015,
                  f"{v:.1f}", ha="center", fontsize=9, color="#475569")

    ax2.set_xticks(x_ho)
    ax2.set_xticklabels(labels_ho, fontsize=10)
    ax2.set_ylabel("吞吐 (tokens/s)", fontsize=11, color="#0F172A")
    ax2b.set_ylabel("请求 E2E P99 (s)", fontsize=11, color="#475569")

    pct_tok = (means_ho[1] / means_ho[0] - 1) * 100
    pct_p99 = (p99_ho[1] / p99_ho[0] - 1) * 100
    ax2.set_title(f"(b) 2048 行 held-out:fixed-50 吞吐 {pct_tok:+.2f}%,P99 {pct_p99:+.2f}%(adaptive 略差)",
                  fontsize=10.5, color="#0F172A", loc="left", pad=4)

    # 合并图例
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2b.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=9)

    fig.text(0.5, 0.005,
             "证据:(a) adaptive_flush_cross_rate_20260726/screen(每 scenario 1 repeat,fast=51.4 / slow=12.85 req/s arrival rate)。"
             "(b) text_heldout_2048_20260726/screen(2048 行 1 repeat)。\n"
             "边界:cross-rate 为 screen 性质(单 repeat),用于策略排序而非精确 effect size;"
             "2048 held-out 因持续积压放大尾延迟,吞吐较 512 下降 ~10%。Source: figures/scripts/generate_rc2_flush_charts.py",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.05, 1, 0.96])
    fig.savefig(output_dir / "rc2_flush_cross_rate_and_heldout.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc2_flush_cross_rate_and_heldout.svg", bbox_inches="tight")
    plt.close(fig)
    return {"pct_tok_2048": pct_tok, "pct_p99_2048": pct_p99}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="RC2 flush 策略实验数据图(批次 2)")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    here = Path(__file__).resolve()
    project_root = Path(args.project_root) if args.project_root else here.parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "figures" / "data" / "report_main"
    output_dir.mkdir(parents=True, exist_ok=True)
    results = project_root / "experiments" / "results"

    print(f"[RC2-flush] project_root = {project_root}")
    print(f"[RC2-flush] output_dir   = {output_dir}")

    # Figure 1
    three_way_csv = results / "adaptive_flush_randomized_20260726" / "chatml_three_way_512" / "summary_long.csv"
    formal_512_csv = results / "adaptive_flush_randomized_20260726" / "chatml_flush_formal_512" / "summary_long.csv"
    if not three_way_csv.exists() or not formal_512_csv.exists():
        print(f"[RC2-flush][ERROR] 缺少 three_way 或 formal_512 summary_long.csv")
        return 1
    three_way_mean, three_way_std = load_summary_long(three_way_csv)
    formal_512_mean, formal_512_std = load_summary_long(formal_512_csv)
    print(f"[RC2-flush] Figure 1 数据: three_way scenarios={list(three_way_mean.index)}, formal_512 scenarios={list(formal_512_mean.index)}")
    stats1 = make_three_way_figure(three_way_mean, three_way_std, formal_512_mean, formal_512_std, output_dir)
    print(f"[RC2-flush] Figure 1 saved: rc2_flush_three_way_natural_eos.png/svg  stats={stats1}")

    # Figure 2
    cross_rate_csv = results / "adaptive_flush_cross_rate_20260726" / "screen" / "runs.csv"
    heldout_csv = results / "text_heldout_2048_20260726" / "screen" / "runs.csv"
    if not cross_rate_csv.exists() or not heldout_csv.exists():
        print(f"[RC2-flush][ERROR] 缺少 cross_rate 或 heldout runs.csv")
        return 1
    cross_rate_mean, cross_rate_std = load_runs_formal(cross_rate_csv)
    heldout_mean, heldout_std = load_runs_formal(heldout_csv)
    print(f"[RC2-flush] Figure 2 数据: cross_rate scenarios={list(cross_rate_mean.index)}, heldout scenarios={list(heldout_mean.index)}")
    stats2 = make_cross_rate_heldout_figure(cross_rate_mean, cross_rate_std, heldout_mean, heldout_std, output_dir)
    print(f"[RC2-flush] Figure 2 saved: rc2_flush_cross_rate_and_heldout.png/svg  stats={stats2}")

    print("\n[RC2-flush] 全部完成。两张图位于:", output_dir)
    print("  - rc2_flush_three_way_natural_eos.{png,svg}")
    print("  - rc2_flush_cross_rate_and_heldout.{png,svg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
