#!/usr/bin/env python3
"""RC1 × RC2 联合消融实验数据图(批次 4)。

数据源(2026-07-26 真实单 GPU vLLM 0.25.1 + Qwen2.5-1.5B BF16):
- joint_batching_submission_512_20260726/screen/summary_long.csv
    18 单元 token_budget{4096/6144/8192} × K_max{4/8/16} × flush{fixed/adaptive} 筛选
- joint_batching_submission_512_20260726/candidate_repeat/summary_long.csv
    4 候选 × 3 formal repeat:baseline / independent / joint / mechanism

输出两张图到 figures/data/report_main/:
- rc2_joint_screen_heatmap.png/svg       18 单元筛选热力图(tokens/s + SLO guardrail)
- rc2_joint_candidate_repeat.png/svg     4 候选重复对照

复现:
    .conda/pg-ai-profile/python.exe figures/scripts/generate_rc2_joint_ablation_charts.py
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch


COLOR_BASELINE = "#94A3B8"
COLOR_INDEPENDENT = "#2F6FEB"
COLOR_JOINT = "#F97316"
COLOR_MECHANISM = "#16A34A"
COLOR_PASS = "#DCFCE7"
COLOR_FAIL = "#FEE2E2"
GRID_COLOR = "#E2E8F0"
NOTE_KW = dict(fontsize=8.5, color="#64748B", style="italic")

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["axes.edgecolor"] = "#334155"
plt.rcParams["axes.labelcolor"] = "#0F172A"
plt.rcParams["xtick.color"] = "#334155"
plt.rcParams["ytick.color"] = "#334155"


def load_summary_long(csv_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(csv_path)
    mean = df.pivot_table(index="scenario_id", columns="metric",
                          values="mean", aggfunc="first")
    std = df.pivot_table(index="scenario_id", columns="metric",
                         values="sample_std", aggfunc="first")
    return mean, std


def parse_screen_scenario(scenario_id: str) -> tuple[int, int, str]:
    """解析 screen scenario_id 如 'b6144_k8_adaptive' → (budget, K, flush)。"""
    m = re.match(r"b(\d+)_k(\d+)_(fixed|adaptive)", scenario_id)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), m.group(3)


# ---------------------------------------------------------------------------
# Figure 1: 18 单元筛选热力图
# ---------------------------------------------------------------------------


def make_screen_heatmap_figure(screen_mean: pd.DataFrame, output_dir: Path):
    """Figure 1:18 单元 token_budget × K_max × flush 的 tokens/s 热力图 + SLO guardrail。"""
    budgets = [4096, 6144, 8192]
    ks = [4, 8, 16]
    flushes = ["fixed", "adaptive"]

    fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.4))
    fig.suptitle("RC2 · 18 单元 token_budget × K_max × flush 筛选:K=16 全部违反 1% SLO guardrail",
                 fontsize=13, fontweight="bold", color="#0F172A", y=1.0)

    # 自定义 colormap:浅蓝 → 深蓝
    cmap = LinearSegmentedColormap.from_list("blue_seq", ["#DBEAFE", "#3B82F6", "#1E3A8A"])

    for ax, flush in zip(axes, flushes):
        # 构造 tokens/s 矩阵 + SLO violation 矩阵
        tokens_matrix = np.zeros((len(budgets), len(ks)))
        slo_matrix = np.zeros((len(budgets), len(ks)))
        for i, b in enumerate(budgets):
            for j, k in enumerate(ks):
                sc = f"b{b}_k{k}_{flush}"
                if sc in screen_mean.index:
                    tokens_matrix[i, j] = screen_mean.loc[sc, "tokens_per_s"]
                    slo_matrix[i, j] = screen_mean.loc[sc, "request_slo_violation_ratio"]

        # 主热力图
        im = ax.imshow(tokens_matrix, cmap=cmap, aspect="auto",
                       vmin=tokens_matrix.min(), vmax=tokens_matrix.max())

        # 在每格标值 + SLO 颜色框
        for i in range(len(budgets)):
            for j in range(len(ks)):
                tok = tokens_matrix[i, j]
                slo = slo_matrix[i, j]
                # 文字颜色根据背景明暗
                txt_color = "white" if tok > tokens_matrix.mean() else "#0F172A"
                ax.text(j, i - 0.12, f"{tok:.0f}", ha="center", va="center",
                        fontsize=11, fontweight="bold", color=txt_color)
                ax.text(j, i + 0.20, f"SLO {slo*100:.2f}%", ha="center", va="center",
                        fontsize=8.5, color=txt_color)
                # SLO 违规 >1% 的格子加红框
                if slo > 0.01:
                    ax.add_patch(plt.Rectangle((j-0.45, i-0.45), 0.9, 0.9,
                                                fill=False, edgecolor="#DC2626", linewidth=2.5))

        ax.set_xticks(range(len(ks)))
        ax.set_xticklabels([f"K_max={k}" for k in ks], fontsize=10)
        ax.set_yticks(range(len(budgets)))
        ax.set_yticklabels([f"token_budget={b}" for b in budgets], fontsize=10)
        flush_label = "fixed 25ms" if flush == "fixed" else "adaptive 25/50ms"
        ax.set_title(f"{flush_label}", fontsize=11.5, color="#0F172A", loc="center", pad=8)

        # colorbar
        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label("tokens/s", fontsize=9.5)

    # 总图例
    legend_elements = [
        Patch(facecolor="white", edgecolor="#DC2626", linewidth=2,
              label="SLO violation > 1%(被 guardrail 排除)"),
    ]
    fig.legend(handles=legend_elements, loc="lower center",
               bbox_to_anchor=(0.5, -0.01), fontsize=10, ncol=1, frameon=True)

    fig.text(0.5, -0.04,
             "证据:joint_batching_submission_512_20260726/screen/summary_long.csv(每单元 1 repeat)。"
             "所有 K_max=16 配置 SLO violation 在 1.76%–3.13% 之间,被 1% guardrail 排除。\n"
             "结论:进入候选重复的只剩 K_max=4 与 K_max=8 配置;K_max=8 tokens/s 更高 → 后续候选用 K_max=8。"
             "Source: figures/scripts/generate_rc2_joint_ablation_charts.py",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.02, 1, 0.96])
    fig.savefig(output_dir / "rc2_joint_screen_heatmap.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc2_joint_screen_heatmap.svg", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 2: 4 候选重复对照
# ---------------------------------------------------------------------------


def make_candidate_repeat_figure(cand_mean: pd.DataFrame, cand_std: pd.DataFrame,
                                  output_dir: Path):
    """Figure 2:4 候选重复对照。"""
    scenarios = [
        ("baseline_b6144_k8_fixed25", "baseline\nb6144/K8/fixed-25", COLOR_BASELINE),
        ("independent_b6144_k8_adaptive", "independent\nb6144/K8/adaptive", COLOR_INDEPENDENT),
        ("joint_b8192_k8_adaptive", "joint\nb8192/K8/adaptive", COLOR_JOINT),
        ("mechanism_b8192_k8_fixed50", "mechanism\nb8192/K8/fixed-50", COLOR_MECHANISM),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(15.5, 5.6))
    fig.suptitle("RC2 · 4 候选重复:联合 vs 独立拼接不可分辨(-0.26% ± 2.07%)",
                 fontsize=13, fontweight="bold", color="#0F172A", y=1.0)

    bar_width = 0.55
    x = np.arange(len(scenarios))

    metrics = [
        ("tokens_per_s", "吞吐 (tokens/s)", "(a) 吞吐:四种配置在 ±5% 内不可分辨"),
        ("e2e_s", "Operator E2E (s)", "(b) 端到端:baseline 因 fixed-25 多花 ~1s"),
        ("request_e2e_s_p99", "请求 P99 (s)", "(c) 尾延迟:四种配置 P99 差异大但置信区间重叠"),
    ]

    for ax, (metric, ylabel, title) in zip(axes, metrics):
        means = [cand_mean.loc[s, metric] for s, _, _ in scenarios]
        stds = [cand_std.loc[s, metric] if not np.isnan(cand_std.loc[s, metric]) else 0
                for s, _, _ in scenarios]
        colors = [c for _, _, c in scenarios]

        ax.bar(x, means, bar_width, yerr=stds, color=colors,
               edgecolor="#0F172A", linewidth=0.6, capsize=4,
               error_kw=dict(ecolor="#0F172A", lw=0.8))

        # 柱顶标值
        for i, (v, s) in enumerate(zip(means, stds)):
            ax.text(i, v + s + max(means) * 0.02, f"{v:.2f}",
                    ha="center", fontsize=9, color="#0F172A")

        ax.set_xticks(x)
        ax.set_xticklabels([lbl for _, lbl, _ in scenarios], fontsize=9)
        ax.set_ylabel(ylabel, fontsize=11)
        ax.set_title(title, fontsize=10.5, color="#0F172A", loc="left", pad=4)

    # 在 (a) 上标关键百分比
    ax_a = axes[0]
    base = cand_mean.loc["baseline_b6144_k8_fixed25", "tokens_per_s"]
    indep = cand_mean.loc["independent_b6144_k8_adaptive", "tokens_per_s"]
    joint = cand_mean.loc["joint_b8192_k8_adaptive", "tokens_per_s"]
    mech = cand_mean.loc["mechanism_b8192_k8_fixed50", "tokens_per_s"]
    pct_indep_vs_base = (indep / base - 1) * 100
    pct_joint_vs_indep = (joint / indep - 1) * 100
    pct_mech_vs_indep = (mech / indep - 1) * 100
    ax_a.text(0.02, 0.92,
              f"独立拼接 vs baseline:\n  tokens/s {pct_indep_vs_base:+.2f}%\n"
              f"联合 vs 独立拼接:\n  tokens/s {pct_joint_vs_indep:+.2f}%\n"
              f"mechanism vs 独立拼接:\n  tokens/s {pct_mech_vs_indep:+.2f}%",
              transform=ax_a.transAxes, fontsize=9,
              bbox=dict(boxstyle="round,pad=0.3", fc="#FEF3C7", ec="#F59E0B", lw=0.6))

    fig.text(0.5, -0.02,
             "证据:joint_batching_submission_512_20260726/candidate_repeat/summary_long.csv(每候选 3 formal repeat)。\n"
             "结论:联合候选相对独立拼接 -0.26%(不可分辨),分层独立优化足够;"
             "mechanism(b8192/K8/fixed-50)是当前 workload 的最佳简单配置。"
             "Source: figures/scripts/generate_rc2_joint_ablation_charts.py",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.03, 1, 0.96])
    fig.savefig(output_dir / "rc2_joint_candidate_repeat.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc2_joint_candidate_repeat.svg", bbox_inches="tight")
    plt.close(fig)
    return {"pct_indep_vs_base": pct_indep_vs_base,
            "pct_joint_vs_indep": pct_joint_vs_indep,
            "pct_mech_vs_indep": pct_mech_vs_indep}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="RC1 × RC2 联合消融图(批次 4)")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    here = Path(__file__).resolve()
    project_root = Path(args.project_root) if args.project_root else here.parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "figures" / "data" / "report_main"
    output_dir.mkdir(parents=True, exist_ok=True)
    results = project_root / "experiments" / "results"

    print(f"[RC2-joint] project_root = {project_root}")
    print(f"[RC2-joint] output_dir   = {output_dir}")

    screen_csv = results / "joint_batching_submission_512_20260726" / "screen" / "summary_long.csv"
    cand_csv = results / "joint_batching_submission_512_20260726" / "candidate_repeat" / "summary_long.csv"
    if not screen_csv.exists() or not cand_csv.exists():
        print(f"[RC2-joint][ERROR] 缺少 screen 或 candidate_repeat summary_long.csv")
        return 1

    screen_mean, _ = load_summary_long(screen_csv)
    cand_mean, cand_std = load_summary_long(cand_csv)
    print(f"[RC2-joint] screen scenarios={len(screen_mean)}, candidate scenarios={list(cand_mean.index)}")

    make_screen_heatmap_figure(screen_mean, output_dir)
    print(f"[RC2-joint] Figure 1 saved: rc2_joint_screen_heatmap.png/svg")

    stats = make_candidate_repeat_figure(cand_mean, cand_std, output_dir)
    print(f"[RC2-joint] Figure 2 saved: rc2_joint_candidate_repeat.png/svg  stats={stats}")

    print("\n[RC2-joint] 全部完成。两张图位于:", output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
