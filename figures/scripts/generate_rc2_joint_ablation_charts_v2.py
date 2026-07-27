#!/usr/bin/env python3
"""RC1 x RC2 joint消融图 v2(按 scipilot 规范). 

v2 改进:
- screen_heatmap 保持(已合规:有 colorbar + SLO 红框)
- candidate_repeat 拆 1x3(双 Y 轴隐患)as 1x3 单指标,tokens/s on叠加 stripplot

输出 2 图:
- rc2_joint_screen_heatmap_v2.{pdf,svg,png}
- rc2_joint_candidate_repeat_v2.{pdf,svg,png}

复现:
    .conda/pg-ai-profile/python.exe figures/scripts/generate_rc2_joint_ablation_charts_v2.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch

from _scipilot_helpers import (
    COLOR_BASELINE, COLOR_INDEPENDENT, COLOR_JOINT, COLOR_MECHANISM,
    add_panel_labels, export_figure, load_runs_formal_with_values,
    load_summary_long, notes_caption, overlay_stripplot, setup_style,
)

# 复用 v1 配色名
COLOR_BASELINE = "#999999"
COLOR_INDEPENDENT = "#0072B2"
COLOR_JOINT = "#E69F00"
COLOR_MECHANISM = "#009E73"


def make_screen_heatmap_figure(screen_mean, output_dir):
    """Figure 1:18 单元 token_budget x K_max x flush 热力图(保持 v1,微调字号). """
    budgets = [4096, 6144, 8192]
    ks = [4, 8, 16]
    flushes = ["fixed", "adaptive"]

    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    fig.suptitle("18-cell screen: all K=16 violate 1% SLO guardrail",
                 fontsize=7.5, fontweight="bold", y=1.02)

    cmap = LinearSegmentedColormap.from_list("blue_seq", ["#DBEAFE", "#3B82F6", "#1E3A8A"])

    for ax, flush in zip(axes, flushes):
        tokens_matrix = np.zeros((len(budgets), len(ks)))
        slo_matrix = np.zeros((len(budgets), len(ks)))
        for i, b in enumerate(budgets):
            for j, k in enumerate(ks):
                sc = f"b{b}_k{k}_{flush}"
                if sc in screen_mean.index:
                    tokens_matrix[i, j] = screen_mean.loc[sc, "tokens_per_s"]
                    slo_matrix[i, j] = screen_mean.loc[sc, "request_slo_violation_ratio"]

        im = ax.imshow(tokens_matrix, cmap=cmap, aspect="auto",
                       vmin=tokens_matrix.min(), vmax=tokens_matrix.max())

        for i in range(len(budgets)):
            for j in range(len(ks)):
                tok = tokens_matrix[i, j]
                slo = slo_matrix[i, j]
                txt_color = "white" if tok > tokens_matrix.mean() else "#111827"
                ax.text(j, i - 0.15, f"{tok:.0f}", ha="center", va="center",
                        fontsize=7, fontweight="bold", color=txt_color)
                ax.text(j, i + 0.22, f"SLO {slo*100:.2f}%", ha="center", va="center",
                        fontsize=7, color=txt_color)
                if slo > 0.01:
                    ax.add_patch(plt.Rectangle((j-0.45, i-0.45), 0.9, 0.9,
                                                fill=False, edgecolor="#DC2626", linewidth=2))

        ax.set_xticks(range(len(ks)))
        ax.set_xticklabels([f"K={k}" for k in ks], fontsize=7)
        ax.set_yticks(range(len(budgets)))
        ax.set_yticklabels([f"budget={b}" for b in budgets], fontsize=7)
        flush_label = "fixed 25ms" if flush == "fixed" else "adaptive 25/50ms"
        # [title removed to prevent overlap]

        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        cbar.ax.tick_params(labelsize=6.5)
        cbar.set_label("tokens/s", fontsize=7)

    add_panel_labels(fig, style="parens")

    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. \n"

    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc2_joint_screen_heatmap_v2", output_dir)
    plt.close(fig)
    return paths


def make_candidate_repeat_figure(raw, output_dir):
    """Figure 2:4 候选repeats(1x3 单指标,tokens/s 叠加 stripplot). """
    scenarios = [
        ("baseline_b6144_k8_fixed25", "baseline\nK8/f25", COLOR_BASELINE),
        ("independent_b6144_k8_adaptive", "independent\nK8/adapt", COLOR_INDEPENDENT),
        ("joint_b8192_k8_adaptive", "joint\nK8/adapt", COLOR_JOINT),
        ("mechanism_b8192_k8_fixed50", "mechanism\nK8/f50", COLOR_MECHANISM),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(10, 2.8),
                              gridspec_kw={"wspace": 0.255})
    fig.suptitle("4-candidate repeat: joint vs independent indistinguishable(-0.26%)",
                 fontsize=7.5, fontweight="bold", y=1.04)

    bar_width = 0.55
    x = np.arange(len(scenarios))

    metrics = [
        ("tokens_per_s", "Throughput (tokens/s)"),
        ("e2e_s", "Operator E2E (s)"),
        ("request_e2e_s_p99", "Request P99 (s)"),
    ]

    for ax, (metric, ylabel) in zip(axes, metrics):
        means, stds, vals_per = [], [], []
        for sc, _, _ in scenarios:
            if sc in raw and metric in raw[sc]:
                arr = np.array(raw[sc][metric], dtype=float)
                arr = arr[~np.isnan(arr)]
            else:
                arr = np.array([])
            if len(arr) > 0:
                means.append(arr.mean())
                stds.append(arr.std(ddof=1) if len(arr) > 1 else 0)
                vals_per.append(arr.tolist())
            else:
                means.append(0); stds.append(0); vals_per.append([])

        colors = [c for _, _, c in scenarios]
        ax.bar(x, means, bar_width, yerr=stds, color=colors,
               edgecolor="#111827", linewidth=0.5, capsize=2.5,
               error_kw=dict(ecolor="#111827", lw=0.6))
        # stripplot:each formal repeat  's 点
        overlay_stripplot(ax, x, vals_per, color="#111827",
                           marker="o", size=14, jitter=0.04)

        ax.set_xticks(x)
        ax.set_xticklabels([lbl for _, lbl, _ in scenarios], fontsize=7.5)
        ax.set_ylabel(ylabel, fontsize=7.5)

    # in/at tokens/s panel 标关键百分比
    ax_a = axes[0]
    keys = [sc for sc, _, _ in scenarios]
    base = np.mean([v for v in raw[keys[0]]["tokens_per_s"] if not np.isnan(v)])
    indep = np.mean([v for v in raw[keys[1]]["tokens_per_s"] if not np.isnan(v)])
    joint = np.mean([v for v in raw[keys[2]]["tokens_per_s"] if not np.isnan(v)])
    pct_1 = (indep / base - 1) * 100
    pct_2 = (joint / indep - 1) * 100

    add_panel_labels(fig, style="parens")

    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. \n"

    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc2_joint_candidate_repeat_v2", output_dir)
    plt.close(fig)
    return paths


def main():
    parser = argparse.ArgumentParser(description="RC2 joint ablation v2")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    setup_style()

    here = Path(__file__).resolve()
    project_root = Path(args.project_root) if args.project_root else here.parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "figures" / "data" / "formal_experiments"
    output_dir.mkdir(parents=True, exist_ok=True)
    results = project_root / "experiments" / "results"

    print(f"[RC2-joint-v2] project_root = {project_root}")

    screen_mean, _ = load_summary_long(
        results / "joint_batching_submission_512_20260726" / "screen" / "summary_long.csv")
    p1 = make_screen_heatmap_figure(screen_mean, output_dir)
    print(f"[RC2-joint-v2] Figure 1 saved")

    raw_cand = load_runs_formal_with_values(
        results / "joint_batching_submission_512_20260726" / "candidate_repeat" / "runs.csv")
    p2 = make_candidate_repeat_figure(raw_cand, output_dir)
    print(f"[RC2-joint-v2] Figure 2 saved")

    print(f"\n[RC2-joint-v2] alldone. 2 figures at: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
