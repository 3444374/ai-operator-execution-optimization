#!/usr/bin/env python3
"""RC1 数据组织策略实验数据图 v2(按 scipilot-figure-skill 规范重写). 

v2 改进(v1 -> v2):
- 拆双 Y 轴as多 panel 单指标(skill 原则:双 Y 轴禁忌)
- all n=3 柱图叠加 stripplot showseach formal repeat(skill P1 改进)
- figsize 按双栏 7.16 inch(VLDB/SIGMOD/ICDE 规范)
- 字号 7-9pt(skill 原则 4)
- 矢量优先(PDF + SVG + PNG)(skill 原则 2)
- 误差必有交代(skill 原则 5)

输出 3 图到 figures/data/report_main/:
- rc1_bfd_scaling_512_vs_1024.{pdf,svg,png}      2x2 panel tokens/s, p95, MFU, J/1k
- rc1_row_cap_first_slo_collapse.{pdf,svg,png}   2x2 panel tokens/s, SLO, MFU, J/1k
- rc1_prefix_aware_cache_off_no_signal.{pdf,svg,png}  1x2 panel tokens/s, prefix_group_ratio

复现:
    .conda/pg-ai-profile/python.exe figures/scripts/generate_rc1_data_organization_charts_v2.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from _scipilot_helpers import (
    COLOR_BFD, COLOR_K16, COLOR_PREFIX, COLOR_ROWCAP, COLOR_SEQ,
    add_panel_labels, export_figure, load_runs_formal_with_values,
    load_summary_long, notes_caption, overlay_stripplot, setup_style,
)


def _panel_grouped_bars(ax, scenarios, mean, std, raw_values, metric,
                         ylabel, colors, labels, bar_width=0.32):
    """in/at ax on画分组柱图:scenarios x 2 scale(512 / 1024),叠加 stripplot. 

    raw_values: dict {scenario: {metric: [repeat_values]}} — 用于 stripplot
    """
    x = np.arange(len(scenarios))

    # 柱图
    means = []
    stds = []
    for s in scenarios:
        m = mean.loc[s, metric] if s in mean.index else 0
        sd = std.loc[s, metric] if s in std.index else 0
        if isinstance(sd, float) and np.isnan(sd):
            sd = 0
        means.append(m)
        stds.append(sd)

    bars = ax.bar(x, means, bar_width, yerr=stds,
                  color=colors, edgecolor="#111827", linewidth=0.5, capsize=2.5,
                  error_kw=dict(ecolor="#111827", lw=0.6))

    # stripplot 叠加each repeat actual value
    vals_per_x = []
    for s in scenarios:
        if s in raw_values and metric in raw_values[s]:
            vals_per_x.append(raw_values[s][metric])
        else:
            vals_per_x.append([])
    overlay_stripplot(ax, x, vals_per_x, color="#111827", marker="o",
                       size=10, jitter=0.04)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=7)
    ax.set_ylabel(ylabel, fontsize=7.5)
    # 不自动加 title(让 panel label (a) (b) 当标题)


def make_bfd_scaling_figure(raw_512, raw_1024, output_dir):
    """Figure 1:BFD vs sequential 512->1024 反转(2x2 panel,单指标各一). """
    # 用 raw 数据自己算 mean/std(保持vs summary_long 一致)
    def aggregate(raw, scenarios):
        mean_data, std_data, vals = {}, {}, {}
        for s in scenarios:
            if s not in raw:
                continue
            ms = {}
            ss = {}
            vs = {}
            for metric, lst in raw[s].items():
                arr = np.array([v for v in lst if v is not None and not (isinstance(v, float) and np.isnan(v))], dtype=float)
                if len(arr) > 0:
                    ms[metric] = arr.mean()
                    ss[metric] = arr.std(ddof=1) if len(arr) > 1 else 0
                    vs[metric] = arr.tolist()
            mean_data[s] = ms
            std_data[s] = ss
            vals[s] = vs
        return pd.DataFrame(mean_data).T, pd.DataFrame(std_data).T, vals

    scenarios_512 = ["seq_prompt", "seq_fixed", "seq_trace",
                      "bfd_prompt", "bfd_fixed", "bfd_trace"]
    scenarios_1024 = ["seq_fixed", "seq_trace", "bfd_trace"]

    m512, s512, v512 = aggregate(raw_512, scenarios_512)
    m1024, s1024, v1024 = aggregate(raw_1024, scenarios_1024)

    # 颜色:sequential 蓝,BFD 橙
    colors_512 = [COLOR_SEQ] * 3 + [COLOR_BFD] * 3
    colors_1024 = [COLOR_SEQ, COLOR_SEQ, COLOR_BFD]
    labels_512 = ["seq\nprompt", "seq\nfixed", "seq\ntrace",
                  "bfd\nprompt", "bfd\nfixed", "bfd\ntrace"]
    labels_1024 = ["seq\nfixed", "seq\ntrace", "bfd\ntrace"]

    fig, axes = plt.subplots(2, 2, figsize=(10, 6.0))
    fig.suptitle("BFD vs sequential: 512->1024 reversal",
                 fontsize=7.5, fontweight="bold", y=0.98)

    metrics = [
        ("tokens_per_s", "Throughput (tokens/s)", "(a)"),
        ("request_e2e_s_p95", "Request E2E P95 (s)", "(b)"),
        ("mfu_estimate", "MFU", "(c)"),
        ("energy_j_per_1k_observed_tokens", "J / 1k tokens", "(d)"),
    ]

    for ax, (metric, ylabel, _) in zip(axes.flat, metrics):
        # 512 左半,1024 右半(中间留空)
        n_512 = len(scenarios_512)
        n_1024 = len(scenarios_1024)
        x_512 = np.arange(n_512)
        x_1024 = np.arange(n_1024) + n_512 + 1.2

        bar_width = 0.38

        # 512 组
        means_512 = [m512.loc[s, metric] for s in scenarios_512]
        stds_512 = [s512.loc[s, metric] if not np.isnan(s512.loc[s, metric]) else 0
                    for s in scenarios_512]
        ax.bar(x_512, means_512, bar_width, yerr=stds_512,
               color=colors_512, edgecolor="#111827", linewidth=0.5, capsize=2.5,
               error_kw=dict(ecolor="#111827", lw=0.6))
        # stripplot
        vals_512 = [v512[s].get(metric, []) for s in scenarios_512]
        overlay_stripplot(ax, x_512, vals_512, color="#111827", size=8, jitter=0.04)

        # 1024 组(用 hatch 区分)
        means_1024 = [m1024.loc[s, metric] for s in scenarios_1024]
        stds_1024 = [s1024.loc[s, metric] if not np.isnan(s1024.loc[s, metric]) else 0
                     for s in scenarios_1024]
        ax.bar(x_1024, means_1024, bar_width, yerr=stds_1024,
               color=colors_1024, edgecolor="#111827", linewidth=0.5, capsize=2.5,
               hatch="//", error_kw=dict(ecolor="#111827", lw=0.6))
        vals_1024 = [v1024[s].get(metric, []) for s in scenarios_1024]
        overlay_stripplot(ax, x_1024, vals_1024, color="#111827", size=8, jitter=0.04)

        all_x = list(x_512) + list(x_1024)
        all_labels = labels_512 + labels_1024
        ax.set_xticks(all_x)
        ax.set_xticklabels(all_labels, fontsize=7.5)
        ax.set_ylabel(ylabel, fontsize=7.5)

        # MFU panel 改成百分比
        if metric == "mfu_estimate":
            ax.set_yticklabels([f"{y*100:.0f}%" for y in ax.get_yticks()])

        # 分组标注
        y_top = max(means_512 + means_1024) * 1.1
        ax.set_ylim(top=y_top * 1.15)

    add_panel_labels(fig, style="parens")

    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. "

    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc1_bfd_scaling_512_vs_1024_v2", output_dir)
    plt.close(fig)
    return paths


def make_rowcap_slo_figure(raw_512, raw_1024, output_dir):
    """Figure 2:row-cap-first 1024 SLO 崩溃(2x2 panel). """
    def aggregate(raw, scenarios):
        mean_data, std_data, vals = {}, {}, {}
        for s in scenarios:
            if s not in raw:
                continue
            ms, ss, vs = {}, {}, {}
            for metric, lst in raw[s].items():
                arr = np.array([v for v in lst if v is not None and not (isinstance(v, float) and np.isnan(v))], dtype=float)
                if len(arr) > 0:
                    ms[metric] = arr.mean()
                    ss[metric] = arr.std(ddof=1) if len(arr) > 1 else 0
                    vs[metric] = arr.tolist()
            mean_data[s] = ms
            std_data[s] = ss
            vals[s] = vs
        return pd.DataFrame(mean_data).T, pd.DataFrame(std_data).T, vals

    scenarios = ["r64_b6144_seq", "r64_b6144_bfd", "r64_b6144_rowcap"]
    labels = ["sequential", "classic BFD", "row-cap-first"]
    colors = [COLOR_SEQ, COLOR_BFD, COLOR_ROWCAP]

    m512, s512, v512 = aggregate(raw_512, scenarios)
    m1024, s1024, v1024 = aggregate(raw_1024, scenarios)

    fig, axes = plt.subplots(2, 2, figsize=(10, 6.0))
    fig.suptitle("row-cap-first SLO collapse at 1024 scale",
                 fontsize=7.5, fontweight="bold", y=0.98)

    metrics = [
        ("tokens_per_s", "Throughput (tokens/s)"),
        ("request_slo_violation_ratio", "10s SLO violation"),
        ("mfu_estimate", "MFU"),
        ("energy_j_per_1k_observed_tokens", "J / 1k tokens"),
    ]

    bar_width = 0.38
    x = np.arange(len(scenarios))

    for ax, (metric, ylabel) in zip(axes.flat, metrics):
        # 512
        means_512 = [m512.loc[s, metric] for s in scenarios]
        stds_512 = [s512.loc[s, metric] if not np.isnan(s512.loc[s, metric]) else 0
                    for s in scenarios]
        ax.bar(x - bar_width/2, means_512, bar_width, yerr=stds_512,
               color=colors, edgecolor="#111827", linewidth=0.5, capsize=2.5,
               error_kw=dict(ecolor="#111827", lw=0.6))
        vals_512 = [v512[s].get(metric, []) for s in scenarios]
        overlay_stripplot(ax, x - bar_width/2, vals_512, color="#111827",
                           size=8, jitter=0.03)

        # 1024(同色 + hatch)
        means_1024 = [m1024.loc[s, metric] for s in scenarios]
        stds_1024 = [s1024.loc[s, metric] if not np.isnan(s1024.loc[s, metric]) else 0
                     for s in scenarios]
        ax.bar(x + bar_width/2, means_1024, bar_width, yerr=stds_1024,
               color=colors, edgecolor="#111827", linewidth=0.5, capsize=2.5,
               hatch="//", error_kw=dict(ecolor="#111827", lw=0.6))
        vals_1024 = [v1024[s].get(metric, []) for s in scenarios]
        overlay_stripplot(ax, x + bar_width/2, vals_1024, color="#111827",
                           size=8, jitter=0.03)

        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=7)
        ax.set_ylabel(ylabel, fontsize=7.5)

        # SLO panel 改成百分比
        if metric == "request_slo_violation_ratio":
            ax.set_yticklabels([f"{y*100:.0f}%" for y in ax.get_yticks()])
            ax.set_ylim(0, 1.05)
            # in/at 1024 柱顶标百分比
        elif metric == "mfu_estimate":
            ax.set_yticklabels([f"{y*100:.0f}%" for y in ax.get_yticks()])

    add_panel_labels(fig, style="parens")

    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. "

    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc1_row_cap_first_slo_collapse_v2", output_dir)
    plt.close(fig)
    return paths


def make_prefix_aware_figure(raw, output_dir):
    """Figure 3:prefix-aware cache-off 无信号(1x2 panel). """
    ratios = [0, 30, 70, 100]
    seq_scenarios = [f"p{r}_sequential" for r in ratios]
    pref_scenarios = [f"p{r}_prefix_aware" for r in ratios]

    def get_vals(scenarios, metric):
        out = []
        for s in scenarios:
            if s in raw and metric in raw[s]:
                vals = [v for v in raw[s][metric]
                        if v is not None and not (isinstance(v, float) and np.isnan(v))]
                out.append(vals)
            else:
                out.append([])
        return out

    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    fig.suptitle("prefix-aware no benefit when prefix cache is off",
                 fontsize=7.5, fontweight="bold", y=1.02)

    # (a) tokens/s
    ax = axes[0]
    seq_tok = [np.mean(v) if v else 0 for v in get_vals(seq_scenarios, "tokens_per_s")]
    pref_tok = [np.mean(v) if v else 0 for v in get_vals(pref_scenarios, "tokens_per_s")]
    ax.plot(ratios, seq_tok, "o-", color=COLOR_SEQ, linewidth=1.8,
            markersize=6, label="sequential", markeredgecolor="white", markeredgewidth=0.5)
    ax.plot(ratios, pref_tok, "s--", color=COLOR_PREFIX, linewidth=1.8,
            markersize=6, label="prefix-aware", markeredgecolor="white", markeredgewidth=0.5)
    # 叠加each repeat  's 点(prefix screen is n=1,所以点 = marker 本身;but仍用 scatter 强化)
    seq_vals = get_vals(seq_scenarios, "tokens_per_s")
    pref_vals = get_vals(pref_scenarios, "tokens_per_s")
    for r, vals in zip(ratios, seq_vals):
        if vals:
            ax.scatter([r] * len(vals), vals, color=COLOR_SEQ,
                       s=18, alpha=0.4, zorder=4)
    for r, vals in zip(ratios, pref_vals):
        if vals:
            ax.scatter([r] * len(vals), vals, color=COLOR_PREFIX,
                       s=18, alpha=0.4, zorder=4)

    ax.set_xlabel("Controlled prefix reuse (%)", fontsize=7.5)
    ax.set_ylabel("Throughput (tokens/s)", fontsize=7.5)
    ax.set_xticks(ratios)
    ax.legend(loc="lower right", fontsize=7)

    # (b) prefix_group_ratio
    ax = axes[1]
    seq_pgr = [np.mean(v) if v else 0 for v in get_vals(seq_scenarios, "prefix_group_ratio")]
    pref_pgr = [np.mean(v) if v else 0 for v in get_vals(pref_scenarios, "prefix_group_ratio")]
    ax.plot(ratios, [r/100 for r in ratios], ":", color="#9CA3AF",
            linewidth=1.2, label="y = x reference")
    ax.plot(ratios, seq_pgr, "o-", color=COLOR_SEQ, linewidth=1.8,
            markersize=6, label="sequential", markeredgecolor="white", markeredgewidth=0.5)
    ax.plot(ratios, pref_pgr, "s--", color=COLOR_PREFIX, linewidth=1.8,
            markersize=6, label="prefix-aware", markeredgecolor="white", markeredgewidth=0.5)
    ax.set_xlabel("Controlled prefix reuse (%)", fontsize=7.5)
    ax.set_ylabel("Actual prefix group ratio", fontsize=7.5)
    ax.set_xticks(ratios)
    ax.legend(loc="lower right", fontsize=7)

    add_panel_labels(fig, style="parens")

    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. \n"

    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc1_prefix_aware_cache_off_no_signal_v2", output_dir)
    plt.close(fig)
    return paths


def main():
    parser = argparse.ArgumentParser(description="RC1 v2(BFD/row-cap/prefix)")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    setup_style()

    here = Path(__file__).resolve()
    project_root = Path(args.project_root) if args.project_root else here.parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "figures" / "data" / "formal_experiments"
    output_dir.mkdir(parents=True, exist_ok=True)
    results = project_root / "experiments" / "results"

    print(f"[RC1-v2] project_root = {project_root}")

    # Figure 1: BFD scaling
    bfd_512_raw = load_runs_formal_with_values(
        results / "output_aware_bfd_512_v2_20260726" / "runs.csv")
    bfd_1024_raw = load_runs_formal_with_values(
        results / "output_aware_bfd_1024_20260726" / "runs.csv")
    p1 = make_bfd_scaling_figure(bfd_512_raw, bfd_1024_raw, output_dir)
    print(f"[RC1-v2] Figure 1 saved: {[p.name for p in p1]}")

    # Figure 2: row-cap SLO
    rcap_512_raw = load_runs_formal_with_values(
        results / "row_cap_aware_packing_512_20260726" / "nocache_repeats" / "runs.csv")
    rcap_1024_raw = load_runs_formal_with_values(
        results / "row_cap_aware_packing_1024_20260726" / "runs.csv")
    p2 = make_rowcap_slo_figure(rcap_512_raw, rcap_1024_raw, output_dir)
    print(f"[RC1-v2] Figure 2 saved: {[p.name for p in p2]}")

    # Figure 3: prefix-aware
    prefix_raw = load_runs_formal_with_values(
        results / "prefix_aware_batching_20260726" / "screen_v3" / "runs.csv")
    p3 = make_prefix_aware_figure(prefix_raw, output_dir)
    print(f"[RC1-v2] Figure 3 saved: {[p.name for p in p3]}")

    print(f"\n[RC1-v2] alldone. 3 图(PDF/SVG/PNG 各 3 + 灰度预览)位于: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
