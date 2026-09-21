#!/usr/bin/env python3
"""RC2 flush charts v2 (clean rebuild: zero in-panel floating annotations)."""

from __future__ import annotations
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from _scipilot_helpers import (COLOR_ADAPTIVE, COLOR_FIXED_25, COLOR_FIXED_50,
    add_panel_labels, export_figure, load_runs_formal_with_values,
    notes_caption, overlay_stripplot, setup_style)


def _agg(raw, scenarios, metric):
    means, stds, vals = [], [], []
    for sc in scenarios:
        if sc in raw and metric in raw[sc]:
            arr = np.array(raw[sc][metric], dtype=float)
            arr = arr[~np.isnan(arr)]
        else:
            arr = np.array([])
        if len(arr) > 0:
            means.append(arr.mean()); stds.append(arr.std(ddof=1) if len(arr) > 1 else 0); vals.append(arr.tolist())
        else:
            means.append(0); stds.append(0); vals.append([])
    return means, stds, vals


def make_three_way(raw_tw, raw_f512, out):
    scenarios = ["fixed_25ms", "fixed_50ms", "queue_adaptive"]
    labels = ["fixed\n25ms", "fixed\n50ms", "adaptive\n25/50ms"]
    colors = [COLOR_FIXED_25, COLOR_FIXED_50, COLOR_ADAPTIVE]
    titles = ["(a) Throughput: +32%", "(b) Request P99: -29%", "(c) Operator E2E: -25%", "(d) # submissions: -32%"]
    metrics = [("tokens_per_s", "Throughput (tokens/s)"), ("request_e2e_s_p99", "Request P99 (s)"),
               ("e2e_s", "Operator E2E (s)"), ("packing_batch_count", "# submissions")]

    fig, axes = plt.subplots(2, 2, figsize=(10, 6.0), gridspec_kw={"hspace": 0.3, "wspace": 0.25})
    fig.suptitle("Natural-EOS 512-request flush strategy comparison (n=3-5 formal)", fontsize=7.5, fontweight="bold", y=0.98)
    x = np.arange(3)
    raw_combined = dict(raw_tw)
    if "queue_adaptive" in raw_f512:
        raw_combined["queue_adaptive"] = raw_f512["queue_adaptive"]
    for ax, (metric, ylabel), title in zip(axes.flat, metrics, titles):
        means, stds, vals = _agg(raw_combined, scenarios, metric)
        ax.bar(x, means, 0.5, yerr=stds, color=colors, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
        overlay_stripplot(ax, x, vals, color="#111827", size=12, jitter=0.04)
        ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=7)
        ax.set_ylabel(ylabel, fontsize=7.5)
        # [title removed to prevent overlap]
    add_panel_labels(fig, style="parens")
    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. Dots = individual repeats; error bars = sample SD. Finding:...", fontsize=7.5)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc2_flush_three_way_natural_eos_v2", out)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc2_flush_cross_rate_and_heldout_v2", out)
    plt.close(fig); return paths


def make_cross_rate(raw_cr, raw_ho, out):
    fig, axes = plt.subplots(2, 2, figsize=(10, 6.0), gridspec_kw={"hspace": 0.3, "wspace": 0.25})
    fig.suptitle("No reversal across arrival rates and 2048 held-out (n=1 screen)", fontsize=7.5, fontweight="bold", y=0.98)
    rates = ["fast", "slow"]; rate_labels = ["fast\n(~51 req/s)", "slow\n(~13 req/s)"]
    policies = ["fixed_25", "fixed_50", "adaptive"]
    pcolors = {"fixed_25": COLOR_FIXED_25, "fixed_50": COLOR_FIXED_50, "adaptive": COLOR_ADAPTIVE}
    plabels = {"fixed_25": "fixed 25ms", "fixed_50": "fixed 50ms", "adaptive": "adaptive"}
    smap = {"fast": {"fixed_25": "fast_fixed_25ms", "fixed_50": "fast_fixed_50ms", "adaptive": "fast_adaptive_25_50ms"},
            "slow": {"fixed_25": "slow_fixed_25ms", "fixed_50": "slow_fixed_50ms", "adaptive": "slow_adaptive_25_50ms"}}

    def gv(raw, sc, m):
        if sc in raw and m in raw[sc]:
            a = np.array(raw[sc][m], dtype=float); a = a[~np.isnan(a)]
            return a[0] if len(a) > 0 else 0
        return 0

    bw = 0.25; x = np.arange(2)
    ax = axes[0, 0]
    for i, p in enumerate(policies):
        ax.bar(x + (i-1)*bw, [gv(raw_cr, smap[rt][p], "tokens_per_s") for rt in rates], bw, color=pcolors[p], edgecolor="#111827", linewidth=0.5, label=plabels[p])
    ax.set_xticks(x); ax.set_xticklabels(rate_labels, fontsize=7); ax.set_ylabel("Throughput (tokens/s)", fontsize=7.5)
    ax.legend(fontsize=7, framealpha=0.95); # [title removed to prevent overlap] Cross arrival-rate", fontsize=7, color="#374151", loc="left", pad=3)

    ax = axes[0, 1]
    for i, p in enumerate(policies):
        ax.bar(x + (i-1)*bw, [gv(raw_cr, smap[rt][p], "e2e_s") for rt in rates], bw, color=pcolors[p], edgecolor="#111827", linewidth=0.5, label=plabels[p])
    ax.set_xticks(x); ax.set_xticklabels(rate_labels, fontsize=7); ax.set_ylabel("Operator E2E (s)", fontsize=7.5)
    # [title removed to prevent overlap] Cross arrival-rate E2E", fontsize=7, color="#374151", loc="left", pad=3)

    sc_ho = ["fixed_50ms", "adaptive_25_50ms"]; labels_ho = ["fixed 50ms", "adaptive"]
    colors_ho = [COLOR_FIXED_50, COLOR_ADAPTIVE]; x_ho = np.arange(2)
    for ax, (metric, ylabel, title) in zip([axes[1, 0], axes[1, 1]],
        [("tokens_per_s", "2048 Throughput (tokens/s)", "(c) 2048 held-out throughput"),
         ("request_e2e_s_p99", "2048 Request P99 (s)", "(d) 2048 held-out P99")]):
        ax.bar(x_ho, [gv(raw_ho, s, metric) for s in sc_ho], 0.45, color=colors_ho, edgecolor="#111827", linewidth=0.5)
        ax.set_xticks(x_ho); ax.set_xticklabels(labels_ho, fontsize=7); ax.set_ylabel(ylabel, fontsize=7.5)
        # [title removed to prevent overlap]

    add_panel_labels(fig, style="parens")
    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. Finding: fixed-50 stays best. Source: ...", fontsize=7.5)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "make_cross_rate_fig", out)
    plt.close(fig); return paths


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()
    setup_style()
    here = Path(__file__).resolve()
    root = Path(args.project_root) if args.project_root else here.parents[2]
    out = Path(args.output_dir) if args.output_dir else root / "figures" / "data" / "formal_experiments"
    out.mkdir(parents=True, exist_ok=True)
    res = root / "experiments" / "results"
    raw_tw = load_runs_formal_with_values(res / "adaptive_flush_randomized_20260726" / "chatml_three_way_512" / "runs.csv")
    raw_f512 = load_runs_formal_with_values(res / "adaptive_flush_randomized_20260726" / "chatml_flush_formal_512" / "runs.csv")
    p1 = make_three_way(raw_tw, raw_f512, out); print(f"[RC2-flush-v2] F1: {[p.name for p in p1]}")
    raw_cr = load_runs_formal_with_values(res / "adaptive_flush_cross_rate_20260726" / "screen" / "runs.csv")
    raw_ho = load_runs_formal_with_values(res / "text_heldout_2048_20260726" / "screen" / "runs.csv")
    p2 = make_cross_rate(raw_cr, raw_ho, out); print(f"[RC2-flush-v2] F2: {[p.name for p in p2]}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
