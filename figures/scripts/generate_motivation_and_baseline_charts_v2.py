#!/usr/bin/env python3
"""Motivation + baseline charts v2 (clean rebuild: zero in-panel floating annotations)."""

from __future__ import annotations
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch
from _scipilot_helpers import (add_panel_labels, export_figure, notes_caption, overlay_stripplot, setup_style)

COLOR_FIXED = "#0072B2"
COLOR_TOKEN = "#E69F00"
COLOR_EAGER = "#999999"
COLOR_GRAPH = "#009E73"


def make_token_tail(base, out):
    csv = base / "sharegpt_burstgpt_ray_task_batch128_token_sweep_20260719.csv"
    if not csv.exists(): print(f"[WARN] {csv} missing"); return
    df = pd.read_csv(csv)
    f = df[df.phase=="formal"].copy() if "phase" in df.columns else df.copy()
    g = f.groupby("ray_batch_rows").agg(
        tokens_p95=("batch_tokens_p95","mean"), tokens_max=("batch_tokens_max","mean"),
        tokens_mean=("batch_tokens_mean","mean"), rows_per_s=("rows_per_s","mean"), rows_per_s_sd=("rows_per_s","std"),
    ).reset_index()

    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8), gridspec_kw={"wspace": 0.25})
    fig.suptitle("Token-tail revision: fixed row count is a weak proxy for compute", fontsize=7.5, fontweight="bold", y=0.99)
    x = np.arange(len(g))
    ax = axes[0]
    ax.plot(x, g.tokens_max, "s-", color="#DC2626", linewidth=1.5, markersize=5, label="max")
    ax.plot(x, g.tokens_p95, "o-", color="#E69F00", linewidth=1.5, markersize=5, label="P95")
    ax.plot(x, g.tokens_mean, "^-", color="#0072B2", linewidth=1.5, markersize=5, label="mean")
    ax.fill_between(x, g.tokens_p95, g.tokens_max, alpha=0.12, color="#DC2626")
    ax.set_xticks(x); ax.set_xticklabels([f"b={int(b)}" for b in g.ray_batch_rows], fontsize=7, rotation=30)
    ax.set_xlabel("Fixed batch size (rows)", fontsize=7.5); ax.set_ylabel("Tokens per batch", fontsize=7.5)
    ax.legend(fontsize=7, framealpha=0.95)
    # [title removed to prevent overlap] Token spread grows with batch size", fontsize=7, color="#374151", loc="left", pad=3)

    ax = axes[1]
    ax.bar(x, g.rows_per_s, 0.55, yerr=g.rows_per_s_sd.fillna(0), color=COLOR_FIXED, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
    raw_vals = [f[f.ray_batch_rows==b].rows_per_s.tolist() for b in g.ray_batch_rows]
    overlay_stripplot(ax, x, raw_vals, color="#111827", size=10, jitter=0.05)
    ax.set_xticks(x); ax.set_xticklabels([f"b={int(b)}" for b in g.ray_batch_rows], fontsize=7, rotation=30)
    ax.set_xlabel("Fixed batch size (rows)", fontsize=7.5); ax.set_ylabel("Throughput (rows/s)", fontsize=7.5)
    # [title removed to prevent overlap] Throughput plateaus at b=16-32", fontsize=7, color="#374151", loc="left", pad=3)

    add_panel_labels(fig, style="parens")
    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. Dots=in...", fontsize=7.5)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "motivation_token_tail_revision", out)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "motivation_token_budget_vs_fixed", out)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "baseline_vllm_cuda_graph_vs_eager", out)
    plt.close(fig); return paths


def make_token_budget_vs_fixed(base, out):
    csv = base / "sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv"
    if not csv.exists(): print(f"[WARN] {csv} missing"); return
    df = pd.read_csv(csv); f = df[df.phase=="formal"].copy()
    f["scn"] = f.apply(lambda r: f"fixed_{int(r.ray_batch_rows)}" if r.batching_policy=="fixed_rows" else f"token_{int(r.token_budget)}", axis=1)
    order = ["fixed_16","fixed_32","fixed_64","fixed_128","token_4096","token_6144","token_8192"]
    f = f[f.scn.isin(order)].copy()
    g = f.groupby("scn").agg(tp95=("batch_tokens_p95","mean"), tp95_sd=("batch_tokens_p95","std"),
        rps=("rows_per_s","mean"), rps_sd=("rows_per_s","std")).reindex(order).reset_index()
    x = np.arange(len(g)); is_tok = g.scn.str.startswith("token_")
    colors = [COLOR_TOKEN if t else COLOR_FIXED for t in is_tok]

    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8), gridspec_kw={"wspace": 0.25})
    fig.suptitle("Token-budget vs fixed-row: cap token tail without sacrificing throughput", fontsize=7.5, fontweight="bold", y=0.99)
    ax = axes[0]
    ax.bar(x, g.tp95, 0.55, yerr=g.tp95_sd.fillna(0), color=colors, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
    overlay_stripplot(ax, x, [f[f.scn==s].batch_tokens_p95.tolist() for s in g.scn], color="#111827", size=10, jitter=0.05)
    ax.set_xticks(x); ax.set_xticklabels(g.scn, fontsize=7, rotation=30)
    ax.set_xlabel("Strategy", fontsize=7.5); ax.set_ylabel("Token P95 per batch", fontsize=7.5)
    # [title removed to prevent overlap] Token P95 capped by budget", fontsize=7, color="#374151", loc="left", pad=3)

    ax = axes[1]
    ax.bar(x, g.rps, 0.55, yerr=g.rps_sd.fillna(0), color=colors, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
    overlay_stripplot(ax, x, [f[f.scn==s].rows_per_s.tolist() for s in g.scn], color="#111827", size=10, jitter=0.05)
    ax.set_xticks(x); ax.set_xticklabels(g.scn, fontsize=7, rotation=30)
    ax.set_xlabel("Strategy", fontsize=7.5); ax.set_ylabel("Throughput (rows/s)", fontsize=7.5)
    # [title removed to prevent overlap] token_6144/8192 ~ fixed_32/64", fontsize=7, color="#374151", loc="left", pad=3)

    legend_elements = [Patch(facecolor=COLOR_FIXED, edgecolor="#111827", label="fixed row"),
                       Patch(facecolor=COLOR_TOKEN, edgecolor="#111827", label="token-budget")]
    # fig.legend removed — color coding is sufficient, fontsize=7, ncol=2, frameon=True)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "make_token_budget_vs_fixed_fig", out)
    plt.close(fig); return paths


def make_cuda_graph(csv_path, out):
    if not csv_path.exists(): print(f"[WARN] {csv_path} missing"); return
    df = pd.read_csv(csv_path)
    pivot = df.pivot_table(index="metric", columns="arm", values="mean", aggfunc="first")
    fig, axes = plt.subplots(1, 3, figsize=(10, 2.8), gridspec_kw={"wspace": 0.25})
    fig.suptitle("Deployment baseline: CUDA Graph vs eager (steady-state, not a contribution)", fontsize=7.5, fontweight="bold", y=0.99)
    metrics = [("e2e_s","Operator E2E (s)","(a) E2E: -72%"),("tokens_per_s","Throughput (tokens/s)","(b) tokens/s: +254%"),("mfu_estimate","MFU","(c) MFU: 4%->14.5%")]
    bw = 0.38; x = np.arange(2)
    for ax, (m, ylabel, title) in zip(axes, metrics):
        if m not in pivot.index: continue
        vals = [pivot.loc[m,"eager"], pivot.loc[m,"graph"]]
        ax.bar(x, vals, bw, color=[COLOR_EAGER, COLOR_GRAPH], edgecolor="#111827", linewidth=0.5)
        ax.set_xticks(x); ax.set_xticklabels(["eager","CUDA\nGraph"], fontsize=7.5)
        ax.set_ylabel(ylabel, fontsize=7.5)
        # [title removed to prevent overlap]
        if m == "mfu_estimate": ax.set_yticklabels([f"{y*100:.0f}%" for y in ax.get_yticks()])
    add_panel_labels(fig, style="parens")
    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. Qwen2.5-1.5B BF16, RTX 5070. Used as ...", fontsize=7.5)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "make_cuda_graph_fig", out)
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
    base = root / "experiments" / "results"
    local = base / "local_vllm_qwen15b_baseline"
    p1 = make_token_tail(local, out); print(f"[MOT] F1: done")
    p2 = make_token_budget_vs_fixed(local, out); print(f"[MOT] F2: done")
    p3 = make_cuda_graph(base / "vllm_cuda_graph_512_20260726" / "comparison_summary.csv", out)
    print(f"[MOT] F3: done")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
