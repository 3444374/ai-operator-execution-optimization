#!/usr/bin/env python3
"""算子cost estimation图 v2(按 scipilot 规范). 

v2 改进:
- 5seed_stability 拆双 Y 轴as 1x3 单指标(MAE / R² / MAPE)
- coefficients 水平条形图保持(已合规),仅调字号 + 加冗余编码

输出 2 图:
- rc4_cost_model_5seed_stability_v2.{pdf,svg,png}
- rc4_cost_model_coefficients_v2.{pdf,svg,png}

复现:
    .conda/pg-ai-profile/python.exe figures/scripts/generate_rc4_cost_estimation_charts_v2.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

from _scipilot_helpers import (
    COLOR_BASELINE, COLOR_RIDGE, COLOR_NEG, COLOR_POS, COLOR_COUNTER,
    add_panel_labels, export_figure, notes_caption, setup_style,
)

COLOR_BASELINE = "#999999"
COLOR_RIDGE = "#0072B2"
COLOR_NEG = "#DC2626"
COLOR_POS = "#009E73"
COLOR_COUNTER = "#E69F00"


def load_seed_models(model_dir):
    rows = []
    for seed in [20260726, 20260727, 20260728, 20260729, 20260730]:
        p = model_dir / f"e2e_cost_model_seed_{seed}.json"
        if not p.exists():
            continue
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        rows.append({
            "seed": seed,
            "test_rows": d["test_rows"],
            "ridge_mae": d["ridge_metrics"]["mae"],
            "ridge_mape": d["ridge_metrics"]["mape_pct"],
            "ridge_rmse": d["ridge_metrics"]["rmse"],
            "ridge_r2": d["ridge_metrics"]["r2"],
            "baseline_mae": d["mean_baseline_metrics"]["mae"],
            "baseline_mape": d["mean_baseline_metrics"]["mape_pct"],
            "baseline_rmse": d["mean_baseline_metrics"]["rmse"],
            "baseline_r2": d["mean_baseline_metrics"]["r2"],
            "coefficients": d["model"]["coefficients"],
            "feature_names": d["feature_names"],
        })
    return pd.DataFrame(rows)


def make_5seed_stability_figure(df, output_dir):
    """Figure 1:5-seed 稳定性(1x3 单指标,无双 Y 轴). """
    fig, axes = plt.subplots(1, 3, figsize=(10, 2.8),
                              gridspec_kw={"wspace": 0.25})
    fig.suptitle("Cost model 5-seed grouped held-out stability",
                 fontsize=7.5, fontweight="bold", y=0.99)

    seeds = df.seed.tolist()
    x = np.arange(len(seeds))
    bar_width = 0.38

    # (a) MAE
    ax = axes[0]
    ax.bar(x - bar_width/2, df.ridge_mae, bar_width,
           color=COLOR_RIDGE, edgecolor="#111827", linewidth=0.5, label="ridge")
    ax.bar(x + bar_width/2, df.baseline_mae, bar_width,
           color=COLOR_BASELINE, edgecolor="#111827", linewidth=0.5, label="baseline")
    for i, (r, b) in enumerate(zip(df.ridge_mae, df.baseline_mae)):
        ax.text(i - bar_width/2, r + 0.5, f"{r:.1f}", ha="center", fontsize=7.5)
        ax.text(i + bar_width/2, b + 0.5, f"{b:.1f}", ha="center", fontsize=7, color="#4B5563")
    ridge_mae_mean = df.ridge_mae.mean()
    base_mae_mean = df.baseline_mae.mean()
    ax.axhline(ridge_mae_mean, color=COLOR_RIDGE, linestyle="--", linewidth=0.8, alpha=0.6)
    ax.set_xticks(x); ax.set_xticklabels([str(s)[-4:] for s in seeds], fontsize=7)
    ax.set_xlabel("seed (last 4 digits)", fontsize=7)
    ax.set_ylabel("MAE (s)", fontsize=7.5)
    ax.legend(loc="upper right", fontsize=7, framealpha=0.95)

    # (b) R²
    ax = axes[1]
    ax.bar(x - bar_width/2, df.ridge_r2, bar_width,
           color=COLOR_RIDGE, edgecolor="#111827", linewidth=0.5, label="ridge")
    ax.bar(x + bar_width/2, df.baseline_r2, bar_width,
           color=COLOR_BASELINE, edgecolor="#111827", linewidth=0.5, alpha=0.5,
           label="baseline")
    ax.axhline(0, color="#111827", linewidth=0.6)
    ridge_r2_mean = df.ridge_r2.mean()
    ax.axhline(ridge_r2_mean, color=COLOR_RIDGE, linestyle="--", linewidth=0.8, alpha=0.6)
    for i, r2 in enumerate(df.ridge_r2):
        ax.text(i - bar_width/2, r2 + 0.03 if r2 > 0 else r2 - 0.06,
                f"{r2:.2f}", ha="center", fontsize=7.5)
    ax.set_xticks(x); ax.set_xticklabels([str(s)[-4:] for s in seeds], fontsize=7)
    ax.set_xlabel("seed (last 4 digits)", fontsize=7)
    ax.set_ylabel("R^2 (explained variance)", fontsize=7.5)
    ax.set_ylim(-0.3, 1.05)
    ax.legend(loc="lower right", fontsize=7, framealpha=0.95)

    # (c) MAPE
    ax = axes[2]
    ax.plot(x, df.ridge_mape, "o-", color=COLOR_NEG, linewidth=1.5,
            markersize=6, label="ridge MAPE")
    ax.plot(x, df.baseline_mape, "s--", color="#9333EA", linewidth=1.2,
            markersize=5, alpha=0.7, label="baseline MAPE")
    for i, m in enumerate(df.ridge_mape):
        ax.text(i, m + 5, f"{m:.0f}%", ha="center", fontsize=7, color=COLOR_NEG)
    ridge_mape_mean = df.ridge_mape.mean()
    ax.axhline(ridge_mape_mean, color=COLOR_NEG, linestyle=":", linewidth=0.8, alpha=0.6)
    ax.set_xticks(x); ax.set_xticklabels([str(s)[-4:] for s in seeds], fontsize=7)
    ax.set_xlabel("seed (last 4 digits)", fontsize=7)
    ax.set_ylabel("MAPE (%)", fontsize=7.5)
    ax.legend(loc="upper left", fontsize=7, framealpha=0.95)

    add_panel_labels(fig, style="parens")

    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. \n"

    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc4_cost_model_5seed_stability_v2", output_dir)
    plt.close(fig)
    return paths


def make_coefficients_figure(df, output_dir):
    """Figure 2:main fold featurescoefficients水平条形图(加冗余编码). """
    main_row = df[df.seed == 20260726].iloc[0]
    coefs = main_row.coefficients

    items = sorted(coefs.items(), key=lambda kv: kv[1])
    names = [k for k, _ in items]
    values = [v for _, v in items]

    name_cn = {
        "total_rows": "total_rows (rows)",
        "prompt_token_count": "prompt_token_count [!]",
        "completion_max_tokens": "completion_max_tokens (output cap)",
        "token_budget": "token_budget (token budget)",
        "packing_batch_count": "packing_batch_count (# batches)",
        "batch_estimated_cost_p50": "batch_cost_p50",
        "batch_estimated_cost_p95": "batch_cost_p95",
        "batch_estimated_cost_max": "batch_cost_max",
        "max_inflight_limit": "max_inflight_limit (K_max)",
        "flush_timeout_ms": "flush_timeout_ms",
        "flush_max_wait_ms": "flush_max_wait_ms",
        "arrival_time_scale": "arrival_time_scale [!]",
        "arrival_replay_enabled": "arrival_replay_enabled",
        "flush_is_adaptive": "flush_is_adaptive",
        "flush_is_immediate": "flush_is_immediate",
    }
    labels = [name_cn.get(n, n) for n in names]
    counter_intuitive = {"prompt_token_count", "arrival_time_scale"}

    colors = []
    for n in names:
        if n in counter_intuitive:
            colors.append(COLOR_COUNTER)
        elif coefs[n] >= 0:
            colors.append(COLOR_POS)
        else:
            colors.append(COLOR_NEG)

    fig, ax = plt.subplots(figsize=(10, 6.0))
    fig.suptitle("Cost model feature coefficients(seed=20260726 main fold)",
                 fontsize=7, fontweight="bold", y=0.98)

    y = np.arange(len(names))
    ax.barh(y, values, color=colors, edgecolor="#111827", linewidth=0.4, height=0.7)
    for i, v in enumerate(values):
        offset = 0.015 if v >= 0 else -0.015

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7)
    ax.axvline(0, color="#111827", linewidth=0.6)
    ax.set_xlabel("Standardized Ridge coefficient (on log1p E2E)", fontsize=7.5)
    # [title removed to prevent overlap]",
                 # [residual] fontsize=7, color="#4B5563", loc="left", pad=4)

    legend_elements = [
        Patch(facecolor=COLOR_POS, edgecolor="#111827", label="positive"),
        Patch(facecolor=COLOR_NEG, edgecolor="#111827", label="negative"),
        Patch(facecolor=COLOR_COUNTER, edgecolor="#111827", label="counter-intuitive (small-data Ridge limitation)"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=7)

    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. \n"

    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc4_cost_model_coefficients_v2", output_dir)
    plt.close(fig)
    return paths


def main():
    parser = argparse.ArgumentParser(description="RC4 cost estimation v2")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    setup_style()

    here = Path(__file__).resolve()
    project_root = Path(args.project_root) if args.project_root else here.parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "figures" / "data" / "formal_experiments"
    output_dir.mkdir(parents=True, exist_ok=True)
    model_dir = project_root / "experiments" / "results" / "operator_cost_estimation_20260726"

    print(f"[RC4-v2] project_root = {project_root}")

    df = load_seed_models(model_dir)
    if df.empty:
        print(f"[RC4-v2][ERROR] not found seed JSON")
        return 1

    p1 = make_5seed_stability_figure(df, output_dir)
    print(f"[RC4-v2] Figure 1 saved")

    p2 = make_coefficients_figure(df, output_dir)
    print(f"[RC4-v2] Figure 2 saved")

    print(f"\n[RC4-v2] alldone. 2 figures at: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
