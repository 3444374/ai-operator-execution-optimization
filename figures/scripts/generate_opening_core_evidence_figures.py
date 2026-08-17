#!/usr/bin/env python3
"""Generate the four frozen evidence figures used by the opening materials.

All panels are rebuilt from checked-in formal-result artifacts. Warm-up rows are
excluded. The script intentionally does not ingest the new database-E2E matrix:
that matrix closes a baseline-comparability gap, while these four figures carry
the already-frozen capacity, organization, image, and cost-estimation claims.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import matplotlib.pyplot as plt
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'DejaVu Sans', 'Liberation Sans']
plt.rcParams['svg.fonttype'] = 'none'

from matplotlib.lines import Line2D
import numpy as np
import pandas as pd


ROOT = Path(os.environ.get("AI_OPERATOR_ROOT", Path(__file__).resolve().parents[2])).resolve()
OUTPUT = Path(
    os.environ.get("OPENING_FIGURE_OUTPUT", ROOT / "figures" / "data" / "report_main")
).resolve()

BLUE = "#2864A8"
ORANGE = "#E9843B"
TEAL = "#278C8E"
RED = "#B74B4B"
PURPLE = "#7A62A8"
GREY = "#9AA4AF"
DARK = "#263238"
LIGHT_GRID = "#E5E8EB"
PALE_BLUE = "#EAF2FA"
PALE_RED = "#F9ECEA"


def apply_style() -> None:
    fonts = [
        "Microsoft YaHei",
        "SimHei",
        "PingFang SC",
        "Hiragino Sans GB",
        "Heiti SC",
        "Arial",
        "DejaVu Sans",
        "Liberation Sans",
    ]
    plt.rcParams.update(
        {
            "font.sans-serif": fonts,
            "font.size": 11,
            "axes.labelsize": 11,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.edgecolor": DARK,
            "axes.linewidth": 0.9,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "xtick.labelsize": 9.5,
            "ytick.labelsize": 9.5,
            "legend.fontsize": 9.5,
            "legend.frameon": False,
            "pdf.fonttype": 42,
            "savefig.facecolor": "white",
        }
    )


def finish(fig: plt.Figure, stem: str) -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT / f"{stem}.pdf", bbox_inches="tight")
    fig.savefig(OUTPUT / f"{stem}.svg", bbox_inches="tight")
    fig.savefig(OUTPUT / f"{stem}.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def panel_label(ax: plt.Axes, label: str) -> None:
    ax.text(
        -0.10,
        1.04,
        label,
        transform=ax.transAxes,
        fontsize=13,
        fontweight="bold",
        ha="left",
        va="bottom",
    )


def soft_grid(ax: plt.Axes, axis: str = "y") -> None:
    ax.grid(axis=axis, color=LIGHT_GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def figure_serving_capacity() -> None:
    src = ROOT / "experiments/results/dual_gpu_active_work_saturation_20260729/formal_summary.csv"
    d = pd.read_csv(src).sort_values("active_work_per_endpoint")
    x = d["active_work_per_endpoint"].to_numpy() / 1024

    fig = plt.figure(figsize=(13.2, 5.8), constrained_layout=True)
    gs = fig.add_gridspec(1, 3, width_ratios=[1.55, 1, 1])
    axes = [fig.add_subplot(gs[0, i]) for i in range(3)]

    ax = axes[0]
    ax.errorbar(
        x,
        d["tokens_per_s_mean"] / 1000,
        yerr=d["tokens_per_s_sd"] / 1000,
        color=BLUE,
        marker="o",
        markersize=6,
        linewidth=2.2,
        capsize=3,
        label="Throughput (mean ± SD, n=3)",
    )
    ax.axvspan(64, 132, color=PALE_BLUE, zorder=-2)
    ax.axvline(64, color=BLUE, linestyle="--", linewidth=1.2)
    ax.annotate(
        "65K reaches 97.8% of max\nNext level: only +0.92%",
        xy=(64, float(d.loc[d["active_work_per_endpoint"] == 65536, "tokens_per_s_mean"].iloc[0]) / 1000),
        xytext=(39, 6.35),
        arrowprops={"arrowstyle": "->", "color": DARK, "lw": 1.0},
        fontsize=10,
        color=DARK,
    )
    ax.set(xlabel="Active work per endpoint (K tokens)", ylabel="Throughput (K tokens/s)")
    ax.set_ylim(0, 9)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{v:.0f}" for v in x], rotation=35, ha="right")
    soft_grid(ax)
    panel_label(ax, "a")

    ax = axes[1]
    ax.plot(x, d["request_p99_s_mean"], color=RED, marker="s", linewidth=2.0, markersize=5.5)
    ax.axvspan(64, 132, color=PALE_RED, zorder=-2)
    ax.axvline(64, color=BLUE, linestyle="--", linewidth=1.2)
    ax.annotate(
        "+8.9%",
        xy=(96, float(d.loc[d["active_work_per_endpoint"] == 98304, "request_p99_s_mean"].iloc[0])),
        xytext=(76, 51),
        arrowprops={"arrowstyle": "->", "color": RED, "lw": 1.0},
        fontsize=10,
        color=RED,
    )
    ax.set(xlabel="Active work (K tokens)", ylabel="Request P99 (s)")
    ax.set_ylim(0, 70)
    ax.set_xticks([16, 32, 64, 96, 128])
    soft_grid(ax)
    panel_label(ax, "b")

    ax = axes[2]
    ax.plot(x, d["slo_goodput_per_s_mean"], color=TEAL, marker="^", linewidth=2.0, markersize=6)
    ax.axvspan(64, 132, color=PALE_BLUE, zorder=-2)
    ax.axvline(64, color=BLUE, linestyle="--", linewidth=1.2)
    ax.set(xlabel="Active work (K tokens)", ylabel="30-s SLO goodput (req/s)")
    ax.set_ylim(0, 27)
    ax.set_xticks([16, 32, 64, 96, 128])
    soft_grid(ax)
    panel_label(ax, "c")

    fig.suptitle("Serving capacity saturates near 65K active work while tail latency keeps rising", fontsize=15, fontweight="bold")
    finish(fig, "opening_serving_capacity_frontier")


def _formal_runs(path: Path) -> pd.DataFrame:
    d = pd.read_csv(path)
    return d.loc[d["phase"].eq("formal")].copy()


def figure_work_organization() -> None:
    paths = {
        "2 endpoints / large KV pool": ROOT / "experiments/results/rc1_data_organization/dataorg_2ep_1.5b_cacheON_20260731/raw/runs.csv",
        "4 endpoints / small KV pool": ROOT / "experiments/results/rc1_data_organization/dataorg_4ep_1.5b_cacheON_20260731/raw/runs.csv",
    }
    frames = []
    for topology, path in paths.items():
        d = _formal_runs(path)
        d["topology"] = topology
        frames.append(d)
    all_runs = pd.concat(frames, ignore_index=True)
    methods = ["fixed_rows_16", "sequential_tb", "length_align_tb", "best_fit_tb", "row_cap_aware_tb"]
    labels = ["Fixed rows", "Sequential", "Length align", "Best-fit", "Row-cap aware"]
    colors = [GREY, BLUE, ORANGE, PURPLE, TEAL]

    fig = plt.figure(figsize=(13.2, 6.2), constrained_layout=True)
    gs = fig.add_gridspec(1, 2, width_ratios=[1.55, 1])
    ax_bar = fig.add_subplot(gs[0, 0])
    ax_scatter = fig.add_subplot(gs[0, 1])

    x = np.arange(len(methods))
    width = 0.34
    topo_defs = [
        ("2 endpoints / large KV pool", -width / 2, "white", DARK, "///"),
        ("4 endpoints / small KV pool", width / 2, BLUE, BLUE, ""),
    ]
    rng = np.random.default_rng(20260807)
    for topology, offset, face, edge, hatch in topo_defs:
        sub = all_runs.loc[all_runs["topology"].eq(topology)]
        med = np.array([sub.loc[sub["scenario_id"].eq(m), "tokens_per_s"].median() for m in methods]) / 1000
        bars = ax_bar.bar(
            x + offset,
            med,
            width=width,
            color=face,
            edgecolor=edge,
            linewidth=1.3,
            hatch=hatch,
            label=topology,
            zorder=2,
        )
        for i, method in enumerate(methods):
            vals = sub.loc[sub["scenario_id"].eq(method), "tokens_per_s"].to_numpy() / 1000
            jitter = rng.uniform(-0.035, 0.035, size=len(vals))
            ax_bar.scatter(np.full_like(vals, x[i] + offset) + jitter, vals, s=18, color=edge, zorder=4, alpha=0.8)
        for bar, val in zip(bars, med):
            ax_bar.text(bar.get_x() + bar.get_width() / 2, val + 0.8, f"{val:.1f}", ha="center", va="bottom", fontsize=8.5)

    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(labels, rotation=18, ha="right")
    ax_bar.set_ylabel("End-to-end throughput (K tokens/s)")
    ax_bar.set_ylim(0, 63)
    ax_bar.legend(loc="lower left", ncol=2)
    ax_bar.text(0.02, 0.96, "Low KV pressure (2 endpoints): max 7–10%", transform=ax_bar.transAxes, va="top", color=DARK, fontsize=9.5)
    ax_bar.text(0.02, 0.90, "High KV pressure (4 endpoints): max 98–100%; ranking reverses", transform=ax_bar.transAxes, va="top", color=BLUE, fontsize=9.5, fontweight="bold")
    soft_grid(ax_bar)
    panel_label(ax_bar, "a")

    marker_map = {m: marker for m, marker in zip(methods, ["o", "s", "^", "D", "P"])}
    label_offsets = {
        "fixed_rows_16": (5, 5),
        "sequential_tb": (5, 5),
        "length_align_tb": (7, 19),
        "best_fit_tb": (7, 5),
        "row_cap_aware_tb": (7, -10),
    }
    for topology, color, fill in [
        ("2 endpoints / large KV pool", GREY, "none"),
        ("4 endpoints / small KV pool", BLUE, BLUE),
    ]:
        sub = all_runs.loc[all_runs["topology"].eq(topology)]
        for method, label in zip(methods, labels):
            one = sub.loc[sub["scenario_id"].eq(method)]
            gx = one["prefix_group_ratio"].median()
            gy = one["vllm_prefix_cache_hit_rate"].median()
            ax_scatter.scatter(
                gx,
                gy,
                s=85,
                marker=marker_map[method],
                facecolor=fill,
                edgecolor=color,
                linewidth=1.5,
                zorder=3,
            )
            if topology.startswith("4"):
                ax_scatter.annotate(
                    label,
                    (gx, gy),
                    xytext=label_offsets[method],
                    textcoords="offset points",
                    fontsize=8.5,
                    color=DARK,
                )
    ax_scatter.axhspan(0, 0.10, color=PALE_RED, zorder=-2)
    ax_scatter.set(xlabel="Prefix group ratio", ylabel="Prefix-cache hit rate")
    ax_scatter.set_xlim(0, 0.32)
    ax_scatter.set_ylim(0, 0.82)
    soft_grid(ax_scatter)
    ax_scatter.legend(
        handles=[
            Line2D([0], [0], marker="o", linestyle="none", markerfacecolor="none", markeredgecolor=GREY, label="Low KV pressure (2 endpoints)"),
            Line2D([0], [0], marker="o", linestyle="none", markerfacecolor=BLUE, markeredgecolor=BLUE, label="High KV pressure (4 endpoints)"),
        ],
        loc="center right",
    )
    ax_scatter.text(0.02, 0.03, "Reordering collapses hit rate to 0.06–0.07 under KV pressure", transform=ax_scatter.transAxes, fontsize=9.5, color=RED)
    panel_label(ax_scatter, "b")

    fig.suptitle("Data organization is regime-dependent: disrupting prefix locality backfires under KV pressure", fontsize=15, fontweight="bold")
    finish(fig, "opening_work_organization_regime")


def figure_image_matched_resource() -> None:
    primary = pd.read_csv(ROOT / "experiments/results/image_ai_embed_operator_formal_20260803/summary.csv")
    confirm = pd.read_csv(ROOT / "experiments/results/image_ai_embed_operator_formal_20260803/summary_schemav12.csv")
    cpu_levels = [8, 16]
    arms = [("ray_data_staged", "Ray Data staged", GREY, "///"), ("project_ray", "Project static", BLUE, "")]

    fig = plt.figure(figsize=(13.2, 5.8), constrained_layout=True)
    gs = fig.add_gridspec(1, 2, width_ratios=[1.45, 1])
    ax = fig.add_subplot(gs[0, 0])
    x = np.arange(len(cpu_levels))
    width = 0.34
    for idx, (arm, label, color, hatch) in enumerate(arms):
        vals, errs = [], []
        for cpu in cpu_levels:
            row = primary.loc[(primary["arm"].eq(arm)) & (primary["cpu_workers"].eq(cpu))].iloc[0]
            vals.append(float(row["operator_jct_s"]))
            errs.append(float(row["operator_jct_s"]) * float(row["cv_pct"]) / 100.0)
        pos = x + (idx - 0.5) * width
        bars = ax.bar(
            pos,
            vals,
            width,
            yerr=errs,
            capsize=4,
            color="white" if arm == "ray_data_staged" else color,
            edgecolor=DARK if arm == "ray_data_staged" else color,
            linewidth=1.4,
            hatch=hatch,
            label=label,
        )
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, val + 4, f"{val:.1f}s", ha="center", fontsize=9)
    for i, cpu in enumerate(cpu_levels):
        rd = primary.loc[(primary["arm"].eq("ray_data_staged")) & (primary["cpu_workers"].eq(cpu)), "operator_jct_s"].iloc[0]
        pr = primary.loc[(primary["arm"].eq("project_ray")) & (primary["cpu_workers"].eq(cpu)), "operator_jct_s"].iloc[0]
        gain = (rd - pr) / rd * 100
        ax.text(i, max(rd, pr) + 14, f"−{gain:.1f}%", ha="center", color=BLUE, fontsize=11, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(["8 CPU workers", "16 CPU workers"])
    ax.set_ylabel("Operator JCT (s; lower is better)")
    ax.set_ylim(0, 155)
    ax.legend(loc="upper right")
    soft_grid(ax)
    panel_label(ax, "a")

    ax = fig.add_subplot(gs[0, 1])
    campaigns = []
    for name, frame, cpu_col in [("Primary run", primary, "cpu_workers"), ("Independent repeat", confirm, "cpu")]:
        gains = []
        for cpu in cpu_levels:
            rd = frame.loc[(frame["arm"].eq("ray_data_staged")) & (frame[cpu_col].eq(cpu)), "operator_jct_s"].iloc[0]
            pr = frame.loc[(frame["arm"].eq("project_ray")) & (frame[cpu_col].eq(cpu)), "operator_jct_s"].iloc[0]
            gains.append((rd - pr) / rd * 100)
        campaigns.append((name, gains))
    y = np.arange(len(campaigns))[::-1]
    for yi, (name, gains), color in zip(y, campaigns, [BLUE, TEAL]):
        ax.plot(gains, [yi, yi], color=color, linewidth=2.0, marker="o", markersize=7)
        ax.text(min(gains) - 0.8, yi, "CPU 8", ha="right", va="center", fontsize=8.5, color=DARK)
        ax.text(max(gains) + 0.8, yi, "CPU 16", ha="left", va="center", fontsize=8.5, color=DARK)
    ax.axvline(5, color=RED, linestyle="--", linewidth=1.2)
    ax.text(5.3, 1.35, "Pre-registered 5% gate", color=RED, fontsize=9)
    ax.set_yticks(y)
    ax.set_yticklabels([c[0] for c in campaigns])
    ax.set_xlim(0, 21)
    ax.set_ylim(-0.7, 1.7)
    ax.set_xlabel("JCT reduction versus Ray Data (%)")
    soft_grid(ax, axis="x")
    ax.text(0.04, 0.08, "Both campaigns and CPU levels agree\nFrozen headline: a conservative 13–15%", transform=ax.transAxes, fontsize=10, color=DARK)
    panel_label(ax, "b")

    fig.suptitle("A staged actor execution path reduces image-operator JCT under matched resources", fontsize=15, fontweight="bold")
    finish(fig, "opening_image_matched_resource")


def _load_json_replacing_invalid_utf8(path: Path) -> dict:
    return json.loads(path.read_bytes().decode("utf-8", errors="replace"))


def figure_cost_decision() -> None:
    src = ROOT / "experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/ce_context_loo_rerun_20260807.json"
    data = _load_json_replacing_invalid_utf8(src)["estimators"]
    methods = list(data)
    labels = ["Mean", "Analytical", "Lookup", "Ridge", "LightGBM", "Hybrid"]
    colors = [GREY, GREY, GREY, ORANGE, PURPLE, BLUE]

    median = []
    macro = []
    maximum = []
    pairwise = []
    for method in methods:
        summary = data[method]["summary"]
        regret = summary["macro_fold_distributions"]["decision_regret_pct"]
        median.append(regret["median"])
        macro.append(regret["mean"])
        maximum.append(regret["max"])
        pairwise.append(summary["macro_fold_distributions"]["candidate_pairwise_accuracy"]["mean"])

    fig = plt.figure(figsize=(13.2, 5.8), constrained_layout=True)
    gs = fig.add_gridspec(1, 2, width_ratios=[1.5, 1])
    ax = fig.add_subplot(gs[0, 0])
    x = np.arange(len(methods))
    width = 0.22
    for values, offset, label, hatch in [
        (median, -width, "Median regret", "///"),
        (macro, 0, "Macro-mean regret", ""),
        (maximum, width, "Maximum regret", "xx"),
    ]:
        ax.bar(x + offset, values, width, label=label, color=[c if label == "Macro-mean regret" else "white" for c in colors], edgecolor=colors, linewidth=1.2, hatch=hatch)
    ax.axhline(5, color=TEAL, linestyle="--", linewidth=1.1)
    ax.axhline(15, color=RED, linestyle="--", linewidth=1.1)
    ax.text(5.46, 5.8, "median/macro ≤5%", ha="right", color=TEAL, fontsize=8.5)
    ax.text(5.46, 15.8, "max ≤15%", ha="right", color=RED, fontsize=8.5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=18, ha="right")
    ax.set_ylabel("Candidate-selection regret (%)")
    ax.set_ylim(0, 90)
    ax.legend(loc="upper right")
    soft_grid(ax)
    ax.annotate(
        "14.72%: only 0.28 pp margin",
        xy=(5 + width, maximum[-1]),
        xytext=(3.25, 30),
        arrowprops={"arrowstyle": "->", "color": RED, "lw": 1.0},
        color=RED,
        fontsize=9.5,
    )
    panel_label(ax, "a")

    ax = fig.add_subplot(gs[0, 1])
    for i, (label, p, mx, color) in enumerate(zip(labels, pairwise, maximum, colors)):
        marker = "*" if i == len(labels) - 1 else "o"
        size = 170 if marker == "*" else 75
        ax.scatter(p, mx, color=color, edgecolor=DARK, linewidth=0.7, marker=marker, s=size, zorder=3)
        if label == "Lookup":
            continue
        display_label = "Mean / Lookup" if label == "Mean" else label
        ax.annotate(display_label, (p, mx), xytext=(5, 4), textcoords="offset points", fontsize=8.5)
    ax.axvline(0.75, color=TEAL, linestyle="--", linewidth=1.1)
    ax.axhline(15, color=RED, linestyle="--", linewidth=1.1)
    ax.fill_betweenx([0, 15], 0.75, 0.84, color=PALE_BLUE, zorder=-2)
    ax.set(xlabel="Candidate-level pairwise accuracy", ylabel="Maximum decision regret (%)")
    ax.set_xlim(0.47, 0.84)
    ax.set_ylim(0, 90)
    soft_grid(ax)
    ax.text(0.98, 0.05, "Pass region", transform=ax.transAxes, ha="right", color=BLUE, fontweight="bold")
    panel_label(ax, "b")

    fig.suptitle("Hybrid passes the selection contract, but its maximum regret is only marginally below the gate", fontsize=15, fontweight="bold")
    finish(fig, "opening_cost_model_decision_quality")


def main() -> None:
    apply_style()
    figure_serving_capacity()
    figure_work_organization()
    figure_image_matched_resource()
    figure_cost_decision()


if __name__ == "__main__":
    main()
