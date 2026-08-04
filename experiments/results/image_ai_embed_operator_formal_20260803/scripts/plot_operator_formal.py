#!/usr/bin/env python3
"""Plot the AI_EMBED operator formal experiment (step6 + step8) meaningful figures.

Reads ../summary.csv (4 cells: Ray Data / project x cpu8 / cpu16) and produces:
  fig1_matched_resource_jct.png/svg  -- the causal 2x2 (operator_jct, lower=better)
  fig2_throughput_vs_gpu_ceiling.png/svg -- all arms vs R0 dual-GPU ceiling (headroom)

Style: Okabe-Ito color-blind-safe (matches figures/scripts/_scipilot_helpers.py),
matplotlib-only (no seaborn), paper-ready English labels, PNG + SVG.

Reproducible: raw data = ../raw/runs_step{6,8}_*.csv (committed); this script
derives the 4 cell medians from ../summary.csv.
"""
from __future__ import annotations
import csv
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Okabe-Ito color-blind-safe (project convention)
C_RAYDATA = "#0072B2"   # blue  -- framework-native baseline
C_PROJECT = "#E69F00"   # orange -- project method
C_CEILING = "#CC79A7"   # purple -- capacity ceiling
C_GRID = "#BBBBBB"

BASE = Path(__file__).resolve().parent.parent
FIG = BASE / "figure"
FIG.mkdir(exist_ok=True)

# --- load 4 cells from summary.csv ---
cells = {}
with (BASE / "summary.csv").open() as f:
    for row in csv.DictReader(f):
        cells[row["cell"]] = row

def f(k, cell): return float(cells[cell][k])

# R0 GPU-resident forward ceiling (image_clip_transfer_ceiling_20260802):
# single-GPU ~9700 img/s, dual ~19400 img/s (R0 batch64). Cited in README sec9.
R0_SINGLE = 9700.0
R0_DUAL = 19400.0

plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False,
                     "figure.dpi": 150, "savefig.dpi": 150, "font.family": "DejaVu Sans"})

# ===========================================================================
# Fig 1: matched-resource 2x2 (operator_jct, lower=better) -- the causal result
# ===========================================================================
fig, ax = plt.subplots(figsize=(4.2, 3.0))
cpu_levels = ["cpu8", "cpu16"]
rd_jct = [f("operator_jct_s", "A1"), f("operator_jct_s", "A2")]      # RD cpu8, cpu16
proj_jct = [f("operator_jct_s", "B1"), f("operator_jct_s", "B2")]    # project cpu8, cpu16
x = range(len(cpu_levels))
w = 0.35
b1 = ax.bar([i - w/2 for i in x], rd_jct, w, label="Ray Data native", color=C_RAYDATA)
b2 = ax.bar([i + w/2 for i in x], proj_jct, w, label="project (ours)", color=C_PROJECT)
ax.set_ylabel("operator JCT (s, lower=better)")
ax.set_xticks(list(x)); ax.set_xticklabels(cpu_levels)
ax.set_xlabel("CPU preprocess slots (matched)")
ax.set_title("Matched-resource: project faster at BOTH CPU levels", fontsize=9.5, fontweight="bold")
ax.grid(axis="y", color=C_GRID, alpha=0.4, linewidth=0.5); ax.set_axisbelow(True)
# annotate bars with values + matched advantage
for i in x:
    ax.text(i - w/2, rd_jct[i] + 1.5, f"{rd_jct[i]:.0f}s", ha="center", va="bottom", fontsize=7.5)
    ax.text(i + w/2, proj_jct[i] + 1.5, f"{proj_jct[i]:.0f}s", ha="center", va="bottom", fontsize=7.5)
    adv = (rd_jct[i] - proj_jct[i]) / rd_jct[i] * 100
    ax.text(i, rd_jct[i] + 11, f"project −{adv:.1f}%", ha="center", va="bottom",
            fontsize=8.5, fontweight="bold", color=C_PROJECT)
ax.set_ylim(0, max(rd_jct) * 1.22)
ax.legend(loc="upper right", frameon=False, fontsize=8)
fig.text(0.01, 0.01, "60Kx2 held-out (disjoint) | l2_normalized | 3 formal/cell, CV<=3.2% | commit 37dc8fd",
         fontsize=6, color="#666")
fig.tight_layout(rect=(0, 0.03, 1, 1))
for ext in ("png", "svg"):
    fig.savefig(FIG / f"fig1_matched_resource_jct.{ext}", bbox_inches="tight")
plt.close(fig)

# ===========================================================================
# Fig 2: throughput (img/s) vs R0 dual-GPU ceiling -- the headroom story
# ===========================================================================
fig, ax = plt.subplots(figsize=(4.6, 3.2))
arms = ["Daft built-in\n(5K, separate)", "Ray Data\ncpu8", "Ray Data\ncpu16",
        "project\ncpu8", "project\ncpu16"]
# Daft 5K calib 187; RD/project from summary; R0 dual ceiling as a line
img = [187.0, f("images_per_s", "A1"), f("images_per_s", "A2"),
       f("images_per_s", "B1"), f("images_per_s", "B2")]
colors = ["#999999", C_RAYDATA, C_RAYDATA, C_PROJECT, C_PROJECT]
xpos = range(len(arms))
bars = ax.bar(xpos, img, color=colors, width=0.62)
ax.axhline(R0_DUAL, color=C_CEILING, ls="--", lw=1.3, label=f"R0 GPU ceiling (dual) ~{R0_DUAL:.0f}")
ax.set_ylabel("images / s")
ax.set_xticks(list(xpos)); ax.set_xticklabels(arms, fontsize=7.5)
ax.set_title("All arms far below GPU ceiling -> bottleneck is feeding, not compute", fontsize=9, fontweight="bold")
ax.grid(axis="y", color=C_GRID, alpha=0.4, linewidth=0.5); ax.set_axisbelow(True)
for i, v in enumerate(img):
    pct = v / R0_DUAL * 100
    ax.text(i, v + 250, f"{v:.0f}\n({pct:.0f}%)", ha="center", va="bottom", fontsize=7)
ax.set_ylim(0, R0_DUAL * 1.12)
ax.legend(loc="upper left", frameon=False, fontsize=8)
fig.text(0.01, 0.01, "R0 = GPU-resident forward ceiling (image_clip_transfer_ceiling_20260802); "
         "Daft at 5K (materializes, can't scale); others 60Kx2 held-out",
         fontsize=6, color="#666")
fig.tight_layout(rect=(0, 0.03, 1, 1))
for ext in ("png", "svg"):
    fig.savefig(FIG / f"fig2_throughput_vs_gpu_ceiling.{ext}", bbox_inches="tight")
plt.close(fig)

print("wrote:", sorted(p.name for p in FIG.glob("fig*")))
