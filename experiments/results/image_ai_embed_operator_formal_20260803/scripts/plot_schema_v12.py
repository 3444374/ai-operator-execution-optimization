#!/usr/bin/env python3
"""Plot the schema-v12 per-image resource-efficiency figure (fig3).

Reads ../summary_schemav12.csv (4 cells: project/ray_data x cpu8/cpu16, schema-v12
matched-resource rerun) and produces fig3_per_image_resources_schemav12.png/svg:
a 2x2 of (energy J/1k, GPU ms/image, images/cpuCore-s, first_output_fraction),
each grouped bars project vs ray_data at cpu8/cpu16.

Style: Okabe-Ito color-blind-safe (matches plot_operator_formal.py), matplotlib-only,
paper-ready English labels, PNG + SVG. Reproducible: derived from committed raw
runs_matched_resource_schemav12_20260804.csv via summary_schemav12.csv.
"""
from __future__ import annotations
import csv
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

C_PROJECT = "#E69F00"   # orange (project method)
C_RAYDATA = "#0072B2"   # blue  (Ray Data native baseline)
C_GRID = "#BBBBBB"

BASE = Path(__file__).resolve().parent.parent
FIG = BASE / "figure"
FIG.mkdir(exist_ok=True)

cells = {}
with (BASE / "summary_schemav12.csv").open(encoding="utf-8") as f:
    for row in csv.DictReader(f):
        cells[(row["arm"], row["cpu"])] = row

def v(arm, cpu, key):
    return float(cells[(arm, cpu)][key])

plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False,
                     "figure.dpi": 150, "savefig.dpi": 150, "font.family": "DejaVu Sans"})
cpu_levels = ["8", "16"]
x = range(len(cpu_levels))
w = 0.38

panels = [
    ("joules_per_1k_images", "GPU energy (J / 1k images)\nlower = better", lambda arm, cpu: v(arm, cpu, "joules_per_1k_images"), "{:.0f}"),
    ("gpu_seconds_per_image", "GPU time (ms / image)\nlower = better", lambda arm, cpu: v(arm, cpu, "gpu_seconds_per_image") * 1000.0, "{:.2f}"),
    ("images_per_cpu_core_second", "Throughput / CPU core\n(images / core-s, higher = better)", lambda arm, cpu: v(arm, cpu, "images_per_cpu_core_second"), "{:.1f}"),
    ("first_output_fraction_of_e2e", "Streaming onset\n(first_output / E2E, lower = earlier)", lambda arm, cpu: v(arm, cpu, "first_output_fraction_of_e2e"), "{:.2f}"),
]

fig, axes = plt.subplots(2, 2, figsize=(7.4, 5.2))
for ax, (key, title, fn, fmt) in zip(axes.flat, panels):
    proj = [fn("project_ray", c) for c in cpu_levels]
    rd = [fn("ray_data_staged", c) for c in cpu_levels]
    b1 = ax.bar([i - w/2 for i in x], proj, w, label="project (ours)", color=C_PROJECT)
    b2 = ax.bar([i + w/2 for i in x], rd, w, label="Ray Data native", color=C_RAYDATA)
    ax.set_xticks(list(x)); ax.set_xticklabels([f"cpu{c}" for c in cpu_levels])
    ax.set_title(title, fontsize=8.8, fontweight="bold")
    ax.grid(axis="y", color=C_GRID, alpha=0.4, linewidth=0.5); ax.set_axisbelow(True)
    ymax = max(proj + rd)
    for bars, vals in ((b1, proj), (b2, rd)):
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width()/2, val + ymax*0.015, fmt.format(val),
                    ha="center", va="bottom", fontsize=7)
    ax.set_ylim(0, ymax * 1.18)
axes.flat[0].legend(loc="upper left", frameon=False, fontsize=7.5)
fig.suptitle("schema-v12: project wins on per-image resource efficiency at matched CPU",
             fontsize=10, fontweight="bold")
fig.text(0.01, 0.01,
         "60Kx2 held-out | l2_normalized | 3 formal/cell, CV<=2.1% | single-writer matrix runner | "
         "GPU time = gpu_workers*E2E/rows (allocation, not kernel); energy = nvidia-smi power samples",
         fontsize=5.8, color="#666")
fig.tight_layout(rect=(0, 0.035, 1, 0.95))
for ext in ("png", "svg"):
    fig.savefig(FIG / f"fig3_per_image_resources_schemav12.{ext}", bbox_inches="tight")
plt.close(fig)
print("wrote:", sorted(p.name for p in FIG.glob("fig3*")))
