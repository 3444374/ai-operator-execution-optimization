from __future__ import annotations

import csv
import os
import statistics
from pathlib import Path

os.environ.setdefault(
    "MPLCONFIGDIR",
    str(Path(__file__).resolve().parents[2] / ".cache" / "matplotlib"),
)

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import font_manager


ROOT = Path(__file__).resolve().parents[2]
INPUT_CSV = ROOT / "motivation" / "results" / "gpu" / "ai_embed_pgvector_writeback_20260714.csv"
OUT_DIR = ROOT / "figures" / "data" / "report_main"

FIGSIZE = (12.8, 7.2)
DPI = 220

COLORS = {
    "blue": "#2563EB",
    "orange": "#F97316",
    "green": "#16A34A",
    "purple": "#7C3AED",
    "slate": "#475569",
    "gray": "#CBD5E1",
    "ink": "#111827",
    "muted": "#64748B",
    "grid": "#E2E8F0",
    "bg": "#FFFFFF",
}


def setup_matplotlib() -> None:
    for font_path in [
        Path("C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/msyh.ttc"),
        Path("C:/Windows/Fonts/simhei.ttf"),
    ]:
        if font_path.exists():
            font_manager.fontManager.addfont(str(font_path))
            prop = font_manager.FontProperties(fname=str(font_path))
            plt.rcParams["font.family"] = prop.get_name()
            break
    plt.rcParams.update(
        {
            "axes.unicode_minus": False,
            "figure.facecolor": COLORS["bg"],
            "axes.facecolor": COLORS["bg"],
            "savefig.facecolor": COLORS["bg"],
            "text.color": COLORS["ink"],
            "axes.labelcolor": COLORS["ink"],
            "xtick.color": COLORS["muted"],
            "ytick.color": COLORS["muted"],
            "axes.edgecolor": COLORS["grid"],
            "font.size": 11,
        }
    )


def load_formal_rows() -> list[dict[str, str]]:
    with INPUT_CSV.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [row for row in rows if row["status"] == "ok" and row["phase"] == "formal"]


def mean_by_mode(rows: list[dict[str, str]]) -> dict[str, dict[str, float]]:
    metrics = ["e2e_s", "model_request_wall_s", "operator_wall_s", "writeback_s", "rows_per_s"]
    out: dict[str, dict[str, float]] = {}
    for mode in ["none", "json_text", "pgvector"]:
        selected = [row for row in rows if row["writeback_mode"] == mode]
        if not selected:
            raise ValueError(f"Missing writeback_mode={mode}")
        out[mode] = {metric: statistics.mean(float(row[metric]) for row in selected) for metric in metrics}
    return out


def style_axes(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(COLORS["grid"])
    ax.spines["bottom"].set_color(COLORS["grid"])
    ax.grid(axis="y", color=COLORS["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def save(fig: plt.Figure, stem: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for suffix in ("png", "svg"):
        path = OUT_DIR / f"{stem}.{suffix}"
        fig.savefig(path, dpi=DPI, bbox_inches="tight", pad_inches=0.16)
        print(path)
    plt.close(fig)


def main() -> None:
    setup_matplotlib()
    stats = mean_by_mode(load_formal_rows())
    labels = ["No writeback", "JSON text", "pgvector\nvector(384)"]
    modes = ["none", "json_text", "pgvector"]
    metrics = [
        ("e2e_s", "E2E", COLORS["blue"]),
        ("model_request_wall_s", "Model request wall", COLORS["orange"]),
        ("operator_wall_s", "Operator wall", COLORS["green"]),
        ("writeback_s", "Writeback", COLORS["purple"]),
    ]

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.10, right=0.94, top=0.78, bottom=0.23)
    x = np.arange(len(labels))
    width = 0.18
    for idx, (key, name, color) in enumerate(metrics):
        values = [stats[mode][key] for mode in modes]
        bars = ax.bar(x + (idx - 1.5) * width, values, width=width, label=name, color=color)
        for bar, value in zip(bars, values):
            if value >= 0.05:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    value + 0.055,
                    f"{value:.2f}",
                    ha="center",
                    fontsize=9,
                )

    json_e2e = stats["json_text"]["e2e_s"]
    pgvector_e2e = stats["pgvector"]["e2e_s"]
    json_write = stats["json_text"]["writeback_s"]
    pgvector_write = stats["pgvector"]["writeback_s"]

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Time (s)")
    ax.set_ylim(0, max(json_e2e, pgvector_e2e) * 1.32)
    style_axes(ax)
    ax.legend(frameon=False, ncol=4, loc="upper left", bbox_to_anchor=(0.0, 1.12), fontsize=9.5)
    fig.text(0.06, 0.955, "pgvector(384) changes the sink stage, not the GPU operator", fontsize=18, fontweight="bold")
    fig.text(
        0.06,
        0.915,
        "4096 rows, Ray actor, coalesced batches, one CUDA endpoint, formal repeats mean.",
        fontsize=11,
        color=COLORS["muted"],
    )
    fig.text(
        0.06,
        0.035,
        "Source: ai_embed_pgvector_writeback_20260714.csv. "
        f"E2E JSON {json_e2e:.2f}s vs pgvector {pgvector_e2e:.2f}s; "
        f"writeback JSON {json_write:.2f}s vs pgvector {pgvector_write:.2f}s. "
        "PG18.4 local rehearsal, not PG18.3.",
        fontsize=9.3,
        color=COLORS["muted"],
    )
    save(fig, "09_gpu_pgvector_writeback_comparison_20260714")


if __name__ == "__main__":
    main()
