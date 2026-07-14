from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault(
    "MPLCONFIGDIR",
    str(Path(__file__).resolve().parents[2] / ".cache" / "matplotlib"),
)

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import font_manager


ROOT = Path(__file__).resolve().parents[2]
INPUT_CSV = ROOT / "motivation" / "results" / "gpu" / "ai_embed_pgai_integrated_key_20260714.csv"
OUT_DIR = ROOT / "figures" / "data" / "report_main"

FIGSIZE = (12.8, 7.2)
DPI = 220

COLORS = {
    "blue": "#2563EB",
    "cyan": "#0891B2",
    "green": "#16A34A",
    "orange": "#F97316",
    "amber": "#F59E0B",
    "purple": "#7C3AED",
    "rose": "#E11D48",
    "slate": "#475569",
    "gray": "#CBD5E1",
    "ink": "#111827",
    "muted": "#64748B",
    "grid": "#E2E8F0",
    "bg": "#FFFFFF",
}


def setup_matplotlib() -> None:
    font_candidates = [
        Path("C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/msyh.ttc"),
        Path("C:/Windows/Fonts/simhei.ttf"),
    ]
    for font_path in font_candidates:
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


def load_data() -> pd.DataFrame:
    df = pd.read_csv(INPUT_CSV)
    df = df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()
    numeric = [
        "total_rows",
        "object_count",
        "operator_invocations",
        "model_workers",
        "e2e_s",
        "db_fetch_s",
        "arrow_build_s",
        "model_request_wall_s",
        "operator_wall_s",
        "submit_s",
        "bounded_wait_s",
        "fanin_s",
        "writeback_s",
        "rows_per_s",
    ]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def row(df: pd.DataFrame, experiment_id: str) -> pd.Series:
    rows = df[df["experiment_id"] == experiment_id]
    if rows.empty:
        raise ValueError(f"Missing experiment_id={experiment_id}")
    return rows.iloc[0]


def save(fig: plt.Figure, stem: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for suffix in ("png", "svg"):
        path = OUT_DIR / f"{stem}.{suffix}"
        fig.savefig(path, dpi=DPI, bbox_inches="tight", pad_inches=0.16)
        print(path)
    plt.close(fig)


def style_axes(ax: plt.Axes, xgrid: bool = False, ygrid: bool = True) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(COLORS["grid"])
    ax.spines["bottom"].set_color(COLORS["grid"])
    if ygrid:
        ax.grid(axis="y", color=COLORS["grid"], linewidth=0.8)
    if xgrid:
        ax.grid(axis="x", color=COLORS["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def title_note(fig: plt.Figure, title: str, subtitle: str, note: str) -> None:
    fig.text(0.06, 0.955, title, fontsize=18, fontweight="bold")
    fig.text(0.06, 0.915, subtitle, fontsize=11, color=COLORS["muted"])
    fig.text(0.06, 0.035, note, fontsize=9.3, color=COLORS["muted"])


def chart_granularity(df: pd.DataFrame) -> None:
    coalesced = row(df, "gpu_key_batch_coalesced_nowrite_1024_20260714")
    fine = row(df, "gpu_key_batch_fine_nowrite_1024_20260714")
    labels = [
        f"Coalesced\n{int(coalesced.operator_invocations)} calls",
        f"Fine-grained\n{int(fine.operator_invocations)} calls",
    ]
    values = [coalesced.e2e_s, fine.e2e_s]
    ratio = fine.e2e_s / coalesced.e2e_s

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.12, right=0.94, top=0.80, bottom=0.18)
    bars = ax.barh(labels, values, color=[COLORS["blue"], COLORS["orange"]], height=0.46)
    ax.set_xscale("log")
    ax.set_xlabel("End-to-end time, log scale (s)")
    ax.set_xlim(0.3, 32)
    style_axes(ax, xgrid=True, ygrid=False)
    for bar, value in zip(bars, values):
        ax.text(value * 1.08, bar.get_y() + bar.get_height() / 2, f"{value:.3f}s", va="center", fontweight="bold")
    ax.text(
        2.1,
        0.55,
        f"{ratio:.1f}x slower",
        fontsize=16,
        fontweight="bold",
        color=COLORS["rose"],
    )
    title_note(
        fig,
        "Batch granularity dominates small AI_EMBED calls",
        "1024 rows, PostgreSQL 18.4 local rehearsal, CUDA endpoint, no writeback.",
        "Source: ai_embed_pgai_integrated_key_20260714.csv. This is PG18.4 local rehearsal, not PG18.3.",
    )
    save(fig, "06_gpu_pgai_rerun_granularity_20260714")


def residual(row_data: pd.Series) -> float:
    return max(
        0.0,
        float(row_data.e2e_s)
        - float(row_data.db_fetch_s)
        - float(row_data.arrow_build_s)
        - float(row_data.model_request_wall_s)
        - float(row_data.fanin_s)
        - float(row_data.writeback_s),
    )


def chart_stage_writeback(df: pd.DataFrame) -> None:
    scenarios = [
        ("4K\nno writeback", row(df, "gpu_key_chain_nowrite_4096_20260714")),
        ("4K\nJSON writeback", row(df, "gpu_key_chain_json_writeback_4096_20260714")),
        ("8K\nJSON writeback", row(df, "gpu_key_scale_json_writeback_8192_20260714")),
    ]
    stages = [
        ("DB fetch", "db_fetch_s", COLORS["cyan"]),
        ("Arrow build", "arrow_build_s", COLORS["green"]),
        ("Model request wall", "model_request_wall_s", COLORS["orange"]),
        ("fan-in", "fanin_s", COLORS["slate"]),
        ("Writeback", "writeback_s", COLORS["purple"]),
        ("Other measured residual", "residual", COLORS["gray"]),
    ]

    values = []
    for _, item in scenarios:
        stage_values = []
        for _, key, _ in stages:
            stage_values.append(residual(item) if key == "residual" else float(item[key]))
        values.append(stage_values)
    data = np.array(values)

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.12, right=0.94, top=0.78, bottom=0.24)
    x = np.arange(len(scenarios))
    bottom = np.zeros(len(scenarios))
    for idx, (stage, _, color) in enumerate(stages):
        bars = ax.bar(x, data[:, idx], bottom=bottom, width=0.52, label=stage, color=color)
        for bar, value, base in zip(bars, data[:, idx], bottom):
            if value >= 0.28:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    base + value / 2,
                    f"{value:.1f}s",
                    ha="center",
                    va="center",
                    fontsize=9,
                    fontweight="bold",
                    color="white" if color != COLORS["gray"] else COLORS["ink"],
                )
        bottom += data[:, idx]
    totals = [float(item.e2e_s) for _, item in scenarios]
    for xpos, total in zip(x, totals):
        ax.text(xpos, total + 0.18, f"{total:.2f}s", ha="center", fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([label for label, _ in scenarios])
    ax.set_ylabel("End-to-end stage time (s)")
    ax.set_ylim(0, max(totals) * 1.22)
    style_axes(ax)
    ax.legend(frameon=False, ncol=3, loc="upper left", bbox_to_anchor=(0.0, 1.12), fontsize=9.5)
    title_note(
        fig,
        "Writeback becomes visible after GPU model calls are fast",
        "Coalesced batches, single CUDA endpoint; JSON writeback compared with no-writeback.",
        "Source: ai_embed_pgai_integrated_key_20260714.csv. Current sink is PostgreSQL JSON text, not vector(384).",
    )
    save(fig, "07_gpu_pgai_rerun_stage_writeback_20260714")


def chart_endpoint(df: pd.DataFrame) -> None:
    single = row(df, "gpu_key_ray_actor_single_endpoint_4096_20260714")
    dual = row(df, "gpu_key_ray_actor_dual_endpoint_4096_20260714")
    labels = ["Ray actor\n1 endpoint", "Ray actor\n2 endpoints"]
    metrics = [
        ("e2e_s", "E2E", COLORS["blue"]),
        ("model_request_wall_s", "Model request wall", COLORS["orange"]),
        ("operator_wall_s", "Operator wall", COLORS["green"]),
        ("writeback_s", "Writeback", COLORS["purple"]),
    ]
    rows = [single, dual]

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.10, right=0.94, top=0.78, bottom=0.22)
    x = np.arange(len(labels))
    width = 0.18
    for idx, (key, name, color) in enumerate(metrics):
        vals = [float(r[key]) for r in rows]
        bars = ax.bar(x + (idx - 1.5) * width, vals, width=width, label=name, color=color)
        for bar, value in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, value + 0.06, f"{value:.2f}", ha="center", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Time (s)")
    ax.set_ylim(0, 4.15)
    style_axes(ax)
    ax.legend(frameon=False, ncol=4, loc="upper left", bbox_to_anchor=(0, 1.12), fontsize=9.5)
    title_note(
        fig,
        "Dual local endpoints reduce the operator stage, not writeback",
        "4096 rows, coalesced batches, Ray actor, CUDA endpoints on the same RTX 5070.",
        "Source: ai_embed_pgai_integrated_key_20260714.csv. E2E: 3.62s -> 2.86s; two endpoints are replicas on one GPU, not multi-GPU.",
    )
    save(fig, "08_gpu_pgai_rerun_endpoint_comparison_20260714")


def main() -> None:
    setup_matplotlib()
    df = load_data()
    chart_granularity(df)
    chart_stage_writeback(df)
    chart_endpoint(df)


if __name__ == "__main__":
    main()
