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
GPU_RESULTS = ROOT / "motivation" / "results" / "gpu"
OUT_DIR = ROOT / "figures" / "data" / "generated_gpu"

CHAIN_CSV = GPU_RESULTS / "ai_embed_chain_breakdown_20260712.csv"
MULTI_CSV = GPU_RESULTS / "ai_embed_multi_endpoint_20260712.csv"

FIGSIZE = (12.8, 7.2)
DPI = 220

COLORS = {
    "blue": "#2563EB",
    "sky": "#0EA5E9",
    "green": "#22C55E",
    "orange": "#F97316",
    "amber": "#F59E0B",
    "purple": "#7C3AED",
    "slate": "#64748B",
    "gray": "#CBD5E1",
    "ink": "#1F2937",
    "muted": "#64748B",
    "grid": "#E2E8F0",
    "bg": "#F8FAFC",
}


def setup_matplotlib() -> None:
    font_candidates = [
        Path("C:/Windows/Fonts/msyh.ttc"),
        Path("C:/Windows/Fonts/simhei.ttf"),
        Path("C:/Windows/Fonts/arial.ttf"),
    ]
    for font_path in font_candidates:
        if font_path.exists():
            font_manager.fontManager.addfont(str(font_path))
            prop = font_manager.FontProperties(fname=str(font_path))
            plt.rcParams["font.family"] = prop.get_name()
            break
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.facecolor"] = COLORS["bg"]
    plt.rcParams["axes.facecolor"] = COLORS["bg"]
    plt.rcParams["savefig.facecolor"] = COLORS["bg"]
    plt.rcParams["text.color"] = COLORS["ink"]
    plt.rcParams["axes.labelcolor"] = COLORS["muted"]
    plt.rcParams["xtick.color"] = COLORS["muted"]
    plt.rcParams["ytick.color"] = COLORS["ink"]
    plt.rcParams["axes.edgecolor"] = COLORS["grid"]


def load_formal(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    return df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()


def grouped_mean(df: pd.DataFrame, keys: list[str], metrics: list[str]) -> pd.DataFrame:
    extra = ["operator_invocations", "total_rows"]
    cols = list(dict.fromkeys(keys + metrics + extra))
    out = df[cols].groupby(keys, as_index=False).mean(numeric_only=True)
    out["rows"] = out["total_rows"].astype(int)
    out["calls"] = out["operator_invocations"]
    return out


def row_where(df: pd.DataFrame, **conditions) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for key, value in conditions.items():
        mask &= df[key] == value
    rows = df[mask]
    if rows.empty:
        raise ValueError(f"Missing row for {conditions}")
    return rows.iloc[0]


def save_figure(fig: plt.Figure, name: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    png = OUT_DIR / f"{name}.png"
    svg = OUT_DIR / f"{name}.svg"
    fig.savefig(png, dpi=DPI, bbox_inches="tight", pad_inches=0.18)
    fig.savefig(svg, bbox_inches="tight", pad_inches=0.18)
    plt.close(fig)
    print(png)
    print(svg)


def style_axes(ax: plt.Axes, xgrid: bool = False, ygrid: bool = True) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(COLORS["grid"])
    ax.spines["bottom"].set_color(COLORS["grid"])
    if ygrid:
        ax.grid(axis="y", color=COLORS["grid"], linewidth=0.9, alpha=0.9)
    if xgrid:
        ax.grid(axis="x", color=COLORS["grid"], linewidth=0.9, alpha=0.9)
    ax.set_axisbelow(True)


def title_and_note(fig: plt.Figure, title: str, subtitle: str, note: str) -> None:
    fig.text(0.06, 0.955, title, fontsize=18, fontweight="bold", color=COLORS["ink"])
    fig.text(0.06, 0.915, subtitle, fontsize=11, color=COLORS["muted"])
    fig.text(0.06, 0.035, note, fontsize=9.5, color=COLORS["muted"])


def chart_invocation_granularity(chain_avg: pd.DataFrame) -> None:
    coalesced = row_where(chain_avg, rows=1024, executor="ray_actor", strategy="coalesced")
    fine = row_where(chain_avg, rows=1024, executor="ray_actor", strategy="fine")
    labels = [f"coalesced\n{coalesced.calls:.0f} calls", f"fine\n{fine.calls:.0f} calls"]
    values = [coalesced.e2e_s, fine.e2e_s]
    ratio = fine.e2e_s / coalesced.e2e_s

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.11, right=0.95, top=0.82, bottom=0.18)
    bars = ax.bar(labels, values, color=[COLORS["blue"], COLORS["orange"]], width=0.48)
    style_axes(ax)
    ax.set_ylabel("端到端耗时 (s)")
    ax.set_ylim(0, max(values) * 1.23)
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value + max(values) * 0.025,
            f"{value:.2f}s",
            ha="center",
            va="bottom",
            fontsize=13,
            fontweight="bold",
        )
    ax.annotate(
        f"{ratio:.1f}x slower",
        xy=(1, fine.e2e_s),
        xytext=(0.47, fine.e2e_s * 0.82),
        arrowprops={"arrowstyle": "->", "color": COLORS["slate"], "lw": 1.5},
        fontsize=13,
        fontweight="bold",
        color=COLORS["ink"],
    )
    title_and_note(
        fig,
        "调用粒度决定 GPU-backed AI_EMBED 的端到端耗时",
        "PostgreSQL 18.4 本地预演；Ray actor；真实 CUDA embedding endpoint；formal repeats 平均。",
        "边界：当前写回为 PostgreSQL JSON text，不是 PostgreSQL 18.3 内部平台或 384 维 pgvector 写回结果。",
    )
    save_figure(fig, "py_gpu_embed_invocation_granularity_20260712")


def chart_executor_endpoint_comparison(chain_avg: pd.DataFrame, multi_avg: pd.DataFrame) -> None:
    metrics = [
        ("e2e_s", "E2E", COLORS["blue"]),
        ("operator_wall_s", "operator", COLORS["orange"]),
        ("writeback_s", "writeback", COLORS["purple"]),
    ]
    executors = ["python", "ray_task", "ray_actor"]
    labels = ["Python", "Ray task", "Ray actor"]

    fig, axes = plt.subplots(1, 2, figsize=FIGSIZE, sharey=True)
    fig.subplots_adjust(left=0.08, right=0.97, top=0.80, bottom=0.24, wspace=0.12)
    sources = [
        ("single endpoint", chain_avg, "单 endpoint：Ray 与 Python 接近"),
        ("dual endpoint", multi_avg, "双 endpoint：Ray 降低 operator wall"),
    ]

    width = 0.24
    x = np.arange(len(executors))
    for ax, (_, source, panel_title) in zip(axes, sources):
        rows = [row_where(source, rows=4096, executor=executor, strategy="coalesced") for executor in executors]
        for i, (metric, name, color) in enumerate(metrics):
            values = [float(r[metric]) for r in rows]
            bars = ax.bar(x + (i - 1) * width, values, width=width, label=name, color=color)
            for bar, value in zip(bars, values):
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    value + 0.045,
                    f"{value:.2f}",
                    ha="center",
                    va="bottom",
                    fontsize=8.5,
                    color=COLORS["ink"],
                )
        ax.set_title(panel_title, fontsize=13, fontweight="bold", color=COLORS["ink"], pad=12)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=10)
        ax.set_ylim(0, 3.85)
        style_axes(ax)
    axes[0].set_ylabel("耗时 (s)")
    handles, legend_labels = axes[1].get_legend_handles_labels()
    fig.legend(handles, legend_labels, loc="lower center", frameon=False, ncol=3, bbox_to_anchor=(0.5, 0.105))
    title_and_note(
        fig,
        "single / dual endpoint 下执行方式对比",
        "4096 rows；coalesced batch；真实 CUDA embedding endpoint；formal repeats 平均。",
        "读法：Ray 的价值主要出现在多 endpoint 并发 routing 下；writeback 基本不随 executor 改变。",
    )
    save_figure(fig, "py_gpu_embed_executor_endpoint_comparison_20260712")


def chart_actor_endpoint_scaling(chain_avg: pd.DataFrame, multi_avg: pd.DataFrame) -> None:
    scenarios = [
        ("4K\nsingle", row_where(chain_avg, rows=4096, executor="ray_actor", strategy="coalesced")),
        ("4K\ndual", row_where(multi_avg, rows=4096, executor="ray_actor", strategy="coalesced")),
        ("16K\nsingle", row_where(chain_avg, rows=16384, executor="ray_actor", strategy="coalesced")),
        ("16K\ndual", row_where(multi_avg, rows=16384, executor="ray_actor", strategy="coalesced")),
    ]
    labels = [s[0] for s in scenarios]
    e2e = [float(s[1].e2e_s) for s in scenarios]
    operator = [float(s[1].operator_wall_s) for s in scenarios]
    writeback = [float(s[1].writeback_s) for s in scenarios]

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.10, right=0.95, top=0.80, bottom=0.20)
    x = np.arange(len(labels))
    width = 0.25
    bars1 = ax.bar(x - width, e2e, width=width, color=COLORS["blue"], label="E2E")
    bars2 = ax.bar(x, operator, width=width, color=COLORS["orange"], label="operator")
    bars3 = ax.bar(x + width, writeback, width=width, color=COLORS["purple"], label="writeback")
    for bars in [bars1, bars2, bars3]:
        for bar in bars:
            value = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                value + 0.14,
                f"{value:.1f}",
                ha="center",
                va="bottom",
                fontsize=9.5,
                color=COLORS["ink"],
            )
    style_axes(ax)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("耗时 (s)")
    ax.set_ylim(0, max(e2e) * 1.22)
    ax.legend(frameon=False, ncol=3, loc="upper right")
    title_and_note(
        fig,
        "Ray actor 单 / 双 endpoint 扩展对比",
        "Ray actor coalesced；4K / 16K rows；single endpoint 来自 chain breakdown，dual endpoint 来自 multi-endpoint test。",
        "读法：双 endpoint 降低 operator 阶段，但 16K 时 E2E 收益被 PostgreSQL JSON text writeback 明显限制。",
    )
    save_figure(fig, "py_gpu_embed_actor_endpoint_scaling_20260712")


def stage_values(row: pd.Series) -> dict[str, float]:
    scheduling = max(
        0.0,
        float(row.operator_wall_s)
        - float(row.model_request_wall_s)
        - float(row.submit_s)
        - float(row.bounded_wait_s)
        - float(row.fanin_s),
    )
    return {
        "DB fetch": float(row.db_fetch_s),
        "Arrow batch build": float(row.arrow_build_s),
        "Ray submit / residual": scheduling,
        "GPU request wall": float(row.model_request_wall_s),
        "fan-in": float(row.fanin_s),
        "sink writeback": float(row.writeback_s),
    }


def actor_stage_scenarios(chain_avg: pd.DataFrame, multi_avg: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    return [
        ("4K actor single endpoint", row_where(chain_avg, rows=4096, executor="ray_actor", strategy="coalesced")),
        ("4K actor dual endpoint", row_where(multi_avg, rows=4096, executor="ray_actor", strategy="coalesced")),
        ("16K actor single endpoint", row_where(chain_avg, rows=16384, executor="ray_actor", strategy="coalesced")),
        ("16K actor dual endpoint", row_where(multi_avg, rows=16384, executor="ray_actor", strategy="coalesced")),
    ]


def chart_stage_stack(chain_avg: pd.DataFrame, multi_avg: pd.DataFrame) -> None:
    scenarios = actor_stage_scenarios(chain_avg, multi_avg)
    stage_names = [
        "DB fetch",
        "Arrow batch build",
        "Ray submit / residual",
        "GPU request wall",
        "fan-in",
        "sink writeback",
    ]
    stage_colors = [
        COLORS["sky"],
        COLORS["green"],
        COLORS["amber"],
        COLORS["orange"],
        COLORS["slate"],
        COLORS["purple"],
    ]
    labels = [s[0] for s in scenarios]
    values = pd.DataFrame([stage_values(s[1]) for s in scenarios], index=labels)[stage_names]
    totals = [float(s[1].e2e_s) for s in scenarios]

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.30, right=0.94, top=0.80, bottom=0.18)
    y = np.arange(len(labels))
    left = np.zeros(len(labels))
    for stage, color in zip(stage_names, stage_colors):
        bars = ax.barh(y, values[stage], left=left, color=color, height=0.46, label=stage)
        for i, (bar, value) in enumerate(zip(bars, values[stage])):
            if value >= 0.45:
                ax.text(
                    left[i] + value / 2,
                    bar.get_y() + bar.get_height() / 2,
                    f"{value:.1f}s",
                    ha="center",
                    va="center",
                    fontsize=9.5,
                    fontweight="bold",
                    color="white",
                )
        left += values[stage].to_numpy()
    for i, total in enumerate(totals):
        ax.text(total + 0.08, y[i], f"{total:.1f}s", va="center", fontsize=11, fontweight="bold")
    ax.set_yticks(y)
    ax.set_yticklabels(labels, ha="left")
    ax.tick_params(axis="y", pad=175)
    ax.invert_yaxis()
    ax.set_xlabel("链路阶段耗时 (s)")
    ax.set_xlim(0, max(totals) * 1.16)
    style_axes(ax, xgrid=True, ygrid=False)
    ax.legend(frameon=False, ncol=3, loc="upper left", bbox_to_anchor=(0, 1.13), fontsize=9.5)
    title_and_note(
        fig,
        "数据库到 GPU 再到写回的链路阶段时延",
        "Ray actor coalesced；真实 CUDA embedding endpoint；formal repeats 平均；场景为纵轴，阶段为柱内颜色。",
        "说明：GPU 阶段使用 model_request_wall_s；model_service_s 是请求耗时求和，不作为链路阶段时延。",
    )
    save_figure(fig, "py_gpu_embed_stage_latency_stack_20260712")


def chart_stage_share(chain_avg: pd.DataFrame, multi_avg: pd.DataFrame) -> None:
    scenarios = actor_stage_scenarios(chain_avg, multi_avg)
    stage_names = [
        "DB fetch",
        "Arrow batch build",
        "Ray submit / residual",
        "GPU request wall",
        "fan-in",
        "sink writeback",
    ]
    stage_colors = [
        COLORS["sky"],
        COLORS["green"],
        COLORS["amber"],
        COLORS["orange"],
        COLORS["slate"],
        COLORS["purple"],
    ]
    labels = [s[0] for s in scenarios]
    values = pd.DataFrame([stage_values(s[1]) for s in scenarios], index=labels)[stage_names]
    shares = values.div(values.sum(axis=1), axis=0) * 100.0

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.30, right=0.94, top=0.80, bottom=0.18)
    y = np.arange(len(labels))
    left = np.zeros(len(labels))
    for stage, color in zip(stage_names, stage_colors):
        bars = ax.barh(y, shares[stage], left=left, color=color, height=0.46, label=stage)
        for i, (bar, value) in enumerate(zip(bars, shares[stage])):
            if value >= 8:
                ax.text(
                    left[i] + value / 2,
                    bar.get_y() + bar.get_height() / 2,
                    f"{value:.0f}%",
                    ha="center",
                    va="center",
                    fontsize=9.5,
                    fontweight="bold",
                    color="white",
                )
        left += shares[stage].to_numpy()
    ax.set_yticks(y)
    ax.set_yticklabels(labels, ha="left")
    ax.tick_params(axis="y", pad=175)
    ax.invert_yaxis()
    ax.set_xlabel("阶段占比 (%)")
    ax.set_xlim(0, 100)
    style_axes(ax, xgrid=True, ygrid=False)
    ax.legend(frameon=False, ncol=3, loc="upper left", bbox_to_anchor=(0, 1.13), fontsize=9.5)
    title_and_note(
        fig,
        "链路阶段占比显示收益受写回限制",
        "同一组 Ray actor coalesced 场景；100% 堆叠用于看占比，不替代绝对时延判断。",
        "边界：当前 sink 是 PostgreSQL JSON text writeback；后续 Lance / pgvector 写回需要重新画像。",
    )
    save_figure(fig, "py_gpu_embed_stage_share_20260712")


def main() -> None:
    setup_matplotlib()
    metrics = [
        "e2e_s",
        "db_fetch_s",
        "arrow_build_s",
        "model_request_wall_s",
        "operator_wall_s",
        "submit_s",
        "bounded_wait_s",
        "fanin_s",
        "writeback_s",
    ]
    chain = load_formal(CHAIN_CSV)
    multi = load_formal(MULTI_CSV)
    chain_avg = grouped_mean(chain, ["total_rows", "executor", "strategy"], metrics)
    multi_avg = grouped_mean(multi, ["total_rows", "executor", "strategy"], metrics)

    chart_invocation_granularity(chain_avg)
    chart_executor_endpoint_comparison(chain_avg, multi_avg)
    chart_actor_endpoint_scaling(chain_avg, multi_avg)
    chart_stage_stack(chain_avg, multi_avg)
    chart_stage_share(chain_avg, multi_avg)


if __name__ == "__main__":
    main()
