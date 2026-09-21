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
OUT_DIR = ROOT / "figures" / "data" / "generated_all_meaningful"

MOTIVATION = ROOT / "motivation" / "results"
FEASIBILITY = ROOT / "feasibility" / "results"

COLORS = {
    "blue": "#2563EB",
    "sky": "#0EA5E9",
    "green": "#22C55E",
    "orange": "#F97316",
    "amber": "#F59E0B",
    "purple": "#7C3AED",
    "red": "#EF4444",
    "slate": "#64748B",
    "gray": "#CBD5E1",
    "ink": "#1F2937",
    "muted": "#64748B",
    "grid": "#E2E8F0",
    "bg": "#F8FAFC",
}


def setup_matplotlib() -> None:
    for font_path in [
        Path("C:/Windows/Fonts/msyh.ttc"),
        Path("C:/Windows/Fonts/simhei.ttf"),
        Path("C:/Windows/Fonts/arial.ttf"),
    ]:
        if font_path.exists():
            font_manager.fontManager.addfont(str(font_path))
            plt.rcParams["font.family"] = font_manager.FontProperties(fname=str(font_path)).get_name()
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


def save(fig: plt.Figure, name: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    png = OUT_DIR / f"{name}.png"
    svg = OUT_DIR / f"{name}.svg"
    fig.savefig(png, dpi=220, bbox_inches="tight", pad_inches=0.18)
    fig.savefig(svg, bbox_inches="tight", pad_inches=0.18)
    plt.close(fig)
    print(png)
    print(svg)


def style(ax: plt.Axes, xgrid: bool = False, ygrid: bool = True) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(COLORS["grid"])
    ax.spines["bottom"].set_color(COLORS["grid"])
    if ygrid:
        ax.grid(axis="y", color=COLORS["grid"], linewidth=0.9)
    if xgrid:
        ax.grid(axis="x", color=COLORS["grid"], linewidth=0.9)
    ax.set_axisbelow(True)


def text(fig: plt.Figure, title: str, subtitle: str, note: str) -> None:
    fig.text(0.06, 0.955, title, fontsize=17, fontweight="bold")
    fig.text(0.06, 0.915, subtitle, fontsize=10.5, color=COLORS["muted"])
    fig.text(0.06, 0.035, note, fontsize=9.2, color=COLORS["muted"])


def formal(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if "benchmark" in df.columns:
        df = df[~df["benchmark"].astype(str).str.endswith("_summary")]
    if "phase" in df.columns:
        df = df[df["phase"].eq("formal")]
    elif "repeat" in df.columns:
        repeat = pd.to_numeric(df["repeat"], errors="coerce")
        df = df[repeat.gt(0)]
    if "status" in df.columns:
        df = df[df["status"].eq("ok")]
    return df.copy()


def avg(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    return df.groupby(keys, as_index=False).mean(numeric_only=True)


def pick(df: pd.DataFrame, **conds) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for k, v in conds.items():
        mask &= df[k].eq(v)
    rows = df[mask]
    if rows.empty:
        raise ValueError(f"missing row: {conds}")
    return rows.iloc[0]


def label_bars(ax: plt.Axes, bars, fmt="{:.2f}", dy=0.02, fontsize=8.5) -> None:
    ymax = ax.get_ylim()[1]
    for bar in bars:
        value = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value + ymax * dy,
            fmt.format(value),
            ha="center",
            va="bottom",
            fontsize=fontsize,
            color=COLORS["ink"],
        )


def chart_cpu_vs_gpu() -> None:
    cpu = avg(formal(MOTIVATION / "cpu" / "ai_embed_cpu_profile.csv"), ["total_rows", "executor", "strategy"])
    gpu = avg(formal(MOTIVATION / "gpu" / "ai_embed_chain_breakdown_20260712.csv"), ["total_rows", "executor", "strategy"])
    scenarios = [
        ("1024 coalesced", 1024, "coalesced"),
        ("4096 coalesced", 4096, "coalesced"),
        ("1024 fine", 1024, "fine"),
    ]
    cpu_vals = [pick(cpu, total_rows=r, executor="ray_actor", strategy=s).e2e_s for _, r, s in scenarios]
    gpu_vals = [pick(gpu, total_rows=r, executor="ray_actor", strategy=s).e2e_s for _, r, s in scenarios]
    x = np.arange(len(scenarios))
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.09, right=0.96, top=0.76, bottom=0.22)
    w = 0.34
    b1 = ax.bar(x - w / 2, cpu_vals, w, color=COLORS["slate"], label="CPU endpoint")
    b2 = ax.bar(x + w / 2, gpu_vals, w, color=COLORS["orange"], label="GPU endpoint")
    label_bars(ax, b1, "{:.1f}", dy=0.015)
    label_bars(ax, b2, "{:.1f}", dy=0.015)
    ax.set_xticks(x)
    ax.set_xticklabels([s[0] for s in scenarios])
    ax.set_ylabel("E2E 耗时 (s)")
    ax.set_ylim(0, max(max(cpu_vals), max(gpu_vals)) * 1.28)
    ax.legend(frameon=False, ncol=2, loc="lower right", bbox_to_anchor=(1.0, 1.03), borderaxespad=0)
    style(ax)
    text(
        fig,
        "CPU / GPU endpoint 对比：真实端点改变瓶颈强度",
        "Ray actor；PostgreSQL 18.4 本地预演；formal repeats 平均。",
        "边界：该图只说明真实模型端点差异，不把课题主线转成 GPU kernel 优化。",
    )
    save(fig, "all_cpu_vs_gpu_endpoint_e2e_20260712")


def chart_pg18_baseline_matrix() -> None:
    df = avg(formal(MOTIVATION / "pg18_4_fake" / "baseline_matrix.csv"), ["executor", "strategy"])
    df["label"] = df["executor"] + " / " + df["strategy"]
    df = df.sort_values("e2e_s")
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.27, right=0.96, top=0.80, bottom=0.18)
    colors = [COLORS["blue"] if s == "coalesced" else COLORS["orange"] for s in df["strategy"]]
    bars = ax.barh(df["label"], df["e2e_s"], color=colors, height=0.58)
    for bar in bars:
        v = bar.get_width()
        ax.text(v + 0.5, bar.get_y() + bar.get_height() / 2, f"{v:.1f}s", va="center", fontsize=10, fontweight="bold")
    ax.set_xlabel("E2E 耗时 (s)")
    style(ax, xgrid=True, ygrid=False)
    text(
        fig,
        "PG18.4 fake-model baseline：fine 调用会放大端到端耗时",
        "4096 rows；PostgreSQL 18.4 本地同构预演；formal repeats 平均。",
        "边界：fake-model 历史结果只用于机制信号，不可替代真实 GPU-backed 瓶颈归因。",
    )
    save(fig, "all_pg18_fake_baseline_matrix_e2e_20260712")


def chart_pgvector_scaling() -> None:
    df = avg(formal(MOTIVATION / "pg18_4_fake" / "pgvector_scaling.csv"), ["total_rows", "executor"])
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.09, right=0.96, top=0.80, bottom=0.18)
    for executor, color in [("python", COLORS["slate"]), ("ray_task", COLORS["blue"]), ("ray_actor", COLORS["orange"])]:
        sub = df[df["executor"].eq(executor)].sort_values("total_rows")
        ax.plot(sub["total_rows"], sub["e2e_s"], marker="o", linewidth=2.4, color=color, label=executor)
        for _, row in sub.iterrows():
            ax.text(row.total_rows, row.e2e_s + 0.25, f"{row.e2e_s:.1f}", ha="center", fontsize=8.5)
    ax.set_xscale("log", base=2)
    ax.set_xticks([1024, 4096, 16384])
    ax.set_xticklabels(["1K", "4K", "16K"])
    ax.set_ylabel("E2E 耗时 (s)")
    ax.set_xlabel("行数")
    ax.legend(frameon=False, ncol=3)
    style(ax)
    text(
        fig,
        "PG18.4 pgvector fake-model 规模扩展",
        "coalesced；pgvector writeback；formal repeats 平均。",
        "读法：该图用于观察规模变大后 executor 与写回成本的变化趋势，不是 GPU-backed 结果。",
    )
    save(fig, "all_pg18_pgvector_scaling_e2e_20260712")


def chart_vector_writeback() -> None:
    df = avg(formal(MOTIVATION / "pg18_4_fake" / "vector_writeback.csv"), ["writeback_mode", "write_batch_rows"])
    df["label"] = df.apply(lambda r: f"{r.writeback_mode}\n{int(r.write_batch_rows)} rows/batch", axis=1)
    df = df.sort_values(["writeback_mode", "write_batch_rows"])
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.09, right=0.96, top=0.80, bottom=0.25)
    colors = [COLORS["purple"] if m == "pgvector" else COLORS["slate"] for m in df["writeback_mode"]]
    bars = ax.bar(np.arange(len(df)), df["writeback_s"], color=colors, width=0.62)
    label_bars(ax, bars, "{:.2f}", dy=0.02)
    ax.set_xticks(np.arange(len(df)))
    ax.set_xticklabels(df["label"], fontsize=9)
    ax.set_ylabel("writeback_s (s)")
    style(ax)
    text(
        fig,
        "写回方式与批量大小决定 writeback 成本",
        "PG18.4 fake-model；4096 rows；Ray actor coalesced；formal repeats 平均。",
        "读法：连接验证不能证明性能收益；该图只用于说明写回策略需要单独作为研究变量。",
    )
    save(fig, "all_pg18_vector_writeback_batching_20260712")


def chart_fake_workload_matrix() -> None:
    df = avg(formal(MOTIVATION / "fake_cpu" / "workload_matrix.csv"), ["scenario", "strategy", "upstream", "downstream"])
    df = df[(df["upstream"] == 32) & (df["downstream"] == 32)]
    rows = []
    for scenario in sorted(df["scenario"].unique()):
        fine = pick(df, scenario=scenario, strategy="fine").end_to_end_ms
        coal = pick(df, scenario=scenario, strategy="coalesced").end_to_end_ms
        rows.append((scenario, fine, coal, fine / coal))
    labels = [r[0] for r in rows]
    ratios = [r[3] for r in rows]
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.12, right=0.96, top=0.80, bottom=0.20)
    bars = ax.bar(labels, ratios, color=[COLORS["blue"], COLORS["green"], COLORS["orange"]], width=0.55)
    label_bars(ax, bars, "{:.1f}x", dy=0.025, fontsize=11)
    ax.set_ylabel("fine / coalesced E2E 比值")
    ax.set_ylim(0, max(ratios) * 1.25)
    style(ax)
    text(
        fig,
        "三类 fake AI workload 都对粒度敏感",
        "upstream=32, downstream=32；repeat 0 作为 warm-up 排除。",
        "边界：fake workload 只说明机制假设，不能外推真实 LLM / embedding 的绝对收益。",
    )
    save(fig, "all_fake_cpu_workload_matrix_speedup_20260712")


def chart_fake_granularity() -> None:
    df = avg(formal(MOTIVATION / "fake_cpu" / "granularity.csv"), ["strategy", "upstream", "downstream"])
    df = df[(df["upstream"] == 32) & (df["downstream"] == 32)]
    order = ["fine", "two_stage_coalesced", "downstream_coalesced", "upstream_bundled"]
    df = df.set_index("strategy").loc[order].reset_index()
    fig, ax1 = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.10, right=0.90, top=0.80, bottom=0.25)
    x = np.arange(len(df))
    bars = ax1.bar(x, df["e2e_ms"], color=COLORS["blue"], width=0.55, label="e2e_ms")
    label_bars(ax1, bars, "{:.1f}", dy=0.02, fontsize=9)
    ax1.set_ylabel("E2E (ms)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["strategy"], rotation=15, ha="right")
    style(ax1)
    ax2 = ax1.twinx()
    ax2.plot(x, df["total_ray_tasks"], color=COLORS["orange"], marker="o", linewidth=2.4, label="total Ray tasks")
    ax2.set_ylabel("Ray task 数")
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_color(COLORS["grid"])
    ax1.legend(frameon=False, loc="upper left")
    ax2.legend(frameon=False, loc="upper right")
    text(
        fig,
        "粒度归因：收益不只来自 fan-in refs",
        "fake/CPU；upstream=32, downstream=32；repeat 0 作为 warm-up 排除。",
        "读法：任务数、operator invocation 和 fan-in 依赖共同影响 E2E；不能直接外推真实 GPU 服务。",
    )
    save(fig, "all_fake_cpu_granularity_attribution_20260712")


def chart_backpressure() -> None:
    df = avg(formal(MOTIVATION / "fake_cpu" / "backpressure.csv"), ["queue_limit"])
    df["limit_label"] = df["queue_limit"].apply(lambda x: "unbounded" if x == 0 else str(int(x)))
    fig, ax1 = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.10, right=0.90, top=0.80, bottom=0.20)
    x = np.arange(len(df))
    bars = ax1.bar(x, df["avg_queue_wait_ms"], color=COLORS["orange"], width=0.55, label="avg queue wait")
    label_bars(ax1, bars, "{:.0f}", dy=0.02, fontsize=9)
    ax1.set_ylabel("平均 queue wait (ms)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["limit_label"])
    ax1.set_xlabel("queue_limit")
    style(ax1)
    ax2 = ax1.twinx()
    ax2.plot(x, df["max_token_backlog"], color=COLORS["purple"], marker="o", linewidth=2.4, label="max token backlog")
    ax2.set_ylabel("max token backlog")
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_color(COLORS["grid"])
    ax1.legend(frameon=False, loc="upper left")
    ax2.legend(frameon=False, loc="upper right")
    text(
        fig,
        "backpressure 降低队列压力，但不提升模拟吞吐",
        "fake model-service queue simulation；repeat 0 作为 warm-up 排除。",
        "边界：该图支持后续验证模型服务状态感知调度，不能声称真实 Ray Serve / vLLM 一定相同。",
    )
    save(fig, "all_fake_cpu_backpressure_queue_pressure_20260712")


def chart_fake_embed_pipeline() -> None:
    df = avg(formal(MOTIVATION / "fake_cpu" / "fake_embed_pipeline.csv"), ["strategy", "upstream", "downstream"])
    df = df[df["upstream"].eq(32)]
    piv = df.pivot(index="downstream", columns="strategy", values="end_to_end_ms").sort_index()
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.10, right=0.96, top=0.80, bottom=0.18)
    x = np.arange(len(piv))
    w = 0.34
    b1 = ax.bar(x - w / 2, piv["fine"], w, color=COLORS["orange"], label="fine")
    b2 = ax.bar(x + w / 2, piv["coalesced"], w, color=COLORS["blue"], label="coalesced")
    label_bars(ax, b1, "{:.0f}", dy=0.02, fontsize=9)
    label_bars(ax, b2, "{:.0f}", dy=0.02, fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([f"downstream={int(v)}" for v in piv.index])
    ax.set_ylabel("E2E (ms)")
    ax.legend(frameon=False)
    style(ax)
    text(
        fig,
        "fake AI_EMBED pipeline：coalescing 降低端到端耗时",
        "upstream=32；repeat 0 作为 warm-up 排除。",
        "边界：这是历史预研，用于说明为什么关注 batch/object/fan-in，不作为真实 GPU 链路结论。",
    )
    save(fig, "all_fake_cpu_embed_pipeline_coalescing_20260712")


def chart_ray_arrow_fanout_fanin() -> None:
    df = avg(formal(FEASIBILITY / "ray_arrow_fanout_fanin.csv"), ["strategy", "upstream", "downstream"])
    df = df[df["upstream"].eq(32)]
    piv = df.pivot(index="downstream", columns="strategy", values="fanin_ms").sort_index()
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.10, right=0.96, top=0.80, bottom=0.18)
    x = np.arange(len(piv))
    w = 0.34
    b1 = ax.bar(x - w / 2, piv["fine"], w, color=COLORS["orange"], label="fine")
    b2 = ax.bar(x + w / 2, piv["coalesced"], w, color=COLORS["blue"], label="coalesced")
    label_bars(ax, b1, "{:.1f}", dy=0.02, fontsize=9)
    label_bars(ax, b2, "{:.1f}", dy=0.02, fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([f"downstream={int(v)}" for v in piv.index])
    ax.set_ylabel("fan-in (ms)")
    ax.legend(frameon=False)
    style(ax)
    text(
        fig,
        "Arrow RecordBatch fan-out/fan-in：对象粒度会影响 fan-in",
        "组件级 feasibility benchmark；upstream=32；repeat 0 排除。",
        "边界：组件 microbenchmark 只说明可观测系统信号，不承担开题主线性能结论。",
    )
    save(fig, "all_feasibility_ray_arrow_fanout_fanin_20260712")


def chart_ray_many_objects() -> None:
    df = avg(formal(FEASIBILITY / "ray_many_objects.csv"), ["total_mb", "objects"])
    df = df[df["total_mb"].eq(16)].sort_values("objects")
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.10, right=0.96, top=0.80, bottom=0.18)
    ax.plot(df["objects"], df["fanin_ms"], marker="o", linewidth=2.4, color=COLORS["orange"])
    for _, row in df.iterrows():
        ax.text(row.objects, row.fanin_ms + 0.6, f"{row.fanin_ms:.1f}", ha="center", fontsize=8.5)
    ax.set_xscale("log", base=2)
    ax.set_xlabel("object 数")
    ax.set_ylabel("fan-in (ms)")
    style(ax)
    text(
        fig,
        "固定总数据量下，object 数影响 Ray fan-in 成本",
        "Ray many-object feasibility benchmark；total=16 MB；repeat 0 排除。",
        "边界：该图只说明 object 管理信号，不能单独推出真实 AI workload 瓶颈。",
    )
    save(fig, "all_feasibility_ray_many_objects_fanin_20260712")


def chart_object_transfer() -> None:
    df = avg(formal(FEASIBILITY / "ray_object_transfer.csv"), ["object_type", "size_kb"])
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.10, right=0.96, top=0.80, bottom=0.18)
    for object_type, color in [("bytes", COLORS["slate"]), ("numpy", COLORS["blue"]), ("arrow", COLORS["orange"])]:
        if object_type not in set(df["object_type"]):
            continue
        sub = df[df["object_type"].eq(object_type)].sort_values("size_kb")
        ax.plot(sub["size_kb"], sub["roundtrip_mb_s"], marker="o", linewidth=2.2, label=object_type, color=color)
    ax.set_xscale("log", base=10)
    ax.set_xlabel("object size (KB)")
    ax.set_ylabel("roundtrip MB/s")
    ax.legend(frameon=False)
    style(ax)
    text(
        fig,
        "Ray object transfer：对象大小和类型影响传输吞吐",
        "组件级 feasibility benchmark；repeat 0 排除。",
        "边界：该图用于解释为什么需要控制 object 粒度，不直接代表端到端 AI 算子性能。",
    )
    save(fig, "all_feasibility_ray_object_transfer_20260712")


def chart_arrow_serialization() -> None:
    df = avg(formal(FEASIBILITY / "arrow_serialization.csv"), ["rows", "cols"])
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.10, right=0.96, top=0.80, bottom=0.18)
    for cols, color in [(4, COLORS["blue"]), (16, COLORS["orange"]), (64, COLORS["purple"])]:
        if cols not in set(df["cols"]):
            continue
        sub = df[df["cols"].eq(cols)].sort_values("rows")
        ax.plot(sub["rows"], sub["serialize_mb_s"], marker="o", linewidth=2.2, label=f"{cols} cols", color=color)
    ax.set_xscale("log", base=10)
    ax.set_xlabel("rows")
    ax.set_ylabel("serialize MB/s")
    ax.legend(frameon=False)
    style(ax)
    text(
        fig,
        "Arrow RecordBatch serialization 吞吐随规模变化",
        "组件级 feasibility benchmark；repeat 0 排除。",
        "边界：当前数据不支持把 Arrow serialization 写成 GPU-backed 主瓶颈，只能作为组件成本依据。",
    )
    save(fig, "all_feasibility_arrow_serialization_20260712")


def chart_ray_small_task() -> None:
    df = avg(formal(FEASIBILITY / "ray_small_task.csv"), ["compute", "tasks"])
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.10, right=0.96, top=0.80, bottom=0.18)
    sub = df.sort_values("tasks")
    ax.plot(sub["tasks"], sub["avg_task_ms"], marker="o", linewidth=2.4, color=COLORS["blue"], label="empty task")
    for _, row in sub.iterrows():
        ax.text(row.tasks, row.avg_task_ms + 0.004, f"{row.avg_task_ms:.3f}", ha="center", fontsize=8.5)
    ax.set_xscale("log", base=10)
    ax.set_xlabel("task 数")
    ax.set_ylabel("avg task latency (ms)")
    ax.set_ylim(0, sub["avg_task_ms"].max() * 1.35)
    ax.legend(frameon=False)
    style(ax)
    text(
        fig,
        "Ray small task：过细 task 有固定调度成本",
        "组件级 feasibility benchmark；repeat 0 排除。",
        "边界：调度成本必须放回真实 workload 中评估，不能单独作为 Ray 慢的结论。",
    )
    save(fig, "all_feasibility_ray_small_task_latency_20260712")


def chart_shuffle_simulation() -> None:
    df = avg(formal(FEASIBILITY / "shuffle_simulation.csv"), ["strategy", "upstream", "downstream"])
    df = df[df["downstream"].eq(32)].sort_values("upstream")
    fig, ax = plt.subplots(figsize=(12.8, 7.2))
    fig.subplots_adjust(left=0.10, right=0.96, top=0.80, bottom=0.18)
    for strategy, color in [("fine", COLORS["orange"]), ("coalesced", COLORS["blue"])]:
        sub = df[df["strategy"].eq(strategy)]
        ax.plot(sub["upstream"], sub["total_s"], marker="o", linewidth=2.2, label=strategy, color=color)
    ax.set_xscale("log", base=2)
    ax.set_xlabel("upstream partition 数")
    ax.set_ylabel("total_s")
    ax.legend(frameon=False)
    style(ax)
    text(
        fig,
        "本地 shuffle simulation：作为负结果 / 对照信号保留",
        "downstream=32；repeat 0 排除。",
        "边界：本地 Python shuffle 不是当前主线证据，只用于说明哪些组件信号不应过度包装。",
    )
    save(fig, "all_feasibility_shuffle_simulation_20260712")


def main() -> None:
    setup_matplotlib()
    chart_cpu_vs_gpu()
    chart_pg18_baseline_matrix()
    chart_pgvector_scaling()
    chart_vector_writeback()
    chart_fake_workload_matrix()
    chart_fake_granularity()
    chart_backpressure()
    chart_fake_embed_pipeline()
    chart_ray_arrow_fanout_fanin()
    chart_ray_many_objects()
    chart_object_transfer()
    chart_arrow_serialization()
    chart_ray_small_task()
    chart_shuffle_simulation()


if __name__ == "__main__":
    main()
