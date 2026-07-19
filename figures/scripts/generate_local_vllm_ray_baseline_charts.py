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
SWEEP_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_ray_static_batch_sweep_rerun_20260718.csv"
)
LATENCY_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_ray_task_batch8_latency_metrics_20260718.csv"
)
TOKEN_SWEEP_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_ray_task_batch128_token_sweep_20260719.csv"
)
TOKEN_BUDGET_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv"
)
KMAX_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_arrival_kmax_token6144_20260719.csv"
)
MATRIX_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_batch_policy_kmax_matrix_20260719.csv"
)
INTERFERENCE_SMALL_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_kmax_interference_small_20260719.csv"
)
LENGTH_PREFIX_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_length_prefix_ablation_20260719.csv"
)
INTERFERENCE_SWEEP_SMALL_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_kmax_interference_sweep_small_20260719.csv"
)
INTERFERENCE_SWEEP_BULK_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_kmax_interference_sweep_bulk_20260719.csv"
)
INTERFERENCE_ADAPTIVE_TUNED_SMALL_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_kmax_interference_adaptive_tuned_small_20260719.csv"
)
INTERFERENCE_ADAPTIVE_TUNED_BULK_CSV = (
    ROOT
    / "experiments"
    / "results"
    / "local_vllm_qwen15b_baseline"
    / "sharegpt_burstgpt_kmax_interference_adaptive_tuned_bulk_20260719.csv"
)
OUT_DIR = ROOT / "figures" / "data" / "backup"

DPI = 240
COLORS = {
    "task": "#2563EB",
    "actor": "#F97316",
    "green": "#16A34A",
    "purple": "#7C3AED",
    "cyan": "#0891B2",
    "amber": "#F59E0B",
    "rose": "#E11D48",
    "slate": "#475569",
    "muted": "#64748B",
    "grid": "#E2E8F0",
    "ink": "#111827",
    "bg": "#FFFFFF",
}


def setup_style() -> None:
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
            "svg.fonttype": "none",
            "pdf.fonttype": 42,
            "figure.facecolor": COLORS["bg"],
            "axes.facecolor": COLORS["bg"],
            "savefig.facecolor": COLORS["bg"],
            "text.color": COLORS["ink"],
            "axes.labelcolor": COLORS["ink"],
            "xtick.color": COLORS["muted"],
            "ytick.color": COLORS["muted"],
            "axes.edgecolor": COLORS["grid"],
            "axes.spines.top": False,
            "axes.spines.right": False,
            "font.size": 9,
            "axes.titlesize": 11,
            "axes.labelsize": 9,
            "legend.fontsize": 8,
        }
    )


def formal_sweep() -> pd.DataFrame:
    df = pd.read_csv(SWEEP_CSV)
    df = df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()
    numeric = [
        "ray_batch_rows",
        "rows_per_s",
        "e2e_s",
        "source_fetch_s",
        "organizer_collect_s",
        "operator_wall_s",
        "model_request_wall_s",
        "submit_s",
        "bounded_wait_s",
        "fanin_s",
        "writeback_s",
        "operator_invocations",
        "token_count",
    ]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    grouped = (
        df.groupby(["executor", "ray_batch_rows"], as_index=False)
        .agg(
            rows_per_s=("rows_per_s", "mean"),
            e2e_s=("e2e_s", "mean"),
            source_fetch_s=("source_fetch_s", "mean"),
            organizer_s=("organizer_collect_s", "mean"),
            operator_wall_s=("operator_wall_s", "mean"),
            model_request_wall_s=("model_request_wall_s", "mean"),
            submit_s=("submit_s", "mean"),
            bounded_wait_s=("bounded_wait_s", "mean"),
            fanin_s=("fanin_s", "mean"),
            writeback_s=("writeback_s", "mean"),
            operator_invocations=("operator_invocations", "mean"),
            token_count=("token_count", "mean"),
        )
        .sort_values(["executor", "ray_batch_rows"])
    )
    return grouped


def formal_latency() -> pd.DataFrame:
    df = pd.read_csv(LATENCY_CSV)
    df = df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()
    text_cols = {
        "status",
        "experiment_id",
        "phase",
        "server_version",
        "pgvector_version",
        "operator",
        "executor",
        "data_source",
        "organizer",
        "model_backend",
        "model_name",
        "source_workload_name",
        "writeback_mode",
        "vllm_metrics_status",
    }
    for col in df.columns:
        if col in text_cols:
            continue
        try:
            df[col] = pd.to_numeric(df[col])
        except (TypeError, ValueError):
            pass
    return df


def formal_token_sweep() -> pd.DataFrame:
    df = pd.read_csv(TOKEN_SWEEP_CSV)
    df = df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()
    numeric = [
        "ray_batch_rows",
        "rows_per_s",
        "e2e_s",
        "source_fetch_s",
        "organizer_collect_s",
        "submit_s",
        "bounded_wait_s",
        "fanin_s",
        "operator_wall_s",
        "model_request_wall_s",
        "operator_invocations",
        "max_inflight_seen",
        "max_inflight_limit",
        "batch_tokens_min",
        "batch_tokens_p50",
        "batch_tokens_mean",
        "batch_tokens_p95",
        "batch_tokens_max",
        "batch_service_s_p50",
        "batch_service_s_p95",
        "vllm_request_queue_time_mean_s",
        "vllm_request_inference_time_mean_s",
    ]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    grouped = (
        df.groupby("ray_batch_rows", as_index=False)
        .agg(
            rows_per_s=("rows_per_s", "mean"),
            e2e_s=("e2e_s", "mean"),
            source_fetch_s=("source_fetch_s", "mean"),
            organizer_s=("organizer_collect_s", "mean"),
            submit_s=("submit_s", "mean"),
            bounded_wait_s=("bounded_wait_s", "mean"),
            fanin_s=("fanin_s", "mean"),
            operator_wall_s=("operator_wall_s", "mean"),
            model_request_wall_s=("model_request_wall_s", "mean"),
            operator_invocations=("operator_invocations", "mean"),
            max_inflight_seen=("max_inflight_seen", "mean"),
            max_inflight_limit=("max_inflight_limit", "mean"),
            batch_tokens_min=("batch_tokens_min", "mean"),
            batch_tokens_p50=("batch_tokens_p50", "mean"),
            batch_tokens_mean=("batch_tokens_mean", "mean"),
            batch_tokens_p95=("batch_tokens_p95", "mean"),
            batch_tokens_max=("batch_tokens_max", "mean"),
            batch_service_s_p50=("batch_service_s_p50", "mean"),
            batch_service_s_p95=("batch_service_s_p95", "mean"),
            vllm_request_queue_time_mean_s=("vllm_request_queue_time_mean_s", "mean"),
            vllm_request_inference_time_mean_s=("vllm_request_inference_time_mean_s", "mean"),
        )
        .sort_values("ray_batch_rows")
    )
    grouped["token_tail_ratio"] = grouped["batch_tokens_p95"] / grouped["batch_tokens_p50"]
    grouped["service_tail_penalty_s"] = grouped["batch_service_s_p95"] - grouped["batch_service_s_p50"]
    grouped["inflight_utilization"] = grouped["max_inflight_seen"] / grouped["max_inflight_limit"]
    return grouped


def formal_token_budget_comparison() -> pd.DataFrame:
    df = pd.read_csv(TOKEN_BUDGET_CSV)
    df = df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()
    numeric = [
        "ray_batch_rows",
        "token_budget",
        "rows_per_s",
        "e2e_s",
        "operator_wall_s",
        "operator_invocations",
        "max_inflight_seen",
        "batch_rows_mean",
        "batch_tokens_p95",
        "batch_tokens_max",
        "batch_service_s_p95",
        "batch_service_s_p99",
        "vllm_request_queue_time_mean_s",
        "vllm_request_inference_time_mean_s",
    ]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["config"] = np.where(
        df["batching_policy"] == "fixed_rows",
        "fixed " + df["ray_batch_rows"].astype(int).astype(str),
        "budget " + df["token_budget"].astype(int).astype(str),
    )
    df["sort_key"] = np.where(
        df["batching_policy"] == "fixed_rows",
        df["ray_batch_rows"],
        1000 + df["token_budget"] / 1000.0,
    )
    return (
        df.groupby(["batching_policy", "config", "sort_key"], as_index=False)
        .agg(
            rows_per_s=("rows_per_s", "mean"),
            e2e_s=("e2e_s", "mean"),
            operator_wall_s=("operator_wall_s", "mean"),
            operator_invocations=("operator_invocations", "mean"),
            max_inflight_seen=("max_inflight_seen", "mean"),
            batch_rows_mean=("batch_rows_mean", "mean"),
            batch_tokens_p95=("batch_tokens_p95", "mean"),
            batch_tokens_max=("batch_tokens_max", "mean"),
            batch_service_s_p95=("batch_service_s_p95", "mean"),
            batch_service_s_p99=("batch_service_s_p99", "mean"),
            vllm_request_queue_time_mean_s=("vllm_request_queue_time_mean_s", "mean"),
            vllm_request_inference_time_mean_s=("vllm_request_inference_time_mean_s", "mean"),
        )
        .sort_values("sort_key")
    )


def formal_kmax_sweep() -> pd.DataFrame:
    df = pd.read_csv(KMAX_CSV)
    df = df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()
    numeric = [
        "max_inflight_limit",
        "rows_per_s",
        "e2e_s",
        "operator_wall_s",
        "operator_invocations",
        "max_inflight_seen",
        "bounded_wait_s",
        "batch_tokens_p95",
        "batch_service_s_p95",
        "batch_service_s_p99",
        "vllm_request_queue_time_mean_s",
        "vllm_request_inference_time_mean_s",
    ]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["k_label"] = np.where(
        df["max_inflight_limit"] >= 100000,
        "unbounded",
        df["max_inflight_limit"].astype(int).astype(str),
    )
    df["sort_key"] = np.where(df["max_inflight_limit"] >= 100000, 32, df["max_inflight_limit"])
    grouped = (
        df.groupby(["k_label", "sort_key"], as_index=False)
        .agg(
            rows_per_s=("rows_per_s", "mean"),
            e2e_s=("e2e_s", "mean"),
            operator_wall_s=("operator_wall_s", "mean"),
            operator_invocations=("operator_invocations", "mean"),
            max_inflight_seen=("max_inflight_seen", "mean"),
            bounded_wait_s=("bounded_wait_s", "mean"),
            batch_tokens_p95=("batch_tokens_p95", "mean"),
            batch_service_s_p95=("batch_service_s_p95", "mean"),
            batch_service_s_p99=("batch_service_s_p99", "mean"),
            vllm_request_queue_time_mean_s=("vllm_request_queue_time_mean_s", "mean"),
            vllm_request_inference_time_mean_s=("vllm_request_inference_time_mean_s", "mean"),
        )
        .sort_values("sort_key")
    )
    grouped["xpos"] = np.arange(len(grouped))
    return grouped


def formal_batch_kmax_matrix() -> pd.DataFrame:
    df = pd.read_csv(MATRIX_CSV)
    df = df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()
    numeric = [
        "ray_batch_rows",
        "token_budget",
        "max_inflight_limit",
        "rows_per_s",
        "e2e_s",
        "operator_wall_s",
        "model_request_wall_s",
        "operator_invocations",
        "max_inflight_seen",
        "bounded_wait_s",
        "batch_tokens_p95",
        "batch_service_s_p95",
        "batch_service_s_p99",
        "vllm_request_queue_time_mean_s",
        "vllm_request_inference_time_mean_s",
    ]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["config"] = np.where(
        df["batching_policy"] == "fixed_rows",
        "fixed " + df["ray_batch_rows"].astype(int).astype(str),
        "budget " + df["token_budget"].astype(int).astype(str),
    )
    df["policy_group"] = np.where(df["batching_policy"] == "fixed_rows", "fixed", "token")
    df["sort_key"] = np.where(
        df["batching_policy"] == "fixed_rows",
        df["ray_batch_rows"],
        1000 + df["token_budget"] / 1000.0,
    )
    df["k_label"] = np.where(
        df["max_inflight_limit"] >= 100000,
        "unbounded",
        df["max_inflight_limit"].astype(int).astype(str),
    )
    df["k_sort"] = np.where(df["max_inflight_limit"] >= 100000, 32, df["max_inflight_limit"])
    grouped = (
        df.groupby(["policy_group", "config", "sort_key", "k_label", "k_sort"], as_index=False)
        .agg(
            rows_per_s=("rows_per_s", "mean"),
            e2e_s=("e2e_s", "mean"),
            operator_wall_s=("operator_wall_s", "mean"),
            model_request_wall_s=("model_request_wall_s", "mean"),
            operator_invocations=("operator_invocations", "mean"),
            max_inflight_seen=("max_inflight_seen", "mean"),
            bounded_wait_s=("bounded_wait_s", "mean"),
            batch_tokens_p95=("batch_tokens_p95", "mean"),
            batch_service_s_p95=("batch_service_s_p95", "mean"),
            batch_service_s_p99=("batch_service_s_p99", "mean"),
            vllm_request_queue_time_mean_s=("vllm_request_queue_time_mean_s", "mean"),
            vllm_request_inference_time_mean_s=("vllm_request_inference_time_mean_s", "mean"),
        )
        .sort_values(["policy_group", "sort_key", "k_sort"])
    )
    grouped["xpos"] = grouped["k_sort"].map({2: 0, 4: 1, 8: 2, 16: 3, 32: 4})
    return grouped


def formal_interference_small_job() -> pd.DataFrame:
    df = pd.read_csv(INTERFERENCE_SMALL_CSV)
    df = df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()
    numeric = [
        "e2e_s",
        "operator_wall_s",
        "model_request_wall_s",
        "batch_service_s_p95",
        "vllm_request_queue_time_mean_s",
        "vllm_num_requests_running_after",
        "vllm_num_requests_waiting_after",
        "rows_per_s",
    ]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["scenario"] = "solo"
    df.loc[df["experiment_id"].str.contains("during_bulk_k8"), "scenario"] = "bulk K=8"
    df.loc[df["experiment_id"].str.contains("during_bulk_unbounded"), "scenario"] = "bulk unbounded"
    order = {"solo": 0, "bulk K=8": 1, "bulk unbounded": 2}
    grouped = (
        df.groupby("scenario", as_index=False)
        .agg(
            repeats=("status", "size"),
            e2e_s=("e2e_s", "mean"),
            operator_wall_s=("operator_wall_s", "mean"),
            model_request_wall_s=("model_request_wall_s", "mean"),
            batch_service_s_p95=("batch_service_s_p95", "mean"),
            vllm_request_queue_time_mean_s=("vllm_request_queue_time_mean_s", "mean"),
            vllm_num_requests_running_after=("vllm_num_requests_running_after", "mean"),
            vllm_num_requests_waiting_after=("vllm_num_requests_waiting_after", "mean"),
            rows_per_s=("rows_per_s", "mean"),
        )
        .sort_values("scenario", key=lambda s: s.map(order))
    )
    return grouped


def formal_length_prefix_ablation() -> pd.DataFrame:
    df = pd.read_csv(LENGTH_PREFIX_CSV)
    df = df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()
    numeric = [
        "rows_per_s",
        "e2e_s",
        "operator_wall_s",
        "model_request_wall_s",
        "batch_tokens_p95",
        "batch_service_s_p95",
        "vllm_request_queue_time_mean_s",
        "batch_prompt_token_spread_mean",
        "prefix_group_ratio",
    ]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    labels = {
        "ablation_fixed32": "fixed 32",
        "ablation_token6144": "token 6144",
        "ablation_length_fixed32": "length + fixed 32",
        "ablation_length_token6144": "length + token 6144",
        "ablation_prefix_fixed32": "prefix + fixed 32",
        "ablation_prefix_token6144": "prefix + token 6144",
    }
    order = {key: idx for idx, key in enumerate(labels)}
    df["config"] = df["experiment_id"].map(labels)
    df["sort_key"] = df["experiment_id"].map(order)
    grouped = (
        df.groupby(["config", "sort_key"], as_index=False)
        .agg(
            rows_per_s=("rows_per_s", "mean"),
            e2e_s=("e2e_s", "mean"),
            operator_wall_s=("operator_wall_s", "mean"),
            model_request_wall_s=("model_request_wall_s", "mean"),
            batch_tokens_p95=("batch_tokens_p95", "mean"),
            batch_service_s_p95=("batch_service_s_p95", "mean"),
            vllm_request_queue_time_mean_s=("vllm_request_queue_time_mean_s", "mean"),
            batch_prompt_token_spread_mean=("batch_prompt_token_spread_mean", "mean"),
            prefix_group_ratio=("prefix_group_ratio", "mean"),
        )
        .sort_values("sort_key")
    )
    grouped["prefix_group_pct"] = grouped["prefix_group_ratio"] * 100
    return grouped


def _interference_scenario(experiment_id: str) -> str:
    if "solo" in experiment_id:
        return "solo"
    if "bulk_k8" in experiment_id:
        return "K=8"
    if "bulk_k16" in experiment_id:
        return "K=16"
    if "bulk_unbounded" in experiment_id:
        return "unbounded"
    if "bulk_adaptive" in experiment_id:
        return "adaptive"
    return experiment_id


def formal_interference_sweep(path: Path, tuned_path: Path | None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[(df["status"] == "ok") & (df["phase"] == "formal")].copy()
    df["scenario"] = df["experiment_id"].map(_interference_scenario)
    if tuned_path is not None and tuned_path.exists():
        tuned = pd.read_csv(tuned_path)
        tuned = tuned[
            (tuned["status"] == "ok")
            & (tuned["phase"] == "formal")
            & (tuned["experiment_id"].str.contains("bulk_adaptive"))
        ].copy()
        tuned["scenario"] = "adaptive tuned"
        df = pd.concat([df[df["scenario"] != "adaptive"], tuned], ignore_index=True)
    numeric = [
        "e2e_s",
        "operator_wall_s",
        "model_request_wall_s",
        "batch_service_s_p95",
        "batch_service_s_p99",
        "vllm_request_queue_time_mean_s",
        "vllm_num_requests_running_after",
        "vllm_num_requests_waiting_after",
        "rows_per_s",
        "max_inflight_seen",
        "adaptive_downshifts",
        "adaptive_upshifts",
        "adaptive_limit_mean",
    ]
    for col in numeric:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    order = {"solo": 0, "K=8": 1, "K=16": 2, "unbounded": 3, "adaptive tuned": 4, "adaptive": 5}
    df["sort_key"] = df["scenario"].map(order)
    grouped = (
        df.groupby(["scenario", "sort_key"], as_index=False)
        .agg(
            repeats=("status", "size"),
            e2e_s=("e2e_s", "mean"),
            operator_wall_s=("operator_wall_s", "mean"),
            model_request_wall_s=("model_request_wall_s", "mean"),
            batch_service_s_p95=("batch_service_s_p95", "mean"),
            batch_service_s_p99=("batch_service_s_p99", "mean"),
            vllm_request_queue_time_mean_s=("vllm_request_queue_time_mean_s", "mean"),
            vllm_num_requests_running_after=("vllm_num_requests_running_after", "mean"),
            vllm_num_requests_waiting_after=("vllm_num_requests_waiting_after", "mean"),
            rows_per_s=("rows_per_s", "mean"),
            max_inflight_seen=("max_inflight_seen", "mean"),
            adaptive_downshifts=("adaptive_downshifts", "mean"),
            adaptive_upshifts=("adaptive_upshifts", "mean"),
            adaptive_limit_mean=("adaptive_limit_mean", "mean"),
        )
        .sort_values("sort_key")
    )
    return grouped


def style_axes(ax: plt.Axes, xgrid: bool = False, ygrid: bool = True) -> None:
    ax.spines["left"].set_color(COLORS["grid"])
    ax.spines["bottom"].set_color(COLORS["grid"])
    if ygrid:
        ax.grid(axis="y", color=COLORS["grid"], linewidth=0.8)
    if xgrid:
        ax.grid(axis="x", color=COLORS["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def batch_axis(ax: plt.Axes) -> None:
    ax.set_xscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8, 16, 32, 64, 128])
    ax.set_xticklabels(["1", "2", "4", "8", "16", "32", "64", "128"])


def executor_styles() -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    markers = {"ray_task": "o", "ray_actor": "s"}
    colors = {"ray_task": COLORS["task"], "ray_actor": COLORS["actor"]}
    labels = {"ray_task": "Ray task", "ray_actor": "Ray actor"}
    return markers, colors, labels


def save(fig: plt.Figure, stem: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for suffix in ("png", "svg"):
        path = OUT_DIR / f"{stem}.{suffix}"
        fig.savefig(path, dpi=DPI, bbox_inches="tight", pad_inches=0.14)
        print(path)
    plt.close(fig)


def plot_throughput(token_sweep: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.0, 4.3))
    ax.plot(
        token_sweep["ray_batch_rows"],
        token_sweep["rows_per_s"],
        marker="o",
        linewidth=2.1,
        color=COLORS["task"],
        label="Ray task",
    )
    batch_axis(ax)
    ax.set_xlabel("Rows per Ray invocation")
    ax.set_ylabel("Throughput (rows/s)")
    ax.set_title("Throughput plateaus after fixed row batches reach 16-32 rows")
    ax.legend(loc="upper left")
    style_axes(ax)
    fig.text(
        0.02,
        0.01,
        "Source: 2026-07-19 Ray task token-tail sweep; 512 rows, 3 formal repeats, warmup excluded; no writeback.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save(fig, "b07_local_vllm_ray_throughput")


def plot_e2e_time(token_sweep: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.0, 4.3))
    ax.plot(
        token_sweep["ray_batch_rows"],
        token_sweep["e2e_s"],
        marker="o",
        linewidth=2.1,
        color=COLORS["task"],
        label="Ray task",
    )
    batch_axis(ax)
    ax.set_yscale("log")
    ax.set_xlabel("Rows per Ray invocation")
    ax.set_ylabel("End-to-end time (s, log scale)")
    ax.set_title("End-to-end time stops improving once token tails dominate")
    ax.legend(loc="upper right")
    style_axes(ax)
    fig.text(
        0.02,
        0.01,
        "Same 2026-07-19 Ray task sweep as throughput chart; log y-axis keeps small-batch and large-batch runs readable.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save(fig, "b08_local_vllm_ray_e2e_time")


def plot_ray_task_stage_timing(token_sweep: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    x = np.arange(len(token_sweep))
    stage_cols = [
        ("source_fetch_s", "source fetch", COLORS["cyan"]),
        ("organizer_s", "Daft organize", COLORS["green"]),
        ("submit_s", "Ray submit", COLORS["purple"]),
        ("fanin_s", "Ray fan-in", COLORS["amber"]),
        ("operator_wall_s", "operator wall", COLORS["task"]),
    ]
    bottom = np.zeros(len(token_sweep))
    for col, label, color in stage_cols:
        values = token_sweep[col].to_numpy(dtype=float)
        ax.bar(x, values, bottom=bottom, color=color, label=label, width=0.72)
        bottom += values
    ax.set_xticks(x)
    ax.set_xticklabels(token_sweep["ray_batch_rows"].astype(int).astype(str))
    ax.set_xlabel("Rows per Ray invocation (Ray task)")
    ax.set_ylabel("Mean stage time (s)")
    ax.set_title("Ray task stage timing across the 1-128 fixed-row baseline")
    ax.legend(ncol=2, loc="upper right")
    style_axes(ax)
    fig.text(
        0.02,
        0.01,
        "Source: 2026-07-19 Ray task token-tail sweep. Diagnostic stacked stage times; warmup rows excluded.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save(fig, "b09_local_vllm_ray_task_stage_timing")


def plot_request_count(token_sweep: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(7.2, 6.2), sharex=True, height_ratios=[1.15, 1])
    positions = np.arange(len(token_sweep))
    labels = token_sweep["ray_batch_rows"].astype(int).astype(str)

    axes[0].bar(
        positions,
        token_sweep["operator_invocations"],
        width=0.68,
        color=COLORS["task"],
    )
    for pos, (_, row) in enumerate(token_sweep.iterrows()):
        if row["ray_batch_rows"] < 16:
            continue
        axes[0].text(
            positions[pos],
            row["operator_invocations"] + max(token_sweep["operator_invocations"]) * 0.025,
            f"{int(row['operator_invocations'])}",
            ha="center",
            va="bottom",
            fontsize=8,
            color=COLORS["slate"],
        )
    axes[0].set_ylabel("Model-service\ncalls")
    axes[0].set_title("Large fixed row batches reduce request count and can underfill in-flight slots")
    style_axes(axes[0])

    utilization_pct = token_sweep["inflight_utilization"].to_numpy(dtype=float) * 100.0
    colors = [
        COLORS["rose"] if value < 100.0 else COLORS["task"]
        for value in utilization_pct
    ]
    axes[1].bar(
        positions,
        utilization_pct,
        width=0.68,
        color=colors,
        label="in-flight window utilization",
    )
    axes[1].axhline(100, color=COLORS["grid"], linewidth=1.2)
    for pos, (_, row) in enumerate(token_sweep.iterrows()):
        value = row["inflight_utilization"] * 100.0
        label = f"{value:.0f}%"
        if row["ray_batch_rows"] >= 64:
            label += f"\n{int(row['max_inflight_seen'])}/{int(row['max_inflight_limit'])}"
        axes[1].text(
            positions[pos],
            value + 4,
            label,
            ha="center",
            va="bottom",
            fontsize=8,
            color=COLORS["slate"],
        )
    axes[1].set_xticks(positions)
    axes[1].set_xticklabels(labels)
    axes[1].set_xlabel("Rows per Ray invocation")
    axes[1].set_ylabel("Max in-flight\nutilization (%)")
    axes[1].set_ylim(0, 116)
    axes[1].legend(loc="lower left")
    style_axes(axes[1])
    fig.text(
        0.02,
        0.01,
        "Source: 2026-07-19 Ray task sweep over 512 rows. Utilization = max_inflight_seen / max_inflight_limit; max_inflight_limit=8.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save(fig, "b10_local_vllm_request_count_inflight")


def plot_token_tail_performance(token_sweep: pd.DataFrame) -> None:
    fig, axes = plt.subplots(3, 1, figsize=(7.4, 7.2), sharex=True)
    x = token_sweep["ray_batch_rows"]

    axes[0].plot(
        x,
        token_sweep["rows_per_s"],
        marker="o",
        linewidth=2.1,
        color=COLORS["task"],
    )
    axes[0].set_ylabel("Throughput\n(rows/s)")
    axes[0].set_title("Fixed row batches trade fewer calls for heavier token tails")
    best_idx = token_sweep["rows_per_s"].idxmax()
    best = token_sweep.loc[best_idx]
    axes[0].annotate(
        f"plateau near {int(best['ray_batch_rows'])} rows",
        xy=(best["ray_batch_rows"], best["rows_per_s"]),
        xytext=(best["ray_batch_rows"] / 2, best["rows_per_s"] * 0.82),
        arrowprops={"arrowstyle": "->", "color": COLORS["slate"], "linewidth": 0.9},
        color=COLORS["slate"],
        fontsize=8,
    )

    axes[1].plot(
        x,
        token_sweep["batch_tokens_p50"],
        marker="o",
        linewidth=1.8,
        color=COLORS["cyan"],
        label="token P50",
    )
    axes[1].plot(
        x,
        token_sweep["batch_tokens_p95"],
        marker="s",
        linewidth=1.8,
        color=COLORS["rose"],
        label="token P95",
    )
    axes[1].plot(
        x,
        token_sweep["batch_tokens_max"],
        marker="^",
        linewidth=1.5,
        color=COLORS["slate"],
        label="token max",
    )
    axes[1].set_ylabel("Tokens per\nmodel request")
    axes[1].legend(loc="upper left", ncol=3)

    axes[2].plot(
        x,
        token_sweep["batch_service_s_p50"],
        marker="o",
        linewidth=1.8,
        color=COLORS["green"],
        label="service P50",
    )
    axes[2].plot(
        x,
        token_sweep["batch_service_s_p95"],
        marker="s",
        linewidth=1.8,
        color=COLORS["purple"],
        label="service P95",
    )
    axes[2].bar(
        x,
        token_sweep["vllm_request_queue_time_mean_s"],
        width=x * 0.18,
        color=COLORS["amber"],
        alpha=0.55,
        label="vLLM queue mean",
    )
    axes[2].set_ylabel("Latency\n(s)")
    axes[2].set_xlabel("Rows per Ray invocation")
    axes[2].legend(loc="upper left", ncol=3)

    for ax in axes:
        batch_axis(ax)
        style_axes(ax)

    fig.text(
        0.02,
        0.01,
        "Source: 2026-07-19 Ray task token-tail sweep; 512 rows, 3 formal repeats, warmup excluded. This motivates token-budget batching; it does not evaluate that policy.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save(fig, "b11_local_vllm_token_tail_performance")


def plot_token_tail_penalty(token_sweep: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.0, 4.4))
    sizes = 42 + token_sweep["ray_batch_rows"].to_numpy(dtype=float) * 1.6
    ax.scatter(
        token_sweep["batch_tokens_p95"],
        token_sweep["batch_service_s_p95"],
        s=sizes,
        color=COLORS["task"],
        alpha=0.86,
        edgecolor="white",
        linewidth=0.8,
    )
    for _, row in token_sweep.iterrows():
        ax.text(
            row["batch_tokens_p95"] + 220,
            row["batch_service_s_p95"],
            str(int(row["ray_batch_rows"])),
            fontsize=8,
            color=COLORS["slate"],
            va="center",
        )
    ax.set_xlabel("Token tail per model request (batch_tokens_p95)")
    ax.set_ylabel("Service tail latency (batch_service_s_p95, s)")
    ax.set_title("Heavier token tails correspond to slower model-service requests")
    style_axes(ax)
    fig.text(
        0.02,
        0.01,
        "Point labels are fixed row batch sizes. Larger row batches reduce request count, but their token P95 and service P95 grow sharply.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    save(fig, "b13_local_vllm_token_tail_penalty")


def plot_service_tail_gap(token_sweep: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 4.5))
    x = token_sweep["ray_batch_rows"].to_numpy(dtype=float)
    p50 = token_sweep["batch_service_s_p50"].to_numpy(dtype=float)
    p95 = token_sweep["batch_service_s_p95"].to_numpy(dtype=float)
    gap = p95 - p50

    ax.fill_between(
        x,
        p50,
        p95,
        color=COLORS["purple"],
        alpha=0.18,
        label="tail gap (P95 - P50)",
    )
    ax.plot(x, p50, marker="o", linewidth=2.0, color=COLORS["green"], label="service P50")
    ax.plot(x, p95, marker="s", linewidth=2.0, color=COLORS["purple"], label="service P95")

    for xi, yi, gi in zip(x, p95, gap):
        if xi < 16:
            continue
        ax.text(
            xi,
            yi + max(p95) * 0.035,
            f"+{gi:.2f}s",
            ha="center",
            va="bottom",
            fontsize=8,
            color=COLORS["slate"],
        )

    batch_axis(ax)
    ax.set_xlabel("Rows per Ray invocation")
    ax.set_ylabel("Model-service request latency (s)")
    ax.set_title("Latency tail gap widens for large fixed row batches")
    ax.legend(loc="upper left", ncol=3)
    style_axes(ax)
    fig.text(
        0.02,
        0.01,
        "Source: 2026-07-19 Ray task token-tail sweep. Shaded band shows service P50-to-P95 gap across model-service requests.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    save(fig, "b14_local_vllm_service_tail_gap")


def _comparison_colors(comparison: pd.DataFrame) -> list[str]:
    return [
        COLORS["slate"] if policy == "fixed_rows" else COLORS["task"]
        for policy in comparison["batching_policy"]
    ]


def _comparison_xlabels(comparison: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    x = np.arange(len(comparison))
    labels = comparison["config"].tolist()
    return x, labels


def _add_comparison_group_background(ax: plt.Axes, comparison: pd.DataFrame) -> None:
    split_x = 3.5
    ax.axvspan(-0.5, split_x, color=COLORS["slate"], alpha=0.05, zorder=0)
    ax.axvspan(split_x, len(comparison) - 0.5, color=COLORS["task"], alpha=0.05, zorder=0)
    ax.axvline(split_x, color=COLORS["grid"], linewidth=1.4)


def _label_bars(ax: plt.Axes, bars, precision: int = 0) -> None:
    ymax = ax.get_ylim()[1]
    for patch in bars:
        value = patch.get_height()
        if value <= 0:
            continue
        label = f"{value:.{precision}f}"
        ax.text(
            patch.get_x() + patch.get_width() / 2,
            value + ymax * 0.018,
            label,
            ha="center",
            va="bottom",
            fontsize=8,
            color=COLORS["slate"],
        )


def plot_token_budget_throughput(comparison: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8.2, 4.4))
    x, labels = _comparison_xlabels(comparison)
    bars = ax.bar(x, comparison["rows_per_s"], color=_comparison_colors(comparison), width=0.68)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.set_ylabel("Throughput (rows/s)")
    ax.set_title("Token-budget throughput stays in the fixed-batch range")
    style_axes(ax)
    _add_comparison_group_background(ax, comparison)
    _label_bars(ax, bars, precision=0)
    fig.text(
        0.02,
        0.01,
        "Source: 512 rows, 3 formal repeats, warmup excluded. Same vLLM endpoint, max_inflight=8, request timeout 300s.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    save(fig, "b15_local_vllm_token_budget_throughput")


def plot_token_budget_tail_queue(comparison: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(8.2, 6.2), sharex=True)
    x, labels = _comparison_xlabels(comparison)
    colors = _comparison_colors(comparison)

    token_bars = axes[0].bar(x, comparison["batch_tokens_p95"], color=colors, width=0.68)
    axes[0].set_ylabel("Token P95\nper request")
    axes[0].set_title("Token-budget caps request tail and reduces queue pressure")

    queue_bars = axes[1].bar(x, comparison["vllm_request_queue_time_mean_s"], color=colors, width=0.68)
    axes[1].set_ylabel("vLLM queue\nmean (s)")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels, rotation=20, ha="right")

    for ax, bars, precision in [(axes[0], token_bars, 0), (axes[1], queue_bars, 3)]:
        style_axes(ax)
        _add_comparison_group_background(ax, comparison)
        _label_bars(ax, bars, precision=precision)

    fig.text(
        0.02,
        0.01,
        "Reading: fixed 64/128 keep throughput high but create large token tails and visible queue time. Token budgets bound token P95; K_max still needs tuning.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    save(fig, "b16_local_vllm_token_budget_tail_queue")


def plot_arrival_kmax_sweep(kmax: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(8.0, 6.0), sharex=True)
    x = kmax["xpos"].to_numpy(dtype=float)
    labels = kmax["k_label"].tolist()

    axes[0].plot(
        x,
        kmax["rows_per_s"],
        marker="o",
        linewidth=2.1,
        color=COLORS["task"],
    )
    axes[0].set_ylabel("Throughput\n(rows/s)")
    axes[0].set_title("K_max=8 reaches peak throughput before queue time jumps")

    axes[1].plot(
        x,
        kmax["vllm_request_queue_time_mean_s"],
        marker="s",
        linewidth=2.1,
        color=COLORS["rose"],
    )
    axes[1].set_ylabel("vLLM queue\nmean (s)")
    axes[1].set_xlabel("K_max / max in-flight Ray submissions")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels)

    best_idx = int(kmax["rows_per_s"].idxmax())
    best_x = float(kmax.loc[best_idx, "xpos"])
    for ax in axes:
        ax.axvline(best_x, color=COLORS["grid"], linewidth=1.4)
        style_axes(ax)

    fig.text(
        0.02,
        0.01,
        "Source: arrival_time order, token_budget=6144, 512 rows, 3 formal repeats, warmup excluded. Unbounded uses max_inflight=100000 and submits all 13 requests.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    save(fig, "b17_local_vllm_arrival_kmax_sweep")


def _plot_matrix_lines(
    ax: plt.Axes,
    matrix: pd.DataFrame,
    group: str,
    metric: str,
    ylabel: str,
) -> None:
    labels = ["2", "4", "8", "16", "unbounded"]
    palette = [COLORS["task"], COLORS["actor"], COLORS["green"]]
    markers = ["o", "s", "^"]
    subset = matrix[matrix["policy_group"] == group].copy()
    for i, (config, part) in enumerate(subset.groupby("config", sort=False)):
        part = part.sort_values("xpos")
        ax.plot(
            part["xpos"],
            part[metric],
            marker=markers[i % len(markers)],
            linewidth=2.0,
            color=palette[i % len(palette)],
            label=config,
        )
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels)
    ax.set_ylabel(ylabel)
    style_axes(ax)


def plot_batch_kmax_e2e(matrix: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(9.0, 3.8), sharey=True)
    _plot_matrix_lines(axes[0], matrix, "fixed", "e2e_s", "End-to-end time (s)")
    _plot_matrix_lines(axes[1], matrix, "token", "e2e_s", "End-to-end time (s)")
    axes[0].set_title("Fixed rows: K_max helps until requests are exhausted")
    axes[1].set_title("Token budget: K_max must match request granularity")
    for ax in axes:
        ax.set_xlabel("K_max / max in-flight Ray submissions")
        ax.legend(frameon=False, loc="upper right")
    fig.text(
        0.02,
        0.01,
        "Source: arrival_time order, 512 rows, 3 formal repeats, warmup excluded. Too small K_max waits in Ray; too large K_max mainly shifts waiting into vLLM.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    save(fig, "b18_local_vllm_batch_kmax_e2e")


def plot_batch_kmax_service_pressure(matrix: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(9.2, 6.0), sharex=True)
    _plot_matrix_lines(
        axes[0, 0],
        matrix,
        "fixed",
        "vllm_request_queue_time_mean_s",
        "vLLM queue mean (s)",
    )
    _plot_matrix_lines(
        axes[0, 1],
        matrix,
        "token",
        "vllm_request_queue_time_mean_s",
        "vLLM queue mean (s)",
    )
    _plot_matrix_lines(
        axes[1, 0],
        matrix,
        "fixed",
        "batch_service_s_p95",
        "Batch service P95 (s)",
    )
    _plot_matrix_lines(
        axes[1, 1],
        matrix,
        "token",
        "batch_service_s_p95",
        "Batch service P95 (s)",
    )
    axes[0, 0].set_title("Fixed rows: queue appears when K_max reaches all requests")
    axes[0, 1].set_title("Token budget: more small requests need admission control")
    axes[1, 0].set_xlabel("K_max / max in-flight Ray submissions")
    axes[1, 1].set_xlabel("K_max / max in-flight Ray submissions")
    for ax in axes.ravel():
        ax.legend(frameon=False, loc="upper left")
    fig.text(
        0.02,
        0.01,
        "Reading: increasing K_max removes Ray-side bounded wait, but queue mean and service P95 rise once vLLM receives too many simultaneous requests.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    save(fig, "b19_local_vllm_batch_kmax_service_pressure")


def plot_batch_kmax_request_granularity(matrix: pd.DataFrame) -> None:
    base = (
        matrix[matrix["k_label"] == "unbounded"]
        .sort_values(["policy_group", "sort_key"])
        .copy()
    )
    x = np.arange(len(base))
    labels = base["config"].tolist()

    fig, axes = plt.subplots(1, 2, figsize=(9.0, 3.8))
    colors = [COLORS["slate"] if g == "fixed" else COLORS["task"] for g in base["policy_group"]]
    bars = axes[0].bar(x, base["operator_invocations"], color=colors, width=0.62)
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(labels, rotation=25, ha="right")
    axes[0].set_ylabel("Ray submissions / run")
    axes[0].set_title("Batch shape determines how many requests K_max can control")
    _label_bars(axes[0], bars, precision=0)
    style_axes(axes[0])

    bars = axes[1].bar(x, base["batch_tokens_p95"], color=colors, width=0.62)
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels, rotation=25, ha="right")
    axes[1].set_ylabel("Batch token P95")
    axes[1].set_title("Fixed-row batches expose much larger token tails")
    _label_bars(axes[1], bars, precision=0)
    style_axes(axes[1])

    fig.text(
        0.02,
        0.01,
        "Source: unbounded row from the same matrix. K_max cannot help after it exceeds the number of upstream submissions; token cost still differs sharply by policy.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    save(fig, "b20_local_vllm_batch_kmax_request_granularity")


def plot_kmax_interference_small_job(interference: pd.DataFrame) -> None:
    labels = interference["scenario"].tolist()
    x = np.arange(len(labels))
    colors = [COLORS["muted"], COLORS["task"], COLORS["rose"]]

    fig, axes = plt.subplots(1, 3, figsize=(9.2, 3.6))
    metrics = [
        ("e2e_s", "Small-job E2E (s)", 1),
        ("batch_service_s_p95", "Small-job service P95 (s)", 1),
        ("vllm_request_queue_time_mean_s", "vLLM queue mean (s)", 3),
    ]
    for ax, (metric, ylabel, precision) in zip(axes, metrics):
        bars = ax.bar(x, interference[metric], color=colors, width=0.62)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=20, ha="right")
        ax.set_ylabel(ylabel)
        _label_bars(ax, bars, precision=precision)
        style_axes(ax)
    axes[0].set_title("Shared vLLM: unbounded bulk hurts the foreground job")
    axes[1].set_title("Tail service time rises")
    axes[2].set_title("Queue appears only under unbounded bulk")

    fig.text(
        0.02,
        0.01,
        "Source: foreground 128-row AI_COMPLETE job, 3 repeats. A 1024-row background job shares the same vLLM endpoint; bounded uses K_max=8, unbounded submits all 64 background requests.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    save(fig, "b21_local_vllm_kmax_interference_small_job")


def plot_length_prefix_tail(ablation: pd.DataFrame) -> None:
    labels = ablation["config"].tolist()
    x = np.arange(len(labels))
    colors = [
        COLORS["slate"],
        COLORS["task"],
        COLORS["amber"],
        COLORS["green"],
        COLORS["purple"],
        COLORS["cyan"],
    ]

    fig, axes = plt.subplots(1, 2, figsize=(9.2, 4.1))
    token_bars = axes[0].bar(x, ablation["batch_tokens_p95"], color=colors, width=0.65)
    service_bars = axes[1].bar(x, ablation["batch_service_s_p95"], color=colors, width=0.65)

    axes[0].set_ylabel("Token P95 per request")
    axes[0].set_title("Token-budget caps the request token tail")
    axes[1].set_ylabel("Batch service P95 (s)")
    axes[1].set_title("Service tail must be checked separately")

    for ax, bars, precision in [(axes[0], token_bars, 0), (axes[1], service_bars, 2)]:
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=24, ha="right")
        style_axes(ax)
        _label_bars(ax, bars, precision=precision)

    fig.text(
        0.02,
        0.01,
        "Source: 512-row arrival-order ShareGPT/BurstGPT workload, 3 formal repeats. Length-align alone can concentrate long prompts; combine it with a token budget before treating it as a positive policy.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.09, 1, 1))
    save(fig, "b22_local_vllm_length_prefix_tail")


def plot_length_prefix_signal(ablation: pd.DataFrame) -> None:
    labels = ablation["config"].tolist()
    x = np.arange(len(labels))
    colors = [
        COLORS["slate"],
        COLORS["task"],
        COLORS["amber"],
        COLORS["green"],
        COLORS["purple"],
        COLORS["cyan"],
    ]

    fig, axes = plt.subplots(1, 2, figsize=(9.2, 4.1))
    spread_bars = axes[0].bar(x, ablation["batch_prompt_token_spread_mean"], color=colors, width=0.65)
    prefix_bars = axes[1].bar(x, ablation["prefix_group_pct"], color=colors, width=0.65)

    axes[0].set_ylabel("Mean within-batch\nprompt token spread")
    axes[0].set_title("Length-align reduces within-batch length spread")
    axes[1].set_ylabel("Same-prefix adjacent pairs (%)")
    axes[1].set_title("Prefix locality is weak in this workload")

    for ax, bars, precision in [(axes[0], spread_bars, 0), (axes[1], prefix_bars, 1)]:
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=24, ha="right")
        style_axes(ax)
        _label_bars(ax, bars, precision=precision)

    fig.text(
        0.02,
        0.01,
        "Reading: these are organization signals, not cache-hit proof. Prefix-aware needs APC/cache metrics or a workload with controlled prefix-share ratios before claiming KV-cache benefit.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.09, 1, 1))
    save(fig, "b23_local_vllm_length_prefix_signal")


def plot_interference_sweep_small_job(small: pd.DataFrame) -> None:
    labels = small["scenario"].tolist()
    x = np.arange(len(labels))
    colors = [COLORS["muted"], COLORS["task"], COLORS["amber"], COLORS["rose"], COLORS["green"]]

    fig, axes = plt.subplots(1, 3, figsize=(9.6, 3.8))
    metrics = [
        ("e2e_s", "Foreground E2E (s)", 1),
        ("batch_service_s_p95", "Foreground service P95 (s)", 2),
        ("vllm_request_queue_time_mean_s", "vLLM queue mean (s)", 3),
    ]
    for ax, (metric, ylabel, precision) in zip(axes, metrics):
        bars = ax.bar(x, small[metric], color=colors[: len(labels)], width=0.64)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=24, ha="right")
        ax.set_ylabel(ylabel)
        style_axes(ax)
        _label_bars(ax, bars, precision=precision)

    axes[0].set_title("Foreground slows under shared service")
    axes[1].set_title("Tail service time exposes interference")
    axes[2].set_title("Queue appears at higher background inflight")
    fig.text(
        0.02,
        0.01,
        "Source: foreground 128-row job while a 1024-row background job shares one vLLM endpoint. 3 formal repeats; no writeback. Adaptive uses min=8, max=16 and running-threshold=64.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    save(fig, "b24_local_vllm_interference_sweep_small_job")


def plot_interference_sweep_bulk_tradeoff(bulk: pd.DataFrame) -> None:
    labels = bulk["scenario"].tolist()
    x = np.arange(len(labels))
    colors = [COLORS["task"], COLORS["amber"], COLORS["rose"], COLORS["green"]]

    fig, axes = plt.subplots(1, 3, figsize=(9.6, 3.8))
    metrics = [
        ("rows_per_s", "Bulk throughput (rows/s)", 1),
        ("batch_service_s_p95", "Bulk service P95 (s)", 2),
        ("vllm_request_queue_time_mean_s", "vLLM queue mean (s)", 3),
    ]
    for ax, (metric, ylabel, precision) in zip(axes, metrics):
        bars = ax.bar(x, bulk[metric], color=colors[: len(labels)], width=0.64)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=24, ha="right")
        ax.set_ylabel(ylabel)
        style_axes(ax)
        _label_bars(ax, bars, precision=precision)

    axes[0].set_title("Bulk throughput barely improves")
    axes[1].set_title("Higher inflight raises service tail")
    axes[2].set_title("Queue pressure grows")
    fig.text(
        0.02,
        0.01,
        "Reading: K=16/unbounded do not materially improve bulk throughput over K=8, and tuned adaptive triggers downshift but is not yet better than static K=8.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    save(fig, "b25_local_vllm_interference_sweep_bulk_tradeoff")


def plot_latency_probe(latency: pd.DataFrame) -> None:
    means = latency.mean(numeric_only=True)
    fig, ax = plt.subplots(figsize=(8.0, 4.4))
    names = [
        "client\nP50",
        "client\nP95",
        "client\nP99",
        "server\nqueue",
        "server\nprefill",
        "server\ndecode",
        "server\ninference",
        "server\ne2e",
    ]
    values = [
        means["batch_service_s_p50"],
        means["batch_service_s_p95"],
        means["batch_service_s_p99"],
        means["vllm_request_queue_time_mean_s"],
        means["vllm_request_prefill_time_mean_s"],
        means["vllm_request_decode_time_mean_s"],
        means["vllm_request_inference_time_mean_s"],
        means["vllm_e2e_request_latency_mean_s"],
    ]
    colors = [
        COLORS["task"],
        COLORS["purple"],
        COLORS["rose"],
        COLORS["muted"],
        COLORS["green"],
        COLORS["amber"],
        COLORS["cyan"],
        COLORS["actor"],
    ]
    ax.bar(names, values, color=colors, width=0.68)
    ax.axvline(2.5, color=COLORS["grid"], linewidth=1.2)
    ymax = max(values)
    ax.text(1, ymax * 1.08, "client batch latency", ha="center", fontsize=8, color=COLORS["muted"])
    ax.text(5.5, ymax * 1.08, "vLLM server metrics", ha="center", fontsize=8, color=COLORS["muted"])
    ax.set_ylim(0, ymax * 1.18)
    ax.set_ylabel("Latency / mean request time (s)")
    ax.set_title("Latency probe: client batch quantiles and vLLM server means")
    for i, value in enumerate(values):
        ax.text(i, value + ymax * 0.02, f"{value:.3f}", ha="center", va="bottom", fontsize=8)
    style_axes(ax)
    fig.text(
        0.02,
        0.01,
        "Batch=8 latency probe; 3 formal repeats. Client P95/P99 are batch-level probe statistics; server values are vLLM /metrics means.",
        fontsize=8,
        color=COLORS["muted"],
    )
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    save(fig, "b12_local_vllm_latency_probe_breakdown")


def main() -> None:
    setup_style()
    sweep = formal_sweep()
    latency = formal_latency()
    token_sweep = formal_token_sweep()
    token_budget = formal_token_budget_comparison()
    kmax = formal_kmax_sweep()
    matrix = formal_batch_kmax_matrix()
    interference = formal_interference_small_job()
    length_prefix = formal_length_prefix_ablation()
    interference_sweep_small = formal_interference_sweep(
        INTERFERENCE_SWEEP_SMALL_CSV, INTERFERENCE_ADAPTIVE_TUNED_SMALL_CSV
    )
    interference_sweep_bulk = formal_interference_sweep(
        INTERFERENCE_SWEEP_BULK_CSV, INTERFERENCE_ADAPTIVE_TUNED_BULK_CSV
    )
    plot_throughput(token_sweep)
    plot_e2e_time(token_sweep)
    plot_ray_task_stage_timing(token_sweep)
    plot_request_count(token_sweep)
    plot_token_tail_performance(token_sweep)
    plot_latency_probe(latency)
    plot_token_tail_penalty(token_sweep)
    plot_service_tail_gap(token_sweep)
    plot_token_budget_throughput(token_budget)
    plot_token_budget_tail_queue(token_budget)
    plot_arrival_kmax_sweep(kmax)
    plot_batch_kmax_e2e(matrix)
    plot_batch_kmax_service_pressure(matrix)
    plot_batch_kmax_request_granularity(matrix)
    plot_kmax_interference_small_job(interference)
    plot_length_prefix_tail(length_prefix)
    plot_length_prefix_signal(length_prefix)
    plot_interference_sweep_small_job(interference_sweep_small)
    plot_interference_sweep_bulk_tradeoff(interference_sweep_bulk)


if __name__ == "__main__":
    main()
