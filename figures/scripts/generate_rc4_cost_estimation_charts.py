#!/usr/bin/env python3
"""算子端到端代价估计实验数据图(批次 5)。

数据源(operator_cost_estimation_20260726):
- e2e_cost_model.json = e2e_cost_model_seed_20260726.json(canonical 副本)
- e2e_cost_model_seed_{20260726..20260730}.json 五个 grouped held-out seed 折
- 每折含 model.coefficients、ridge_metrics、mean_baseline_metrics

输出两张图到 figures/data/report_main/:
- rc4_cost_model_5seed_stability.png/svg     5 seed ridge vs mean baseline 稳定性
- rc4_cost_model_coefficients.png/svg        15 特征系数条形图(主 fold seed=20260726)

复现:
    .conda/pg-ai-profile/python.exe figures/scripts/generate_rc4_cost_estimation_charts.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch


COLOR_RIDGE = "#2F6FEB"
COLOR_BASELINE = "#94A3B8"
COLOR_POS = "#16A34A"        # 正系数(增加 E2E)
COLOR_NEG = "#DC2626"        # 负系数(减少 E2E)
COLOR_COUNTER = "#F59E0B"    # 反直觉系数
GRID_COLOR = "#E2E8F0"
NOTE_KW = dict(fontsize=8.5, color="#64748B", style="italic")

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["axes.edgecolor"] = "#334155"
plt.rcParams["axes.labelcolor"] = "#0F172A"
plt.rcParams["xtick.color"] = "#334155"
plt.rcParams["ytick.color"] = "#334155"
plt.rcParams["axes.grid"] = True
plt.rcParams["grid.color"] = GRID_COLOR
plt.rcParams["grid.linewidth"] = 0.6
plt.rcParams["grid.alpha"] = 0.7
plt.rcParams["axes.axisbelow"] = True


def load_seed_models(model_dir: Path) -> pd.DataFrame:
    """加载 5 个 seed JSON,返回汇总 DataFrame。"""
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
            "intercept": d["model"]["intercept"],
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Figure 1: 5-seed 稳定性对照
# ---------------------------------------------------------------------------


def make_5seed_stability_figure(df: pd.DataFrame, output_dir: Path):
    fig, axes = plt.subplots(1, 2, figsize=(14.0, 5.6))
    fig.suptitle("RC4 · 算子端到端代价估计:5-seed grouped held-out 稳定性",
                 fontsize=13, fontweight="bold", color="#0F172A", y=1.0)

    seeds = df.seed.tolist()
    x = np.arange(len(seeds))
    bar_width = 0.38

    # ---------- (a) MAE + RMSE ----------
    ax = axes[0]
    bars_r_mae = ax.bar(x - bar_width/2, df.ridge_mae, bar_width,
                         color=COLOR_RIDGE, edgecolor="#0F172A", linewidth=0.6,
                         label="ridge MAE")
    bars_b_mae = ax.bar(x + bar_width/2, df.baseline_mae, bar_width,
                         color=COLOR_BASELINE, edgecolor="#0F172A", linewidth=0.6,
                         label="mean baseline MAE")

    # 在柱顶标值
    for i, (r, b) in enumerate(zip(df.ridge_mae, df.baseline_mae)):
        ax.text(i - bar_width/2, r + 0.5, f"{r:.2f}", ha="center", fontsize=8.5, color="#0F172A")
        ax.text(i + bar_width/2, b + 0.5, f"{b:.2f}", ha="center", fontsize=8.5, color="#475569")

    # 平均线
    ridge_mae_mean = df.ridge_mae.mean()
    base_mae_mean = df.baseline_mae.mean()
    ax.axhline(ridge_mae_mean, color=COLOR_RIDGE, linestyle="--", linewidth=1, alpha=0.6)
    ax.axhline(base_mae_mean, color=COLOR_BASELINE, linestyle="--", linewidth=1, alpha=0.6)
    ax.text(0.02, 0.55,
            f"ridge 均值 MAE\n  {ridge_mae_mean:.2f}s\nbaseline 均值\n  {base_mae_mean:.2f}s\n"
            f"改善 {((base_mae_mean-ridge_mae_mean)/base_mae_mean*100):.0f}%",
            transform=ax.transAxes, fontsize=9,
            bbox=dict(boxstyle="round,pad=0.3", fc="#DBEAFE", ec=COLOR_RIDGE, lw=0.6))

    ax.set_xticks(x)
    ax.set_xticklabels([str(s) for s in seeds], fontsize=9.5)
    ax.set_xlabel("grouped held-out seed", fontsize=10)
    ax.set_ylabel("MAE (秒)", fontsize=11)
    ax.set_title("(a) MAE:5 seed 上 ridge 均显著优于 mean baseline",
                 fontsize=10.5, color="#0F172A", loc="left", pad=4)
    ax.legend(loc="upper right", fontsize=9)

    # ---------- (b) R² + MAPE ----------
    ax = axes[1]
    ax2 = ax.twinx()
    ax2.grid(False)

    bars_r2 = ax.bar(x - bar_width/2, df.ridge_r2, bar_width,
                     color=COLOR_RIDGE, edgecolor="#0F172A", linewidth=0.6,
                     label="ridge R²")
    bars_b2 = ax.bar(x + bar_width/2, df.baseline_r2, bar_width,
                     color=COLOR_BASELINE, edgecolor="#0F172A", linewidth=0.6,
                     alpha=0.5, label="baseline R²")

    # MAPE 用折线
    line_r_mape = ax2.plot(x, df.ridge_mape, "o-", color=COLOR_NEG, linewidth=2,
                            markersize=9, label="ridge MAPE (%)")
    line_b_mape = ax2.plot(x, df.baseline_mape, "s--", color="#9333EA", linewidth=1.6,
                            markersize=7, alpha=0.7, label="baseline MAPE (%)")

    for i, (r2, mape) in enumerate(zip(df.ridge_r2, df.ridge_mape)):
        ax.text(i - bar_width/2, r2 + 0.03 if r2 > 0 else r2 - 0.06,
                f"R²={r2:.3f}", ha="center", fontsize=8.5, color="#0F172A")
        ax2.text(i, mape + 5, f"{mape:.1f}%", ha="center", fontsize=8, color=COLOR_NEG)

    ridge_r2_mean = df.ridge_r2.mean()
    ridge_mape_mean = df.ridge_mape.mean()
    ax.text(0.02, 0.05,
            f"ridge 均值:\n  R² = {ridge_r2_mean:.3f}\n  MAPE = {ridge_mape_mean:.1f}%",
            transform=ax.transAxes, fontsize=9,
            bbox=dict(boxstyle="round,pad=0.3", fc="#FEF3C7", ec="#F59E0B", lw=0.6))

    ax.set_xticks(x)
    ax.set_xticklabels([str(s) for s in seeds], fontsize=9.5)
    ax.set_xlabel("grouped held-out seed", fontsize=10)
    ax.set_ylabel("R²(解释方差)", fontsize=11, color="#0F172A")
    ax2.set_ylabel("MAPE (%)", fontsize=11, color=COLOR_NEG)
    ax.set_title("(b) R² 与 MAPE:5 seed 上 R² 在 0.62–0.86 间波动,MAPE 30%–90%",
                 fontsize=10.5, color="#0F172A", loc="left", pad=4)
    ax.set_ylim(-0.15, 1.0)

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=9)

    fig.text(0.5, 0.005,
             "证据:operator_cost_estimation_20260726/e2e_cost_model_seed_{20260726..20260730}.json(5 个 grouped held-out 折)。\n"
             "数据:283 真实 profile 行、70 个配置组,按组切分防泄漏。模型:标准化 log1p Ridge(alpha=1,15 个执行前特征)。\n"
             "结论:ridge 在 5 seed 上均显著优于 mean baseline,但 MAPE 30-90% 波动 → 仅作粗粒度编排提示。"
             "Source: figures/scripts/generate_rc4_cost_estimation_charts.py",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.05, 1, 0.96])
    fig.savefig(output_dir / "rc4_cost_model_5seed_stability.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc4_cost_model_5seed_stability.svg", bbox_inches="tight")
    plt.close(fig)
    return {"ridge_mae_mean": ridge_mae_mean, "ridge_r2_mean": ridge_r2_mean,
            "ridge_mape_mean": ridge_mape_mean}


# ---------------------------------------------------------------------------
# Figure 2: 特征系数条形图
# ---------------------------------------------------------------------------


def make_coefficients_figure(df: pd.DataFrame, output_dir: Path):
    """Figure 2:主 fold(seed=20260726)的特征系数条形图。"""
    main_row = df[df.seed == 20260726].iloc[0]
    coefs = main_row.coefficients
    feature_names = main_row.feature_names

    # 按系数绝对值排序
    items = sorted(coefs.items(), key=lambda kv: kv[1])
    names = [k for k, _ in items]
    values = [v for _, v in items]

    # 中文翻译 + 标注反直觉
    name_cn = {
        "total_rows": "total_rows\n(行数)",
        "prompt_token_count": "prompt_token_count\n(prompt 总 token)",
        "completion_max_tokens": "completion_max_tokens\n(输出上限)",
        "token_budget": "token_budget\n(token 预算)",
        "packing_batch_count": "packing_batch_count\n(batch 数)",
        "batch_estimated_cost_p50": "batch_cost_p50\n(batch 中位 cost)",
        "batch_estimated_cost_p95": "batch_cost_p95\n(batch P95 cost)",
        "batch_estimated_cost_max": "batch_cost_max\n(batch 最大 cost)",
        "max_inflight_limit": "max_inflight_limit\n(K_max)",
        "flush_timeout_ms": "flush_timeout_ms",
        "flush_max_wait_ms": "flush_max_wait_ms",
        "arrival_time_scale": "arrival_time_scale\n(到达加速比)",
        "arrival_replay_enabled": "arrival_replay_enabled",
        "flush_is_adaptive": "flush_is_adaptive",
        "flush_is_immediate": "flush_is_immediate",
    }
    labels = [name_cn.get(n, n) for n in names]

    # 反直觉标记:prompt_token_count 负、arrival_time_scale 负
    counter_intuitive = {"prompt_token_count", "arrival_time_scale"}

    colors = []
    for n in names:
        if n in counter_intuitive:
            colors.append(COLOR_COUNTER)
        elif coefs[n] >= 0:
            colors.append(COLOR_POS)
        else:
            colors.append(COLOR_NEG)

    fig, ax = plt.subplots(figsize=(11.5, 7.5))
    fig.suptitle("RC4 · 代价模型特征系数(seed=20260726 主折):执行前特征对 E2E 的影响方向",
                 fontsize=12.5, fontweight="bold", color="#0F172A", y=0.98)

    y = np.arange(len(names))
    bars = ax.barh(y, values, color=colors, edgecolor="#0F172A", linewidth=0.5)

    # 在条尾标值
    for i, v in enumerate(values):
        offset = 0.012 if v >= 0 else -0.012
        ax.text(v + offset, i, f"{v:+.3f}", va="center",
                ha="left" if v >= 0 else "right", fontsize=9, color="#0F172A")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=9)
    ax.axvline(0, color="#0F172A", linewidth=0.8)
    ax.set_xlabel("标准化 Ridge 系数(对 log1p(E2E_s) 的影响)", fontsize=10.5)
    ax.set_title("正系数 = 增加 E2E;负系数 = 减少 E2E。橙色 = 反直觉特征(详见图注)",
                 fontsize=10, color="#334155", loc="left", pad=6)

    legend_elements = [
        Patch(facecolor=COLOR_POS, edgecolor="#0F172A", label="正系数(增加 E2E)"),
        Patch(facecolor=COLOR_NEG, edgecolor="#0F172A", label="负系数(减少 E2E)"),
        Patch(facecolor=COLOR_COUNTER, edgecolor="#0F172A",
              label="反直觉特征(prompt_token_count / arrival_time_scale)"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=9)

    fig.text(0.5, 0.005,
             "证据:operator_cost_estimation_20260726/e2e_cost_model_seed_20260726.json(model.coefficients)。\n"
             "反直觉解读:prompt_token_count 负系数 ≠ 更多 prompt 让 E2E 变短;"
             "而是 prompt 多的配置往往 batch_count 少(packing_batch_count 正系数 0.278),"
             "Ridge 把 batch 数的功劳部分归给了 prompt_token_count。这是小数据(283 行)Ridge 的局限,不是物理事实。\n"
             "结论:系数方向可用于编排提示,但不能直接物理解释;反直觉特征需要更多数据 + output-length 预测器补齐。",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.07, 1, 0.96])
    fig.savefig(output_dir / "rc4_cost_model_coefficients.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc4_cost_model_coefficients.svg", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="算子代价估计图(批次 5)")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    here = Path(__file__).resolve()
    project_root = Path(args.project_root) if args.project_root else here.parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "figures" / "data" / "report_main"
    output_dir.mkdir(parents=True, exist_ok=True)
    model_dir = project_root / "experiments" / "results" / "operator_cost_estimation_20260726"

    print(f"[RC4] project_root = {project_root}")
    print(f"[RC4] output_dir   = {output_dir}")

    df = load_seed_models(model_dir)
    if df.empty:
        print(f"[RC4][ERROR] 未找到 seed JSON in {model_dir}")
        return 1
    print(f"[RC4] 加载 {len(df)} 个 seed 折: {df.seed.tolist()}")

    stats1 = make_5seed_stability_figure(df, output_dir)
    print(f"[RC4] Figure 1 saved: rc4_cost_model_5seed_stability.png/svg  stats={stats1}")

    make_coefficients_figure(df, output_dir)
    print(f"[RC4] Figure 2 saved: rc4_cost_model_coefficients.png/svg")

    print("\n[RC4] 全部完成。两张图位于:", output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
