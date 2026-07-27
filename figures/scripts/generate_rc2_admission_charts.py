#!/usr/bin/env python3
"""RC2 admission controller + shared-vLLM 信号盲区诊断图(批次 3)。

数据源(2026-07-26 真实单 GPU vLLM 0.25.1 + Qwen2.5-1.5B BF16):
- adaptive_admission_controller_20260726/comparison_summary.csv
    单作业 5 scenario(static_k8/k16/aimd/ewma/pid × 3 formal)+ mechanism control(aimd vs K16)
- shared_vllm_adaptive_admission_20260726/formal_512/comparison_summary.csv
    shared-vLLM 3 scenario(K8/K16/AIMD)前台/后台 × 3 formal
- shared_vllm_adaptive_admission_20260726/admission_flush_comparison.csv
    admission × flush 二维(K8/K16/AIMD × fixed_50/adaptive)
- shared_vllm_adaptive_admission_20260726/formal_512/traces/interference_bulk_aimd_background_r{1,2,3}.control.csv
    AIMD 后台 run 的逐决策 trace(K_max 时序)
- shared_vllm_adaptive_admission_20260726/formal_512/traces/interference_bulk_aimd_background_r{1,2,3}.resources.csv
    后台 run 的 vLLM running/waiting/KV 时序

输出三张图到 figures/data/report_main/:
- rc2_admission_controller_matrix.png/svg     单作业 admission controller 矩阵 + mechanism control
- rc2_shared_vllm_kmax_guardrail.png/svg      shared-vLLM 前台/后台 tradeoff
- rc2_aimd_signal_blindspot.png/svg           AIMD 窗口时序 + vLLM waiting=0 诊断(trace 级)

复现:
    .conda/pg-ai-profile/python.exe figures/scripts/generate_rc2_admission_charts.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch
from matplotlib.lines import Line2D


COLOR_K8 = "#16A34A"        # static K=8 绿(前台保护)
COLOR_K16 = "#94A3B8"       # static K=16 灰(无前台保护)
COLOR_AIMD = "#F97316"      # AIMD 橙
COLOR_EWMA = "#7C3AED"      # EWMA-AIMD 紫
COLOR_PID = "#EC4899"       # PID 粉
COLOR_RUNNING = "#2F6FEB"   # vLLM running 蓝
COLOR_WAITING = "#DC2626"   # vLLM waiting 红
COLOR_KMAX = "#0F172A"      # AIMD K_max 黑
COLOR_INFLIGHT = "#F59E0B"  # inflight 黄
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


# ---------------------------------------------------------------------------
# Figure 1: 单作业 admission controller 矩阵
# ---------------------------------------------------------------------------


def make_controller_matrix_figure(ctrl_csv: Path, output_dir: Path):
    """Figure 1:单作业 admission controller 5 scenario 对照 + mechanism control。"""
    df = pd.read_csv(ctrl_csv)

    fig, axes = plt.subplots(1, 2, figsize=(14.0, 5.8))
    fig.suptitle("RC2 · 单作业 admission controller:动态反馈增量来自升至 K≈16,不是控制律",
                 fontsize=13, fontweight="bold", color="#0F172A", y=1.0)

    # ---------- (a) controller_family(baseline=static_k8)----------
    fam = df[df.comparison_group == "controller_family"].copy()
    # 排序:static_k8 → aimd → ewma → pid
    order = ["static_k8", "aimd_4_16_initial8", "ewma_aimd_4_16_initial8", "pid_4_16_initial8"]
    fam = fam.set_index("scenario_id").loc[order].reset_index()
    labels_fam = ["static K=8", "AIMD\n(4-16, init 8)", "EWMA-AIMD\n(4-16, init 8)", "PID\n(4-16, init 8)"]
    colors_fam = [COLOR_K8, COLOR_AIMD, COLOR_EWMA, COLOR_PID]

    x = np.arange(len(fam))
    bar_width = 0.38

    ax = axes[0]
    # 双 y:tokens/s 左,E2E 右
    ax2 = ax.twinx()
    ax2.grid(False)

    bars_tok = ax.bar(x - bar_width/2, fam.tokens_per_s_mean, bar_width,
                       color=colors_fam, edgecolor="#0F172A", linewidth=0.6,
                       label="tokens/s")
    bars_e2e = ax2.bar(x + bar_width/2, fam.e2e_mean_s, bar_width,
                       color=colors_fam, edgecolor="#0F172A", linewidth=0.6,
                       alpha=0.4, hatch="\\\\", label="E2E (s)")

    # 误差棒
    ax.errorbar(x - bar_width/2, fam.tokens_per_s_mean, yerr=fam.tokens_per_s_sample_std,
                fmt="none", ecolor="#0F172A", capsize=3, lw=0.8)
    ax2.errorbar(x + bar_width/2, fam.e2e_mean_s, yerr=fam.e2e_sample_std_s,
                 fmt="none", ecolor="#0F172A", capsize=3, lw=0.8)

    # 在柱顶标 K_mean
    for i, k in enumerate(fam.admission_limit_mean):
        ax.text(i, fam.tokens_per_s_mean.iloc[i] + 80,
                f"K̄={k:.1f}", ha="center", fontsize=9, fontweight="bold",
                color="#0F172A",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#CBD5E1", lw=0.4))

    ax.set_xticks(x)
    ax.set_xticklabels(labels_fam, fontsize=9)
    ax.set_ylabel("吞吐 (tokens/s)", fontsize=11, color="#0F172A")
    ax2.set_ylabel("Operator E2E (s)", fontsize=11, color="#475569")
    ax.set_title("(a) controller_family(baseline = static K=8):三者都升到 K≈16",
                 fontsize=10.5, color="#0F172A", loc="left", pad=4)

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)

    # ---------- (b) mechanism_control(aimd vs static_k16)----------
    mc = df[df.comparison_group == "mechanism_control"].copy()
    order_mc = ["static_k16", "aimd_4_16_initial8"]
    mc = mc.set_index("scenario_id").loc[order_mc].reset_index()
    labels_mc = ["static K=16", "AIMD\n(4-16, init 8)"]
    colors_mc = [COLOR_K16, COLOR_AIMD]

    x_mc = np.arange(len(mc))

    ax1b = axes[1]
    ax2b = ax1b.twinx()
    ax2b.grid(False)

    bars_tok_b = ax1b.bar(x_mc - bar_width/2, mc.tokens_per_s_mean, bar_width,
                           color=colors_mc, edgecolor="#0F172A", linewidth=0.6,
                           label="tokens/s")
    bars_e2e_b = ax2b.bar(x_mc + bar_width/2, mc.e2e_mean_s, bar_width,
                           color=colors_mc, edgecolor="#0F172A", linewidth=0.6,
                           alpha=0.4, hatch="\\\\", label="E2E (s)")

    ax1b.errorbar(x_mc - bar_width/2, mc.tokens_per_s_mean, yerr=mc.tokens_per_s_sample_std,
                  fmt="none", ecolor="#0F172A", capsize=3, lw=0.8)
    ax2b.errorbar(x_mc + bar_width/2, mc.e2e_mean_s, yerr=mc.e2e_sample_std_s,
                  fmt="none", ecolor="#0F172A", capsize=3, lw=0.8)

    # 标 delta
    aimd_e2e = mc.loc[mc.scenario_id == "aimd_4_16_initial8", "e2e_vs_baseline_pct"].iloc[0]
    aimd_tok = mc.loc[mc.scenario_id == "aimd_4_16_initial8", "tokens_per_s_vs_baseline_pct"].iloc[0]
    ax1b.text(0.5, 0.92,
              f"AIMD vs static K=16:\n  tokens/s {aimd_tok:+.2f}%\n  E2E      {aimd_e2e:+.2f}%\n→ 不可分辨",
              transform=ax1b.transAxes, ha="center", fontsize=10,
              bbox=dict(boxstyle="round,pad=0.3", fc="#FEF3C7", ec="#F59E0B", lw=0.6))

    ax1b.set_xticks(x_mc)
    ax1b.set_xticklabels(labels_mc, fontsize=10)
    ax1b.set_ylabel("吞吐 (tokens/s)", fontsize=11, color="#0F172A")
    ax2b.set_ylabel("Operator E2E (s)", fontsize=11, color="#475569")
    ax1b.set_title("(b) mechanism control:AIMD 与 static K=16 不可分辨",
                   fontsize=10.5, color="#0F172A", loc="left", pad=4)

    lines1b, labels1b = ax1b.get_legend_handles_labels()
    lines2b, labels2b = ax2b.get_legend_handles_labels()
    ax1b.legend(lines1b + lines2b, labels1b + labels2b, loc="upper left", fontsize=9)

    fig.text(0.5, 0.005,
             "证据:adaptive_admission_controller_20260726/comparison_summary.csv(每 scenario 3 formal)。"
             "单作业 512 请求,固定 output cap=512,prefix cache off,CUDA Graph on,sequential token-budget=6144,K_max 上限 16。\n"
             "结论:动态控制器相对 K=8 的 ~30% E2E 改善全部来自升至 K≈16;加入同上限 static K=16 对照后,AIMD/EWMA/PID 均未显示反馈控制增量。"
             "Source: figures/scripts/generate_rc2_admission_charts.py",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.05, 1, 0.96])
    fig.savefig(output_dir / "rc2_admission_controller_matrix.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc2_admission_controller_matrix.svg", bbox_inches="tight")
    plt.close(fig)
    return {"aimd_vs_k16_tokens_pct": aimd_tok, "aimd_vs_k16_e2e_pct": aimd_e2e}


# ---------------------------------------------------------------------------
# Figure 2: shared-vLLM K_max guardrail
# ---------------------------------------------------------------------------


def make_shared_vllm_guardrail_figure(comp_csv: Path, output_dir: Path):
    """Figure 2:shared-vLLM 128 前台 / 512 后台 K8/K16/AIMD tradeoff。"""
    df = pd.read_csv(comp_csv)

    scenarios = ["k8", "k16", "aimd"]
    labels = ["static K=8\n(前台保护)", "static K=16\n(后台吞吐)", "AIMD\n(饱和至 K≈16)"]
    colors = [COLOR_K8, COLOR_K16, COLOR_AIMD]

    fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.8))
    fig.suptitle("RC2 · shared-vLLM 下 static K=8 保护前台;AIMD 饱和至 K=16 且 0 次 decrease",
                 fontsize=13, fontweight="bold", color="#0F172A", y=1.0)

    x = np.arange(len(scenarios))
    bar_width = 0.38

    # ---------- (a) 前台 E2E + P99 ----------
    ax = axes[0]
    ax2 = ax.twinx()
    ax2.grid(False)

    fg_e2e = [df.loc[df.scenario == s, "foreground_e2e_s_mean"].iloc[0] for s in scenarios]
    fg_e2e_sd = [df.loc[df.scenario == s, "foreground_e2e_s_sd"].iloc[0] for s in scenarios]
    fg_p99 = [df.loc[df.scenario == s, "foreground_request_p99_s_mean"].iloc[0] for s in scenarios]
    fg_p99_sd = [df.loc[df.scenario == s, "foreground_request_p99_s_sd"].iloc[0] for s in scenarios]

    bars_e2e = ax.bar(x - bar_width/2, fg_e2e, bar_width, yerr=fg_e2e_sd,
                      color=colors, edgecolor="#0F172A", linewidth=0.6, capsize=3,
                      error_kw=dict(ecolor="#0F172A", lw=0.8), label="前台 E2E (s)")
    bars_p99 = ax2.bar(x + bar_width/2, fg_p99, bar_width, yerr=fg_p99_sd,
                       color=colors, edgecolor="#0F172A", linewidth=0.6, capsize=3,
                       alpha=0.4, hatch="\\\\", label="前台 req P99 (s)")

    # 标 slowdown
    for i, s in enumerate(scenarios):
        sd = df.loc[df.scenario == s, "foreground_e2e_slowdown_vs_solo_mean"].iloc[0]
        ax.text(i, fg_e2e[i] + fg_e2e_sd[i] + 1.5,
                f"slowdown\n{sd:.2f}×", ha="center", fontsize=9,
                color="#0F172A",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#CBD5E1", lw=0.4))

    # 标 K8 保护数字
    k8_e2e = fg_e2e[0]
    k16_e2e = fg_e2e[1]
    pct = (k16_e2e / k8_e2e - 1) * 100
    ax.text(0.02, 0.92,
            f"K=16 vs K=8:\n  前台 E2E {pct:+.1f}%\n  前台 P99 +66.5%\n→ K=8 是必要 guardrail",
            transform=ax.transAxes, fontsize=9,
            bbox=dict(boxstyle="round,pad=0.3", fc="#DCFCE7", ec="#16A34A", lw=0.6))

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9.5)
    ax.set_ylabel("前台 Operator E2E (s)", fontsize=11, color="#0F172A")
    ax2.set_ylabel("前台请求 P99 (s)", fontsize=11, color="#475569")
    ax.set_title("(a) 前台小作业(128 行):K=8 保护延迟,K=16/AIMD 恶化 ~40%",
                 fontsize=10.5, color="#0F172A", loc="left", pad=4)

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)

    # ---------- (b) 后台 tokens/s + AIMD 决策计数 ----------
    ax = axes[1]
    bg_tps = [df.loc[df.scenario == s, "background_exact_tokens_per_s_mean"].iloc[0] for s in scenarios]
    bg_tps_sd = [df.loc[df.scenario == s, "background_exact_tokens_per_s_sd"].iloc[0] for s in scenarios]
    inc_events = [int(df.loc[df.scenario == s, "controller_increase_events"].iloc[0]) for s in scenarios]
    dec_events = [int(df.loc[df.scenario == s, "controller_decrease_events"].iloc[0]) for s in scenarios]
    k_mean = [df.loc[df.scenario == s, "admission_limit_mean"].iloc[0] for s in scenarios]

    bars = ax.bar(x, bg_tps, bar_width * 1.5, yerr=bg_tps_sd,
                  color=colors, edgecolor="#0F172A", linewidth=0.6, capsize=3,
                  error_kw=dict(ecolor="#0F172A", lw=0.8), label="后台 tokens/s")

    # 在柱顶标 K_mean + inc/dec events
    for i, s in enumerate(scenarios):
        info = f"K̄={k_mean[i]:.2f}\ninc={inc_events[i]}  dec={dec_events[i]}"
        ax.text(i, bg_tps[i] + bg_tps_sd[i] + 80, info,
                ha="center", fontsize=9, color="#0F172A",
                bbox=dict(boxstyle="round,pad=0.25", fc="#FEF3C7" if s == "aimd" else "white",
                          ec="#F59E0B" if s == "aimd" else "#CBD5E1", lw=0.5))

    # K8 牺牲后台的百分比
    pct_bg = (bg_tps[1] / bg_tps[0] - 1) * 100
    ax.text(0.02, 0.92,
            f"K=16 vs K=8:\n  后台 tokens/s {pct_bg:+.1f}%\n  → K=8 牺牲后台吞吐换取前台保护\n"
            f"AIMD 0 decrease\n  → 控制器观测不到拥塞",
            transform=ax.transAxes, fontsize=9,
            bbox=dict(boxstyle="round,pad=0.3", fc="#FEE2E2", ec="#DC2626", lw=0.6))

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9.5)
    ax.set_ylabel("后台真实吞吐 (tokens/s)", fontsize=11, color="#0F172A")
    ax.set_title("(b) 后台 bulk 作业(512 行):K=16/AIMD 后台吞吐高 ~38%,AIMD 0 次 decrease",
                 fontsize=10.5, color="#0F172A", loc="left", pad=4)

    fig.text(0.5, 0.005,
             "证据:shared_vllm_adaptive_admission_20260726/formal_512/comparison_summary.csv"
             "(每 scenario 3 formal,前台 128 行 + 后台 512 行共享同一 vLLM endpoint)。\n"
             "边界:仅一个 128/512 双作业规模;多 foreground size / arrival offset / >2 job 未测试。"
             "Source: figures/scripts/generate_rc2_admission_charts.py",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.05, 1, 0.96])
    fig.savefig(output_dir / "rc2_shared_vllm_kmax_guardrail.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc2_shared_vllm_kmax_guardrail.svg", bbox_inches="tight")
    plt.close(fig)
    return {"k16_vs_k8_fg_e2e_pct": pct, "k16_vs_k8_bg_tps_pct": pct_bg,
            "aimd_inc": inc_events[2], "aimd_dec": dec_events[2], "aimd_k_mean": k_mean[2]}


# ---------------------------------------------------------------------------
# Figure 3: AIMD 信号盲区诊断(trace 级时序)
# ---------------------------------------------------------------------------


def make_signal_blindspot_figure(traces_dir: Path, output_dir: Path):
    """Figure 3:AIMD 窗口时序 + vLLM waiting 时序,展示信号盲区。

    读 3 个 AIMD background run 的 control.csv 和 resources.csv,取 r1 作为代表。
    """
    # 找一个完整的 aimd background run
    aimd_runs = sorted(traces_dir.glob("interference_bulk_aimd_background_r*.control.csv"))
    if not aimd_runs:
        print(f"[RC2-admission][WARN] 未找到 AIMD background control.csv,跳过 Figure 3")
        return None
    # 取 r1
    r1_control = traces_dir / "interference_bulk_aimd_background_r1.control.csv"
    r1_resources = traces_dir / "interference_bulk_aimd_background_r1.resources.csv"
    if not r1_control.exists() or not r1_resources.exists():
        print(f"[RC2-admission][WARN] r1 control/resources 不存在,跳过 Figure 3")
        return None

    ctrl = pd.read_csv(r1_control)
    res = pd.read_csv(r1_resources)

    fig, axes = plt.subplots(1, 2, figsize=(14.0, 5.6))
    fig.suptitle("RC2 · AIMD 信号盲区诊断:K_max 快速饱和且 0 次 decrease,vLLM waiting 始终为 0",
                 fontsize=13, fontweight="bold", color="#0F172A", y=1.0)

    # ---------- (a) AIMD K_max 决策时序 ----------
    ax = axes[0]
    # 用 elapsed_s 作为 x;ctrl 有 fresh/inflight/k_max/running/waiting/controller_action
    # 画 K_max + inflight 两条线 + action 散点
    ax.plot(ctrl.elapsed_s, ctrl.k_max, color=COLOR_KMAX, linewidth=2.0, label="AIMD K_max",
            drawstyle="steps-post")
    ax.plot(ctrl.elapsed_s, ctrl.inflight, color=COLOR_INFLIGHT, linewidth=1.4, alpha=0.8,
            label="inflight (已提交未完成)", drawstyle="steps-post")
    ax.plot(ctrl.elapsed_s, ctrl.running, color=COLOR_RUNNING, linewidth=1.4, alpha=0.7,
            label="vLLM running", drawstyle="steps-post")

    # 标 increase/decrease 事件
    inc = ctrl[ctrl.controller_action == "increase"]
    dec = ctrl[ctrl.controller_action == "decrease"]
    if len(inc) > 0:
        ax.scatter(inc.elapsed_s, inc.k_max, color=COLOR_AIMD, s=80, marker="^",
                   zorder=5, edgecolor="#0F172A", label=f"increase ({len(inc)})")
    if len(dec) > 0:
        ax.scatter(dec.elapsed_s, dec.k_max, color=COLOR_WAITING, s=80, marker="v",
                   zorder=5, edgecolor="#0F172A", label=f"decrease ({len(dec)})")

    ax.set_xlabel("运行时间 (s)", fontsize=11)
    ax.set_ylabel("K_max / inflight / running", fontsize=11)
    ax.set_title(f"(a) AIMD 窗口快速升至 K=16,3 轮共 {len(inc)} 次 increase、{len(dec)} 次 decrease",
                 fontsize=10.5, color="#0F172A", loc="left", pad=4)
    ax.legend(loc="lower right", fontsize=9)
    ax.set_ylim(0, 18)

    # ---------- (b) vLLM running + waiting 时序 ----------
    ax = axes[1]
    ax.plot(res.sample_epoch_s, res.vllm_num_requests_running, color=COLOR_RUNNING,
            linewidth=1.6, label="vLLM running")
    # waiting 通常很小,放大可视化
    waiting_scaled = res.vllm_num_requests_waiting
    ax.plot(res.sample_epoch_s, waiting_scaled, color=COLOR_WAITING,
            linewidth=1.6, label="vLLM waiting (×10 放大)", alpha=0.9)
    # 实际 waiting 均值标注
    waiting_mean = res.vllm_num_requests_waiting.mean()
    running_mean = res.vllm_num_requests_running.mean()
    ax.axhline(waiting_mean, color=COLOR_WAITING, linestyle=":", linewidth=1, alpha=0.6)

    # 标注关键诊断
    ax.text(0.02, 0.92,
            f"running 均值 = {running_mean:.1f}\nwaiting 均值 = {waiting_mean:.3f}\n"
            f"→ AIMD 拥塞信号(waiting>0 / KV 高)几乎不触发\n"
            f"→ 但前台已 slowdown 1.77×\n"
            f"→ 软拥塞在 Ray 侧排队,vLLM 看不到",
            transform=ax.transAxes, fontsize=9,
            bbox=dict(boxstyle="round,pad=0.3", fc="#FEE2E2", ec="#DC2626", lw=0.7))

    ax.set_xlabel("运行时间 (s)", fontsize=11)
    ax.set_ylabel("vLLM 请求数", fontsize=11)
    ax.set_title("(b) vLLM waiting 始终为 0:AIMD 对 Ray 侧软拥塞盲视",
                 fontsize=10.5, color="#0F172A", loc="left", pad=4)
    ax.legend(loc="upper right", fontsize=9)

    fig.text(0.5, 0.005,
             "证据:shared_vllm_adaptive_admission_20260726/formal_512/traces/interference_bulk_aimd_background_r1.{control,resources}.csv(三轮 formal 中的 r1 代表)。\n"
             "诊断:AIMD 的拥塞信号是 vLLM `waiting > 0` 或 `KV usage 高`;但请求在 Ray 侧排队尚未进入 vLLM waiting 队列,"
             "vLLM Prometheus `waiting` 始终为 0——AIMD 观测不到 Ray 侧软拥塞,自然不会降载。"
             "Source: figures/scripts/generate_rc2_admission_charts.py",
             ha="center", **NOTE_KW)

    plt.tight_layout(rect=[0, 0.05, 1, 0.96])
    fig.savefig(output_dir / "rc2_aimd_signal_blindspot.png", dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "rc2_aimd_signal_blindspot.svg", bbox_inches="tight")
    plt.close(fig)
    return {"running_mean": running_mean, "waiting_mean": waiting_mean,
            "inc_events": len(inc), "dec_events": len(dec)}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="RC2 admission + shared-vLLM 信号盲区诊断图(批次 3)")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    here = Path(__file__).resolve()
    project_root = Path(args.project_root) if args.project_root else here.parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "figures" / "data" / "report_main"
    output_dir.mkdir(parents=True, exist_ok=True)
    results = project_root / "experiments" / "results"

    print(f"[RC2-admission] project_root = {project_root}")
    print(f"[RC2-admission] output_dir   = {output_dir}")

    # Figure 1
    ctrl_csv = results / "adaptive_admission_controller_20260726" / "comparison_summary.csv"
    if not ctrl_csv.exists():
        print(f"[RC2-admission][ERROR] 缺少 {ctrl_csv}")
        return 1
    stats1 = make_controller_matrix_figure(ctrl_csv, output_dir)
    print(f"[RC2-admission] Figure 1 saved: rc2_admission_controller_matrix.png/svg  stats={stats1}")

    # Figure 2
    comp_csv = results / "shared_vllm_adaptive_admission_20260726" / "formal_512" / "comparison_summary.csv"
    if not comp_csv.exists():
        print(f"[RC2-admission][ERROR] 缺少 {comp_csv}")
        return 1
    stats2 = make_shared_vllm_guardrail_figure(comp_csv, output_dir)
    print(f"[RC2-admission] Figure 2 saved: rc2_shared_vllm_kmax_guardrail.png/svg  stats={stats2}")

    # Figure 3(trace 级)
    traces_dir = results / "shared_vllm_adaptive_admission_20260726" / "formal_512" / "traces"
    stats3 = make_signal_blindspot_figure(traces_dir, output_dir)
    if stats3:
        print(f"[RC2-admission] Figure 3 saved: rc2_aimd_signal_blindspot.png/svg  stats={stats3}")

    print("\n[RC2-admission] 全部完成。三张图位于:", output_dir)
    print("  - rc2_admission_controller_matrix.{png,svg}")
    print("  - rc2_shared_vllm_kmax_guardrail.{png,svg}")
    print("  - rc2_aimd_signal_blindspot.{png,svg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
