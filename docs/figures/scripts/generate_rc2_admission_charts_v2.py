#!/usr/bin/env python3
"""RC2 admission + shared-vLLM charts v2 (clean rebuild: zero in-panel floating annotations)."""

from __future__ import annotations
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from _scipilot_helpers import (COLOR_AIMD, COLOR_EWMA, COLOR_K16, COLOR_K8, COLOR_PID,
    COLOR_RUNNING, COLOR_WAITING, COLOR_INFLIGHT, COLOR_KMAX,
    add_panel_labels, export_figure, notes_caption, overlay_stripplot, setup_style)


def make_controller_matrix(ctrl_csv, out):
    df = pd.read_csv(ctrl_csv)
    fam = df[df.comparison_group == "controller_family"].copy()
    mc = df[df.comparison_group == "mechanism_control"].copy()
    fam_order = ["static_k8", "aimd_4_16_initial8", "ewma_aimd_4_16_initial8", "pid_4_16_initial8"]
    mc_order = ["static_k16", "aimd_4_16_initial8"]
    fam = fam.set_index("scenario_id").loc[fam_order].reset_index()
    mc = mc.set_index("scenario_id").loc[mc_order].reset_index()
    fam_labels = ["static K=8", "AIMD\n(4-16)", "EWMA\n-AIMD", "PID"]
    mc_labels = ["static K=16", "AIMD\n(4-16)"]
    fam_colors = [COLOR_K8, COLOR_AIMD, COLOR_EWMA, COLOR_PID]
    mc_colors = [COLOR_K16, COLOR_AIMD]

    fig, axes = plt.subplots(2, 2, figsize=(10, 6.0), gridspec_kw={"hspace": 0.3, "wspace": 0.25})
    fig.suptitle("Single-job admission: gain comes from rising to K~16, not control law (n=3 formal)", fontsize=7.5, fontweight="bold", y=0.98)
    bw = 0.55
    # (a) tokens/s family
    ax = axes[0, 0]; x = np.arange(len(fam))
    ax.bar(x, fam.tokens_per_s_mean, bw, yerr=fam.tokens_per_s_sample_std, color=fam_colors, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
    ax.set_xticks(x); ax.set_xticklabels(fam_labels, fontsize=7); ax.set_ylabel("Throughput (tokens/s)", fontsize=7.5)
    # [title removed to prevent overlap] Family (baseline=K=8): all reach K~16", fontsize=7, color="#374151", loc="left", pad=3)
    # (b) tokens/s mechanism
    ax = axes[0, 1]; x = np.arange(len(mc))
    ax.bar(x, mc.tokens_per_s_mean, bw, yerr=mc.tokens_per_s_sample_std, color=mc_colors, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
    ax.set_xticks(x); ax.set_xticklabels(mc_labels, fontsize=7); ax.set_ylabel("Throughput (tokens/s)", fontsize=7.5)
    # [title removed to prevent overlap] Mechanism control: AIMD vs K=16 = -0.69%", fontsize=7, color="#374151", loc="left", pad=3)
    # (c) E2E family
    ax = axes[1, 0]; x = np.arange(len(fam))
    ax.bar(x, fam.e2e_mean_s, bw, yerr=fam.e2e_sample_std_s, color=fam_colors, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
    ax.set_xticks(x); ax.set_xticklabels(fam_labels, fontsize=7); ax.set_ylabel("Operator E2E (s)", fontsize=7.5)
    # [title removed to prevent overlap] Family E2E", fontsize=7, color="#374151", loc="left", pad=3)
    # (d) E2E mechanism
    ax = axes[1, 1]; x = np.arange(len(mc))
    ax.bar(x, mc.e2e_mean_s, bw, yerr=mc.e2e_sample_std_s, color=mc_colors, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
    ax.set_xticks(x); ax.set_xticklabels(mc_labels, fontsize=7); ax.set_ylabel("Operator E2E (s)", fontsize=7.5)
    # [title removed to prevent overlap] AIMD vs K=16 E2E = +0.66% (indistinguishable)", fontsize=7, color="#374151", loc="left", pad=3)

    add_panel_labels(fig, style="parens")
    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. Left=family (baseline K=8); Right=...", fontsize=7.5)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc2_admission_controller_matrix_v2", out)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc2_shared_vllm_kmax_guardrail_v2", out)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "rc2_aimd_signal_blindspot_v2", out)
    plt.close(fig); return paths


def make_shared_vllm(comp_csv, out):
    df = pd.read_csv(comp_csv)
    scenarios = ["k8", "k16", "aimd"]
    labels = ["static K=8", "static K=16", "AIMD"]
    colors = [COLOR_K8, COLOR_K16, COLOR_AIMD]
    fig, axes = plt.subplots(2, 2, figsize=(10, 6.0), gridspec_kw={"hspace": 0.3, "wspace": 0.25})
    fig.suptitle("Shared-vLLM: K=8 protects foreground; AIMD saturates with 0 decreases (n=3)", fontsize=7.5, fontweight="bold", y=0.98)
    bw = 0.5; x = np.arange(3)

    ax = axes[0, 0]
    ax.bar(x, [df.loc[df.scenario==s,"foreground_e2e_s_mean"].iloc[0] for s in scenarios], bw,
           yerr=[df.loc[df.scenario==s,"foreground_e2e_s_sd"].iloc[0] for s in scenarios],
           color=colors, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
    ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=7); ax.set_ylabel("Foreground E2E (s)", fontsize=7.5)
    # [title removed to prevent overlap] FG E2E: K=8 -28% vs K=16", fontsize=7, color="#374151", loc="left", pad=3)

    ax = axes[0, 1]
    ax.bar(x, [df.loc[df.scenario==s,"foreground_request_p99_s_mean"].iloc[0] for s in scenarios], bw,
           yerr=[df.loc[df.scenario==s,"foreground_request_p99_s_sd"].iloc[0] for s in scenarios],
           color=colors, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
    ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=7); ax.set_ylabel("Foreground P99 (s)", fontsize=7.5)
    # [title removed to prevent overlap] FG P99: K=8 -40% vs K=16", fontsize=7, color="#374151", loc="left", pad=3)

    ax = axes[1, 0]
    ax.bar(x, [df.loc[df.scenario==s,"background_exact_tokens_per_s_mean"].iloc[0] for s in scenarios], bw,
           yerr=[df.loc[df.scenario==s,"background_exact_tokens_per_s_sd"].iloc[0] for s in scenarios],
           color=colors, edgecolor="#111827", linewidth=0.5, capsize=2.5, error_kw=dict(ecolor="#111827", lw=0.6))
    ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=7); ax.set_ylabel("Background throughput (tokens/s)", fontsize=7.5)
    # [title removed to prevent overlap] BG throughput: K=8 -28% vs K=16 (tradeoff)", fontsize=7, color="#374151", loc="left", pad=3)

    ax = axes[1, 1]
    ax.bar(x, [df.loc[df.scenario==s,"foreground_e2e_slowdown_vs_solo_mean"].iloc[0] for s in scenarios], bw,
           color=colors, edgecolor="#111827", linewidth=0.5)
    ax.axhline(1.0, color="#9CA3AF", linestyle=":", linewidth=0.8)
    ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=7); ax.set_ylabel("FG slowdown vs solo (x)", fontsize=7.5)
    # [title removed to prevent overlap] FG slowdown: AIMD 1.77x (0 decreases)", fontsize=7, color="#374151", loc="left", pad=3)

    add_panel_labels(fig, style="parens")
    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. AIMD: 0 decreases, window sat...", fontsize=7.5)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "make_shared_vllm_fig", out)
    plt.close(fig); return paths


def make_signal_blindspot(traces_dir, out):
    rc = traces_dir / "interference_bulk_aimd_background_r1.control.csv"
    rr = traces_dir / "interference_bulk_aimd_background_r1.resources.csv"
    if not rc.exists() or not rr.exists():
        print("[WARN] r1 trace missing, skip"); return
    ctrl = pd.read_csv(rc); res = pd.read_csv(rr)
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8), gridspec_kw={"wspace": 0.25})
    fig.suptitle("AIMD signal blindspot: K_max saturates; vLLM waiting stays 0", fontsize=7.5, fontweight="bold", y=0.99)
    ax = axes[0]
    ax.plot(ctrl.elapsed_s, ctrl.k_max, color=COLOR_KMAX, linewidth=1.5, label="K_max", drawstyle="steps-post")
    ax.plot(ctrl.elapsed_s, ctrl.inflight, color=COLOR_INFLIGHT, linewidth=1.0, alpha=0.7, label="inflight", drawstyle="steps-post")
    ax.plot(ctrl.elapsed_s, ctrl.running, color=COLOR_RUNNING, linewidth=1.0, alpha=0.7, label="running", drawstyle="steps-post")
    inc = ctrl[ctrl.controller_action=="increase"]; dec = ctrl[ctrl.controller_action=="decrease"]
    if len(inc)>0: ax.scatter(inc.elapsed_s, inc.k_max, color=COLOR_AIMD, s=40, marker="^", zorder=5, label=f"increase ({len(inc)})")
    if len(dec)>0: ax.scatter(dec.elapsed_s, dec.k_max, color=COLOR_WAITING, s=40, marker="v", zorder=5, label=f"decrease ({len(dec)})")
    ax.set_xlabel("Elapsed (s)", fontsize=7.5); ax.set_ylabel("K_max / inflight / running", fontsize=7.5)
    ax.legend(fontsize=7, framealpha=0.95, loc="lower right"); ax.set_ylim(0, 18)

    ax = axes[1]
    ax.plot(res.sample_epoch_s, res.vllm_num_requests_running, color=COLOR_RUNNING, linewidth=1.2, label="running")
    ax.plot(res.sample_epoch_s, res.vllm_num_requests_waiting, color=COLOR_WAITING, linewidth=1.2, label="waiting")
    ax.set_xlabel("Elapsed (s)", fontsize=7.5); ax.set_ylabel("# vLLM requests", fontsize=7.5)
    ax.legend(fontsize=7, framealpha=0.95, loc="upper right")
    # [title removed to prevent overlap]

    add_panel_labels(fig, style="parens")
    # notes_caption removed — evidence is in EXPERIMENT_DATA_ANALYSIS_20260727.md. Diagnosis: AIMD congestion signal ...", fontsize=7.5)
    plt.tight_layout(rect=[0, 0.02, 1, 0.97])
    paths = export_figure(fig, "make_signal_blindspot_fig", out)
    plt.close(fig); return paths


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()
    setup_style()
    here = Path(__file__).resolve()
    root = Path(args.project_root) if args.project_root else here.parents[2]
    out = Path(args.output_dir) if args.output_dir else root / "figures" / "data" / "formal_experiments"
    out.mkdir(parents=True, exist_ok=True)
    res = root / "experiments" / "results"
    p1 = make_controller_matrix(res / "adaptive_admission_controller_20260726" / "comparison_summary.csv", out)
    print(f"[RC2-adm-v2] F1: {[p.name for p in p1]}")
    p2 = make_shared_vllm(res / "shared_vllm_adaptive_admission_20260726" / "formal_512" / "comparison_summary.csv", out)
    print(f"[RC2-adm-v2] F2: {[p.name for p in p2]}")
    p3 = make_signal_blindspot(res / "shared_vllm_adaptive_admission_20260726" / "formal_512" / "traces", out)
    print(f"[RC2-adm-v2] F3: done")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
