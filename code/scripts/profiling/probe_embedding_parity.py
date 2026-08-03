#!/usr/bin/env python
"""Offline per-row embedding parity probe between two image arms.

Verifiable goal
---------------
Given two ``.npz`` dumps (each with ``embeddings`` + ``doc_ids``) produced by
``run_image_clip_e2e.py --save-embeddings``, decide whether the two arms produce
**the same embedding up to scale/normalization** (→ a unified L2-normalized
AI_EMBED contract is defensible) or a **composite semantic difference**
(processor/dtype/pooling differ → separate-boundary track; do not rank together,
do not modify vendor internals to force alignment).

Pre-registered "scale/normalization only" verdict (ALL must hold):
  - doc_id sets identical, no duplicate / missing, alignable by doc_id;
  - both embeddings finite, same dimension;
  - offline-L2-normalized per-row cosine: P1 >= 0.999 AND min >= 0.99;
  - mean non-self Top-10 neighbor overlap >= 0.90.
If any fails → COMPOSITE_SEMANTIC_DIFFERENCE (separate-boundary).

Reports the cosine DISTRIBUTION (min/P1/P50/P99/mean) and non-self Top-K
overlap for K in --ks (the sample itself is EXCLUDED, else Recall@1 is
trivially ~100%). Does NOT judge by checksum alone and does NOT claim any
performance difference (this is a 256-row semantic probe).

Usage
-----
    python probe_embedding_parity.py \\
        --arm-a daft_builtin.npz --arm-b project_ray.npz \\
        --out-dir motivation/results/gpu/image_embedding_parity_20260803
"""

import argparse
import csv
import json
import os
import sys

import numpy as np


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--arm-a", required=True, help=".npz from arm A (e.g. daft_builtin_embed)")
    p.add_argument("--arm-b", required=True, help=".npz from arm B (e.g. project_ray)")
    p.add_argument("--label-a", default="", help="human label for arm A (default: manifest arm)")
    p.add_argument("--label-b", default="", help="human label for arm B")
    p.add_argument("--ks", default="1,5,10", help="comma list of K for non-self neighbor overlap")
    p.add_argument("--out-dir", required=True)
    # pre-registered thresholds (scale-only verdict)
    p.add_argument("--cosine-p1-min", type=float, default=0.999)
    p.add_argument("--cosine-abs-min", type=float, default=0.99)
    p.add_argument("--overlap10-min", type=float, default=0.90)
    return p.parse_args()


def load_arm(path):
    z = np.load(path, allow_pickle=True)
    emb = np.asarray(z["embeddings"])
    ids = [str(x) for x in z["doc_ids"]]
    manifest = {}
    sidecar = path + ".manifest.json"
    if os.path.exists(sidecar):
        with open(sidecar) as f:
            manifest = json.load(f)
    return ids, emb, manifest


def l2_normalize(x):
    x = np.asarray(x, dtype=np.float64)  # float64 for a stable cosine
    n = np.linalg.norm(x, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return x / n


def pct(vals, q):
    vals = np.asarray(vals, dtype=np.float64)
    if not len(vals):
        return float("nan")
    s = np.sort(vals)
    k = (len(s) - 1) * q
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return float(s[f] + (s[c] - s[f]) * (k - f))


def topk_nonself(sim_row, i, k):
    """Top-k neighbor indices in sim_row, excluding self i."""
    s = sim_row.copy()
    s[i] = -np.inf
    k = min(k, len(s) - 1)
    return set(np.argpartition(-s, k - 1)[:k].tolist())


def main():
    args = parse_args()
    ks = [int(x) for x in args.ks.split(",") if x.strip()]
    os.makedirs(args.out_dir, exist_ok=True)

    ids_a, emb_a, man_a = load_arm(args.arm_a)
    ids_b, emb_b, man_b = load_arm(args.arm_b)
    label_a = args.label_a or man_a.get("arm", "A")
    label_b = args.label_b or man_b.get("arm", "B")

    dim_a = emb_a.shape[1] if emb_a.ndim == 2 else "?"
    dim_b = emb_b.shape[1] if emb_b.ndim == 2 else "?"
    print(f"arm A: {label_a} | rows={len(ids_a)} dim={dim_a} | "
          f"model={man_a.get('model_revision')} dtype={man_a.get('dtype')}")
    print(f"arm B: {label_b} | rows={len(ids_b)} dim={dim_b} | "
          f"model={man_b.get('model_revision')} dtype={man_b.get('dtype')}")

    # --- success criteria: doc_id set + dup + dim + finite ---
    set_a, set_b = set(ids_a), set(ids_b)
    dup_a = len(ids_a) - len(set_a)
    dup_b = len(ids_b) - len(set_b)
    common = sorted(set_a & set_b)
    only_a, only_b = set_a - set_b, set_b - set_a
    print(f"\ndoc_id: A unique={len(set_a)} dup={dup_a} | B unique={len(set_b)} dup={dup_b} | "
          f"common={len(common)} only_a={len(only_a)} only_b={len(only_b)}")
    dim_ok = emb_a.ndim == 2 and emb_b.ndim == 2 and emb_a.shape[1] == emb_b.shape[1]
    finite_a, finite_b = bool(np.all(np.isfinite(emb_a))), bool(np.all(np.isfinite(emb_b)))
    print(f"dim: A={emb_a.shape} B={emb_b.shape} same={dim_ok} | finite A={finite_a} B={finite_b}")

    setup_ok = (
        dup_a == 0 and dup_b == 0
        and len(only_a) == 0 and len(only_b) == 0
        and dim_ok and finite_a and finite_b
    )
    if not setup_ok:
        print("\nVERDICT: SETUP_FAIL -- doc_id/dim/finite checks failed; cannot compare semantics.")
        sys.exit(1)

    # align both matrices to the common doc_id order
    idx_a = {d: i for i, d in enumerate(ids_a)}
    idx_b = {d: i for i, d in enumerate(ids_b)}
    A = emb_a[[idx_a[d] for d in common]]
    B = emb_b[[idx_b[d] for d in common]]
    n = len(common)

    # raw norm distribution (shows the scale difference, if any)
    norm_a = np.linalg.norm(emb_a.astype(np.float64), axis=1)
    norm_b = np.linalg.norm(emb_b.astype(np.float64), axis=1)
    print(f"\nraw norm A: min/p50/p99/max = {norm_a.min():.4f}/{pct(norm_a,0.5):.4f}/"
          f"{pct(norm_a,0.99):.4f}/{norm_a.max():.4f}")
    print(f"raw norm B: min/p50/p99/max = {norm_b.min():.4f}/{pct(norm_b,0.5):.4f}/"
          f"{pct(norm_b,0.99):.4f}/{norm_b.max():.4f}")

    # offline L2-normalize, then per-row cosine (same doc_id)
    A_n, B_n = l2_normalize(A), l2_normalize(B)
    cos = np.sum(A_n * B_n, axis=1)
    max_abs = np.max(np.abs(A_n - B_n), axis=1)
    cos_p1, cos_min = pct(cos, 0.01), float(cos.min())
    print("\npost-norm per-row cosine (A vs B, same doc_id):")
    print(f"  min={cos_min:.6f} P1={cos_p1:.6f} P50={pct(cos,0.5):.6f} "
          f"P99={pct(cos,0.99):.6f} mean={cos.mean():.6f}")
    print(f"  post-norm max-abs: min={max_abs.min():.6f} P50={pct(max_abs,0.5):.6f} "
          f"P99={pct(max_abs,0.99):.6f} max={max_abs.max():.6f}")

    # non-self top-K neighbor overlap (cosine sim matrix, post-norm)
    sim_a = A_n @ A_n.T
    sim_b = B_n @ B_n.T
    overlap = {k: np.empty(n) for k in ks}
    for i in range(n):
        for k in ks:
            na = topk_nonself(sim_a[i], i, k)
            nb = topk_nonself(sim_b[i], i, k)
            overlap[k][i] = len(na & nb) / k
    print("\nnon-self neighbor overlap (sample itself excluded):")
    ov_summary = {}
    for k in ks:
        ov_summary[k] = {"mean": float(overlap[k].mean()), "p1": pct(overlap[k], 0.01),
                         "p50": pct(overlap[k], 0.5)}
        print(f"  @{k}: mean={ov_summary[k]['mean']:.4f} "
              f"P1={ov_summary[k]['p1']:.4f} P50={ov_summary[k]['p50']:.4f}")

    # pre-registered verdict
    k_for_check = 10 if 10 in ks else max(ks)
    cos_ok = cos_p1 >= args.cosine_p1_min and cos_min >= args.cosine_abs_min
    ov_ok = ov_summary[k_for_check]["mean"] >= args.overlap10_min
    if cos_ok and ov_ok:
        verdict = "SCALE_NORMALIZATION_ONLY"
        print(f"\nVERDICT: {verdict} -- post-norm cosine P1>=0.999 & min>=0.99 AND "
              f"mean @{k_for_check} overlap>=0.90. A unified L2-normalized AI_EMBED "
              f"contract is defensible; normalization cost must be fairly counted in "
              f"E2E for all systems, and raw vendor-native results must be kept too.")
    else:
        verdict = "COMPOSITE_SEMANTIC_DIFFERENCE"
        reasons = []
        if not cos_ok:
            reasons.append(f"cosine P1={cos_p1:.4f}/min={cos_min:.4f} below threshold")
        if not ov_ok:
            reasons.append(f"mean @{k_for_check} overlap={ov_summary[k_for_check]['mean']:.4f} below 0.90")
        print(f"\nVERDICT: {verdict} -- {'; '.join(reasons)}. Daft built-in path differs in "
              f"processor/dtype/pooling -> separate-boundary track; do NOT rank with the "
              f"project CLIP path on one throughput axis, do NOT modify vendor internals "
              f"to force alignment.")

    # per-row CSV
    perrow = os.path.join(args.out_dir, "per_row.csv")
    with open(perrow, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["doc_id", "cosine_postnorm", "max_abs_postnorm"]
                   + [f"overlap_at{k}" for k in ks])
        for i, d in enumerate(common):
            w.writerow([d, f"{cos[i]:.6f}", f"{max_abs[i]:.6f}"]
                       + [f"{overlap[k][i]:.4f}" for k in ks])
    # summary CSV
    summary = os.path.join(args.out_dir, "summary.csv")
    with open(summary, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["field", "value"])
        w.writerow(["arm_a", label_a]); w.writerow(["arm_b", label_b])
        w.writerow(["n_common", n]); w.writerow(["dimension", emb_a.shape[1]])
        w.writerow(["dup_a", dup_a]); w.writerow(["dup_b", dup_b])
        w.writerow(["raw_norm_a_p50", f"{pct(norm_a,0.5):.4f}"])
        w.writerow(["raw_norm_b_p50", f"{pct(norm_b,0.5):.4f}"])
        w.writerow(["cosine_min", f"{cos_min:.6f}"])
        w.writerow(["cosine_P1", f"{cos_p1:.6f}"])
        w.writerow(["cosine_P50", f"{pct(cos,0.5):.6f}"])
        w.writerow(["cosine_P99", f"{pct(cos,0.99):.6f}"])
        w.writerow(["cosine_mean", f"{cos.mean():.6f}"])
        for k in ks:
            w.writerow([f"overlap{k}_mean", f"{ov_summary[k]['mean']:.4f}"])
            w.writerow([f"overlap{k}_P1", f"{ov_summary[k]['p1']:.4f}"])
        w.writerow(["verdict", verdict])
        w.writerow(["model_a", man_a.get("model_revision", "")])
        w.writerow(["model_b", man_b.get("model_revision", "")])
        w.writerow(["dtype_a", man_a.get("dtype", "")])
        w.writerow(["dtype_b", man_b.get("dtype", "")])
        w.writerow(["daft_version_a", man_a.get("daft_version", "")])
        w.writerow(["transformers_version_a", man_a.get("transformers_version", "")])
        w.writerow(["daft_version_b", man_b.get("daft_version", "")])
        w.writerow(["transformers_version_b", man_b.get("transformers_version", "")])
    print(f"\nper-row -> {perrow}")
    print(f"summary -> {summary}")


if __name__ == "__main__":
    main()
