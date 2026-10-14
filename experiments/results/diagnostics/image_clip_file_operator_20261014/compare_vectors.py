#!/usr/bin/env python3
"""Compare two diagnostic NPZ captures by ID, preserving raw FP32 values.

Tolerance is supplied explicitly and produces descriptive counts, not an
overall semantic pass gate. Do not use capture-enabled time for performance.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path

import numpy as np


def load(path):
    with np.load(path, allow_pickle=False) as data:
        ids = tuple(str(item) for item in data["doc_ids"])
        vectors = np.asarray(data["embeddings"], dtype=np.float32)
    if not ids or len(set(ids)) != len(ids) or vectors.ndim != 2 or vectors.shape[0] != len(ids) or not np.isfinite(vectors).all():
        raise ValueError("invalid ID/shape/finite contract")
    return ids, vectors


def compare(a_path, b_path, rtol, atol, out):
    if min(rtol, atol) < 0 or not np.isfinite([rtol, atol]).all():
        raise ValueError("tolerances must be finite and non-negative")
    a_ids, a = load(a_path)
    b_ids, b = load(b_path)
    if set(a_ids) != set(b_ids) or a.shape != b.shape:
        raise ValueError("ID sets or dimensions differ")
    if len(a_ids) > 4096:
        raise ValueError("diagnostic limit is 4096 rows")
    index_a = {doc_id: index for index, doc_id in enumerate(a_ids)}
    index_b = {doc_id: index for index, doc_id in enumerate(b_ids)}
    ids = sorted(index_a)
    a = a[[index_a[doc_id] for doc_id in ids]].astype(np.float64)
    b = b[[index_b[doc_id] for doc_id in ids]].astype(np.float64)
    difference = np.abs(a-b)
    # Symmetric bound, recorded explicitly. No hidden post-normalization.
    within = difference <= atol + rtol * np.maximum(np.abs(a), np.abs(b))
    norm_a, norm_b = np.linalg.norm(a, axis=1), np.linalg.norm(b, axis=1)
    if np.any(norm_a == 0) or np.any(norm_b == 0):
        raise ValueError("zero-norm output")
    cosine = np.sum(a*b, axis=1) / (norm_a*norm_b)
    fields = ["doc_id", "max_abs_error", "mean_abs_error", "l2_error", "cosine", "norm_a", "norm_b", "within_explicit_tolerance", "components_outside_tolerance"]
    out.mkdir(parents=True, exist_ok=False)
    with (out / "per_id.csv").open("x", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(fields)
        for index, doc_id in enumerate(ids):
            writer.writerow([doc_id, float(difference[index].max()), float(difference[index].mean()), float(np.linalg.norm(a[index]-b[index])), float(cosine[index]), float(norm_a[index]), float(norm_b[index]), bool(within[index].all()), int((~within[index]).sum())])
    result = {
        "arm_a_npz_sha256": hashlib.sha256(a_path.read_bytes()).hexdigest(),
        "arm_b_npz_sha256": hashlib.sha256(b_path.read_bytes()).hexdigest(),
        "rows": len(ids), "dimension": a.shape[1], "alignment": "exact doc_id; sorted offline after native execution",
        "raw_value_comparison": "float32 capture converted losslessly to float64 for differences; original outputs not renormalized",
        "tolerance": {"rtol": rtol, "atol": atol, "definition": "abs(a-b) <= atol + rtol*max(abs(a),abs(b))", "purpose": "descriptive only; no new overall pass gate"},
        "components_outside_tolerance": int((~within).sum()), "rows_outside_tolerance": int((~within.all(axis=1)).sum()),
        "max_abs_error": float(difference.max()), "mean_abs_error": float(difference.mean()),
        "per_row_max_abs_percentiles": {str(q): float(np.percentile(difference.max(axis=1), q)) for q in (0, 1, 50, 99, 100)},
        "cosine_percentiles": {str(q): float(np.percentile(cosine, q)) for q in (0, 1, 50, 99, 100)},
        "norm_error_a_max": float(np.abs(norm_a-1).max()), "norm_error_b_max": float(np.abs(norm_b-1).max()),
        "bitwise_equal": bool(np.array_equal(a.astype(np.float32).view(np.uint32), b.astype(np.float32).view(np.uint32))),
        "semantic_equivalence_verdict": "not inferred from numerical diagnostics alone", "performance_eligible": False,
    }
    (out / "comparison.json").write_text(json.dumps(result, indent=2)+"\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm-a", required=True, type=Path)
    parser.add_argument("--arm-b", required=True, type=Path)
    parser.add_argument("--rtol", required=True, type=float)
    parser.add_argument("--atol", required=True, type=float)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(args.arm_a, args.arm_b, args.rtol, args.atol, args.out_dir), indent=2))
