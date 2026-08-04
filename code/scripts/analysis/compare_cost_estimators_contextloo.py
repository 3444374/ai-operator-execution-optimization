#!/usr/bin/env python3
"""Cost-estimator selection-metric evaluation via leave-one-context-out CV (B-line 2a).

Agent B's finding: the scenario_group split yields only ~3.6 multi-candidate contexts
per test seed -> noisy selection metrics. Switching the selection evaluation to
leave-one-context-out (LOO) CV over multi-candidate decision contexts puts EACH held-out
context's full candidate set into test, so 13 multi-candidate contexts -> 13 selection
evaluations (vs 3.6). Zero GPU; reuses the estimators + metrics from the full driver.

Reports per-estimator LOO-averaged selection (pick_rate / decision_regret / surpassed /
selected_rank) + the LOO count. Compare to the scenario_group table in
compare_cost_estimators_full.py to see the noise reduction.
"""
from __future__ import annotations

import csv
import importlib.util
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
REPO_ROOT = CODE_ROOT.parent
sys.path.insert(0, str(CODE_ROOT))

_spec = importlib.util.spec_from_file_location(
    "estimate_operator_cost", CODE_ROOT / "scripts" / "analysis" / "estimate_operator_cost.py"
)
_drv = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_drv)

from src.planning.costs.regression import (  # noqa: E402
    pairwise_accuracy,
    regression_metrics,
    selection_metrics,
    top_k_precision,
)

REF_JSON = REPO_ROOT / "experiments" / "results" / "operator_cost_estimation_20260726" / "e2e_cost_model.json"
TARGET = "e2e_s"
# import the full driver's loader + estimator dispatch (single source of truth)
_full_spec = importlib.util.spec_from_file_location(
    "compare_cost_estimators_full",
    CODE_ROOT / "scripts" / "analysis" / "compare_cost_estimators_full.py",
)
_full = importlib.util.module_from_spec(_full_spec)
assert _full_spec and _full_spec.loader
_full_spec.loader.exec_module(_full)
load_rows = _full.load_rows
build = _full.build
_fit_predict = _full._fit_predict
ESTIMATORS = _full.ESTIMATORS


def main() -> None:
    rows = load_rows()
    features, targets, groups, contexts, candidates = build(rows)
    # index rows by decision context
    ctx_to_idx: dict[str, list[int]] = defaultdict(list)
    for i, ctx in enumerate(contexts):
        ctx_to_idx[ctx].append(i)
    multi = {
        ctx: idxs
        for ctx, idxs in ctx_to_idx.items()
        if len({candidates[i] for i in idxs}) >= 2
    }
    print(f"rows={len(rows)} total_contexts={len(ctx_to_idx)} multi_candidate_contexts={len(multi)}")

    acc = {e: {k: [] for k in ("mae", "spearman", "pairwise", "topk5", "pick_rate", "regret", "surpassed", "selected_rank", "n_cand")} for e in ESTIMATORS}
    evaluated = 0
    for ctx, test_idx in multi.items():
        test_set = set(test_idx)
        train_idx = [i for i in range(len(rows)) if i not in test_set]
        if not train_idx:
            continue
        tr = np.asarray(train_idx, dtype=int)
        te = np.asarray(test_idx, dtype=int)
        train_rows = [rows[i] for i in tr]
        test_rows = [rows[i] for i in te]
        train_y, test_y = targets[tr], targets[te]
        train_feat, test_feat = features[tr], features[te]
        t_ctx = [contexts[i] for i in te]
        t_cand = [candidates[i] for i in te]
        evaluated += 1
        for name in ESTIMATORS:
            test_pred, _ = _fit_predict(name, train_rows, test_rows, train_feat, test_feat, train_y)
            if test_pred is None or len(test_pred) != len(test_y):
                continue
            sel = selection_metrics(test_y, test_pred, t_ctx, t_cand)
            reg = regression_metrics(test_y, test_pred)
            a = acc[name]
            a["mae"].append(reg.mae)
            a["spearman"].append(reg.spearman_rho)
            a["pairwise"].append(pairwise_accuracy(test_y, test_pred) if len(test_y) >= 2 else float("nan"))
            a["topk5"].append(top_k_precision(test_y, test_pred, k=5) if len(test_y) >= 4 else float("nan"))
            a["pick_rate"].append(sel.get("pick_rate", 0.0))
            a["regret"].append(sel.get("decision_regret_pct", 0.0))
            a["surpassed"].append(sel.get("surpassed_plans", 0))
            a["selected_rank"].append(sel.get("selected_plan_rank_mean", 0.0))
            a["n_cand"].append(len(set(t_cand)))

    def mean(key, fmt="{:.3f}"):
        vals = [v for v in a[key] if isinstance(v, (int, float)) and np.isfinite(v)]
        return fmt.format(statistics.mean(vals)) if vals else "  nan"

    print(f"\n=== LOO over {evaluated} multi-candidate contexts (each context's full candidate set in test) ===")
    print(f"{'estimator':16s} {'MAE':>7s} {'Sρ':>6s} {'pairwise':>9s} {'topK5':>6s} {'pick':>6s} {'regret%':>8s} {'selRank':>8s} {'surpassed':>10s}")
    print("-" * 86)
    for name in ESTIMATORS:
        a = acc[name]
        if not a["mae"]:
            print(f"{name:16s}  (skipped)")
            continue
        print(
            f"{name:16s} {mean('mae','{:7.2f}')} {mean('spearman','{:6.3f}')} "
            f"{mean('pairwise','{:9.3f}')} {mean('topk5','{:6.2f}')} {mean('pick_rate','{:6.2f}')} "
            f"{mean('regret','{:8.2f}')} {mean('selected_rank','{:8.2f}')} {mean('surpassed','{:10.2f}')}"
        )
    print("\nCompare: scenario_group split gave ~3.6 test contexts/seed (noisy); this LOO")
    print(f"gives {evaluated} evaluations. Selection metrics (pick/regret) are now per-FULL-context.")


if __name__ == "__main__":
    main()
