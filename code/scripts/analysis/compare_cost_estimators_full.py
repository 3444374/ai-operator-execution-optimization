#!/usr/bin/env python3
"""Unified cost-estimator baseline comparison: CE0-CE6 x 3 metric layers x 5 seeds.

Implements the project cost-estimation baseline plan: compare the full estimator
hierarchy on the same 283-row AI_COMPLETE profile, same grouped split, same candidate
contexts, across prediction / ranking / decision layers. Supersedes
compare_cost_estimators.py (mean-vs-ridge only). Reuses estimate_operator_cost loaders
+ regression.py + estimators.py; modifies no existing code.

Estimators:
  CE0 mean (sanity) | CE1 analytical | CE2 lookup | CE3 ridge (existing) |
  CE4 lightgbm (lazy import; skipped if unavailable) | CE5 hybrid (analytical+residual)
CE6 oracle is the actual target (perfect, upper bound) and is reported as a reference
row, not fit. Prediction interval uses empirical training-residual quantiles.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import statistics
import sys
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
    RidgeCostEstimator,
    grouped_train_test_split,
    pairwise_accuracy,
    regression_metrics,
    residual_interval_bounds,
    selection_metrics,
    top_k_precision,
)
from src.planning.costs.estimators import (  # noqa: E402
    AnalyticalCostEstimator,
    HybridCostEstimator,
    LightGBMCostEstimator,
    LookupCostEstimator,
)

REF_JSON = REPO_ROOT / "experiments" / "results" / "operator_cost_estimation_20260726" / "e2e_cost_model.json"
SEEDS = (20260726, 20260727, 20260728, 20260729, 20260730)
TARGET = "e2e_s"
TEST_FRACTION = 0.25
CONFIDENCE = 0.9
ESTIMATORS = ("CE0_mean", "CE1_analytical", "CE2_lookup", "CE3_ridge", "CE4_lightgbm", "CE5_hybrid")


def load_rows() -> list[dict]:
    ref = json.loads(REF_JSON.read_text(encoding="utf-8"))
    paths = [REPO_ROOT / p.replace("\\", "/") for p in ref["source_csvs"]]
    missing = [p for p in paths if not p.exists()]
    if missing:
        raise SystemExit(f"MISSING input csvs: {[str(p) for p in missing[:3]]}")
    rows: list[dict] = []
    for path in paths:
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if row.get("status") != "ok":
                    continue
                try:
                    _drv.feature_vector(row)
                    _drv._number(row, TARGET)
                    float(row["total_rows"])
                    float(row["token_count"])
                    float(row["completion_max_tokens"])
                except (KeyError, TypeError, ValueError):
                    continue
                if float(_drv._number(row, TARGET)) < 0:
                    continue
                rows.append(row)
    return rows


def build(rows: list[dict]):
    features = np.asarray([_drv.feature_vector(r) for r in rows], dtype=float)
    targets = np.asarray([_drv._number(r, TARGET) for r in rows], dtype=float)
    groups = [_drv.scenario_group(r) for r in rows]
    contexts = [
        _drv._payload_signature(_drv.decision_context_payload(r)) for r in rows
    ]
    candidates = [
        _drv._payload_signature(_drv.candidate_payload(r)) for r in rows
    ]
    return features, targets, groups, contexts, candidates


def _fit_predict(name, train_rows, test_rows, train_feat, test_feat, train_y):
    """Return (test_pred, train_pred_for_residuals) or (None, None) if unavailable."""
    if name == "CE0_mean":
        m = float(np.mean(train_y))
        return np.full(len(test_rows), m), np.full(len(train_rows), m)
    if name == "CE1_analytical":
        e = AnalyticalCostEstimator().fit(train_rows, train_y)
        return e.predict(test_rows), e.predict(train_rows)
    if name == "CE2_lookup":
        e = LookupCostEstimator().fit(train_rows, train_y)
        return e.predict(test_rows), e.predict(train_rows)
    if name == "CE3_ridge":
        e = RidgeCostEstimator(alpha=1.0).fit(train_feat, train_y)
        return e.predict(test_feat), e.predict(train_feat)
    if name == "CE4_lightgbm":
        try:
            e = LightGBMCostEstimator().fit(train_feat, train_y)
        except (ImportError, ModuleNotFoundError):
            return None, None
        return e.predict(test_feat), e.predict(train_feat)
    if name == "CE5_hybrid":
        e = HybridCostEstimator(alpha=1.0).fit(train_rows, train_feat, train_y)
        return e.predict(test_rows, test_feat), e.predict(train_rows, train_feat)
    raise ValueError(name)


METRIC_KEYS = (
    "mae", "rmse", "q95", "spearman", "pairwise", "topk5",
    "pick_rate", "decision_regret_pct", "interval_coverage", "interval_width",
    "n_contexts",
)


def main() -> None:
    rows = load_rows()
    features, targets, groups, contexts, candidates = build(rows)
    print(f"rows={len(rows)} seeds={list(SEEDS)} target={TARGET} test_fraction={TEST_FRACTION}")
    acc = {e: {k: [] for k in METRIC_KEYS} for e in ESTIMATORS}
    skipped: dict[str, str] = {}

    for seed in SEEDS:
        split = grouped_train_test_split(groups, test_fraction=TEST_FRACTION, seed=seed)
        tr = np.asarray(split.train_indices, dtype=int)
        te = np.asarray(split.test_indices, dtype=int)
        train_rows = [rows[i] for i in tr]
        test_rows = [rows[i] for i in te]
        train_feat, test_feat = features[tr], features[te]
        train_y, test_y = targets[tr], targets[te]
        t_ctx = [contexts[i] for i in te]
        t_cand = [candidates[i] for i in te]
        for name in ESTIMATORS:
            test_pred, train_pred = _fit_predict(
                name, train_rows, test_rows, train_feat, test_feat, train_y
            )
            if test_pred is None:
                skipped[name] = "lightgbm unavailable"
                continue
            reg = regression_metrics(test_y, test_pred)
            sel = selection_metrics(test_y, test_pred, t_ctx, t_cand)
            residuals = train_y - train_pred
            lo, hi = residual_interval_bounds(residuals, confidence=CONFIDENCE)
            if np.isfinite(lo) and np.isfinite(hi):
                errors = test_y - test_pred
                coverage = float(np.mean((errors >= lo) & (errors <= hi)))
                width = float(np.mean(hi - lo))
            else:
                coverage = float("nan")
                width = float("nan")
            m = acc[name]
            m["mae"].append(reg.mae)
            m["rmse"].append(reg.rmse)
            m["q95"].append(reg.q_error_p95)
            m["spearman"].append(reg.spearman_rho)
            m["pairwise"].append(pairwise_accuracy(test_y, test_pred))
            m["topk5"].append(top_k_precision(test_y, test_pred, k=5))
            m["pick_rate"].append(sel.get("pick_rate", 0.0))
            m["decision_regret_pct"].append(sel.get("decision_regret_pct", 0.0))
            m["interval_coverage"].append(coverage)
            m["interval_width"].append(width)
            m["n_contexts"].append(sel.get("decision_contexts_evaluated", 0))

    print(f"\ncontexts_evaluated avg={statistics.mean(acc['CE3_ridge']['n_contexts']):.1f} "
          f"(selection metrics noisy at this scale)")
    if skipped:
        print(f"skipped: {skipped}")
    print("\n=== unified cost-estimator baseline (5-seed mean) ===")
    print("(prediction: lower MAE/RMSE/Q95 better, higher Spearman better; "
          "ranking: higher pairwise/topk/pick better; decision: lower regret better)")
    hdr = f"{'estimator':16s} {'MAE':>7s} {'RMSE':>7s} {'Q95':>6s} {'Sρ':>6s} {'pairwise':>9s} {'topK5':>6s} {'pick':>6s} {'regret%':>8s} {'intvCov':>8s} {'intvW':>7s}"
    print(hdr)
    print("-" * len(hdr))
    for name in ESTIMATORS:
        if not acc[name]["mae"]:
            print(f"{name:16s}  (skipped: {skipped.get(name,'?')})")
            continue
        a = acc[name]

        def mm(key, fmt="{:.2f}"):
            vals = [v for v in a[key] if isinstance(v, (int, float)) and np.isfinite(v)]
            return fmt.format(statistics.mean(vals)) if vals else "  nan"

        print(
            f"{name:16s} {mm('mae','{:7.2f}')} {mm('rmse','{:7.2f}')} {mm('q95','{:6.2f}')} "
            f"{mm('spearman','{:6.3f}')} {mm('pairwise','{:9.3f}')} {mm('topk5','{:6.2f}')} "
            f"{mm('pick_rate','{:6.2f}')} {mm('decision_regret_pct','{:8.2f}')} "
            f"{mm('interval_coverage','{:8.2f}')} {mm('interval_width','{:7.2f}')}"
        )
    print("\nCE6_oracle: MAE=0, Q=1, Spearman=1, pairwise=1, pick=1, regret=0 (reference upper bound; not fit).")
    print("\nRead: Heinrich 'accuracy != selection' — a lower-MAE estimator can still pick worse plans")
    print("(higher regret). Watch pairwise/topK vs MAE divergence, and interval coverage vs width.")


if __name__ == "__main__":
    main()
